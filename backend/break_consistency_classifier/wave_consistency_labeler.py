import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2


class WaveConsistencyLabeler:
    def __init__(self):
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        self.db_url_existing = (
            f"postgresql://{user}:{password}@{host}/localized_wave_data"
        )
        self.db_url_new = (
            f"postgresql://{user}:{password}@{host}/wave_consistency_training"
        )
        self.engine_existing = create_engine(self.db_url_existing)
        self.engine_new = create_engine(self.db_url_new)

    def create_new_database(self):
        """
        Create the new 'wave_consistency_training' database if it does not exist.
        """
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE wave_consistency_training;")
            cursor.close()
            conn.close()
            print("Database 'wave_consistency_training' created successfully.")
        except Exception as e:
            print(f"Error creating database or it may already exist: {e}")

    def load_data_from_db(self):
        """
        Load wave data from the 'localized_wave_data' table.
        """
        query = """
            SELECT station_id, datetime, wvht, dpd, apd, mwd
            FROM localized_wave_data;
        """
        df = pd.read_sql(query, self.engine_existing)
        print(f"Loaded {len(df)} records from 'localized_wave_data'.")
        return df

    def calculate_consistency(self, df):
        """
        Calculate the consistency of each station's metrics over time.
        """
        # Group by station_id and compute standard deviation for each metric
        grouped = df.groupby("station_id")[["wvht", "dpd", "apd", "mwd"]].std()

        # Define consistency categories based on standard deviation thresholds.
        thresholds = {
            "wvht": 0.5,
            "dpd": 1.0,
            "apd": 1.0,
            "mwd": 1.0,
        }

        def label_consistency(row):
            consistency_score = 0
            for metric in ["wvht", "dpd", "apd", "mwd"]:
                if row[metric] <= thresholds[metric]:
                    consistency_score += 1
            if consistency_score == 4:
                return "High Consistency"
            elif 2 <= consistency_score < 4:
                return "Medium Consistency"
            else:
                return "Low Consistency"

        # Apply consistency labeling
        grouped["Consistency"] = grouped.apply(label_consistency, axis=1)
        print("Consistency labels generated.")

        # Reset index to make station_id a column again
        grouped = grouped.reset_index()
        return grouped

    def save_to_new_database(self, df):
        """
        Save the labeled data to the 'wave_consistency_training' database.
        """
        try:
            df.to_sql(
                "wave_consistency_labels",
                self.engine_new,
                if_exists="replace",
                index=False,
            )
            print(
                "Labeled data saved to 'wave_consistency_labels' table in 'wave_consistency_training' database."
            )
        except Exception as e:
            print(f"Error saving to database: {e}")

    def run(self):
        """
        Run the full workflow.
        """
        # Create the new database
        self.create_new_database()

        # Load data from the existing database
        df = self.load_data_from_db()

        # Calculate consistency labels
        labeled_df = self.calculate_consistency(df)

        # Save the labeled data to the new database
        self.save_to_new_database(labeled_df)


if __name__ == "__main__":
    labeler = WaveConsistencyLabeler()
    labeler.run()

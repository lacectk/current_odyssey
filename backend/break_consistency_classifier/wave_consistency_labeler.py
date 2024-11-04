from backend.create_database import create_database
import os
import pandas as pd
from sqlalchemy import create_engine


class WaveConsistencyLabeler:
    def __init__(self):
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        self.db_url_existing = (
            f"postgresql://{user}:{password}@{host}/localized_wave_data"
        )
        self.db_url_new = f"postgresql://{user}:{password}@{host}/wave_consistency"
        self.engine_existing = create_engine(self.db_url_existing)
        self.engine_new = create_engine(self.db_url_new)

    def load_data_from_db(self):
        """
        Load wave data from the 'localized_wave_data' table.
        """
        query = """
            SELECT station_id, datetime, wvht, dpd, apd, mwd
            FROM localized_wave_data;
        """
        df = pd.read_sql(query, self.engine_existing)
        return df

    def calculate_std_dev(self, df):
        """
        Calculate the standard deviation of each metric per station.
        """
        return df.groupby("station_id")[["wvht", "dpd", "apd", "mwd"]].std()

    def apply_binning(self, std_df):
        """
        Apply quantile-based binning to assign consistency labels.
        """
        # Use quantiles for each metric to create labels
        bins = pd.qcut(
            std_df["wvht"],
            q=3,
            labels=["High Consistency", "Medium Consistency", "Low Consistency"],
        )
        std_df["WVHT_Consistency"] = bins

        bins = pd.qcut(
            std_df["dpd"],
            q=3,
            labels=["High Consistency", "Medium Consistency", "Low Consistency"],
        )
        std_df["DPD_Consistency"] = bins

        bins = pd.qcut(
            std_df["apd"],
            q=3,
            labels=["High Consistency", "Medium Consistency", "Low Consistency"],
        )
        std_df["APD_Consistency"] = bins

        bins = pd.qcut(
            std_df["mwd"],
            q=3,
            labels=["High Consistency", "Medium Consistency", "Low Consistency"],
        )
        std_df["MWD_Consistency"] = bins

        return std_df

    def save_to_new_database(self, df):
        """
        Save the labeled data to the 'wave_consistency' database.
        """
        try:
            df.to_sql(
                "wave_consistency",
                self.engine_new,
                if_exists="replace",
                index=False,
            )
            print(
                "Labeled data saved to 'wave_consistency' table in 'wave_consistency' database."
            )
        except Exception as e:
            print(f"Error saving to database: {e}")

    def run(self):
        create_database("wave_consistency")
        # Load data and calculate standard deviations
        df = self.load_data_from_db()
        std_df = self.calculate_std_dev(df)

        # Apply binning
        labeled_df = self.apply_binning(std_df)
        self.save_to_new_database(labeled_df)


labeler = WaveConsistencyLabeler()
labeler.run()

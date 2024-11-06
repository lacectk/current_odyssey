from backend.create_database import create_database
import numpy as np
import pandas as pd
import os
from sklearn.decomposition import PCA
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
            SELECT station_id, datetime, latitude, longitude, wvht, dpd, apd, mwd
            FROM localized_wave_data;
        """
        df = pd.read_sql(query, self.engine_existing)
        return df

    def calculate_monthly_std_dev(self, df):
        """
        Calculate the monthly standard deviation of each metric per station.
        """
        # Convert datetime to monthly periods for grouping
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()

        # Calculate the standard deviation per station per month
        monthly_std_df = df.groupby(["station_id", "month"])[
            ["wvht", "dpd", "apd", "mwd"]
        ].std()
        monthly_std_df.columns = [
            "wvht_monthly_std",
            "dpd_monthly_std",
            "apd_monthly_std",
            "mwd_monthly_std",
        ]
        return monthly_std_df.reset_index()

    def apply_pca_consistency(self, monthly_std_df):
        """
        Use PCA to calculate a single monthly consistency score per station.
        """
        # Filter rows with complete data only (no NaNs)
        valid_std_df = monthly_std_df.dropna()

        # Apply PCA to rows with valid data
        pca = PCA(n_components=1)
        if not valid_std_df.empty:
            try:
                valid_std_df["ConsistencyScore"] = pca.fit_transform(
                    valid_std_df[
                        [
                            "wvht_monthly_std",
                            "dpd_monthly_std",
                            "apd_monthly_std",
                            "mwd_monthly_std",
                        ]
                    ]
                )
            except Exception as e:
                print(f"PCA error: {e}")
                valid_std_df["ConsistencyScore"] = np.nan

        # Merge PCA results back with the full DataFrame
        monthly_std_df = monthly_std_df.merge(
            valid_std_df[["station_id", "month", "ConsistencyScore"]],
            on=["station_id", "month"],
            how="left",
        )

        # Label "Unknown, not enough data" for rows with NaN in ConsistencyScore
        monthly_std_df["ConsistencyLabel"] = np.where(
            monthly_std_df["ConsistencyScore"].isna(), "Unknown, not enough data", None
        )

        # Assign quantile-based labels only for stations with valid PCA scores
        valid_scores = monthly_std_df["ConsistencyScore"].dropna()
        if len(valid_scores.unique()) > 1:  # Ensure enough variation for quantiles
            monthly_std_df.loc[valid_scores.index, "ConsistencyLabel"] = pd.qcut(
                valid_scores,
                q=3,
                labels=["High Consistency", "Medium Consistency", "Low Consistency"],
            )

        return monthly_std_df

    def save_to_new_database(self, df, table_name):
        """
        Save the labeled data to the specified table in the 'wave_consistency' database.
        """
        try:
            df.to_sql(
                table_name,
                self.engine_new,
                if_exists="replace",
                index=False,
            )
            print(
                f"Labeled data saved to '{table_name}' table in 'wave_consistency' database."
            )
        except Exception as e:
            print(f"Error saving to database: {e}")

    def run(self):
        create_database("wave_consistency")

        # Load data and calculate monthly standard deviations
        df = self.load_data_from_db()
        monthly_std_df = self.calculate_monthly_std_dev(df)

        # Apply PCA for monthly consistency label
        labeled_df = self.apply_pca_consistency(monthly_std_df)
        self.save_to_new_database(labeled_df, "wave_consistency_trends")


labeler = WaveConsistencyLabeler()
labeler.run()

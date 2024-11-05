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
            SELECT station_id, datetime, wvht, dpd, apd, mwd
            FROM localized_wave_data;
        """
        df = pd.read_sql(query, self.engine_existing)
        return df

    def calculate_std_dev(self, df):
        """
        Calculate the standard deviation of each metric per station.
        """
        std_df = df.groupby("station_id")[["wvht", "dpd", "apd", "mwd"]].std()
        std_df.columns = ["wvht_std", "dpd_std", "apd_std", "mwd_std"]
        return std_df

    def apply_pca_consistency(self, std_df):
        """
        Use PCA to calculate a single consistency score, excluding stations with NaNs or zeros in all metrics.
        """
        # Filter rows with complete data only (no NaNs)
        valid_std_df = std_df.dropna()

        # Apply PCA to rows with valid data
        pca = PCA(n_components=1)
        if not valid_std_df.empty:
            try:
                valid_std_df.loc[:, "ConsistencyScore"] = pca.fit_transform(
                    valid_std_df
                )
            except Exception as e:
                print(f"PCA error: {e}")
                valid_std_df["ConsistencyScore"] = np.nan

        # Merge PCA results back with the full DataFrame
        std_df = std_df.merge(
            valid_std_df[["ConsistencyScore"]],
            left_index=True,
            right_index=True,
            how="left",
        )

        # Label "Unknown, not enough data" for rows with NaN in ConsistencyScore
        std_df["ConsistencyLabel"] = np.where(
            std_df["ConsistencyScore"].isna(), "Unknown, not enough data", None
        )

        # Assign quantile-based labels only for stations with valid PCA scores
        valid_scores = std_df["ConsistencyScore"].dropna()
        if len(valid_scores.unique()) > 1:  # Ensure enough variation for quantiles
            std_df.loc[valid_scores.index, "ConsistencyLabel"] = pd.qcut(
                valid_scores,
                q=3,
                labels=["High Consistency", "Medium Consistency", "Low Consistency"],
            )

        return std_df.reset_index()

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
            print("Labeled data saved to 'wave_consistency' database.")
        except Exception as e:
            print(f"Error saving to database: {e}")

    def run(self):
        create_database("wave_consistency")
        # Load data and calculate standard deviations
        df = self.load_data_from_db()
        std_df = self.calculate_std_dev(df)

        # Apply binning
        labeled_df = self.apply_pca_consistency(std_df)
        self.save_to_new_database(labeled_df)


labeler = WaveConsistencyLabeler()
labeler.run()

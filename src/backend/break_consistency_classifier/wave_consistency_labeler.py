import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sqlalchemy import create_engine, text
from src.backend.database.db_manager import DatabaseManager

from src.backend.config.database import (
    localized_wave_engine,
    wave_consistency_engine,
)


class WaveConsistencyLabeler:
    def __init__(self):
        self.engine_existing = localized_wave_engine
        self.engine_new = wave_consistency_engine

    def load_data_from_db(self):
        """Load wave data from the raw_data.localized_wave_data table."""
        query = """
            SELECT 
                station_id, 
                datetime, 
                latitude, 
                longitude, 
                "wave_height(wvht)" as wvht,
                "dominant_period(dpd)" as dpd,
                "average_period(apd)" as apd,
                "mean_wave_direction(mwd)" as mwd
            FROM raw_data.localized_wave_data;
        """
        df = pd.read_sql(query, self.engine_existing)
        return df

    def calculate_monthly_std_dev(self, df):
        """
        Calculate the monthly standard deviation of each metric per station.
        """
        # Convert datetime to monthly periods for grouping
        df["month"] = pd.to_datetime(df["datetime"]).dt.to_period("M").dt.to_timestamp()

        # First, get the lat/lon for each station (assuming they're constant per station)
        station_locations = df.groupby("station_id")[["latitude", "longitude"]].first()

        # First, get the lat/lon for each station (assuming they're constant per station)
        station_locations = df.groupby("station_id")[["latitude", "longitude"]].first()

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
        monthly_std_df = monthly_std_df.reset_index()

        # Merge the location data back
        monthly_std_df = monthly_std_df.merge(
            station_locations.reset_index(), on="station_id", how="left"
        )

        return monthly_std_df

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
                valid_std_df["consistency_score"] = pca.fit_transform(
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
                valid_std_df["consistency_score"] = np.nan

        # Merge PCA results back with the full DataFrame
        monthly_std_df = monthly_std_df.merge(
            valid_std_df[["station_id", "month", "consistency_score"]],
            on=["station_id", "month"],
            how="left",
        )

        # Label "Unknown, not enough data" for rows with NaN in consistency_score
        monthly_std_df["consistency_label"] = np.where(
            monthly_std_df["consistency_score"].isna(),
            "Unknown, not enough data",
            None,
        )

        # Assign quantile-based labels only for stations with valid PCA scores
        valid_scores = monthly_std_df["consistency_score"].dropna()
        if len(valid_scores.unique()) > 1:  # Ensure enough variation for quantiles
            monthly_std_df.loc[valid_scores.index, "consistency_label"] = pd.qcut(
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
        """Execute the full wave consistency labeling process."""
        # Initialize database manager
        db_manager = DatabaseManager()

        print("Loading data from database...")
        df = self.load_data_from_db()

        print("Calculating monthly standard deviations...")
        monthly_std_df = self.calculate_monthly_std_dev(df)

        print("Applying PCA for consistency labeling...")
        labeled_df = self.apply_pca_consistency(monthly_std_df)

        print("Saving results to database...")
        self.save_to_new_database(labeled_df, "wave_consistency_trends")

        print("Process completed successfully!")


if __name__ == "__main__":
    labeler = WaveConsistencyLabeler()
    labeler.run()

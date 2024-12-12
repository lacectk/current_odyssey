import os
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from sqlalchemy import create_engine


class WaveConsistencyMapPlotter:
    def __init__(self):
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        self.db_url = f"postgresql://{user}:{password}@{host}/wave_consistency"
        self.engine = create_engine(self.db_url)

        # Color coding for each consistency label
        self.color_map = {
            "High Consistency": "blue",
            "Medium Consistency": "orange",
            "Low Consistency": "red",
            "Unknown, not enough data": "gray",
        }

    def load_consistency_data(self):
        """
        Load consistency data from the 'wave_consistency' table.
        """
        query = """
            SELECT station_id, latitude, longitude, consistency_label
            FROM wave_consistency;
        """
        df = pd.read_sql(query, self.engine)
        return df

    def plot_map(self, df):
        fig, ax = plt.subplots(
            figsize=(12, 8), subplot_kw={"projection": ccrs.PlateCarree()}
        )
        ax.set_extent([-180, 180, -90, 90])  # Global extent

        # Add coastlines and land features for context
        ax.coastlines()
        ax.add_feature(cfeature.LAND, edgecolor="black")
        ax.add_feature(cfeature.BORDERS, linestyle=":")

        # Plot each station based on its consistency label
        for _, row in df.iterrows():
            label = row["consistency_label"]
            color = self.color_map.get(
                label, "black"
            )  # Default to black if label is missing
            ax.plot(
                row["longitude"],
                row["latitude"],
                marker="o",
                color=color,
                markersize=6,
                transform=ccrs.PlateCarree(),
                label=label if label not in ax.get_legend_handles_labels()[1] else "",
            )

        # Add a legend for the consistency labels
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), loc="upper left")

        # Display the map
        plt.title("Wave Consistency Map Overlay")
        plt.show()

    def run(self):
        # Load data and plot map
        df = self.load_consistency_data()
        self.plot_map(df)


# Run the overlay map
overlay = WaveConsistencyMapPlotter()
overlay.run()

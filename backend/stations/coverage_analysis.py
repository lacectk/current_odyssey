import asyncio
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from dotenv import load_dotenv
import psycopg2
import os
import matplotlib.pyplot as plt
from backend.stations.ndbc_stations_data import NDBCDataFetcher


class CoverageAnalysis:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

        # Lists to hold the latitude and longitude data
        self.stations_with_met = []
        self.stations_without_met = []
        self.fetcher = NDBCDataFetcher()

    async def fetch_stations(self):
        # Fetch all station data (both with and without met data)
        stations_data = await self.fetcher.fetch_station_data()

        for station in stations_data["stations"]["station"]:
            station_id = station["@id"]
            latitude = float(station["@lat"])
            longitude = float(station["@lon"])

            try:
                self.cursor.execute(
                    "SELECT latitude, longitude FROM stations WHERE station_id = %s",
                    (station_id,),
                )
                row = self.cursor.fetchone()

                if row:
                    lat, lon = row
                    self.stations_with_met.append((lat, lon))
                else:
                    # If the station_id doesn't exist, append lat/lon from the XML data
                    self.stations_without_met.append((latitude, longitude))
            except Exception as e:
                print(f"Error fetching data for station {station_id}: {e}")
                self.conn.rollback()

    def plot_coverage(self):
        projection = ccrs.PlateCarree()
        fig, ax = plt.subplots(figsize=(10, 7), subplot_kw={"projection": projection})

        ax.coastlines()
        ax.add_feature(cfeature.BORDERS, linestyle=":")

        ax.add_feature(cfeature.LAND, edgecolor="black", zorder=0)
        ax.add_feature(cfeature.OCEAN, zorder=0)

        # Unpack the data for plotting
        if self.stations_with_met:
            lat_with_met, lon_with_met = zip(*self.stations_with_met)
            ax.scatter(
                lon_with_met,
                lat_with_met,
                c="blue",
                label="Buoys with Meteorological Data",
                alpha=0.7,
            )

        if self.stations_without_met:
            lat_without_met, lon_without_met = zip(*self.stations_without_met)
            ax.scatter(
                lon_without_met,
                lat_without_met,
                c="red",
                label="Buoys without Meteorological Data",
                alpha=0.5,
            )

        # Plot the data
        plt.title("Coverage of All Buoys vs Buoys with Meteorological Data")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend()
        plt.grid(True)
        plt.show()

    async def close(self):
        await self.fetcher.close()
        self.cursor.close()
        self.conn.close()


async def main():
    load_dotenv()
    analysis = CoverageAnalysis()

    await analysis.fetch_stations()  # Fetch the station data (from DB and XML)
    analysis.plot_coverage()  # Plot the coverage
    await analysis.close()  # Close the sessions


if __name__ == "__main__":
    asyncio.run(main())

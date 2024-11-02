import aiohttp
import asyncio
from backend.create_database import create_database
from backend.stations.stations import StationsFetcher
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import os
import psycopg2

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"


class LocalizedWaveDataFetcher:
    def __init__(self, station_ids):
        """Initialize with a list of station IDs."""
        self.station_ids = station_ids
        self.conn = psycopg2.connect(
            dbname="localized_wave_data",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

    def setup_localized_data_table(self):
        """Create the localized_wave_data table with latitude and longitude."""
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS localized_wave_data (
                    station_id VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    latitude FLOAT,
                    longitude FLOAT,
                    WVHT FLOAT,
                    DPD FLOAT,
                    APD FLOAT,
                    MWD FLOAT,
                    PRIMARY KEY (station_id, datetime)
                );
                """
            )
            self.conn.commit()
            print("Table 'localized_wave_data' checked/created successfully.")
        except Exception as e:
            print(f"Error setting up the localized data table: {e}")
            self.conn.rollback()

    async def fetch_station_wave_data(self):
        """Fetch wave data for each station and insert localized data."""
        async with aiohttp.ClientSession() as session:
            for station_id in self.station_ids:
                print(f"Fetching wave data for station: {station_id}")
                data, lat, lon = await self.get_station_data(session, station_id)
                if data is not None and not data.empty:  # Check if data is valid
                    await self.insert_localized_wave_data_into_db(
                        station_id, data, lat, lon
                    )

    async def get_station_data(self, session, station_id):
        """Get wave observation data from NOAA, including potential latitude and longitude."""
        file_variants = [
            f"{station_id}.spec",
            f"{station_id}.txt",
            f"{station_id}.drift",
        ]
        all_dfs = []
        lat, lon = None, None

        for file_name in file_variants:
            request_url = f"{OBSERVATION_BASE_URL}{file_name}"
            try:
                async with session.get(request_url) as resp:
                    if resp.status == 200:
                        response = await resp.text()
                        # Determine columns and extract latitude/longitude if it's a drift file
                        if file_name.endswith(".drift"):
                            columns = [
                                "Year",
                                "Month",
                                "Day",
                                "HourMinute",
                                "LAT",
                                "LON",
                                "WDIR",
                                "WSPD",
                                "GST",
                                "PRES",
                                "ATMP",
                                "WTMP",
                                "DEWP",
                                "WVHT",
                                "DPD",
                            ]
                            lat, lon = self.extract_coordinates_from_drift(response)
                        else:
                            columns = (
                                [
                                    "Year",
                                    "Month",
                                    "Day",
                                    "Hour",
                                    "Minute",
                                    "WVHT",
                                    "DPD",
                                    "APD",
                                    "MWD",
                                ]
                                if file_name.endswith(".spec")
                                or file_name.endswith(".txt")
                                else None
                            )
                        if columns:
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,
                                sep=r"\s+",
                                names=columns,
                                na_values=["MM"],
                            )
                            df["DateTime"] = pd.to_datetime(
                                df[["Year", "Month", "Day", "Hour", "Minute"]],
                                errors="coerce",
                            )
                            all_dfs.append(df)
            except Exception as e:
                print(f"Failed to fetch data for {file_name}: {e}")
                continue

        if all_dfs:
            concatenated_df = pd.concat(all_dfs)
            wave_columns = ["DateTime", "WVHT", "DPD", "APD", "MWD"]
            aggregated_df = (
                concatenated_df[wave_columns].groupby("DateTime").first().reset_index()
            )
            aggregated_df = aggregated_df.dropna(subset=["DateTime"])

            if not aggregated_df.empty:
                return aggregated_df, lat, lon
        return None, lat, lon

    def extract_coordinates_from_drift(self, response_text):
        """Extract latitude and longitude from the drift file if available."""
        df = pd.read_csv(
            StringIO(response_text),
            skiprows=2,
            sep=r"\s+",
            usecols=["LAT", "LON"],
            na_values=["MM"],
        )
        lat = (
            df["LAT"].iloc[0]
            if "LAT" in df.columns and not df["LAT"].isnull().all()
            else None
        )
        lon = (
            df["LON"].iloc[0]
            if "LON" in df.columns and not df["LON"].isnull().all()
            else None
        )
        return lat, lon

    async def insert_localized_wave_data_into_db(self, station_id, data, lat, lon):
        """Insert localized wave data with station coordinates into the database."""
        try:
            # If lat/lon are still None, fallback to stations table data
            if not lat or not lon:
                self.cursor.execute(
                    "SELECT latitude, longitude FROM stations WHERE station_id = %s",
                    (station_id,),
                )
                result = self.cursor.fetchone()
                if result:
                    lat, lon = result

            for _, row in data.iterrows():
                self.cursor.execute(
                    """
                    INSERT INTO localized_wave_data (station_id, datetime, latitude, longitude, WVHT, DPD, APD, MWD)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, datetime) DO NOTHING;
                    """,
                    (
                        station_id,
                        row["DateTime"],
                        lat,
                        lon,
                        row.get("WVHT"),
                        row.get("DPD"),
                        row.get("APD"),
                        row.get("MWD"),
                    ),
                )
            self.conn.commit()
            print(f"Localized wave data inserted for station {station_id}")
        except Exception as e:
            print(f"Error inserting localized wave data for station {station_id}: {e}")
            self.conn.rollback()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()


async def main():
    load_dotenv()
    create_database("localized_wave_data")

    stations = StationsFetcher()
    station_id_list = stations.fetch_station_ids()

    fetcher = LocalizedWaveDataFetcher(station_id_list)
    fetcher.setup_localized_data_table()
    try:
        await fetcher.fetch_station_wave_data()
    finally:
        fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())

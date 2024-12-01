import aiohttp
import asyncio
from backend.config.database import localized_wave_engine
from backend.create_database import create_database
from backend.stations.stations import StationsFetcher
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
from sqlalchemy import text
import logging

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

logger = logging.getLogger(__name__)


class LocalizedWaveProcessor:
    def __init__(self, station_ids=None):
        self.engine = localized_wave_engine
        self.station_ids = station_ids or []

    def create_wave_table(self):
        """
        Create the localized wave data table if it doesn't exist.
        """
        create_database("localized_wave_data")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS localized_wave_data (
            id SERIAL PRIMARY KEY,
            station_id VARCHAR(10),
            datetime TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            wvht FLOAT,
            dpd FLOAT,
            apd FLOAT,
            mwd FLOAT,
            UNIQUE(station_id, datetime)
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()

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
                with self.engine.connect() as conn:
                    result = conn.execute(
                        text(
                            "SELECT latitude, longitude FROM stations WHERE station_id = :station_id"
                        ),
                        {"station_id": station_id},
                    ).fetchone()
                    if result:
                        lat, lon = result

            with self.engine.connect() as conn:
                for _, row in data.iterrows():
                    conn.execute(
                        text(
                            """
                        INSERT INTO localized_wave_data (station_id, datetime, latitude, longitude, WVHT, DPD, APD, MWD)
                        VALUES (:station_id, :datetime, :lat, :lon, :wvht, :dpd, :apd, :mwd)
                        ON CONFLICT (station_id, datetime) DO NOTHING;
                        """
                        ),
                        {
                            "station_id": station_id,
                            "datetime": row["DateTime"],
                            "lat": lat,
                            "lon": lon,
                            "wvht": row.get("WVHT"),
                            "dpd": row.get("DPD"),
                            "apd": row.get("APD"),
                            "mwd": row.get("MWD"),
                        },
                    )
                conn.commit()
            print(f"Localized wave data inserted for station {station_id}")
        except Exception as e:
            print(f"Error inserting localized wave data for station {station_id}: {e}")

    def close(self):
        """Close the database connection."""
        self.engine.connect().close()

    def process_data(self):
        try:
            logger.info("Starting wave data processing")
            stations = StationsFetcher()
            station_id_list = stations.fetch_station_ids()

            fetcher = LocalizedWaveProcessor(station_id_list)
            fetcher.create_wave_table()
            try:
                fetcher.fetch_station_wave_data()
            finally:
                fetcher.close()
            logger.info("Wave data processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            raise


async def main():
    load_dotenv()
    create_database("localized_wave_data")

    stations = StationsFetcher()
    station_id_list = stations.fetch_station_ids()

    fetcher = LocalizedWaveProcessor(station_id_list)
    fetcher.create_wave_table()
    try:
        await fetcher.fetch_station_wave_data()
    finally:
        fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())

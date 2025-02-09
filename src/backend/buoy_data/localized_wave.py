"""
Module for processing and storing NOAA buoy wave data.

This module provides functionality to fetch, process, and store wave measurement data
from NOAA buoy stations. It handles multiple data formats and includes support for
localized wave measurements with geographical coordinates.
"""

import logging
import asyncio
from io import StringIO
import aiohttp
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Table,
    MetaData,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from src.backend.config.database import wave_analytics_engine
from src.backend.stations.stations import StationsFetcher
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import insert

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define valid file types as a set for O(1) lookup
VALID_FILE_TYPES = {".drift", ".txt", ".spec"}


class LocalizedWaveProcessor:
    """
    A class to process and store localized wave data from NOAA stations.

    This class handles fetching wave observation data from NOAA stations, processes it,
    and stores it in a database. It supports handling multiple data file formats
    including .spec, .txt, and .drift files.

    Attributes:
        engine: SQLAlchemy engine instance for database operations
        station_ids (list): List of NOAA station IDs to process
        metadata (MetaData): SQLAlchemy metadata object
        localized_wave_table (Table): SQLAlchemy table definition for wave data
    """

    def __init__(self, station_ids=None):
        """
        Initialize the LocalizedWaveProcessor.

        Args:
            station_ids (list, optional): List of station IDs to process. Defaults to empty list.
        """
        self.engine = wave_analytics_engine
        self.station_ids = station_ids or []
        self.metadata = MetaData(schema="raw_data")

        # Define tables
        self.localized_wave_table = Table(
            "localized_wave_data", self.metadata, autoload_with=self.engine
        )

        # Create sessionmaker
        self.session_local = sessionmaker(bind=self.engine)

    async def fetch_stations_data(self):
        """
        Fetch and process wave data for all configured stations.

        Iterates through each station ID, fetches its wave data, and inserts it into
        the database. Handles each station independently and logs the process.

        Returns:
            None
        """
        async with aiohttp.ClientSession() as session:
            for station_id in self.station_ids:
                logger.info("Fetching wave data for station: %s", station_id)
                data, lat, lon = await self._fetch_station_data(session, station_id)
                if data is not None and not data.empty:  # Check if data is valid
                    await self._insert_localized_wave_data_into_db(
                        station_id, data, lat, lon
                    )

    async def _fetch_station_data(self, session, station_id):
        """
        Fetch wave observation data for a specific station.
        Only processes .drift, .txt, and .spec files.
        """
        file_variants = [f"{station_id}{ext}" for ext in VALID_FILE_TYPES]
        all_dfs = []
        lat, lon = None, None

        for file_name in file_variants:
            request_url = f"{OBSERVATION_BASE_URL}{file_name}"
            try:
                async with session.get(request_url) as resp:
                    logger.info(
                        "Trying URL: %s with status %s", request_url, resp.status
                    )
                    if resp.status == 200:
                        response = await resp.text()

                        if file_name.endswith(".drift"):
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,  # Skip header and units rows
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=[
                                    "YY",
                                    "MM",
                                    "DD",
                                    "hhmm",
                                    "LAT",
                                    "LON",
                                    "WDIR",
                                    "WSPD",
                                    "GST",
                                    "PRES",
                                    "PTDY",
                                    "ATMP",
                                    "WTMP",
                                    "DEWP",
                                    "WVHT",
                                    "DPD",
                                ],
                            )

                            # Rename columns to match expected format
                            df = df.rename(
                                columns={
                                    "YY": "year",
                                    "MM": "month",
                                    "DD": "day",
                                }
                            )

                            # Extract hour and minute from hhmm
                            df["hour"] = df["hhmm"].astype(str).str[:2].astype(int)
                            df["minute"] = df["hhmm"].astype(str).str[2:].astype(int)

                            lat = (
                                df["LAT"].iloc[0]
                                if not df["LAT"].isnull().all()
                                else None
                            )
                            lon = (
                                df["LON"].iloc[0]
                                if not df["LON"].isnull().all()
                                else None
                            )

                        elif file_name.endswith(".spec"):
                            # Handle spec files
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,  # Skip header and units rows
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=[
                                    "YY",
                                    "MM",
                                    "DD",
                                    "hh",
                                    "mm",
                                    "WVHT",
                                    "SwH",
                                    "SwP",
                                    "WWH",
                                    "WWP",
                                    "SwD",
                                    "WWD",
                                    "STEEPNESS",
                                    "APD",
                                    "MWD",
                                ],
                            )
                            df = df.rename(
                                columns={
                                    "YY": "year",
                                    "MM": "month",
                                    "DD": "day",
                                    "hh": "hour",
                                    "mm": "minute",
                                }
                            )

                        elif file_name.endswith(".txt"):
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,  # Skip header and units rows
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=[
                                    "YY",
                                    "MM",
                                    "DD",
                                    "hh",
                                    "mm",
                                    "WDIR",
                                    "WSPD",
                                    "GST",
                                    "WVHT",
                                    "DPD",
                                    "APD",
                                    "MWD",
                                    "PRES",
                                    "ATMP",
                                    "WTMP",
                                    "DEWP",
                                    "VIS",
                                    "PTDY",
                                    "TIDE",
                                ],
                            )
                            df = df.rename(
                                columns={
                                    "YY": "year",
                                    "MM": "month",
                                    "DD": "day",
                                    "hh": "hour",
                                    "mm": "minute",
                                }
                            )

                        # Create datetime column
                        df["datetime"] = pd.to_datetime(
                            df[["year", "month", "day", "hour", "minute"]]
                        )

                        all_dfs.append(df)

            except (
                aiohttp.ClientError,
                pd.errors.EmptyDataError,
                ValueError,
                UnicodeDecodeError,
            ) as e:
                logger.warning("Failed to fetch data for %s, error: %s", file_name, e)
                continue

        if all_dfs:
            concatenated_df = pd.concat(all_dfs)
            wave_columns = ["datetime", "WVHT", "DPD", "APD", "MWD"]
            aggregated_df = (
                concatenated_df[wave_columns].groupby("datetime").first().reset_index()
            )
            return aggregated_df, lat, lon

        return None, lat, lon

    async def _insert_localized_wave_data_into_db(self, station_id, data, lat, lon):
        """Insert processed wave data into the database."""
        if lat is None or lon is None:
            logger.warning("Lat/Lon missing for %s, querying database...", station_id)
            with Session(self.engine) as session:
                result = (
                    session.query(
                        self.localized_wave_table.c.latitude,
                        self.localized_wave_table.c.longitude,
                    )
                    .filter(self.localized_wave_table.c.station_id == station_id)
                    .first()
                )

                if result:
                    lat, lon = result.latitude, result.longitude
                    logger.info(
                        "Fetched lat/lon for station %s: %s, %s", station_id, lat, lon
                    )

        logger.info(
            "Inserting data for station %s: lat=%s, lon=%s", station_id, lat, lon
        )

        # Process data in batches
        BATCH_SIZE = 1000
        total_rows = len(data)
        processed_rows = 0

        with Session(self.engine) as session:
            try:
                for start_idx in range(0, total_rows, BATCH_SIZE):
                    end_idx = min(start_idx + BATCH_SIZE, total_rows)
                    batch = data.iloc[start_idx:end_idx]

                    rows = [
                        {
                            "station_id": station_id,
                            "datetime": row["datetime"],
                            "latitude": lat,
                            "longitude": lon,
                            "wave_height(wvht)": row.get("WVHT"),
                            "dominant_period(dpd)": row.get("DPD"),
                            "average_period(apd)": row.get("APD"),
                            "mean_wave_direction(mwd)": row.get("MWD"),
                        }
                        for _, row in batch.iterrows()
                    ]

                    stmt = insert(self.localized_wave_table).values(rows)
                    session.execute(stmt)
                    session.commit()

                    processed_rows += len(batch)
                    logger.info(
                        "Processed %d/%d rows for station %s",
                        processed_rows,
                        total_rows,
                        station_id,
                    )

                logger.info("Data insertion completed for station %s", station_id)

            except IntegrityError as e:
                session.rollback()
                logger.warning(
                    "Duplicate entries for station %s ignored: %s", station_id, e
                )
            except SQLAlchemyError as e:
                session.rollback()
                logger.error("Error inserting data for station %s: %s", station_id, e)

    async def process_data(self):
        """
        Main processing method to handle all wave data collection and storage.

        Coordinates the entire data processing workflow, including:
        - Fetching station IDs
        - Collecting wave data for each station
        - Storing the data in the database

        Raises:
            aiohttp.ClientError: If there are network-related issues
            SQLAlchemyError: If there are database-related issues
            ValueError: If there are data processing issues
        """
        try:
            logger.info("Starting wave data processing")
            stations = StationsFetcher()
            station_id_list = stations.fetch_station_ids()

            fetcher = LocalizedWaveProcessor(station_id_list)
            try:
                await fetcher.fetch_stations_data()
            finally:
                fetcher.close()
            logger.info("Wave data processing completed successfully")
        except (aiohttp.ClientError, SQLAlchemyError, ValueError) as e:
            logger.error("Error processing wave data: %s", str(e))
            raise

    def close(self):
        """Close the database connection."""
        self.engine.dispose()


async def main():
    load_dotenv()

    stations = StationsFetcher()
    station_id_list = stations.fetch_station_ids()

    fetcher = LocalizedWaveProcessor(station_id_list)
    try:
        await fetcher.fetch_stations_data()
    finally:
        fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())

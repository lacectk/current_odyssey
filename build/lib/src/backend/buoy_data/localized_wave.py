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
from dataclasses import dataclass
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import insert
from src.backend.config.database import wave_analytics_engine
from src.backend.stations.stations import StationsFetcher

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define valid file types as a set for O(1) lookup
VALID_FILE_TYPES = {".drift", ".txt", ".spec"}


@dataclass
class WaveProcessorConfig:
    """Configuration settings for wave data processing.

    Attributes:
        batch_size (int): Number of records to process in each batch
        pool_size (int): Maximum database connections in the pool
        timeout (int): Request timeout in seconds
        retry_attempts (int): Number of retry attempts for failed operations
        base_url (str): Base URL for NDBC data
    """

    batch_size: int = 1000
    pool_size: int = 5
    timeout: int = 30
    retry_attempts: int = 3
    base_url: str = "https://www.ndbc.noaa.gov/data/realtime2/"


def process_single_df(df):
    """Process a single dataframe of wave data."""
    wave_columns = ["datetime", "WVHT", "DPD", "APD", "MWD"]
    processed_df = df[wave_columns].copy()
    processed_df = processed_df.groupby("datetime").first().reset_index()
    return processed_df


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

    def __init__(self, station_ids=None, pool_size=5):
        """
        Initialize the LocalizedWaveProcessor with connection pooling configuration.

        Args:
            station_ids (list, optional): List of station IDs to process. Defaults to empty list.
            pool_size (int, optional): Maximum number of connections in the pool. Defaults to 5.
        """
        self.station_ids = station_ids or []
        self.metadata = MetaData(schema="raw_data")

        # Configure connection pooling
        self.engine = wave_analytics_engine.execution_options(
            pool_size=pool_size,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
        )

        # Define tables
        self.localized_wave_table = Table(
            "localized_wave_data", self.metadata, autoload_with=self.engine
        )

        self.session_local = sessionmaker(bind=self.engine)
        self._http_session = None

    async def __aenter__(self):
        """Async context manager entry."""
        self._http_session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""
        if self._http_session:
            await self._http_session.close()
        if self.engine:
            self.engine.dispose()

    async def fetch_stations_data(self):
        """Fetch and process wave data for all configured stations."""
        async with aiohttp.ClientSession() as session:
            # Use asyncio.gather to handle multiple async tasks
            tasks = [
                self._fetch_station_data(session, station_id)
                for station_id in self.station_ids
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for station_id, result in zip(self.station_ids, results):
                if isinstance(result, Exception):
                    logger.error("Error processing station %s: %s", station_id, result)
                    continue

                data, lat, lon = result
                if data is not None:
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

            except Exception as e:
                logger.warning(
                    "Failed to fetch data for %s, error: %s", file_name, str(e)
                )
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

        logger.info(
            "Inserting data for station %s: lat=%s, lon=%s", station_id, lat, lon
        )

        rows = []
        for _, row in data.iterrows():
            rows.append(
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
            )

        if not rows:
            logger.warning("No data to insert for station %s", station_id)
            return

        with Session(self.engine) as session:
            try:
                # Insert the data
                stmt = insert(self.localized_wave_table).values(rows)
                session.execute(stmt)
                session.commit()
                logger.info(
                    "Successfully inserted %d rows for station %s",
                    len(rows),
                    station_id,
                )
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

            async with LocalizedWaveProcessor(station_id_list) as processor:
                await processor.fetch_stations_data()

            logger.info("Wave data processing completed successfully")
        except (aiohttp.ClientError, SQLAlchemyError, ValueError) as e:
            logger.error("Error processing wave data: %s", str(e))
            raise

    async def close(self):
        """Close all connections."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        if self.engine:
            self.engine.dispose()


async def main():
    load_dotenv()

    stations = StationsFetcher()
    station_id_list = stations.fetch_station_ids()

    async with LocalizedWaveProcessor(station_id_list) as processor:
        await processor.fetch_stations_data()


if __name__ == "__main__":
    asyncio.run(main())

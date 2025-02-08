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
    select,
    Table,
    MetaData,
    Column,
    String,
    Float,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from src.backend.config.database import wave_analytics_engine
from src.backend.stations.stations import StationsFetcher

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        self.metadata = MetaData()

        self.localized_wave_table = Table(
            "localized_wave_data",
            self.metadata,
            Column("station_id", String(10), primary_key=True),
            Column("datetime", DateTime, nullable=False),
            Column("latitude", Float, nullable=False),
            Column("longitude", Float, nullable=False),
            Column("wave_height(wvht)", Float),
            Column("dominant_period(dpd)", Float),
            Column("average_period(apd)", Float),
            Column("mean_wave_direction(mwd)", Float),
            UniqueConstraint("station_id", "datetime", name="unix_station_datetime"),
            schema="raw_data",
        )

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
        Fetch wave observation data for a specific station from NOAA.

        Attempts to fetch data from multiple file formats (.spec, .txt, .drift)
        and combines the results. Also extracts station coordinates when available.

        Args:
            session (aiohttp.ClientSession): Active HTTP session for making requests
            station_id (str): NOAA station identifier

        Returns:
            tuple: (DataFrame of wave data, latitude, longitude)
                  Returns (None, None, None) if no data is available
        """
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
                    logger.info(
                        "Trying URL: %s with status %s", request_url, resp.status
                    )
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
                            lat, lon = self._extract_coordinates_from_drift(response)
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=columns,
                            )
                            df["hour"] = (
                                df["HourMinute"].astype(str).str.slice(0, 2).astype(int)
                            )
                            df["minute"] = (
                                df["HourMinute"].astype(str).str.slice(2, 4).astype(int)
                            )

                        elif file_name.endswith(".txt"):
                            columns = [
                                "Year",
                                "Month",
                                "Day",
                                "Hour",
                                "Minute",
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
                            ]
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,  # Skip header and units rows
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=columns,
                            )

                        elif file_name.endswith(".spec"):
                            columns = [
                                "Year",
                                "Month",
                                "Day",
                                "Hour",
                                "Minute",
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
                            ]
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,  # Skip header and units rows
                                sep=r"\s+",
                                comment="#",
                                na_values=["MM"],
                                names=columns,
                            )

                        if columns is not None:
                            df["datetime"] = pd.to_datetime(
                                df[["year", "month", "day", "hour", "minute"]]
                            )
                        if columns is None:
                            logger.warning(
                                "No valid columns matched for this file: %s", file_name
                            )
                            continue
                        all_dfs.append(df)
            except (aiohttp.ClientError, pd.errors.EmptyDataError, ValueError) as e:
                logger.warning("Failed to fetch data for %s, error: %s", file_name, e)
                continue

        if all_dfs:
            concatenated_df = pd.concat(all_dfs)
            wave_columns = ["datetime", "wvht", "dpt", "apd", "mwd"]
            aggregated_df = (
                concatenated_df[wave_columns].groupby("datetime").first().reset_index()
            )
            aggregated_df = aggregated_df.dropna(subset=["datetime"])

            if not aggregated_df.empty:
                return aggregated_df, lat, lon
        return None, lat, lon

    def _extract_coordinates_from_drift(self, response_text):
        """
        Extract latitude and longitude coordinates from a drift file response.

        Args:
            response_text (str): Raw text content of the drift file

        Returns:
            tuple: (latitude, longitude) as floats, or (None, None) if not found
        """
        df = pd.read_csv(
            StringIO(response_text),
            skiprows=2,
            sep=r"\s+",
            comment="#",
            na_values=["MM"],
            names=[
                "year",
                "month",
                "day",
                "hrmn",
                "lat",
                "lon",
                "wdir",
                "wspd",
                "gst",
                "pres",
                "ptdy",
                "atmp",
                "wtmp",
                "dewp",
                "wvht",
                "dpd",
            ],
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

    async def _insert_localized_wave_data_into_db(self, station_id, data, lat, lon):
        """
        Insert processed wave data into the database.

        If coordinates are not provided, attempts to fetch them from the stations table.
        Handles duplicate entries gracefully.

        Args:
            station_id (str): NOAA station identifier
            data (DataFrame): Processed wave data
            lat (float): Station latitude
            lon (float): Station longitude

        Returns:
            None
        """

        if lat is None or lon is None:
            logger.warn("Lat/Lon missing for %s, querying database...", station_id)
            with self.engine.connect() as conn:
                result = conn.execute(
                    select(
                        self.localized_wave_table.c.latitude,
                        self.localized_wave_table.c.longitude,
                    ).where(self.localized_wave_table.c.station_id == station_id)
                ).fetchone()

                logger.info(
                    "Database fetch result for station %s: %s", station_id, result
                )
                if result:
                    lat, lon = result["latitude"], result["longitude"]
                    logger.info(
                        "Fetched lat/lon for station %s: %s, %s", station_id, lat, lon
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
                    "wave_height(wvht)": row.get("wvht"),
                    "dominant_period(dpd)": row.get("dpd"),
                    "average_period(apd)": row.get("apd"),
                    "mean_wave_direction(mwd)": row.get("mwd"),
                }
            )

        with self.engine.begin() as conn:
            try:
                conn.execute(self.localized_wave_table.insert().values(rows))
                logger.info("Data inserted for station %s", station_id)
            except IntegrityError as e:
                logger.warning(
                    "Duplicate entries for station %s ignored: %s", station_id, e
                )

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
        """
        Close the database connection.

        Should be called when done with the processor to clean up resources.
        """
        if hasattr(self, "_connection"):
            self._connection.close()


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

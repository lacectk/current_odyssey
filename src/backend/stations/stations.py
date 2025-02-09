"""
NOAA Station Management Module.

This module handles the fetching, processing, and storage of NOAA meteorological station data.
It provides functionality to retrieve station information from NDBC (National Data Buoy Center)
and maintain a local database of station coordinates and metadata.
"""

import asyncio
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import (
    MetaData,
    Table,
    insert,
)
from sqlalchemy.orm import Session, sessionmaker
from src.backend.config.database import wave_analytics_engine
from src.backend.stations.ndbc_stations_data import NDBCDataFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Station:
    """
    Represents a meteorological station with its location coordinates.

    Attributes:
        station_id (str): Unique identifier for the station
        latitude (float): Station's latitude coordinate
        longitude (float): Station's longitude coordinate
    """

    def __init__(self, station_id, latitude, longitude):
        self.station_id = station_id
        self.latitude = latitude
        self.longitude = longitude


class StationsFetcher:
    """
    Handles fetching and storing meteorological station data.

    This class manages the retrieval of station data from NDBC and its storage
    in the PostgreSQL database. It handles both the initial fetch and subsequent
    database operations.

    Attributes:
        fetcher (NDBCDataFetcher): Instance for fetching NDBC data
        engine: SQLAlchemy database engine
        metadata (MetaData): SQLAlchemy metadata for table definitions
        stations_table (Table): SQLAlchemy table definition for stations
        SessionLocal (sessionmaker): SQLAlchemy sessionmaker for database operations
    """

    def __init__(self):
        self.fetcher = NDBCDataFetcher()
        self.engine = wave_analytics_engine
        self.metadata = MetaData(schema="raw_data")
        self.stations_table = Table(
            "stations", self.metadata, autoload_with=self.engine
        )
        # Create sessionmaker
        self.SessionLocal = sessionmaker(bind=self.engine)

    async def meteorological_stations(self):
        """
        Fetch meteorological station data from NDBC.

        Returns:
            dict: Dictionary of Station objects keyed by station_id
                 Only includes stations with meteorological capabilities
        """
        try:
            stations_data = await self.fetcher.fetch_station_data()
            stations_list = {}

            for station in stations_data["stations"]["station"]:
                if station.get("@met") == "y":
                    station_id = station["@id"]
                    latitude = float(station["@lat"])
                    longitude = float(station["@lon"])

                    station_obj = Station(station_id, latitude, longitude)
                    stations_list[station_id] = station_obj

            return stations_list
        finally:
            await self.fetcher.close()

    def check_table_exists(self) -> bool:
        """
        Check if the stations table exists in the raw_data schema.

        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                return self.engine.dialect.has_table(
                    conn, "stations", schema="raw_data"
                )
        except SQLAlchemyError as e:
            logger.error("Error checking table existence: %s", e)
            return False

    def fetch_station_ids(self):
        """
        Fetch all station IDs from the database using Session query.

        Returns:
            list: List of station IDs
        """
        try:
            with Session(self.engine) as session:
                result = (
                    session.query(self.stations_table.c.station_id).distinct().all()
                )
                return [r[0] for r in result]
        except SQLAlchemyError as e:
            logger.error("Error fetching station IDs: %s", str(e))
            return []

    def insert_into_database(self, station_list):
        """
        Insert or update station data in the database using Session.

        Args:
            station_list (dict): Dictionary of Station objects to insert/update
        """
        with Session(self.engine) as session:
            for station_id, station in station_list.items():
                try:
                    stmt = (
                        insert(self.stations_table)
                        .values(
                            station_id=station.station_id,
                            latitude=station.latitude,
                            longitude=station.longitude,
                        )
                        .on_conflict_do_nothing(index_elements=["station_id"])
                    )

                    result = session.execute(stmt)

                    if result.rowcount > 0:
                        logger.info("Inserted new station %s", station_id)

                except SQLAlchemyError as e:
                    logger.error(
                        "Error inserting data for station %s: %s", station_id, e
                    )
                    continue

            session.commit()

    async def close(self):
        """Clean up resources and close connections."""
        if hasattr(self, "fetcher"):
            await self.fetcher.close()


async def main():
    """
    Main entry point for station data processing.

    Fetches station data and stores it in the database.
    """
    try:
        # Create an instance of the Stations class and fetch the data
        stations = StationsFetcher()
        station_list = await stations.meteorological_stations()

        # Fetch and insert the station data into the PostgreSQL database
        stations.insert_into_database(station_list)
    finally:
        # Close the session
        await stations.close()


if __name__ == "__main__":
    asyncio.run(main())

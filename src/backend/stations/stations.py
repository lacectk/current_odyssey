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
    create_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool
from src.backend.config.database import wave_analytics_engine
from src.backend.stations.ndbc_stations_data import NDBCDataFetcher
from src.backend.models import StationModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        session_local (sessionmaker): SQLAlchemy sessionmaker for database operations
    """

    def __init__(self):
        self.fetcher = NDBCDataFetcher()
        self.engine = create_engine(
            wave_analytics_engine.url,
            poolclass=QueuePool,
            isolation_level="REPEATABLE READ",
        )
        self.metadata = MetaData(schema="raw_data")
        self.stations_table = Table(
            "stations", self.metadata, autoload_with=self.engine
        )
        self.session_local = sessionmaker(bind=self.engine)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""
        await self.close()

    async def meteorological_stations(self):
        """
        Fetch meteorological station data from NDBC.

        Returns:
            dict: Dictionary of StationModel objects keyed by station_id
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

                    station_obj = StationModel(
                        station_id=station_id, latitude=latitude, longitude=longitude
                    )
                    stations_list[station_id] = station_obj

            return stations_list
        except Exception as e:
            logger.error("Failed to fetch station data: %s", e)
            raise

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
        """Fetch all station IDs using StationModel."""
        try:
            with Session(self.engine) as session:
                result = session.query(StationModel.station_id).distinct().all()
                return [r[0] for r in result]
        except SQLAlchemyError as e:
            logger.error("Error fetching station IDs: %s", str(e))
            return []

    def insert_into_database(self, station_list):
        """
        Insert or update station data in the database using Session.

        Args:
            station_list (dict): Dictionary of StationModel objects to insert/update
        """
        with Session(self.engine) as session:
            try:
                for _, station in station_list.items():
                    session.merge(station)
                session.commit()
            except SQLAlchemyError as e:
                session.rollback()
                logger.error("Error inserting stations: %s", str(e))
                raise

    async def close(self):
        """Clean up resources."""
        if hasattr(self, "fetcher"):
            await self.fetcher.close()
        if hasattr(self, "engine"):
            self.engine.dispose()


async def main():
    """
    Main entry point for station data processing.

    Fetches station data and stores it in the database.
    """
    try:
        async with StationsFetcher() as stations:
            station_list = await stations.meteorological_stations()
            stations.insert_into_database(station_list)
    except Exception as e:
        logger.error("Error in station processing: %s", e)
        raise


if __name__ == "__main__":
    asyncio.run(main())

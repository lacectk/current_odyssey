import asyncio
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, Table, Column, String, Float, DateTime, text
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
    """

    def __init__(self):
        self.fetcher = NDBCDataFetcher()
        self.engine = wave_analytics_engine
        self.metadata = MetaData()

        self.stations_table = Table(
            "stations",
            self.metadata,
            Column("station_id", String(10), primary_key=True),
            Column("latitude", Float, nullable=False),
            Column("longitude", Float, nullable=False),
            Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
            Column("updated_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
        )

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

    async def insert_into_database(self, station_list):
        """
        Insert or update station data in the database.

        Args:
            station_list (dict): Dictionary of Station objects to insert/update

        Note:
            Uses ON CONFLICT DO NOTHING to handle duplicate stations
        """
        with self.engine.connect() as conn:
            for station_id, station in station_list.items():
                try:
                    result = conn.execute(
                        self.stations_table.insert().values(
                            station_id=station.station_id,
                            latitude=station.latitude,
                            longitude=station.longitude,
                        )
                    ).prefix_with("ON CONFLICT (station_id) DO NOTHING")
                    if result.rowcount > 0:
                        logger.info("Inserted new station %s", station_id)

                except SQLAlchemyError as e:
                    logger.error(
                        "Error inserting data for station %d, %s", station_id, e
                    )
                    continue

            conn.commit()

    def fetch_station_ids(self):
        """
        Fetch all station IDs from the database.

        Returns:
            list: List of station IDs stored in the database
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.stations_table.select().with_only_columns(
                        [self.stations_table.c.station_id]
                    )
                )
                station_ids = []
                for row in result:
                    station_id = row[0]
                    station_ids.append(station_id)
                return station_ids
        except SQLAlchemyError as e:
            logger.error("Error fetching station IDs: %s", e)
            return []

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
        await stations.insert_into_database(station_list)
    finally:
        # Close the session
        await stations.close()


if __name__ == "__main__":
    asyncio.run(main())

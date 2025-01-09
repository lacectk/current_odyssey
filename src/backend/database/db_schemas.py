import os
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer
from dotenv import load_dotenv


class SchemaManager:
    def __init__(self, database_name="wave_analytics"):
        load_dotenv()

        self.engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{database_name}"
        )
        self.metadata = MetaData()

    def create_schemas(self):
        """Create database schemas."""
        schemas = ["stations"]

        for schema in schemas:
            self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_stations_table(self):
        """Create the stations table."""
        Table(
            "stations",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("latitude", Float),
            Column("longitude", Float),
            schema="raw_data",
        )

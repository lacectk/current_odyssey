from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()


def create_database(database_name):
    """
    Create a PostgresSQL database if it does not already exist.
    """
    postgres_engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/postgres"
    )

    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_catalog.pg_database WHERE datname = :dbname"),
            {"dbname": database_name},
        )
        if not result.scalar():
            conn.execute(text(f"CREATE DATABASE {database_name}"))
            print(f"Database '{database_name}' created.")
        else:
            print(f"Database '{database_name}' already exists.")

"""
Database setup and migration module.

Handles the creation of the wave_analytics database and runs all necessary
Alembic migrations to set up the schema and tables.
"""

import logging
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from alembic.config import Config
from alembic import command
from alembic.util.exc import CommandError

logger = logging.getLogger(__name__)

load_dotenv()


def setup_database():
    """Set up database and run migrations."""
    try:
        # Create database if it doesn't exist
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/postgres",
            isolation_level="AUTOCOMMIT",
        )

        with engine.connect() as conn:
            # Check if database exists
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = 'wave_analytics'")
            ).fetchone()

            if not result:
                conn.execute(text("CREATE DATABASE wave_analytics"))
                logger.info("Created wave_analytics database")

        # Run migrations
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")

        logger.info("Database setup completed successfully")
        return True
    except SQLAlchemyError as e:
        logger.error("Database creation failed: %s", e)
        return False
    except CommandError as e:
        logger.error("Migration failed: %s", e)
        return False


if __name__ == "__main__":
    setup_database()

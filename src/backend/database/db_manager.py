import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.backend.database.db_config import get_admin_engine, get_wave_analytics_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        load_dotenv()

        self.admin_engine = get_admin_engine()
        self.db_engine = get_wave_analytics_engine()
        self.protected_dbs = {"postgres", "template0", "template1", "dagster"}

    def list_databases(self):
        """List all databases in the PostgreSQL instance."""
        query = text(
            "SELECT datname FROM pg_database WHERE datname NOT IN :protected_dbs"
        )
        with self.db_engine.connect() as conn:
            results = conn.execute(
                query, {"protected_dbs": tuple(sorted(self.protected_dbs))}
            )
            databases = [row[0] for row in results]

        logger.info("Databases in the instance: %s", databases)
        return databases

    def terminate_connections(self, db_name: str):
        """Force terminate all connections to a specific database."""
        query = text(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = :db_name
            AND pid <> pg_backend_pid()
        """
        )
        with self.db_engine.connect() as conn:
            conn.execute(query, {"db_name": db_name})
        logger.info("Terminated all connections to %s", db_name)

    def drop_database(self, db_name: str):
        """Drop a specific database if it exists."""
        if db_name in self.protected_dbs:
            logger.warning("Database %s is protected and cannot be dropped", db_name)
            return False

        try:
            self.terminate_connections(db_name)

            with self.db_engine.connect() as conn:
                conn.execute(text("COMMIT"))
                conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
                conn.commit()
            logger.info("Successfully dropped database %s", db_name)
            return True
        except Exception as e:
            logger.error("Failed to drop database %s: %s", db_name, e)
            return False

    def create_database(self, db_name: str):
        """Create a new database."""
        try:
            with self.admin_engine.connect() as conn:
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {db_name}"))
            logger.info("Successfully created database %s", db_name)
            return True
        except Exception as e:
            logger.error("Failed to create database %s: %s", db_name, e)
            return False


def main():
    """Main function to run database management tasks."""
    manager = DatabaseManager()
    logger.info("Current databases:")
    manager.list_databases()
    manager.drop_database("my_pgdb")
    manager.drop_database("localized_wave_data")
    manager.list_databases()


if __name__ == "__main__":
    main()

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


def verify_migration():
    """Verify if migrations were successful."""
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    with engine.connect() as conn:
        # Check schemas
        schemas = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata")
        ).fetchall()
        print("Available schemas:", [s[0] for s in schemas])

        # Check alembic version
        version = conn.execute(
            text("SELECT version_num FROM alembic_version")
        ).fetchone()
        print("Current migration version:", version[0] if version else "No migrations")

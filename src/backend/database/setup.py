from src.backend.database.manage_db import DatabaseManager
from alembic.config import Config
from alembic import command
import logging

logger = logging.getLogger(__name__)


def setup_database():
    """Set up database and run migrations."""
    try:
        # Create database if it doesn't exist
        db_manager = DatabaseManager()
        db_manager.create_database("wave_analytics")

        # Run migrations
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")

        logger.info("Database setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False


if __name__ == "__main__":
    setup_database()

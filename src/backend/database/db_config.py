"""Database configuration and connection management."""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


def get_admin_engine():
    """
    Create engine for administrative tasks (database creation/deletion).
    Uses AUTOCOMMIT only for specific operations that require it.
    """
    load_dotenv()

    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/postgres",
        # Only use AUTOCOMMIT for specific admin operations. Required for CREATE/DROP DATABASE
        isolation_level="AUTOCOMMIT",
    )


def get_wave_analytics_engine():
    """
    Create engine for regular database operations.
    Uses normal transaction mode for safety.
    """
    load_dotenv()

    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/wave_analytics",
        # Default transaction mode for safety
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
    )

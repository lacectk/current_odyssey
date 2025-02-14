"""
Alembic environment configuration module.

This module configures the Alembic migration environment, including database connection,
logging setup, and migration execution modes (online/offline). It loads configuration
from environment variables and sets up the SQLAlchemy engine accordingly.
"""

import os
import logging
from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from dotenv import load_dotenv
from alembic import context
from src.backend.models import Base

# Load environment variables
load_dotenv()

config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")

# Set SQLAlchemy URL from environment variables
section = config.config_ini_section
config.set_section_option(section, "DB_USER", os.getenv("DB_USER"))
config.set_section_option(section, "DB_PASSWORD", os.getenv("DB_PASSWORD"))
config.set_section_option(section, "DB_HOST", os.getenv("DB_HOST"))
config.set_section_option(section, "DB_PORT", os.getenv("DB_PORT", "5432"))
config.set_section_option(section, "DB_NAME", os.getenv("DB_NAME", "wave_analytics"))

# Model's MetaData object for autogenerate support
target_metadata = Base.metadata


def include_object(object, name, type_, reflected, compare_to):
    """
    Filter objects to include in the migration.

    Args:
        object: The database object being processed
        name: The name of the object
        type_: The type of object (table, column, etc.)
        reflected: Whether the object was reflected from the database
        compare_to: The object being compared to, if applicable

    Returns:
        bool: Whether to include the object in the migration
    """
    # Skip certain tables or patterns if needed
    if type_ == "table":
        # Skip tables with prefix 'tmp_'
        if name.startswith("tmp_"):
            return False
    return True


def process_revision_directives(context, revision, directives):
    """
    Process revision directives to prevent empty migrations.

    Args:
        context: Migration context
        revision: Revision being processed
        directives: Migration directives
    """
    if config.cmd_opts.autogenerate:
        script = directives[0]
        if script.upgrade_ops.is_empty():
            logger.info("No changes detected; skipping migration creation")
            directives[:] = []
            return


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        process_revision_directives=process_revision_directives,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            process_revision_directives=process_revision_directives,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

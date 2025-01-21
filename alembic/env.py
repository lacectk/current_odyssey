import os
import logging
from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from dotenv import load_dotenv
from alembic import context

# Load environment variables
load_dotenv()

config = context.config

logging.basicConfig(level=logging.DEBUG)

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set SQLAlchemy URL from environment variables
section = config.config_ini_section
config.set_section_option(section, "DB_USER", os.getenv("DB_USER"))
config.set_section_option(section, "DB_PASSWORD", os.getenv("DB_PASSWORD"))
config.set_section_option(section, "DB_HOST", os.getenv("DB_HOST"))
config.set_section_option(section, "DB_PORT", os.getenv("DB_PORT", "5432"))
config.set_section_option(section, "DB_NAME", "wave_analytics")


# Model's MetaData object
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
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
            include_schemas=True,  # Add this for schema support
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

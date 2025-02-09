import os
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RetryPolicy,
    resource,
)
from dotenv import load_dotenv
from src.backend.dagster_break_analytics.io_managers.postgres_io_manager import (
    PostgresIOManager,
)
from src.backend.dagster_break_analytics.assets.buoy_data_dag import raw_buoy_data
from src.backend.dagster_break_analytics.resources.email_notification import (
    EmailNotification,
)
import logging

load_dotenv(override=True)

# Define retry policy
buoy_data_retry_policy = RetryPolicy(max_retries=3, delay=600)

# Define the job
buoy_data_job = define_asset_job(
    name="buoy_data_job",
    selection=["raw_buoy_data"],
    op_retry_policy=buoy_data_retry_policy,
)

# Run at 2 AM every day (often better for data processing)
buoy_data_schedule = ScheduleDefinition(
    job=buoy_data_job, cron_schedule="0 2 * * *"  # At 02:00 every day
)

# Configure logging levels for specific loggers
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("alembic").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


@resource
def postgres_io_manager(init_context):
    """Resource for managing PostgreSQL I/O operations with configurable connection parameters.

    Args:
        init_context: The initialization context containing resource configuration.

    Returns:
        PostgresIOManager: Configured PostgreSQL I/O manager instance.
    """
    return PostgresIOManager(
        username=init_context.resource_config["username"],
        password=init_context.resource_config["password"],
        host=init_context.resource_config["host"],
        port=init_context.resource_config["port"],
        database=init_context.resource_config["database"],
    )


defs = Definitions(
    assets=[raw_buoy_data],
    resources={
        "postgres_io": PostgresIOManager(
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME"),
        ),
        "email_notification": EmailNotification(
            smtp_server=os.getenv("SMTP_SERVER"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            sender_email=os.getenv("SENDER_EMAIL"),
            sender_password=os.getenv("SENDER_PASSWORD"),
            recipient_emails=os.getenv("RECIPIENT_EMAILS").split(","),
        ),
    },
    schedules=[buoy_data_schedule],
)

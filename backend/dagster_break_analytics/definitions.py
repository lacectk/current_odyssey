from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RetryPolicy,
)
from backend.dagster_break_analytics.io_managers.postgres_io_manager import (
    PostgresIOManager,
)
from backend.dagster_break_analytics.assets.buoy_data import raw_buoy_data
from backend.dagster_break_analytics.resources.email_notification import (
    EmailNotification,
)
import os

# Define retry policy
buoy_data_retry_policy = RetryPolicy(max_retries=3, delay=600)

# Define the job
buoy_data_job = define_asset_job(
    name="buoy_data_job",
    selection=[raw_buoy_data],
    op_retry_policy=buoy_data_retry_policy,
)

# Run at 2 AM every day (often better for data processing)
buoy_data_schedule = ScheduleDefinition(
    job=buoy_data_job, cron_schedule="0 2 * * *"  # At 02:00 every day
)

defs = Definitions(
    assets=[raw_buoy_data],
    resources={
        "postgres_io": PostgresIOManager(
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
        ),
        "email_notification": EmailNotification,
    },
    schedules=[buoy_data_schedule],
)

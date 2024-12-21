import os
from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    RetryPolicy,
)
from backend.dagster_break_analytics.io_managers.postgres_io_manager import (
    PostgresIOManager,
)
from backend.dagster_break_analytics.assets.buoy_data_dag import raw_buoy_data
from backend.dagster_break_analytics.resources.email_notification import (
    EmailNotification,
)

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
            username=EnvVar("DB_USER"),
            password=EnvVar("DB_PASSWORD"),
            host=EnvVar("DB_HOST"),
            port=EnvVar.int("DB_PORT"),
            database="wave_data",
        ),
        "email_notification": EmailNotification(
            smtp_server=EnvVar("SMTP_SERVER"),
            smtp_port=EnvVar.int("SMTP_PORT"),
            sender_email=EnvVar("SENDER_EMAIL"),
            sender_password=EnvVar("EMAIL_APP_PASSWORD"),
            recipient_emails=os.getenv("RECIPIENT_EMAILS").split(","),
        ),
    },
    schedules=[buoy_data_schedule],
)

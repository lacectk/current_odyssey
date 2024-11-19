from dagster import Definitions, ScheduleDefinition, define_asset_job, RetryPolicy
from .io_managers.postgres import PostgresIOManager
from .assets.sources.buoy_data import raw_buoy_data

# Define retry policy
buoy_data_retry_policy = RetryPolicy(
    max_retries=3, delay=600, jitter_factor=0.1, retry_intervals=[300, 600, 1000]
)
# Define the job
buoy_data_job = define_asset_job(
    name="buoy_data_job", selection=[raw_buoy_data], retry_policy=buoy_data_retry_policy
)

# Run at 2 AM every day (often better for data processing)
buoy_data_schedule = ScheduleDefinition(
    job=buoy_data_job, cron_schedule="0 2 * * *"  # At 02:00 every day
)

defs = Definitions(
    assets=[raw_buoy_data],
    resources={
        "postgres_io": PostgresIOManager(
            username={"env": "DB_USERNAME"},
            password={"env": "DB_PASSWORD"},
            host={"env": "DB_HOST"},
            port=5432,
            database={"env": "DB_NAME"},
        )
    },
    schedules=[buoy_data_schedule],
)

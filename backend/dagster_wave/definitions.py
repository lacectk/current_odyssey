from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from backend.dagster_wave.assets import assets
from dagster_postgres import postgres_resource

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define jobs
daily_wave_job = define_asset_job(
    name="daily_wave_job",
    selection=["check_database", "daily_wave_data"],
)

monthly_consistency_job = define_asset_job(
    name="monthly_consistency_job",
    selection=["check_source_data", "wave_consistency_scores"],
)

# Define schedules
daily_schedule = ScheduleDefinition(
    job=daily_wave_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
)

monthly_schedule = ScheduleDefinition(
    job=monthly_consistency_job,
    cron_schedule="0 3 1 * *",  # Monthly on 1st at 3 AM
)

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource.configured(
            {
                "connection_string": "postgresql://user:password@localhost:5432/wave_consistency"
            }
        )
    },
    schedules=[daily_schedule, monthly_schedule],
)

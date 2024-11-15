from dagster import (
    asset,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    Output,
    MetadataValue,
)
from dagster_postgres import postgres_resource
from datetime import datetime, timedelta

from backend.buoy_data.localized_wave import LocalizedWaveProcessor
from backend.break_consistency_classifier.wave_consistency_labeler import (
    WaveConsistencyLabeler,
)

# Partition definitions
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")


@asset(
    description="Verifies the existence of required database tables",
    group="wave_data",
)
def check_database(context: AssetExecutionContext, postgres: postgres_resource) -> bool:
    """Check if required database tables exist."""
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'localized_wave_data'
                );
            """
            )
            exists = cursor.fetchone()[0]

            if not exists:
                context.log.error("Required database table does not exist")
                raise Exception("localized_wave_data table not found")

            return exists


@asset(
    description="Daily wave data collection from NDBC stations",
    group="wave_data",
    partitions_def=daily_partitions,
    deps=["check_database"],
)
def daily_wave_data(context: AssetExecutionContext) -> Output:
    """Process daily wave data from NDBC stations."""
    try:
        context.log.info("Starting wave data collection")
        processor = LocalizedWaveProcessor()
        processor.process_data()

        # Get some metadata about the processing
        with processor.engine.connect() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) as count, 
                       MAX(datetime) as latest_date
                FROM localized_wave_data
                WHERE datetime >= NOW() - INTERVAL '24 hours'
            """
            )
            row = result.fetchone()

        return Output(
            value=True,
            metadata={
                "records_processed": MetadataValue.int(row[0]),
                "latest_date": MetadataValue.text(str(row[1])),
            },
        )
    except Exception as e:
        context.log.error(f"Error processing wave data: {str(e)}")
        raise


@asset(
    description="Check if sufficient source data is available",
    group="consistency",
    partitions_def=monthly_partitions,
)
def check_source_data(
    context: AssetExecutionContext, postgres: postgres_resource
) -> bool:
    """Verify sufficient data exists for consistency calculation."""
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM localized_wave_data
                WHERE datetime >= NOW() - INTERVAL '30 days';
            """
            )
            count = cursor.fetchone()[0]

            if count < 1000:  # Adjust threshold as needed
                context.log.warning(f"Insufficient data points: {count}")
                return False

            context.log.info(f"Found {count} data points for processing")
            return True


@asset(
    description="Monthly wave consistency calculation",
    group="consistency",
    partitions_def=monthly_partitions,
    deps=["check_source_data", "daily_wave_data"],
)
def wave_consistency_scores(context: AssetExecutionContext) -> Output:
    """Calculate monthly wave consistency scores."""
    try:
        context.log.info("Starting consistency calculation")
        labeler = WaveConsistencyLabeler()
        labeler.run()

        # Get metadata about the calculation
        with labeler.engine_new.connect() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) as count,
                       AVG(consistency_score) as avg_score
                FROM wave_consistency_trends
                WHERE month_year = date_trunc('month', CURRENT_DATE)
            """
            )
            row = result.fetchone()

        return Output(
            value=True,
            metadata={
                "stations_processed": MetadataValue.int(row[0]),
                "average_consistency": MetadataValue.float(float(row[1])),
            },
        )
    except Exception as e:
        context.log.error(f"Error calculating consistency: {str(e)}")
        raise

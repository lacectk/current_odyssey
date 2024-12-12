from dagster import asset, AssetExecutionContext, MetadataValue, Output
from datetime import datetime
import pandas as pd
from backend.buoy_data.localized_wave import LocalizedWaveProcessor
from backend.stations.stations import StationsFetcher


@asset(
    description="Raw NDBC buoy data with wave measurements",
    metadata={
        "source": "NDBC API",
        "update_frequency": "daily",
    },
    io_manager_key="postgres_io",
)
def raw_buoy_data(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch and process raw buoy data from NDBC stations."""

    try:
        context.log.info("Starting wave data collection")

        # Initialize stations
        stations = StationsFetcher()
        station_ids = stations.fetch_station_ids()
        context.log.info(f"Found {len(station_ids)} stations to process")

        # Initialize processor
        processor = LocalizedWaveProcessor(station_ids)

        # Create table if needed
        processor.create_wave_table()

        # Process the data
        processor.process_data()

        # Fetch the processed data for output
        with processor.engine.connect() as conn:
            df = pd.read_sql(
                """
                SELECT
                    station_id,
                    datetime,
                    latitude,
                    longitude,
                    wvht as wave_height,
                    dpd as wave_period,
                    mwd as wave_direction,
                    apd as avg_wave_period
                FROM localized_wave_data
                WHERE datetime >= NOW() - INTERVAL '24 hours'
            """,
                conn,
            )

        context.log.info("Wave data fetched successfully")

        # Calculate quality metrics
        quality_metrics = {
            "record_count": len(df),
            "stations_count": df["station_id"].nunique(),
            "missing_data_pct": df[["wave_height", "wave_period", "wave_direction"]]
            .isna()
            .mean()
            .mean()
            * 100,
            "time_range": f"{df['datetime'].min()} to {df['datetime'].max()}",
        }

        context.log.info(
            f"Processed {quality_metrics['record_count']} records from {quality_metrics['stations_count']} stations"
        )

        return Output(
            df,
            metadata={
                "record_count": MetadataValue.int(quality_metrics["record_count"]),
                "stations_count": MetadataValue.int(quality_metrics["stations_count"]),
                "missing_data_percentage": MetadataValue.float(
                    quality_metrics["missing_data_pct"]
                ),
                "time_range": MetadataValue.text(quality_metrics["time_range"]),
                "process_date": MetadataValue.text(str(datetime.now())),
            },
        )

    except Exception as e:
        context.log.error(f"Error processing wave data: {str(e)}")
        email_client = context.resources.email_notification.get_client()
        email_client.send_message(
            subject="Wave Data Processing Failed",
            message=f"An error occurred: {str(e)}",
        )
        raise
    finally:
        processor.close()

from datetime import datetime
from dagster import asset, AssetExecutionContext, MetadataValue, Output
import pandas as pd
from sqlalchemy import select, Table, MetaData, text
from src.backend.buoy_data.localized_wave import LocalizedWaveProcessor
from src.backend.stations.stations import StationsFetcher


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

        # Define table
        metadata = MetaData(schema="raw_data")
        wave_table = Table(
            "localized_wave_data", metadata, autoload_with=processor.engine
        )

        # Fetch the processed data for output
        with processor.engine.connect() as conn:
            query = select(
                wave_table.c.station_id,
                wave_table.c.datetime,
                wave_table.c.latitude,
                wave_table.c.longitude,
                wave_table.c["wave_height(wvht)"],
                wave_table.c["dominant_period(dpd)"],
                wave_table.c["mean_wave_direction(mwd)"],
                wave_table.c["average_period(apd)"],
            ).where(wave_table.c.datetime >= text("NOW() - INTERVAL '24 hours'"))

            df = pd.read_sql(query, conn)

        context.log.info("Wave data fetched successfully")

        # Calculate quality metrics
        quality_metrics = {
            "record_count": len(df),
            "stations_count": df["station_id"].nunique(),
            "missing_data_pct": df[
                [
                    "wave_height(wvht)",
                    "dominant_period(dpd)",
                    "mean_wave_direction(mwd)",
                ]
            ]
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
        if "processor" in locals() and processor:
            processor.close()

from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext, MetadataValue, Output
from sqlalchemy import select, Table, MetaData
import pandas as pd
import pytz
import aiohttp
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from contextlib import asynccontextmanager
from src.backend.buoy_data.localized_wave import LocalizedWaveProcessor
from src.backend.stations.stations import StationsFetcher


@asynccontextmanager
async def get_processor_session(station_ids, pool_size=5):
    """Context manager for processor and database sessions."""
    processor = None
    try:
        processor = LocalizedWaveProcessor(station_ids, pool_size=pool_size)
        yield processor
    finally:
        if processor:
            await processor.close()


@asset(
    description="Raw NDBC buoy data with wave measurements",
    metadata={
        "source": "NDBC API",
        "update_frequency": "daily",
    },
    io_manager_key="postgres_io",
    required_resource_keys={"email_notification"},
)
async def raw_buoy_data(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch and process raw buoy data from NDBC stations."""
    processor = None
    conn = None

    try:
        context.log.info("Starting wave data collection")

        # Initialize stations
        stations = StationsFetcher()
        station_ids = stations.fetch_station_ids()
        context.log.info("Found %d stations to process", len(station_ids))

        # Use context manager for processor
        async with get_processor_session(station_ids) as processor:
            # Fetch new data
            await processor.fetch_stations_data()

            # Query the newly fetched data
            metadata = MetaData(schema="raw_data")
            wave_table = Table(
                "localized_wave_data", metadata, autoload_with=processor.engine
            )

            utc_now = datetime.now(pytz.utc)
            start_time = utc_now - timedelta(days=1)

            # Use connection context manager
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
                ).where(wave_table.c.datetime >= start_time)

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
                "Processed %d records from %d stations",
                quality_metrics["record_count"],
                quality_metrics["stations_count"],
            )

            return Output(
                df,
                metadata={
                    "record_count": MetadataValue.int(quality_metrics["record_count"]),
                    "stations_count": MetadataValue.int(
                        quality_metrics["stations_count"]
                    ),
                    "missing_data_percentage": MetadataValue.float(
                        quality_metrics["missing_data_pct"]
                    ),
                    "time_range": MetadataValue.text(quality_metrics["time_range"]),
                    "process_date": MetadataValue.text(str(datetime.now())),
                },
            )

    except (aiohttp.ClientError, SQLAlchemyError, ValueError) as e:
        context.log.error("Error processing wave data: %s", str(e))
        email_client = context.resources.email_notification.get_client()
        email_client.send_message(
            subject="Wave Data Processing Failed",
            message=f"An error occurred: {str(e)}",
        )
        raise

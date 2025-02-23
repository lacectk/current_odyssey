"""
Module for processing and storing NOAA buoy wave data.

This module provides functionality to fetch, process, and store wave measurement data
from NOAA buoy stations. It handles multiple data formats and includes support for
localized wave measurements with geographical coordinates.
"""

from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext, MetadataValue, Output, AssetKey
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import pytz
import aiohttp
from src.backend.buoy_data.localized_wave import LocalizedWaveProcessor
from src.backend.stations.stations import StationsFetcher
from src.backend.models import WaveDataModel


@asset(
    key=AssetKey("raw_buoy_data"),
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
    try:
        context.log.info("Starting wave data collection")

        # Initialize stations
        stations = StationsFetcher()
        station_ids = stations.fetch_station_ids()
        context.log.info("Found %d stations to process", len(station_ids))

        # Use the class's own context manager
        async with LocalizedWaveProcessor(station_ids) as processor:
            await processor.fetch_stations_data()

            utc_now = datetime.now(pytz.utc)
            start_time = utc_now - timedelta(days=1)

            # Use connection context manager
            with processor.engine.connect() as conn:
                stmt = select(WaveDataModel).where(WaveDataModel.datetime >= start_time)
                result = conn.execute(stmt)
                df = pd.DataFrame(result.fetchall())

                # Limit to 10 records if more exist. For testing purposes only.
                # if len(df) > 10:
                #     context.log.info(
                #         "Limiting output to 10 records from %d total", len(df)
                #     )
                #     df = df.head(10)
                # else:
                #     context.log.info(
                #         "Found %d records (less than limit of 10)", len(df)
                #     )

            context.log.info("Wave data fetched successfully")

            # Calculate quality metrics
            quality_metrics = {
                "record_count": int(len(df)),
                "stations_count": int(df["station_id"].nunique()),
                "missing_data_pct": float(
                    df[
                        [
                            "wave_height(wvht)",
                            "dominant_period(dpd)",
                            "mean_wave_direction(mwd)",
                        ]
                    ]
                    .isna()
                    .mean()
                    .mean()
                    * 100
                ),
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

from dagster import build_op_context
from unittest.mock import MagicMock
import pytest
import pandas as pd
from backend.dagster_break_analytics.assets.buoy_data_dag import raw_buoy_data
from backend.stations.stations import StationsFetcher, Station
from backend.buoy_data.localized_wave import LocalizedWaveProcessor


@pytest.fixture
def mock_context():
    """Create a mock Dagster context for testing."""
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_context.resources = {"email_notification": MagicMock()}
    return mock_context


@pytest.fixture
async def setup_test_database():
    """Ensure the database is set up for the test."""
    stations_fetcher = StationsFetcher()
    stations_fetcher.setup_database()

    processor = LocalizedWaveProcessor()
    processor.create_wave_table()

    yield  # Allows running the test after setup

    # Cleanup code if necessary, e.g., deleting test data
    await processor.close()
    stations_fetcher.conn.close()


@pytest.mark.asyncio
async def test_raw_buoy_data(setup_test_database):
    # Step 1: Insert test station data
    stations_fetcher = StationsFetcher()
    stations_fetcher.setup_database()

    stations_data = {
        "station_1": {"latitude": 34.0, "longitude": -118.0},
        "station_2": {"latitude": 35.0, "longitude": -119.0},
    }

    station_objects = {
        station_id: Station(station_id, coords["latitude"], coords["longitude"])
        for station_id, coords in stations_data.items()
    }
    await stations_fetcher.insert_into_database(station_objects)

    # Step 2: Run wave processor and insert test wave data
    processor = LocalizedWaveProcessor(list(stations_data.keys()))
    processor.create_wave_table()

    test_wave_data = pd.DataFrame(
        [
            {
                "station_id": station_id,
                "DateTime": "2024-01-01 00:00:00",
                "wvht": 1.5,
                "dpd": 10,
                "apd": 9,
                "mwd": 180,
                "latitude": coords["latitude"],
                "longitude": coords["longitude"],
            }
            for station_id, coords in stations_data.items()
        ]
    )

    for _, row in test_wave_data.iterrows():
        await processor.insert_localized_wave_data_into_db(
            station_id=row["station_id"],
            data=test_wave_data[test_wave_data["station_id"] == row["station_id"]],
            lat=row["latitude"],
            lon=row["longitude"],
        )
    processor.close

    # Step 3: Execute the asset
    context = build_op_context()
    output = raw_buoy_data(context)

    # Step 4: Assertions
    # Validate DataFrame structure
    assert isinstance(output, pd.DataFrame)
    assert list(output.columns) == [
        "station_id",
        "DateTime",
        "latitude",
        "longitude",
        "wave_height",
        "wave_period",
        "wave_direction",
        "avg_wave_period",
    ]

    # Validate metadata
    assert output.metadata["record_count"] == 2
    assert output.metadata["stations_count"] == 2
    assert output.metadata["missing_data_percentage"] == 0.0

from unittest.mock import MagicMock, patch
import pandas as pd
from src.backend.dagster_break_analytics.io_managers.postgres_io_manager import (
    PostgresIOManager,
)
from dagster import OutputContext, InputContext


@patch(
    "src.backend.dagster_break_analytics.io_managers.postgres_io_manager.create_engine"
)
@patch("src.backend.dagster_break_analytics.io_managers.postgres_io_manager.inspect")
@patch("pandas.DataFrame.to_sql")
def test_handle_output(mock_to_sql, mock_inspect, mock_create_engine):
    # Mock the SQLAlchemy engine and connection
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_connection = mock_engine.begin.return_value.__enter__.return_value

    # Mock the inspect function to simulate the table check
    mock_inspector = MagicMock()
    mock_inspect.return_value = mock_inspector
    mock_inspector.has_table.return_value = False  # Simulate table not existing

    # Initialize PostgresIOManager
    manager = PostgresIOManager(
        username="test_user",
        password="test_password",
        host="localhost",
        port=5432,
        database="test_db",
    )

    # Mock the OutputContext
    mock_context = MagicMock(spec=OutputContext)
    mock_context.asset_key.path = ["test_asset"]
    mock_context.has_partition_key = False
    mock_context.log.info = MagicMock()

    # Create a sample DataFrame
    test_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    manager.handle_output(mock_context, test_df)

    # Assertions to verify database interactions
    mock_create_engine.assert_called_once_with(
        "postgresql://test_user:test_password@localhost:5432/test_db"
    )
    mock_engine.begin.assert_called_once()
    mock_inspect.assert_called_once_with(mock_engine)
    mock_inspector.has_table.assert_called_once_with("wave_data_test_asset")

    # No partition key, so no DELETE query
    mock_connection.execute.assert_not_called()
    mock_to_sql.assert_any_call(
        "wave_data_test_asset",
        mock_connection,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
        dtype={},
    )


@patch(
    "src.backend.dagster_break_analytics.io_managers.postgres_io_manager.create_engine"
)
@patch(
    "src.backend.dagster_break_analytics.io_managers.postgres_io_manager.pd.read_sql"
)
def test_load_input(mock_read_sql, mock_create_engine):
    # Mock the SQLAlchemy engine
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    # Initialize PostgresIOManager
    manager = PostgresIOManager(
        username="test_user",
        password="test_password",
        host="localhost",
        port=5432,
        database="test_db",
    )

    # Mock the InputContext
    mock_context = MagicMock(spec=InputContext)
    mock_context.asset_key.path = ["test_asset"]
    mock_context.has_partition_key = False

    # Mock the returned DataFrame from read_sql
    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_sql.return_value = mock_df

    # Call load_input
    result_df = manager.load_input(mock_context)

    # Assertions
    mock_read_sql.assert_called_once_with(
        "SELECT * FROM wave_data_test_asset", manager._engine
    )
    assert result_df.equals(mock_df)

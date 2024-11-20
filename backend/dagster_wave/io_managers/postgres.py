from dagster import (
    IOManager,
    OutputContext,
    InputContext,
    ConfigurableIOManager,
    InitResourceContext,
    Field,
    StringSource,
    IntSource,
)
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from datetime import datetime


class PostgresIOManager(ConfigurableIOManager):
    """I/O Manager for handling DataFrame persistence to PostgreSQL."""

    class Config:
        username: str = Field(
            StringSource, description="PostgreSQL username", is_required=True
        )
        password: str = Field(
            StringSource, description="PostgreSQL password", is_required=True
        )
        host: str = Field(StringSource, description="PostgreSQL host", is_required=True)
        port: int = Field(IntSource, description="PostgreSQL port", default=5432)
        database: str = Field(
            StringSource, description="PostgreSQL database name", is_required=True
        )

    def __init__(
        self, username: str, password: str, host: str, port: int, database: str
    ):
        self._engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        self._metadata = MetaData()

    @staticmethod
    def _get_table_name(context: OutputContext) -> str:
        """Generate table name from asset key."""
        return f"wave_data_{context.asset_key.path[-1]}"

    def _get_partition_key(self, context: OutputContext) -> str:
        """Get partition key if asset is partitioned."""
        if context.has_partition_key:
            return context.partition_key
        return datetime.now().strftime("%Y%m")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """Persist DataFrame to PostgreSQL."""
        if not isinstance(obj, pd.DataFrame):
            raise TypeError("This I/O Manager only handles pandas DataFrames")

        table_name = self._get_table_name(context)
        partition_key = self._get_partition_key(context)

        # Add metadata columns
        obj["_partition_key"] = partition_key
        obj["_updated_at"] = datetime.now()
        obj["_asset_partition"] = (
            context.partition_key if context.has_partition_key else None
        )

        # Log operation metadata
        context.log.info(f"Writing {len(obj)} rows to {table_name}")

        try:
            # Write to database
            with self._engine.begin() as conn:
                # If partitioned, delete existing partition data
                if context.has_partition_key:
                    conn.execute(
                        f"DELETE FROM {table_name} WHERE _partition_key = '{partition_key}'"
                    )

                # Write new data
                obj.to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000,
                )

            # Add metadata to context
            context.add_output_metadata(
                {
                    "table": table_name,
                    "partition": partition_key,
                    "row_count": len(obj),
                    "columns": list(obj.columns),
                }
            )

        except Exception as e:
            context.log.error(f"Error writing to PostgreSQL: {str(e)}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load DataFrame from PostgreSQL."""
        table_name = self._get_table_name(context)

        try:
            # Build query
            query = f"SELECT * FROM {table_name}"

            # Add partition filter if applicable
            if context.has_partition_key:
                partition_key = context.partition_key
                query += f" WHERE _partition_key = '{partition_key}'"

            # Load data
            df = pd.read_sql(query, self._engine)

            # Remove metadata columns for processing
            for col in ["_partition_key", "_updated_at", "_asset_partition"]:
                if col in df.columns:
                    df = df.drop(columns=[col])

            context.log.info(f"Loaded {len(df)} rows from {table_name}")
            return df

        except Exception as e:
            context.log.error(f"Error reading from PostgreSQL: {str(e)}")
            raise

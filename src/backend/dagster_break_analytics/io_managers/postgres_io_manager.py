from backend.dagster_break_analytics.io_managers.postgres_schemas import ASSET_DTYPES
from dagster import OutputContext, InputContext, ConfigurableIOManager
from datetime import datetime
import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine, inspect, MetaData


class PostgresIOManager(ConfigurableIOManager):
    """I/O Manager for handling DataFrame persistence to PostgreSQL."""

    username: str = Field(..., description="PostgreSQL username")
    password: str = Field(..., description="PostgreSQL password")
    host: str = Field(..., description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(..., description="PostgreSQL database name")

    def __post_init__(self):
        """Custom post-initialization logic."""
        self._engine = create_engine(
            f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
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

        asset_key = context.asset_key.path[-1]
        dtypes = ASSET_DTYPES.get(asset_key, {})
        # Add metadata columns
        obj_copy = obj.copy()
        obj_copy["_partition_key"] = partition_key
        obj_copy["_updated_at"] = datetime.now()
        obj_copy["_asset_partition"] = (
            context.partition_key if context.has_partition_key else None
        )

        # Log operation metadata
        context.log.info(f"Writing {len(obj_copy)} rows to {table_name}")

        try:
            # Write to database
            with self._engine.begin() as conn:
                # Check if the table exists
                inspector = inspect(self._engine)
                if not inspector.has_table(table_name):
                    context.log.info(f"Table {table_name} does not exist. Creating it.")
                    obj_copy.head(0).to_sql(
                        table_name, conn, if_exists="replace", index=False
                    )

                # If partitioned, delete existing partition data
                if context.has_partition_key:
                    conn.execute(
                        f"DELETE FROM {table_name} WHERE _partition_key = :partition_key",
                        {"partition_key": partition_key},
                    )

                # Write new data
                obj_copy.to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000,
                    dtype=dtypes,
                )

            # Add metadata to context
            context.add_output_metadata(
                {
                    "table": table_name,
                    "partition": partition_key,
                    "row_count": len(obj_copy),
                    "columns": list(obj_copy.columns),
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

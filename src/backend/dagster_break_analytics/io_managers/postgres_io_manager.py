from src.backend.dagster_break_analytics.io_managers.postgres_schemas import (
    ASSET_DTYPES,
)
from dagster import OutputContext, InputContext, ConfigurableIOManager
from datetime import datetime
import pandas as pd
from pydantic import Field
from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
    delete,
    insert,
    select,
)
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine


class PostgresIOManager(ConfigurableIOManager):
    """I/O Manager for handling DataFrame persistence to PostgreSQL."""

    username: str = Field(..., description="PostgreSQL username")
    password: str = Field(..., description="PostgreSQL password")
    host: str = Field(..., description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(..., description="PostgreSQL database name")

    def __init__(self, **data):
        """Initialize the I/O manager with database connection."""
        super().__init__(**data)
        self._engine = self._create_engine()
        self._metadata = MetaData()

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with proper configuration."""
        return create_engine(
            f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            pool_pre_ping=True,  # Enable connection health checks
            pool_size=5,  # Set connection pool size
            max_overflow=10,  # Allow up to 10 connections beyond pool_size
        )

    @staticmethod
    def _get_table_name(context: OutputContext | InputContext) -> str:
        """Generate table name from asset key."""
        return f"wave_data_{context.asset_key.path[-1]}"

    def _get_table(self, table_name: str) -> Table:
        """Get SQLAlchemy Table object with reflection."""
        return Table(
            table_name, self._metadata, autoload_with=self._engine, extend_existing=True
        )

    def _prepare_data(
        self, df: pd.DataFrame, context: OutputContext
    ) -> tuple[pd.DataFrame, str]:
        """Prepare DataFrame for insertion with metadata columns."""
        partition_key = datetime.now().strftime("%Y%m")

        df_copy = df.copy()
        df_copy["_partition_key"] = partition_key
        df_copy["_updated_at"] = datetime.now()
        df_copy["_asset_partition"] = (
            context.partition_key if context.has_partition_key else None
        )

        return df_copy, partition_key

    def _delete_partition_data(
        self, session: Session, table: Table, partition_key: str
    ) -> None:
        """Delete existing partition data."""
        delete_stmt = delete(table).where(table.c._partition_key == partition_key)
        session.execute(delete_stmt)

    def _insert_dataframe(
        self, session: Session, table: Table, df: pd.DataFrame, chunk_size: int = 10000
    ) -> None:
        """Insert DataFrame in chunks using SQLAlchemy Core."""
        records = df.to_dict(orient="records")

        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            session.execute(insert(table), chunk)
            session.flush()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """Persist DataFrame to PostgreSQL using SQLAlchemy."""
        if not isinstance(obj, pd.DataFrame):
            raise TypeError("This I/O Manager only handles pandas DataFrames")

        table_name = self._get_table_name(context)
        df_with_metadata, partition_key = self._prepare_data(obj, context)

        try:
            table = self._get_table(table_name)

            with Session(self._engine) as session:
                if context.has_partition_key:
                    self._delete_partition_data(session, table, partition_key)

                self._insert_dataframe(session, table, df_with_metadata)
                session.commit()

            # Add metadata to context
            context.add_output_metadata(
                {
                    "table": table_name,
                    "partition": partition_key,
                    "row_count": len(df_with_metadata),
                    "columns": list(df_with_metadata.columns),
                }
            )

        except Exception as e:
            context.log.error(f"Error writing to PostgreSQL: {str(e)}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load DataFrame from PostgreSQL using SQLAlchemy."""
        table_name = self._get_table_name(context)
        table = self._get_table(table_name)

        try:
            # Build query using SQLAlchemy
            query = select(table)
            if context.has_partition_key:
                query = query.where(table.c._partition_key == context.partition_key)

            # Execute query and load into DataFrame
            with Session(self._engine) as session:
                result = session.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

            # Remove metadata columns
            metadata_columns = ["_partition_key", "_updated_at", "_asset_partition"]
            df = df.drop(columns=[col for col in metadata_columns if col in df.columns])

            context.log.info(f"Loaded {len(df)} rows from {table_name}")
            return df

        except Exception as e:
            context.log.error(f"Error reading from PostgreSQL: {str(e)}")
            raise

    def __del__(self):
        """Cleanup database connections."""
        if hasattr(self, "_engine"):
            self._engine.dispose()

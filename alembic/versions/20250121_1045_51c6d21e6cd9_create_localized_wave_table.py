"""create localized_wave_table

Revision ID: 51c6d21e6cd9
Revises: 4c421cc0d6f6
Create Date: 2025-01-21 10:45:34.919804

"""

from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "51c6d21e6cd9"
down_revision: Union[str, None] = "4c421cc0d6f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the localized_wave_data table in raw_data schema.

    Creates a table to store wave measurements with station coordinates and timestamps.
    """
    # type: ignore  # for op.create_table linter error
    op.create_table(
        "localized_wave_data",
        sa.Column("station_id", sa.String(length=10), nullable=False),
        sa.Column("datetime", sa.DateTime(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("wave_height(wvht)", sa.Float(), nullable=False),
        sa.Column("dominant_period(dpd)", sa.Float(), nullable=False),
        sa.Column("average_period(apd)", sa.Float(), nullable=False),
        sa.Column("mean_wave_direction(mwd)", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("station_id", "datetime"),
        sa.UniqueConstraint("station_id", "datetime", name="unix_station_datetime"),
        schema="raw_data",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Remove the localized_wave_data table from raw_data schema."""
    # type: ignore  # for op.drop_table linter error
    op.drop_table("localized_wave_data", schema="raw_data")
    # ### end Alembic commands ###

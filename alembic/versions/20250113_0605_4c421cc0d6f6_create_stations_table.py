"""create stations table

Revision ID: 4c421cc0d6f6
Revises: d04103c27067
Create Date: 2025-01-13 06:05:46.807956

"""

from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "4c421cc0d6f6"
down_revision: Union[str, None] = "d04103c27067"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the stations table in raw_data schema.

    Creates a table to store NOAA station metadata including coordinates and timestamps.
    """
    # type: ignore
    op.create_table(
        "stations",
        sa.Column("station_id", sa.String(length=10), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("station_id"),
        schema="raw_data",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Remove the stations table from raw_data schema."""
    # type: ignore
    op.drop_table("stations", schema="raw_data")
    # ### end Alembic commands ###

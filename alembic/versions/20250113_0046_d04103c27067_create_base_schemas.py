from alembic import op

# revision identifiers, used by Alembic.
revision = "d04103c27067"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    op.execute("CREATE SCHEMA IF NOT EXISTS processed_data")
    op.execute("CREATE SCHEMA IF NOT EXISTS metadata")


def downgrade():
    op.execute("DROP SCHEMA IF EXISTS metadata CASCADE")
    op.execute("DROP SCHEMA IF EXISTS processed_data CASCADE")
    op.execute("DROP SCHEMA IF EXISTS raw_data CASCADE")

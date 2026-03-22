"""add config table

Revision ID: 0009_add_config_table
Revises: f1cbd84cb5d6
Create Date: 2026-02-22 11:48:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = '0009_add_config_table'
down_revision = 'da271bac25f9'
branch_labels = None
depends_on = None


def upgrade():
    # Use IF NOT EXISTS so this is safe even if config_manager.py's
    # Base.metadata.create_all() already created the table.
    op.execute("""
        CREATE TABLE IF NOT EXISTS config_entries (
            id VARCHAR NOT NULL,
            value TEXT,
            updated_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_config_entries_id ON config_entries (id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_config_entries_updated_at ON config_entries (updated_at)")


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_config_entries_id")
    op.execute("DROP INDEX IF EXISTS ix_config_entries_updated_at")
    op.execute("DROP TABLE IF EXISTS config_entries")
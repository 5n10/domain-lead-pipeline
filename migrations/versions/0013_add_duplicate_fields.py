"""Add duplicate_of and duplicate_reason fields to businesses.

Revision ID: 0013
Revises: 0012
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("businesses", sa.Column("duplicate_of", UUID(as_uuid=True), sa.ForeignKey("businesses.id", ondelete="SET NULL"), nullable=True))
    op.add_column("businesses", sa.Column("duplicate_reason", sa.Text(), nullable=True))
    op.create_index("businesses_duplicate_of_idx", "businesses", ["duplicate_of"])


def downgrade() -> None:
    op.drop_index("businesses_duplicate_of_idx", table_name="businesses")
    op.drop_column("businesses", "duplicate_reason")
    op.drop_column("businesses", "duplicate_of")

"""Add verification retry fields to businesses.

Tracks retry attempts, last error message, and error timestamp
for the verification retry/dead-letter system.

Revision ID: 0011
Revises: 0010
"""
from alembic import op
import sqlalchemy as sa


revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("businesses", sa.Column("verification_retry_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("businesses", sa.Column("verification_error", sa.Text(), nullable=True))
    op.add_column("businesses", sa.Column("verification_error_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("businesses", "verification_error_at")
    op.drop_column("businesses", "verification_error")
    op.drop_column("businesses", "verification_retry_count")

"""Add verification_confidence column to businesses.

Denormalizes the computed confidence level so the metrics endpoint
can aggregate it via SQL instead of loading all JSONB into Python.

Revision ID: 0010
Revises: f1cbd84cb5d6
"""
from alembic import op
import sqlalchemy as sa


revision = "0010"
down_revision = "f1cbd84cb5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("businesses", sa.Column("verification_confidence", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("businesses", "verification_confidence")

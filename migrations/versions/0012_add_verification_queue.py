"""Add verification_queue table for job queue with advisory lock pattern.

Revision ID: 0012
Revises: 0011
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "verification_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("business_id", UUID(as_uuid=True), sa.ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("layer", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("claimed_by", sa.Text()),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text()),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("vq_status_priority_idx", "verification_queue", ["status", "priority", "created_at"])
    op.create_index("vq_business_idx", "verification_queue", ["business_id"])
    op.create_index("vq_layer_idx", "verification_queue", ["layer"])


def downgrade() -> None:
    op.drop_index("vq_layer_idx", table_name="verification_queue")
    op.drop_index("vq_business_idx", table_name="verification_queue")
    op.drop_index("vq_status_priority_idx", table_name="verification_queue")
    op.drop_table("verification_queue")

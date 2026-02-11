"""add business lead scoring and export tracking

Revision ID: 0008_biz_leads
Revises: 0007_ckpt_scope_norm
Create Date: 2026-02-11
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0008_biz_leads"
down_revision = "0007_ckpt_scope_norm"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("businesses", sa.Column("lead_score", sa.Numeric()))
    op.add_column("businesses", sa.Column("score_reasons", postgresql.JSONB()))
    op.add_column("businesses", sa.Column("scored_at", sa.DateTime(timezone=True)))
    op.create_index("businesses_lead_score_idx", "businesses", ["lead_score"])

    op.create_table(
        "business_outreach_exports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("business_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("platform", sa.Text(), nullable=False),
        sa.Column("campaign_id", sa.Text()),
        sa.Column("exported_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("raw", postgresql.JSONB()),
    )
    op.create_unique_constraint(
        "business_outreach_exports_business_platform_uidx",
        "business_outreach_exports",
        ["business_id", "platform"],
    )
    op.create_index(
        "business_outreach_exports_platform_status_idx",
        "business_outreach_exports",
        ["platform", "status"],
    )


def downgrade():
    op.drop_index("business_outreach_exports_platform_status_idx", table_name="business_outreach_exports")
    op.drop_constraint(
        "business_outreach_exports_business_platform_uidx",
        "business_outreach_exports",
        type_="unique",
    )
    op.drop_table("business_outreach_exports")

    op.drop_index("businesses_lead_score_idx", table_name="businesses")
    op.drop_column("businesses", "scored_at")
    op.drop_column("businesses", "score_reasons")
    op.drop_column("businesses", "lead_score")

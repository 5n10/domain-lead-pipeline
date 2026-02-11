"""add business domain links

Revision ID: 0005_business_domain_links
Revises: 0004_schema_hardening
Create Date: 2026-02-11
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0005_business_domain_links"
down_revision = "0004_schema_hardening"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "business_domain_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("business_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_unique_constraint(
        "business_domain_links_business_domain_uidx",
        "business_domain_links",
        ["business_id", "domain_id"],
    )
    op.create_index("business_domain_links_domain_idx", "business_domain_links", ["domain_id"])


def downgrade():
    op.drop_index("business_domain_links_domain_idx", table_name="business_domain_links")
    op.drop_constraint("business_domain_links_business_domain_uidx", "business_domain_links", type_="unique")
    op.drop_table("business_domain_links")

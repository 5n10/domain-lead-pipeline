"""init

Revision ID: 0001_init
Revises: 
Create Date: 2026-02-10
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")

    op.create_table(
        "domains",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("domain", postgresql.CITEXT(), nullable=False, unique=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'new'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "whois_checks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
        sa.Column("is_registered", sa.Boolean()),
        sa.Column("is_parked", sa.Boolean()),
        sa.Column("has_http", sa.Boolean()),
        sa.Column("http_status", sa.Integer()),
        sa.Column("registrar", sa.Text()),
        sa.Column("raw", postgresql.JSONB()),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "domaintools_checks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
        sa.Column("investor_flag", sa.Boolean()),
        sa.Column("score", sa.Numeric()),
        sa.Column("raw", postgresql.JSONB()),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "organizations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("raw", postgresql.JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "contacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("email", postgresql.CITEXT()),
        sa.Column("first_name", sa.Text()),
        sa.Column("last_name", sa.Text()),
        sa.Column("title", sa.Text()),
        sa.Column("source", sa.Text()),
        sa.Column("confidence", sa.Numeric()),
        sa.Column("raw", postgresql.JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "outreach_exports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("contact_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("platform", sa.Text(), nullable=False),
        sa.Column("campaign_id", sa.Text()),
        sa.Column("exported_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("raw", postgresql.JSONB()),
    )

    op.create_index("domains_status_idx", "domains", ["status"])
    op.create_index("whois_domain_idx", "whois_checks", ["domain_id"])
    op.create_index("domaintools_domain_idx", "domaintools_checks", ["domain_id"])
    op.create_index("contacts_email_idx", "contacts", ["email"])


def downgrade():
    op.drop_index("contacts_email_idx", table_name="contacts")
    op.drop_index("domaintools_domain_idx", table_name="domaintools_checks")
    op.drop_index("whois_domain_idx", table_name="whois_checks")
    op.drop_index("domains_status_idx", table_name="domains")

    op.drop_table("outreach_exports")
    op.drop_table("contacts")
    op.drop_table("organizations")
    op.drop_table("domaintools_checks")
    op.drop_table("whois_checks")
    op.drop_table("domains")

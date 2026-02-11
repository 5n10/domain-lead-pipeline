"""add contact scoring columns

Revision ID: 0006_contact_scoring
Revises: 0005_business_domain_links
Create Date: 2026-02-11
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0006_contact_scoring"
down_revision = "0005_business_domain_links"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("contacts", sa.Column("lead_score", sa.Numeric()))
    op.add_column("contacts", sa.Column("score_reasons", postgresql.JSONB()))
    op.add_column("contacts", sa.Column("scored_at", sa.DateTime(timezone=True)))
    op.create_index("contacts_lead_score_idx", "contacts", ["lead_score"])


def downgrade():
    op.drop_index("contacts_lead_score_idx", table_name="contacts")
    op.drop_column("contacts", "scored_at")
    op.drop_column("contacts", "score_reasons")
    op.drop_column("contacts", "lead_score")

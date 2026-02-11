"""add dns fields

Revision ID: 0002_add_dns_fields
Revises: 0001_init
Create Date: 2026-02-10
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_add_dns_fields"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("whois_checks", sa.Column("has_a", sa.Boolean()))
    op.add_column("whois_checks", sa.Column("has_aaaa", sa.Boolean()))
    op.add_column("whois_checks", sa.Column("has_cname", sa.Boolean()))
    op.add_column("whois_checks", sa.Column("has_mx", sa.Boolean()))


def downgrade():
    op.drop_column("whois_checks", "has_mx")
    op.drop_column("whois_checks", "has_cname")
    op.drop_column("whois_checks", "has_aaaa")
    op.drop_column("whois_checks", "has_a")

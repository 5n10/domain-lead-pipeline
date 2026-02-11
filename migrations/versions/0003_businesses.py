"""add businesses and cities

Revision ID: 0003_businesses
Revises: 0002_add_dns_fields
Create Date: 2026-02-10
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0003_businesses"
down_revision = "0002_add_dns_fields"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cities",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("country", sa.Text()),
        sa.Column("region", sa.Text()),
        sa.Column("min_lat", sa.Numeric()),
        sa.Column("min_lon", sa.Numeric()),
        sa.Column("max_lat", sa.Numeric()),
        sa.Column("max_lon", sa.Numeric()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "businesses",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("category", sa.Text()),
        sa.Column("website_url", sa.Text()),
        sa.Column("address", sa.Text()),
        sa.Column("lat", sa.Numeric()),
        sa.Column("lon", sa.Numeric()),
        sa.Column("raw", postgresql.JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("city_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("cities.id", ondelete="SET NULL")),
    )

    op.create_table(
        "business_contacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("business_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("contact_type", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("source", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index("cities_name_idx", "cities", ["name"])
    op.create_index("businesses_city_idx", "businesses", ["city_id"])
    op.create_unique_constraint("businesses_source_uidx", "businesses", ["source", "source_id"])


def downgrade():
    op.drop_constraint("businesses_source_uidx", "businesses", type_="unique")
    op.drop_index("businesses_city_idx", table_name="businesses")
    op.drop_index("cities_name_idx", table_name="cities")
    op.drop_table("business_contacts")
    op.drop_table("businesses")
    op.drop_table("cities")

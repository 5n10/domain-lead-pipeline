"""schema hardening constraints and job tracking

Revision ID: 0004_schema_hardening
Revises: 0003_businesses
Create Date: 2026-02-11
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0004_schema_hardening"
down_revision = "0003_businesses"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        WITH ranked AS (
            SELECT id, ROW_NUMBER() OVER (
                PARTITION BY org_id, email
                ORDER BY created_at ASC, id ASC
            ) AS rn
            FROM contacts
            WHERE email IS NOT NULL
        )
        DELETE FROM contacts c
        USING ranked r
        WHERE c.id = r.id AND r.rn > 1
        """
    )
    op.execute(
        """
        WITH ranked AS (
            SELECT id, ROW_NUMBER() OVER (
                PARTITION BY contact_id, platform
                ORDER BY exported_at ASC, id ASC
            ) AS rn
            FROM outreach_exports
        )
        DELETE FROM outreach_exports oe
        USING ranked r
        WHERE oe.id = r.id AND r.rn > 1
        """
    )
    op.execute(
        """
        WITH ranked AS (
            SELECT id, ROW_NUMBER() OVER (
                PARTITION BY business_id, contact_type, value
                ORDER BY created_at ASC, id ASC
            ) AS rn
            FROM business_contacts
        )
        DELETE FROM business_contacts bc
        USING ranked r
        WHERE bc.id = r.id AND r.rn > 1
        """
    )

    op.create_unique_constraint(
        "contacts_org_email_uidx",
        "contacts",
        ["org_id", "email"],
    )
    op.create_unique_constraint(
        "outreach_exports_contact_platform_uidx",
        "outreach_exports",
        ["contact_id", "platform"],
    )
    op.create_unique_constraint(
        "business_contacts_business_type_value_uidx",
        "business_contacts",
        ["business_id", "contact_type", "value"],
    )

    op.create_table(
        "job_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_name", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text()),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("processed_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("details", postgresql.JSONB()),
        sa.Column("error", sa.Text()),
    )
    op.create_index("job_runs_name_status_idx", "job_runs", ["job_name", "status"])
    op.create_index("job_runs_started_at_idx", "job_runs", ["started_at"])

    op.create_table(
        "job_checkpoints",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("job_runs.id", ondelete="SET NULL")),
        sa.Column("job_name", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text()),
        sa.Column("checkpoint_key", sa.Text(), nullable=False),
        sa.Column("checkpoint_value", sa.Text(), nullable=False),
        sa.Column("details", postgresql.JSONB()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_unique_constraint(
        "job_checkpoints_unique_scope_key_uidx",
        "job_checkpoints",
        ["job_name", "scope", "checkpoint_key"],
    )
    op.create_index("job_checkpoints_name_scope_idx", "job_checkpoints", ["job_name", "scope"])


def downgrade():
    op.drop_index("job_checkpoints_name_scope_idx", table_name="job_checkpoints")
    op.drop_constraint("job_checkpoints_unique_scope_key_uidx", "job_checkpoints", type_="unique")
    op.drop_table("job_checkpoints")

    op.drop_index("job_runs_started_at_idx", table_name="job_runs")
    op.drop_index("job_runs_name_status_idx", table_name="job_runs")
    op.drop_table("job_runs")

    op.drop_constraint("business_contacts_business_type_value_uidx", "business_contacts", type_="unique")
    op.drop_constraint("outreach_exports_contact_platform_uidx", "outreach_exports", type_="unique")
    op.drop_constraint("contacts_org_email_uidx", "contacts", type_="unique")

"""normalize checkpoint scope for global jobs

Revision ID: 0007_ckpt_scope_norm
Revises: 0006_contact_scoring
Create Date: 2026-02-11
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0007_ckpt_scope_norm"
down_revision = "0006_contact_scoring"
branch_labels = None
depends_on = None


GLOBAL_SCOPE = "__global__"


def upgrade():
    op.execute(
        """
        WITH ranked AS (
            SELECT
                ctid,
                row_number() OVER (
                    PARTITION BY job_name, checkpoint_key
                    ORDER BY updated_at DESC, id DESC
                ) AS rn
            FROM job_checkpoints
            WHERE scope IS NULL
        )
        DELETE FROM job_checkpoints jc
        USING ranked r
        WHERE jc.ctid = r.ctid
          AND r.rn > 1
        """
    )
    op.execute(
        sa.text("UPDATE job_checkpoints SET scope = :scope WHERE scope IS NULL").bindparams(scope=GLOBAL_SCOPE)
    )
    op.alter_column(
        "job_checkpoints",
        "scope",
        existing_type=sa.Text(),
        nullable=False,
        server_default=sa.text(f"'{GLOBAL_SCOPE}'"),
    )


def downgrade():
    op.alter_column(
        "job_checkpoints",
        "scope",
        existing_type=sa.Text(),
        nullable=True,
        server_default=None,
    )

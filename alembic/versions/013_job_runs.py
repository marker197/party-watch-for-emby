"""Add job_runs table for scheduler job run history.

Revision ID: 013_job_runs
Revises: 012_watch_history_null_dedup
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "013_job_runs"
down_revision = "012_watch_history_null_dedup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing_tables = inspector.get_table_names()

    if "job_runs" not in existing_tables:
        op.create_table(
            "job_runs",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("job_id", sa.String(64), nullable=False, index=True),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("started_at", sa.DateTime(), nullable=False),
            sa.Column("duration_s", sa.Float(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    op.drop_table("job_runs")

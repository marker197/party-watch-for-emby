"""Add Simkl Auth V2 columns to users table.

Revision ID: 014_simkl_v2_auth
Revises: 013_job_runs
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect


revision = "014_simkl_v2_auth"
down_revision = "013_job_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    cols = {c["name"] for c in inspector.get_columns("users")}

    if "simkl_v2_access_token" not in cols:
        op.add_column("users", sa.Column("simkl_v2_access_token", sa.Text(), nullable=True))
    if "simkl_v2_refresh_token" not in cols:
        op.add_column("users", sa.Column("simkl_v2_refresh_token", sa.Text(), nullable=True))
    if "simkl_v2_token_expires" not in cols:
        op.add_column("users", sa.Column("simkl_v2_token_expires", sa.DateTime(), nullable=True))
    if "simkl_user_id" not in cols:
        op.add_column("users", sa.Column("simkl_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("users", "simkl_user_id")
    op.drop_column("users", "simkl_v2_token_expires")
    op.drop_column("users", "simkl_v2_refresh_token")
    op.drop_column("users", "simkl_v2_access_token")

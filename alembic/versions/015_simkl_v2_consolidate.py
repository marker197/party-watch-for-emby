"""Consolidate Simkl V2 auth into primary columns.

Copies V2 tokens into the V1 columns (overwriting expired V1 tokens),
adds simkl_refresh_token, drops the separate simkl_v2_* columns.

Revision ID: 015_simkl_v2_consolidate
Revises: 014_simkl_v2_auth
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect, text


revision = "015_simkl_v2_consolidate"
down_revision = "014_simkl_v2_auth"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    cols = {c["name"] for c in inspector.get_columns("users")}

    # 1. Add simkl_refresh_token if not present
    if "simkl_refresh_token" not in cols:
        op.add_column("users", sa.Column("simkl_refresh_token", sa.Text(), nullable=True))

    # 2. Copy V2 tokens into primary columns (only where V2 token exists)
    if "simkl_v2_access_token" in cols:
        bind.execute(text("""
            UPDATE users
            SET simkl_access_token = simkl_v2_access_token,
                simkl_refresh_token = simkl_v2_refresh_token,
                simkl_token_expires = simkl_v2_token_expires
            WHERE simkl_v2_access_token IS NOT NULL
        """))

    # 3. Drop the separate V2 columns
    for col in ("simkl_v2_access_token", "simkl_v2_refresh_token", "simkl_v2_token_expires"):
        if col in cols:
            op.drop_column("users", col)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    cols = {c["name"] for c in inspector.get_columns("users")}

    # Re-create V2 columns
    if "simkl_v2_access_token" not in cols:
        op.add_column("users", sa.Column("simkl_v2_access_token", sa.Text(), nullable=True))
    if "simkl_v2_refresh_token" not in cols:
        op.add_column("users", sa.Column("simkl_v2_refresh_token", sa.Text(), nullable=True))
    if "simkl_v2_token_expires" not in cols:
        op.add_column("users", sa.Column("simkl_v2_token_expires", sa.DateTime(), nullable=True))

    # Copy back
    bind.execute(text("""
        UPDATE users
        SET simkl_v2_access_token = simkl_access_token,
            simkl_v2_refresh_token = simkl_refresh_token,
            simkl_v2_token_expires = simkl_token_expires
        WHERE simkl_refresh_token IS NOT NULL
    """))

    if "simkl_refresh_token" in cols:
        op.drop_column("users", "simkl_refresh_token")

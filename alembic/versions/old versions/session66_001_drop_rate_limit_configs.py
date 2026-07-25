"""Drop orphaned rate_limit_configs table.

Revision ID: session66_001
Revises: session65_001
"""

import sqlalchemy as sa
from alembic import op

revision = "session66_001"
down_revision = "session65_001"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index(
        "ix_rate_limit_configs_endpoint_type",
        table_name="rate_limit_configs",
        if_exists=True,
    )
    op.drop_table("rate_limit_configs")


def downgrade():
    # Table was unused on LAN deployment — no need to recreate
    pass

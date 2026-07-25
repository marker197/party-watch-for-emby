"""Drop orphaned queue_weight_snapshots table.

Revision ID: session54_001
Revises: session48_001
"""

from alembic import op

revision = "session54_001"
down_revision = "session48_001"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table("queue_weight_snapshots")


def downgrade():
    # Table was unused — no need to recreate on downgrade
    pass

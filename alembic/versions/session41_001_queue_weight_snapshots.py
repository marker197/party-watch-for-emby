"""Add queue_weight_snapshots table for weight evolution tracking.

Revision ID: session41_001
Revises: session40_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session41_001"
down_revision = "session40_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "queue_weight_snapshots",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("weights_json", sa.JSON, nullable=False),
        sa.Column("stats_json", sa.JSON, nullable=True),
        sa.Column("snapshot_at", sa.DateTime, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("queue_weight_snapshots")

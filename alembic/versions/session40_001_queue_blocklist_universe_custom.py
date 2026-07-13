"""Add queue_blocklist table and is_custom flag on universes.

Revision ID: session40_001
Revises: session38_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session40_001"
down_revision = "session38_001"
branch_labels = None
depends_on = None


def upgrade():
    # Queue blocklist — permanently dismissed smart queue items
    op.create_table(
        "queue_blocklist",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trakt_id", sa.String(64), nullable=False),
        sa.Column("title", sa.String(512)),
        sa.Column("item_type", sa.String(32)),
        sa.Column("blocked_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "trakt_id", name="uq_blocklist_user_trakt"),
    )
    op.create_index("ix_queue_blocklist_user_id", "queue_blocklist", ["user_id"])

    # Universe custom flag — protects user-created universes from scan overwrite
    op.add_column("universes", sa.Column("is_custom", sa.Boolean(), server_default=sa.text("false")))


def downgrade():
    op.drop_column("universes", "is_custom")
    op.drop_index("ix_queue_blocklist_user_id", "queue_blocklist")
    op.drop_table("queue_blocklist")

"""Add dismissed_health_items table for persisting Library Health dismissals.

Revision ID: session65_001
Revises: session55_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session65_001"
down_revision = "session55_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dismissed_health_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("item_type", sa.String(16), nullable=False),
        sa.Column("item_id", sa.String(64), nullable=False),
        sa.Column("dismissed_at", sa.DateTime()),
    )
    # Composite index for fast lookups during report filtering
    op.create_index(
        "ix_dismissed_health_user_type_id",
        "dismissed_health_items",
        ["user_id", "item_type", "item_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_dismissed_health_user_type_id", table_name="dismissed_health_items")
    op.drop_table("dismissed_health_items")

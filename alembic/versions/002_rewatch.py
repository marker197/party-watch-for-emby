"""Add dismissed_rewatch_items table

Revision ID: 002_rewatch
Revises: 001_initial
Create Date: 2026-07-27
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "002_rewatch"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "dismissed_rewatch_items" not in inspector.get_table_names():
        op.create_table(
            "dismissed_rewatch_items",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("item_key", sa.String(128), nullable=False),
            sa.Column("dismissed_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("user_id", "item_key", name="uq_rewatch_dismiss_user_item"),
        )
        op.create_index(op.f("ix_dismissed_rewatch_items_user_id"), "dismissed_rewatch_items", ["user_id"])


def downgrade() -> None:
    op.drop_index(op.f("ix_dismissed_rewatch_items_user_id"), table_name="dismissed_rewatch_items")
    op.drop_table("dismissed_rewatch_items")

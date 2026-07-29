"""Add progress column to watch_history

Revision ID: 005_watch_history_progress
Revises: 004_watch_history_genres
Create Date: 2026-07-29
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "005_watch_history_progress"
down_revision = "004_watch_history_genres"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "watch_history" in inspector.get_table_names():
        columns = [c["name"] for c in inspector.get_columns("watch_history")]
        if "progress" not in columns:
            op.add_column("watch_history", sa.Column("progress", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("watch_history", "progress")

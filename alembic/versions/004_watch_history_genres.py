"""Add genres column to watch_history

Revision ID: 004_watch_history_genres
Revises: 003_watch_history
Create Date: 2026-07-28
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "004_watch_history_genres"
down_revision = "003_watch_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "watch_history" in inspector.get_table_names():
        columns = [c["name"] for c in inspector.get_columns("watch_history")]
        if "genres" not in columns:
            op.add_column("watch_history", sa.Column("genres", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("watch_history", "genres")

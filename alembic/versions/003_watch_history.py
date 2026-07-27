"""Add watch_history table

Revision ID: 003_watch_history
Revises: 002_rewatch
Create Date: 2026-07-27
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "003_watch_history"
down_revision = "002_rewatch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "watch_history" not in inspector.get_table_names():
        op.create_table(
            "watch_history",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("emby_id", sa.String(64), nullable=True),
            sa.Column("item_type", sa.String(32), nullable=False),
            sa.Column("title", sa.String(512), nullable=True),
            sa.Column("series_name", sa.String(512), nullable=True),
            sa.Column("season_number", sa.Integer(), nullable=True),
            sa.Column("episode_number", sa.Integer(), nullable=True),
            sa.Column("imdb_id", sa.String(32), nullable=True),
            sa.Column("tmdb_id", sa.String(32), nullable=True),
            sa.Column("trakt_id", sa.String(32), nullable=True),
            sa.Column("tvdb_id", sa.String(32), nullable=True),
            sa.Column("watched_at", sa.DateTime(), nullable=False),
            sa.Column("runtime_minutes", sa.Integer(), nullable=True),
            sa.Column("source", sa.String(32), nullable=False, server_default="webhook"),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("user_id", "emby_id", "watched_at", name="uq_watch_history_user_item_time"),
        )
        op.create_index(op.f("ix_watch_history_user_id"), "watch_history", ["user_id"])
        op.create_index(op.f("ix_watch_history_watched_at"), "watch_history", ["watched_at"])
        op.create_index("ix_watch_history_user_imdb", "watch_history", ["user_id", "imdb_id"])
        op.create_index("ix_watch_history_user_emby", "watch_history", ["user_id", "emby_id"])


def downgrade() -> None:
    op.drop_index("ix_watch_history_user_emby", table_name="watch_history")
    op.drop_index("ix_watch_history_user_imdb", table_name="watch_history")
    op.drop_index(op.f("ix_watch_history_watched_at"), table_name="watch_history")
    op.drop_index(op.f("ix_watch_history_user_id"), table_name="watch_history")
    op.drop_table("watch_history")

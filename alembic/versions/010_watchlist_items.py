"""Add local watchlist_items table for caching provider watchlists.

Revision ID: 010_watchlist_items
Revises: 009_episode_ratings
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "010_watchlist_items"
down_revision = "009_episode_ratings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing_tables = inspector.get_table_names()

    if "watchlist_items" not in existing_tables:
        op.create_table(
            "watchlist_items",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("imdb_id", sa.String(32), nullable=True),
            sa.Column("tmdb_id", sa.String(32), nullable=True),
            sa.Column("title", sa.String(512), nullable=True),
            sa.Column("item_type", sa.String(32), nullable=True),  # movie | show
            sa.Column("source", sa.String(32), nullable=True),     # simkl | mdblist | user
            sa.Column("added_at", sa.DateTime, nullable=True),
            sa.Column("synced_at", sa.DateTime, nullable=True),
            sa.UniqueConstraint("user_id", "imdb_id", name="uq_watchlist_user_imdb"),
        )


def downgrade() -> None:
    op.drop_table("watchlist_items")

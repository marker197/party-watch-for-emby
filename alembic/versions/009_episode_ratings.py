"""Add episode-level rating columns to user_ratings.

Revision ID: 009_episode_ratings
Revises: 008_user_rating
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "009_episode_ratings"
down_revision = "008_user_rating"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = [c["name"] for c in inspector.get_columns("user_ratings")]

    if "season_number" not in existing:
        op.add_column("user_ratings", sa.Column("season_number", sa.Integer(), nullable=True))
    if "episode_number" not in existing:
        op.add_column("user_ratings", sa.Column("episode_number", sa.Integer(), nullable=True))
    if "series_name" not in existing:
        op.add_column("user_ratings", sa.Column("series_name", sa.String(512), nullable=True))


def downgrade() -> None:
    op.drop_column("user_ratings", "series_name")
    op.drop_column("user_ratings", "episode_number")
    op.drop_column("user_ratings", "season_number")

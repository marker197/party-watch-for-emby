"""Add viewing_goals table.

Revision ID: 010_viewing_goals
Revises: 009_episode_ratings
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "010_viewing_goals"
down_revision = "009_episode_ratings"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa_inspect(conn)
    existing = inspector.get_table_names()

    if "viewing_goals" not in existing:
        op.create_table(
            "viewing_goals",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("title", sa.String(256), nullable=False),
            sa.Column("goal_type", sa.String(32), nullable=False),
            sa.Column("target_count", sa.Integer, nullable=True),
            sa.Column("target_imdb_id", sa.String(32), nullable=True),
            sa.Column("target_series", sa.String(256), nullable=True),
            sa.Column("deadline", sa.DateTime, nullable=True),
            sa.Column("item_types", sa.String(64), server_default="movie,episode"),
            sa.Column("current_count", sa.Integer, server_default="0"),
            sa.Column("completed", sa.Boolean, server_default="false"),
            sa.Column("created_at", sa.DateTime),
            sa.Column("completed_at", sa.DateTime, nullable=True),
        )


def downgrade():
    op.drop_table("viewing_goals")

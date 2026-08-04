"""Add source/imdb_id/tmdb_id to user_ratings + dismissed_rating_items table.

Revision ID: 008_user_rating
Revises: 007_simkl
"""

revision = "008_user_rating"
down_revision = "007_simkl"

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


def upgrade():
    conn = op.get_bind()
    inspector = inspect(conn)
    existing_cols = {c["name"] for c in inspector.get_columns("user_ratings")}

    if "source" not in existing_cols:
        op.add_column(
            "user_ratings",
            sa.Column("source", sa.String(32), server_default="imported", nullable=False),
        )
    if "imdb_id" not in existing_cols:
        op.add_column(
            "user_ratings",
            sa.Column("imdb_id", sa.String(32), nullable=True),
        )
    if "tmdb_id" not in existing_cols:
        op.add_column(
            "user_ratings",
            sa.Column("tmdb_id", sa.String(32), nullable=True),
        )

    existing_tables = inspector.get_table_names()
    if "dismissed_rating_items" not in existing_tables:
        op.create_table(
            "dismissed_rating_items",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "user_id",
                sa.Integer(),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("item_key", sa.String(128), nullable=False),
            sa.Column(
                "dismissed_at",
                sa.DateTime(),
                server_default=sa.func.now(),
            ),
            sa.UniqueConstraint("user_id", "item_key", name="uq_dismissed_rating_user_item"),
        )
        op.create_index("ix_dismissed_rating_user", "dismissed_rating_items", ["user_id"])


def downgrade():
    op.drop_table("dismissed_rating_items")
    op.drop_column("user_ratings", "tmdb_id")
    op.drop_column("user_ratings", "imdb_id")
    op.drop_column("user_ratings", "source")

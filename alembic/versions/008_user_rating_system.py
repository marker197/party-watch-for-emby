"""Add source/imdb_id/tmdb_id to user_ratings + dismissed_rating_items table.

Revision ID: 008
Revises: 007
"""

revision = "008"
down_revision = "007"

from alembic import op
import sqlalchemy as sa


def upgrade():
    # Add source column to distinguish user-submitted from imported ratings
    op.add_column(
        "user_ratings",
        sa.Column("source", sa.String(32), server_default="imported", nullable=False),
    )
    # Add provider ID columns for direct lookups (UserRating currently only has simkl_id)
    op.add_column(
        "user_ratings",
        sa.Column("imdb_id", sa.String(32), nullable=True),
    )
    op.add_column(
        "user_ratings",
        sa.Column("tmdb_id", sa.String(32), nullable=True),
    )

    # Table for dismissed rating prompts (items the user chose to skip)
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

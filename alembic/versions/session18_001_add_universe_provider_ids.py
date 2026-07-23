"""Add imdb_id and tmdb_id to universe_items for provider-ID matching.

Revision ID: session18_001
Revises: session17_001
"""
from alembic import op
import sqlalchemy as sa

revision = "session18_001"
down_revision = "session17_001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("universe_items", sa.Column("imdb_id", sa.String(16), nullable=True))
    op.add_column("universe_items", sa.Column("tmdb_id", sa.String(16), nullable=True))


def downgrade():
    op.drop_column("universe_items", "tmdb_id")
    op.drop_column("universe_items", "imdb_id")

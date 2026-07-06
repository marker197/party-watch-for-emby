"""initial schema

Revision ID: 186401ba8047
Revises: 
Create Date: 2026-06-29 18:08:49.381665

This migration creates all tables for emby-trakt-suite:
  - users (with Trakt OAuth tokens)
  - queue_items (Smart Queue)
  - user_ratings (ML training data cache)
  - predictions (ML Rating Predictor output)
  - ml_models (model version tracking)
  - universes + universe_items (Shared Universe Discovery)
  - watch_parties + watch_party_participants (Watch Party real-time sync)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '186401ba8047'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create users table
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('emby_user_id', sa.String(64), nullable=False),
        sa.Column('emby_username', sa.String(128), nullable=True),
        sa.Column('trakt_username', sa.String(128), nullable=True),
        sa.Column('trakt_access_token', sa.Text(), nullable=True),
        sa.Column('trakt_refresh_token', sa.Text(), nullable=True),
        sa.Column('trakt_token_expires', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('emby_user_id'),
    )
    op.create_index(op.f('ix_users_emby_user_id'), 'users', ['emby_user_id'], unique=True)

    # Create queue_items table (Smart Queue #1)
    op.create_table(
        'queue_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=False),
        sa.Column('title', sa.String(512), nullable=True),
        sa.Column('item_type', sa.String(32), nullable=True),
        sa.Column('source', sa.String(32), nullable=True),
        sa.Column('score', sa.Float(), nullable=True),
        sa.Column('trakt_trending_rank', sa.Integer(), nullable=True),
        sa.Column('trakt_rating', sa.Float(), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.Column('played', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create user_ratings table (ML Predictor #2 - caching)
    op.create_table(
        'user_ratings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('trakt_id', sa.String(64), nullable=False),
        sa.Column('trakt_slug', sa.String(256), nullable=True),
        sa.Column('title', sa.String(512), nullable=True),
        sa.Column('item_type', sa.String(32), nullable=True),
        sa.Column('rating', sa.Float(), nullable=False),
        sa.Column('genres', sa.JSON(), nullable=True),
        sa.Column('year', sa.Integer(), nullable=True),
        sa.Column('runtime', sa.Integer(), nullable=True),
        sa.Column('trakt_rating', sa.Float(), nullable=True),
        sa.Column('network', sa.String(128), nullable=True),
        sa.Column('rated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create predictions table (ML Predictor #2 - output)
    op.create_table(
        'predictions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=False),
        sa.Column('title', sa.String(512), nullable=True),
        sa.Column('predicted_rating', sa.Float(), nullable=False),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('explanation', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create ml_models table (version tracking)
    op.create_table(
        'ml_models',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('model_path', sa.String(512), nullable=True),
        sa.Column('training_samples', sa.Integer(), nullable=True),
        sa.Column('mae', sa.Float(), nullable=True),
        sa.Column('r2_score', sa.Float(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create universes table (Shared Universe Discovery #5)
    op.create_table(
        'universes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(256), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('universe_type', sa.String(32), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )

    # Create universe_items table
    op.create_table(
        'universe_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('universe_id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=True),
        sa.Column('title', sa.String(512), nullable=False),
        sa.Column('item_type', sa.String(32), nullable=True),
        sa.Column('watch_order', sa.Integer(), nullable=True),
        sa.Column('chronological_order', sa.Integer(), nullable=True),
        sa.Column('watched', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['universe_id'], ['universes.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create watch_parties table (Watch Party #4)
    op.create_table(
        'watch_parties',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('party_code', sa.String(16), nullable=False),
        sa.Column('creator_user_id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('ended_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['creator_user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('party_code'),
    )

    # Create watch_party_participants table
    op.create_table(
        'watch_party_participants',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('party_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('joined_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['party_id'], ['watch_parties.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('watch_party_participants')
    op.drop_table('watch_parties')
    op.drop_table('universe_items')
    op.drop_table('universes')
    op.drop_table('ml_models')
    op.drop_table('predictions')
    op.drop_table('user_ratings')
    op.drop_table('queue_items')
    op.drop_index(op.f('ix_users_emby_user_id'), table_name='users')
    op.drop_table('users')


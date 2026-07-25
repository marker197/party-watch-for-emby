"""Reconcile DB schema with current SQLAlchemy models.

Earlier migrations created older versions of several tables. The models
have since evolved, causing runtime errors like:
  column universes.slug does not exist

All affected tables are empty until a Trakt account is linked, so the
safe fix is drop + recreate to match the models exactly:
  - universes / universe_items
  - ml_models
  - watch_parties / watch_party_participants
Plus one additive change:
  - predictions.features_json

Revision ID: 0013_schema_reconcile
Revises: phase5_000001
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '0013_schema_reconcile'
down_revision: Union[str, Sequence[str], None] = 'phase5_000001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Shared Universe Discovery -------------------------------------
    op.drop_table('universe_items')
    op.drop_table('universes')

    op.create_table(
        'universes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(256), nullable=False),
        sa.Column('slug', sa.String(256), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('total_items', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
        sa.UniqueConstraint('slug'),
    )

    op.create_table(
        'universe_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('universe_id', sa.Integer(), nullable=False),
        sa.Column('trakt_id', sa.String(64), nullable=True),
        sa.Column('emby_item_id', sa.String(64), nullable=True),
        sa.Column('title', sa.String(512), nullable=False),
        sa.Column('item_type', sa.String(32), nullable=True),
        sa.Column('year', sa.Integer(), nullable=True),
        sa.Column('release_order', sa.Integer(), nullable=True),
        sa.Column('chronological_order', sa.Integer(), nullable=True),
        sa.Column('in_library', sa.Boolean(), nullable=True),
        sa.Column('watched', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['universe_id'], ['universes.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # --- ML Rating Predictor --------------------------------------------
    op.drop_table('ml_models')
    op.create_table(
        'ml_models',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=True),
        sa.Column('training_samples', sa.Integer(), nullable=True),
        sa.Column('mae', sa.Float(), nullable=True),
        sa.Column('r2', sa.Float(), nullable=True),
        sa.Column('model_path', sa.String(512), nullable=True),
        sa.Column('trained_at', sa.DateTime(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    op.add_column('predictions', sa.Column('features_json', sa.JSON(), nullable=True))

    # --- Watch Party ------------------------------------------------------
    op.drop_table('watch_party_participants')
    op.drop_table('watch_parties')

    op.create_table(
        'watch_parties',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('code', sa.String(8), nullable=False),
        sa.Column('host_user_id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=True),
        sa.Column('title', sa.String(512), nullable=True),
        sa.Column('status', sa.String(32), nullable=True),
        sa.Column('playback_position_ticks', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('ended_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['host_user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('code'),
    )

    op.create_table(
        'watch_party_participants',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('party_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('joined_at', sa.DateTime(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['party_id'], ['watch_parties.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    # Downgrade not supported for schema reconcile (tables were empty).
    pass

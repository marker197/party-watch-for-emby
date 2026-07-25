"""Add Phase 5 tables: social_watching, library_gaps, enriched_metadata, library_health_report, bulk_actions

Revision ID: phase5_000001
Revises: 0012_add_rate_limit_configs
Create Date: 2026-06-30 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = 'phase5_000001'
down_revision = '0012_add_rate_limit_configs'
branch_labels = None
depends_on = None


def upgrade():
    # Table 1: social_watching
    op.create_table(
        'social_watching',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('friend_trakt_username', sa.String(128), nullable=False),
        sa.Column('friend_profile_url', sa.String(256)),
        sa.Column('is_watching', sa.Boolean(), default=False),
        sa.Column('current_item_title', sa.String(512)),
        sa.Column('current_item_trakt_id', sa.String(64)),
        sa.Column('item_type', sa.String(32)),  # 'movie' | 'episode'
        sa.Column('started_at', sa.DateTime()),
        sa.Column('last_seen_at', sa.DateTime()),
        sa.Column('in_library', sa.Boolean(), default=False),
        sa.Column('friend_rating', sa.Float()),
        sa.Column('influence_score', sa.Float(), default=0.0),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_social_watching_user_friend', 'social_watching', ['user_id', 'friend_trakt_username'])
    op.create_index('idx_social_watching_user', 'social_watching', ['user_id'])

    # Table 2: library_gaps
    op.create_table(
        'library_gaps',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('gap_type', sa.String(32), nullable=False),
        sa.Column('title', sa.String(512)),
        sa.Column('emby_item_id', sa.String(64)),
        sa.Column('trakt_id', sa.String(64)),
        sa.Column('trakt_slug', sa.String(256)),
        sa.Column('description', sa.Text()),
        sa.Column('gap_details', postgresql.JSON()),
        sa.Column('priority', sa.String(32), default='medium'),
        sa.Column('status', sa.String(32), default='open'),
        sa.Column('user_rating', sa.Float()),
        sa.Column('detected_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('dismissed_at', sa.DateTime()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_library_gaps_user_type', 'library_gaps', ['user_id', 'gap_type'])
    op.create_index('idx_library_gaps_user_priority', 'library_gaps', ['user_id', 'priority'])

    # Table 3: enriched_metadata
    op.create_table(
        'enriched_metadata',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('emby_item_id', sa.String(64), nullable=False, unique=True),
        sa.Column('trakt_id', sa.String(64)),
        sa.Column('trakt_slug', sa.String(256)),
        sa.Column('title', sa.String(512)),
        sa.Column('tagline', sa.String(512)),
        sa.Column('themes', postgresql.JSON()),
        sa.Column('quotes', postgresql.JSON()),
        sa.Column('social_score', sa.Float()),
        sa.Column('trakt_rating', sa.Float()),
        sa.Column('trakt_votes', sa.Integer()),
        sa.Column('themes_from_trakt', sa.Boolean(), default=False),
        sa.Column('enriched_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('expires_at', sa.DateTime()),
        sa.Column('metadata_json', postgresql.JSON()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_enriched_metadata_emby_id', 'enriched_metadata', ['emby_item_id'])
    op.create_index('idx_enriched_metadata_expires', 'enriched_metadata', ['expires_at'])

    # Table 4: library_health_report
    op.create_table(
        'library_health_report',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('total_items', sa.Integer()),
        sa.Column('unwatched_items', sa.Integer()),
        sa.Column('incomplete_series', sa.Integer()),
        sa.Column('orphaned_episodes', sa.Integer()),
        sa.Column('related_missing', sa.Integer()),
        sa.Column('missing_acclaimed', sa.Integer()),
        sa.Column('series_completion_pct', sa.Float()),
        sa.Column('acquisition_cost_estimate', sa.Integer()),
        sa.Column('report_json', postgresql.JSON()),
        sa.Column('generated_at', sa.DateTime(), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_library_health_user_date', 'library_health_report', ['user_id', 'generated_at'])

    # Table 5: bulk_actions
    op.create_table(
        'bulk_actions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('action_type', sa.String(32)),
        sa.Column('item_ids', postgresql.JSON()),
        sa.Column('status', sa.String(32), default='pending'),
        sa.Column('result_json', postgresql.JSON()),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('completed_at', sa.DateTime()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_bulk_actions_user_status', 'bulk_actions', ['user_id', 'status'])


def downgrade():
    # Drop tables in reverse order
    op.drop_table('bulk_actions')
    op.drop_table('library_health_report')
    op.drop_table('enriched_metadata')
    op.drop_table('library_gaps')
    op.drop_table('social_watching')

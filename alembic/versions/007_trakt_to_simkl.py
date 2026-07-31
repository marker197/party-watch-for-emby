"""Rename Trakt columns to Simkl and drop refresh_token.

Revision ID: 007
Revises: 006
"""
from alembic import op

revision = "007_simkl"
down_revision = "006_dedup_watch_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- Users table: rename auth columns, drop refresh_token --
    op.execute("ALTER TABLE users RENAME COLUMN trakt_username TO simkl_username")
    op.execute("ALTER TABLE users RENAME COLUMN trakt_access_token TO simkl_access_token")
    op.execute("ALTER TABLE users RENAME COLUMN trakt_token_expires TO simkl_token_expires")
    # Drop refresh_token (Simkl uses 5-year tokens, no refresh)
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS trakt_refresh_token")

    # -- queue_items: rename trakt columns --
    op.execute("ALTER TABLE queue_items RENAME COLUMN trakt_trending_rank TO simkl_trending_rank")
    op.execute("ALTER TABLE queue_items RENAME COLUMN trakt_rating TO simkl_rating")

    # -- queue_blocklist: rename trakt_id --
    op.execute("ALTER TABLE queue_blocklist RENAME COLUMN trakt_id TO simkl_id")
    # Rename constraint (safe even if doesn't exist)
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE queue_blocklist RENAME CONSTRAINT uq_blocklist_user_trakt TO uq_blocklist_user_simkl;
        EXCEPTION WHEN undefined_object THEN NULL;
        END $$
    """)

    # -- user_ratings: rename trakt columns --
    op.execute("ALTER TABLE user_ratings RENAME COLUMN trakt_id TO simkl_id")
    op.execute("ALTER TABLE user_ratings RENAME COLUMN trakt_slug TO simkl_slug")
    op.execute("ALTER TABLE user_ratings RENAME COLUMN trakt_rating TO simkl_rating")

    # -- universe_items: rename trakt_id --
    op.execute("ALTER TABLE universe_items RENAME COLUMN trakt_id TO simkl_id")

    # -- library_gaps: rename trakt columns --
    op.execute("ALTER TABLE library_gaps RENAME COLUMN trakt_id TO simkl_id")
    op.execute("ALTER TABLE library_gaps RENAME COLUMN trakt_slug TO simkl_slug")

    # -- watch_history: rename trakt_id --
    op.execute("ALTER TABLE watch_history RENAME COLUMN trakt_id TO simkl_id")
    # Update source values
    op.execute("UPDATE watch_history SET source = 'backfill_simkl' WHERE source = 'backfill_trakt'")

    # -- dismissed_rewatch_items: update item_key prefix --
    op.execute("UPDATE dismissed_rewatch_items SET item_key = REPLACE(item_key, 'trakt:', 'simkl:') WHERE item_key LIKE 'trakt:%'")

    # -- dismissed_health_items: no trakt-specific columns to rename --

    # -- social_watching: rename if exists, or drop --
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE social_watching RENAME COLUMN friend_trakt_username TO friend_username;
        EXCEPTION WHEN undefined_table THEN NULL;
                  WHEN undefined_column THEN NULL;
        END $$
    """)
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE social_watching RENAME COLUMN current_item_trakt_id TO current_item_simkl_id;
        EXCEPTION WHEN undefined_table THEN NULL;
                  WHEN undefined_column THEN NULL;
        END $$
    """)

    # -- app_settings: update integration_provider values --
    op.execute("UPDATE app_settings SET value = 'simkl' WHERE key = 'integration_provider' AND value = 'trakt'")
    op.execute("UPDATE app_settings SET value = 'simkl' WHERE key = 'integration_provider' AND value = 'both'")


def downgrade() -> None:
    pass  # One-way migration

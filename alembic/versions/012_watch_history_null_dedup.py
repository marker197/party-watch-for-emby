"""Add partial unique index on watch_history for NULL emby_id rows.

PostgreSQL unique constraints treat NULL as always distinct, so the
existing uq_watch_history_user_item_time constraint on
(user_id, emby_id, watched_at) doesn't catch duplicate backfill
entries where emby_id is NULL.

This migration adds a partial unique index:
  (user_id, title, watched_at::date) WHERE emby_id IS NULL

so two backfill entries for the same user+title on the same calendar
day are rejected at the DB level.

Revision ID: 012_watch_history_null_dedup
Revises: 011_dismissed_issues
"""

from alembic import op
from sqlalchemy import text as sa_text

revision = "012_watch_history_null_dedup"
down_revision = "011_dismissed_issues"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # Check if the index already exists (idempotent)
    result = bind.execute(sa_text(
        "SELECT 1 FROM pg_indexes WHERE indexname = 'ix_watch_history_null_emby_dedup'"
    ))
    if result.fetchone():
        return  # already exists

    # Remove existing duplicates before creating the unique index.
    # Keep the row with the highest id (most recently inserted) for each
    # (user_id, lower(title), watched_at::date) group where emby_id IS NULL.
    bind.execute(sa_text("""
        DELETE FROM watch_history
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY user_id, lower(title), watched_at::date
                           ORDER BY id DESC
                       ) AS rn
                FROM watch_history
                WHERE emby_id IS NULL
            ) ranked
            WHERE rn > 1
        )
    """))

    # Create partial unique index
    op.execute(sa_text("""
        CREATE UNIQUE INDEX ix_watch_history_null_emby_dedup
        ON watch_history (user_id, lower(title), (watched_at::date))
        WHERE emby_id IS NULL
    """))


def downgrade() -> None:
    op.execute(sa_text("DROP INDEX IF EXISTS ix_watch_history_null_emby_dedup"))

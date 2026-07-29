"""Deduplicate same-day watch_history rows.

Collapses multiple rows for the same item on the same calendar day into one,
keeping the row with the highest progress (ties broken by lowest id).

Two passes:
  A) Rows WITH emby_id — grouped by (user_id, emby_id, date)
  B) Rows WITHOUT emby_id — grouped by (user_id, item_type, title/series+ep, date)

Safe on empty tables (new installs) — DELETE with NOT IN on zero rows is a no-op.

Revision ID: 006_dedup_watch_history
Revises: 005_watch_history_progress
Create Date: 2026-07-29
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text as sa_text

revision = "006_dedup_watch_history"
down_revision = "005_watch_history_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "watch_history" not in inspector.get_table_names():
        return

    # ── Pass A: rows WITH emby_id ─────────────────────────────────────
    # Keep one row per (user_id, emby_id, calendar day).
    # Prefer highest progress, then lowest id.
    result_a = conn.execute(sa_text("""
        WITH keep AS (
            SELECT DISTINCT ON (user_id, emby_id, watched_at::date) id
            FROM watch_history
            WHERE emby_id IS NOT NULL
            ORDER BY user_id, emby_id, watched_at::date,
                     COALESCE(progress, -1) DESC,
                     id ASC
        )
        DELETE FROM watch_history
        WHERE emby_id IS NOT NULL
          AND id NOT IN (SELECT id FROM keep)
    """))
    removed_a = result_a.rowcount

    # ── Pass B: rows WITHOUT emby_id (backfill sources) ───────────────
    # Episodes: group by (user_id, series_name, season, episode, day)
    # Movies:   group by (user_id, imdb_id or title, day)
    # Combined into one query using a composite key that covers both.
    result_b = conn.execute(sa_text("""
        WITH keep AS (
            SELECT DISTINCT ON (
                user_id,
                item_type,
                LOWER(COALESCE(series_name, '')),
                COALESCE(season_number, -1),
                COALESCE(episode_number, -1),
                LOWER(COALESCE(title, '')),
                watched_at::date
            ) id
            FROM watch_history
            WHERE emby_id IS NULL
            ORDER BY user_id,
                     item_type,
                     LOWER(COALESCE(series_name, '')),
                     COALESCE(season_number, -1),
                     COALESCE(episode_number, -1),
                     LOWER(COALESCE(title, '')),
                     watched_at::date,
                     COALESCE(progress, -1) DESC,
                     id ASC
        )
        DELETE FROM watch_history
        WHERE emby_id IS NULL
          AND id NOT IN (SELECT id FROM keep)
    """))
    removed_b = result_b.rowcount

    total = removed_a + removed_b
    if total:
        print(f"    dedup: removed {removed_a} (with emby_id) + {removed_b} (backfill) = {total} duplicate rows")


def downgrade() -> None:
    # Data deletion is irreversible — nothing to undo.
    pass

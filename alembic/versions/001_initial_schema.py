"""Initial schema — all tables for Emby-Trakt Suite v2.

Revision ID: 001_initial
Revises: None

Squashed from 18 incremental migrations (sessions 1–66).
Tables dropped during development (enriched_metadata, queue_weight_snapshots,
rate_limit_configs) are excluded.
"""

import sqlalchemy as sa
from alembic import op

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Users ──
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("emby_user_id", sa.String(64), unique=True, nullable=False, index=True),
        sa.Column("emby_username", sa.String(128)),
        sa.Column("trakt_username", sa.String(128)),
        sa.Column("trakt_access_token", sa.Text()),
        sa.Column("trakt_refresh_token", sa.Text()),
        sa.Column("trakt_token_expires", sa.DateTime()),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime()),
    )

    # ── Smart Queue ──
    op.create_table(
        "queue_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("emby_item_id", sa.String(64)),
        sa.Column("title", sa.String(512)),
        sa.Column("item_type", sa.String(32)),
        sa.Column("source", sa.String(32)),
        sa.Column("score", sa.Float(), default=0.0),
        sa.Column("trakt_trending_rank", sa.Integer()),
        sa.Column("trakt_rating", sa.Float()),
        sa.Column("metadata_json", sa.JSON()),
        sa.Column("in_library", sa.Boolean(), default=True),
        sa.Column("played", sa.Boolean(), default=False),
        sa.Column("played_at", sa.DateTime()),
        sa.Column("played_duration_ticks", sa.Integer()),
        sa.Column("created_at", sa.DateTime()),
    )

    op.create_table(
        "queue_blocklist",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("trakt_id", sa.String(64), nullable=False),
        sa.Column("title", sa.String(512)),
        sa.Column("item_type", sa.String(32)),
        sa.Column("blocked_at", sa.DateTime()),
        sa.UniqueConstraint("user_id", "trakt_id", name="uq_blocklist_user_trakt"),
    )

    # ── ML Rating Predictor ──
    op.create_table(
        "user_ratings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trakt_id", sa.String(64), nullable=False),
        sa.Column("trakt_slug", sa.String(256)),
        sa.Column("title", sa.String(512)),
        sa.Column("item_type", sa.String(32)),
        sa.Column("rating", sa.Float(), nullable=False),
        sa.Column("genres", sa.JSON()),
        sa.Column("year", sa.Integer()),
        sa.Column("runtime", sa.Integer()),
        sa.Column("trakt_rating", sa.Float()),
        sa.Column("network", sa.String(128)),
        sa.Column("rated_at", sa.DateTime()),
    )

    op.create_table(
        "predictions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("emby_item_id", sa.String(64), nullable=False),
        sa.Column("title", sa.String(512)),
        sa.Column("predicted_rating", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float()),
        sa.Column("explanation", sa.Text()),
        sa.Column("overview", sa.Text()),
        sa.Column("features_json", sa.JSON()),
        sa.Column("created_at", sa.DateTime()),
    )

    op.create_table(
        "ml_models",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version", sa.Integer(), default=1),
        sa.Column("training_samples", sa.Integer()),
        sa.Column("mae", sa.Float()),
        sa.Column("r2", sa.Float()),
        sa.Column("feature_count", sa.Integer()),
        sa.Column("model_path", sa.String(512)),
        sa.Column("trained_at", sa.DateTime()),
        sa.Column("is_active", sa.Boolean(), default=True),
    )

    # ── Shared Universe Discovery ──
    op.create_table(
        "universes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(256), unique=True, nullable=False),
        sa.Column("slug", sa.String(256), unique=True, nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("total_items", sa.Integer(), default=0),
        sa.Column("is_custom", sa.Boolean(), default=False),
        sa.Column("playlist_enabled", sa.Boolean(), default=False),
        sa.Column("custom_name", sa.String(256)),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime()),
    )

    op.create_table(
        "universe_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("universe_id", sa.Integer(), sa.ForeignKey("universes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("trakt_id", sa.String(64)),
        sa.Column("imdb_id", sa.String(16)),
        sa.Column("tmdb_id", sa.String(16)),
        sa.Column("emby_item_id", sa.String(64)),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("item_type", sa.String(32)),
        sa.Column("year", sa.Integer()),
        sa.Column("release_order", sa.Integer()),
        sa.Column("chronological_order", sa.Integer()),
        sa.Column("in_library", sa.Boolean(), default=False),
        sa.Column("watched", sa.Boolean(), default=False),
    )

    # ── Watch Party ──
    op.create_table(
        "watch_parties",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("code", sa.String(8), unique=True, nullable=False),
        sa.Column("host_user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("emby_item_id", sa.String(64)),
        sa.Column("title", sa.String(512)),
        sa.Column("status", sa.String(32), default="waiting"),
        sa.Column("playback_position_ticks", sa.Integer(), default=0),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("ended_at", sa.DateTime()),
    )

    op.create_table(
        "watch_party_participants",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("joined_at", sa.DateTime()),
        sa.Column("is_active", sa.Boolean(), default=True),
    )

    op.create_table(
        "watch_party_reactions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id")),
        sa.Column("emoji", sa.String(16), nullable=False),
        sa.Column("position_ticks", sa.Integer(), default=0),
        sa.Column("created_at", sa.DateTime()),
    )

    op.create_table(
        "watch_party_comments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id")),
        sa.Column("username", sa.String(128)),
        sa.Column("comment_text", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime()),
    )

    # ── Rating Bias Detector ──
    op.create_table(
        "rating_biases",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("total_ratings", sa.Integer()),
        sa.Column("analysis_json", sa.JSON()),
        sa.Column("analyzed_at", sa.DateTime()),
    )

    # ── Social Watching Graph ──
    op.create_table(
        "social_watching",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("friend_trakt_username", sa.String(128), nullable=False),
        sa.Column("friend_profile_url", sa.String(256)),
        sa.Column("is_watching", sa.Boolean(), default=False),
        sa.Column("current_item_title", sa.String(512)),
        sa.Column("current_item_trakt_id", sa.String(64)),
        sa.Column("item_type", sa.String(32)),
        sa.Column("started_at", sa.DateTime()),
        sa.Column("last_seen_at", sa.DateTime()),
        sa.Column("in_library", sa.Boolean(), default=False),
        sa.Column("friend_rating", sa.Float()),
        sa.Column("influence_score", sa.Float(), default=0.0),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime()),
    )

    # ── Library Health Monitor ──
    op.create_table(
        "library_gaps",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("gap_type", sa.String(32), nullable=False),
        sa.Column("title", sa.String(512)),
        sa.Column("emby_item_id", sa.String(64)),
        sa.Column("trakt_id", sa.String(64)),
        sa.Column("trakt_slug", sa.String(256)),
        sa.Column("description", sa.Text()),
        sa.Column("gap_details", sa.JSON()),
        sa.Column("priority", sa.String(32), default="medium"),
        sa.Column("status", sa.String(32), default="open"),
        sa.Column("user_rating", sa.Float()),
        sa.Column("detected_at", sa.DateTime()),
        sa.Column("dismissed_at", sa.DateTime()),
    )

    op.create_table(
        "library_health_report",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("total_items", sa.Integer()),
        sa.Column("unwatched_items", sa.Integer()),
        sa.Column("incomplete_series", sa.Integer()),
        sa.Column("orphaned_episodes", sa.Integer()),
        sa.Column("related_missing", sa.Integer()),
        sa.Column("missing_acclaimed", sa.Integer()),
        sa.Column("series_completion_pct", sa.Float()),
        sa.Column("acquisition_cost_estimate", sa.Integer()),
        sa.Column("report_json", sa.JSON()),
        sa.Column("generated_at", sa.DateTime()),
    )

    op.create_table(
        "dismissed_health_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("item_type", sa.String(16), nullable=False),
        sa.Column("item_id", sa.String(64), nullable=False),
        sa.Column("dismissed_at", sa.DateTime()),
    )
    op.create_index(
        "ix_dismissed_health_user_type_id",
        "dismissed_health_items",
        ["user_id", "item_type", "item_id"],
        unique=True,
    )

    # ── App Settings ──
    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(128), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime()),
    )

    # ── Bulk Actions ──
    op.create_table(
        "bulk_actions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("action_type", sa.String(32)),
        sa.Column("item_ids", sa.JSON()),
        sa.Column("status", sa.String(32), default="pending"),
        sa.Column("result_json", sa.JSON()),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("completed_at", sa.DateTime()),
    )


def downgrade() -> None:
    op.drop_table("bulk_actions")
    op.drop_table("app_settings")
    op.drop_index("ix_dismissed_health_user_type_id", table_name="dismissed_health_items")
    op.drop_table("dismissed_health_items")
    op.drop_table("library_health_report")
    op.drop_table("library_gaps")
    op.drop_table("social_watching")
    op.drop_table("rating_biases")
    op.drop_table("watch_party_comments")
    op.drop_table("watch_party_reactions")
    op.drop_table("watch_party_participants")
    op.drop_table("watch_parties")
    op.drop_table("universe_items")
    op.drop_table("universes")
    op.drop_table("ml_models")
    op.drop_table("predictions")
    op.drop_table("user_ratings")
    op.drop_table("queue_blocklist")
    op.drop_table("queue_items")
    op.drop_table("users")

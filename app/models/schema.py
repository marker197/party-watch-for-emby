"""Database models shared across all services."""

from datetime import datetime
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, JSON,
)
from sqlalchemy.orm import relationship

from app.utils.database import Base


# ---------------------------------------------------------------------------
# User / Auth
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    emby_user_id = Column(String(64), unique=True, nullable=False, index=True)
    emby_username = Column(String(128))
    trakt_username = Column(String(128))
    trakt_access_token = Column(Text)
    trakt_refresh_token = Column(Text)
    trakt_token_expires = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # relationships
    ratings = relationship("UserRating", back_populates="user", cascade="all, delete-orphan")
    predictions = relationship("Prediction", back_populates="user", cascade="all, delete-orphan")
    queue_items = relationship("QueueItem", back_populates="user", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# Smart Queue  (#1)
# ---------------------------------------------------------------------------

class QueueItem(Base):
    __tablename__ = "queue_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    emby_item_id = Column(String(64), nullable=False)
    title = Column(String(512))
    item_type = Column(String(32))   # movie | episode
    source = Column(String(32))      # watchlist | trending | friend | calendar
    score = Column(Float, default=0.0)
    trakt_trending_rank = Column(Integer)
    trakt_rating = Column(Float)
    metadata_json = Column(JSON)
    played = Column(Boolean, default=False)
    played_at = Column(DateTime)
    played_duration_ticks = Column(Integer)  # how long they watched
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="queue_items")


# ---------------------------------------------------------------------------
# ML Rating Predictor  (#2)
# ---------------------------------------------------------------------------

class UserRating(Base):
    """Cached copy of a user's Trakt ratings for ML training."""
    __tablename__ = "user_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    trakt_id = Column(String(64), nullable=False)
    trakt_slug = Column(String(256))
    title = Column(String(512))
    item_type = Column(String(32))
    rating = Column(Float, nullable=False)
    genres = Column(JSON)           # ["sci-fi","drama"]
    year = Column(Integer)
    runtime = Column(Integer)       # minutes
    trakt_rating = Column(Float)    # community rating
    network = Column(String(128))
    rated_at = Column(DateTime)

    user = relationship("User", back_populates="ratings")


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    emby_item_id = Column(String(64), nullable=False)
    title = Column(String(512))
    predicted_rating = Column(Float, nullable=False)
    confidence = Column(Float)
    explanation = Column(Text)       # human-readable reason
    features_json = Column(JSON)     # raw feature vector for debugging
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="predictions")


class MLModel(Base):
    """Track which model version is active per user."""
    __tablename__ = "ml_models"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, default=1)
    training_samples = Column(Integer)
    mae = Column(Float)              # mean absolute error on validation
    r2 = Column(Float)
    feature_count = Column(Integer)   # number of features model was trained with
    model_path = Column(String(512)) # /app/models/{user_id}_v{version}.pkl
    trained_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)


# ---------------------------------------------------------------------------
# Shared Universe Discovery  (#3)
# ---------------------------------------------------------------------------

class Universe(Base):
    __tablename__ = "universes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(256), unique=True, nullable=False)
    slug = Column(String(256), unique=True, nullable=False)
    description = Column(Text)
    total_items = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    items = relationship("UniverseItem", back_populates="universe", cascade="all, delete-orphan")


class UniverseItem(Base):
    __tablename__ = "universe_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    universe_id = Column(Integer, ForeignKey("universes.id", ondelete="CASCADE"), nullable=False)
    trakt_id = Column(String(64))
    imdb_id = Column(String(16))      # e.g. "tt0371746"
    tmdb_id = Column(String(16))      # e.g. "1726"
    emby_item_id = Column(String(64))
    title = Column(String(512), nullable=False)
    item_type = Column(String(32))   # movie | show
    year = Column(Integer)
    release_order = Column(Integer)
    chronological_order = Column(Integer)
    in_library = Column(Boolean, default=False)
    watched = Column(Boolean, default=False)

    universe = relationship("Universe", back_populates="items")


# ---------------------------------------------------------------------------
# Watch Party  (#4)
# ---------------------------------------------------------------------------

class WatchParty(Base):
    __tablename__ = "watch_parties"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(8), unique=True, nullable=False)  # join code
    host_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    emby_item_id = Column(String(64))
    title = Column(String(512))
    status = Column(String(32), default="waiting")  # waiting | playing | paused | ended
    playback_position_ticks = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime)

    participants = relationship("WatchPartyParticipant", back_populates="party", cascade="all, delete-orphan")


class WatchPartyParticipant(Base):
    __tablename__ = "watch_party_participants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    party_id = Column(Integer, ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    joined_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

    party = relationship("WatchParty", back_populates="participants")


# ---------------------------------------------------------------------------
# Rating Bias Detector  (#10)
# ---------------------------------------------------------------------------

class RatingBias(Base):
    """Analysis of user's rating patterns, biases, and blind spots."""
    __tablename__ = "rating_biases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True)
    total_ratings = Column(Integer)
    analysis_json = Column(JSON)  # full bias report
    analyzed_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ---------------------------------------------------------------------------
# Trakt Social Watching Graph  (#6)
# ---------------------------------------------------------------------------

class SocialWatching(Base):
    """Track what friends are watching and influence scoring."""
    __tablename__ = "social_watching"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    friend_trakt_username = Column(String(128), nullable=False)
    friend_profile_url = Column(String(256))
    is_watching = Column(Boolean, default=False)
    current_item_title = Column(String(512))
    current_item_trakt_id = Column(String(64))
    item_type = Column(String(32))  # 'movie' | 'episode'
    started_at = Column(DateTime)
    last_seen_at = Column(DateTime)
    in_library = Column(Boolean, default=False)
    friend_rating = Column(Float)
    influence_score = Column(Float, default=0.0)  # 0-100, % of overlap
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", foreign_keys=[user_id])


# ---------------------------------------------------------------------------
# Library Health Monitor  (#9)
# ---------------------------------------------------------------------------

class LibraryGap(Base):
    """Detected gaps and issues in user's library."""
    __tablename__ = "library_gaps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    gap_type = Column(String(32), nullable=False)  # 'incomplete_series' | 'orphaned_episode' | 'missing_sequel' | 'director_gap'
    title = Column(String(512))
    emby_item_id = Column(String(64))
    trakt_id = Column(String(64))
    trakt_slug = Column(String(256))
    description = Column(Text)
    gap_details = Column(JSON)  # series_id, episodes_missing, director_name, etc.
    priority = Column(String(32), default='medium')  # low | medium | high | critical
    status = Column(String(32), default='open')  # open | dismissed | acquired
    user_rating = Column(Float)
    detected_at = Column(DateTime, default=datetime.utcnow)
    dismissed_at = Column(DateTime)

    user = relationship("User", foreign_keys=[user_id])


class LibraryHealthReport(Base):
    """Overall health analysis of user's library."""
    __tablename__ = "library_health_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    total_items = Column(Integer)
    unwatched_items = Column(Integer)
    incomplete_series = Column(Integer)
    orphaned_episodes = Column(Integer)
    related_missing = Column(Integer)
    missing_acclaimed = Column(Integer)
    series_completion_pct = Column(Float)
    acquisition_cost_estimate = Column(Integer)
    report_json = Column(JSON)  # full analysis blob
    generated_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", foreign_keys=[user_id])


# ---------------------------------------------------------------------------
# Metadata Enrichment  (#12)
# ---------------------------------------------------------------------------

class EnrichedMetadata(Base):
    """Enriched metadata from Trakt for Emby items."""
    __tablename__ = "enriched_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    emby_item_id = Column(String(64), unique=True, nullable=False, index=True)
    trakt_id = Column(String(64))
    trakt_slug = Column(String(256))
    title = Column(String(512))
    tagline = Column(String(512))
    themes = Column(JSON)  # ["sci-fi", "dystopian", "adventure"]
    quotes = Column(JSON)  # ["Quote 1", "Quote 2", ...]
    social_score = Column(Float)  # 0-1 trending score
    trakt_rating = Column(Float)  # community rating
    trakt_votes = Column(Integer)
    themes_from_trakt = Column(Boolean, default=False)
    enriched_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)  # refresh every 30 days
    metadata_json = Column(JSON)  # full enriched metadata blob


# ---------------------------------------------------------------------------
# Bulk Actions  (UI Feature)
# ---------------------------------------------------------------------------

class BulkAction(Base):
    """Track bulk operations on library items."""
    __tablename__ = "bulk_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    action_type = Column(String(32))  # 'delete' | 'rate_batch' | 'export' | 'add_collection'
    item_ids = Column(JSON)  # array of emby IDs
    status = Column(String(32), default='pending')  # pending | in_progress | completed | failed
    result_json = Column(JSON)  # results/errors
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

    user = relationship("User", foreign_keys=[user_id])

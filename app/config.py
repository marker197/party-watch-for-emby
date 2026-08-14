"""Centralised configuration loaded from environment variables.

Zero-config mode: the app can start with NO .env file at all.
Infrastructure (DB, Redis) uses baked-in defaults.
Emby + integration credentials can be empty at startup and collected
via the setup wizard on first run.
"""

import os
import secrets
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator


# ---------------------------------------------------------------------------
# Auto-generate JWT secret on first run if not provided
# ---------------------------------------------------------------------------
_JWT_SECRET_PATH = "/app/cache/.jwt_secret"


def _get_or_create_jwt_secret() -> str:
    """Return a JWT secret, creating one on first run if needed.

    Priority: env var > persisted file > generate new.
    The generated secret is written to a file inside the persistent
    /app/cache volume so it survives container restarts.
    """
    env_val = os.environ.get("JWT_SECRET_KEY", "").strip()
    if env_val:
        return env_val

    try:
        with open(_JWT_SECRET_PATH, "r") as f:
            stored = f.read().strip()
            if stored:
                return stored
    except FileNotFoundError:
        pass

    new_secret = secrets.token_hex(32)
    try:
        os.makedirs(os.path.dirname(_JWT_SECRET_PATH), exist_ok=True)
        with open(_JWT_SECRET_PATH, "w") as f:
            f.write(new_secret)
    except OSError:
        pass  # If we can't persist, use ephemeral — next restart will regen
    return new_secret


class Settings(BaseSettings):
    """All env vars loaded here. .env file or environment variables.

    Zero-config: everything has a sensible default except Emby + integrations,
    which are collected via the setup wizard on first run.
    """

    # -- Simkl (optional) ---------------------------------------------------
    simkl_client_id: str = Field(default="", alias="SIMKL_CLIENT_ID")

    # -- MDBList (optional) -------------------------------------------------
    mdblist_client_id: str = Field(default="", alias="MDBLIST_CLIENT_ID")
    mdblist_client_secret: str = Field(default="", alias="MDBLIST_CLIENT_SECRET")

    # -- Emby (collected via setup wizard if empty) -------------------------
    emby_url: str = Field(default="", alias="EMBY_URL")
    emby_api_key: str = Field(default="", alias="EMBY_API_KEY")

    # -- Redis (baked defaults — internal Docker network) -------------------
    redis_url: str = Field(
        default="redis://redis:6379/0",
        alias="REDIS_URL",
    )
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")

    # -- Postgres (baked defaults — internal Docker network) ----------------
    database_url: str = Field(
        default="postgresql+asyncpg://embysimkl:embysimkl-internal-only@postgres:5432/embysimkl",
        alias="DATABASE_URL",
    )
    db_user: str = Field(default="embysimkl", alias="DB_USER")
    db_password: str = Field(
        default="embysimkl-internal-only",
        alias="DB_PASSWORD",
    )
    db_name: str = Field(default="embysimkl", alias="DB_NAME")

    # -- Feature toggles ----------------------------------------------------
    enable_smart_queue: bool = Field(default=True, alias="ENABLE_SMART_QUEUE")
    enable_ml_predictor: bool = Field(default=True, alias="ENABLE_ML_PREDICTOR")
    enable_universe_discovery: bool = Field(
        default=True, alias="ENABLE_UNIVERSE_DISCOVERY"
    )
    enable_watch_party: bool = Field(default=True, alias="ENABLE_WATCH_PARTY")
    enable_rating_bias_detector: bool = Field(
        default=True, alias="ENABLE_RATING_BIAS_DETECTOR"
    )

    # -- Scheduler cron expressions -----------------------------------------
    smart_queue_cron: str = Field(default="0 2 * * *", alias="SMART_QUEUE_CRON")
    universe_scan_cron: str = Field(default="0 3 * * 0", alias="UNIVERSE_SCAN_CRON")
    ml_retrain_cron: str = Field(default="0 4 * * 1", alias="ML_RETRAIN_CRON")

    # -- SSL monitoring (optional) ------------------------------------------
    ssl_domain: str = Field(default="", alias="SSL_DOMAIN")

    # -- Security & Logging -------------------------------------------------
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: str = Field(
        default="/app/logs/emby-simkl-suite.log", alias="LOG_FILE"
    )
    log_max_bytes: int = Field(default=10_485_760, alias="LOG_MAX_BYTES")
    log_backup_count: int = Field(default=5, alias="LOG_BACKUP_COUNT")
    jwt_secret_key: str = Field(
        default_factory=_get_or_create_jwt_secret,
        alias="JWT_SECRET_KEY",
    )
    allowed_origins: str = Field(
        default="http://localhost:8000",
        alias="ALLOWED_ORIGINS",
    )
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")

    model_config = {"env_file": ".env", "extra": "ignore", "case_sensitive": False}

    # -- Validators ---------------------------------------------------------

    @field_validator("db_password")
    @classmethod
    def validate_db_password(cls, v: str, info) -> str:
        """DB password must not be empty or trivially weak."""
        weak = {"changeme", "password", "secret", "default", "test", ""}
        if not v or v.strip() == "" or v.lower() in weak:
            field_name = info.field_name
            raise ValueError(
                f"{field_name} contains a weak/default value. "
                "Use a strong, unique secret."
            )
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of: {valid_levels}")
        return v.upper()

    @field_validator("allowed_origins")
    @classmethod
    def validate_origins(cls, v: str) -> str:
        origins = [o.strip() for o in v.split(",")]
        for origin in origins:
            if not origin.startswith(("http://", "https://")):
                raise ValueError(
                    f"Invalid origin: {origin} — must start with http:// or https://"
                )
        return v

    def get_allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    @property
    def setup_required(self) -> bool:
        """True if Emby credentials haven't been configured yet."""
        return not self.emby_url or not self.emby_api_key


settings = Settings()

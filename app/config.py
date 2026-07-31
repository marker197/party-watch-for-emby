"""Centralised configuration loaded from environment variables (SECURITY HARDENED)."""

from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
import logging


class Settings(BaseSettings):
    """All env vars loaded here. .env file or environment variables only.
    
    SECURITY: No weak default values for secrets - must be explicitly set.
    """

    # -- Simkl (optional — not needed if using MDBList-only mode) ---------------------
    simkl_client_id: str = Field(
        default="",
        alias="SIMKL_CLIENT_ID",
    )

    # -- MDBList (optional — OAuth for full integration) ----------------------------
    mdblist_client_id: str = Field(default="", alias="MDBLIST_CLIENT_ID")
    mdblist_client_secret: str = Field(default="", alias="MDBLIST_CLIENT_SECRET")

    # -- Emby (REQUIRED) ----------------------------------------------------------------
    emby_url: str = Field(
        ...,  # Required
        alias="EMBY_URL",
    )
    emby_api_key: str = Field(
        ...,  # Required
        alias="EMBY_API_KEY",
    )

    # -- Redis ------------------------------------------------------------------
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        alias="REDIS_URL",
    )
    redis_password: str = Field(
        default="",
        alias="REDIS_PASSWORD",
    )

    # -- Postgres (REQUIRED PASSWORD) ------------------------------------------------
    database_url: str = Field(
        ...,  # Required
        alias="DATABASE_URL",
    )
    db_user: str = Field(default="embysimkl", alias="DB_USER")
    db_password: str = Field(..., alias="DB_PASSWORD")  # Required
    db_name: str = Field(default="embysimkl", alias="DB_NAME")

    # -- Feature toggles -----------------------------------------------------
    enable_smart_queue: bool = Field(default=True, alias="ENABLE_SMART_QUEUE")
    enable_ml_predictor: bool = Field(default=True, alias="ENABLE_ML_PREDICTOR")
    enable_universe_discovery: bool = Field(default=True, alias="ENABLE_UNIVERSE_DISCOVERY")
    enable_watch_party: bool = Field(default=True, alias="ENABLE_WATCH_PARTY")
    enable_rating_bias_detector: bool = Field(default=True, alias="ENABLE_RATING_BIAS_DETECTOR")

    # -- Scheduler cron expressions ------------------------------------------
    smart_queue_cron: str = Field(default="0 2 * * *", alias="SMART_QUEUE_CRON")
    universe_scan_cron: str = Field(default="0 3 * * 0", alias="UNIVERSE_SCAN_CRON")
    ml_retrain_cron: str = Field(default="0 4 * * 1", alias="ML_RETRAIN_CRON")

    # -- SSL monitoring (optional — production only) -------------------------
    ssl_domain: str = Field(default="", alias="SSL_DOMAIN")

    # -- Security & Logging --------------------------------------------------
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: str = Field(default="/app/logs/emby-simkl-suite.log", alias="LOG_FILE")
    log_max_bytes: int = Field(default=10_485_760, alias="LOG_MAX_BYTES")  # 10 MB
    log_backup_count: int = Field(default=5, alias="LOG_BACKUP_COUNT")
    jwt_secret_key: str = Field(
        ...,  # Required
        alias="JWT_SECRET_KEY",
        description="Secret key for JWT signing",
    )
    allowed_origins: str = Field(
        default="http://localhost:8000",
        alias="ALLOWED_ORIGINS",
    )
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")

    model_config = {"env_file": ".env", "extra": "ignore", "case_sensitive": False}

    # ✅ SECURITY: Validate secrets are not weak/default
    @field_validator('emby_api_key', 'db_password', 'jwt_secret_key')
    @classmethod
    def validate_no_weak_secrets(cls, v: str, info) -> str:
        if not v or v.strip() == "":
            field_name = info.field_name
            raise ValueError(f"{field_name} is required and must not be empty.")
        
        weak_values = {'changeme', 'password', 'secret', 'default', 'test', ''}
        if v.lower() in weak_values:
            field_name = info.field_name
            raise ValueError(f"{field_name} contains a weak/default value. Use a strong, unique secret.")
        
        return v

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of: {valid_levels}")
        return v.upper()

    @field_validator('allowed_origins')
    @classmethod
    def validate_origins(cls, v: str) -> str:
        """Validate that all origins are proper HTTP(S) URLs."""
        origins = [o.strip() for o in v.split(',')]
        for origin in origins:
            if not origin.startswith(('http://', 'https://')):
                raise ValueError(f"Invalid origin: {origin} - must start with http:// or https://")
        return v

    def get_allowed_origins_list(self) -> list[str]:
        """Split allowed_origins string into list."""
        return [o.strip() for o in self.allowed_origins.split(',')]


try:
    settings = Settings()
except ValueError as e:
    print(f"❌ Configuration error: {e}")
    print("\nRequired environment variables:")
    print("  - EMBY_URL")
    print("  - EMBY_API_KEY")
    print("  - DATABASE_URL")
    print("  - DB_PASSWORD")
    print("  - JWT_SECRET_KEY")
    print("\nOptional (at least one integration recommended):")
    print("  - SIMKL_CLIENT_ID")
    print("  - MDBLIST_CLIENT_ID / MDBLIST_CLIENT_SECRET")
    raise

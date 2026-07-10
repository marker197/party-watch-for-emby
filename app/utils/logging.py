"""Structured logging setup (SECURITY HARDENED) with log rotation."""

import logging
import os
from logging.handlers import RotatingFileHandler

import structlog
from app.config import settings


def setup_logging():
    """Setup structured logging with security audit trail and file rotation.

    Logs go to:
      1. stdout (for Docker / Synology Container Manager log viewer)
      2. Rotating file at LOG_FILE (default /app/logs/emby-trakt-suite.log)
         Max LOG_MAX_BYTES per file (default 10 MB), keeps LOG_BACKUP_COUNT backups (default 5).
    """

    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # -- Shared structlog processors (applied to every log line) --
    shared_processors = [
        _scrub_sensitive,
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    # -- Build stdlib root logger with two handlers --
    root = logging.getLogger()
    root.setLevel(level)
    # Clear any pre-existing handlers from previous calls / uvicorn defaults
    root.handlers.clear()

    formatter = logging.Formatter("%(message)s")

    # Handler 1: stdout (Docker captures this)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Handler 2: rotating file
    log_dir = os.path.dirname(settings.log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        filename=settings.log_file,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # -- Silence noisy third-party loggers --
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # -- Filter repetitive poll endpoints from uvicorn access log --
    _access_logger = logging.getLogger("uvicorn.access")
    _access_logger.addFilter(_PollEndpointFilter())

    # -- Configure structlog to use stdlib as its sink --
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


class _PollEndpointFilter(logging.Filter):
    """Suppress uvicorn access log lines for high-frequency poll endpoints.

    The dashboard polls /health, /api/activity, /api/scheduler/status, and
    /parties every ~10 seconds.  Docker health checks hit /health every 30s.
    Logging every one of these creates ~500 lines/hour of zero-value noise.
    """

    _NOISY = frozenset({"/health", "/api/activity", "/api/scheduler/status", "/parties"})

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(ep in msg for ep in self._NOISY)


def _scrub_sensitive(logger, name, event_dict):
    """Scrub sensitive data from logs."""
    sensitive_keys = ['password', 'secret', 'token', 'api_key', 'access_token', 'refresh_token']
    for key in list(event_dict.keys()):
        if any(s in key.lower() for s in sensitive_keys):
            event_dict[key] = '***REDACTED***'
    return event_dict


# ✅ SECURITY: Separate security logger for audit trail
security_log = structlog.get_logger("security")

"""Rate limiting middleware using slowapi.

Provides per-user and per-endpoint rate limiting to prevent abuse.
Limits:
  - General endpoints: 100 req/min
  - Auth endpoints: 10 req/min (prevent brute force)
  - Heavy endpoints (ML, universe scan): 5 req/min
  - WebSocket connections: 1 per user
"""

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
import structlog

log = structlog.get_logger()

# Initialize limiter
limiter = Limiter(key_func=get_remote_address)

# Custom rate limit strings
LIMITS = {
    "general": "100/minute",           # Most endpoints
    "auth": "10/minute",               # Auth/login endpoints (prevent brute force)
    "heavy": "5/minute",               # ML training, universe scan
    "search": "30/minute",             # Search/filter operations
    "write": "50/minute",              # POST/PUT operations
    "read": "150/minute",              # GET operations (less risky)
}


async def rate_limit_error_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded errors."""
    log.warning(
        "rate_limit_exceeded",
        client_ip=get_remote_address(request),
        path=request.url.path,
        method=request.method,
    )
    return {
        "error": "Rate limit exceeded",
        "detail": str(exc.detail),
        "retry_after": exc.headers.get("Retry-After", "60"),
    }


# Rate limit decorators by endpoint type
# Usage: @limiter.limit(LIMITS["auth"])
# def my_endpoint(request: Request): ...

__all__ = ["limiter", "LIMITS", "rate_limit_error_handler"]

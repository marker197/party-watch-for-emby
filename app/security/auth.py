"""Authentication and Authorization Module (JWT-based)."""

from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
import structlog
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from app.config import settings
from app.models.schema import User
from app.utils.database import get_db

log = structlog.get_logger()
security_log = structlog.get_logger("security")

# Home-LAN deployments use the dashboard without a login flow, so JWT auth
# is optional by default. Set REQUIRE_AUTH=true in .env to enforce tokens
# (e.g. if the suite is ever exposed beyond the home network).
import os
REQUIRE_AUTH = os.environ.get("REQUIRE_AUTH", "false").lower() == "true"

security = HTTPBearer(auto_error=REQUIRE_AUTH)

# Simple credentials class for type annotation
class HTTPAuthCredentials:
    def __init__(self, credentials: str):
        self.credentials = credentials

JWT_SECRET = settings.jwt_secret_key
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
JWT_REFRESH_EXPIRY_DAYS = 7


class TokenResponse(BaseModel):
    """Response with access and refresh tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


def create_access_token(user_id: int) -> str:
    """Create a JWT access token for a user."""
    now = datetime.now(timezone.utc)
    exp = now + timedelta(hours=JWT_EXPIRY_HOURS)
    
    payload = {
        "user_id": user_id,
        "type": "access",
        "exp": exp.timestamp(),
    }
    
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    security_log.info("token_created", user_id=user_id, type="access")
    return token


def create_refresh_token(user_id: int) -> str:
    """Create a JWT refresh token (longer expiry)."""
    now = datetime.now(timezone.utc)
    exp = now + timedelta(days=JWT_REFRESH_EXPIRY_DAYS)
    
    payload = {
        "user_id": user_id,
        "type": "refresh",
        "exp": exp.timestamp(),
    }
    
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    security_log.info("token_created", user_id=user_id, type="refresh")
    return token


def verify_token(token: str) -> dict:
    """Verify and decode JWT token."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        security_log.warning("token_expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        security_log.warning("token_invalid")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: HTTPAuthCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    """FastAPI dependency to get current authenticated user.

    When REQUIRE_AUTH is false (home-LAN default) and no bearer token is
    supplied, falls back to the first linked user so the dashboard works
    without a login flow.
    """
    if credentials is None:
        if REQUIRE_AUTH:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )
        user = (await db.execute(
            select(User).order_by(User.id).limit(1)
        )).scalar_one_or_none()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No linked user yet. Link your Trakt account first.",
            )
        return user

    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("user_id")
    
    if not user_id:
        security_log.warning("token_missing_user_id")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
    
    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    
    if not user:
        security_log.warning("user_not_found", user_id=user_id)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    return user


def require_user_ownership(requesting_user_id: int, resource_owner_id: int, resource_name: str = "resource"):
    """Verify user can access another user's resource.

    Skipped in home-LAN mode (REQUIRE_AUTH=false): without a login flow all
    dashboard requests resolve to the first user, so ownership can't be
    meaningfully enforced.
    """
    if not REQUIRE_AUTH:
        return
    if requesting_user_id != resource_owner_id:
        security_log.warning("unauthorized_access",
            requesting_user=requesting_user_id,
            resource_owner=resource_owner_id,
            resource_type=resource_name,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"You do not have permission to access this {resource_name}",
        )


async def issue_tokens(user: User) -> TokenResponse:
    """Issue access and refresh tokens for a user."""
    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token(user.id)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=JWT_EXPIRY_HOURS * 3600,
    )

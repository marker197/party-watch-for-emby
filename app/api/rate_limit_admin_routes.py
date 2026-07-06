"""Rate limit configuration API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
import structlog

from app.utils.database import get_db
from app.security.auth import get_current_user
from app.services.rate_limit_service import RateLimitService
from app.models.rate_limit import RateLimitConfig

log = structlog.get_logger()
router = APIRouter(prefix="/admin/rate-limits", tags=["rate-limiting"], dependencies=[Depends(get_current_user)])


# Response schemas
class RateLimitResponse(BaseModel):
    id: int
    endpoint_type: str
    limit_value: str
    requests_per_period: int
    period_seconds: int
    description: str
    enabled: bool
    modified_by: str
    modified_at: str
    
    class Config:
        from_attributes = True


class UpdateRateLimitRequest(BaseModel):
    requests_per_period: int = Field(ge=1, le=10000, description="Requests allowed")
    period_seconds: int = Field(default=60, description="Time period in seconds (60=minute, 3600=hour)")


class RateLimitStatsResponse(BaseModel):
    total_limits: int
    enabled_count: int
    disabled_count: int
    limits: list[RateLimitResponse]


@router.get("", name="Get All Rate Limits")
async def get_all_rate_limits(db: AsyncSession = Depends(get_db)):
    """Get all rate limit configurations."""
    try:
        limits = await RateLimitService.get_all_limits(db)
        total = len(limits)
        enabled = sum(1 for l in limits if l.enabled)
        
        return RateLimitStatsResponse(
            total_limits=total,
            enabled_count=enabled,
            disabled_count=total - enabled,
            limits=[RateLimitResponse.model_validate(l) for l in limits]
        )
    except Exception as e:
        log.error("rate_limits.get_all_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{endpoint_type}", name="Get Rate Limit")
async def get_rate_limit(endpoint_type: str, db: AsyncSession = Depends(get_db)):
    """Get specific rate limit configuration."""
    try:
        limit = await RateLimitService.get_limit(db, endpoint_type)
        if not limit:
            raise HTTPException(status_code=404, detail=f"Rate limit for '{endpoint_type}' not found")
        
        return RateLimitResponse.model_validate(limit)
    except HTTPException:
        raise
    except Exception as e:
        log.error("rate_limits.get_failed", endpoint_type=endpoint_type, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{endpoint_type}", name="Update Rate Limit")
async def update_rate_limit(
    endpoint_type: str,
    request: UpdateRateLimitRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Update rate limit configuration."""
    try:
        # Validate endpoint_type
        valid_types = list(RateLimitService.DEFAULT_LIMITS.keys())
        if endpoint_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid endpoint type. Must be one of: {', '.join(valid_types)}"
            )
        
        updated = await RateLimitService.update_limit(
            db,
            endpoint_type,
            request.requests_per_period,
            request.period_seconds,
            modified_by=current_user.get("sub", "unknown")
        )
        
        return RateLimitResponse.model_validate(updated)
    except HTTPException:
        raise
    except Exception as e:
        log.error("rate_limits.update_failed", endpoint_type=endpoint_type, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{endpoint_type}/toggle", name="Toggle Rate Limit")
async def toggle_rate_limit(
    endpoint_type: str,
    enabled: bool,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Enable or disable a rate limit."""
    try:
        updated = await RateLimitService.toggle_limit(
            db,
            endpoint_type,
            enabled,
            modified_by=current_user.get("sub", "unknown")
        )
        
        return RateLimitResponse.model_validate(updated)
    except Exception as e:
        log.error("rate_limits.toggle_failed", endpoint_type=endpoint_type, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset/defaults", name="Reset to Defaults")
async def reset_to_defaults(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Reset all rate limits to defaults."""
    try:
        await RateLimitService.reset_to_defaults(
            db,
            modified_by=current_user.get("sub", "unknown")
        )
        
        limits = await RateLimitService.get_all_limits(db)
        return {
            "message": "Rate limits reset to defaults",
            "limits": [RateLimitResponse.model_validate(l) for l in limits]
        }
    except Exception as e:
        log.error("rate_limits.reset_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


__all__ = ["router"]

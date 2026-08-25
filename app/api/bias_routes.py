"""Routes extracted from routes.py — bias_routes.py."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.security.auth import get_current_user
from app.services.rating_bias_detector.service import RatingBiasDetectorService

log = structlog.get_logger()

router = APIRouter()

bias_detector_svc = RatingBiasDetectorService()


@router.post("/bias/analyze/{user_id}")
async def analyze_bias(user_id: int, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Trigger bias analysis for a user."""
    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    result = await bias_detector_svc.analyze_user(user)
    return result


@router.get("/bias/report/{user_id}")
async def get_bias_report(user_id: int):
    """Get full bias report for a user."""
    report = await bias_detector_svc.get_bias_report(user_id)
    if not report:
        raise HTTPException(status_code=404, detail="Bias report not found. Run analysis first.")
    return report


@router.get("/bias/hidden-gems/{user_id}")
async def get_hidden_gems(user_id: int, limit: int = 20):
    """Get hidden gems (items user should rate higher based on patterns)."""
    gems = await bias_detector_svc.get_hidden_gems(user_id, limit)
    return {"gems": gems}


@router.get("/bias/challenges/{user_id}")
async def get_challenges(user_id: int):
    """Get rating challenges to explore blind spots."""
    challenges = await bias_detector_svc.get_challenges(user_id)
    return {"challenges": challenges}


@router.get("/bias/library-matches/{user_id}")
async def get_library_matches(user_id: int, criteria: str):
    """Find Emby library items matching a challenge/gem profile.
    
    criteria: genre:ACTION, era:1990s, against:diversity
    """
    matches = await bias_detector_svc.find_library_matches(user_id, criteria)
    return {"matches": matches}


# ═══════════════════════════════════════════════════════════════════════════

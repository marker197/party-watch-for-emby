"""
API Routes — Library Health Monitor (legacy v2 endpoints) & Bulk Actions.

Social Watching endpoints removed (Simkl has no friends API — permanently
unavailable).

Library Health v2 endpoints now delegate to the main LibraryHealthService
(the same service used by /api/library-health/* in routes.py and the
weekly scheduler job).  The old LibraryHealthMonitor class has been removed.

Total: 9 endpoints
- Library Health: 6 endpoints
- Bulk Actions: 3 endpoints
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import (
    LibraryGap, LibraryHealthReport, BulkAction, User
)
from app.services.library_health_service import LibraryHealthService
from app.utils.database import get_db

from app.security.auth import get_current_user, require_user_ownership

log = structlog.get_logger()

# Create router
router = APIRouter(prefix="/api/v2", tags=["Extended Features"])

# Shared service instance (stateless — safe to share)
_health_svc = LibraryHealthService()


# ============================================================================
# LIBRARY HEALTH MONITOR (#9) - 6 Endpoints
#
# These v2 endpoints provide the same data as the main /api/library-health
# endpoints in routes.py but under the /api/v2 prefix.  They delegate to
# the same LibraryHealthService.
# ============================================================================

@router.get("/health/report/{user_id}")
async def get_health_report(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get most recent library health report."""
    require_user_ownership(_user.id, user_id, "health_report")
    try:
        result = await db.execute(
            select(LibraryHealthReport).filter(
                LibraryHealthReport.user_id == user_id
            ).order_by(LibraryHealthReport.generated_at.desc())
        )
        latest_report = result.scalars().first()

        if not latest_report:
            return {"success": True, "data": None, "message": "No health report generated yet"}

        report_data = latest_report.report_json or {}
        return {"success": True, "data": report_data}
    except HTTPException:
        raise
    except Exception as e:
        log.error("phase5.get_health_report_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/gaps/{user_id}")
async def get_library_gaps(
    user_id: int,
    gap_type: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get detected gaps in library (incomplete series, orphaned episodes, etc.)."""
    require_user_ownership(_user.id, user_id, "library_gaps")
    try:
        stmt = select(LibraryGap).filter(LibraryGap.user_id == user_id)

        if gap_type:
            stmt = stmt.filter(LibraryGap.gap_type == gap_type)
        if priority:
            stmt = stmt.filter(LibraryGap.priority == priority)

        result = await db.execute(stmt.order_by(LibraryGap.detected_at.desc()).limit(limit))
        gaps = result.scalars().all()

        return {
            "success": True,
            "total": len(gaps),
            "gaps": [
                {
                    "id": g.id,
                    "gap_type": g.gap_type,
                    "title": g.title,
                    "description": g.description,
                    "priority": g.priority,
                    "status": g.status,
                    "user_rating": g.user_rating,
                    "detected_at": g.detected_at.isoformat() if g.detected_at else None
                }
                for g in gaps
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("phase5.get_library_gaps_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/incomplete-series/{user_id}")
async def get_incomplete_series(
    user_id: int,
    min_completion: float = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get list of incomplete series from the most recent scan."""
    require_user_ownership(_user.id, user_id, "incomplete_series")
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    try:
        report = await _health_svc.get_report(user)
        series = report.get("incomplete_series", [])
        filtered = [s for s in series if s.get("completion_pct", 0) >= min_completion]
        return {
            "success": True,
            "total": len(filtered),
            "series": filtered[:limit],
        }
    except Exception as e:
        log.error("phase5.incomplete_series_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/orphaned-episodes/{user_id}")
async def get_orphaned_episodes(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get watched-not-in-library items from the most recent scan."""
    require_user_ownership(_user.id, user_id, "orphaned_episodes")
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    try:
        report = await _health_svc.get_report(user)
        wnil = report.get("watched_not_in_library", {})
        movies = wnil.get("movies", [])
        shows = wnil.get("shows", [])
        combined = movies + shows
        return {
            "success": True,
            "total": len(combined),
            "orphaned_episodes": combined[:limit],
        }
    except Exception as e:
        log.error("phase5.orphaned_episodes_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/acquisitions/{user_id}")
async def get_acquisition_recommendations(
    user_id: int,
    priority: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get recommendations for what to acquire to fill library gaps."""
    require_user_ownership(_user.id, user_id, "acquisitions")
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    try:
        report = await _health_svc.get_report(user)
        recs = report.get("missing_sequels", [])
        if priority:
            recs = [r for r in recs if r.get("priority") == priority]
        return {
            "success": True,
            "total": len(recs),
            "recommendations": recs[:limit],
        }
    except Exception as e:
        log.error("phase5.acquisitions_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/health/analyze/{user_id}")
async def analyze_library_health(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Run full library health analysis."""
    require_user_ownership(_user.id, user_id, "health_analysis")
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    try:
        report = await _health_svc.scan(user)
        return {
            "success": True,
            "analysis_complete": True,
            "report": report,
        }
    except Exception as e:
        log.error("phase5.health_analysis_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# BULK ACTIONS UI - 3 Endpoints
#
# NOTE: Bulk action execution is not yet implemented.  create_bulk_action
# records the request; a future background worker will process pending
# actions.  Until then, status remains 'pending'.
# ============================================================================

@router.post("/bulk/action")
async def create_bulk_action(
    user_id: int,
    action_type: str,
    item_ids: list[str],
    metadata: dict = None,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Create bulk action record.

    NOTE: Execution is not yet implemented — the action is recorded with
    status='pending' for a future background processor.
    """
    require_user_ownership(_user.id, user_id, "bulk_action")
    try:
        if not item_ids or len(item_ids) > 1000:
            raise HTTPException(status_code=400, detail="Invalid item count (1-1000)")

        valid_types = ['delete', 'rate_batch', 'export', 'add_collection']
        if action_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid action type: {action_type}")

        action = BulkAction(
            user_id=user_id,
            action_type=action_type,
            item_ids=item_ids,
            status='pending',
            result_json=metadata or {}
        )

        db.add(action)
        await db.commit()
        await db.refresh(action)

        return {
            "success": True,
            "action_id": action.id,
            "status": "pending",
            "item_count": len(item_ids),
            "note": "Bulk execution is not yet implemented — action recorded for future processing.",
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("phase5.bulk_action_create_failed", error=str(e)[:200])
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bulk/status/{action_id}")
async def get_bulk_action_status(
    action_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get status of a bulk action."""
    try:
        result = await db.execute(select(BulkAction).filter(BulkAction.id == action_id))
        action = result.scalars().first()

        if not action:
            raise HTTPException(status_code=404, detail="Action not found")

        require_user_ownership(_user.id, action.user_id, "bulk_action_status")

        return {
            "success": True,
            "action_id": action.id,
            "action_type": action.action_type,
            "status": action.status,
            "item_count": len(action.item_ids) if action.item_ids else 0,
            "result": action.result_json or {},
            "created_at": action.created_at.isoformat() if action.created_at else None,
            "completed_at": action.completed_at.isoformat() if action.completed_at else None
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("phase5.bulk_status_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bulk/history/{user_id}")
async def get_bulk_action_history(
    user_id: int,
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get history of bulk actions for a user."""
    require_user_ownership(_user.id, user_id, "bulk_history")
    try:
        stmt = select(BulkAction).filter(BulkAction.user_id == user_id)

        if status:
            stmt = stmt.filter(BulkAction.status == status)

        result = await db.execute(stmt.order_by(BulkAction.created_at.desc()).limit(limit))
        actions = result.scalars().all()

        return {
            "success": True,
            "total": len(actions),
            "history": [
                {
                    "action_id": a.id,
                    "action_type": a.action_type,
                    "status": a.status,
                    "item_count": len(a.item_ids) if a.item_ids else 0,
                    "created_at": a.created_at.isoformat() if a.created_at else None,
                    "completed_at": a.completed_at.isoformat() if a.completed_at else None
                }
                for a in actions
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("phase5.bulk_history_failed", error=str(e)[:200])
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Export router for main.py
# ============================================================================

def get_phase5_router():
    """Get extended features router for mounting in main.py"""
    return router

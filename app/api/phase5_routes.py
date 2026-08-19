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
# Bulk actions are auto-executed in background on creation via
# _process_bulk_action.  Pending actions can also be manually triggered
# via POST /bulk/execute/{action_id}.
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
    """Create and execute a bulk action.

    Supported action_types: delete, rate_batch, export, add_collection.
    Execution starts immediately in background.
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

        # Auto-execute immediately in background
        import asyncio
        asyncio.create_task(_process_bulk_action(action.id))

        return {
            "success": True,
            "action_id": action.id,
            "status": "processing",
            "item_count": len(item_ids),
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
# BULK ACTION PROCESSOR
# ============================================================================


async def _process_bulk_action(action_id: int) -> None:
    """Background worker: execute a pending bulk action.

    Called after creation or via the manual /bulk/execute endpoint.
    Each action type operates on the list of item IDs stored on the
    BulkAction row and writes per-item results back to result_json.
    """
    from app.utils.database import async_session
    from app.utils.emby_client import EmbyClient

    async with async_session() as db:
        action = (await db.execute(
            select(BulkAction)
            .filter(BulkAction.id == action_id)
            .with_for_update(skip_locked=True)
        )).scalars().first()
        if not action or action.status != "pending":
            return

        action.status = "in_progress"
        # Preserve the caller's input metadata so retries are possible
        input_meta = dict(action.result_json) if action.result_json else {}
        await db.commit()

        item_ids = action.item_ids or []
        results: dict = {"processed": 0, "succeeded": 0, "failed": 0, "errors": []}

        try:
            if action.action_type == "delete":
                async with EmbyClient() as emby:
                    for eid in item_ids:
                        try:
                            item = await emby.get_item_safe(str(eid))
                            if not item:
                                results["errors"].append(
                                    {"emby_id": eid, "error": "not found"})
                                results["failed"] += 1
                            else:
                                await emby.delete_item(str(eid))
                                results["succeeded"] += 1
                        except Exception as e:
                            results["errors"].append(
                                {"emby_id": eid, "error": str(e)[:200]})
                            results["failed"] += 1
                        results["processed"] += 1

            elif action.action_type == "rate_batch":
                # item_ids are expected to be dicts serialised in the
                # metadata: [{emby_id, imdb_id, rating, item_type, title}, ...]
                rating_items = input_meta.get("items") or []
                from app.models.schema import UserRating
                for ri in rating_items:
                    try:
                        imdb_id = ri.get("imdb_id")
                        rating = ri.get("rating")
                        if not imdb_id or not rating:
                            results["errors"].append(
                                {"imdb_id": imdb_id, "error": "missing data"})
                            results["failed"] += 1
                            continue
                        # Store locally
                        existing = (await db.execute(
                            select(UserRating).where(
                                UserRating.user_id == action.user_id,
                                UserRating.imdb_id == imdb_id,
                            )
                        )).scalar_one_or_none()
                        if existing:
                            existing.rating = float(rating)
                            existing.source = "user"
                        else:
                            db.add(UserRating(
                                user_id=action.user_id,
                                simkl_id=imdb_id,
                                title=ri.get("title", ""),
                                item_type=ri.get("item_type", "movie"),
                                rating=float(rating),
                                imdb_id=imdb_id,
                                tmdb_id=ri.get("tmdb_id"),
                                source="user",
                                rated_at=datetime.now(timezone.utc).replace(
                                    tzinfo=None),
                            ))
                        await db.commit()
                        results["succeeded"] += 1
                    except Exception as e:
                        await db.rollback()
                        results["errors"].append(
                            {"imdb_id": ri.get("imdb_id"),
                             "error": str(e)[:200]})
                        results["failed"] += 1
                    results["processed"] += 1

            elif action.action_type == "add_collection":
                collection_name = input_meta.get("collection_name", "Bulk Collection")
                async with EmbyClient() as emby:
                    str_ids = [str(eid) for eid in item_ids]
                    try:
                        col_id = await emby.find_or_create_collection(
                            collection_name, str_ids)
                        results["succeeded"] = len(str_ids)
                        results["processed"] = len(str_ids)
                        results["collection_id"] = col_id
                    except Exception as e:
                        results["failed"] = len(str_ids)
                        results["processed"] = len(str_ids)
                        results["errors"].append(
                            {"error": str(e)[:200]})

            elif action.action_type == "export":
                # Export watch history rows for the given emby IDs to JSON
                from app.models.schema import WatchHistory
                rows = (await db.execute(
                    select(WatchHistory).where(
                        WatchHistory.user_id == action.user_id,
                        WatchHistory.emby_id.in_(
                            [str(eid) for eid in item_ids]),
                    ).order_by(WatchHistory.watched_at.desc())
                )).scalars().all()

                export_data = []
                for row in rows:
                    export_data.append({
                        "title": row.title,
                        "imdb_id": row.imdb_id,
                        "tmdb_id": row.tmdb_id,
                        "item_type": row.item_type,
                        "watched_at": row.watched_at.isoformat()
                        if row.watched_at else None,
                        "series_name": row.series_name,
                    })
                results["succeeded"] = len(export_data)
                results["processed"] = len(item_ids)
                results["export_data"] = export_data

            else:
                results["errors"].append(
                    {"error": f"Unknown action_type: {action.action_type}"})

            action.status = "completed"

        except Exception as e:
            log.error("bulk.processor_failed", action_id=action_id,
                      error=str(e)[:200])
            results["errors"].append({"error": str(e)[:200]})
            action.status = "failed"

        # Trim errors list to avoid unbounded JSON growth
        if len(results.get("errors", [])) > 50:
            results["errors"] = results["errors"][:50]
            results["errors_truncated"] = True

        # Preserve caller's input alongside output so retries are possible
        results["_input"] = input_meta
        action.result_json = results
        action.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        await db.commit()

        log.info("bulk.action_completed", action_id=action_id,
                 action_type=action.action_type, status=action.status,
                 succeeded=results.get("succeeded", 0),
                 failed=results.get("failed", 0))


@router.post("/bulk/execute/{action_id}")
async def execute_bulk_action(
    action_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Manually trigger execution of a pending bulk action."""
    action = (await db.execute(
        select(BulkAction).filter(BulkAction.id == action_id)
    )).scalars().first()

    if not action:
        raise HTTPException(404, "Action not found")

    require_user_ownership(_user.id, action.user_id, "bulk_execute")

    if action.status != "pending":
        raise HTTPException(
            400, f"Action is '{action.status}', not 'pending'")

    import asyncio
    asyncio.create_task(_process_bulk_action(action_id))

    return {
        "success": True,
        "action_id": action_id,
        "status": "processing",
        "note": "Execution started in background.",
    }


# ============================================================================
# Export router for main.py
# ============================================================================

def get_phase5_router():
    """Get extended features router for mounting in main.py"""
    return router

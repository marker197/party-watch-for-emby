"""
API Routes — Social Watching Graph, Library Health Monitor, Bulk Actions.

Total: 14 endpoints
- Social Watching: 5 endpoints
- Library Health: 6 endpoints
- Bulk Actions: 3 endpoints
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import (
    LibraryGap, LibraryHealthReport, BulkAction, User
)
from app.services.library_health import LibraryHealthMonitor
from app.utils.database import get_db
from app.utils.simkl_client import SimklClient
from app.utils.library_cache import LibraryCache
from app.utils.emby_client import EmbyClient

# ✅ SECURITY: Import auth module
from app.security.auth import get_current_user, require_user_ownership

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/v2", tags=["Extended Features"])

# Stateless singletons (no per-user auth needed)
cache = LibraryCache()


def _get_emby_client() -> EmbyClient:
    """Create a fresh EmbyClient per-request — avoids stale connections."""
    return EmbyClient()


async def _get_user_simkl(user_id: int, db: AsyncSession) -> SimklClient:
    """Build an authenticated SimklClient for the given user."""
    user = await db.get(User, user_id)
    if not user or not user.simkl_access_token:
        raise HTTPException(status_code=401, detail="User has no linked Simkl account")

    return SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )


# ============================================================================
# SOCIAL WATCHING GRAPH (#6) - Disabled (Simkl has no social/friends API)
# ============================================================================

_SOCIAL_UNAVAILABLE = {
    "success": False,
    "detail": "Social watching is not available with Simkl (no friends API)"
}


@router.get("/social/friends-watching/{user_id}")
async def get_friends_watching_now(
    user_id: int,
    limit: int = Query(20, ge=1, le=100),
    _user: User = Depends(get_current_user),
):
    """Social watching — disabled (Simkl has no friends API)."""
    return _SOCIAL_UNAVAILABLE


@router.get("/social/influence-leaderboard/{user_id}")
async def get_influence_leaderboard(
    user_id: int,
    limit: int = Query(20, ge=1, le=100),
    _user: User = Depends(get_current_user),
):
    """Influence leaderboard — disabled (Simkl has no friends API)."""
    return _SOCIAL_UNAVAILABLE


@router.get("/social/overlap/{user_id}/{friend_username}")
async def get_library_overlap(
    user_id: int,
    friend_username: str,
    _user: User = Depends(get_current_user),
):
    """Library overlap — disabled (Simkl has no friends API)."""
    return _SOCIAL_UNAVAILABLE


@router.post("/social/sync-mode/{user_id}/{friend_username}")
async def enable_social_sync_mode(
    user_id: int,
    friend_username: str,
    _user: User = Depends(get_current_user),
):
    """Social sync mode — disabled (Simkl has no friends API)."""
    return _SOCIAL_UNAVAILABLE


@router.post("/social/refresh/{user_id}")
async def refresh_social_graph(
    user_id: int,
    _user: User = Depends(get_current_user),
):
    """Refresh social graph — disabled (Simkl has no friends API)."""
    return _SOCIAL_UNAVAILABLE


# ============================================================================
# LIBRARY HEALTH MONITOR (#9) - 6 Endpoints
# ============================================================================

@router.get("/health/report/{user_id}")
async def get_health_report(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get most recent library health report.
    
    Returns:
        {
            'total_items': int,
            'unwatched_items': int,
            'incomplete_series': int,
            'orphaned_episodes': int,
            'related_missing': int,
            'series_completion_pct': float,
            'health_score': float,
            'generated_at': datetime,
            'recommendations': [str]
        }
    """
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
    except Exception as e:
        logger.error(f"Error in get_health_report: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.get("/health/gaps/{user_id}")
async def get_library_gaps(
    user_id: int,
    gap_type: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get detected gaps in library (incomplete series, orphaned episodes, etc.).
    
    Query params:
        - gap_type: Filter by type (incomplete_series, orphaned_episode, missing_sequel, director_gap)
        - priority: Filter by priority (critical, high, medium, low)
        - limit: Number of results (1-500)
    
    Returns:
        [{
            'gap_type': str,
            'title': str,
            'description': str,
            'priority': str,
            'user_rating': float,
            'status': str
        }, ...]
    """
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
    except Exception as e:
        logger.error(f"Error in get_library_gaps: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.get("/health/incomplete-series/{user_id}")
async def get_incomplete_series(
    user_id: int,
    min_completion: float = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get list of incomplete series (shows where you've watched some but not all episodes).
    
    Query params:
        - min_completion: Minimum completion % to include
        - limit: Number of results
    
    Returns:
        [{
            'title': str,
            'total_episodes': int,
            'watched_episodes': int,
            'completion_pct': float,
            'missing_seasons': [int],
            'your_rating': float
        }, ...]
    """
    try:
        _emby = _get_emby_client()
        simkl_client = await _get_user_simkl(user_id, db)
        monitor = LibraryHealthMonitor(db, simkl_client, _emby, cache)
        series = await monitor.detect_incomplete_series(user_id)

        # Filter by completion
        filtered = [s for s in series if s.get('completion_pct', 0) >= min_completion]

        return {
            "success": True,
            "total": len(filtered),
            "series": filtered[:limit]
        }
    except Exception as e:
        logger.error(f"Error in get_incomplete_series: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.get("/health/orphaned-episodes/{user_id}")
async def get_orphaned_episodes(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get orphaned episodes (watched episodes without watching season/series premiere).
    
    Returns:
        [{
            'title': str,
            'show_title': str,
            'episode_number': str,  # S02E05
            'your_rating': float,
            'status': str,
            'missing_premiere': bool,
            'missing_season_premiere': bool
        }, ...]
    """
    try:
        _emby = _get_emby_client()
        simkl_client = await _get_user_simkl(user_id, db)
        monitor = LibraryHealthMonitor(db, simkl_client, _emby, cache)
        orphaned = await monitor.find_orphaned_episodes(user_id)

        return {
            "success": True,
            "total": len(orphaned),
            "orphaned_episodes": orphaned[:limit]
        }
    except Exception as e:
        logger.error(f"Error in get_orphaned_episodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.get("/health/acquisitions/{user_id}")
async def get_acquisition_recommendations(
    user_id: int,
    priority: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get recommendations for what to acquire to fill gaps in library.
    
    Returns:
        [{
            'title': str,
            'type': str,  # 'sequel' | 'complete_series' | 'related_work'
            'reason': str,
            'priority': str,
            'estimated_cost': float,
            'why_you_should_get_it': str
        }, ...]
    """
    try:
        _emby = _get_emby_client()
        simkl_client = await _get_user_simkl(user_id, db)
        monitor = LibraryHealthMonitor(db, simkl_client, _emby, cache)
        recommendations = await monitor.acquisition_recommendations(user_id, limit)

        if priority:
            recommendations = [r for r in recommendations if r.get('priority') == priority]

        return {
            "success": True,
            "total": len(recommendations),
            "recommendations": recommendations[:limit]
        }
    except Exception as e:
        logger.error(f"Error in get_acquisition_recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.post("/health/analyze/{user_id}")
async def analyze_library_health(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Run full library health analysis (generate report, detect gaps, etc.).
    This is an async operation - status can be checked via /health/report endpoint.
    
    Returns:
        {
            'analysis_started': bool,
            'estimated_time': str,
            'job_id': str
        }
    """
    try:
        _emby = _get_emby_client()
        simkl_client = await _get_user_simkl(user_id, db)
        monitor = LibraryHealthMonitor(db, simkl_client, _emby, cache)
        report = await monitor.generate_health_report(user_id)

        return {
            "success": True,
            "analysis_complete": True,
            "report": report
        }
    except Exception as e:
        logger.error(f"Error in analyze_library_health: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


# ============================================================================
# BULK ACTIONS UI - 3 Endpoints
# ============================================================================

@router.post("/bulk/action")
async def create_bulk_action(
    user_id: int,
    action_type: str,  # 'delete' | 'rate_batch' | 'export' | 'add_collection'
    item_ids: List[str],
    metadata: Dict = None,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Create bulk action (delete multiple, batch rate, export, add to collection).
    
    Body:
        {
            'user_id': int,
            'action_type': str,
            'item_ids': [str],  # Emby item IDs
            'metadata': {
                'rating': int,  # for rate_batch
                'collection_name': str,  # for add_collection
                'format': str  # 'json' | 'csv' for export
            }
        }
    
    Returns:
        {'action_id': int, 'status': 'pending', 'estimated_time': str}
    """
    try:
        if not item_ids or len(item_ids) > 1000:
            raise HTTPException(status_code=400, detail="Invalid item count (1-1000)")

        # Validate action type
        valid_types = ['delete', 'rate_batch', 'export', 'add_collection']
        if action_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid action type: {action_type}")

        # Create bulk action record
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
            "estimated_time_seconds": len(item_ids) * 0.5  # Rough estimate
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in create_bulk_action: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bulk/status/{action_id}")
async def get_bulk_action_status(
    action_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get status of a bulk action.
    
    Returns:
        {
            'action_id': int,
            'status': str,  # 'pending' | 'in_progress' | 'completed' | 'failed'
            'progress_pct': float,
            'result': {...}
        }
    """
    try:
        result = await db.execute(select(BulkAction).filter(BulkAction.id == action_id))
        action = result.scalars().first()

        if not action:
            raise HTTPException(status_code=404, detail="Action not found")

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
        logger.error(f"Error in get_bulk_action_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


@router.get("/bulk/history/{user_id}")
async def get_bulk_action_history(
    user_id: int,
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """
    Get history of bulk actions for a user.
    
    Query params:
        - status: Filter by status (pending, in_progress, completed, failed)
        - limit: Number of results
    
    Returns:
        [{
            'action_id': int,
            'action_type': str,
            'status': str,
            'item_count': int,
            'created_at': datetime,
            'completed_at': datetime
        }, ...]
    """
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
    except Exception as e:
        logger.error(f"Error in get_bulk_action_history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if "_emby" in dir():
            await _emby.close()


# ============================================================================
# Export router for main.py
# ============================================================================

def get_phase5_router():
    """Get extended features router for mounting in main.py"""
    return router

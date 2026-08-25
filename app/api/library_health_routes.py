"""Routes extracted from routes.py — library_health_routes.py."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import AppSetting, User
from app.utils.database import get_db
from app.utils.redis_cache import get_redis
from app.security.auth import get_current_user, require_user_ownership

log = structlog.get_logger()

router = APIRouter()



from app.services.library_health_service import LibraryHealthService

_library_health_svc = LibraryHealthService()


@router.get("/library-health", response_class=HTMLResponse)
async def get_library_health_page():
    """Serve the Library Health Monitor page."""
    try:
        with open("frontend/templates/library_health.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/api/library-health/{user_id}")
async def get_library_health_report(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return cached library health report for a user."""
    require_user_ownership(current_user.id, user_id, "library_health")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _library_health_svc.get_report(user)


@router.post("/api/library-health/{user_id}/scan")
async def scan_library_health(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Run a full library health scan for a user."""
    require_user_ownership(current_user.id, user_id, "library_health")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _library_health_svc.scan(user)


@router.post("/api/library-health/{user_id}/dismiss")
async def dismiss_library_health_item(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dismiss a 'watched not in library' item so it no longer appears in reports."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    body = await request.json()
    item_type = body.get("type", "")  # "movie" or "show"
    item_id = str(body.get("id", ""))  # imdb/tmdb/tvdb/simkl ID
    if not item_id or not item_type:
        raise HTTPException(400, "type and id required")

    # Upsert: only insert if not already dismissed
    existing = (await db.execute(
        select(DismissedHealthItem).where(
            DismissedHealthItem.user_id == user_id,
            DismissedHealthItem.item_type == item_type,
            DismissedHealthItem.item_id == item_id,
        )
    )).scalar_one_or_none()
    if not existing:
        db.add(DismissedHealthItem(user_id=user_id, item_type=item_type, item_id=item_id))
        await db.commit()

    count = (await db.execute(
        select(func.count()).select_from(DismissedHealthItem).where(
            DismissedHealthItem.user_id == user_id
        )
    )).scalar()
    return {"status": "dismissed", "total_dismissed": count}


@router.get("/api/library-health/{user_id}/dismissed")
async def get_dismissed_library_health_items(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the list of dismissed library health items for a user."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    rows = (await db.execute(
        select(DismissedHealthItem).where(DismissedHealthItem.user_id == user_id)
    )).scalars().all()
    return {"dismissed": [
        {"type": r.item_type, "id": r.item_id, "dismissed_at": r.dismissed_at.isoformat() if r.dismissed_at else None}
        for r in rows
    ]}


@router.post("/api/library-health/{user_id}/undismiss-all")
async def undismiss_all_library_health(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Clear all dismissed library health items for a user."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    await db.execute(
        DismissedHealthItem.__table__.delete().where(
            DismissedHealthItem.user_id == user_id
        )
    )
    await db.commit()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# DB Inspector — browse app_settings from the browser
# ---------------------------------------------------------------------------

@router.get("/api/db/app-settings")
async def db_app_settings(prefix: str = "", db: AsyncSession = Depends(get_db)):
    """List app_settings rows, optionally filtered by key prefix.

    GET /api/db/app-settings              → all rows
    GET /api/db/app-settings?prefix=scrobble  → keys starting with 'scrobble'
    GET /api/db/app-settings?prefix=ml_drift  → drift snapshots
    """
    if prefix:
        rows = (await db.execute(
            select(AppSetting).where(AppSetting.key.like(f"{prefix}%"))
        )).scalars().all()
    else:
        rows = (await db.execute(select(AppSetting))).scalars().all()
    result = []
    for r in rows:
        try:
            val = _json.loads(r.value) if r.value else None
        except Exception:
            val = r.value
        result.append({
            "key": r.key,
            "value": val,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        })
    return {"settings": result}


@router.get("/api/debug-mode")
async def get_debug_mode():
    """Return current log level / debug state."""
    import logging as _logging
    current = _logging.getLogger().level
    return {"debug": current <= _logging.DEBUG, "level": _logging.getLevelName(current)}


@router.put("/api/debug-mode")
async def set_debug_mode(payload: dict, _user: User = Depends(get_current_user)):
    """Toggle debug logging at runtime. No restart needed."""
    import logging as _logging
    import structlog as _sl

    enabled = payload.get("enabled", False)
    new_level = _logging.DEBUG if enabled else _logging.INFO

    root = _logging.getLogger()
    root.setLevel(new_level)
    for h in root.handlers:
        h.setLevel(new_level)

    # Reconfigure structlog's filtering threshold — must pass the full
    # processor chain (not just wrapper_class) to avoid resetting to
    # defaults.  With cache_logger_on_first_use=False (set in
    # setup_logging), all existing module-level loggers pick up the new
    # level immediately.
    _sl.configure(
        wrapper_class=_sl.make_filtering_bound_logger(new_level),
    )

    # Persist preference so it survives page reload (not container restart)
    r = await get_redis()
    await r.set("debug_mode", "1" if enabled else "0")

    level_name = "DEBUG" if enabled else "INFO"
    log.info("settings.debug_mode_changed", level=level_name)
    return {"debug": enabled, "level": level_name}


# ═══════════════════════════════════════════════════════════════════════════

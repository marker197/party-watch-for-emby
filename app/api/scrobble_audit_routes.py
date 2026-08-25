"""Routes extracted from routes.py — scrobble_audit_routes.py."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.security.auth import get_current_user, require_user_ownership
from app.services.scrobble_audit.service import ScrobbleAuditService

log = structlog.get_logger()

router = APIRouter()

scrobble_audit_svc = ScrobbleAuditService()


@router.get("/api/scrobble-audit/{user_id}")
async def scrobble_audit(
    user_id: int,
    force: bool = False,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Emby played items vs Simkl history — surface missed scrobbles."""
    require_user_ownership(current_user.id, user_id, "scrobble_audit")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await scrobble_audit_svc.run_audit(user, force=force)


@router.post("/api/scrobble-audit/{user_id}/backfill")
async def scrobble_backfill(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backfill selected items to Simkl history.

    Body: {items: [{type, imdb_id, tmdb_id, title}, ...]}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_backfill")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    body = await request.json()
    items = body.get("items", [])
    if not items:
        raise HTTPException(400, "No items provided")
    return await scrobble_audit_svc.backfill(user, items)


@router.post("/api/scrobble-audit/{user_id}/backfill-all")
async def scrobble_backfill_all(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backfill ALL missed scrobbles to Simkl history."""
    require_user_ownership(current_user.id, user_id, "scrobble_backfill_all")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    audit = await scrobble_audit_svc.run_audit(user)
    all_items = audit.get("movies", []) + audit.get("shows", [])
    if not all_items:
        return {"added": 0, "message": "Nothing to backfill"}
    return await scrobble_audit_svc.backfill(user, all_items)


@router.post("/api/scrobble-audit/{user_id}/dismiss")
async def scrobble_dismiss(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    """Dismiss an item from the scrobble audit list.

    Body: {emby_id: "..."}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_dismiss")
    body = await request.json()
    emby_id = body.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id required")
    return await scrobble_audit_svc.dismiss_item(user_id, emby_id)


@router.post("/api/scrobble-audit/{user_id}/undismiss")
async def scrobble_undismiss(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    """Re-enable a previously dismissed audit item.

    Body: {emby_id: "..."}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_undismiss")
    body = await request.json()
    emby_id = body.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id required")
    return await scrobble_audit_svc.undismiss_item(user_id, emby_id)


@router.post("/api/scrobble-audit/{user_id}/clear-dismissals")
async def scrobble_clear_dismissals(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Remove all dismissed items so they reappear in the scrobble audit."""
    require_user_ownership(current_user.id, user_id, "scrobble_clear_dismissals")
    return await scrobble_audit_svc.clear_all_dismissals(user_id)


# ═══════════════════════════════════════════════════════════════════════════

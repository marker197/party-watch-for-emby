"""REST API routes for the Emby-Trakt Suite."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models.schema import User, QueueItem, Prediction, MLModel, Universe, UniverseItem, AppSetting, WatchPartyParticipant, WatchParty
from app.utils.database import get_db
from app.utils.trakt_client import TraktClient
from app.utils.library_cache import LibraryCache
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis

# ✅ SECURITY: Import auth module
from app.security.auth import get_current_user, require_user_ownership, issue_tokens

from app.services.smart_queue.service import SmartQueueService
from app.middleware.rate_limit import limiter, LIMITS
from app.services.ml_predictor.service import MLPredictorService
from app.services.universe_discovery.service import UniverseDiscoveryService
from app.services.watch_party.service import WatchPartyService
from app.utils.database import async_session as async_session_ctx


async def _first_emby_user_id() -> str | None:
    """Return the emby_user_id of the first linked user (for user-scoped queries)."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
    return user.emby_user_id if user else None
from app.services.rating_bias_detector.service import RatingBiasDetectorService
from app.services.airing_alerts.service import AiringAlertsService
from app.services.scrobble_audit.service import ScrobbleAuditService
from app.services.watch_stats.service import WatchStatsService

smart_queue_svc = SmartQueueService()
ml_predictor_svc = MLPredictorService()
universe_svc = UniverseDiscoveryService()
watch_party_svc = WatchPartyService()
bias_detector_svc = RatingBiasDetectorService()
airing_alerts_svc = AiringAlertsService()
scrobble_audit_svc = ScrobbleAuditService()
watch_stats_svc = WatchStatsService()

log = structlog.get_logger()

router = APIRouter()


# ═══════════════════════════════════════════════════════════════════════════
# Health
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/health")
async def health():
    cache_stats = await LibraryCache.get_stats()
    return {
        "status": "ok",
        "features": {
            "smart_queue": settings.enable_smart_queue,
            "ml_predictor": settings.enable_ml_predictor,
            "universe_discovery": settings.enable_universe_discovery,
            "watch_party": settings.enable_watch_party,
        },
        "library_cache": cache_stats,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Integration Provider Selection
# ═══════════════════════════════════════════════════════════════════════════

VALID_PROVIDERS = {"trakt", "mdblist", "both", "none"}

async def _get_integration_provider(db: AsyncSession | None = None) -> str:
    """Return the configured integration provider: 'trakt', 'mdblist', 'both', or 'none'.
    Checks Redis first (fast), falls back to DB, defaults to 'trakt' for legacy installs."""
    r = await get_redis()
    raw = await r.get("integration_provider")
    if raw:
        val = raw if isinstance(raw, str) else raw.decode()
        if val in VALID_PROVIDERS:
            return val
    if db:
        row = (await db.execute(select(AppSetting).where(AppSetting.key == "integration_provider"))).scalar_one_or_none()
        if row and row.value in VALID_PROVIDERS:
            await r.set("integration_provider", row.value)
            return row.value
    # Legacy installs without this setting default to 'trakt' if trakt creds exist
    if settings.trakt_client_id:
        return "trakt"
    return "none"


def _provider_set(provider: str) -> set[str]:
    """Convert provider string to set of active integrations."""
    if provider == "both":
        return {"trakt", "mdblist"}
    if provider in ("trakt", "mdblist"):
        return {provider}
    return set()


async def _get_active_providers(db: AsyncSession | None = None) -> set[str]:
    """Return set of active integration providers, e.g. {'trakt', 'mdblist'}."""
    return _provider_set(await _get_integration_provider(db))


@router.get("/api/integration-provider")
async def get_integration_provider(db: AsyncSession = Depends(get_db)):
    """Return the current integration provider setting."""
    provider = await _get_integration_provider(db)
    return {"provider": provider, "active": list(_provider_set(provider))}


@router.put("/api/integration-provider")
async def set_integration_provider(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Set the integration provider: 'trakt', 'mdblist', 'both', or 'none'."""
    provider = payload.get("provider", "").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise HTTPException(400, f"Invalid provider. Must be one of: {', '.join(sorted(VALID_PROVIDERS))}")

    r = await get_redis()
    await r.set("integration_provider", provider)
    # Inline upsert (can't use _put_setting — defined later in file)
    row = (await db.execute(select(AppSetting).where(AppSetting.key == "integration_provider"))).scalar_one_or_none()
    if row:
        row.value = provider
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    else:
        db.add(AppSetting(key="integration_provider", value=provider, updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))
    await db.commit()

    log.info("integration_provider.changed", provider=provider)
    return {"status": "ok", "provider": provider, "active": list(_provider_set(provider))}


@router.get("/api/integration-provider/setup-required")
async def check_setup_required(db: AsyncSession = Depends(get_db)):
    """Check if first-run setup is needed (no provider configured yet)."""
    row = (await db.execute(
        select(AppSetting).where(AppSetting.key == "integration_provider")
    )).scalar_one_or_none()
    return {"setup_required": row is None}


# ═══════════════════════════════════════════════════════════════════════════
# Auth — Trakt device-code OAuth
# ═══════════════════════════════════════════════════════════════════════════

class LinkRequest(BaseModel):
    emby_user_id: str
    emby_username: str = ""


class LinkPollRequest(BaseModel):
    emby_user_id: str
    device_code: str


@router.post("/auth/trakt/device-code")
@limiter.limit(LIMITS["auth"])
async def trakt_device_code(request: Request, db: AsyncSession = Depends(get_db)):
    """Start Trakt device-code flow.  Returns user_code + verification_url."""
    body = await request.json()
    emby_user_id = body.get("emby_user_id", "").strip()
    emby_username = body.get("emby_username", "").strip()
    if not emby_user_id:
        raise HTTPException(400, "emby_user_id is required")

    user = (await db.execute(
        select(User).where(User.emby_user_id == emby_user_id)
    )).scalar_one_or_none()

    if not user:
        user = User(emby_user_id=emby_user_id, emby_username=emby_username)
        db.add(user)
        await db.commit()
        await db.refresh(user)

    trakt = TraktClient()
    try:
        result = await trakt.get_device_code()
    finally:
        await trakt.close()

    return {
        "user_code": result["user_code"],
        "verification_url": result["verification_url"],
        "device_code": result["device_code"],
        "expires_in": result["expires_in"],
        "interval": result["interval"],
    }


@router.post("/auth/trakt/poll")
@limiter.limit(LIMITS["auth"])
async def trakt_poll(request: Request, db: AsyncSession = Depends(get_db)):
    """Poll for completed Trakt authorisation."""
    body = await request.json()
    device_code = body.get("device_code", "").strip()
    emby_user_id = body.get("emby_user_id", "").strip()
    if not device_code or not emby_user_id:
        raise HTTPException(400, "device_code and emby_user_id are required")

    trakt = TraktClient()
    try:
        token_data = await trakt.poll_device_token(device_code)
    finally:
        await trakt.close()

    if not token_data:
        return {"status": "pending"}

    user = (await db.execute(
        select(User).where(User.emby_user_id == emby_user_id)
    )).scalar_one_or_none()

    if not user:
        raise HTTPException(404, "User not found — call device-code first")

    user.trakt_access_token = token_data["access_token"]
    user.trakt_refresh_token = token_data["refresh_token"]
    user.trakt_token_expires = (datetime.now(timezone.utc) + timedelta(seconds=token_data.get("expires_in", 7776000))).replace(tzinfo=None)

    # fetch trakt username
    authed = TraktClient(access_token=token_data["access_token"])
    try:
        me = await authed.get_me()
        user.trakt_username = me.get("user", {}).get("username", "")
    finally:
        await authed.close()

    await db.commit()

    # ✅ SECURITY: Issue JWT tokens to user
    tokens = await issue_tokens(user)

    return {
        "status": "linked",
        "trakt_username": user.trakt_username,
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "token_type": tokens.token_type,
        "expires_in": tokens.expires_in,
    }


@router.get("/auth/users")
async def list_users(db: AsyncSession = Depends(get_db)):
    users = (await db.execute(select(User))).scalars().all()
    now = datetime.now(timezone.utc)
    result = []
    for u in users:
        expires = u.trakt_token_expires
        token_info = {}
        if expires:
            # DB-stored expires may be naive (pre-timezone-aware migration)
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            delta = expires - now
            total_secs = int(delta.total_seconds())
            if total_secs > 0:
                days = total_secs // 86400
                hours = (total_secs % 86400) // 3600
                minutes = (total_secs % 3600) // 60
                token_info = {
                    "token_expires": expires.isoformat(),
                    "token_days_left": days,
                    "token_hours_left": hours,
                    "token_minutes_left": minutes,
                    "token_status": "ok" if days > 7 else "expiring_soon" if days >= 1 else "expiring_today",
                }
            else:
                token_info = {
                    "token_expires": expires.isoformat(),
                    "token_days_left": 0,
                    "token_hours_left": 0,
                    "token_minutes_left": 0,
                    "token_status": "expired",
                }
        result.append({
            "id": u.id,
            "emby_user_id": u.emby_user_id,
            "emby_username": u.emby_username,
            "trakt_username": u.trakt_username,
            "linked": bool(u.trakt_access_token),
            **token_info,
        })
    return result


@router.get("/auth/emby-users")
async def list_all_emby_users(db: AsyncSession = Depends(get_db)):
    """Return all Emby server users, auto-creating DB records for any missing.

    The watch party page needs every Emby user in the dropdown, not just
    those who have been through the Trakt link flow.
    """
    emby = EmbyClient()
    try:
        emby_users = await emby.get_users()
    except Exception as e:
        await emby.close()
        raise HTTPException(502, f"Could not reach Emby server: {e}")
    await emby.close()

    # Load existing DB users keyed by emby_user_id
    existing = (await db.execute(select(User))).scalars().all()
    by_emby_id = {u.emby_user_id: u for u in existing}

    created = 0
    for eu in emby_users:
        eid = eu.get("Id", "")
        if not eid:
            continue
        if eid not in by_emby_id:
            new_user = User(
                emby_user_id=eid,
                emby_username=eu.get("Name", ""),
            )
            db.add(new_user)
            by_emby_id[eid] = new_user
            created += 1
        else:
            # Update username if it changed on the Emby side
            db_user = by_emby_id[eid]
            emby_name = eu.get("Name", "")
            if emby_name and db_user.emby_username != emby_name:
                db_user.emby_username = emby_name

    if created:
        await db.commit()
        # Refresh to get auto-generated IDs
        for u in by_emby_id.values():
            await db.refresh(u)

    now = datetime.now(timezone.utc)
    result = []
    for u in by_emby_id.values():
        token_info = {}
        if u.trakt_token_expires:
            _exp = u.trakt_token_expires
            if _exp.tzinfo is None:
                _exp = _exp.replace(tzinfo=timezone.utc)
            delta = _exp - now
            total_secs = int(delta.total_seconds())
            if total_secs > 0:
                days = total_secs // 86400
                hours = (total_secs % 86400) // 3600
                minutes = (total_secs % 3600) // 60
                token_info = {
                    "token_status": "ok" if days > 7 else "expiring_soon" if days >= 1 else "expiring_today",
                    "token_days_left": days,
                    "token_hours_left": hours,
                    "token_minutes_left": minutes,
                }
            else:
                token_info = {
                    "token_status": "expired",
                    "token_days_left": 0,
                    "token_hours_left": 0,
                    "token_minutes_left": 0,
                }
        result.append({
            "id": u.id,
            "emby_user_id": u.emby_user_id,
            "emby_username": u.emby_username,
            "trakt_username": u.trakt_username,
            "linked": bool(u.trakt_access_token),
            **token_info,
        })

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Feature #1 — Smart Watch Queue
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/queue/refresh")
async def refresh_queue(_user: User = Depends(get_current_user)):
    """Manually trigger a Smart Queue update for all users."""
    asyncio.create_task(smart_queue_svc.run_for_all_users())
    return {"status": "refresh_started"}


@router.get("/queue/{user_id}")
async def get_queue(
    user_id: int,
    limit: int = Query(20, ge=1, le=1000),  # ✅ SECURITY: Input validation
    current_user: User = Depends(get_current_user),  # ✅ SECURITY: Authentication
    db: AsyncSession = Depends(get_db),
):
    # ✅ SECURITY: Authorization check
    require_user_ownership(current_user.id, user_id, "watch_queue")
    
    items = (await db.execute(
        select(QueueItem)
        .where(
            QueueItem.user_id == user_id,
            QueueItem.played == False,
        )
        .order_by(QueueItem.score.desc())
        .limit(limit)
    )).scalars().all()

    ratings: dict[str, dict] = {}
    emby = EmbyClient()
    try:
        lib_ids = [i.emby_item_id for i in items if i.emby_item_id]
        if lib_ids:
            for it in await emby.get_items_by_ids(lib_ids):
                ratings[str(it.get("Id"))] = {
                    "community_rating": it.get("CommunityRating"),
                    "official_rating": it.get("OfficialRating"),
                    "date_created": it.get("DateCreated"),
                }
    except Exception:
        pass
    finally:
        await emby.close()

    return [
        {
            "emby_item_id": i.emby_item_id,
            "title": i.title,
            "type": i.item_type,
            "source": i.source,
            "score": i.score,
            "trending_rank": i.trakt_trending_rank,
            "played": i.played,
            "played_at": i.played_at.isoformat() if i.played_at else None,
            "in_library": i.in_library if i.in_library is not None else True,
            "community_rating": ratings.get(str(i.emby_item_id), {}).get("community_rating"),
            "official_rating": ratings.get(str(i.emby_item_id), {}).get("official_rating"),
            "date_created": ratings.get(str(i.emby_item_id), {}).get("date_created"),
            "tmdb_id": (i.metadata_json or {}).get("ids", {}).get("tmdb"),
            "imdb_id": (i.metadata_json or {}).get("ids", {}).get("imdb"),
            "tvdb_id": (i.metadata_json or {}).get("ids", {}).get("tvdb"),
            "trakt_id": (i.metadata_json or {}).get("trakt_id"),
            "year": (i.metadata_json or {}).get("year"),
        }
        for i in items
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue Blocklist
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/queue/block")
async def block_queue_item(
    payload: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Permanently dismiss a smart queue item so it never reappears."""
    from app.models.schema import QueueBlocklist

    user_id = payload.get("user_id")
    trakt_id = str(payload.get("trakt_id", ""))
    title = payload.get("title", "")
    item_type = payload.get("item_type", "")

    if not trakt_id or not user_id:
        raise HTTPException(400, "user_id and trakt_id required")
    require_user_ownership(current_user.id, user_id, "queue_block")

    # Insert into blocklist (ignore duplicate)
    existing = (await db.execute(
        select(QueueBlocklist).where(
            QueueBlocklist.user_id == user_id,
            QueueBlocklist.trakt_id == trakt_id,
        )
    )).scalar_one_or_none()
    if not existing:
        db.add(QueueBlocklist(
            user_id=user_id, trakt_id=trakt_id,
            title=title, item_type=item_type,
        ))
        await db.flush()

    # Find and remove matching queue item
    queue_items = (await db.execute(
        select(QueueItem).where(QueueItem.user_id == user_id)
    )).scalars().all()
    removed = False
    for qi in queue_items:
        qi_trakt = str((qi.metadata_json or {}).get("trakt_id", ""))
        if qi_trakt == trakt_id:
            await db.delete(qi)
            removed = True
    await db.commit()

    # Backfill from overflow and re-sync playlist
    if removed:
        try:
            # Pop best overflow candidate
            replacement = await smart_queue_svc._pop_best_overflow(user_id, 0.0)
            if replacement:
                emby_id = await smart_queue_svc._find_in_emby(replacement)
                async with async_session_ctx() as db2:
                    db2.add(QueueItem(
                        user_id=user_id,
                        emby_item_id=emby_id,
                        title=replacement["title"],
                        item_type=replacement["item_type"],
                        source=replacement["source"],
                        score=replacement["score"],
                        trakt_trending_rank=replacement.get("trending_rank"),
                        trakt_rating=replacement.get("friend_rating"),
                        metadata_json=replacement,
                        in_library=emby_id is not None,
                    ))
                    await db2.commit()
                log.info("queue.backfill_after_block",
                         user_id=user_id, title=replacement["title"])
            # Re-sync Emby playlist
            await smart_queue_svc._resync_playlist_from_db(user_id)
        except Exception as e:
            log.warning("queue.backfill_failed", error=str(e)[:120])

    log.info("queue.item_blocked", user_id=user_id, trakt_id=trakt_id, title=title)
    return {"status": "ok", "blocked": trakt_id, "title": title}


@router.get("/api/queue/blocklist/{user_id}")
async def get_queue_blocklist(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return all permanently blocked queue items for a user."""
    from app.models.schema import QueueBlocklist
    require_user_ownership(current_user.id, user_id, "queue_blocklist")

    items = (await db.execute(
        select(QueueBlocklist)
        .where(QueueBlocklist.user_id == user_id)
        .order_by(QueueBlocklist.blocked_at.desc())
    )).scalars().all()

    return {
        "count": len(items),
        "items": [
            {
                "id": i.id,
                "trakt_id": i.trakt_id,
                "title": i.title,
                "item_type": i.item_type,
                "blocked_at": i.blocked_at.isoformat() if i.blocked_at else None,
            }
            for i in items
        ],
    }


@router.delete("/api/queue/block/{block_id}")
async def unblock_queue_item(
    block_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove an item from the blocklist so it can appear in future queues."""
    from app.models.schema import QueueBlocklist

    item = (await db.execute(
        select(QueueBlocklist).where(QueueBlocklist.id == block_id)
    )).scalar_one_or_none()
    if not item:
        raise HTTPException(404, "blocklist entry not found")
    require_user_ownership(current_user.id, item.user_id, "queue_unblock")

    title = item.title
    await db.delete(item)
    await db.commit()
    log.info("queue.item_unblocked", block_id=block_id, title=title)
    return {"status": "ok", "unblocked": title}


@router.get("/api/sonarr/imported")
async def get_sonarr_imported():
    """Return all Sonarr-imported episodes stored in Redis.
    Used by Airing Soon card to show 'Imported' badge.
    Returns {"{tvdb_id}:S{s}E{e}": {...}, ...}
    """
    r = await get_redis()
    cursor = b"0"
    imported: dict = {}
    while True:
        cursor, keys = await r.scan(cursor, match="sonarr_imported:*", count=200)
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            val = await r.get(key)
            if val:
                val_str = val if isinstance(val, str) else val.decode()
                # Strip prefix to get "tvdb_id:SxEx"
                short_key = key_str.replace("sonarr_imported:", "")
                try:
                    imported[short_key] = _json.loads(val_str)
                except Exception:
                    imported[short_key] = {"raw": val_str}
        if cursor == b"0" or cursor == 0:
            break
    return imported


@router.get("/api/airing-soon/{user_id}")
async def get_airing_soon(
    user_id: int,
    days: int = Query(30, ge=1, le=90),
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Upcoming episodes (for shows already in the library) with premiere/
    finale badges and a days-until-air countdown."""
    require_user_ownership(current_user.id, user_id, "airing_soon")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "user not found")

    try:
        result = await airing_alerts_svc.get_airing_soon(user, days=days)
    except Exception:
        log.exception("airing_soon.fetch_failed", user_id=user_id)
        raise HTTPException(502, "failed to fetch airing calendar from Trakt")

    alerts = result.get("items", [])
    home_releases = result.get("upcoming_home_releases", [])

    # Fire watchlist sync in background — ensures missing Radarr/Sonarr
    # items are on the Trakt/MDBList watchlist so they appear in Airing Soon.
    # Throttled: runs at most once every 30 minutes per user.
    r = await get_redis()
    throttle_key = f"watchlist_sync:last_run:{user_id}"
    already_ran = await r.get(throttle_key)
    if not already_ran:
        from app.services.watchlist_sync.service import WatchlistSyncService
        _wls = WatchlistSyncService()

        async def _bg_sync():
            try:
                await r.setex(throttle_key, 1800, "1")  # 30 min throttle
                await _wls._sync_user(user)
            except Exception:
                log.warning("watchlist_sync.background_failed", user_id=user_id)

        background_tasks.add_task(_bg_sync)

    return {
        "count": len(alerts),
        "items": alerts,
        "upcoming_home_releases": home_releases,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Feature #2 — ML Rating Predictor
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/ml/train/{user_id}")
async def train_model(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    """Trigger model training for a specific user."""
    require_user_ownership(current_user.id, user_id, "ml_training")  # ✅ SECURITY
    
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    if not user.trakt_access_token:
        raise HTTPException(400, "User not linked to Trakt")
    result = await ml_predictor_svc.train_for_user(user)
    return result


@router.get("/ml/predictions/{user_id}")
async def get_predictions(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),  # ✅ SECURITY: Input validation
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
):
    require_user_ownership(current_user.id, user_id, "predictions")  # ✅ SECURITY
    return await ml_predictor_svc.get_predictions(user_id, limit)


@router.get("/ml/model/{user_id}")
async def get_model_info(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    require_user_ownership(current_user.id, user_id, "model_info")  # ✅ SECURITY
    
    model = (await db.execute(
        select(MLModel).where(MLModel.user_id == user_id, MLModel.is_active == True)
    )).scalar_one_or_none()
    if not model:
        return {"status": "no_model"}

    # Genre insights from the latest bias analysis, if one exists
    genre_insights = None
    try:
        from app.models.schema import RatingBias
        bias = (await db.execute(
            select(RatingBias).where(RatingBias.user_id == user_id)
        )).scalar_one_or_none()
        if bias and bias.analysis_json:
            genre_stats = bias.analysis_json.get("genre_biases", {})
            top = sorted(genre_stats.items(), key=lambda kv: kv[1].get("count", 0), reverse=True)[:6]
            genre_insights = {
                genre: {
                    "delta": s.get("bias_score", 0.0),
                    "user_avg": s.get("avg", 0.0),
                    "trakt_avg": round(s.get("avg", 0.0) - s.get("bias_score", 0.0), 1),
                }
                for genre, s in top
            }
    except Exception:
        genre_insights = None

    import math
    def _finite(v):
        return v if (v is not None and isinstance(v, (int, float)) and math.isfinite(v)) else None
    mae_val = _finite(model.mae)
    r2_val = _finite(model.r2)

    return {
        "version": model.version,
        "training_samples": model.training_samples,
        "mae": mae_val,
        "r2": r2_val,
        "accuracy": r2_val,
        "feature_count": model.feature_count,
        "genre_insights": genre_insights,
        "trained_at": model.trained_at.isoformat() if model.trained_at else None,
    }


@router.get("/ml/drift/{user_id}")
async def get_rating_drift(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Return taste drift data — how feature importances shifted over training runs."""
    require_user_ownership(current_user.id, user_id, "drift")
    return await ml_predictor_svc.get_drift(user_id)


# ═══════════════════════════════════════════════════════════════════════════
# Missed Scrobble Audit
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/scrobble-audit/{user_id}")
async def scrobble_audit(
    user_id: int,
    force: bool = False,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Emby played items vs Trakt history — surface missed scrobbles."""
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
    """Backfill selected items to Trakt history.

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
    """Backfill ALL missed scrobbles to Trakt history."""
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


# ═══════════════════════════════════════════════════════════════════════════
# Feature #3 — Shared Universe Discovery
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/universes/scan")
async def scan_universes(_user: User = Depends(get_current_user)):
    """Trigger a full universe scan."""
    asyncio.create_task(universe_svc.run_scan())
    return {"status": "scan_started"}


@router.get("/api/universes")
async def list_universes():
    return await universe_svc.get_universes()


@router.post("/api/universes")
async def create_universe(payload: dict, _user: User = Depends(get_current_user)):
    """Create a new custom universe.

    Payload: {"name": "...", "description": "..."}
    """
    name = (payload.get("name") or "").strip()
    if not name:
        return {"status": "error", "reason": "name_required"}

    description = (payload.get("description") or "").strip() or None

    # Generate slug from name
    import re
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        return {"status": "error", "reason": "invalid_name"}

    async with async_session_ctx() as db:
        # Check for duplicate name or slug
        existing = (await db.execute(
            select(Universe).where(
                (Universe.name == name) | (Universe.slug == slug)
            )
        )).scalar_one_or_none()
        if existing:
            return {"status": "error", "reason": "universe_already_exists"}

        universe = Universe(
            name=name,
            slug=slug,
            description=description,
            total_items=0,
            is_custom=True,
        )
        db.add(universe)
        await db.commit()
        await db.refresh(universe)

    return {
        "status": "ok",
        "id": universe.id,
        "name": universe.name,
        "slug": universe.slug,
    }


@router.delete("/api/universes/{universe_id}")
async def delete_universe(universe_id: int, _user: User = Depends(get_current_user)):
    """Delete an entire universe and all its items."""
    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        name = universe.name
        await db.delete(universe)  # cascade deletes items
        await db.commit()
    return {"status": "ok", "removed": name}


@router.put("/api/universes/{universe_id}/settings")
async def update_universe_settings(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Update universe display settings (playlist toggle, custom name, description).

    Payload: {"playlist_enabled": bool, "custom_name": str|null, "description": str|null}
    """
    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        if "playlist_enabled" in payload:
            universe.playlist_enabled = bool(payload["playlist_enabled"])
        if "custom_name" in payload:
            val = (payload["custom_name"] or "").strip() or None
            universe.custom_name = val
        if "description" in payload:
            universe.description = (payload["description"] or "").strip() or None

        await db.commit()

    return {
        "status": "ok",
        "playlist_enabled": bool(universe.playlist_enabled),
        "custom_name": universe.custom_name,
        "description": universe.description,
    }
async def export_universes():
    """Export all universes and their items as JSON for backup/transfer."""
    async with async_session_ctx() as db:
        universes = (await db.execute(select(Universe))).scalars().all()
        result = []
        for u in universes:
            items = (await db.execute(
                select(UniverseItem).where(UniverseItem.universe_id == u.id)
                .order_by(UniverseItem.release_order)
            )).scalars().all()
            result.append({
                "name": u.name,
                "slug": u.slug,
                "description": u.description,
                "items": [
                    {
                        "title": i.title,
                        "year": i.year,
                        "item_type": i.item_type,
                        "release_order": i.release_order,
                        "chronological_order": i.chronological_order,
                        "trakt_id": i.trakt_id,
                        "imdb_id": i.imdb_id,
                        "tmdb_id": i.tmdb_id,
                    }
                    for i in items
                ],
            })
    return {"universes": result, "count": len(result)}


@router.get("/api/universes/export")
async def export_universes():
    """Export all universes and their items as JSON for backup/transfer."""
    async with async_session_ctx() as db:
        result = await db.execute(
            select(Universe).options(selectinload(Universe.items)).order_by(Universe.name)
        )
        universes = result.scalars().all()

        export = []
        for u in universes:
            items = sorted(u.items, key=lambda i: (i.release_order or 0))
            export.append({
                "name": u.name,
                "slug": u.slug,
                "description": u.description,
                "is_custom": u.is_custom,
                "playlist_enabled": u.playlist_enabled,
                "custom_name": u.custom_name,
                "items": [
                    {
                        "title": item.title,
                        "year": item.year,
                        "item_type": item.item_type,
                        "release_order": item.release_order,
                        "chronological_order": item.chronological_order,
                        "trakt_id": item.trakt_id,
                        "imdb_id": item.imdb_id,
                        "tmdb_id": item.tmdb_id,
                    }
                    for item in items
                ],
            })

    return {"universes": export, "count": len(export)}


@router.post("/api/universes/import")
async def import_universes(request: Request, _user: User = Depends(get_current_user)):
    """Import universes from JSON. Skips universes that already exist (by slug).

    Accepts either raw JSON body or multipart form upload with field 'file'.
    """
    import json as _json

    content_type = request.headers.get("content-type", "")
    if "multipart" in content_type:
        form = await request.form()
        upload = form.get("file")
        if not upload:
            return {"status": "error", "reason": "no_file"}
        raw = await upload.read()
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            return {"status": "error", "reason": "invalid_json"}
    else:
        data = await request.json()

    universe_list = data.get("universes", [])
    if not universe_list:
        return {"status": "error", "reason": "no_universes_in_payload"}

    created = 0
    skipped = 0

    async with async_session_ctx() as db:
        for u_data in universe_list:
            name = (u_data.get("name") or "").strip()
            slug = (u_data.get("slug") or "").strip()
            if not name or not slug:
                skipped += 1
                continue

            existing = (await db.execute(
                select(Universe).where(Universe.slug == slug)
            )).scalar_one_or_none()
            if existing:
                skipped += 1
                continue

            universe = Universe(
                name=name,
                slug=slug,
                description=u_data.get("description"),
                total_items=len(u_data.get("items", [])),
                is_custom=u_data.get("is_custom", False),
                playlist_enabled=u_data.get("playlist_enabled", False),
                custom_name=u_data.get("custom_name"),
            )
            db.add(universe)
            await db.flush()

            for item_data in u_data.get("items", []):
                db.add(UniverseItem(
                    universe_id=universe.id,
                    title=item_data.get("title", "Unknown"),
                    year=item_data.get("year"),
                    item_type=item_data.get("item_type", "movie"),
                    release_order=item_data.get("release_order", 0),
                    chronological_order=item_data.get("chronological_order", 0),
                    trakt_id=item_data.get("trakt_id"),
                    imdb_id=item_data.get("imdb_id"),
                    tmdb_id=item_data.get("tmdb_id"),
                    in_library=False,
                    watched=False,
                ))

            created += 1

        await db.commit()

    return {"status": "ok", "created": created, "skipped": skipped}


@router.post("/api/universes/{universe_id}/reorder")
async def reorder_universe(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Reorder items within a universe, persist to DB, and recreate Emby playlist.

    Payload: {"item_ids": [db_item_id_1, db_item_id_2, ...]}
    The order of IDs is the new watch order.  Updates release_order on
    each UniverseItem row so the order survives scans and restarts.
    """
    item_ids = payload.get("item_ids", [])
    if not item_ids:
        return {"status": "error", "reason": "no_item_ids"}

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        # Update release_order on each item to match the new order
        items = (await db.execute(
            select(UniverseItem).where(UniverseItem.universe_id == universe_id)
        )).scalars().all()
        id_to_item = {i.id: i for i in items}
        emby_ids = []
        for pos, item_id in enumerate(item_ids):
            item_id_int = int(item_id) if not isinstance(item_id, int) else item_id
            if item_id_int in id_to_item:
                id_to_item[item_id_int].release_order = pos + 1
                if id_to_item[item_id_int].emby_item_id:
                    emby_ids.append(id_to_item[item_id_int].emby_item_id)

        await db.commit()

        first_user = (await db.execute(
            select(User).order_by(User.id)
        )).scalars().first()
        emby_user_id = first_user.emby_user_id if first_user else None

    # Recreate Emby playlist with new order
    if emby_ids:
        emby = EmbyClient()
        display_name = universe.custom_name or universe.name
        playlist_name = f"🌌 {display_name}"
        try:
            playlist_id = await emby.recreate_playlist(
                playlist_name, emby_ids, user_id=emby_user_id,
            )
            if playlist_id and universe.description:
                await emby.set_playlist_overview(
                    playlist_id, universe.description,
                    user_id=emby_user_id,
                )
        finally:
            await emby.close()
    else:
        playlist_id = None

    log.info("universe.reordered", universe_id=universe_id, items=len(item_ids))
    return {"status": "ok", "playlist_id": playlist_id, "items": len(item_ids)}


@router.post("/api/universes/{universe_id}/items")
async def add_universe_item(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Add a custom item to a universe.

    Payload: {"title": "...", "year": 2024, "imdb_id": "tt1234567", "item_type": "movie"}
    """
    title = (payload.get("title") or "").strip()
    if not title:
        return {"status": "error", "reason": "title_required"}

    year = payload.get("year")
    imdb_id = (payload.get("imdb_id") or "").strip() or None
    tmdb_id = (payload.get("tmdb_id") or "").strip() or None
    item_type = payload.get("item_type", "movie")

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        # Determine next release_order
        max_order = (await db.execute(
            select(func.max(UniverseItem.release_order)).where(
                UniverseItem.universe_id == universe_id
            )
        )).scalar() or 0

        new_item = UniverseItem(
            universe_id=universe_id,
            title=title,
            year=int(year) if year else None,
            imdb_id=imdb_id,
            tmdb_id=tmdb_id,
            item_type=item_type,
            release_order=max_order + 1,
            chronological_order=max_order + 1,
            in_library=False,
            watched=False,
        )
        db.add(new_item)
        universe.total_items = (universe.total_items or 0) + 1
        await db.commit()
        await db.refresh(new_item)

    # Trigger a library match for the new item
    asyncio.create_task(universe_svc.run_scan())

    return {
        "status": "ok",
        "item_id": new_item.id,
        "title": new_item.title,
        "message": f"Added '{title}' — library match running in background",
    }


@router.delete("/api/universes/{universe_id}/items/{item_id}")
async def remove_universe_item(universe_id: int, item_id: int, _user: User = Depends(get_current_user)):
    """Remove a custom item from a universe."""
    async with async_session_ctx() as db:
        item = (await db.execute(
            select(UniverseItem).where(
                UniverseItem.id == item_id,
                UniverseItem.universe_id == universe_id,
            )
        )).scalar_one_or_none()
        if not item:
            return {"status": "error", "reason": "item_not_found"}

        title = item.title
        await db.delete(item)

        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if universe and universe.total_items:
            universe.total_items = max(0, universe.total_items - 1)

        await db.commit()
    return {"status": "ok", "removed": title}


@router.get("/api/universes/auto-discover/setting")
async def get_auto_discover_setting(db: AsyncSession = Depends(get_db)):
    """Return auto-discovery setting: enabled | disabled | unset (never configured)."""
    val = await _get_setting(db, "universe_auto_discover", "")
    if val == "":
        return {"status": "unset"}
    return {"status": val}


@router.put("/api/universes/auto-discover/setting")
async def set_auto_discover_setting(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Set auto-discovery to 'enabled' or 'disabled'."""
    enabled = payload.get("enabled", False)
    val = "enabled" if enabled else "disabled"
    await _put_setting(db, "universe_auto_discover", val)
    await db.commit()
    return {"status": val}


# ═══════════════════════════════════════════════════════════════════════════
# Feature #4 — Watch Party
# ═══════════════════════════════════════════════════════════════════════════

class CreatePartyRequest(BaseModel):
    host_user_id: int
    emby_item_id: str | None = None


class JoinPartyRequest(BaseModel):
    code: str
    user_id: int


@router.post("/party/create")
async def create_party(body: CreatePartyRequest, _user: User = Depends(get_current_user)):
    try:
        return await watch_party_svc.create_party(body.host_user_id, body.emby_item_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/party/join")
async def join_party(body: JoinPartyRequest, _user: User = Depends(get_current_user)):
    result = await watch_party_svc.join_party(body.code, body.user_id)
    if not result:
        raise HTTPException(404, "Party not found or has ended")
    return result


@router.post("/party/{code}/end")
async def end_party(code: str, _user: User = Depends(get_current_user)):
    await watch_party_svc.end_party(code)
    return {"status": "ended"}


@router.post("/party/{code}/start")
async def start_party_playback(code: str, _user: User = Depends(get_current_user)):
    """Start playback on all participants' Emby sessions simultaneously."""
    return await watch_party_svc.start_playback(code)


@router.get("/party/{code}/sessions")
async def list_party_sessions(code: str):
    """List active Emby sessions for party participants (device picker)."""
    return await watch_party_svc.list_sessions_for_party(code)


@router.post("/party/{code}/start-selected")
async def start_selected_playback(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Start playback on specific devices only.

    Payload: {"session_ids": ["sid1", "sid2"], "emby_item_id": "optional_override",
              "start_position_ticks": 0}
    """
    session_ids = payload.get("session_ids", [])
    item_id = payload.get("emby_item_id")
    start_ticks = int(payload.get("start_position_ticks", 0))
    if not session_ids:
        raise HTTPException(400, "No sessions selected")
    return await watch_party_svc.start_playback_on_sessions(
        code, session_ids, item_id, start_position_ticks=start_ticks,
    )


@router.post("/party/{code}/pause")
async def pause_party_playback(code: str, _user: User = Depends(get_current_user)):
    """Toggle pause/play on all participants' Emby sessions."""
    return await watch_party_svc.pause_all(code)


@router.post("/party/{code}/seek")
async def seek_party_playback(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Seek all participants to a specific position.

    Payload: {"position_ticks": int}
    """
    position_ticks = payload.get("position_ticks", 0)
    return await watch_party_svc.seek_all(code, position_ticks)


# ═══════════════════════════════════════════════════════════════════════════
# Theater Mode — Pick Together
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/party/{code}/pick-together")
async def start_pick_together(code: str, payload: dict = None, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Start a Pick Together voting round for a watch party.

    Pulls top candidates from each participant's smart queue (in-library only),
    dedupes, and stores the voting state in Redis.  Emits pick_started to all
    members via Socket.IO.

    Optional payload: {"candidate_count": 8}
    """
    from app.services.watch_party.service import sio

    r = await get_redis()
    state = await r.hgetall(f"party:{code}")
    if not state:
        raise HTTPException(404, "Party not found")

    party_id = int(state["id"])
    candidate_count = (payload or {}).get("candidate_count", 8)
    if candidate_count < 4:
        candidate_count = 4
    if candidate_count > 20:
        candidate_count = 20

    # Collect participant user IDs
    participants = (await db.execute(
        select(WatchPartyParticipant.user_id)
        .where(WatchPartyParticipant.party_id == party_id)
    )).scalars().all()

    if not participants:
        raise HTTPException(400, "No participants in party")

    # Pull top queue items from each participant (in-library only, unplayed)
    seen_titles: set[str] = set()
    candidates: list[dict] = []

    for uid in participants:
        items = (await db.execute(
            select(QueueItem)
            .where(
                QueueItem.user_id == uid,
                QueueItem.played == False,
                QueueItem.in_library == True,
                QueueItem.emby_item_id.isnot(None),
            )
            .order_by(QueueItem.score.desc())
            .limit(candidate_count * 2)  # over-fetch to account for dedup
        )).scalars().all()

        for item in items:
            key = (item.title or "").lower().strip()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            candidates.append({
                "emby_item_id": item.emby_item_id,
                "title": item.title,
                "type": item.item_type,
                "year": (item.metadata_json or {}).get("year"),
                "score": round(item.score, 2) if item.score else 0,
                "source": item.source,
                "votes": 0,
                "voters": [],
            })

    # Sort by score descending, take top N
    candidates.sort(key=lambda c: c["score"], reverse=True)
    candidates = candidates[:candidate_count]

    if not candidates:
        raise HTTPException(400, "No queue items available — run a queue refresh first")

    # Store voting state in Redis
    import json as _json
    pick_state = {
        "candidates": candidates,
        "phase": "voting",  # voting | countdown | done
        "winner_idx": None,
    }
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Broadcast to room
    await sio.emit("pick_started", {"candidates": candidates}, room=code)

    log.info("theater_mode.pick_started", code=code,
             candidates=len(candidates), participants=len(participants))
    return {"status": "ok", "candidates": candidates}


@router.get("/party/{code}/pick-status")
async def get_pick_status(code: str):
    """Get current Pick Together voting state."""
    import json as _json
    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        return {"status": "none"}
    return _json.loads(raw)


@router.post("/party/{code}/vote")
async def cast_vote(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Cast a vote for a candidate.

    Payload: {"candidate_idx": 0, "user_id": 1}
    """
    import json as _json
    from app.services.watch_party.service import sio

    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        raise HTTPException(404, "No active Pick Together session")

    pick_state = _json.loads(raw)
    if pick_state.get("phase") != "voting":
        raise HTTPException(400, "Voting is not active")

    idx = payload.get("candidate_idx")
    user_id = payload.get("user_id")
    if idx is None or not isinstance(idx, int):
        raise HTTPException(400, "candidate_idx required")

    candidates = pick_state["candidates"]
    if idx < 0 or idx >= len(candidates):
        raise HTTPException(400, "Invalid candidate index")

    # Remove previous vote by this user (one vote per user)
    for c in candidates:
        if user_id in c.get("voters", []):
            c["voters"].remove(user_id)
            c["votes"] = len(c["voters"])

    # Cast new vote
    candidates[idx].setdefault("voters", []).append(user_id)
    candidates[idx]["votes"] = len(candidates[idx]["voters"])

    pick_state["candidates"] = candidates
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Broadcast vote update (strip voter IDs for privacy, just send counts)
    vote_summary = [{"title": c["title"], "votes": c["votes"], "idx": i}
                    for i, c in enumerate(candidates)]
    await sio.emit("vote_update", {"votes": vote_summary}, room=code)

    return {"status": "ok", "votes": vote_summary}


@router.post("/party/{code}/pick-winner")
async def confirm_pick_winner(code: str, payload: dict = None, _user: User = Depends(get_current_user)):
    """Host confirms the winner (auto-selects top vote, or override).

    Payload: {"winner_idx": 0}  (optional — defaults to highest vote)
    Sets the party item to the winner. Host then uses device picker + start.
    """
    import json as _json
    from app.services.watch_party.service import sio

    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        raise HTTPException(404, "No active Pick Together session")

    pick_state = _json.loads(raw)
    candidates = pick_state["candidates"]

    # Determine winner
    winner_idx = (payload or {}).get("winner_idx")
    if winner_idx is None:
        # Auto-pick highest votes, tie-break by score
        best_idx = 0
        best_votes = candidates[0].get("votes", 0)
        best_score = candidates[0].get("score", 0)
        for i, c in enumerate(candidates):
            v = c.get("votes", 0)
            s = c.get("score", 0)
            if v > best_votes or (v == best_votes and s > best_score):
                best_idx = i
                best_votes = v
                best_score = s
        winner_idx = best_idx

    if winner_idx < 0 or winner_idx >= len(candidates):
        raise HTTPException(400, "Invalid winner index")

    winner = candidates[winner_idx]
    pick_state["phase"] = "done"
    pick_state["winner_idx"] = winner_idx
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Update party item to the winner
    emby_item_id = winner.get("emby_item_id", "")
    winner_title = winner.get("title", "")
    display_title = f"Pick Together Lobby - {winner_title}" if winner_title else "Pick Together Lobby"
    if emby_item_id:
        await r.hset(f"party:{code}", mapping={
            "item": emby_item_id,
            "title": display_title,
        })

    # Update DB record so recent parties list shows the item played
    state = await r.hgetall(f"party:{code}")
    party_id = int(state.get("id", 0))
    if party_id:
        async with async_session_ctx() as db_sess:
            from sqlalchemy import update as sa_update
            await db_sess.execute(
                sa_update(WatchParty)
                .where(WatchParty.id == party_id)
                .values(title=display_title, emby_item_id=emby_item_id)
            )
            await db_sess.commit()

    # Broadcast winner — no countdown here, host uses device picker next
    await sio.emit("pick_winner", {
        "winner": winner,
        "winner_idx": winner_idx,
    }, room=code)

    log.info("theater_mode.winner_selected", code=code,
             title=winner.get("title"), votes=winner.get("votes", 0))
    return {"status": "ok", "winner": winner}


@router.post("/party/{code}/start-with-countdown")
async def start_with_countdown(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Start playback on selected devices after a 20-second countdown.

    Payload: {"session_ids": ["sid1", "sid2"]}
    Broadcasts countdown to all party members via Socket.IO, then starts
    playback on the specified sessions.
    """
    import asyncio
    from app.services.watch_party.service import sio

    session_ids = payload.get("session_ids", [])
    if not session_ids:
        raise HTTPException(400, "No sessions selected")

    r = await get_redis()
    state = await r.hgetall(f"party:{code}")
    if not state:
        raise HTTPException(404, "Party not found")

    item_id = state.get("item", "")
    title = state.get("title", "")

    # Broadcast countdown start to all members
    await sio.emit("countdown_started", {
        "title": title,
        "countdown_seconds": 20,
    }, room=code)

    async def _countdown_then_play():
        for remaining in range(19, -1, -1):
            await asyncio.sleep(1)
            await sio.emit("countdown_tick", {"remaining": remaining}, room=code)

        # Countdown finished — start playback on selected devices
        result = await watch_party_svc.start_playback_on_sessions(
            code, session_ids, item_id, start_position_ticks=0,
        )
        await sio.emit("countdown_play", {
            "started": result.get("started", 0),
        }, room=code)

    asyncio.create_task(_countdown_then_play())

    log.info("theater_mode.countdown_started", code=code,
             title=title, devices=len(session_ids))
    return {"status": "ok", "countdown_seconds": 20}



@router.post("/api/pick-together/solo")
async def solo_pick_together(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Standalone Pick Together (no party needed).

    For in-the-room use: pulls candidates from a user's queue for group
    decision-making on a single device.

    Payload: {"user_id": 1, "candidate_count": 8}
    """
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(400, "user_id required")

    candidate_count = payload.get("candidate_count", 8)
    if candidate_count < 4:
        candidate_count = 4
    if candidate_count > 20:
        candidate_count = 20

    items = (await db.execute(
        select(QueueItem)
        .where(
            QueueItem.user_id == user_id,
            QueueItem.played == False,
            QueueItem.in_library == True,
            QueueItem.emby_item_id.isnot(None),
        )
        .order_by(QueueItem.score.desc())
        .limit(candidate_count)
    )).scalars().all()

    if not items:
        raise HTTPException(400, "No queue items available — run a queue refresh first")

    candidates = [
        {
            "emby_item_id": i.emby_item_id,
            "title": i.title,
            "type": i.item_type,
            "year": (i.metadata_json or {}).get("year"),
            "score": round(i.score, 2) if i.score else 0,
            "source": i.source,
            "votes": 0,
        }
        for i in items
    ]

    return {"status": "ok", "candidates": candidates}


@router.get("/party/{code}")
async def get_party(code: str):
    result = await watch_party_svc.get_party(code)
    if not result:
        raise HTTPException(404, "Party not found")
    return result


@router.get("/parties")
async def list_parties():
    return await watch_party_svc.list_active_parties()


@router.get("/parties/recent")
async def list_recent_parties(limit: int = Query(10, ge=1, le=50)):
    """Return recently ended parties for the watch party lobby."""
    return await watch_party_svc.list_recent_parties(limit)


# ═══════════════════════════════════════════════════════════════════════════
# Emby Webhook receiver
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# Webhook — Sonarr (import complete / grab / series added)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/webhook/sonarr")
@router.post("/webhook/sonarr/")
async def sonarr_webhook(request: Request):
    """Receive Sonarr webhooks for import/grab/series events.

    On 'Download' (import complete):
      - Stores imported episode info in Redis keyed by TVDB ID + SxxExx
      - Airing Soon card reads this to show 'Imported' badge instead of 'In Sonarr'

    On 'Grab':
      - Logs the grab event to the activity log

    On 'SeriesAdd':
      - Logs the new series event
    """
    try:
        payload = await request.json()
    except Exception:
        return {"status": "error", "message": "invalid JSON"}

    event_type = payload.get("eventType", "")
    series = payload.get("series", {})
    episodes = payload.get("episodes", [])

    series_title = series.get("title", "Unknown")
    tvdb_id = series.get("tvdbId")

    log.info("webhook.sonarr", event_type=event_type, series=series_title,
             tvdb_id=tvdb_id, episodes=len(episodes))

    if event_type == "Test":
        await _activity_log(f"📡 Sonarr test webhook received", category="webhook")
        return {"status": "ok", "event": "Test"}

    if event_type == "Download":
        # Import complete — store each imported episode in Redis
        r = await get_redis()
        imported_count = 0
        for ep in episodes:
            s_num = ep.get("seasonNumber", 0)
            e_num = ep.get("episodeNumber", 0)
            ep_title = ep.get("title", "")

            if tvdb_id and s_num and e_num:
                # Key format: sonarr_imported:{tvdb_id}:S{s}E{e}
                redis_key = f"sonarr_imported:{tvdb_id}:S{s_num}E{e_num}"
                import_data = _json.dumps({
                    "series": series_title,
                    "season": s_num,
                    "episode": e_num,
                    "episode_title": ep_title,
                    "quality": ep.get("quality", ""),
                    "imported_at": datetime.now(timezone.utc).isoformat(),
                })
                # TTL 30 days — airing soon card only shows ~30 days ahead
                await r.setex(redis_key, 30 * 86400, import_data)
                imported_count += 1

                await _activity_log(
                    f"📥 Sonarr imported: {series_title} S{s_num:02d}E{e_num:02d}"
                    + (f" — {ep_title}" if ep_title else ""),
                    category="webhook",
                )

        log.info("webhook.sonarr_import_stored", series=series_title,
                 tvdb_id=tvdb_id, episodes_imported=imported_count)
        return {"status": "ok", "event": event_type, "imported": imported_count}

    if event_type == "Grab":
        ep_list = ", ".join(
            f"S{ep.get('seasonNumber', 0):02d}E{ep.get('episodeNumber', 0):02d}"
            for ep in episodes
        )
        await _activity_log(
            f"🎣 Sonarr grabbed: {series_title} {ep_list}",
            category="webhook",
        )
        return {"status": "ok", "event": event_type}

    if event_type == "SeriesAdd":
        await _activity_log(
            f"📺 Sonarr series added: {series_title}",
            category="webhook",
        )
        return {"status": "ok", "event": event_type}

    # Any other event — just log it
    await _activity_log(
        f"📡 Sonarr webhook: {event_type} — {series_title}",
        category="webhook",
    )
    return {"status": "ok", "event": event_type}


# ═══════════════════════════════════════════════════════════════════════════
# Webhook — Emby (playback, scrobble, mark played)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/webhook/emby")
@router.post("/")
async def emby_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """Receive Emby webhooks for real-time events.

    Emby sends webhooks as either:
      - application/json (raw JSON body)
      - multipart/form-data or form-urlencoded with a 'data' field containing JSON

    Also registered at POST / as a fallback since Emby may be configured
    with just the root URL.

    Event types:
      - PlaybackStart: user started playing an item
      - PlaybackStop: user stopped playing an item
      - ItemMarkedPlayed: user marked item as watched

    On PlaybackStop / ItemMarkedPlayed:
      1. Record feedback for Smart Queue scoring
      2. Scrobble to Trakt watch history (if user has linked Trakt account)
    """
    import json as _json

    # Parse payload from whatever format Emby sends
    content_type = request.headers.get("content-type", "")
    payload = {}

    try:
        if "application/json" in content_type:
            payload = await request.json()
        elif "form" in content_type or "multipart" in content_type:
            form = await request.form()
            raw = form.get("data", "{}")
            payload = _json.loads(raw) if isinstance(raw, str) else {}
        else:
            # Try JSON first, fall back to reading body as text
            body = await request.body()
            if body:
                try:
                    payload = _json.loads(body)
                except (ValueError, _json.JSONDecodeError):
                    payload = {}
    except Exception:
        return {"status": "ignored", "reason": "unparseable_body"}

    if not payload:
        return {"status": "ignored", "reason": "empty_payload"}

    # Emby uses "Event" (not "EventType") with lowercase dot-notation
    # e.g. "playback.stop", "item.markplayed", "system.webhooktest"
    event_type = payload.get("Event", "") or payload.get("EventType", "")
    item_data = payload.get("Item", {})
    user_data = payload.get("User", {})
    session_data = payload.get("Session", {})

    item_name = item_data.get("Name", "")
    item_type_raw = item_data.get("Type", "")
    emby_item_id = item_data.get("Id", "")
    emby_user_id = user_data.get("Id", "")
    emby_username = user_data.get("Name", "")

    # Unified display name for activity logs:
    #   Movies: "Movie Title"
    #   Episodes: "Series Name : Episode Name : S1E1"
    if item_type_raw == "Episode":
        _sn = item_data.get("SeriesName", "")
        _snum = item_data.get("ParentIndexNumber", "")
        _enum = item_data.get("IndexNumber", "")
        _ep_tag = f"S{_snum}E{_enum}" if _snum and _enum else ""
        parts = [p for p in (_sn, item_name, _ep_tag) if p]
        display_name = " : ".join(parts) if parts else item_name
    else:
        display_name = item_name

    # Test webhooks and events without an item are acknowledged but not processed
    if not emby_item_id:
        return {"status": "ok", "event": event_type, "note": "no item data"}

    # Library-level events (library.new, item.added, item.removed) don't require a user
    event_lower = event_type.lower()
    is_library_event = event_lower in ("library.new", "librarynew",
                                        "item.added", "itemadded")
    is_library_removed = event_lower in ("library.deleted", "librarydeleted",
                                          "item.removed", "itemremoved")

    if not emby_user_id and not is_library_event and not is_library_removed:
        return {"status": "ok", "event": event_type, "note": "no user data"}

    # Find our user (may be None for library events)
    user = None
    if emby_user_id:
        user = (await db.execute(
            select(User).where(User.emby_user_id == emby_user_id)
        )).scalar_one_or_none()

    if not user and not is_library_event and not is_library_removed:
        await _activity_log(
            f"Webhook ignored: unknown Emby user {emby_username} ({emby_user_id})",
            category="webhook",
        )
        return {"status": "ignored", "reason": "unknown_user"}

    trakt_synced = False

    # -- Helper: build a Trakt client with auto-refresh for this user ---------
    async def _get_trakt_client():
        async def _on_refresh(access, refresh, expires):
            async with async_session() as _db:
                u = await _db.get(User, user.id)
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await _db.commit()

        return TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=_on_refresh,
        )

    # -- Helper: build Trakt scrobble payload from webhook item data ----------
    def _build_scrobble_payload():
        provider_ids = item_data.get("ProviderIds", {})
        trakt_ids = {}
        if provider_ids.get("Imdb"):
            trakt_ids["imdb"] = provider_ids["Imdb"]
        if provider_ids.get("Tmdb"):
            trakt_ids["tmdb"] = int(provider_ids["Tmdb"])
        if provider_ids.get("Tvdb"):
            trakt_ids["tvdb"] = int(provider_ids["Tvdb"])
        if not trakt_ids:
            return None

        if item_type_raw == "Movie":
            return {"movie": {"ids": trakt_ids}}
        elif item_type_raw == "Episode":
            # Try to get series-level provider IDs from the webhook payload
            series_ids = {}
            series_provider = item_data.get("SeriesProviderIds", {})
            if series_provider.get("Imdb"):
                series_ids["imdb"] = series_provider["Imdb"]
            if series_provider.get("Tmdb"):
                series_ids["tmdb"] = int(series_provider["Tmdb"])
            if series_provider.get("Tvdb"):
                series_ids["tvdb"] = int(series_provider["Tvdb"])

            episode_obj = {
                "season": item_data.get("ParentIndexNumber", 1),
                "number": item_data.get("IndexNumber", 1),
            }

            if series_ids:
                # Best case: we have show-level IDs + season/episode numbers
                return {"show": {"ids": series_ids}, "episode": episode_obj}
            else:
                # Fallback: put episode's own IDs on the episode object directly.
                # Trakt accepts episode.ids as an alternative to show.ids + season/number.
                episode_obj["ids"] = trakt_ids
                return {"episode": episode_obj}
        return None

    # -- Helper: get MDBList client for scrobble if enabled --------------------
    async def _get_mdblist_client_for_scrobble():
        """Build an MDBListClient using the stored API key, if MDBList is active."""
        providers = await _get_active_providers()
        if "mdblist" not in providers:
            return None
        key = await _get_mdblist_key()
        if not key:
            return None
        from app.utils.mdblist_client import MDBListClient
        return MDBListClient(api_key=key)

    # -- Helper: resolve series-level provider IDs for an episode ---------------
    async def _resolve_series_ids() -> dict:
        """Get series-level provider IDs for the current episode item.
        Three fallback levels:
          1. SeriesProviderIds from the webhook payload (fastest)
          2. Library cache lookup by SeriesName
          3. Emby API lookup by SeriesId (network call, last resort)
        Returns dict like {"imdb": "tt...", "tmdb": 12345, "tvdb": 67890} or {}.
        """
        # Level 1: SeriesProviderIds from webhook
        series_provider = item_data.get("SeriesProviderIds", {})
        result = {}
        for key in ("Imdb", "Tmdb", "Tvdb"):
            val = series_provider.get(key)
            if val:
                result[key.lower()] = int(val) if key != "Imdb" else val
        if result:
            return result

        # Level 2: Library cache by series name
        series_name = item_data.get("SeriesName", "")
        if series_name:
            cached = await LibraryCache.find_by_title(series_name, item_type="Series")
            if cached:
                cpids = cached.get("provider_ids", {})
                for key in ("Imdb", "Tmdb", "Tvdb"):
                    val = cpids.get(key)
                    if val:
                        result[key.lower()] = int(val) if key != "Imdb" else val
                if result:
                    log.debug("webhook.series_ids_from_cache", series=series_name, ids=result)
                    return result

        # Level 3: Emby API lookup by SeriesId
        series_emby_id = item_data.get("SeriesId")
        if series_emby_id:
            try:
                async with EmbyClient() as emby:
                    series_item = await emby.get_item_safe(series_emby_id)
                    if series_item:
                        spids = series_item.get("ProviderIds", {})
                        for key in ("Imdb", "Tmdb", "Tvdb"):
                            val = spids.get(key)
                            if val:
                                result[key.lower()] = int(val) if key != "Imdb" else val
                        if result:
                            log.debug("webhook.series_ids_from_emby", series=series_name,
                                      series_emby_id=series_emby_id, ids=result)
                            return result
            except Exception as e:
                log.debug("webhook.series_id_emby_lookup_failed",
                          series_emby_id=series_emby_id, error=str(e)[:80])

        return result

    # -- Helper: build MDBList scrobble payload --------------------------------
    async def _build_mdblist_scrobble_payload():
        """Build MDBList-compatible scrobble payload from webhook item data.
        Supports movies and TV episodes.
        Movie IDs accepted: imdb, tmdb, trakt, kitsu, mdblist (NOT tvdb).
        Show/episode IDs accepted: imdb, tmdb, trakt, tvdb, mdblist.
        Episode payload uses MDBList's nested format:
          {"show": {"ids": {...}, "season": {"number": N, "episode": {"number": M}}}}
        """
        provider_ids = item_data.get("ProviderIds", {})

        if item_type_raw == "Movie":
            mdb_ids = {}
            if provider_ids.get("Imdb"):
                mdb_ids["imdb"] = provider_ids["Imdb"]
            if provider_ids.get("Tmdb"):
                mdb_ids["tmdb"] = int(provider_ids["Tmdb"])
            # Note: tvdb is NOT supported by MDBList scrobble for movies
            if not mdb_ids:
                return None
            return {"movie": {"ids": mdb_ids}}

        elif item_type_raw == "Episode":
            show_ids = await _resolve_series_ids()

            if not show_ids:
                return None

            season_num = item_data.get("ParentIndexNumber", 1)
            episode_num = item_data.get("IndexNumber", 1)

            return {
                "show": {
                    "ids": show_ids,
                    "season": {
                        "number": season_num,
                        "episode": {"number": episode_num},
                    },
                },
            }

        return None

    # -- Helper: scrobble to MDBList (fire-and-forget, non-blocking) -----------
    async def _mdblist_scrobble(action: str, progress: float):
        """Send a scrobble event to MDBList if enabled. Never raises.
        Fires for movies and TV episodes.
        """
        try:
            mdb = await _get_mdblist_client_for_scrobble()
            if not mdb:
                return
            payload = await _build_mdblist_scrobble_payload()
            if not payload:
                return
            log.debug("webhook.mdblist_scrobble_payload",
                      action=action, progress=round(progress, 1),
                      payload=payload, item_type=item_type_raw)
            try:
                progress_pct = round(progress, 1)
                pos_secs = _get_position_ticks() // 10000000
                mm, ss = divmod(pos_secs, 60)
                time_str = f"{mm}:{ss:02d}"

                if action == "start":
                    await mdb.scrobble_start(payload, progress=progress)
                    await _activity_log(f"📋 MDBList watching: {display_name}", category="trakt")
                elif action == "pause":
                    await mdb.scrobble_pause(payload, progress=progress)
                    await _activity_log(
                        f"📋 MDBList paused: {display_name} at {time_str} ({progress_pct}%)",
                        category="trakt",
                    )
                elif action == "stop":
                    result = await mdb.scrobble_stop(payload, progress=progress)
                    await _activity_log(
                        f"📋 MDBList stop: {display_name} ({progress_pct}%)",
                        category="trakt",
                    )
                    return result
                elif action == "resume":
                    await mdb.scrobble_start(payload, progress=progress)
                    await _activity_log(
                        f"📋 MDBList resumed: {display_name} at {time_str} ({progress_pct}%)",
                        category="trakt",
                    )
            finally:
                await mdb.close()
        except Exception as e:
            import re
            err_str = re.sub(r'apikey=[^&\s\'"]+', 'apikey=***', str(e)[:200])
            # Try to extract response body for 400 errors
            resp_body = ""
            status_code = ""
            if hasattr(e, 'response') and e.response is not None:
                try:
                    status_code = str(e.response.status_code)
                    resp_body = e.response.text[:200]
                except Exception:
                    pass
            log.warning(f"webhook.mdblist_scrobble_{action}_failed",
                        error=err_str, response_body=resp_body)
            # Include status + body in activity log so it's visible on dashboard
            detail = f" [{status_code}]" if status_code else ""
            if resp_body:
                detail += f" {resp_body[:120]}"
            await _activity_log(f"⚠ MDBList {action} failed: {display_name}{detail}", category="trakt")

    # -- Helper: add to MDBList watched history --------------------------------
    async def _mdblist_add_to_history():
        """Add item to MDBList watched history if enabled. Never raises."""
        try:
            mdb = await _get_mdblist_client_for_scrobble()
            if not mdb:
                return
            provider_ids = item_data.get("ProviderIds", {})
            ids: dict = {}
            if provider_ids.get("Imdb"):
                ids["imdb"] = provider_ids["Imdb"]
            if provider_ids.get("Tmdb"):
                ids["tmdb"] = int(provider_ids["Tmdb"])
            if provider_ids.get("Tvdb"):
                ids["tvdb"] = int(provider_ids["Tvdb"])
            if not ids:
                return
            watched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            try:
                if item_type_raw == "Movie":
                    await mdb.add_to_watched(
                        movies=[{"ids": ids, "watched_at": watched_at}],
                    )
                elif item_type_raw == "Episode":
                    series_ids = await _resolve_series_ids()
                    show_ids = series_ids or ids
                    season_num = item_data.get("ParentIndexNumber", 1)
                    episode_num = item_data.get("IndexNumber", 1)
                    await mdb.add_to_watched(
                        shows=[{
                            "ids": show_ids,
                            "seasons": [{"number": season_num, "episodes": [{"number": episode_num, "watched_at": watched_at}]}],
                        }],
                    )
                if item_type_raw == "Movie":
                    await _activity_log(f"✓ Synced to MDBList: {display_name}", category="trakt")
                elif item_type_raw == "Episode":
                    season_num = item_data.get("ParentIndexNumber", "?")
                    episode_num = item_data.get("IndexNumber", "?")
                    await _activity_log(
                        f"✓ Synced to MDBList: {display_name}",
                        category="trakt",
                    )
            finally:
                await mdb.close()
        except Exception as e:
            log.warning("webhook.mdblist_history_failed", error=str(e)[:120])

    # -- Helper: extract playback position ticks from webhook payload ---------
    def _get_position_ticks():
        """Emby sends position in various locations depending on event type."""
        # Try Session.PlayState.PositionTicks (most common)
        pos = session_data.get("PlayState", {}).get("PositionTicks", 0)
        if pos:
            return pos
        # Try root-level PlaybackPositionTicks
        pos = payload.get("PlaybackPositionTicks", 0)
        if pos:
            return pos
        # Try PlaybackInfo
        pos = payload.get("PlaybackInfo", {}).get("PositionTicks", 0)
        return pos

    # -- Helper: calculate playback progress as 0-100 -------------------------
    def _calc_progress():
        pos = _get_position_ticks()
        duration = item_data.get("RunTimeTicks", 0)
        if duration > 0 and pos > 0:
            return min(99.9, max(1.0, pos / duration * 100))
        # Trakt rejects progress < 1% with 422, so default to 1% minimum
        return 1.0

    # ── Match Emby event names ───────────────────────────────────────────────
    # Emby uses lowercase dot-notation (playback.start) but some builds use
    # PascalCase. Normalise to lowercase for matching (already set above).

    is_play_start = event_lower in ("playback.start", "playbackstart")
    is_play_stop = event_lower in ("playback.stop", "playbackstop")
    is_play_pause = event_lower in ("playback.pause", "playbackpause")
    is_play_unpause = event_lower in ("playback.unpause", "playbackunpause",
                                       "playback.resume", "playbackresume")
    is_mark_played = event_lower in ("item.markplayed", "item.markedplayed",
                                      "itemmarkplayed", "itemmarkedplayed")
    is_watched = is_play_stop or is_mark_played

    # ── Pause/unpause suppression ───────────────────────────────────────────
    # Three layers of dedup for pause/unpause events:
    #   1. Watch party seek: seek_all() fires pause→seek→resume per session
    #   2. Init burst: Emby fires rapid pause/unpause during playback start
    #      (buffering, player initialisation). Suppressed for 10s after start.
    #   3. Same-event debounce: duplicate pause or unpause for the same
    #      user+item within 5s is suppressed.
    if is_play_pause or is_play_unpause:
        session_id = session_data.get("Id", "")
        try:
            r = await get_redis()
            # Layer 1: watch party seek
            if session_id and await r.get(f"party_seek_suppress:{session_id}"):
                return {"status": "suppressed", "reason": "party_seek_in_progress"}
            # Layer 2: init burst (set by playback.start above)
            if user and await r.get(f"scrobble_init_suppress:{user.id}:{emby_item_id}"):
                return {"status": "suppressed", "reason": "init_burst"}
            # Layer 3: same-event debounce (5s window)
            if user:
                evt_key = "pause" if is_play_pause else "unpause"
                dedup_key = f"scrobble_dedup:{user.id}:{emby_item_id}:{evt_key}"
                if await r.get(dedup_key):
                    return {"status": "suppressed", "reason": "debounce"}
                await r.set(dedup_key, "1", ex=5)
        except Exception:
            pass

    # ── Helper: invalidate Continue Watching cache ─────────────────────────
    async def _invalidate_continue_watching():
        """Delete the Continue Watching Redis cache for this user so
        the next dashboard load fetches fresh data from Emby."""
        try:
            r = await get_redis()
            key = f"continue_watching_v2:{user.id}"
            deleted = await r.delete(key)
            if deleted:
                log.debug("webhook.continue_watching_cache_invalidated", user=user.id)
        except Exception:
            pass  # non-critical

    # ── playback.start → Trakt scrobble/start ("Watching…") ─────────────────
    if is_play_start:
        # Invalidate continue watching cache — a new resume point is being created
        await _invalidate_continue_watching()

        # Set init-burst suppression flag — Emby fires rapid pause/unpause
        # webhooks during playback initialisation (buffering, seeking).
        # Suppress those for 10 seconds after the start event.
        if user:
            try:
                r = await get_redis()
                await r.set(
                    f"scrobble_init_suppress:{user.id}:{emby_item_id}",
                    "1", ex=10,
                )
            except Exception:
                pass

        if user.trakt_access_token:
            try:
                trakt = await _get_trakt_client()
                scrobble = _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    await trakt.scrobble_start(scrobble, progress=progress)
                    trakt_synced = True
                    await _activity_log(f"▶ Trakt watching: {display_name}", category="trakt")
            except Exception as e:
                log.warning("webhook.trakt_scrobble_start_failed", error=str(e))
                await _activity_log(f"⚠ Trakt start failed: {display_name} — {str(e)[:80]}", category="trakt")
        # MDBList scrobble start
        asyncio.create_task(_mdblist_scrobble("start", _calc_progress()))
        return {"status": "received", "event": event_type, "trakt_synced": trakt_synced}

    # ── playback.pause → Trakt scrobble/pause ───────────────────────────────
    if is_play_pause:
        if user.trakt_access_token:
            progress = _calc_progress()
            # Trakt rejects pause at >80% progress (considers it watched).
            # Skip the scrobble — the stop event that follows will sync history.
            if progress > 80:
                await _activity_log(
                    f"⏸ Paused near end: {item_name} ({progress:.0f}%) — skipped scrobble, stop will sync",
                    category="trakt",
                )
            else:
                try:
                    trakt = await _get_trakt_client()
                    scrobble = _build_scrobble_payload()
                    if scrobble:
                        await trakt.scrobble_pause(scrobble, progress=progress)
                        trakt_synced = True
                        pos_secs = _get_position_ticks() // 10000000
                        mm, ss = divmod(pos_secs, 60)
                        await _activity_log(
                            f"⏸ Trakt paused: {display_name} at {mm}:{ss:02d} ({progress:.0f}%)",
                            category="trakt",
                        )
                except Exception as e:
                    err_str = str(e)
                    if "422" in err_str:
                        # Trakt rejected — likely near end of content, not a real error
                        await _activity_log(
                            f"⏸ Pause skipped by Trakt: {display_name} ({progress:.0f}%) — will sync on stop",
                            category="trakt",
                        )
                    else:
                        log.warning("webhook.trakt_scrobble_pause_failed", error=err_str)
                        await _activity_log(f"⚠ Trakt pause failed: {display_name} — {err_str[:80]}", category="trakt")
        # MDBList scrobble pause
        asyncio.create_task(_mdblist_scrobble("pause", _calc_progress()))
        return {"status": "received", "event": event_type, "trakt_synced": trakt_synced}

    # ── playback.unpause → Trakt scrobble/start (resume) ────────────────────
    if is_play_unpause:
        if user.trakt_access_token:
            try:
                trakt = await _get_trakt_client()
                scrobble = _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    await trakt.scrobble_start(scrobble, progress=progress)
                    trakt_synced = True
                    pos_secs = _get_position_ticks() // 10000000
                    mm, ss = divmod(pos_secs, 60)
                    await _activity_log(
                        f"▶ Trakt resumed: {display_name} at {mm}:{ss:02d} ({progress:.0f}%)",
                        category="trakt",
                    )
            except Exception as e:
                log.warning("webhook.trakt_scrobble_resume_failed", error=str(e))
                await _activity_log(f"⚠ Trakt resume failed: {display_name} — {str(e)[:80]}", category="trakt")
        # MDBList scrobble resume
        asyncio.create_task(_mdblist_scrobble("resume", _calc_progress()))
        return {"status": "received", "event": event_type, "trakt_synced": trakt_synced}

    # ── playback.stop / item.markplayed → Trakt watch history ───────────────
    if is_watched:
        # Invalidate continue watching cache — item finished or resume point changed
        await _invalidate_continue_watching()

        # Extract playback duration from session data
        duration_ticks = session_data.get("PlayState", {}).get("PositionTicks", 0)

        # Record feedback for Smart Queue
        await smart_queue_svc.record_play(
            user_id=user.id,
            emby_item_id=emby_item_id,
            duration_ticks=duration_ticks,
        )

        # Remove watched item from queue and backfill with next best
        try:
            await smart_queue_svc.remove_and_backfill(
                user_id=user.id,
                emby_item_id=emby_item_id,
            )
        except Exception as e:
            log.warning("webhook.backfill_failed", error=str(e)[:120])

        await _activity_log(
            f"⏹ Stopped: {display_name} ({item_type_raw}) — {emby_username}",
            category="playback",
        )

        # ── Send scrobble/stop to clear Trakt "watching" state ──────────
        # Only for actual playback stops (not manual mark-as-played).
        # If progress > 80%, Trakt auto-adds to history (action=scrobble)
        # and we skip the manual add_to_history to avoid duplicates.
        scrobble_already_added = False
        if is_play_stop and user.trakt_access_token:
            try:
                trakt = await _get_trakt_client()
                scrobble = _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    result = await trakt.scrobble_stop(scrobble, progress=progress)
                    action = result.get("action", "") if isinstance(result, dict) else ""
                    if action == "scrobble":
                        # >80% progress — Trakt added to history automatically
                        scrobble_already_added = True
                        trakt_synced = True
                        await _activity_log(
                            f"✓ Trakt scrobbled: {display_name} ({progress:.0f}%)",
                            category="trakt",
                        )
                    else:
                        # <80% — Trakt saved as pause/playback progress
                        await _activity_log(
                            f"⏹ Trakt stop: {display_name} ({progress:.0f}%) — action={action}",
                            category="trakt",
                        )
            except Exception as e:
                err_str = str(e)
                if "409" in err_str:
                    # Already scrobbled recently — watching state is cleared
                    scrobble_already_added = True
                    trakt_synced = True
                    await _activity_log(
                        f"⏹ Trakt stop (already scrobbled): {display_name}",
                        category="trakt",
                    )
                elif "422" in err_str:
                    # Progress < 1% — Trakt ignores, but watching state is cleared
                    await _activity_log(
                        f"⏹ Trakt stop ignored (<1%): {display_name}",
                        category="trakt",
                    )
                else:
                    log.warning("webhook.trakt_scrobble_stop_failed", error=err_str)
                    await _activity_log(
                        f"⚠ Trakt stop failed: {display_name} — {err_str[:80]}",
                        category="trakt",
                    )

        # Scrobble to Trakt watch history if user has a token
        if user.trakt_access_token and not scrobble_already_added:
            try:
                trakt = await _get_trakt_client()

                # Build Trakt item from provider IDs in the webhook payload
                provider_ids = item_data.get("ProviderIds", {})
                trakt_ids = {}
                if provider_ids.get("Imdb"):
                    trakt_ids["imdb"] = provider_ids["Imdb"]
                if provider_ids.get("Tmdb"):
                    trakt_ids["tmdb"] = int(provider_ids["Tmdb"])
                if provider_ids.get("Tvdb"):
                    trakt_ids["tvdb"] = int(provider_ids["Tvdb"])

                if trakt_ids:
                    watched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

                    if item_type_raw in ("Movie",):
                        history_item = {
                            "ids": trakt_ids,
                            "watched_at": watched_at,
                        }
                        await trakt.add_to_history([history_item])
                        trakt_synced = True
                        log.info("webhook.trakt_history_synced",
                                 type="movie", ids=trakt_ids, user=user.id)
                        await _activity_log(
                            f"✓ Synced to Trakt: {display_name}",
                            category="trakt",
                        )

                    elif item_type_raw in ("Episode",):
                        series_ids = await _resolve_series_ids()

                        episode = {
                            "watched_at": watched_at,
                            "ids": trakt_ids,
                        }
                        season_num = item_data.get("ParentIndexNumber")
                        episode_num = item_data.get("IndexNumber")
                        if season_num is not None:
                            episode["season"] = season_num
                        if episode_num is not None:
                            episode["number"] = episode_num

                        show_item = {
                            "_type": "show",
                            "ids": series_ids or trakt_ids,
                            "seasons": [{
                                "number": season_num or 1,
                                "episodes": [episode],
                            }],
                        }
                        await trakt.add_to_history([show_item])
                        trakt_synced = True
                        log.info("webhook.trakt_history_synced",
                                 type="episode", ids=series_ids or trakt_ids,
                                 ep_ids=trakt_ids, user=user.id)
                        await _activity_log(
                            f"✓ Synced to Trakt: {display_name}",
                            category="trakt",
                        )
                    else:
                        await _activity_log(
                            f"Skipped Trakt sync: {display_name} — unsupported type '{item_type_raw}'",
                            category="trakt",
                        )
                else:
                    await _activity_log(
                        f"Skipped Trakt sync: {display_name} — no provider IDs (IMDB/TMDB/TVDB)",
                        category="trakt",
                    )

            except Exception as e:
                log.error("webhook.trakt_sync_failed", error=str(e), user=user.id)
                await _activity_log(
                    f"✗ Trakt sync failed: {display_name} — {str(e)[:80]}",
                    category="trakt",
                )

            # Invalidate scrobble audit cache so newly synced items
            # don't appear as missed on the next audit view
            if trakt_synced:
                await scrobble_audit_svc.invalidate_cache(user.id)
        else:
            if not scrobble_already_added:
                await _activity_log(
                    f"Skipped Trakt sync: {display_name} — user has no Trakt token",
                    category="trakt",
                )

        # ── MDBList: scrobble stop + history sync ─────────────────────────
        if is_play_stop:
            asyncio.create_task(_mdblist_scrobble("stop", _calc_progress()))
        if not scrobble_already_added or True:
            # Always try MDBList history (independent of Trakt scrobble state)
            asyncio.create_task(_mdblist_add_to_history())

        # ── Persistent watch history (local DB) ──────────────────────────
        # Record every completed watch for rewatch suggestions & stats.
        # For PlaybackStop: only if progress >= 80% (actually watched).
        # For MarkPlayed: always (user explicitly marked it).
        should_record = is_mark_played
        if is_play_stop:
            try:
                should_record = _calc_progress() >= 80
            except Exception:
                should_record = True  # err on the side of recording

        if should_record:
            try:
                from app.models.schema import WatchHistory
                provider_ids = item_data.get("ProviderIds", {})
                runtime_ticks = item_data.get("RunTimeTicks", 0) or 0
                runtime_min = int(runtime_ticks / 600_000_000) if runtime_ticks else None

                wh_item_type = "episode" if item_type_raw == "Episode" else "movie"
                wh_series = item_data.get("SeriesName") if item_type_raw == "Episode" else None
                wh_season = item_data.get("ParentIndexNumber") if item_type_raw == "Episode" else None
                wh_episode = item_data.get("IndexNumber") if item_type_raw == "Episode" else None

                # For episodes, get series-level provider IDs
                wh_imdb = provider_ids.get("Imdb", "") or ""
                wh_tmdb = str(provider_ids.get("Tmdb", "")) if provider_ids.get("Tmdb") else ""
                wh_tvdb = str(provider_ids.get("Tvdb", "")) if provider_ids.get("Tvdb") else ""
                wh_trakt = ""

                if item_type_raw == "Episode":
                    series_ids = await _resolve_series_ids()
                    wh_imdb = wh_imdb or str(series_ids.get("imdb", ""))
                    wh_tmdb = wh_tmdb or str(series_ids.get("tmdb", ""))
                    wh_tvdb = wh_tvdb or str(series_ids.get("tvdb", ""))

                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                entry = WatchHistory(
                    user_id=user.id,
                    emby_id=emby_item_id,
                    item_type=wh_item_type,
                    title=item_name,
                    series_name=wh_series,
                    season_number=wh_season,
                    episode_number=wh_episode,
                    imdb_id=wh_imdb or None,
                    tmdb_id=wh_tmdb or None,
                    trakt_id=wh_trakt or None,
                    tvdb_id=wh_tvdb or None,
                    watched_at=now_naive,
                    runtime_minutes=runtime_min,
                    source="webhook",
                )
                db.add(entry)
                await db.commit()
                log.debug("webhook.watch_history_recorded", user_id=user.id,
                          title=display_name, item_type=wh_item_type)
            except Exception as e:
                await db.rollback()
                # IntegrityError from unique constraint = duplicate, not an error
                if "uq_watch_history_user_item_time" in str(e):
                    log.debug("webhook.watch_history_duplicate", title=display_name)
                else:
                    log.warning("webhook.watch_history_failed", error=str(e)[:200])

    # ── library.new / item.added → check smart queue for missing items ─────
    if is_library_event and item_type_raw in ("Movie", "Episode", "Series"):
        try:
            # Extract provider IDs from the new item
            provider_ids = item_data.get("ProviderIds", {})
            tmdb_id = provider_ids.get("Tmdb")
            imdb_id = provider_ids.get("Imdb")
            tvdb_id = provider_ids.get("Tvdb")

            # Skip cache updates for unpack/extraction events (no real item yet)
            _is_unpack = "unpack" in item_name.lower() or "unpack" in (item_data.get("Path") or "").lower()

            # Immediately update library cache for Movies and Series
            # so all features (Library Health, Universe Discovery, etc.)
            # see the new item without waiting for the nightly rebuild.
            # Only when we have provider IDs (real item, not unpack stub).
            has_provider_ids = any(provider_ids.get(k) for k in ("Tmdb", "Imdb", "Tvdb"))
            if item_type_raw in ("Movie", "Series") and emby_item_id and has_provider_ids and not _is_unpack:
                try:
                    cache_type = "movie" if item_type_raw == "Movie" else "series"
                    # Check if already cached (avoid double-counting on duplicate webhooks)
                    already_cached = False
                    for _pid_type in ("Tmdb", "Imdb", "Tvdb"):
                        _pid_val = provider_ids.get(_pid_type)
                        if _pid_val:
                            existing = await LibraryCache.find_by_provider_id(_pid_type, str(_pid_val))
                            if existing:
                                already_cached = True
                                break
                    await LibraryCache._cache_item(item_data, item_type=cache_type)
                    if not already_cached:
                        _r = await get_redis()
                        stat_key = f"library::stat:{'movies' if item_type_raw == 'Movie' else 'series'}"
                        await _r.incr(stat_key)
                        # Bump version so dashboard knows to refresh library counts
                        await _r.incr("library::stat:version")
                    log.info("webhook.library_cache_updated",
                             title=item_name, type=cache_type,
                             emby_id=emby_item_id, new=not already_cached)
                except Exception as _ce:
                    log.debug("webhook.library_cache_update_failed",
                              error=str(_ce)[:120])

            # For episodes, also extract the series-level IDs from SeriesId
            # so we can match the queue item (which tracks the series, not
            # individual episodes)
            series_provider_ids = {}
            series_emby_id = item_data.get("SeriesId")
            if item_type_raw == "Episode" and series_emby_id:
                try:
                    emby = EmbyClient()
                    series_item = await emby.get_items_by_ids([series_emby_id])
                    if series_item:
                        series_provider_ids = series_item[0].get("ProviderIds", {})
                except Exception:
                    log.debug("webhook.series_lookup_failed", series_id=series_emby_id)
                finally:
                    try:
                        await emby.close()
                    except Exception:
                        pass

            # Determine which IDs and queue item_type to match
            if item_type_raw == "Movie":
                match_type = "movie"
                match_ids = {"tmdb": tmdb_id, "imdb": imdb_id}
                resolved_emby_id = emby_item_id
            else:
                # Episode or Series → match against show queue items
                match_type = "show"
                if item_type_raw == "Episode" and series_provider_ids:
                    match_ids = {
                        "tmdb": series_provider_ids.get("Tmdb"),
                        "imdb": series_provider_ids.get("Imdb"),
                        "tvdb": series_provider_ids.get("Tvdb"),
                    }
                    resolved_emby_id = series_emby_id or emby_item_id
                elif item_type_raw == "Series":
                    match_ids = {"tmdb": tmdb_id, "imdb": imdb_id, "tvdb": tvdb_id}
                    resolved_emby_id = emby_item_id
                else:
                    # Episode without series lookup fallback
                    match_ids = {"tmdb": tmdb_id, "imdb": imdb_id, "tvdb": tvdb_id}
                    resolved_emby_id = emby_item_id

            has_ids = any(v for v in match_ids.values())

            if has_ids:
                # Find any missing queue items that match
                missing_items = (await db.execute(
                    select(QueueItem).where(
                        QueueItem.in_library == False,
                        QueueItem.item_type == match_type,
                    )
                )).scalars().all()

                promoted = 0
                for qi in missing_items:
                    meta = qi.metadata_json or {}
                    ids = meta.get("ids", {})
                    match = False
                    for id_key, id_val in match_ids.items():
                        if id_val and str(ids.get(id_key, "")) == str(id_val):
                            match = True
                            break

                    if match:
                        qi.emby_item_id = resolved_emby_id
                        qi.in_library = True
                        promoted += 1
                        log.info("webhook.queue_item_promoted",
                                 title=qi.title, emby_id=resolved_emby_id,
                                 item_type=match_type)

                if promoted:
                    await db.commit()
                    # Invalidate availability cache — the item just arrived
                    try:
                        _r = await get_redis()
                        await _r.delete("availability_monitor_v2")
                    except Exception:
                        pass
                    # Re-sync playlist for each affected user
                    affected_users = {qi.user_id for qi in missing_items if qi.in_library}
                    for uid in affected_users:
                        try:
                            await smart_queue_svc._resync_playlist_from_db(uid)
                        except Exception:
                            log.warning("webhook.playlist_resync_failed", user_id=uid)

                    await _activity_log(
                        f"📥 Library added: {item_name} — promoted {promoted} queue item(s) to in-library",
                        category="queue",
                    )
                else:
                    await _activity_log(
                        f"📥 Library added: {item_name} ({item_type_raw}) — not in smart queue",
                        category="library",
                    )
            else:
                await _activity_log(
                    f"📥 Library added: {item_name} ({item_type_raw}) — no provider IDs to match",
                    category="library",
                )

            # ── Update Recently Arrived from webhook ──────────────────────
            # Check if this item was in the pending snapshot and surface it
            # as arrived immediately, rather than waiting for the next poll.
            try:
                import json as _json
                _r = await get_redis()
                raw_prev = await _r.get("recently_arrived_pending_v1")
                if raw_prev:
                    prev = _json.loads(raw_prev)
                    arrived_item = None

                    if item_type_raw == "Movie" and tmdb_id:
                        prev_movie_ids = {str(m) for m in prev.get("movies", [])}
                        if str(tmdb_id) in prev_movie_ids:
                            arrived_item = {
                                "title": item_name,
                                "year": item_data.get("ProductionYear"),
                                "tmdb_id": tmdb_id,
                                "type": "movie",
                                "id": tmdb_id,
                                "arrived_at": datetime.now(timezone.utc).isoformat() + "Z",
                            }
                    elif item_type_raw in ("Series", "Episode") and tvdb_id:
                        series_tvdb = tvdb_id
                        if item_type_raw == "Episode" and series_provider_ids:
                            series_tvdb = series_provider_ids.get("Tvdb") or tvdb_id
                        prev_show_eps = {}
                        for s in prev.get("shows", []):
                            if isinstance(s, dict):
                                prev_show_eps[str(s.get("id", ""))] = s.get("eps", 0)
                        if str(series_tvdb) in prev_show_eps:
                            series_name = item_data.get("SeriesName") or item_name
                            arrived_item = {
                                "title": series_name,
                                "year": item_data.get("ProductionYear"),
                                "tvdb_id": series_tvdb,
                                "type": "show",
                                "id": series_tvdb,
                                "new_episodes": 1,
                                "arrived_at": datetime.now(timezone.utc).isoformat() + "Z",
                            }

                    if arrived_item:
                        # Append to arrived items list (dedup by type+id)
                        arrived_key = "recently_arrived_items_v1"
                        raw_arr = await _r.get(arrived_key)
                        existing = _json.loads(raw_arr) if raw_arr else []
                        existing_ids = {(i.get("type"), str(i.get("id", ""))) for i in existing}
                        item_key = (arrived_item["type"], str(arrived_item["id"]))

                        if item_key not in existing_ids:
                            existing.append(arrived_item)
                            await _r.setex(arrived_key, 86400 * 2, _json.dumps(existing))
                            log.info("webhook.recently_arrived_added",
                                     title=arrived_item["title"],
                                     type=arrived_item["type"])

                        # Clear the result cache so the dashboard picks it up
                        await _r.delete("recently_arrived_result_v1")

            except Exception as e:
                log.debug("webhook.recently_arrived_update_failed",
                          error=str(e)[:120])

        except Exception as e:
            log.warning("webhook.item_added_handler_failed", error=str(e)[:120])

        return {"status": "received", "event": event_type}

    # ── library.deleted / item.removed → remove from Trakt watchlist ─────
    if is_library_removed and item_type_raw in ("Movie", "Series"):
        try:
            provider_ids = item_data.get("ProviderIds", {})
            tmdb_id = provider_ids.get("Tmdb")
            imdb_id = provider_ids.get("Imdb")
            tvdb_id = provider_ids.get("Tvdb")

            if tmdb_id or imdb_id or tvdb_id:
                # Remove from Trakt watchlist for all linked users
                async with async_session() as _db:
                    linked_users = (await _db.execute(
                        select(User).where(User.trakt_access_token.isnot(None))
                    )).scalars().all()

                removed_for: list[str] = []
                for lu in linked_users:
                    trakt = None
                    try:
                        async def _on_refresh_rm(access, refresh, expires, _uid=lu.id):
                            async with async_session() as __db:
                                u = await __db.get(User, _uid)
                                u.trakt_access_token = access
                                u.trakt_refresh_token = refresh
                                u.trakt_token_expires = expires
                                await __db.commit()

                        trakt = TraktClient(
                            access_token=lu.trakt_access_token,
                            refresh_token=lu.trakt_refresh_token,
                            token_expires=lu.trakt_token_expires,
                            token_refresh_callback=_on_refresh_rm,
                        )

                        if item_type_raw == "Movie":
                            ids = {}
                            if tmdb_id:
                                ids["tmdb"] = int(tmdb_id)
                            if imdb_id:
                                ids["imdb"] = imdb_id
                            result = await trakt.remove_from_watchlist(
                                movies=[{"ids": ids}]
                            )
                            deleted = (result.get("deleted") or {}).get("movies", 0)
                        else:
                            ids = {}
                            if tvdb_id:
                                ids["tvdb"] = int(tvdb_id)
                            if imdb_id:
                                ids["imdb"] = imdb_id
                            result = await trakt.remove_from_watchlist(
                                shows=[{"ids": ids}]
                            )
                            deleted = (result.get("deleted") or {}).get("shows", 0)

                        if deleted:
                            removed_for.append(lu.emby_username or str(lu.id))
                            log.info("webhook.trakt_watchlist_removed",
                                     title=item_name, user=lu.id, deleted=deleted)
                    except Exception as e:
                        log.debug("webhook.trakt_watchlist_remove_failed",
                                  user=lu.id, error=str(e)[:120])
                    finally:
                        if trakt:
                            await trakt.close()

                if removed_for:
                    await _activity_log(
                        f"🗑️ Library removed: {item_name} — removed from Trakt watchlist for {', '.join(removed_for)}",
                        category="trakt",
                    )
                else:
                    await _activity_log(
                        f"🗑️ Library removed: {item_name} — not on any user's Trakt watchlist",
                        category="library",
                    )
            else:
                await _activity_log(
                    f"🗑️ Library removed: {item_name} ({item_type_raw}) — no provider IDs",
                    category="library",
                )
        except Exception as e:
            log.warning("webhook.item_removed_handler_failed", error=str(e)[:120])

        return {"status": "received", "event": event_type}

    if not is_watched and not is_library_removed:
        # Unmatched event — log for debugging
        await _activity_log(
            f"📡 Unhandled webhook: {event_type} — {item_name}",
            category="webhook",
        )

    return {"status": "received", "event": event_type, "trakt_synced": trakt_synced}


# -- Activity log (Redis-backed, last 100 entries) ---------------------------

async def _activity_log(message: str, category: str = "general"):
    """Append an entry to the activity log in Redis and push to dashboard."""
    import json as _json
    try:
        r = await get_redis()
        entry_dict = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "cat": category,
            "msg": message,
        }
        entry = _json.dumps(entry_dict)
        await r.lpush("activity_log", entry)
        await r.ltrim("activity_log", 0, 99)  # keep last 100
        # Push to any connected dashboard clients
        try:
            from app.services.watch_party.service import sio
            await sio.emit("activity_entry", entry_dict)
        except Exception:
            pass
    except Exception:
        pass  # logging should never crash the request



# ═══════════════════════════════════════════════════════════════════════════
# Scheduler status
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/scheduler/status")
async def scheduler_status():
    """Return last-run status for each scheduled job."""
    import json as _json
    from app.main import _job_crons
    r = await get_redis()
    jobs = {}
    for job_id, cron in _job_crons.items():
        raw = await r.get(f"scheduler:status:{job_id}")
        if raw:
            data = _json.loads(raw)
        else:
            data = {"last_run": None, "status": "pending", "duration_s": None, "error": None}
        data["cron"] = cron
        jobs[job_id] = data
    return jobs


# ═══════════════════════════════════════════════════════════════════════════
# Job completion events (toast notifications)
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/dashboard-poll")
async def dashboard_poll(
    category: str = Query(default=None),
):
    """Consolidated polling endpoint for dashboard.

    Returns health, activity, and job-completion events in a single response,
    replacing three separate polled endpoints.
    """
    import json as _json
    r = await get_redis()

    # --- Health ---
    cache_stats = await LibraryCache.get_stats()

    # --- Activity ---
    fetch_count = 99 if category else 29
    raw_activity = await r.lrange("activity_log", 0, fetch_count)
    entries = []
    limit = 30
    for item in raw_activity:
        try:
            entry = _json.loads(item)
            if category and entry.get("cat") != category:
                continue
            entries.append(entry)
            if len(entries) >= limit:
                break
        except Exception:
            pass

    # --- Job completions (consuming) ---
    job_events = []
    while True:
        raw = await r.rpop("job_completions")
        if raw is None:
            break
        try:
            job_events.append(_json.loads(raw))
        except Exception:
            pass

    return {
        "health": {
            "status": "ok",
            "features": {
                "smart_queue": settings.enable_smart_queue,
                "ml_predictor": settings.enable_ml_predictor,
                "universe_discovery": settings.enable_universe_discovery,
                "watch_party": settings.enable_watch_party,
            },
            "library_cache": cache_stats,
        },
        "activity": entries,
        "job_completions": job_events,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SSL Certificate Status
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/ssl/status")
async def ssl_status():
    """Return SSL certificate expiry info.

    Reads the latest result written by the scheduled ssl_cert_check job.
    If SSL_DOMAIN is not set, returns disabled status.
    """
    import json as _json
    domain = settings.ssl_domain
    if not domain:
        return {"enabled": False, "message": "SSL_DOMAIN not set in .env"}

    r = await get_redis()
    raw = await r.get("ssl:cert_status")
    if raw:
        data = _json.loads(raw)
        data["enabled"] = True
        return data

    # No cached result yet — do a live check
    result = await _check_ssl_cert(domain)
    result["enabled"] = True
    return result


async def _check_ssl_cert(domain: str) -> dict:
    """Connect to domain over TLS and read certificate expiry."""
    import ssl
    import socket
    import asyncio

    def _fetch_cert(d: str) -> dict:
        ctx = ssl.create_default_context()
        with socket.create_connection((d, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=d) as ssock:
                return ssock.getpeercert()

    try:
        cert = await asyncio.to_thread(_fetch_cert, domain)

        not_after_str = cert.get("notAfter", "")
        not_before_str = cert.get("notBefore", "")
        # e.g. "Sep 28 12:00:00 2026 GMT"
        from datetime import datetime as _dt
        not_after = _dt.strptime(not_after_str, "%b %d %H:%M:%S %Y %Z")
        not_before = _dt.strptime(not_before_str, "%b %d %H:%M:%S %Y %Z")
        days_left = (not_after - _dt.now(timezone.utc).replace(tzinfo=None)).days

        issuer_parts = dict(x[0] for x in cert.get("issuer", ()))
        issuer = issuer_parts.get("organizationName", issuer_parts.get("commonName", "Unknown"))

        subject_parts = dict(x[0] for x in cert.get("subject", ()))
        cn = subject_parts.get("commonName", domain)

        san = [entry[1] for entry in cert.get("subjectAltName", ())]

        status = "ok" if days_left > 30 else "expiring_soon" if days_left > 7 else "critical" if days_left > 0 else "expired"

        return {
            "domain": domain,
            "common_name": cn,
            "issuer": issuer,
            "not_before": not_before.strftime("%Y-%m-%d %H:%M:%S"),
            "not_after": not_after.strftime("%Y-%m-%d %H:%M:%S"),
            "days_left": days_left,
            "status": status,
            "san": san,
            "checked_at": _dt.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "error": None,
        }

    except Exception as e:
        return {
            "domain": domain,
            "status": "error",
            "days_left": None,
            "error": str(e)[:200],
            "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Library Cache management
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/cache/rebuild")
async def rebuild_cache(_user: User = Depends(get_current_user)):
    """Manually trigger library cache rebuild."""
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        summary = await LibraryCache.index_library(emby, user_id=uid)
    return {"status": "rebuilt", **summary}


@router.get("/cache/stats")
async def cache_stats():
    return await LibraryCache.get_stats()


@router.post("/cache/clear")
async def clear_cache(_user: User = Depends(get_current_user)):
    return await LibraryCache.clear()


@router.get("/api/libraries")
async def list_libraries():
    """Return Emby library folders (virtual folders)."""
    async with EmbyClient() as emby:
        return await emby.get_virtual_folders()


@router.get("/api/libraries/stats")
async def library_stats():
    """Return media libraries (no collections/playlists) with item counts."""
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        folders = await emby.get_virtual_folders()
        # Only media libraries — filter out boxsets, playlists, music, etc.
        media_types = {"movies", "tvshows"}
        results = []
        for f in folders:
            ct = f.get("collection_type", "")
            if ct not in media_types:
                continue
            # Count items — Movie for movies, Series for tvshows (not seasons/episodes)
            item_type = "Movie" if ct == "movies" else "Series"
            try:
                resp = await emby.get_items(
                    user_id=uid,
                    parent_id=f.get("item_id"),
                    item_type=item_type,
                    fields="",
                    limit=0,
                )
                count = resp.get("TotalRecordCount", 0)
            except Exception:
                count = 0
            results.append({
                "name": f.get("name", ""),
                "collection_type": ct,
                "item_count": count,
            })
    return results


@router.get("/api/library/search")
async def library_search(q: str = Query(..., min_length=2, max_length=100)):
    """Search Emby library by title (used by the watch party item picker).

    Returns resolution/quality info so users can distinguish 1080p from 4K
    when duplicates exist.
    """
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        resp = await emby.get_items(
            user_id=uid,
            search_term=q,
            item_type=None,
            fields="ProviderIds,Genres,Overview,People,Studios,RunTimeTicks,MediaSources",
            limit=20,
        )
    results = []
    for it in resp.get("Items", []):
        if it.get("Type") not in ("Movie", "Series", "Episode"):
            continue

        # Extract resolution/quality from MediaSources
        quality = ""
        media_sources = it.get("MediaSources") or []
        if media_sources:
            ms = media_sources[0]
            # Video stream resolution
            for stream in ms.get("MediaStreams", []):
                if stream.get("Type") == "Video":
                    w = stream.get("Width", 0)
                    h = stream.get("Height", 0)
                    if w >= 3840 or h >= 2160:
                        quality = "4K"
                    elif w >= 1920 or h >= 1080:
                        quality = "1080p"
                    elif w >= 1280 or h >= 720:
                        quality = "720p"
                    elif w > 0:
                        quality = f"{h}p"
                    # Add HDR if present
                    if stream.get("VideoRangeType") in ("HDR10", "HDR10Plus", "DolbyVision", "HLG"):
                        quality += " HDR"
                    elif stream.get("VideoRange") == "HDR":
                        quality += " HDR"
                    break
            # Add container/codec info
            container = ms.get("Container", "")
            if container:
                quality += f" ({container})" if quality else container

        results.append({
            "id": it.get("Id"),
            "title": it.get("Name"),
            "year": it.get("ProductionYear"),
            "type": it.get("Type"),
            "quality": quality,
        })

    return results[:15]


# ═══════════════════════════════════════════════════════════════════════════
# HTML Pages
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/setup", response_class=HTMLResponse)
async def get_setup_page():
    """Serve the first-run integration provider setup page."""
    try:
        with open("frontend/templates/setup.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Setup page not found</h1>"


@router.get("/lists", response_class=HTMLResponse)
@router.get("/universes", response_class=HTMLResponse)
async def get_universes_page():
    """Serve the lists page (renamed from universes)."""
    try:
        with open("frontend/templates/universes.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/predictions", response_class=HTMLResponse)
async def get_predictions_page():
    """Serve the ML predictions chart page."""
    try:
        with open("frontend/templates/predictions.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/settings", response_class=HTMLResponse)
async def get_settings_page():
    """Serve the settings configuration page."""
    try:
        with open("frontend/templates/settings.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/watch-party", response_class=HTMLResponse)
async def get_watch_party_page(code: str = None):
    """Serve the watch party chat page."""
    try:
        with open("frontend/templates/watch_party.html", "r") as f:
            html = f.read()
        # Inject party code if provided
        if code:
            html = html.replace("const partyCode = null;", f"const partyCode = '{code}';")
        return html
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/bias", response_class=HTMLResponse)
async def get_bias_page():
    """Serve the Rating Bias Detector analysis page."""
    try:
        with open("frontend/templates/bias_detector.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


# ═══════════════════════════════════════════════════════════════════════════
# Rating Bias Detector API
# ═══════════════════════════════════════════════════════════════════════════

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
# Database Backup / Restore
# ═══════════════════════════════════════════════════════════════════════════

def _parse_db_url(url: str) -> tuple[str, str, str, str]:
    """Parse DATABASE_URL into (user, password, host, dbname).

    Handles: postgresql+asyncpg://user:pass@host:port/dbname
    """
    from urllib.parse import urlparse, unquote
    parsed = urlparse(url)
    user = unquote(parsed.username or "embytrakt")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "postgres"
    dbname = (parsed.path or "/embytrakt").lstrip("/")
    return user, password, host, dbname

@router.post("/api/db/backup")
async def create_db_backup(_user: User = Depends(get_current_user)):
    """Create a pg_dump backup and return a download token."""
    import subprocess
    import uuid

    backup_dir = "/app/cache/backups"
    os.makedirs(backup_dir, exist_ok=True)
    backup_id = uuid.uuid4().hex[:12]
    filename = f"emby-trakt-backup-{backup_id}.sql"
    filepath = os.path.join(backup_dir, filename)

    # Parse connection details from DATABASE_URL
    # Format: postgresql+asyncpg://user:pass@host:port/dbname
    db_url = os.environ.get("DATABASE_URL", "")
    db_user, db_pass, db_host, db_name = _parse_db_url(db_url)

    env = {**os.environ, "PGPASSWORD": db_pass}
    try:
        result = subprocess.run(
            ["pg_dump", "-h", db_host, "-U", db_user, "-d", db_name, "-f", filepath],
            capture_output=True, text=True, env=env, timeout=120,
        )
    except FileNotFoundError:
        return {"status": "error", "reason": "pg_dump not found — rebuild the container image to install postgresql-client"}

    if result.returncode != 0:
        return {"status": "error", "reason": result.stderr[:300]}

    size_bytes = os.path.getsize(filepath)
    return {
        "status": "ok",
        "backup_id": backup_id,
        "filename": filename,
        "size_bytes": size_bytes,
    }


@router.get("/api/db/backup/{backup_id}")
async def download_db_backup(backup_id: str, _user: User = Depends(get_current_user)):
    """Download a previously created backup file."""
    import re
    from fastapi.responses import FileResponse

    # SECURITY: backup_id must be hex-only (generated by uuid4().hex[:12])
    if not re.fullmatch(r"[a-f0-9]{1,24}", backup_id):
        raise HTTPException(400, "Invalid backup ID")

    filepath = f"/app/cache/backups/emby-trakt-backup-{backup_id}.sql"
    if not os.path.isfile(filepath):
        raise HTTPException(404, "Backup not found — create one first")
    return FileResponse(
        filepath,
        media_type="application/sql",
        filename=os.path.basename(filepath),
    )


@router.post("/api/db/restore")
async def restore_db_backup(request: Request, _user: User = Depends(get_current_user)):
    """Restore a database from an uploaded .sql backup.

    Accepts multipart form upload with fields:
      - 'file': the .sql backup file (max 50 MB)
      - 'confirm': must be the string "RESTORE" to proceed

    WARNING: This overwrites all current data.
    """
    import subprocess
    import re as _re

    MAX_RESTORE_SIZE = 50 * 1024 * 1024  # 50 MB

    form = await request.form()

    # Require explicit confirmation
    confirm = form.get("confirm", "")
    if confirm != "RESTORE":
        raise HTTPException(400, "Confirmation required: include form field confirm=RESTORE")

    upload = form.get("file")
    if not upload:
        raise HTTPException(400, "No file uploaded")

    # Validate filename extension
    filename = getattr(upload, "filename", "") or ""
    if not filename.lower().endswith(".sql"):
        raise HTTPException(400, "Only .sql files are accepted")

    # Read with size limit — stream in chunks to avoid OOM
    chunks = []
    total_size = 0
    while True:
        chunk = await upload.read(1024 * 64)  # 64 KB chunks
        if not chunk:
            break
        total_size += len(chunk)
        if total_size > MAX_RESTORE_SIZE:
            raise HTTPException(413, f"File too large — maximum {MAX_RESTORE_SIZE // (1024*1024)} MB")
        chunks.append(chunk)

    contents = b"".join(chunks)

    if total_size == 0:
        raise HTTPException(400, "Uploaded file is empty")

    # Basic content validation — must look like a pg_dump SQL file
    # Check first 4 KB for SQL-like content
    head = contents[:4096].decode("utf-8", errors="replace")

    # Reject if it looks like binary (not text)
    non_printable = sum(1 for c in head if ord(c) < 32 and c not in '\n\r\t')
    if non_printable > len(head) * 0.1:
        raise HTTPException(400, "File does not appear to be a text SQL file")

    # Must contain at least one pg_dump indicator
    pg_dump_markers = ("pg_dump", "SET statement_timeout", "CREATE TABLE", "COPY ", "INSERT INTO", "ALTER TABLE")
    has_marker = any(marker in head for marker in pg_dump_markers)
    if not has_marker:
        raise HTTPException(400, "File does not appear to be a valid pg_dump backup — no recognisable SQL statements found")

    # Reject dangerous statements that shouldn't be in a data restore
    dangerous_patterns = [
        r'\bCREATE\s+ROLE\b', r'\bCREATE\s+USER\b', r'\bALTER\s+ROLE\b',
        r'\bDROP\s+DATABASE\b', r'\bCREATE\s+DATABASE\b',
        r'\bCOPY\b.*\bFROM\s+PROGRAM\b', r'\bCREATE\s+EXTENSION\b.*\buntrusted\b',
    ]
    full_text = contents.decode("utf-8", errors="replace")
    for pat in dangerous_patterns:
        if _re.search(pat, full_text, _re.IGNORECASE):
            raise HTTPException(
                400,
                f"File contains disallowed SQL statement matching: {pat} — "
                "only data restore files from this application's pg_dump are accepted",
            )

    restore_path = "/app/cache/backups/restore_upload.sql"
    os.makedirs("/app/cache/backups", exist_ok=True)
    with open(restore_path, "wb") as f:
        f.write(contents)

    db_url = os.environ.get("DATABASE_URL", "")
    db_user, db_pass, db_host, db_name = _parse_db_url(db_url)

    env = {**os.environ, "PGPASSWORD": db_pass}
    try:
        result = subprocess.run(
            ["psql", "-h", db_host, "-U", db_user, "-d", db_name, "-f", restore_path],
            capture_output=True, text=True, env=env, timeout=120,
        )
    except FileNotFoundError:
        os.remove(restore_path)
        return {"status": "error", "reason": "psql not found — rebuild the container image to install postgresql-client"}

    os.remove(restore_path)

    if result.returncode != 0:
        return {"status": "error", "reason": result.stderr[:300]}

    log.warning("security.db_restored", user_id=_user.id, file_size=total_size)
    return {"status": "ok", "message": "Database restored. Restart the container for changes to take full effect."}


# ═══════════════════════════════════════════════════════════════════════════
# Radarr Integration
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/radarr/servers")
async def get_radarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Radarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await r.get("radarr_servers")
    if not raw:
        raw = (await _get_setting(db, "radarr_servers", ""))
    if not raw:
        return {"servers": []}
    try:
        servers = _json.loads(raw)
        for s in servers:
            s["api_key"] = _mask_api_key(s.get("api_key", ""))
        return {"servers": servers}
    except Exception:
        return {"servers": []}


@router.put("/api/radarr/servers")
async def save_radarr_servers(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save Radarr server configs to DB + Redis.

    Payload: {"servers": [{"name": "...", "url": "...", "api_key": "..."}, ...]}
    Max 2 servers. If api_key is masked (unchanged from GET), the stored key is preserved.
    """
    import json as _json
    servers = payload.get("servers", [])[:2]
    servers = await _resolve_servers(servers, "radarr_servers")
    clean = []
    for s in servers:
        if s.get("url") and s.get("api_key"):
            srv = {
                "name": s.get("name", "Radarr"),
                "url": s["url"].rstrip("/"),
                "api_key": s["api_key"],
            }
            if s.get("quality_profile_id"):
                srv["quality_profile_id"] = int(s["quality_profile_id"])
                srv["quality_profile_name"] = s.get("quality_profile_name", "")
            clean.append(srv)
    encoded = _json.dumps(clean)
    r = await get_redis()
    await r.set("radarr_servers", encoded)
    await _put_setting(db, "radarr_servers", encoded)
    await db.commit()
    # Invalidate download-queue cache so the next poll picks up changes
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass
    return {"status": "ok", "servers": len(clean)}


@router.post("/api/radarr/test")
async def test_radarr_connection(payload: dict, _user: User = Depends(get_current_user)):
    """Test a Radarr server connection. Returns quality profiles on success."""
    from app.utils.radarr_client import RadarrClient
    url = payload.get("url", "")
    api_key = payload.get("api_key", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # If key is masked, resolve from stored config
    if not api_key or _is_masked(api_key):
        resolved = await _resolve_servers([{"url": url, "api_key": api_key or "x****"}], "radarr_servers")
        api_key = resolved[0]["api_key"] if resolved else ""
    if not api_key:
        return {"status": "error", "message": "API key required"}
    client = RadarrClient(url, api_key)
    result = await client.test_connection()
    if result.get("status") == "ok":
        try:
            profiles = await client.get_quality_profiles()
            result["quality_profiles"] = [
                {"id": p.get("id"), "name": p.get("name")}
                for p in profiles
            ]
        except Exception:
            result["quality_profiles"] = []
    await client.close()
    return result


@router.post("/api/radarr/add")
async def add_to_radarr(payload: dict, _user: User = Depends(get_current_user)):
    """Add movies to a Radarr server.

    Payload: {
      "server_index": 0,
      "movies": [{"tmdb_id": 123, "imdb_id": "tt...", "title": "...", "year": 2024}, ...]
    }
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient

    server_idx = payload.get("server_index", 0)
    movies = payload.get("movies", [])
    if not movies:
        raise HTTPException(400, "No movies provided")

    r = await get_redis()
    raw = await r.get("radarr_servers")
    if not raw:
        raise HTTPException(400, "No Radarr servers configured — add one in Settings")
    servers = _json.loads(raw)
    if server_idx >= len(servers):
        raise HTTPException(400, f"Server index {server_idx} out of range")

    srv = servers[server_idx]
    client = RadarrClient(srv["url"], srv["api_key"], name=srv["name"])
    profile_id = srv.get("quality_profile_id")

    results = []
    for movie in movies:
        result = await client.add_movie(
            tmdb_id=movie.get("tmdb_id"),
            imdb_id=movie.get("imdb_id"),
            title=movie.get("title", ""),
            year=movie.get("year"),
            quality_profile_id=profile_id,
        )
        results.append(result)

    await client.close()

    added = sum(1 for r in results if r.get("status") == "ok")
    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(movies),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Sonarr Integration
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/sonarr/servers")
async def get_sonarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Sonarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await r.get("sonarr_servers")
    if not raw:
        raw = (await _get_setting(db, "sonarr_servers", ""))
    if not raw:
        return {"servers": []}
    try:
        servers = _json.loads(raw)
        for s in servers:
            s["api_key"] = _mask_api_key(s.get("api_key", ""))
        return {"servers": servers}
    except Exception:
        return {"servers": []}


@router.put("/api/sonarr/servers")
async def save_sonarr_servers(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save Sonarr server configs to DB + Redis.

    Payload: {"servers": [{"name": "...", "url": "...", "api_key": "..."}, ...]}
    Max 2 servers. If api_key is masked (unchanged from GET), the stored key is preserved.
    """
    import json as _json
    servers = payload.get("servers", [])[:2]
    servers = await _resolve_servers(servers, "sonarr_servers")
    clean = []
    for s in servers:
        if s.get("url") and s.get("api_key"):
            srv = {
                "name": s.get("name", "Sonarr"),
                "url": s["url"].rstrip("/"),
                "api_key": s["api_key"],
            }
            if s.get("quality_profile_id"):
                srv["quality_profile_id"] = int(s["quality_profile_id"])
                srv["quality_profile_name"] = s.get("quality_profile_name", "")
            clean.append(srv)
    encoded = _json.dumps(clean)
    r = await get_redis()
    await r.set("sonarr_servers", encoded)
    await _put_setting(db, "sonarr_servers", encoded)
    await db.commit()
    # Invalidate download-queue cache so the next poll picks up changes
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass
    return {"status": "ok", "servers": len(clean)}


@router.post("/api/sonarr/test")
async def test_sonarr_connection(payload: dict, _user: User = Depends(get_current_user)):
    """Test a Sonarr server connection. Returns quality profiles on success."""
    from app.utils.sonarr_client import SonarrClient
    url = payload.get("url", "")
    api_key = payload.get("api_key", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # If key is masked, resolve from stored config
    if not api_key or _is_masked(api_key):
        resolved = await _resolve_servers([{"url": url, "api_key": api_key or "x****"}], "sonarr_servers")
        api_key = resolved[0]["api_key"] if resolved else ""
    if not api_key:
        return {"status": "error", "message": "API key required"}
    client = SonarrClient(url, api_key)
    result = await client.test_connection()
    if result.get("status") == "ok":
        try:
            profiles = await client.get_quality_profiles()
            result["quality_profiles"] = [
                {"id": p.get("id"), "name": p.get("name")}
                for p in profiles
            ]
        except Exception:
            result["quality_profiles"] = []
    await client.close()
    return result


@router.post("/api/sonarr/add")
async def add_to_sonarr(payload: dict, _user: User = Depends(get_current_user)):
    """Add TV series to a Sonarr server.

    Payload: {
      "server_index": 0,
      "shows": [{"tvdb_id": 123, "imdb_id": "tt...", "title": "...", "year": 2024}, ...]
    }
    """
    import json as _json
    from app.utils.sonarr_client import SonarrClient

    server_idx = payload.get("server_index", 0)
    shows = payload.get("shows", [])
    if not shows:
        raise HTTPException(400, "No shows provided")

    r = await get_redis()
    raw = await r.get("sonarr_servers")
    if not raw:
        raise HTTPException(400, "No Sonarr servers configured — add one in Settings")
    servers = _json.loads(raw)
    if server_idx >= len(servers):
        raise HTTPException(400, f"Server index {server_idx} out of range")

    srv = servers[server_idx]
    client = SonarrClient(srv["url"], srv["api_key"], name=srv["name"])
    profile_id = srv.get("quality_profile_id")

    results = []
    for show in shows:
        result = await client.add_series(
            tvdb_id=show.get("tvdb_id"),
            imdb_id=show.get("imdb_id"),
            title=show.get("title", ""),
            year=show.get("year"),
            quality_profile_id=profile_id,
        )
        results.append(result)

    await client.close()

    added = sum(1 for r in results if r.get("status") == "ok")
    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(shows),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Auto-Send Toggles
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/auto-send")
async def get_auto_send_settings(db: AsyncSession = Depends(get_db)):
    """Read auto-send toggle state (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("auto_send_settings")
    if not raw:
        raw = await _get_setting(db, "auto_send_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    # Defaults: both off
    return {"radarr_enabled": False, "sonarr_enabled": False}


@router.put("/api/auto-send")
async def update_auto_send_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save auto-send toggle state to DB + Redis.

    Payload: {"radarr_enabled": true/false, "sonarr_enabled": true/false}
    """
    import json as _json
    r = await get_redis()
    auto_settings = {
        "radarr_enabled": bool(payload.get("radarr_enabled", False)),
        "sonarr_enabled": bool(payload.get("sonarr_enabled", False)),
    }
    encoded = _json.dumps(auto_settings)
    await r.set("auto_send_settings", encoded)
    await _put_setting(db, "auto_send_settings", encoded)
    await db.commit()
    log.info("auto_send.settings_saved", **auto_settings)
    return {"status": "ok", **auto_settings}


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue Settings
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/queue-settings")
async def get_queue_settings(db: AsyncSession = Depends(get_db)):
    """Read smart queue settings (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("queue_settings")
    if not raw:
        raw = await _get_setting(db, "queue_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    return {"s01e01_only": False}


@router.put("/api/queue-settings")
async def update_queue_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save smart queue settings to DB + Redis."""
    import json as _json
    r = await get_redis()
    queue_settings = {
        "s01e01_only": bool(payload.get("s01e01_only", False)),
    }
    encoded = _json.dumps(queue_settings)
    await r.set("queue_settings", encoded)
    await _put_setting(db, "queue_settings", encoded)
    await db.commit()
    log.info("queue_settings.saved", **queue_settings)
    return {"status": "ok", **queue_settings}


# ═══════════════════════════════════════════════════════════════════════════
# Settings API
# ═══════════════════════════════════════════════════════════════════════════

class SettingsRequest(BaseModel):
    trakt_client_id: str = None
    trakt_client_secret: str = None
    emby_url: str = None
    emby_api_key: str = None
    cron_smart_queue: str = None
    cron_ml_retrain: str = None
    cron_universe_scan: str = None
    features: dict = None


MASKED_SUFFIX = "****"


def _mask_api_key(key: str) -> str:
    """Return first 4 chars + **** for display. Never return the full key."""
    if not key:
        return ""
    return key[:4] + MASKED_SUFFIX


def _is_masked(key: str) -> bool:
    """True if the key looks like a masked value (ends with ****)."""
    return key.endswith(MASKED_SUFFIX) and len(key) <= 8


async def _resolve_servers(new_servers: list[dict], storage_key: str) -> list[dict]:
    """For each server in the incoming list, if the api_key is masked,
    look up the real key from the currently stored config."""
    import json as _json
    needs_resolve = any(_is_masked(s.get("api_key", "")) for s in new_servers)
    if not needs_resolve:
        return new_servers
    # Load existing servers to get real keys
    existing = {}
    try:
        r = await get_redis()
        raw = await r.get(storage_key)
        if raw:
            for srv in _json.loads(raw):
                existing[srv.get("url", "")] = srv.get("api_key", "")
    except Exception:
        pass
    resolved = []
    for s in new_servers:
        key = s.get("api_key", "")
        if _is_masked(key):
            # Preserve existing key matched by URL
            real_key = existing.get(s.get("url", "").rstrip("/"), "")
            if real_key:
                s = {**s, "api_key": real_key}
            else:
                continue  # can't resolve — skip this server
        resolved.append(s)
    return resolved


async def _get_setting(db: AsyncSession, key: str, env_fallback: str) -> str:
    """Read a setting from DB; fall back to .env value if no DB row."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    return row.value if row else env_fallback


async def _put_setting(db: AsyncSession, key: str, value: str):
    """Upsert a setting into the app_settings table."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    if row:
        row.value = value
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    else:
        db.add(AppSetting(key=key, value=value, updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))


@router.get("/api/settings")
async def read_settings(db: AsyncSession = Depends(get_db)):
    """Read current settings — DB overrides, .env fallbacks."""
    return {
        "trakt_client_id": os.getenv("TRAKT_CLIENT_ID", "")[:8] + "****" if os.getenv("TRAKT_CLIENT_ID") else "",
        "trakt_client_secret": os.getenv("TRAKT_CLIENT_SECRET", "")[:8] + "****" if os.getenv("TRAKT_CLIENT_SECRET") else "",
        "emby_url": os.getenv("EMBY_URL", ""),
        "emby_api_key": os.getenv("EMBY_API_KEY", "")[:8] + "****" if os.getenv("EMBY_API_KEY") else "",
        "cron_smart_queue": await _get_setting(db, "cron_smart_queue", os.getenv("SMART_QUEUE_CRON", "0 2 * * *")),
        "cron_ml_retrain": await _get_setting(db, "cron_ml_retrain", os.getenv("ML_RETRAIN_CRON", "0 4 * * 1")),
        "cron_universe_scan": await _get_setting(db, "cron_universe_scan", os.getenv("UNIVERSE_SCAN_CRON", "0 3 * * 0")),
        "features": {
            "smart_queue": os.getenv("ENABLE_SMART_QUEUE", "true").lower() == "true",
            "ml_predictor": os.getenv("ENABLE_ML_PREDICTOR", "true").lower() == "true",
            "universe_discovery": os.getenv("ENABLE_UNIVERSE_DISCOVERY", "true").lower() == "true",
            "watch_party": os.getenv("ENABLE_WATCH_PARTY", "true").lower() == "true",
        }
    }


@router.put("/api/settings")
async def update_settings(request: SettingsRequest, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Persist schedule settings to DB and reschedule live APScheduler jobs."""
    import json as _json
    from app.main import reschedule_job

    saved = []

    # Scheduler cron settings — persist and reschedule
    cron_map = {
        "cron_smart_queue": ("smart_queue", request.cron_smart_queue),
        "cron_ml_retrain": ("ml_retrain", request.cron_ml_retrain),
        "cron_universe_scan": ("universe_scan", request.cron_universe_scan),
    }
    for db_key, (job_id, cron_val) in cron_map.items():
        if cron_val:
            await _put_setting(db, db_key, cron_val)
            reschedule_job(job_id, cron_val)
            saved.append(db_key)

    await db.commit()

    return {
        "status": "ok",
        "saved": saved,
        "message": f"Saved {len(saved)} setting(s). Schedules updated immediately.",
    }


class TestConnectionRequest(BaseModel):
    service: str
    client_id: str | None = None
    client_secret: str | None = None
    url: str | None = None
    api_key: str | None = None


@router.post("/api/settings/test-connection")
async def test_connection(body: TestConnectionRequest, _user: User = Depends(get_current_user)):
    """Test Trakt or Emby connection (uses credentials from .env)."""
    service = body.service
    if service == "trakt":
        # Test Trakt API
        trakt = TraktClient()
        try:
            result = await trakt.get_trending(kind="shows")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await trakt.close()
    
    elif service == "emby":
        # Test Emby API
        emby = EmbyClient()
        try:
            info = await emby.get_system_info()
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await emby.close()

    return {"status": "error", "message": f"Unknown service: {service}"}


@router.post("/api/settings/reset-oauth")
async def reset_oauth(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Clear all stored Trakt OAuth tokens (users must re-link)."""
    users = (await db.execute(select(User))).scalars().all()
    for user in users:
        user.trakt_access_token = None
        user.trakt_refresh_token = None
        user.trakt_token_expires = None
    await db.commit()
    return {"status": "ok", "message": f"OAuth tokens cleared for {len(users)} user(s). Re-link on the Link page."}


@router.post("/api/settings/factory-reset")
async def factory_reset(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Delete all users (cascades to ratings, predictions, queue) and clear the library cache.
    Requires body: {"confirm": "FACTORY_RESET"}
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if body.get("confirm") != "FACTORY_RESET":
        raise HTTPException(400, "Confirmation required: send {\"confirm\": \"FACTORY_RESET\"}")

    from sqlalchemy import delete as sa_delete
    users = (await db.execute(select(User))).scalars().all()
    count = len(users)
    for user in users:
        await db.delete(user)
    await db.commit()
    try:
        await LibraryCache.clear()
    except Exception:
        pass
    log.warning("security.factory_reset", users_deleted=count)
    return {"status": "ok", "message": f"Factory reset complete. Removed {count} user(s) and cleared cache."}


# ═══════════════════════════════════════════════════════════════════════════
# Connection Status (heartbeat-based)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/connection-status")
async def connection_status():
    """Return cached heartbeat results for Emby, Trakt, and Radarr."""
    import json as _json
    r = await get_redis()
    result = {}
    for svc in ("emby", "trakt"):
        raw = await r.get(f"heartbeat:{svc}")
        if raw:
            result[svc] = _json.loads(raw)
        else:
            result[svc] = {"status": "unknown", "checked_at": None}
    # Radarr — may have 0, 1, or 2 servers
    radarr_list = []
    raw_servers = await r.get("radarr_servers")
    if raw_servers:
        servers = _json.loads(raw_servers)
        for i, _srv in enumerate(servers):
            raw_hb = await r.get(f"heartbeat:radarr:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"Radarr {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"Radarr {i+1}")}
            radarr_list.append(hb)
    result["radarr"] = radarr_list
    # Sonarr — may have 0, 1, or 2 servers
    sonarr_list = []
    raw_sonarr = await r.get("sonarr_servers")
    if raw_sonarr:
        sonarr_servers = _json.loads(raw_sonarr)
        for i, _srv in enumerate(sonarr_servers):
            raw_hb = await r.get(f"heartbeat:sonarr:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"Sonarr {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"Sonarr {i+1}")}
            sonarr_list.append(hb)
    result["sonarr"] = sonarr_list
    # SABnzbd — may have 0, 1, or 2 servers
    sab_list = []
    raw_sab = await r.get("sabnzbd_servers")
    if raw_sab:
        sab_servers = _json.loads(raw_sab)
        for i, _srv in enumerate(sab_servers):
            raw_hb = await r.get(f"heartbeat:sabnzbd:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"SABnzbd {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"SABnzbd {i+1}")}
            sab_list.append(hb)
    result["sabnzbd"] = sab_list
    # MDBList (optional — only present if API key is configured)
    raw_mdb = await r.get("heartbeat:mdblist")
    if raw_mdb:
        result["mdblist"] = _json.loads(raw_mdb)
    else:
        # Check if key is configured at all
        mdb_key = await r.get("mdblist_api_key")
        if mdb_key:
            result["mdblist"] = {"status": "unknown", "checked_at": None}
    # Integration provider
    raw_prov = await r.get("integration_provider")
    result["integration_provider"] = (raw_prov if isinstance(raw_prov, str) else raw_prov.decode()) if raw_prov else "trakt"
    return result


@router.post("/api/connection-status/refresh")
async def refresh_connection_status(_user: User = Depends(get_current_user)):
    """Force an immediate heartbeat check for all services."""
    from app.main import run_heartbeat
    await run_heartbeat()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════
# Watch History Stats
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/stats/person-items")
async def get_person_items(
    name: str,
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return all library items (movies + series) featuring a person.

    Queries Emby by person name — returns everything in the library,
    not just played items.  Used by the stats page hover modals.
    """
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "User not found")

    from app.utils.emby_client import EmbyClient

    emby = EmbyClient()
    try:
        resp = await emby.get_items(
            user_id=user.emby_user_id,
            item_type="Movie,Series",
            fields="ProductionYear",
            recursive=True,
            limit=200,
            extra_params={"Person": name},
        )
        items = resp.get("Items", [])
        titles = []
        for item in items:
            title = item.get("Name", "Unknown")
            year = item.get("ProductionYear")
            display = f"{title} ({year})" if year else title
            titles.append(display)
        return {"name": name, "titles": sorted(titles), "count": len(titles)}
    finally:
        await emby.close()


@router.get("/api/stats/{user_id}")
async def get_watch_stats(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return aggregated watch history stats for a user."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await watch_stats_svc.get_stats(user)


@router.get("/stats", response_class=HTMLResponse)
async def get_stats_page():
    """Serve the Watch Stats page."""
    try:
        with open("frontend/templates/stats.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/rewatch", response_class=HTMLResponse)
async def get_rewatch_page():
    """Serve the Rewatch Recommender page."""
    try:
        with open("frontend/templates/rewatch.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/guide", response_class=HTMLResponse)
async def get_guide_page():
    """Serve the User Guide page."""
    try:
        with open("frontend/templates/guide.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


# ═══════════════════════════════════════════════════════════════════════════
# Continue Watching Audit
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/continue-watching/{user_id}")
async def get_continue_watching(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return items the user started watching but hasn't finished.

    Uses Emby's ``Filters=IsResumable`` to find movies and episodes
    with an active playback resume point.  Episodes are grouped by
    their parent series for a cleaner display.
    """
    import json as _json

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "User not found or no Emby user linked")

    # Changed cache key so old stale cache is bypassed
    cache_key = f"continue_watching_v2:{user.id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    emby = EmbyClient()
    try:
        # Fetch all resumable items (movies + episodes with a resume point)
        start = 0
        batch = 500
        all_resumable: list[dict] = []
        while True:
            resp = await emby.get_items(
                user_id=user.emby_user_id,
                fields="ProviderIds,UserData,UserDataLastPlayedDate,RunTimeTicks",
                filters="IsResumable",
                sort_by="DatePlayed",
                sort_order="Descending",
                limit=batch,
                start_index=start,
            )
            all_resumable.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
    finally:
        await emby.close()

    log.info("continue_watching.fetched", resumable_count=len(all_resumable))

    movies: list[dict] = []
    # Group episodes by series
    series_map: dict[str, dict] = {}  # series_id → {info + episodes}

    for item in all_resumable:
        item_type = item.get("Type", "")
        ud = item.get("UserData", {})
        position_ticks = ud.get("PlaybackPositionTicks", 0) or 0
        runtime_ticks = item.get("RunTimeTicks", 0) or 0
        last_played = ud.get("LastPlayedDate")

        # Calculate progress percentage
        progress_pct = round(position_ticks / runtime_ticks * 100, 1) if runtime_ticks > 0 else 0

        # Calculate how long ago
        days_ago = None
        if last_played:
            try:
                lp_dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                days_ago = (datetime.now(timezone.utc) - lp_dt).days
            except (ValueError, AttributeError):
                pass

        if item_type == "Movie":
            movies.append({
                "emby_id": item.get("Id", ""),
                "title": item.get("Name", ""),
                "year": item.get("ProductionYear"),
                "type": "movie",
                "progress_pct": progress_pct,
                "position_ticks": position_ticks,
                "last_played": last_played,
                "days_ago": days_ago,
                "imdb_id": item.get("ProviderIds", {}).get("Imdb"),
            })

        elif item_type == "Episode":
            series_id = item.get("SeriesId", "")
            series_name = item.get("SeriesName", "")
            s_num = item.get("ParentIndexNumber", 0)
            e_num = item.get("IndexNumber", 0)
            ep_name = item.get("Name", "")

            if series_id not in series_map:
                series_map[series_id] = {
                    "emby_id": series_id,
                    "title": series_name,
                    "type": "show",
                    "episodes": [],
                    "last_played": last_played,
                    "days_ago": days_ago,
                }

            series_map[series_id]["episodes"].append({
                "emby_id": item.get("Id", ""),
                "season": s_num,
                "episode": e_num,
                "title": ep_name,
                "progress_pct": progress_pct,
                "position_ticks": position_ticks,
                "last_played": last_played,
            })

            # Update series-level last_played to the most recent episode
            existing_days = series_map[series_id].get("days_ago")
            if days_ago is not None and (existing_days is None or days_ago < existing_days):
                series_map[series_id]["days_ago"] = days_ago
                series_map[series_id]["last_played"] = last_played

    shows = list(series_map.values())
    for show in shows:
        show["episode_count"] = len(show["episodes"])
        show["episodes"].sort(key=lambda e: (e["season"], e["episode"]))
        # Set resume target to the most recently played episode
        most_recent = max(show["episodes"], key=lambda e: e.get("last_played") or "", default=None)
        if most_recent:
            show["resume_emby_id"] = most_recent["emby_id"]
            show["resume_ticks"] = most_recent.get("position_ticks", 0)

    # Combine and sort: oldest first (most abandoned)
    all_items = movies + shows
    all_items.sort(key=lambda x: x.get("days_ago") or 0, reverse=True)

    result = {"items": all_items, "total": len(all_items)}

    try:
        r = await get_redis()
        await r.setex(cache_key, 3600, _json.dumps(result))
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Radarr/Sonarr Availability Monitor
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/availability")
async def get_availability():
    """Check download status of items in Radarr/Sonarr.

    Cross-references queue items marked as not-in-library with their
    status in Radarr/Sonarr: monitored, downloading, available.
    Does NOT cache results when any server is unreachable so a
    subsequent request can pick up the missing server.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    # Check cache
    cache_key = "availability_monitor_v2"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    r = await get_redis()
    movies_status: list[dict] = []
    shows_status: list[dict] = []
    any_server_failed = False
    failed_servers: list[str] = []

    # --- Radarr movies ---
    raw_radarr = await r.get("radarr_servers")
    if raw_radarr:
        radarr_servers = _json.loads(raw_radarr)
        for srv in radarr_servers:
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                all_movies = await client.get_all_movies()
                # Fetch download queue for accurate status
                dl_queue = await client.get_download_queue()
                await client.close()
                dl_movie_ids = {d.get("tmdb_id") for d in dl_queue if d.get("tmdb_id")}

                for movie in all_movies:
                    if not movie.get("monitored", False):
                        continue
                    has_file = movie.get("hasFile", False)
                    tmdb_id = movie.get("tmdbId")
                    status = "available" if has_file else "monitored"

                    # Check real download queue for active download
                    if not has_file and tmdb_id in dl_movie_ids:
                        status = "downloading"

                    movies_status.append({
                        "title": movie.get("title", ""),
                        "year": movie.get("year"),
                        "tmdb_id": tmdb_id,
                        "imdb_id": movie.get("imdbId"),
                        "status": status,
                        "has_file": has_file,
                        "server": srv.get("name", "Radarr"),
                        "size_on_disk": movie.get("sizeOnDisk", 0),
                    })
            except Exception as e:
                any_server_failed = True
                failed_servers.append(srv.get("name", "Radarr"))
                log.warning("availability.radarr_failed", server=srv.get("name"), error=str(e)[:120])

    # --- Sonarr series ---
    raw_sonarr = await r.get("sonarr_servers")
    if raw_sonarr:
        sonarr_servers = _json.loads(raw_sonarr)
        for srv in sonarr_servers:
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                all_series = await client.get_all_series()
                # Fetch download queue for accurate status
                dl_queue = await client.get_download_queue()
                await client.close()
                dl_tvdb_ids = {d.get("tvdb_id") for d in dl_queue if d.get("tvdb_id")}

                for series in all_series:
                    if not series.get("monitored", False):
                        continue
                    stats = series.get("statistics") or {}
                    ep_file_count = stats.get("episodeFileCount", 0)
                    ep_count = stats.get("episodeCount", 0)
                    tvdb_id = series.get("tvdbId")

                    if ep_count == 0:
                        continue

                    if ep_file_count >= ep_count:
                        status = "available"
                    elif tvdb_id in dl_tvdb_ids:
                        status = "downloading"
                    elif ep_file_count > 0:
                        status = "partial"
                    else:
                        status = "monitored"

                    shows_status.append({
                        "title": series.get("title", ""),
                        "year": series.get("year"),
                        "tvdb_id": tvdb_id,
                        "imdb_id": series.get("imdbId"),
                        "status": status,
                        "episodes_on_disk": ep_file_count,
                        "episodes_total": ep_count,
                        "server": srv.get("name", "Sonarr"),
                        "size_on_disk": stats.get("sizeOnDisk", 0) or series.get("sizeOnDisk", 0),
                    })
            except Exception as e:
                any_server_failed = True
                failed_servers.append(srv.get("name", "Sonarr"))
                log.warning("availability.sonarr_failed", server=srv.get("name"), error=str(e)[:120])

    # Filter to show only items that aren't fully available yet
    pending_movies = [m for m in movies_status if m["status"] != "available"]
    pending_shows = [s for s in shows_status if s["status"] != "available"]

    result = {
        "movies": {
            "pending": pending_movies,
            "available_count": len(movies_status) - len(pending_movies),
            "total_monitored": len(movies_status),
        },
        "shows": {
            "pending": pending_shows,
            "available_count": len(shows_status) - len(pending_shows),
            "total_monitored": len(shows_status),
        },
        "partial": any_server_failed,
        "failed_servers": failed_servers,
    }

    # Only cache if ALL servers responded — partial results should be
    # retried on the next request so the waking server gets picked up.
    if not any_server_failed:
        try:
            r = await get_redis()
            await r.setex(cache_key, 300, _json.dumps(result))
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Download Queue Progress (Radarr/Sonarr active downloads)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/download-queue")
async def get_download_queue():
    """Fetch active download queue from all Radarr/Sonarr servers.

    Returns items currently being downloaded with progress, ETA, and
    size info.  Keyed by tmdb_id (movies) and tvdb_id (shows) so the
    frontend can match them to smart queue cards.

    No caching — SABnzbd/Radarr/Sonarr on LAN respond in <100ms.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    r = await get_redis()

    downloads: list[dict] = []

    # --- Radarr queues ---
    raw_radarr = await r.get("radarr_servers")
    if raw_radarr:
        for srv in _json.loads(raw_radarr):
            client = None
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                items = await client.get_download_queue()
                downloads.extend(items)
            except Exception as e:
                log.warning("download_queue.radarr_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # --- Sonarr queues ---
    raw_sonarr = await r.get("sonarr_servers")
    if raw_sonarr:
        for srv in _json.loads(raw_sonarr):
            client = None
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                items = await client.get_download_queue()
                downloads.extend(items)
            except Exception as e:
                log.warning("download_queue.sonarr_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # --- SABnzbd enrichment ---
    # Build a lookup of nzo_id → SABnzbd slot data from all configured
    # SABnzbd instances, then overlay real-time progress onto the
    # Radarr/Sonarr items matched by downloadId.
    sab_lookup: dict[str, dict] = {}
    raw_sab = await r.get("sabnzbd_servers")
    if raw_sab:
        from app.utils.sabnzbd_client import SabnzbdClient
        for srv in _json.loads(raw_sab):
            client = None
            try:
                client = SabnzbdClient(srv["url"], srv["api_key"], name=srv.get("name", "SABnzbd"))
                slots = await client.get_queue()
                for slot in slots:
                    nzo = slot.get("nzo_id")
                    if nzo:
                        sab_lookup[nzo] = slot
                # Also fetch history for post-processing states
                history = await client.get_history(limit=10)
                for slot in history:
                    nzo = slot.get("nzo_id")
                    if nzo and nzo not in sab_lookup:
                        sab_lookup[nzo] = slot
            except Exception as e:
                log.warning("download_queue.sabnzbd_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # Merge: replace Radarr/Sonarr progress with SABnzbd real-time data
    if sab_lookup:
        for dl in downloads:
            did = dl.get("download_id", "")
            sab = sab_lookup.get(did)
            if sab:
                dl["progress"] = sab["progress"]
                dl["size_mb"] = sab["size_mb"]
                dl["sizeleft_mb"] = sab["sizeleft_mb"]
                dl["sab_status"] = sab["status"]
                dl["sab_eta"] = sab["timeleft"]
                dl["sab_speed"] = sab["speed"]

    result = {"downloads": downloads, "count": len(downloads)}

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Realtime Download Progress (SABnzbd only — lightweight)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/download-progress")
async def get_download_progress():
    """Lightweight SABnzbd-only progress snapshot.

    Returns only nzo_id-keyed progress data — no Radarr/Sonarr calls.
    Designed to be polled at 500ms for realtime progress bar updates.
    No caching — SABnzbd on LAN responds in <50ms.
    """
    import json as _json
    r = await get_redis()
    raw_sab = await r.get("sabnzbd_servers")
    if not raw_sab:
        return {"slots": {}, "count": 0}

    from app.utils.sabnzbd_client import SabnzbdClient

    slots: dict[str, dict] = {}
    for srv in _json.loads(raw_sab):
        client = None
        try:
            client = SabnzbdClient(srv["url"], srv["api_key"], name=srv.get("name", "SABnzbd"))
            for slot in await client.get_queue():
                nzo = slot.get("nzo_id")
                if nzo:
                    slots[nzo] = {
                        "progress": slot["progress"],
                        "speed": slot["speed"],
                        "eta": slot["timeleft"],
                        "status": slot["status"],
                        "sizeleft_mb": slot["sizeleft_mb"],
                    }
            # Also fetch history for post-processing states
            for slot in await client.get_history(limit=10):
                nzo = slot.get("nzo_id")
                if nzo and nzo not in slots:
                    slots[nzo] = {
                        "progress": slot["progress"],
                        "speed": "",
                        "eta": "",
                        "status": slot["status"],
                        "sizeleft_mb": 0,
                    }
        except Exception as e:
            log.warning("download_progress.sabnzbd_failed",
                        server=srv.get("name"), error=str(e)[:120])
        finally:
            if client:
                await client.close()

    return {"slots": slots, "count": len(slots)}


# ═══════════════════════════════════════════════════════════════════════════
# SABnzbd Server Management
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/sabnzbd/servers")
async def get_sabnzbd_servers():
    """Read configured SABnzbd servers from Redis."""
    import json as _json
    r = await get_redis()
    raw = await r.get("sabnzbd_servers")
    servers = _json.loads(raw) if raw else []
    # Mask API keys
    masked = []
    for srv in servers:
        masked.append({
            **srv,
            "api_key": srv["api_key"][:4] + "****" if len(srv.get("api_key", "")) > 4 else "****",
        })
    return {"servers": masked}


@router.put("/api/sabnzbd/servers")
async def save_sabnzbd_servers(request: Request, _user: User = Depends(get_current_user)):
    """Save SABnzbd server configs (max 2) to Redis + DB."""
    import json as _json
    body = await request.json()
    servers = body.get("servers", [])[:2]

    r = await get_redis()

    # Resolve masked keys — if a key looks masked, keep the existing one
    raw_existing = await r.get("sabnzbd_servers")
    existing = _json.loads(raw_existing) if raw_existing else []

    for i, srv in enumerate(servers):
        key = srv.get("api_key", "")
        if "****" in key and i < len(existing):
            srv["api_key"] = existing[i].get("api_key", key)

    encoded = _json.dumps(servers)
    await r.set("sabnzbd_servers", encoded)

    # Persist to DB (survives Redis restarts)
    async with async_session_ctx() as db:
        await _put_setting(db, "sabnzbd_servers", encoded)

    # Invalidate download-queue cache so the next poll picks up the new server
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass

    return {"status": "ok", "servers": len(servers)}


@router.post("/api/sabnzbd/test")
async def test_sabnzbd(request: Request, _user: User = Depends(get_current_user)):
    """Test connection to a SABnzbd server."""
    import json as _json
    from app.utils.sabnzbd_client import SabnzbdClient
    body = await request.json()
    url = body.get("url", "").strip()
    api_key = body.get("api_key", "").strip()
    if not url or not api_key:
        return {"status": "error", "message": "URL and API key required"}

    # Resolve masked key — if the frontend sent a masked value,
    # find the real key from the saved server config by matching URL
    if "****" in api_key:
        r = await get_redis()
        raw_existing = await r.get("sabnzbd_servers")
        if raw_existing:
            for srv in _json.loads(raw_existing):
                if srv.get("url", "").rstrip("/") == url.rstrip("/"):
                    api_key = srv.get("api_key", api_key)
                    break

    client = None
    try:
        client = SabnzbdClient(url, api_key)
        result = await client.test_connection()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}
    finally:
        if client:
            await client.close()


# ═══════════════════════════════════════════════════════════════════════════
# Watchlist Sync (Radarr/Sonarr → Trakt Watchlist)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/watchlist-sync/run")
async def run_watchlist_sync(
    current_user: User = Depends(get_current_user),
):
    """Manually trigger a Radarr/Sonarr ↔ Trakt watchlist sync."""
    from app.services.watchlist_sync.service import WatchlistSyncService
    svc = WatchlistSyncService()
    try:
        await svc._sync_user(current_user)
        return {"status": "ok"}
    except Exception as e:
        log.exception("watchlist_sync.manual_failed", user_id=current_user.id)
        raise HTTPException(500, f"Watchlist sync failed: {e}")


@router.get("/api/watchlist-sync/settings")
async def get_watchlist_sync_settings(db: AsyncSession = Depends(get_db)):
    """Read watchlist sync toggle state (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("watchlist_sync_settings")
    if not raw:
        raw = await _get_setting(db, "watchlist_sync_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    return {"arr_to_watchlist": False, "watchlist_to_arr": False}


@router.put("/api/watchlist-sync/settings")
async def update_watchlist_sync_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save watchlist sync toggle state to DB + Redis.

    Payload: {"arr_to_watchlist": true/false, "watchlist_to_arr": true/false}
    """
    import json as _json
    r = await get_redis()
    sync_settings = {
        "arr_to_watchlist": bool(payload.get("arr_to_watchlist", False)),
        "watchlist_to_arr": bool(payload.get("watchlist_to_arr", False)),
    }
    encoded = _json.dumps(sync_settings)
    await r.set("watchlist_sync_settings", encoded)
    await _put_setting(db, "watchlist_sync_settings", encoded)
    await db.commit()
    log.info("watchlist_sync.settings_saved", **sync_settings)
    return {"status": "ok", **sync_settings}


# ═══════════════════════════════════════════════════════════════════════════
# TMDB API Key
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/tmdb/key")
async def get_tmdb_key(db: AsyncSession = Depends(get_db)):
    """Return whether a TMDB API key is configured (never returns the key itself)."""
    r = await get_redis()
    raw = await r.get("tmdb_api_key")
    if not raw:
        raw = await _get_setting(db, "tmdb_api_key", "")
        if raw:
            await r.set("tmdb_api_key", raw)
    return {"configured": bool(raw)}


@router.put("/api/tmdb/key")
async def save_tmdb_key(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save or clear the TMDB API key."""
    import json as _json
    key = (payload.get("api_key") or "").strip()
    r = await get_redis()
    if key:
        await r.set("tmdb_api_key", key)
        await _put_setting(db, "tmdb_api_key", key)
        await db.commit()
        # Clear any cached empty provider results from before the key was set
        try:
            cached_keys = []
            cursor = b"0"
            while True:
                cursor, keys = await r.scan(cursor, match="tmdb_providers:*", count=100)
                cached_keys.extend(keys)
                if cursor == b"0" or cursor == 0:
                    break
            if cached_keys:
                await r.delete(*cached_keys)
                log.info("tmdb.cache_cleared", keys_removed=len(cached_keys))
        except Exception:
            pass
        return {"status": "ok", "configured": True}
    else:
        await r.delete("tmdb_api_key")
        await _put_setting(db, "tmdb_api_key", "")
        await db.commit()
        return {"status": "ok", "configured": False}


@router.post("/api/tmdb/test")
async def test_tmdb_key(payload: dict, _user: User = Depends(get_current_user)):
    """Test a TMDB API key by fetching a known movie."""
    import httpx
    key = (payload.get("api_key") or "").strip()
    if not key:
        raise HTTPException(400, "api_key required")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.themoviedb.org/3/movie/550",
                params={"api_key": key},
            )
            if resp.status_code == 401:
                return {"status": "error", "message": "Invalid API key"}
            resp.raise_for_status()
            data = resp.json()
            return {"status": "ok", "message": f"Connected — {data.get('title', 'OK')}"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════════════════
# Trakt Personal Lists → Emby Playlists
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/trakt-lists")
async def get_trakt_lists():
    """Fetch all Trakt lists available to the user: personal, liked, and collaborations."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.trakt_access_token:
            raise HTTPException(400, "No Trakt-linked user found")

        async def _on_refresh(access, refresh, expires):
            async with async_session_ctx() as rdb:
                u = (await rdb.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await rdb.commit()

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=_on_refresh,
        )

    try:
        my_lists = await trakt.get_my_lists()
        liked_lists = await trakt.get_liked_lists()
        collab_lists = await trakt.get_collaborations()
    finally:
        await trakt.close()

    results = []
    seen_slugs = set()

    def _add(lst, owner):
        ids = lst.get("ids", {})
        slug = ids.get("slug", "")
        if slug in seen_slugs:
            return
        seen_slugs.add(slug)
        u = lst.get("user", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": slug,
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "privacy": lst.get("privacy", "private"),
            "likes": lst.get("likes", 0),
            "owner": owner,
            "user_name": u.get("username", ""),
        })

    for lst in (my_lists or []):
        _add(lst, "self")

    for entry in (liked_lists or []):
        # Liked lists response wraps list in a "list" key
        lst = entry.get("list", entry)
        _add(lst, "liked")

    for lst in (collab_lists or []):
        _add(lst, "collaboration")

    return {"lists": results}


@router.post("/api/trakt-lists/import")
async def import_trakt_list(payload: dict, _user: User = Depends(get_current_user)):
    """Import a Trakt list into an Emby playlist.

    Payload: {"list_slug": "...", "playlist_name": "...", "username": "..."}
    username defaults to "me" for the user's own lists.
    Resolves list items against LibraryCache, creates an Emby playlist
    with matched items in list order.
    """
    list_slug = (payload.get("list_slug") or "").strip()
    if not list_slug:
        raise HTTPException(400, "list_slug required")
    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()
    username = (payload.get("username") or "").strip() or "me"

    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.trakt_access_token:
            raise HTTPException(400, "No Trakt-linked user found")

        async def _on_refresh(access, refresh, expires):
            async with async_session_ctx() as rdb:
                u = (await rdb.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await rdb.commit()

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=_on_refresh,
        )

    try:
        # Fetch items — the endpoint returns items under /users/{username}/lists/{slug}/items
        items = await trakt.get_list_items(username, list_slug)
    finally:
        await trakt.close()

    if not items:
        return {"status": "ok", "matched": 0, "unmatched": 0, "message": "List is empty"}

    emby = EmbyClient()
    emby_ids = []
    unmatched = []

    try:
        for entry in items:
            # Each entry has a type key ("movie", "show") and the item data under that key
            item_type = entry.get("type", "")
            item_data = entry.get(item_type, {}) if item_type else {}
            ids = item_data.get("ids", {})
            title = item_data.get("title", "Unknown")

            # Try to resolve via LibraryCache using provider IDs
            match = None

            # Try IMDB
            if ids.get("imdb"):
                match = await LibraryCache.find_by_provider_id("Imdb", ids["imdb"])

            # Try TMDB
            if not match and ids.get("tmdb"):
                match = await LibraryCache.find_by_provider_id("Tmdb", str(ids["tmdb"]))

            # Try TVDB (shows)
            if not match and ids.get("tvdb"):
                match = await LibraryCache.find_by_provider_id("Tvdb", str(ids["tvdb"]))

            if match and match.get("emby_id"):
                emby_ids.append(match["emby_id"])
            else:
                unmatched.append({"title": title, "year": item_data.get("year")})

        # Create Emby playlist
        playlist_id = None
        if emby_ids:
            emby_user_id = (await _first_emby_user_id()) or None
            final_name = playlist_name or f"📋 {list_slug}"
            playlist_id = await emby.recreate_playlist(
                final_name, emby_ids, user_id=emby_user_id,
            )
            # Set Overview (description) on the playlist item
            if playlist_id and description:
                await emby.set_playlist_overview(
                    playlist_id, description,
                    user_id=emby_user_id,
                )
            log.info("trakt_list.imported", slug=list_slug, name=final_name,
                     matched=len(emby_ids), unmatched=len(unmatched))
    finally:
        await emby.close()

    return {
        "status": "ok",
        "matched": len(emby_ids),
        "unmatched": len(unmatched),
        "unmatched_items": unmatched[:20],  # cap to avoid huge responses
        "playlist_id": playlist_id,
    }



# ═══════════════════════════════════════════════════════════════════════════
# MDBList Integration
# ═══════════════════════════════════════════════════════════════════════════


async def _get_mdblist_key(db: AsyncSession | None = None) -> str:
    """Return the configured MDBList API key from Redis (fast) or DB fallback."""
    r = await get_redis()
    raw = await r.get("mdblist_api_key")
    if raw:
        return raw if isinstance(raw, str) else raw.decode()
    if db:
        raw = await _get_setting(db, "mdblist_api_key", "")
        if raw:
            await r.set("mdblist_api_key", raw)
        return raw or ""
    return ""


@router.get("/api/mdblist/key")
async def get_mdblist_key(db: AsyncSession = Depends(get_db)):
    """Return whether an MDBList API key is configured."""
    key = await _get_mdblist_key(db)
    return {"configured": bool(key)}


@router.put("/api/mdblist/key")
async def save_mdblist_key(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save or clear the MDBList API key."""
    key = (payload.get("api_key") or "").strip()
    r = await get_redis()
    if key:
        await r.set("mdblist_api_key", key)
        await _put_setting(db, "mdblist_api_key", key)
        await db.commit()
        return {"status": "ok", "configured": True}
    else:
        await r.delete("mdblist_api_key")
        await _put_setting(db, "mdblist_api_key", "")
        await db.commit()
        return {"status": "ok", "configured": False}


@router.post("/api/mdblist/test")
async def test_mdblist_key(payload: dict, _user: User = Depends(get_current_user)):
    """Test an MDBList API key."""
    from app.utils.mdblist_client import MDBListClient
    key = (payload.get("api_key") or "").strip()
    if not key:
        raise HTTPException(400, "api_key required")
    client = MDBListClient(key)
    try:
        result = await client.test_connection()
        if result["status"] == "ok":
            return {
                "status": "ok",
                "message": (
                    f"Connected — {result['username']} "
                    f"({result['plan']}, "
                    f"{result['requests_remaining']}/{result['requests_limit']} requests left)"
                ),
            }
        return {"status": "error", "message": result.get("message", "Unknown error")}
    finally:
        await client.close()


@router.get("/api/mdblist/lists")
async def get_mdblist_lists(db: AsyncSession = Depends(get_db)):
    """Fetch all lists available to the MDBList user.
    Returns own lists (dynamic, static, private) and liked lists.
    """
    from app.utils.mdblist_client import MDBListClient
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    client = MDBListClient(key)
    try:
        my_lists = await client.get_my_lists()
        liked_lists = await client.get_liked_lists()
    finally:
        await client.close()

    # Normalise into a unified format
    results = []
    seen_ids = set()

    for lst in (my_lists or []):
        lid = lst.get("id")
        if lid in seen_ids:
            continue
        seen_ids.add(lid)
        results.append({
            "id": lid,
            "name": lst.get("name", ""),
            "slug": lst.get("slug", ""),
            "description": lst.get("description") or "",
            "mediatype": lst.get("mediatype", ""),
            "items": lst.get("items", 0),
            "likes": lst.get("likes", 0),
            "type": lst.get("type", "static"),
            "dynamic": lst.get("dynamic", False),
            "private": lst.get("private", False),
            "owner": "self",
            "user_name": lst.get("user_name", ""),
        })

    for lst in (liked_lists or []):
        if not isinstance(lst, dict):
            continue
        lid = lst.get("id")
        if not lid or lid in seen_ids:
            continue
        seen_ids.add(lid)
        results.append({
            "id": lid,
            "name": lst.get("name", ""),
            "slug": lst.get("slug", ""),
            "description": lst.get("description") or "",
            "mediatype": lst.get("mediatype", ""),
            "items": lst.get("items", 0),
            "likes": lst.get("likes", 0),
            "type": lst.get("type", "static"),
            "dynamic": lst.get("dynamic", False),
            "private": lst.get("private", False),
            "owner": "liked",
            "user_name": lst.get("user_name", ""),
        })

    return {"lists": results}


@router.post("/api/mdblist/import")
async def import_mdblist_list(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Import an MDBList list into an Emby playlist.

    Payload: {"list_id": 123, "playlist_name": "...", "description": "..."}
    Resolves list items against LibraryCache, creates an Emby playlist
    with matched items in list order.
    """
    from app.utils.mdblist_client import MDBListClient

    list_id = payload.get("list_id")
    if not list_id:
        raise HTTPException(400, "list_id required")
    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    client = MDBListClient(key)
    try:
        items = await client.get_all_list_items(int(list_id))
    finally:
        await client.close()

    if not items:
        return {"status": "ok", "matched": 0, "unmatched": 0, "message": "List is empty"}

    emby = EmbyClient()
    emby_ids = []
    unmatched = []

    try:
        for entry in items:
            ids = entry.get("ids") or {}
            imdb_id = entry.get("imdb_id") or ids.get("imdb")
            tmdb_id = ids.get("tmdb") or entry.get("id")
            tvdb_id = entry.get("tvdb_id") or ids.get("tvdb")
            title = entry.get("title", "Unknown")
            mediatype = entry.get("mediatype", "movie")

            match = None

            # Try IMDB
            if imdb_id:
                match = await LibraryCache.find_by_provider_id("Imdb", str(imdb_id))

            # Try TMDB
            if not match and tmdb_id:
                match = await LibraryCache.find_by_provider_id("Tmdb", str(tmdb_id))

            # Try TVDB (shows)
            if not match and tvdb_id:
                match = await LibraryCache.find_by_provider_id("Tvdb", str(tvdb_id))

            if match and match.get("emby_id"):
                emby_ids.append(match["emby_id"])
            else:
                unmatched.append({
                    "title": title,
                    "year": entry.get("release_year"),
                    "type": mediatype,
                })

        # Create Emby playlist
        playlist_id = None
        if emby_ids:
            emby_user_id = (await _first_emby_user_id()) or None
            final_name = playlist_name or f"📋 MDB: {list_id}"
            playlist_id = await emby.recreate_playlist(
                final_name, emby_ids, user_id=emby_user_id,
            )
            if playlist_id and description:
                await emby.set_playlist_overview(
                    playlist_id, description,
                    user_id=emby_user_id,
                )
            log.info("mdblist.imported", list_id=list_id, name=final_name,
                     matched=len(emby_ids), unmatched=len(unmatched))
    finally:
        await emby.close()

    # Track the import for auto-sync
    import json as _json
    r = await get_redis()
    synced_key = "mdblist_synced_lists"
    raw = await r.get(synced_key)
    synced = _json.loads(raw) if raw else []

    # Update or add entry
    entry_found = False
    for entry in synced:
        if entry.get("list_id") == int(list_id):
            entry["playlist_name"] = playlist_name or entry.get("playlist_name", "")
            entry["description"] = description or entry.get("description", "")
            entry["last_synced"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            entry["matched"] = len(emby_ids)
            entry_found = True
            break

    if not entry_found:
        synced.append({
            "list_id": int(list_id),
            "playlist_name": playlist_name or f"📋 MDB: {list_id}",
            "description": description,
            "last_synced": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "matched": len(emby_ids),
            "auto_sync": True,
        })

    await r.set(synced_key, _json.dumps(synced))
    # Also persist to DB so it survives Redis restart
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()

    return {
        "status": "ok",
        "matched": len(emby_ids),
        "unmatched": len(unmatched),
        "unmatched_items": unmatched[:20],
        "playlist_id": playlist_id,
    }


# -- Trakt synced list tracking (mirrors MDBList pattern) ------------------

@router.get("/api/trakt-lists/synced")
async def get_trakt_synced(db: AsyncSession = Depends(get_db)):
    """Return Trakt lists that have been imported and are tracked for sync."""
    import json as _json
    r = await get_redis()
    raw = await r.get("trakt_synced_lists")
    if not raw:
        raw = await _get_setting(db, "trakt_synced_lists", "[]")
        if raw and raw != "[]":
            await r.set("trakt_synced_lists", raw)
    synced = _json.loads(raw) if raw else []
    return {"synced": synced}


@router.post("/api/trakt-lists/track")
async def track_trakt_list(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Add or update a Trakt list in synced tracking after import."""
    import json as _json
    slug = (payload.get("list_slug") or "").strip()
    if not slug:
        raise HTTPException(400, "list_slug required")

    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()
    username = (payload.get("username") or "").strip() or "me"
    matched = payload.get("matched", 0)

    r = await get_redis()
    raw = await r.get("trakt_synced_lists")
    synced = _json.loads(raw) if raw else []

    entry_found = False
    for entry in synced:
        if entry.get("slug") == slug:
            entry["playlist_name"] = playlist_name or entry.get("playlist_name", "")
            entry["description"] = description or entry.get("description", "")
            entry["username"] = username
            entry["last_synced"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            entry["matched"] = matched
            entry_found = True
            break

    if not entry_found:
        synced.append({
            "slug": slug,
            "playlist_name": playlist_name or f"📋 {slug}",
            "description": description,
            "username": username,
            "last_synced": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "matched": matched,
            "auto_sync": True,
        })

    await r.set("trakt_synced_lists", _json.dumps(synced))
    await _put_setting(db, "trakt_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.put("/api/trakt-lists/synced/{slug}/auto-sync")
async def toggle_trakt_auto_sync(slug: str, payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Toggle auto-sync for a tracked Trakt list."""
    import json as _json
    enabled = payload.get("enabled", True)
    r = await get_redis()
    raw = await r.get("trakt_synced_lists")
    synced = _json.loads(raw) if raw else []

    for entry in synced:
        if entry.get("slug") == slug:
            entry["auto_sync"] = enabled
            break
    else:
        raise HTTPException(404, "List not tracked")

    await r.set("trakt_synced_lists", _json.dumps(synced))
    await _put_setting(db, "trakt_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug, "auto_sync": enabled}


@router.delete("/api/trakt-lists/synced/{slug}")
async def remove_trakt_synced(slug: str, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Remove a Trakt list from sync tracking (does NOT delete the Emby playlist)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("trakt_synced_lists")
    synced = _json.loads(raw) if raw else []

    synced = [e for e in synced if e.get("slug") != slug]

    await r.set("trakt_synced_lists", _json.dumps(synced))
    await _put_setting(db, "trakt_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.post("/api/trakt-lists/sync-all")
async def sync_all_trakt_lists(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Re-import all auto-synced Trakt lists."""
    import json as _json
    r = await get_redis()
    raw = await r.get("trakt_synced_lists")
    if not raw:
        raw = await _get_setting(db, "trakt_synced_lists", "[]")
    synced = _json.loads(raw) if raw else []

    results = []
    for entry in synced:
        if not entry.get("auto_sync", True):
            results.append({"slug": entry["slug"], "status": "skipped", "reason": "auto_sync_off"})
            continue
        try:
            result = await import_trakt_list({
                "list_slug": entry["slug"],
                "playlist_name": entry.get("playlist_name", ""),
                "description": entry.get("description", ""),
                "username": entry.get("username", "me"),
            })
            results.append({"slug": entry["slug"], "status": "ok", "matched": result.get("matched", 0)})
        except Exception as e:
            results.append({"slug": entry["slug"], "status": "error", "message": str(e)[:200]})

    return {"status": "ok", "results": results}


@router.get("/api/trakt-lists/popular")
async def get_trakt_popular_lists():
    """Fetch popular Trakt community lists (public endpoint, no auth needed)."""
    trakt = TraktClient()

    try:
        raw = await trakt.get_popular_lists(limit=25)
    finally:
        await trakt.close()

    results = []
    for entry in (raw or []):
        lst = entry.get("list", entry)
        u = lst.get("user", {})
        ids = lst.get("ids", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": ids.get("slug", ""),
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "likes": lst.get("likes", 0) if "likes" in lst else entry.get("like_count", 0),
            "user_name": u.get("username", ""),
        })
    return {"lists": results}


@router.get("/api/trakt-lists/trending")
async def get_trakt_trending_lists():
    """Fetch trending Trakt community lists (public endpoint, no auth needed)."""
    trakt = TraktClient()

    try:
        raw = await trakt.get_trending_lists(limit=25)
    finally:
        await trakt.close()

    results = []
    for entry in (raw or []):
        lst = entry.get("list", entry)
        u = lst.get("user", {})
        ids = lst.get("ids", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": ids.get("slug", ""),
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "likes": lst.get("likes", 0) if "likes" in lst else entry.get("like_count", 0),
            "user_name": u.get("username", ""),
        })
    return {"lists": results}


@router.get("/api/trakt-lists/items")
async def get_trakt_list_items_detail(slug: str, username: str = "me"):
    """Fetch items from a Trakt list with in-library/missing status for each item."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.trakt_access_token:
            raise HTTPException(400, "No Trakt-linked user found")

        async def _on_refresh(access, refresh, expires):
            async with async_session_ctx() as rdb:
                u = (await rdb.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await rdb.commit()

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=_on_refresh,
        )

    try:
        items = await trakt.get_list_items(username, slug)
    finally:
        await trakt.close()

    results = []
    for entry in (items or []):
        item_type = entry.get("type", "")
        item_data = entry.get(item_type, {}) if item_type else {}
        ids = item_data.get("ids", {})
        title = item_data.get("title", "Unknown")
        year = item_data.get("year")

        # Resolve against library cache
        match = None
        if ids.get("imdb"):
            match = await LibraryCache.find_by_provider_id("Imdb", ids["imdb"])
        if not match and ids.get("tmdb"):
            match = await LibraryCache.find_by_provider_id("Tmdb", str(ids["tmdb"]))
        if not match and ids.get("tvdb"):
            match = await LibraryCache.find_by_provider_id("Tvdb", str(ids["tvdb"]))

        in_library = bool(match and match.get("emby_id"))
        results.append({
            "title": title,
            "year": year,
            "type": item_type,
            "in_library": in_library,
            "imdb_id": ids.get("imdb"),
            "tmdb_id": ids.get("tmdb"),
            "tvdb_id": ids.get("tvdb"),
        })

    in_lib = sum(1 for r in results if r["in_library"])
    return {"items": results, "total": len(results), "in_library": in_lib, "missing": len(results) - in_lib}


@router.get("/api/mdblist/synced")
async def get_mdblist_synced(db: AsyncSession = Depends(get_db)):
    """Return the list of MDBList lists that have been imported and are tracked for auto-sync."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    if not raw:
        raw = await _get_setting(db, "mdblist_synced_lists", "[]")
        if raw and raw != "[]":
            await r.set("mdblist_synced_lists", raw)
    synced = _json.loads(raw) if raw else []
    return {"synced": synced}


@router.put("/api/mdblist/synced/{list_id}/auto-sync")
async def toggle_mdblist_auto_sync(list_id: int, payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Toggle auto-sync on/off for a synced MDBList list."""
    import json as _json
    enabled = payload.get("enabled", True)
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    synced = _json.loads(raw) if raw else []

    for entry in synced:
        if entry.get("list_id") == list_id:
            entry["auto_sync"] = enabled
            break
    else:
        raise HTTPException(404, "List not tracked")

    await r.set("mdblist_synced_lists", _json.dumps(synced))
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "list_id": list_id, "auto_sync": enabled}


@router.delete("/api/mdblist/synced/{list_id}")
async def remove_mdblist_synced(list_id: int, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Remove a list from auto-sync tracking (does NOT delete the Emby playlist)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    synced = _json.loads(raw) if raw else []

    synced = [e for e in synced if e.get("list_id") != list_id]

    await r.set("mdblist_synced_lists", _json.dumps(synced))
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "list_id": list_id}


@router.post("/api/mdblist/sync-all")
async def sync_all_mdblist_lists(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Re-import all auto-synced MDBList lists (used by the daily cron and manual refresh)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    if not raw:
        raw = await _get_setting(db, "mdblist_synced_lists", "[]")
    synced = _json.loads(raw) if raw else []

    key = await _get_mdblist_key(db)
    if not key:
        return {"status": "skipped", "reason": "no_api_key"}

    results = []
    for entry in synced:
        if not entry.get("auto_sync", True):
            results.append({"list_id": entry["list_id"], "status": "skipped", "reason": "auto_sync_off"})
            continue
        try:
            result = await import_mdblist_list(
                {
                    "list_id": entry["list_id"],
                    "playlist_name": entry.get("playlist_name", ""),
                    "description": entry.get("description", ""),
                },
                db,
            )
            results.append({"list_id": entry["list_id"], "status": "ok", "matched": result.get("matched", 0)})
        except Exception as e:
            results.append({"list_id": entry["list_id"], "status": "error", "message": str(e)[:200]})

    return {"status": "ok", "results": results}


@router.get("/api/mdblist/items")
async def get_mdblist_list_items_detail(list_id: int, db: AsyncSession = Depends(get_db)):
    """Fetch items from an MDBList list with in-library/missing status for each item."""
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        items = await client.get_all_list_items(list_id)
    finally:
        await client.close()

    results = []
    for item in items:
        imdb_id = item.get("imdb_id") or ""
        tvdb_id = item.get("tvdb_id")
        ids = item.get("ids") or {}
        tmdb_id = ids.get("tmdb") or item.get("tmdb_id")
        title = item.get("title", "Unknown")
        year = item.get("release_year") or item.get("year")
        mediatype = item.get("mediatype", "movie")

        # Resolve against library cache
        match = None
        if imdb_id:
            match = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
        if not match and tmdb_id:
            match = await LibraryCache.find_by_provider_id("Tmdb", str(tmdb_id))
        if not match and tvdb_id:
            match = await LibraryCache.find_by_provider_id("Tvdb", str(tvdb_id))

        in_library = bool(match and match.get("emby_id"))
        results.append({
            "title": title,
            "year": year,
            "type": "show" if mediatype == "show" else "movie",
            "in_library": in_library,
            "imdb_id": imdb_id or None,
            "tmdb_id": tmdb_id,
            "tvdb_id": tvdb_id,
        })

    in_lib = sum(1 for r in results if r["in_library"])
    return {"items": results, "total": len(results), "in_library": in_lib, "missing": len(results) - in_lib}


@router.get("/api/mdblist/top")
async def get_mdblist_top_lists(db: AsyncSession = Depends(get_db)):
    """Fetch top/popular public lists from MDBList."""
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        raw = await client.get_top_lists()
    finally:
        await client.close()

    results = []
    for lst in (raw or []):
        results.append({
            "name": lst.get("name", ""),
            "list_id": lst.get("id", 0),
            "item_count": lst.get("items", lst.get("item_count", 0)),
            "likes": lst.get("likes", 0),
            "user_name": lst.get("user_name", lst.get("username", "")),
            "mediatype": lst.get("mediatype", ""),
            "dynamic": lst.get("dynamic", False),
        })
    return {"lists": results}


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue History & Feedback
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/queue-history/{user_id}")
async def get_queue_history(
    user_id: int,
    days: int = Query(90, ge=7, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Return queue recommendation vs play stats over time.

    Shows play rates by source, current scoring weights, and recent
    recommendation items with their played/ignored status.
    """
    from app.utils.redis_cache import cache_get

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(tzinfo=None)

    # Per-source stats
    rows = (await db.execute(
        select(
            QueueItem.source,
            func.count(QueueItem.id).label("total"),
            func.count(QueueItem.played_at).label("played"),
        )
        .where(
            QueueItem.user_id == user_id,
            QueueItem.created_at >= cutoff,
        )
        .group_by(QueueItem.source)
    )).all()

    source_stats = []
    for source, total, played in rows:
        play_rate = round(played / total, 3) if total > 0 else 0
        source_stats.append({
            "source": source,
            "recommended": total,
            "played": played,
            "play_rate": play_rate,
        })

    # Weekly breakdown for chart
    weekly_rows = (await db.execute(
        select(
            func.date_trunc("week", QueueItem.created_at).label("week"),
            QueueItem.source,
            func.count(QueueItem.id).label("total"),
            func.count(QueueItem.played_at).label("played"),
        )
        .where(
            QueueItem.user_id == user_id,
            QueueItem.created_at >= cutoff,
        )
        .group_by("week", QueueItem.source)
        .order_by("week")
    )).all()

    weekly_data = []
    for week, source, total, played in weekly_rows:
        weekly_data.append({
            "week": week.strftime("%Y-%m-%d") if week else None,
            "source": source,
            "recommended": total,
            "played": played,
        })

    # Current weights
    weights = {}
    try:
        raw = await cache_get(f"queue_weights:{user_id}")
        if raw:
            weights = raw if isinstance(raw, dict) else {}
    except Exception:
        pass

    # Recent items (last 30)
    recent_items = (await db.execute(
        select(QueueItem)
        .where(QueueItem.user_id == user_id)
        .order_by(QueueItem.created_at.desc())
        .limit(30)
    )).scalars().all()

    recent = [{
        "title": item.title,
        "source": item.source,
        "score": round(item.score, 2) if item.score else 0,
        "played": item.played,
        "played_at": item.played_at.isoformat() if item.played_at else None,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "in_library": item.in_library,
    } for item in recent_items]

    return {
        "source_stats": source_stats,
        "weekly": weekly_data,
        "weights": weights,
        "recent_items": recent,
        "period_days": days,
    }



# ═══════════════════════════════════════════════════════════════════════════
# Arr Library Check — items already in Radarr / Sonarr
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/arr-library")
async def get_arr_library():
    """Return sets of TMDB/TVDB IDs for items already in Radarr/Sonarr.

    Used by the frontend to show 'In Radarr' / 'In Sonarr' instead of
    the send button, and by the watchlist sync job to find missing items.
    Cached 60s to avoid hammering the *arr APIs.
    """
    import json as _json
    from app.utils.redis_cache import cache_get, cache_set

    cache_key = "arr_library_ids_v2"
    try:
        cached = await cache_get(cache_key)
        if cached:
            data = _json.loads(cached) if isinstance(cached, str) else cached
            return data
    except Exception:
        pass

    radarr_tmdb: list[int] = []
    sonarr_tvdb: list[int] = []
    radarr_server_names: dict[int, str] = {}
    sonarr_server_names: dict[int, str] = {}
    radarr_missing_tmdb: list[int] = []
    sonarr_missing_tvdb: list[int] = []

    r = await get_redis()

    # --- Radarr ---
    raw_radarr = await r.get("radarr_servers")
    if raw_radarr:
        from app.utils.radarr_client import RadarrClient
        for srv in _json.loads(raw_radarr):
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                movies = await client.get_all_movies()
                await client.close()
                for m in movies:
                    tmdb = m.get("tmdbId")
                    if tmdb:
                        radarr_tmdb.append(tmdb)
                        radarr_server_names[tmdb] = srv.get("name", "Radarr")
                        if m.get("monitored") and not m.get("hasFile"):
                            radarr_missing_tmdb.append(tmdb)
            except Exception as e:
                log.warning("arr_library.radarr_failed", server=srv.get("name"), error=str(e)[:120])

    # --- Sonarr ---
    raw_sonarr = await r.get("sonarr_servers")
    if raw_sonarr:
        from app.utils.sonarr_client import SonarrClient
        for srv in _json.loads(raw_sonarr):
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                series = await client.get_all_series()
                await client.close()
                for s in series:
                    tvdb = s.get("tvdbId")
                    if tvdb:
                        sonarr_tvdb.append(tvdb)
                        sonarr_server_names[tvdb] = srv.get("name", "Sonarr")
                        if s.get("monitored"):
                            stats = s.get("statistics") or {}
                            total = stats.get("episodeCount", 0)
                            on_disk = stats.get("episodeFileCount", 0)
                            if total > 0 and on_disk < total:
                                sonarr_missing_tvdb.append(tvdb)
            except Exception as e:
                log.warning("arr_library.sonarr_failed", server=srv.get("name"), error=str(e)[:120])

    # Deduplicate missing IDs (dual-server setups)
    radarr_missing_tmdb = list(set(radarr_missing_tmdb))
    sonarr_missing_tvdb = list(set(sonarr_missing_tvdb))

    result = {
        "radarr_tmdb": radarr_tmdb,
        "sonarr_tvdb": sonarr_tvdb,
        "radarr_names": radarr_server_names,
        "sonarr_names": sonarr_server_names,
        "radarr_missing_tmdb": radarr_missing_tmdb,
        "sonarr_missing_tvdb": sonarr_missing_tvdb,
    }

    # Cache for 60 seconds
    try:
        await cache_set(cache_key, _json.dumps(result), ttl=60)
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Remote Play — browser extension integration
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/remote-play/libraries")
async def remote_play_libraries(db: AsyncSession = Depends(get_db)):
    """Return Emby library folders for the extension options page.

    Lists movies/tvshows libraries so the user can set priority order.
    """
    emby = EmbyClient()
    try:
        folders = await emby.get_virtual_folders()
        media_folders = [
            f for f in folders
            if f.get("collection_type") in ("movies", "tvshows", "mixed", "")
        ]
        return {"libraries": media_folders}
    except Exception as e:
        log.warning("remote_play.libraries_failed", error=str(e)[:200])
        raise HTTPException(502, "failed to fetch Emby libraries")
    finally:
        await emby.close()


@router.get("/api/remote-play/sessions/{user_id}")
async def remote_play_sessions(user_id: int, db: AsyncSession = Depends(get_db)):
    """Return active controllable Emby sessions for a user."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found or no Emby account linked")

    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
        user_sessions = []
        for s in all_sessions:
            if s.get("UserId") != user.emby_user_id:
                continue
            if not s.get("SupportsRemoteControl", False):
                continue
            user_sessions.append({
                "session_id": s.get("Id"),
                "device_name": s.get("DeviceName", "Unknown"),
                "client": s.get("Client", ""),
                "now_playing": s.get("NowPlayingItem", {}).get("Name"),
            })
        return {"sessions": user_sessions}
    except Exception as e:
        log.warning("remote_play.sessions_failed", error=str(e)[:200])
        raise HTTPException(502, "failed to fetch Emby sessions")
    finally:
        await emby.close()


@router.post("/api/remote-play")
async def remote_play(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Resolve a media item from provider IDs and start playback on Emby.

    Called by the browser extension.  Accepts IDs from Trakt, IMDB, or TMDB,
    resolves to an Emby library item, finds an active session for the user,
    and sends a play command.
    """
    body = await request.json()
    user_id = body.get("user_id")
    media_type = body.get("media_type", "movie")
    ids = body.get("ids", {})
    season = body.get("season")
    episode = body.get("episode")
    session_id = body.get("session_id")
    library_priority = body.get("library_priority", [])

    if not user_id:
        raise HTTPException(400, "user_id required")
    if not ids:
        raise HTTPException(400, "at least one ID required (imdb_id, tmdb_id, trakt_slug)")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found or no Emby account linked")

    # ── Step 1: Resolve to Emby library item ──

    matches = []

    for provider_type, id_key in [
        ("Imdb", "imdb_id"),
        ("Tmdb", "tmdb_id"),
        ("Tvdb", "tvdb_id"),
    ]:
        pid = ids.get(id_key)
        if pid:
            cached = await LibraryCache.find_by_provider_id(provider_type, str(pid))
            if cached:
                matches.append(cached)

    # Trakt slug → resolve via Trakt API to get provider IDs
    if not matches and ids.get("trakt_slug") and user.trakt_access_token:
        try:
            trakt = TraktClient(access_token=user.trakt_access_token)
            kind = "movie" if media_type == "movie" else "show"
            results = await trakt.search(query=ids["trakt_slug"], kind=kind)
            await trakt.close()
            if results:
                item_data = results[0].get(kind, {})
                trakt_ids = item_data.get("ids", {})
                for ptype, tkey in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
                    pid = trakt_ids.get(tkey)
                    if pid:
                        cached = await LibraryCache.find_by_provider_id(ptype, str(pid))
                        if cached:
                            matches.append(cached)
                            break
        except Exception:
            log.debug("remote_play.trakt_resolve_failed", slug=ids.get("trakt_slug"))

    # Title fallback
    if not matches and ids.get("title"):
        cached = await LibraryCache.find_by_title(
            ids["title"], year=ids.get("year"),
        )
        if cached:
            matches.append(cached)

    if not matches:
        return {"status": "not_in_library", "message": "Item not found in Emby library"}

    # Dedupe and pick best match based on library priority
    seen_ids: set[str] = set()
    unique_matches = []
    for m in matches:
        if m["emby_id"] not in seen_ids:
            seen_ids.add(m["emby_id"])
            unique_matches.append(m)

    if len(unique_matches) > 1 and library_priority:
        emby = EmbyClient()
        try:
            for m in unique_matches:
                item_detail = await emby.get_item(m["emby_id"], user_id=user.emby_user_id)
                m["_parent_id"] = item_detail.get("ParentId", "")
        except Exception:
            pass
        finally:
            await emby.close()

        def priority_key(m):
            pid = m.get("_parent_id", "")
            try:
                return library_priority.index(pid)
            except ValueError:
                return 999

        unique_matches.sort(key=priority_key)

    emby_item = unique_matches[0]

    # ── Step 2: For shows, resolve to specific episode ──

    play_item_id = emby_item["emby_id"]

    if media_type == "show" and season is not None and episode is not None:
        emby = EmbyClient()
        try:
            episodes = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Episode",
                parent_id=emby_item["emby_id"],
                fields="ProviderIds,ProductionYear",
                sort_by="ParentIndexNumber,IndexNumber",
            )
            for ep in episodes.get("Items", []):
                if (ep.get("ParentIndexNumber") == season
                        and ep.get("IndexNumber") == episode):
                    play_item_id = ep["Id"]
                    break
            else:
                return {
                    "status": "episode_not_found",
                    "message": f"S{season:02d}E{episode:02d} not found in library",
                    "series_found": emby_item["title"],
                }
        except Exception as e:
            log.warning("remote_play.episode_resolve_failed", error=str(e)[:200])
            return {"status": "error", "message": "Failed to resolve episode"}
        finally:
            await emby.close()
    elif media_type == "show" and season is None:
        # No specific episode — play next unwatched
        emby = EmbyClient()
        try:
            next_up = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Episode",
                parent_id=emby_item["emby_id"],
                filters="IsUnplayed",
                sort_by="ParentIndexNumber,IndexNumber",
                limit=1,
            )
            next_items = next_up.get("Items", [])
            if next_items:
                play_item_id = next_items[0]["Id"]
        except Exception:
            pass
        finally:
            await emby.close()

    # ── Step 3: Find active session ──

    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
    except Exception as e:
        await emby.close()
        log.warning("remote_play.sessions_failed", error=str(e)[:200])
        return {"status": "error", "message": "Failed to connect to Emby"}

    user_sessions = [
        s for s in all_sessions
        if s.get("UserId") == user.emby_user_id
        and s.get("SupportsRemoteControl", False)
    ]

    if not user_sessions:
        await emby.close()
        return {
            "status": "no_active_session",
            "message": "No controllable Emby session found — open Emby on a device first",
        }

    target_session = None
    if session_id:
        target_session = next((s for s in user_sessions if s.get("Id") == session_id), None)
        if not target_session:
            await emby.close()
            return {"status": "session_not_found", "message": "Requested session no longer active"}
    elif len(user_sessions) == 1:
        target_session = user_sessions[0]
    else:
        playing = [s for s in user_sessions if s.get("NowPlayingItem")]
        if len(playing) == 1:
            target_session = playing[0]
        else:
            await emby.close()
            return {
                "status": "multiple_sessions",
                "message": "Multiple Emby sessions found — pick one",
                "sessions": [
                    {
                        "session_id": s.get("Id"),
                        "device_name": s.get("DeviceName", "Unknown"),
                        "client": s.get("Client", ""),
                        "now_playing": s.get("NowPlayingItem", {}).get("Name"),
                    }
                    for s in user_sessions
                ],
            }

    # ── Step 4: Send play command ──

    try:
        await emby.play_item_on_session(
            session_id=target_session["Id"],
            item_id=play_item_id,
            controlling_user_id=user.emby_user_id,
        )
        return {
            "status": "playing",
            "title": emby_item.get("title", ""),
            "emby_id": play_item_id,
            "device": target_session.get("DeviceName", ""),
        }
    except Exception as e:
        log.warning("remote_play.play_failed", error=str(e)[:200])
        return {"status": "error", "message": f"Play command failed: {str(e)[:100]}"}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════
# Recently Arrived — items that moved from monitored/downloading → available
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/recently-arrived")
async def get_recently_arrived():
    """Surface items that recently became available in the library.

    Compares current Radarr/Sonarr available items against a previously
    stored snapshot of pending items.  Items that were pending last check
    but are now available (have files) are returned as "recently arrived".
    The snapshot is updated each call so the next call shows only new arrivals.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    r = await get_redis()

    # Check short-lived result cache (5 min)
    result_cache_key = "recently_arrived_result_v1"
    try:
        cached = await r.get(result_cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    # Load previously-pending snapshot
    prev_key = "recently_arrived_pending_v1"
    try:
        raw_prev = await r.get(prev_key)
        prev_pending = _json.loads(raw_prev) if raw_prev else {"movies": [], "shows": []}
    except Exception:
        prev_pending = {"movies": [], "shows": []}

    prev_movie_ids = {str(m) for m in prev_pending.get("movies", [])}
    # Shows: track {tvdb_id: ep_file_count} to detect new episodes
    prev_show_eps = {}
    for s in prev_pending.get("shows", []):
        if isinstance(s, dict):
            prev_show_eps[str(s.get("id", ""))] = s.get("eps", 0)
        else:
            # Legacy format: just a tvdb_id string
            prev_show_eps[str(s)] = 0

    current_pending_movies: list[str] = []
    current_pending_shows: list[dict] = []
    arrived_movies: list[dict] = []
    arrived_shows: list[dict] = []

    # --- Radarr ---
    raw_radarr = await r.get("radarr_servers")
    if raw_radarr:
        for srv in _json.loads(raw_radarr):
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                all_movies = await client.get_all_movies()
                await client.close()
                for movie in all_movies:
                    if not movie.get("monitored", False):
                        continue
                    tmdb_id = str(movie.get("tmdbId", ""))
                    if not tmdb_id:
                        continue
                    has_file = movie.get("hasFile", False)
                    if has_file:
                        # Was it pending last time?
                        if tmdb_id in prev_movie_ids:
                            arrived_movies.append({
                                "title": movie.get("title", ""),
                                "year": movie.get("year"),
                                "tmdb_id": movie.get("tmdbId"),
                                "type": "movie",
                            })
                    else:
                        current_pending_movies.append(tmdb_id)
            except Exception as e:
                log.warning("recently_arrived.radarr_failed", error=str(e)[:120])

    # --- Sonarr ---
    raw_sonarr = await r.get("sonarr_servers")
    if raw_sonarr:
        for srv in _json.loads(raw_sonarr):
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                all_series = await client.get_all_series()
                await client.close()
                for series in all_series:
                    if not series.get("monitored", False):
                        continue
                    tvdb_id = str(series.get("tvdbId", ""))
                    if not tvdb_id:
                        continue
                    stats = series.get("statistics") or {}
                    ep_file_count = stats.get("episodeFileCount", 0)
                    ep_count = stats.get("episodeCount", 0)
                    if ep_count == 0:
                        continue
                    fully_available = ep_file_count >= ep_count

                    # Check if new episodes arrived since last snapshot
                    prev_eps = prev_show_eps.get(tvdb_id)
                    if prev_eps is not None and ep_file_count > prev_eps:
                        arrived_shows.append({
                            "title": series.get("title", ""),
                            "year": series.get("year"),
                            "tvdb_id": series.get("tvdbId"),
                            "type": "show",
                            "episodes_on_disk": ep_file_count,
                            "episodes_total": ep_count,
                            "new_episodes": ep_file_count - prev_eps,
                        })

                    # Track shows that still need episodes
                    if not fully_available:
                        current_pending_shows.append({"id": tvdb_id, "eps": ep_file_count})
            except Exception as e:
                log.warning("recently_arrived.sonarr_failed", error=str(e)[:120])

    # Save current pending as the new snapshot (30 day TTL)
    try:
        await r.setex(prev_key, 86400 * 30, _json.dumps({
            "movies": current_pending_movies,
            "shows": current_pending_shows,
        }))
    except Exception:
        pass

    # ── Merge with existing arrived items (for 24hr window) ──
    arrived_key = "recently_arrived_items_v1"
    now_ts = datetime.now(timezone.utc).isoformat() + "Z"
    try:
        raw_existing = await r.get(arrived_key)
        existing_items = _json.loads(raw_existing) if raw_existing else []
    except Exception:
        existing_items = []

    # Filter out items older than 24 hours
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat() + "Z"
    existing_items = [i for i in existing_items if i.get("arrived_at", "") > cutoff]

    # Add new arrivals with timestamp (dedup by id)
    existing_ids = {(i.get("type"), str(i.get("id", ""))) for i in existing_items}
    for m in arrived_movies:
        key = ("movie", str(m.get("tmdb_id", "")))
        if key not in existing_ids:
            existing_items.append({**m, "id": m.get("tmdb_id"), "arrived_at": now_ts})
    for s in arrived_shows:
        key = ("show", str(s.get("tvdb_id", "")))
        if key not in existing_ids:
            existing_items.append({**s, "id": s.get("tvdb_id"), "arrived_at": now_ts})

    # Persist with 48hr TTL (items self-expire at 24hr via filter above)
    try:
        await r.setex(arrived_key, 86400 * 2, _json.dumps(existing_items))
    except Exception:
        pass

    result = {
        "arrived_movies": [i for i in existing_items if i.get("type") == "movie"],
        "arrived_shows": [i for i in existing_items if i.get("type") == "show"],
        "total": len(existing_items),
    }

    # Cache result for 5 min
    try:
        await r.setex(result_cache_key, 300, _json.dumps(result))
    except Exception:
        pass

    return result


@router.post("/api/recently-arrived/dismiss")
async def dismiss_recently_arrived(_user: User = Depends(get_current_user)):
    """Clear the recently arrived list by resetting the pending snapshot."""
    r = await get_redis()
    await r.delete("recently_arrived_result_v1")
    await r.delete("recently_arrived_pending_v1")
    await r.delete("recently_arrived_items_v1")
    return {"status": "cleared"}


@router.post("/api/recently-arrived/dismiss-item")
async def dismiss_arrived_item(request: Request, _user: User = Depends(get_current_user)):
    """Remove a single item from the recently arrived list."""
    import json as _json
    body = await request.json()
    item_type = body.get("type")  # "movie" or "show"
    item_id = str(body.get("id", ""))

    r = await get_redis()
    arrived_key = "recently_arrived_items_v1"
    try:
        raw = await r.get(arrived_key)
        items = _json.loads(raw) if raw else []
        items = [i for i in items if not (i.get("type") == item_type and str(i.get("id", "")) == item_id)]
        await r.setex(arrived_key, 86400 * 2, _json.dumps(items))
        # Invalidate result cache
        await r.delete("recently_arrived_result_v1")
    except Exception:
        pass

    return {"status": "dismissed"}


@router.post("/api/play-on-session")
async def play_on_session(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Start playing an Emby item by its ID on a specific session.

    Unlike /api/remote-play (which resolves from provider IDs), this
    takes a direct emby_item_id and session_id for the continue watching
    play button.
    """
    body = await request.json()
    user_id = body.get("user_id")
    emby_item_id = body.get("emby_item_id")
    session_id = body.get("session_id")
    start_position_ticks = body.get("start_position_ticks", 0)

    if not all([user_id, emby_item_id, session_id]):
        raise HTTPException(400, "user_id, emby_item_id, and session_id required")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found")

    emby = EmbyClient()
    try:
        await emby.play_item_on_session(
            session_id=session_id,
            item_id=emby_item_id,
            start_position_ticks=start_position_ticks,
            controlling_user_id=user.emby_user_id,
        )
        return {"status": "playing", "emby_id": emby_item_id}
    except Exception as e:
        log.warning("play_on_session.failed", error=str(e)[:200])
        return {"status": "error", "message": f"Play failed: {str(e)[:100]}"}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════
# Watch Party Quick-Start — server-wide session enumeration
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/watch-party/server-sessions")
async def watch_party_server_sessions(db: AsyncSession = Depends(get_db)):
    """Return all active controllable Emby sessions grouped by server user.

    Used by the one-click Watch Party launcher on the Continue Watching
    panel.  Returns every user who has at least one remote-controllable
    device online, with their devices listed underneath.
    """
    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
    except Exception as e:
        log.warning("watch_party.server_sessions_failed", error=str(e)[:200])
        raise HTTPException(502, "Failed to fetch Emby sessions")
    finally:
        await emby.close()

    # Build a mapping of emby_user_id → DB user for display names + DB IDs
    db_users = (await db.execute(select(User))).scalars().all()
    by_emby_id = {u.emby_user_id: u for u in db_users}

    # Group sessions by UserId
    user_sessions: dict[str, dict] = {}  # emby_user_id → {info + devices}
    for s in all_sessions:
        uid = s.get("UserId")
        if not uid:
            continue
        if not s.get("SupportsRemoteControl", False):
            continue

        if uid not in user_sessions:
            db_user = by_emby_id.get(uid)
            user_sessions[uid] = {
                "emby_user_id": uid,
                "db_user_id": db_user.id if db_user else None,
                "username": s.get("UserName") or (db_user.emby_username if db_user else "Unknown"),
                "devices": [],
            }

        user_sessions[uid]["devices"].append({
            "session_id": s.get("Id"),
            "device_name": s.get("DeviceName", "Unknown"),
            "client": s.get("Client", ""),
            "now_playing": s.get("NowPlayingItem", {}).get("Name"),
        })

    return {"users": list(user_sessions.values())}


# ═══════════════════════════════════════════════════════════════════════════
# Trakt Playback Sync — compare Trakt resume points with Emby
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/playback-sync/{user_id}")
async def get_playback_sync(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Trakt in-progress playback items with Emby resume points.

    Returns items that exist on Trakt's playback list, enriched with
    Emby resume data if available.  Surfaces mismatches (Trakt has a
    resume point but Emby doesn't, or vice versa) and stale entries
    (paused > 30 days ago).
    """
    import json as _json

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.trakt_access_token:
        raise HTTPException(404, "User not found or no Trakt account linked")
    require_user_ownership(current_user.id, user_id, "playback_sync")

    # Cache for 10 min
    cache_key = f"playback_sync_v1:{user.id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    # Fetch Trakt playback progress
    async def _on_refresh(access, refresh, expires):
        async with async_session() as _db:
            u = await _db.get(User, user.id)
            u.trakt_access_token = access
            u.trakt_refresh_token = refresh
            u.trakt_token_expires = expires
            await _db.commit()

    trakt = TraktClient(
        access_token=user.trakt_access_token,
        refresh_token=user.trakt_refresh_token,
        token_expires=user.trakt_token_expires,
        token_refresh_callback=_on_refresh,
    )

    try:
        trakt_playback = await trakt.get_playback()
    except Exception as e:
        log.warning("playback_sync.trakt_fetch_failed", error=str(e)[:120])
        raise HTTPException(502, f"Failed to fetch Trakt playback: {str(e)[:100]}")

    if not trakt_playback:
        result = {"items": [], "total": 0}
        try:
            r = await get_redis()
            await r.setex(cache_key, 600, _json.dumps(result))
        except Exception:
            pass
        return result

    # Fetch Emby resumable items for cross-reference
    emby = EmbyClient()
    emby_resume: dict[str, dict] = {}  # provider_id → emby data
    try:
        resp = await emby.get_items(
            user_id=user.emby_user_id,
            fields="ProviderIds,UserData,RunTimeTicks",
            filters="IsResumable",
            limit=500,
        )
        for item in resp.get("Items", []):
            pids = item.get("ProviderIds", {})
            ud = item.get("UserData", {})
            runtime = item.get("RunTimeTicks", 0) or 0
            pos = ud.get("PlaybackPositionTicks", 0) or 0
            progress = round(pos / runtime * 100, 1) if runtime > 0 else 0

            entry = {
                "emby_id": item.get("Id"),
                "emby_progress": progress,
                "emby_title": item.get("Name", ""),
            }
            for key in ("Imdb", "Tmdb", "Tvdb"):
                if pids.get(key):
                    emby_resume[f"{key.lower()}:{pids[key]}"] = entry
    except Exception as e:
        log.warning("playback_sync.emby_fetch_failed", error=str(e)[:120])
    finally:
        await emby.close()

    # Build comparison items
    items: list[dict] = []
    now = datetime.now(timezone.utc)

    for pb in trakt_playback:
        pb_id = pb.get("id")
        pb_type = pb.get("type", "")
        progress = pb.get("progress", 0)
        paused_at = pb.get("paused_at", "")

        # Extract title and IDs
        media = pb.get(pb_type, {})
        title = media.get("title", "")
        ids = media.get("ids", {})

        # For episodes, include show + episode info
        ep_label = ""
        if pb_type == "episode":
            show = pb.get("show", {})
            title = show.get("title", title)
            ep_title = media.get("title", "")
            season = media.get("season", 0)
            number = media.get("number", 0)
            ep_label = f"S{season:02d}E{number:02d}" + (f" — {ep_title}" if ep_title else "")
            # Use show IDs for matching
            ids = show.get("ids", ids)

        # Calculate days since paused
        days_stale = None
        if paused_at:
            try:
                pa_dt = datetime.fromisoformat(paused_at.replace("Z", "+00:00"))
                days_stale = (now.astimezone() - pa_dt).days if pa_dt.tzinfo else (now - pa_dt.replace(tzinfo=None)).days
            except Exception:
                pass

        # Try to match with Emby resume
        emby_match = None
        for id_type in ("imdb", "tmdb", "tvdb"):
            id_val = ids.get(id_type)
            if id_val:
                key = f"{id_type}:{id_val}"
                if key in emby_resume:
                    emby_match = emby_resume[key]
                    break

        item_entry = {
            "trakt_playback_id": pb_id,
            "type": pb_type,
            "title": title,
            "episode": ep_label,
            "trakt_progress": round(progress, 1),
            "paused_at": paused_at,
            "days_stale": days_stale,
            "trakt_ids": {k: v for k, v in ids.items() if v},
        }

        if emby_match:
            item_entry["emby_id"] = emby_match["emby_id"]
            item_entry["emby_progress"] = emby_match["emby_progress"]
            diff = abs(progress - emby_match["emby_progress"])
            item_entry["progress_diff"] = round(diff, 1)
            item_entry["synced"] = diff < 5  # within 5% = synced
        else:
            item_entry["emby_id"] = None
            item_entry["emby_progress"] = None
            item_entry["progress_diff"] = None
            item_entry["synced"] = False

        items.append(item_entry)

    # Sort: unsynced first, then by staleness
    items.sort(key=lambda x: (x["synced"], -(x["days_stale"] or 0)))

    result = {"items": items, "total": len(items)}

    try:
        r = await get_redis()
        await r.setex(cache_key, 600, _json.dumps(result))
    except Exception:
        pass

    return result


@router.delete("/api/playback-sync/{user_id}/{playback_id}")
async def delete_trakt_playback(
    user_id: int,
    playback_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a stale playback entry from Trakt."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.trakt_access_token:
        raise HTTPException(404, "User not found or no Trakt account linked")
    require_user_ownership(current_user.id, user_id, "playback_sync")

    async def _on_refresh(access, refresh, expires):
        async with async_session() as _db:
            u = await _db.get(User, user.id)
            u.trakt_access_token = access
            u.trakt_refresh_token = refresh
            u.trakt_token_expires = expires
            await _db.commit()

    trakt = TraktClient(
        access_token=user.trakt_access_token,
        refresh_token=user.trakt_refresh_token,
        token_expires=user.trakt_token_expires,
        token_refresh_callback=_on_refresh,
    )

    await trakt.delete_playback(playback_id)

    # Invalidate cache
    try:
        r = await get_redis()
        await r.delete(f"playback_sync_v1:{user.id}")
    except Exception:
        pass

    return {"status": "deleted", "playback_id": playback_id}


# ═══════════════════════════════════════════════════════════════════════════
# Library Health Monitor
# ═══════════════════════════════════════════════════════════════════════════

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
    item_id = str(body.get("id", ""))  # imdb/tmdb/tvdb/trakt ID
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
            "value_length": len(r.value) if r.value else 0,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        })
    result.sort(key=lambda x: x["key"])
    return {"count": len(result), "rows": result}


# ═══════════════════════════════════════════════════════════════════════════
# Trakt ↔ MDBList Cross-Sync
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/mdblist/sync-status")
async def mdblist_sync_status(db: AsyncSession = Depends(get_db)):
    """Compare Trakt watched history against MDBList to show what's missing.
    Returns counts and sample items for movies and shows."""
    import json as _json

    providers = await _get_active_providers(db)
    if "mdblist" not in providers:
        raise HTTPException(400, "MDBList is not an active provider")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    # Get the first linked user
    user = (await db.execute(
        select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
    )).scalars().first()
    if not user:
        raise HTTPException(400, "No linked Trakt user found")

    from app.utils.mdblist_client import MDBListClient

    trakt = TraktClient(
        access_token=user.trakt_access_token,
        refresh_token=user.trakt_refresh_token,
        token_expires=user.trakt_token_expires,
    )
    mdb = MDBListClient(api_key=key)

    try:
        # Fetch Trakt watched movies
        trakt_movies = await trakt.get_watched(kind="movies")
        # Fetch MDBList watched
        mdb_watched = await mdb.get_watched()

        # Build MDBList watched ID sets
        mdb_movie_ids: set[str] = set()
        for entry in mdb_watched.get("movies", []):
            ids = entry.get("movie", {}).get("ids", {})
            for k in ("imdb", "tmdb", "tvdb", "trakt"):
                v = ids.get(k)
                if v:
                    mdb_movie_ids.add(f"{k}:{v}")

        mdb_show_keys: set[str] = set()
        for entry in mdb_watched.get("shows", []):
            ids = entry.get("show", {}).get("ids", {})
            for k in ("imdb", "tmdb", "tvdb", "trakt"):
                v = ids.get(k)
                if v:
                    mdb_show_keys.add(f"{k}:{v}")

        # Find Trakt movies not in MDBList
        missing_movies = []
        for entry in trakt_movies:
            movie = entry.get("movie", {})
            ids = movie.get("ids", {})
            item_keys = set()
            for k in ("imdb", "tmdb", "tvdb", "trakt"):
                v = ids.get(k)
                if v:
                    item_keys.add(f"{k}:{v}")
            if not item_keys & mdb_movie_ids:
                missing_movies.append({
                    "title": movie.get("title", ""),
                    "year": movie.get("year"),
                    "ids": ids,
                    "last_watched_at": entry.get("last_watched_at"),
                })

        # Find Trakt shows not in MDBList (show-level only)
        trakt_shows = await trakt.get_watched(kind="shows")
        missing_shows = []
        for entry in trakt_shows:
            show = entry.get("show", {})
            ids = show.get("ids", {})
            item_keys = set()
            for k in ("imdb", "tmdb", "tvdb", "trakt"):
                v = ids.get(k)
                if v:
                    item_keys.add(f"{k}:{v}")
            if not item_keys & mdb_show_keys:
                missing_shows.append({
                    "title": show.get("title", ""),
                    "year": show.get("year"),
                    "ids": ids,
                    "last_watched_at": entry.get("last_watched_at"),
                })

        return {
            "trakt_movies": len(trakt_movies),
            "trakt_shows": len(trakt_shows),
            "mdblist_movies": len(mdb_watched.get("movies", [])),
            "mdblist_shows": len(mdb_watched.get("shows", [])),
            "missing_movies": len(missing_movies),
            "missing_shows": len(missing_shows),
            "sample_movies": missing_movies[:20],
            "sample_shows": missing_shows[:20],
        }
    finally:
        await trakt.close()
        await mdb.close()


@router.post("/api/mdblist/sync-from-trakt")
async def sync_trakt_to_mdblist(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Incremental sync of Trakt watched history into MDBList.

    On first run, pushes everything. On subsequent runs, only pushes items
    watched after the last successful sync timestamp (stored in Redis).
    Pass {"full": true} in the body to force a full re-sync.
    """
    import json as _json

    providers = await _get_active_providers(db)
    if "mdblist" not in providers:
        raise HTTPException(400, "MDBList is not an active provider")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    user = (await db.execute(
        select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
    )).scalars().first()
    if not user:
        raise HTTPException(400, "No linked Trakt user found")

    # Check for force-full flag
    force_full = False
    try:
        body = await request.json()
        force_full = body.get("full", False)
    except Exception:
        pass

    # Load last sync timestamp from Redis
    r = await get_redis()
    last_sync_ts = None
    if not force_full:
        raw = await r.get("mdblist_sync_last_completed")
        if raw:
            last_sync_ts = raw if isinstance(raw, str) else raw.decode()

    from app.utils.mdblist_client import MDBListClient

    trakt = TraktClient(
        access_token=user.trakt_access_token,
        refresh_token=user.trakt_refresh_token,
        token_expires=user.trakt_token_expires,
    )
    mdb = MDBListClient(api_key=key)

    sync_started_at = datetime.now(timezone.utc).isoformat()

    try:
        # Fetch full Trakt watched history
        trakt_movies = await trakt.get_watched(kind="movies")
        trakt_shows = await trakt.get_watched(kind="shows")

        # Build MDBList movie payloads — filter by last_watched_at if delta sync
        mdb_movies = []
        skipped_movies = 0
        for entry in trakt_movies:
            watched_at = entry.get("last_watched_at", "")
            if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                skipped_movies += 1
                continue
            movie = entry.get("movie", {})
            ids = movie.get("ids", {})
            mdb_ids = {}
            for k in ("imdb", "tmdb", "tvdb", "trakt"):
                if ids.get(k):
                    mdb_ids[k] = ids[k]
            if mdb_ids:
                mdb_movies.append({
                    "ids": mdb_ids,
                    "watched_at": watched_at or datetime.now(timezone.utc).isoformat(),
                })

        mdb_shows = []
        # Paginate through /users/me/history/episodes for episode-level data
        from collections import defaultdict
        show_eps: dict[str, dict] = {}

        ep_page = 1
        ep_per_page = 500
        ep_max_pages = 50
        total_eps_fetched = 0
        skipped_eps = 0

        while ep_page <= ep_max_pages:
            await asyncio.sleep(0)
            try:
                params: dict = {"page": ep_page, "limit": ep_per_page}
                # For delta sync, use start_at filter if Trakt supports it
                # Otherwise filter client-side
                resp = await trakt._client.get(
                    "/users/me/history/episodes",
                    headers=trakt._auth_headers(),
                    params=params,
                )
                trakt._update_rate_limit(resp)
                if resp.status_code == 429:
                    await asyncio.sleep(min(30, float(resp.headers.get("Retry-After", "10"))))
                    continue
                if resp.status_code != 200:
                    log.warning("mdblist_sync.history_page_failed",
                                page=ep_page, status=resp.status_code)
                    break
                entries = resp.json()
                if not entries:
                    break

                # For delta sync: episodes are returned newest-first.
                # Once we hit an entry older than last_sync_ts, we can stop.
                page_all_old = True

                for entry in entries:
                    watched_at = entry.get("watched_at", "")
                    if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                        skipped_eps += 1
                        continue

                    page_all_old = False
                    show = entry.get("show", {})
                    show_ids = show.get("ids", {})
                    ep = entry.get("episode", {})
                    s_num = ep.get("season", 0)
                    e_num = ep.get("number", 0)

                    show_key = str(show_ids.get("trakt", "")) or str(show_ids.get("imdb", ""))
                    if not show_key:
                        continue

                    if show_key not in show_eps:
                        mdb_ids = {}
                        for k in ("imdb", "tmdb", "tvdb", "trakt"):
                            if show_ids.get(k):
                                mdb_ids[k] = show_ids[k]
                        show_eps[show_key] = {"ids": mdb_ids, "seasons": defaultdict(list)}

                    show_eps[show_key]["seasons"][s_num].append({
                        "number": e_num,
                        "watched_at": watched_at,
                    })
                    total_eps_fetched += 1

                # If delta sync and entire page was old, stop paginating
                if last_sync_ts and page_all_old:
                    break

                total_pages = int(resp.headers.get("X-Pagination-Page-Count", "1"))
                if ep_page >= total_pages:
                    break
                ep_page += 1
            except Exception as e:
                log.warning("mdblist_sync.history_page_error",
                            page=ep_page, error=str(e)[:120])
                break

        log.info("mdblist_sync.episodes_fetched",
                 shows=len(show_eps), episodes=total_eps_fetched,
                 skipped_eps=skipped_eps, skipped_movies=skipped_movies,
                 pages=ep_page, mode="delta" if last_sync_ts else "full")

        # Convert grouped data to MDBList payload
        for show_key, show_data in show_eps.items():
            if not show_data["ids"]:
                continue
            mdb_seasons = []
            for s_num, eps in sorted(show_data["seasons"].items()):
                seen = set()
                deduped = []
                for ep in eps:
                    if ep["number"] not in seen:
                        seen.add(ep["number"])
                        deduped.append(ep)
                mdb_seasons.append({"number": s_num, "episodes": deduped})
            if mdb_seasons:
                mdb_shows.append({"ids": show_data["ids"], "seasons": mdb_seasons})

        # Send to MDBList in batches
        results = {"movies": 0, "shows": 0, "episodes": 0}
        batch_size = 100

        for i in range(0, len(mdb_movies), batch_size):
            batch = mdb_movies[i:i + batch_size]
            try:
                resp_data = await mdb.add_to_watched(movies=batch)
                results["movies"] += resp_data.get("updated", {}).get("movies", 0)
            except Exception as e:
                log.warning("mdblist_sync.movie_batch_failed", batch=i, error=str(e)[:120])

        for i in range(0, len(mdb_shows), batch_size):
            batch = mdb_shows[i:i + batch_size]
            try:
                resp_data = await mdb.add_to_watched(shows=batch)
                results["shows"] += resp_data.get("updated", {}).get("seasons", 0)
                results["episodes"] += resp_data.get("updated", {}).get("episodes", 0)
            except Exception as e:
                log.warning("mdblist_sync.show_batch_failed", batch=i, error=str(e)[:120])

        # Also sync ratings (only new ones since last sync)
        ratings_result = {"movies": 0, "episodes": 0}
        try:
            trakt_ratings = await trakt.get_user_ratings(kind="movies")
            mdb_rate_movies = []
            for entry in trakt_ratings:
                rated_at = entry.get("rated_at", "")
                if last_sync_ts and rated_at and rated_at <= last_sync_ts:
                    continue
                movie = entry.get("movie", {})
                ids = movie.get("ids", {})
                mdb_ids = {}
                for k in ("imdb", "tmdb", "tvdb", "trakt"):
                    if ids.get(k):
                        mdb_ids[k] = ids[k]
                if mdb_ids and entry.get("rating"):
                    mdb_rate_movies.append({
                        "ids": mdb_ids,
                        "rating": entry["rating"],
                        "rated_at": rated_at or datetime.now(timezone.utc).isoformat(),
                    })
            if mdb_rate_movies:
                for i in range(0, len(mdb_rate_movies), batch_size):
                    batch = mdb_rate_movies[i:i + batch_size]
                    try:
                        resp_data = await mdb.add_ratings(movies=batch)
                        ratings_result["movies"] += resp_data.get("updated", {}).get("movies", 0)
                    except Exception as e:
                        log.warning("mdblist_sync.rating_batch_failed", batch=i, error=str(e)[:120])
        except Exception as e:
            log.warning("mdblist_sync.ratings_failed", error=str(e)[:120])

        # Store sync timestamp on success
        await r.set("mdblist_sync_last_completed", sync_started_at)

        log.info("mdblist_sync.complete",
                 movies_synced=results["movies"],
                 shows_synced=results["shows"],
                 episodes_synced=results["episodes"],
                 ratings_movies=ratings_result["movies"],
                 mode="delta" if last_sync_ts else "full",
                 skipped_movies=skipped_movies,
                 skipped_eps=skipped_eps)

        return {
            "status": "ok",
            "mode": "delta" if last_sync_ts else "full",
            "watched": results,
            "ratings": ratings_result,
            "totals": {
                "trakt_movies": len(trakt_movies),
                "trakt_shows": len(trakt_shows),
                "pushed_movies": len(mdb_movies),
                "pushed_shows": len(mdb_shows),
                "skipped_movies": skipped_movies,
                "skipped_episodes": skipped_eps,
            },
        }
    finally:
        await trakt.close()
        await mdb.close()


# ═══════════════════════════════════════════════════════════════════════════
# Direct Emby Playback (Theater Mode solo play)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/emby/play")
async def emby_direct_play(payload: dict, _user: User = Depends(get_current_user)):
    """Start playback of an Emby item on a specific session.

    Payload: {"session_id": "abc", "item_id": "123", "start_position_ticks": 0}
    Used by the solo Pick Together flow to play directly on a device.
    """
    session_id = payload.get("session_id")
    item_id = payload.get("item_id")
    start_ticks = int(payload.get("start_position_ticks", 0))

    if not session_id or not item_id:
        raise HTTPException(400, "session_id and item_id required")

    emby = EmbyClient()
    try:
        await emby.play_item_on_session(session_id, item_id, start_position_ticks=start_ticks)
        return {"status": "ok"}
    except Exception as e:
        log.warning("emby_direct_play.failed", session_id=session_id, error=str(e)[:200])
        raise HTTPException(502, f"Playback failed: {str(e)[:100]}")
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════
# Rewatch Recommender
# ═══════════════════════════════════════════════════════════════════════════

from app.services.rewatch.service import RewatchRecommender

_rewatch_svc = RewatchRecommender()


@router.get("/api/rewatch/{user_id}")
async def get_rewatch_suggestions(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return cached rewatch suggestions for a user."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    items = await _rewatch_svc.get_suggestions(user_id)
    return {"items": items, "count": len(items)}


@router.post("/api/rewatch/{user_id}/refresh")
async def refresh_rewatch(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Force rebuild rewatch suggestions."""
    require_user_ownership(_user.id, user_id, "rewatch_refresh")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    items = await _rewatch_svc.build_suggestions(user_id)
    return {"items": items, "count": len(items), "status": "rebuilt"}


@router.post("/api/rewatch/{user_id}/dismiss/{item_key:path}")
async def dismiss_rewatch_item(
    user_id: int,
    item_key: str,
    _user: User = Depends(get_current_user),
):
    """Dismiss a rewatch suggestion permanently."""
    require_user_ownership(_user.id, user_id, "rewatch_dismiss")
    return await _rewatch_svc.dismiss(user_id, item_key)


@router.get("/api/rewatch/{user_id}/history/{item_key:path}")
async def get_rewatch_item_history(
    user_id: int,
    item_key: str,
    db: AsyncSession = Depends(get_db),
):
    """Lazy-load watch history for hover flyout."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _rewatch_svc.get_item_history(user_id, item_key)


@router.get("/api/rewatch/{user_id}/settings")
async def get_rewatch_settings(user_id: int):
    """Read rewatch recommender settings for a user."""
    import json as _json
    r = await get_redis()
    raw = await r.get(f"rewatch:settings:{user_id}")
    if raw:
        return _json.loads(raw)
    return {"min_rating": 8, "min_months": 12, "seasonal": True}


@router.put("/api/rewatch/{user_id}/settings")
async def update_rewatch_settings(
    user_id: int,
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Save rewatch recommender settings."""
    import json as _json
    require_user_ownership(_user.id, user_id, "rewatch_settings")
    r = await get_redis()
    settings_data = {
        "min_rating": int(payload.get("min_rating", 8)),
        "min_months": int(payload.get("min_months", 12)),
        "seasonal": bool(payload.get("seasonal", True)),
    }
    await r.set(f"rewatch:settings:{user_id}", _json.dumps(settings_data))
    # Also persist to DB for durability
    from app.models.schema import AppSetting
    async with async_session_ctx() as db:
        existing = (await db.execute(
            select(AppSetting).where(AppSetting.key == f"rewatch_settings:{user_id}")
        )).scalar_one_or_none()
        if existing:
            existing.value = _json.dumps(settings_data)
        else:
            db.add(AppSetting(key=f"rewatch_settings:{user_id}", value=_json.dumps(settings_data)))
        await db.commit()
    return {"status": "ok", **settings_data}


# ═══════════════════════════════════════════════════════════════════════════
# Watch History — persistent local record
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/watch-history/{user_id}")
async def get_watch_history(
    user_id: int,
    limit: int = 50,
    offset: int = 0,
    item_type: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Return paginated watch history for a user."""
    from app.models.schema import WatchHistory
    q = select(WatchHistory).where(WatchHistory.user_id == user_id)
    if item_type:
        q = q.where(WatchHistory.item_type == item_type)
    q = q.order_by(WatchHistory.watched_at.desc()).offset(offset).limit(limit)

    count_q = select(func.count(WatchHistory.id)).where(WatchHistory.user_id == user_id)
    if item_type:
        count_q = count_q.where(WatchHistory.item_type == item_type)

    rows = (await db.execute(q)).scalars().all()
    total = (await db.execute(count_q)).scalar() or 0

    return {
        "items": [
            {
                "id": r.id,
                "emby_id": r.emby_id,
                "item_type": r.item_type,
                "title": r.title,
                "series_name": r.series_name,
                "season_number": r.season_number,
                "episode_number": r.episode_number,
                "imdb_id": r.imdb_id,
                "tmdb_id": r.tmdb_id,
                "watched_at": r.watched_at.isoformat() if r.watched_at else None,
                "runtime_minutes": r.runtime_minutes,
                "source": r.source,
            }
            for r in rows
        ],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/watch-history/{user_id}/item/{item_key:path}")
async def get_item_watch_history(
    user_id: int,
    item_key: str,
    db: AsyncSession = Depends(get_db),
):
    """Return all watch events for a specific item (rewatch flyout).

    item_key formats: 'emby:xxx', 'imdb:ttxxx', 'trakt:123'
    """
    from app.models.schema import WatchHistory
    from sqlalchemy import or_

    if ":" not in item_key:
        return {"watches": [], "play_count": 0}

    provider, value = item_key.split(":", 1)
    filters = [WatchHistory.user_id == user_id]

    if provider == "emby":
        filters.append(WatchHistory.emby_id == value)
    elif provider == "imdb":
        filters.append(WatchHistory.imdb_id == value)
    elif provider == "tmdb":
        filters.append(WatchHistory.tmdb_id == value)
    elif provider == "trakt":
        filters.append(WatchHistory.trakt_id == value)
    elif provider == "tvdb":
        filters.append(WatchHistory.tvdb_id == value)
    else:
        return {"watches": [], "play_count": 0}

    rows = (await db.execute(
        select(WatchHistory).where(*filters).order_by(WatchHistory.watched_at.desc())
    )).scalars().all()

    return {
        "watches": [
            {"date": r.watched_at.strftime("%Y-%m-%d %H:%M") if r.watched_at else "", "source": r.source}
            for r in rows
        ],
        "play_count": len(rows),
    }


@router.get("/api/watch-history/{user_id}/stats")
async def get_watch_history_stats(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Aggregated stats from local watch history — no API calls needed."""
    from app.models.schema import WatchHistory
    from sqlalchemy import extract, case, distinct

    base = WatchHistory.user_id == user_id

    # Total counts
    total_watches = (await db.execute(select(func.count(WatchHistory.id)).where(base))).scalar() or 0
    total_movies = (await db.execute(
        select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "movie")
    )).scalar() or 0
    total_episodes = (await db.execute(
        select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "episode")
    )).scalar() or 0

    # Total hours watched
    total_minutes = (await db.execute(
        select(func.coalesce(func.sum(WatchHistory.runtime_minutes), 0)).where(base)
    )).scalar() or 0
    total_hours = round(total_minutes / 60, 1)

    # Unique titles (movies) and series
    unique_movies = (await db.execute(
        select(func.count(distinct(WatchHistory.title))).where(base, WatchHistory.item_type == "movie")
    )).scalar() or 0
    unique_series = (await db.execute(
        select(func.count(distinct(WatchHistory.series_name))).where(
            base, WatchHistory.item_type == "episode", WatchHistory.series_name.isnot(None)
        )
    )).scalar() or 0

    # Most rewatched movies (top 10)
    most_rewatched_q = (
        select(
            WatchHistory.title,
            WatchHistory.emby_id,
            WatchHistory.imdb_id,
            func.count(WatchHistory.id).label("plays"),
            func.max(WatchHistory.watched_at).label("last_watched"),
        )
        .where(base, WatchHistory.item_type == "movie")
        .group_by(WatchHistory.title, WatchHistory.emby_id, WatchHistory.imdb_id)
        .having(func.count(WatchHistory.id) > 1)
        .order_by(func.count(WatchHistory.id).desc())
        .limit(10)
    )
    most_rewatched = [
        {"title": r.title, "emby_id": r.emby_id, "imdb_id": r.imdb_id,
         "plays": r.plays, "last_watched": r.last_watched.isoformat() if r.last_watched else None}
        for r in (await db.execute(most_rewatched_q)).all()
    ]

    # Most watched series (by episode count)
    most_watched_series_q = (
        select(
            WatchHistory.series_name,
            func.count(WatchHistory.id).label("episodes_watched"),
            func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("total_minutes"),
        )
        .where(base, WatchHistory.item_type == "episode", WatchHistory.series_name.isnot(None))
        .group_by(WatchHistory.series_name)
        .order_by(func.count(WatchHistory.id).desc())
        .limit(10)
    )
    most_watched_series = [
        {"series_name": r.series_name, "episodes_watched": r.episodes_watched,
         "total_hours": round(r.total_minutes / 60, 1)}
        for r in (await db.execute(most_watched_series_q)).all()
    ]

    # Watches per month (last 12 months)
    twelve_months_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365)
    monthly_q = (
        select(
            extract("year", WatchHistory.watched_at).label("year"),
            extract("month", WatchHistory.watched_at).label("month"),
            func.count(WatchHistory.id).label("count"),
            func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("minutes"),
        )
        .where(base, WatchHistory.watched_at >= twelve_months_ago)
        .group_by("year", "month")
        .order_by("year", "month")
    )
    monthly = [
        {"year": int(r.year), "month": int(r.month), "count": r.count,
         "hours": round(r.minutes / 60, 1)}
        for r in (await db.execute(monthly_q)).all()
    ]

    # Day-of-week distribution
    dow_q = (
        select(
            extract("dow", WatchHistory.watched_at).label("dow"),
            func.count(WatchHistory.id).label("count"),
        )
        .where(base)
        .group_by("dow")
        .order_by("dow")
    )
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    dow_raw = {int(r.dow): r.count for r in (await db.execute(dow_q)).all()}
    by_day_of_week = [{"day": day_names[i], "count": dow_raw.get(i, 0)} for i in range(7)]

    # Viewing streak (consecutive days)
    from sqlalchemy import text as sa_text_stats
    streak_q = sa_text_stats(
        "SELECT date_trunc('day', watched_at) AS d "
        "FROM watch_history WHERE user_id = :uid "
        "GROUP BY d ORDER BY d"
    )
    date_rows = (await db.execute(streak_q, {"uid": user_id})).scalars().all()
    current_streak = 0
    max_streak = 0
    if date_rows:
        dates_list = sorted(set(d.date() if hasattr(d, "date") else d for d in date_rows))
        if dates_list:
            streak = 1
            for i in range(1, len(dates_list)):
                if (dates_list[i] - dates_list[i-1]).days == 1:
                    streak += 1
                else:
                    max_streak = max(max_streak, streak)
                    streak = 1
            max_streak = max(max_streak, streak)

            # Current streak
            today = datetime.now(timezone.utc).date()
            if dates_list[-1] >= today - timedelta(days=1):
                current_streak = 1
                for i in range(len(dates_list) - 2, -1, -1):
                    if (dates_list[i+1] - dates_list[i]).days == 1:
                        current_streak += 1
                    else:
                        break

    return {
        "total_watches": total_watches,
        "total_movies": total_movies,
        "total_episodes": total_episodes,
        "total_hours": total_hours,
        "unique_movies": unique_movies,
        "unique_series": unique_series,
        "most_rewatched": most_rewatched,
        "most_watched_series": most_watched_series,
        "monthly": monthly,
        "by_day_of_week": by_day_of_week,
        "current_streak": current_streak,
        "max_streak": max_streak,
    }


@router.post("/api/watch-history/{user_id}/backfill")
async def backfill_watch_history(
    user_id: int,
    _user: User = Depends(get_current_user),
):
    """One-time import of watch history from Trakt, MDBList, and Emby.

    Runs in-request (not background) so the caller sees the result.
    Deduplicates via unique constraint — safe to run multiple times.
    """
    require_user_ownership(_user.id, user_id, "watch_history_backfill")

    from app.models.schema import WatchHistory
    from sqlalchemy import or_, and_
    import structlog
    log = structlog.get_logger()

    async with async_session_ctx() as db:
        user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(404, "User not found")

        # ── Clean up duplicates from prior buggy runs ─────────────────
        # NULL emby_id made the unique constraint ineffective.
        # Keep the oldest row per (user_id, item_type, title, watched_at).
        from sqlalchemy import text as sa_text
        cleanup_q = sa_text("""
            DELETE FROM watch_history
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM watch_history
                WHERE user_id = :uid
                GROUP BY user_id, item_type, COALESCE(title, ''), watched_at
            )
            AND user_id = :uid
        """)
        result = await db.execute(cleanup_q, {"uid": user_id})
        dupes_removed = result.rowcount
        if dupes_removed:
            await db.commit()
            log.info("backfill.duplicates_cleaned", user_id=user_id, removed=dupes_removed)

        added = {"trakt": 0, "mdblist": 0, "emby": 0}
        skipped = {"trakt": 0, "mdblist": 0, "emby": 0}

        async def _exists(watched_at_naive, title_val, item_type_val) -> bool:
            """Code-level dedup: check if a matching row already exists."""
            q = select(func.count(WatchHistory.id)).where(
                WatchHistory.user_id == user_id,
                WatchHistory.watched_at == watched_at_naive,
                WatchHistory.item_type == item_type_val,
                WatchHistory.title == (title_val or ""),
            )
            c = (await db.execute(q)).scalar() or 0
            return c > 0

        # ── 1. Trakt (richest — individual timestamps) ────────────────
        if user.trakt_access_token:
            try:
                from app.utils.trakt_client import TraktClient
                trakt = TraktClient(
                    access_token=user.trakt_access_token,
                    refresh_token=user.trakt_refresh_token,
                    token_expires=user.trakt_token_expires,
                )
                try:
                    for kind in ("movies", "episodes"):
                        page = 1
                        per_page = 500
                        while page <= 50:  # safety cap
                            history = await trakt.get_history(kind, limit=per_page, page=page)
                            if not history:
                                break
                            for entry in history:
                                watched_at = entry.get("watched_at", "")
                                if not watched_at:
                                    continue
                                try:
                                    dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                    dt_naive = dt.replace(tzinfo=None)
                                except (ValueError, TypeError):
                                    continue

                                if kind == "movies":
                                    item = entry.get("movie", {})
                                    ids = item.get("ids", {})
                                    runtime = item.get("runtime")
                                    wh = WatchHistory(
                                        user_id=user.id,
                                        item_type="movie",
                                        title=item.get("title", ""),
                                        imdb_id=ids.get("imdb") or None,
                                        tmdb_id=str(ids.get("tmdb")) if ids.get("tmdb") else None,
                                        trakt_id=str(ids.get("trakt")) if ids.get("trakt") else None,
                                        tvdb_id=None,
                                        watched_at=dt_naive,
                                        runtime_minutes=runtime,
                                        source="backfill_trakt",
                                    )
                                else:
                                    ep = entry.get("episode", {})
                                    show = entry.get("show", {})
                                    show_ids = show.get("ids", {})
                                    ep_ids = ep.get("ids", {})
                                    runtime = ep.get("runtime") or show.get("runtime")
                                    wh = WatchHistory(
                                        user_id=user.id,
                                        item_type="episode",
                                        title=ep.get("title", ""),
                                        series_name=show.get("title"),
                                        season_number=ep.get("season"),
                                        episode_number=ep.get("number"),
                                        imdb_id=show_ids.get("imdb") or None,
                                        tmdb_id=str(show_ids.get("tmdb")) if show_ids.get("tmdb") else None,
                                        trakt_id=str(ep_ids.get("trakt")) if ep_ids.get("trakt") else None,
                                        tvdb_id=str(show_ids.get("tvdb")) if show_ids.get("tvdb") else None,
                                        watched_at=dt_naive,
                                        runtime_minutes=runtime,
                                        source="backfill_trakt",
                                    )

                                try:
                                    if await _exists(dt_naive, wh.title, wh.item_type):
                                        skipped["trakt"] += 1
                                    else:
                                        db.add(wh)
                                        await db.flush()
                                        added["trakt"] += 1
                                except Exception:
                                    await db.rollback()
                                    skipped["trakt"] += 1

                            if len(history) < per_page:
                                break  # last page
                            page += 1
                    await db.commit()
                finally:
                    await trakt.close()
            except Exception as e:
                log.warning("backfill.trakt_failed", error=str(e)[:200])
                await db.rollback()

        # ── 2. MDBList (last watched date + plays count) ──────────────
        try:
            r = await get_redis()
            raw_key = await r.get("mdblist_api_key")
            if raw_key:
                from app.utils.mdblist_client import MDBListClient
                key = raw_key if isinstance(raw_key, str) else raw_key.decode()
                mdb = MDBListClient(api_key=key)
                try:
                    watched_data = await mdb.get_watched()
                    for kind, wh_type in (("movies", "movie"), ("shows", "show")):
                        for entry in watched_data.get(kind, []):
                            watched_at = entry.get("watched_at") or entry.get("last_watched_at", "")
                            if not watched_at:
                                continue
                            try:
                                dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                dt_naive = dt.replace(tzinfo=None)
                            except (ValueError, TypeError):
                                continue

                            ids = entry.get("ids", {})
                            # MDBList only gives last watched date, not individual plays
                            wh = WatchHistory(
                                user_id=user.id,
                                item_type=wh_type if wh_type == "movie" else "episode",
                                title=entry.get("title", ""),
                                imdb_id=ids.get("imdb") or None,
                                tmdb_id=str(ids.get("tmdb")) if ids.get("tmdb") else None,
                                trakt_id=str(ids.get("trakt")) if ids.get("trakt") else None,
                                tvdb_id=str(ids.get("tvdb")) if ids.get("tvdb") else None,
                                watched_at=dt_naive,
                                source="backfill_mdblist",
                            )
                            try:
                                wh_title = entry.get("title", "")
                                wh_type = wh_type if wh_type == "movie" else "episode"
                                if await _exists(dt_naive, wh_title, wh_type):
                                    skipped["mdblist"] += 1
                                else:
                                    db.add(wh)
                                    await db.flush()
                                    added["mdblist"] += 1
                            except Exception:
                                await db.rollback()
                                skipped["mdblist"] += 1
                    await db.commit()
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("backfill.mdblist_failed", error=str(e)[:200])
            await db.rollback()

        # ── 3. Emby (LastPlayedDate only, one date per item) ──────────
        if user.emby_user_id:
            try:
                emby = EmbyClient()
                try:
                    for emby_type in ("Movie", "Episode"):
                        start = 0
                        batch = 500
                        while True:
                            resp = await emby.get_items(
                                user_id=user.emby_user_id,
                                item_type=emby_type,
                                filters="IsPlayed",
                                fields="ProviderIds,UserData,UserDataLastPlayedDate,RunTimeTicks,SeriesName,ParentIndexNumber,IndexNumber",
                                limit=batch,
                                start_index=start,
                            )
                            items = resp.get("Items", []) if isinstance(resp, dict) else resp
                            if not items:
                                break

                            for item in items:
                                ud = item.get("UserData", {})
                                last_played = ud.get("LastPlayedDate", "")
                                if not last_played:
                                    start += batch
                                    continue
                                try:
                                    dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                                    dt_naive = dt.replace(tzinfo=None)
                                except (ValueError, TypeError):
                                    continue

                                pids = item.get("ProviderIds", {})
                                runtime_ticks = item.get("RunTimeTicks", 0) or 0
                                runtime_min = int(runtime_ticks / 600_000_000) if runtime_ticks else None

                                wh = WatchHistory(
                                    user_id=user.id,
                                    emby_id=item.get("Id"),
                                    item_type="episode" if emby_type == "Episode" else "movie",
                                    title=item.get("Name", ""),
                                    series_name=item.get("SeriesName") if emby_type == "Episode" else None,
                                    season_number=item.get("ParentIndexNumber") if emby_type == "Episode" else None,
                                    episode_number=item.get("IndexNumber") if emby_type == "Episode" else None,
                                    imdb_id=pids.get("Imdb") or None,
                                    tmdb_id=str(pids.get("Tmdb")) if pids.get("Tmdb") else None,
                                    tvdb_id=str(pids.get("Tvdb")) if pids.get("Tvdb") else None,
                                    watched_at=dt_naive,
                                    runtime_minutes=runtime_min,
                                    source="backfill_emby",
                                )
                                try:
                                    wh_title = item.get("Name", "")
                                    wh_type = "episode" if emby_type == "Episode" else "movie"
                                    if await _exists(dt_naive, wh_title, wh_type):
                                        skipped["emby"] += 1
                                    else:
                                        db.add(wh)
                                        await db.flush()
                                        added["emby"] += 1
                                except Exception:
                                    await db.rollback()
                                    skipped["emby"] += 1

                            if len(items) < batch:
                                break
                            start += batch
                    await db.commit()
                finally:
                    await emby.close()
            except Exception as e:
                log.warning("backfill.emby_failed", error=str(e)[:200])
                await db.rollback()

    total_added = sum(added.values())
    total_skipped = sum(skipped.values())
    log.info("backfill.complete", user_id=user_id, added=added, skipped=skipped,
             duplicates_cleaned=dupes_removed)
    return {
        "status": "ok",
        "added": added,
        "skipped_duplicates": skipped,
        "duplicates_cleaned": dupes_removed,
        "total_added": total_added,
        "total_skipped": total_skipped,
    }

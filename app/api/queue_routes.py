"""Routes extracted from routes.py — queue_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import QueueItem, User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.security.auth import get_current_user, require_user_ownership
from app.api.route_helpers import _get_mdblist_key, _get_setting, _put_setting, record_job_run
from app.services.airing_alerts.service import AiringAlertsService
from app.services.smart_queue.service import SmartQueueService

log = structlog.get_logger()

router = APIRouter()


async def _fetch_mdblist_ratings(
    items: list, mdb_key: str
) -> dict[str, dict]:
    """Batch-fetch MDBList media info for queue items.

    Returns {imdb_id: {imdb, tmdb, tomatoes, popcorn}} where each value
    is a number or None.  Uses the same 24h Redis cache as the individual
    ``/api/mdblist/media-info`` endpoint.
    """
    import json as _json
    from app.utils.mdblist_client import MDBListClient

    result: dict[str, dict] = {}

    # Dedupe by imdb_id — many items share no IMDB, skip those
    id_map: dict[str, str] = {}  # imdb_id → media_type
    for i in items:
        ids = (i.metadata_json or {}).get("ids", {})
        imdb = ids.get("imdb")
        if imdb and imdb not in id_map:
            id_map[imdb] = "show" if i.item_type == "show" else "movie"

    if not id_map:
        return result

    r = await get_redis()

    # Check Redis cache first
    uncached: dict[str, str] = {}
    for imdb_id, media_type in id_map.items():
        cache_key = f"mdblist_media_info:imdb:{media_type}:{imdb_id}"
        try:
            raw = await r.get(cache_key)
            if raw:
                data = _json.loads(raw)
                result[imdb_id] = _extract_ratings(data)
                continue
        except Exception:
            pass
        uncached[imdb_id] = media_type

    if not uncached:
        return result

    # Fetch uncached items in parallel (max 5 concurrent)
    sem = asyncio.Semaphore(5)
    client = MDBListClient(api_key=mdb_key)

    async def _fetch_one(imdb_id: str, media_type: str):
        async with sem:
            try:
                data = await client.get_media_info("imdb", media_type, imdb_id)
                if data:
                    cache_key = f"mdblist_media_info:imdb:{media_type}:{imdb_id}"
                    try:
                        await r.set(cache_key, _json.dumps(data), ex=86400)
                    except Exception:
                        pass
                    extracted = _extract_ratings(data)
                    result[imdb_id] = extracted
            except Exception as exc:
                log.warning("queue.mdblist_ratings.fetch_error",
                            imdb_id=imdb_id, error=str(exc)[:120])

    try:
        await asyncio.gather(
            *[_fetch_one(iid, mt) for iid, mt in uncached.items()]
        )
    finally:
        await client.close()

    return result


def _extract_ratings(data: dict) -> dict:
    """Pull the 4 display ratings from an MDBList media-info response.

    MDBList stores ratings in two places:
    1. ``data["ratings"]`` — list of ``{source, value, score, votes}``
    2. Top-level fields: ``imdbrating``, ``tmdbrating``, etc.
    We check the list first (more reliable), then fall back to top-level.
    """
    r: dict = {}
    # Source 1: ratings array
    for item in (data.get("ratings") or []):
        src = (item.get("source") or "").lower()
        val = item.get("value")
        if val is None:
            continue
        if src == "imdb" and "imdb" not in r:
            r["imdb"] = val
        elif src == "tmdb" and "tmdb" not in r:
            r["tmdb"] = val
        elif src == "tomatoes" and "tomatoes" not in r:
            r["tomatoes"] = val
        elif src in ("tomatoesaudience", "audience") and "popcorn" not in r:
            r["popcorn"] = val
    # Source 2: top-level fields (fill gaps only)
    if "imdb" not in r and data.get("imdbrating"):
        r["imdb"] = data["imdbrating"]
    if "tmdb" not in r and data.get("tmdbrating"):
        r["tmdb"] = data["tmdbrating"]
    if "tomatoes" not in r and data.get("tomatoesrating"):
        r["tomatoes"] = data["tomatoesrating"]
    if "popcorn" not in r and data.get("tomatoesaudience"):
        r["popcorn"] = data["tomatoesaudience"]
    return r

airing_alerts_svc = AiringAlertsService()
smart_queue_svc = SmartQueueService()


@router.post("/queue/refresh")
async def refresh_queue(_user: User = Depends(get_current_user)):
    """Manually trigger a Smart Queue update for all users."""
    import time as _time
    async def _run():
        t = _time.time()
        try:
            await smart_queue_svc.run_for_all_users()
            await record_job_run("smart_queue", "ok", _time.time() - t)
        except Exception as e:
            await record_job_run("smart_queue", "error", _time.time() - t, str(e)[:200])
    asyncio.create_task(_run())
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

    # ── MDBList multi-source ratings enrichment ───────────────────────────
    mdb_ratings: dict[str, dict] = {}
    mdb_key = await _get_mdblist_key(db)
    if mdb_key:
        mdb_ratings = await _fetch_mdblist_ratings(items, mdb_key)

    return [
        {
            "emby_item_id": i.emby_item_id,
            "title": i.title,
            "type": i.item_type,
            "source": i.source,
            "score": i.score,
            "trending_rank": i.simkl_trending_rank,
            "played": i.played,
            "played_at": i.played_at.isoformat() if i.played_at else None,
            "in_library": i.in_library if i.in_library is not None else True,
            "community_rating": ratings.get(str(i.emby_item_id), {}).get("community_rating"),
            "official_rating": ratings.get(str(i.emby_item_id), {}).get("official_rating"),
            "date_created": ratings.get(str(i.emby_item_id), {}).get("date_created"),
            "tmdb_id": (i.metadata_json or {}).get("ids", {}).get("tmdb"),
            "imdb_id": (i.metadata_json or {}).get("ids", {}).get("imdb"),
            "tvdb_id": (i.metadata_json or {}).get("ids", {}).get("tvdb"),
            "simkl_id": (i.metadata_json or {}).get("simkl_id"),
            "year": (i.metadata_json or {}).get("year"),
            "mdblist_ratings": mdb_ratings.get(
                (i.metadata_json or {}).get("ids", {}).get("imdb", ""), {}
            ),
        }
        for i in items
    ]


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
    simkl_id = str(payload.get("simkl_id", ""))
    title = payload.get("title", "")
    item_type = payload.get("item_type", "")

    if not simkl_id or not user_id:
        raise HTTPException(400, "user_id and simkl_id required")
    require_user_ownership(current_user.id, user_id, "queue_block")

    # Insert into blocklist (ignore duplicate)
    existing = (await db.execute(
        select(QueueBlocklist).where(
            QueueBlocklist.user_id == user_id,
            QueueBlocklist.simkl_id == simkl_id,
        )
    )).scalar_one_or_none()
    if not existing:
        db.add(QueueBlocklist(
            user_id=user_id, simkl_id=simkl_id,
            title=title, item_type=item_type,
        ))
        await db.flush()

    # Find and remove matching queue item
    queue_items = (await db.execute(
        select(QueueItem).where(QueueItem.user_id == user_id)
    )).scalars().all()
    removed = False
    for qi in queue_items:
        qi_simkl = str((qi.metadata_json or {}).get("simkl_id", ""))
        if qi_simkl == simkl_id:
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
                        simkl_trending_rank=replacement.get("trending_rank"),
                        simkl_rating=replacement.get("friend_rating"),
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

    log.info("queue.item_blocked", user_id=user_id, simkl_id=simkl_id, title=title)
    return {"status": "ok", "blocked": simkl_id, "title": title}


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
                "simkl_id": i.simkl_id,
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
    force_refresh: bool = Query(False),
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

    # Clear response cache if force refresh requested
    if force_refresh:
        try:
            r = await get_redis()
            await r.delete(f"airing_alerts:response:{user_id}:{days}")
        except Exception:
            pass

    try:
        result = await airing_alerts_svc.get_airing_soon(user, days=days)
    except Exception:
        log.exception("airing_soon.fetch_failed", user_id=user_id)
        raise HTTPException(502, "failed to fetch airing calendar from Simkl")

    alerts = result.get("items", [])
    home_releases = result.get("upcoming_home_releases", [])

    # Fire watchlist sync in background — ensures missing Radarr/Sonarr
    # items are on the Simkl/MDBList watchlist so they appear in Airing Soon.
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


QUEUE_SETTINGS_DEFAULTS = {
    "s01e01_only": False,
    "queue_size": 20,
    "create_playlist": True,
}
VALID_QUEUE_SIZES = (20, 30, 40, 50)


def _normalise_queue_settings(data: dict) -> dict:
    """Coerce a raw settings blob to known keys with valid values."""
    out = dict(QUEUE_SETTINGS_DEFAULTS)
    if not isinstance(data, dict):
        return out
    out["s01e01_only"] = bool(data.get("s01e01_only", out["s01e01_only"]))
    out["create_playlist"] = bool(data.get("create_playlist", out["create_playlist"]))
    try:
        size = int(data.get("queue_size", out["queue_size"]))
    except (TypeError, ValueError):
        size = out["queue_size"]
    out["queue_size"] = size if size in VALID_QUEUE_SIZES else QUEUE_SETTINGS_DEFAULTS["queue_size"]
    return out


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
            return _normalise_queue_settings(_json.loads(raw))
        except Exception:
            pass
    return dict(QUEUE_SETTINGS_DEFAULTS)


@router.put("/api/queue-settings")
async def update_queue_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save smart queue settings to DB + Redis.

    Merges into the existing blob rather than replacing it, so a partial
    payload (e.g. just the size dropdown) doesn't wipe the other settings.
    """
    import json as _json
    r = await get_redis()

    # Start from what's already stored so unsent keys survive
    existing = dict(QUEUE_SETTINGS_DEFAULTS)
    try:
        raw = await r.get("queue_settings") or await _get_setting(db, "queue_settings", "")
        if raw:
            existing = _normalise_queue_settings(_json.loads(raw))
    except Exception:
        pass

    merged = dict(existing)
    for key in QUEUE_SETTINGS_DEFAULTS:
        if key in payload:
            merged[key] = payload[key]
    queue_settings = _normalise_queue_settings(merged)

    encoded = _json.dumps(queue_settings)
    await r.set("queue_settings", encoded)
    await _put_setting(db, "queue_settings", encoded)
    await db.commit()
    log.info("queue_settings.saved", **queue_settings)
    return {"status": "ok", **queue_settings}


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

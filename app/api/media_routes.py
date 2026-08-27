"""Routes extracted from routes.py — media_routes.py."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy import distinct, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import AppSetting, User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user, require_user_ownership
from app.api.route_helpers import _first_emby_user_id, _get_active_providers, _get_mdblist_key, _validate_item_key, record_job_run

log = structlog.get_logger()

router = APIRouter()



class RewatchSettings(BaseModel):
    """Validated rewatch recommender settings."""
    min_rating: int = Field(default=8, ge=1, le=10)
    min_months: int = Field(default=12, ge=1, le=120)
    seasonal: bool = True


@router.get("/api/simkl-lists")
async def get_simkl_lists():
    """Fetch all Simkl lists available to the user: personal, liked, and collaborations."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.simkl_access_token:
            raise HTTPException(400, "No Simkl-linked user found")

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        my_lists = await simkl.get_my_lists()
        liked_lists = await simkl.get_liked_lists()
        collab_lists = await simkl.get_collaborations()
    finally:
        await simkl.close()

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


@router.post("/api/simkl-lists/import")
async def import_simkl_list(payload: dict, _user: User = Depends(get_current_user)):
    """Import a Simkl list into an Emby playlist.

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
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.simkl_access_token:
            raise HTTPException(400, "No Simkl-linked user found")

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        # Fetch items — the endpoint returns items under /users/{username}/lists/{slug}/items
        items = await simkl.get_list_items(username, list_slug)
    finally:
        await simkl.close()

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
            log.info("simkl_list.imported", slug=list_slug, name=final_name,
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

    Called by the browser extension.  Accepts IDs from Simkl, IMDB, or TMDB,
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
        raise HTTPException(400, "at least one ID required (imdb_id, tmdb_id, simkl_slug)")

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

    # Simkl slug → resolve via Simkl API to get provider IDs
    if not matches and ids.get("simkl_slug") and user.simkl_access_token:
        try:
            simkl = SimklClient(access_token=user.simkl_access_token)
            kind = "movie" if media_type == "movie" else "show"
            results = await simkl.search(query=ids["simkl_slug"], kind=kind)
            await simkl.close()
            if results:
                item_data = results[0].get(kind, {})
                simkl_ids = item_data.get("ids", {})
                for ptype, tkey in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
                    pid = simkl_ids.get(tkey)
                    if pid:
                        cached = await LibraryCache.find_by_provider_id(ptype, str(pid))
                        if cached:
                            matches.append(cached)
                            break
        except Exception:
            log.debug("remote_play.simkl_resolve_failed", slug=ids.get("simkl_slug"))

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


@router.get("/api/playback-sync/{user_id}")
async def get_playback_sync(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Simkl in-progress playback items with Emby resume points.

    Returns items that exist on Simkl's playback list, enriched with
    Emby resume data if available.  Surfaces mismatches (Simkl has a
    resume point but Emby doesn't, or vice versa) and stale entries
    (paused > 30 days ago).
    """
    import json as _json

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.simkl_access_token:
        raise HTTPException(404, "User not found or no Simkl account linked")
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

    # Fetch Simkl playback progress
    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )

    try:
        simkl_playback = await simkl.get_playback()
    except Exception as e:
        log.warning("playback_sync.simkl_fetch_failed", error=str(e)[:120])
        raise HTTPException(502, f"Failed to fetch Simkl playback: {str(e)[:100]}")

    if not simkl_playback:
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

    for pb in simkl_playback:
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
            "simkl_playback_id": pb_id,
            "type": pb_type,
            "title": title,
            "episode": ep_label,
            "simkl_progress": round(progress, 1),
            "paused_at": paused_at,
            "days_stale": days_stale,
            "simkl_ids": {k: v for k, v in ids.items() if v},
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
async def delete_simkl_playback(
    user_id: int,
    playback_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a stale playback entry from Simkl."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.simkl_access_token:
        raise HTTPException(404, "User not found or no Simkl account linked")
    require_user_ownership(current_user.id, user_id, "playback_sync")

    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )

    await simkl.delete_playback(playback_id)

    # Invalidate cache
    try:
        r = await get_redis()
        await r.delete(f"playback_sync_v1:{user.id}")
    except Exception:
        pass

    return {"status": "deleted", "playback_id": playback_id}


# ═══════════════════════════════════════════════════════════════════════════


from app.services.rewatch.service import RewatchRecommender

_rewatch_svc = RewatchRecommender()


@router.get("/api/rewatch/{user_id}")
async def get_rewatch_suggestions(
    user_id: int,
    page: int = 1,
    page_size: int = 30,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return cached rewatch suggestions for a user (paginated)."""
    require_user_ownership(current_user.id, user_id, "rewatch")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    all_items = await _rewatch_svc.get_suggestions(user_id)
    total = len(all_items)
    start = (page - 1) * page_size
    end = start + page_size
    items = all_items[start:end]
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "items": items,
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.post("/api/rewatch/{user_id}/refresh")
async def refresh_rewatch(
    user_id: int,
    page: int = 1,
    page_size: int = 30,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Force rebuild rewatch suggestions (clears cache first)."""
    import time as _time
    require_user_ownership(_user.id, user_id, "rewatch_refresh")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    t = _time.time()
    try:
        all_items = await _rewatch_svc.build_suggestions(user_id)
        await record_job_run("rewatch_rebuild", "ok", _time.time() - t)
    except Exception as e:
        await record_job_run("rewatch_rebuild", "error", _time.time() - t, str(e)[:200])
        raise
    total = len(all_items)
    start = (page - 1) * page_size
    end = start + page_size
    items = all_items[start:end]
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "items": items,
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "status": "rebuilt",
    }


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
    current_user: User = Depends(get_current_user),
):
    """Lazy-load watch history for hover flyout."""
    require_user_ownership(current_user.id, user_id, "rewatch_history")
    _validate_item_key(item_key)
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _rewatch_svc.get_item_history(user_id, item_key)


@router.get("/api/rewatch/{user_id}/settings")
async def get_rewatch_settings(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Read rewatch recommender settings for a user."""
    require_user_ownership(current_user.id, user_id, "rewatch_settings")
    import json as _json
    r = await get_redis()
    raw = await r.get(f"rewatch:settings:{user_id}")
    if raw:
        return _json.loads(raw)
    return {"min_rating": 8, "min_months": 12, "seasonal": True}


@router.put("/api/rewatch/{user_id}/settings")
async def update_rewatch_settings(
    user_id: int,
    payload: RewatchSettings,
    _user: User = Depends(get_current_user),
):
    """Save rewatch recommender settings."""
    import json as _json
    require_user_ownership(_user.id, user_id, "rewatch_settings")
    r = await get_redis()
    settings_data = payload.model_dump()
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


_IMAGE_TYPE_RE = re.compile(r"^(Primary|Thumb|Backdrop|Banner|Logo|Art|Disc|Box|BoxRear|Screenshot)$")


@router.get("/api/emby/image/{item_id}/{image_type}")
async def proxy_emby_image(
    item_id: str,
    image_type: str,
    maxWidth: int = 400,
):
    """Proxy Emby item images so the frontend never sees the API key."""
    if not _IMAGE_TYPE_RE.match(image_type):
        raise HTTPException(400, "Invalid image type")
    if not re.match(r"^[A-Za-z0-9]+$", item_id):
        raise HTTPException(400, "Invalid item ID")
    maxWidth = max(50, min(maxWidth, 1920))

    emby_url = os.getenv("EMBY_URL", "")
    emby_key = os.getenv("EMBY_API_KEY", "")
    if not emby_url or not emby_key:
        raise HTTPException(503, "Emby not configured")

    import httpx
    url = f"{emby_url}/Items/{item_id}/Images/{image_type}?maxWidth={maxWidth}&api_key={emby_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
        if resp.status_code == 404:
            raise HTTPException(404, "Image not found")
        if resp.status_code != 200:
            raise HTTPException(502, "Emby returned an error")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(
            content=resp.content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except httpx.TimeoutException:
        raise HTTPException(504, "Emby image request timed out")
    except httpx.RequestError:
        raise HTTPException(502, "Could not reach Emby server")


@router.get("/api/tmdb/image/{path:path}")
async def proxy_tmdb_image(path: str):
    """Proxy TMDB images so they work on networks that block image.tmdb.org."""
    import httpx
    # Validate path looks like a TMDB image path (e.g. w185/abc123.jpg)
    if not re.match(r"^w\d+/[A-Za-z0-9]+\.\w{3,4}$", path):
        raise HTTPException(400, "Invalid TMDB image path")
    url = f"https://image.tmdb.org/t/p/{path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            raise HTTPException(resp.status_code, "TMDB image not found")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(
            content=resp.content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=604800"},
        )
    except httpx.TimeoutException:
        raise HTTPException(504, "TMDB image request timed out")
    except httpx.RequestError:
        raise HTTPException(502, "Could not reach TMDB")


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/watch-history/{user_id}/recommendations")
async def get_history_recommendations(
    user_id: int,
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Recommend unwatched library items based on the user's watch history
    and MDBList/Simkl ratings.

    Approach:
      1. Fetch user's rated items (MDBList primary, Simkl supplement)
      2. Identify top-rated genres (weighted by rating)
      3. Find unwatched items in Emby library matching those genres
      4. Score by genre overlap × community rating
    """
    require_user_ownership(current_user.id, user_id, "recommendations")
    from app.utils.library_cache import LibraryCache
    from app.utils.mdblist_client import MDBListClient
    from app.utils.secure_redis import secure_get
    from app.utils.emby_client import EmbyClient
    from collections import Counter

    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    r = await get_redis()
    cache_key = f"history_recs_v1:{user_id}"
    cached = await r.get(cache_key)
    if cached:
        import json as _json
        return _json.loads(cached)

    # ── 1. Collect rated items from MDBList + Simkl ──
    genre_scores: Counter = Counter()  # genre → sum of ratings
    genre_counts: Counter = Counter()  # genre → count
    rated_imdb: set[str] = set()       # already rated/watched items

    # MDBList ratings (primary — has a rating for every watched item)
    mdb_key = await secure_get("mdblist_api_key")
    if mdb_key:
        mdb = MDBListClient(api_key=mdb_key)
        try:
            mdb_ratings = await mdb.get_ratings()
            for kind in ("movies", "shows"):
                for item in (mdb_ratings.get(kind, [])
                             if isinstance(mdb_ratings, dict) else []):
                    rating = item.get("rating")
                    iid = (item.get("ids") or {}).get("imdb", "")
                    genres = [g.lower() for g in item.get("genres", [])]
                    if rating and iid:
                        rated_imdb.add(iid)
                        for g in genres:
                            genre_scores[g] += float(rating)
                            genre_counts[g] += 1
        except Exception:
            pass
        finally:
            await mdb.close()

    # Simkl ratings (supplement — may have items MDBList doesn't)
    if user.simkl_access_token:
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )
        try:
            sr = await simkl.get_user_ratings("all")
            for entry in sr:
                item_obj = entry.get("movie") or entry.get("show") or entry
                iid = (item_obj.get("ids") or {}).get("imdb", "")
                rating = entry.get("rating")
                genres = [g.lower() for g in item_obj.get("genres", [])]
                if rating and iid and iid not in rated_imdb:
                    rated_imdb.add(iid)
                    for g in genres:
                        genre_scores[g] += float(rating)
                        genre_counts[g] += 1
        except Exception:
            pass
        finally:
            await simkl.close()

    if not genre_scores:
        return {"items": [], "top_genres": [], "rated_count": len(rated_imdb)}

    # ── 2. Compute genre affinity: avg rating per genre ──
    genre_affinity = {
        g: genre_scores[g] / genre_counts[g]
        for g in genre_scores
        if genre_counts[g] >= 3  # need at least 3 ratings to be meaningful
    }
    top_genres = sorted(genre_affinity.items(), key=lambda x: x[1], reverse=True)[:8]
    top_genre_set = {g for g, _ in top_genres}

    # ── 3. Scan Emby library for unwatched items matching top genres ──
    emby = EmbyClient()
    try:
        # Get user's played items from Emby
        played_items = await emby.get_items(
            user_id=user.emby_user_id,
            params={"IsPlayed": "true", "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series",
                    "Fields": "ProviderIds,Genres,CommunityRating",
                    "Limit": "10000"}
        )
        played_imdb = set()
        for pi in (played_items or []):
            pid = (pi.get("ProviderIds") or {}).get("Imdb", "")
            if pid:
                played_imdb.add(pid)

        # Get all unwatched items
        all_items = await emby.get_items(
            user_id=user.emby_user_id,
            params={"IsPlayed": "false", "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series",
                    "Fields": "ProviderIds,Genres,CommunityRating,Overview",
                    "Limit": "5000"}
        )
    finally:
        await emby.close()

    # ── 4. Score unwatched items ──
    scored: list[dict] = []
    for item in (all_items or []):
        item_imdb = (item.get("ProviderIds") or {}).get("Imdb", "")
        if item_imdb in rated_imdb or item_imdb in played_imdb:
            continue  # already watched/rated

        item_genres = {g.lower() for g in item.get("Genres", [])}
        overlap = item_genres & top_genre_set
        if not overlap:
            continue

        community_rating = item.get("CommunityRating") or 0
        # Score = genre overlap count × avg genre affinity × community rating boost
        genre_boost = sum(genre_affinity.get(g, 0) for g in overlap) / len(overlap)
        score = len(overlap) * genre_boost * (1 + community_rating / 10)

        scored.append({
            "emby_id": item.get("Id"),
            "title": item.get("Name", ""),
            "year": item.get("ProductionYear"),
            "item_type": "movie" if item.get("Type") == "Movie" else "show",
            "genres": list(item_genres),
            "matched_genres": list(overlap),
            "community_rating": round(community_rating, 1),
            "score": round(score, 2),
            "imdb_id": item_imdb,
            "overview": (item.get("Overview") or "")[:200],
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    result_items = scored[:limit]

    result = {
        "items": result_items,
        "top_genres": [{"genre": g, "avg_rating": round(s, 1)} for g, s in top_genres],
        "rated_count": len(rated_imdb),
    }

    # Cache for 6 hours
    import json as _json
    try:
        await r.setex(cache_key, 21600, _json.dumps(result))
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════



@router.post("/api/watchlist/sync/{user_id}")
async def sync_watchlist_local(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Pull watchlist from active providers and store locally for fast lookups."""
    from app.models.schema import WatchlistItem
    _now = datetime.now(timezone.utc).replace(tzinfo=None)
    imported = 0

    # Collect all items keyed by tmdb_id (primary) — Simkl items often lack IMDB
    wl_items: dict[str, dict] = {}  # tmdb_id -> {imdb_id, title, item_type, source}
    providers = await _get_active_providers(db)

    # ── Simkl ──
    if "simkl" in providers and current_user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=current_user.simkl_access_token,
                token_expires=current_user.simkl_token_expires,
            )
            try:
                for kind, itype in [("movies", "movie"), ("shows", "show"), ("anime", "show")]:
                    entries = await simkl.get_watchlist(kind=kind)
                    for entry in entries:
                        # Simkl wraps plantowatch items: {"movie": {"title": ..., "ids": {...}}}
                        inner = entry.get("movie") or entry.get("show") or entry
                        ids = inner.get("ids", {})
                        tmdb = str(ids.get("tmdb")) if ids.get("tmdb") else None
                        imdb = ids.get("imdb")
                        # Need at least one ID to store
                        if not tmdb and not imdb:
                            continue
                        key = tmdb or imdb  # prefer tmdb as dedup key
                        if key not in wl_items:
                            wl_items[key] = {
                                "imdb_id": imdb,
                                "tmdb_id": tmdb,
                                "title": inner.get("title"),
                                "item_type": itype,
                                "source": "simkl",
                            }
                log.info("watchlist_local_sync.simkl_collected",
                         count=len(wl_items), user_id=current_user.id)
            finally:
                await simkl.close()
        except Exception as e:
            log.warning("watchlist_local_sync.simkl_failed", error=str(e)[:120])

    # ── MDBList ──
    if "mdblist" in providers:
        _pre = len(wl_items)
        try:
            mdb_key = await _get_mdblist_key(db)
            if mdb_key:
                from app.utils.mdblist_client import MDBListClient
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    wl_data = await mdb.get_watchlist()
                    for mtype, itype in [("movies", "movie"), ("shows", "show")]:
                        for entry in wl_data.get(mtype, []):
                            tmdb = str(entry.get("tmdb_id") or entry.get("tmdb") or "") or None
                            imdb = entry.get("imdb_id") or entry.get("imdb")
                            if not tmdb and not imdb:
                                continue
                            key = tmdb or imdb
                            if key not in wl_items:
                                wl_items[key] = {
                                    "imdb_id": imdb,
                                    "tmdb_id": tmdb,
                                    "title": entry.get("title"),
                                    "item_type": itype,
                                    "source": "mdblist",
                                }
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("watchlist_local_sync.mdblist_failed", error=str(e)[:120])
        log.info("watchlist_local_sync.mdblist_collected",
                 added=len(wl_items) - _pre, user_id=current_user.id)

    # ── Upsert locally, preserve user-submitted entries ──
    # Delete provider-synced rows (not user-submitted)
    await db.execute(
        WatchlistItem.__table__.delete().where(
            WatchlistItem.user_id == current_user.id,
            WatchlistItem.source != "user",
        )
    )

    # Collect existing user-submitted keys for dedup
    _user_rows = (await db.execute(
        select(WatchlistItem.imdb_id, WatchlistItem.tmdb_id).where(
            WatchlistItem.user_id == current_user.id,
            WatchlistItem.source == "user",
        )
    )).all()
    existing_user_keys: set[str] = set()
    for row in _user_rows:
        if row[0]:
            existing_user_keys.add(row[0])
        if row[1]:
            existing_user_keys.add(row[1])

    _seen_imdb: set[str] = set(existing_user_keys)
    _seen_tmdb: set[str] = set(existing_user_keys)
    for key, info in wl_items.items():
        imdb = info["imdb_id"]
        tmdb = info["tmdb_id"]
        # Skip if user already has, or if we'd violate a unique constraint
        if imdb and imdb in _seen_imdb:
            continue
        if tmdb and tmdb in _seen_tmdb:
            continue
        if imdb:
            _seen_imdb.add(imdb)
        if tmdb:
            _seen_tmdb.add(tmdb)
        db.add(WatchlistItem(
            user_id=current_user.id,
            imdb_id=imdb,
            tmdb_id=tmdb,
            title=info["title"],
            item_type=info["item_type"],
            source=info["source"],
            added_at=_now,
            synced_at=_now,
        ))
        imported += 1

    await db.commit()
    log.info("watchlist_local_sync.complete", user_id=current_user.id, imported=imported,
             user_kept=len(_user_rows), provider_total=len(wl_items))

    return {"synced": imported, "user_kept": len(_user_rows), "total": imported + len(_user_rows)}


@router.get("/api/watchlist/check")
async def check_watchlist(
    imdb_id: str | None = None,
    tmdb_id: str | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Check if an item is on the local watchlist."""
    from app.models.schema import WatchlistItem
    from sqlalchemy import or_
    if not imdb_id and not tmdb_id:
        return {"on_watchlist": False}
    conditions = []
    if imdb_id:
        conditions.append(WatchlistItem.imdb_id == imdb_id)
    if tmdb_id:
        conditions.append(WatchlistItem.tmdb_id == str(tmdb_id))
    q = select(WatchlistItem.id).where(
        WatchlistItem.user_id == current_user.id,
        or_(*conditions),
    )
    row = (await db.execute(q.limit(1))).first()
    return {"on_watchlist": row is not None}


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/filmography")
async def filmography_page():
    """Serve the filmography tracker page."""
    with open("frontend/templates/filmography.html", "r") as f:
        return HTMLResponse(f.read())


@router.get("/api/filmography/popular")
async def get_popular_people(current_user: User = Depends(get_current_user)):
    """Return popular actors/directors from TMDB for suggestions."""
    from app.utils.tmdb_client import get_popular_people as _get_popular
    people = await _get_popular(limit=20)
    return {"people": people}


@router.get("/api/filmography/{person_name}")
async def get_filmography(
    person_name: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch a person's filmography from TMDB, cross-referenced with library, watch history & watchlist."""
    from app.utils.tmdb_client import get_person_details
    from app.models.schema import WatchHistory, WatchlistItem

    person = await get_person_details(person_name)
    if not person:
        raise HTTPException(404, f"Person '{person_name}' not found on TMDB")

    # Merge cast + crew entries, dedup by id+media_type
    all_works = {}
    for item in person.get("cast", []):
        key = f"{item['media_type']}:{item['id']}"
        if key not in all_works:
            all_works[key] = {**item, "roles": []}
        all_works[key]["roles"].append(f"Actor ({item.get('character', '?')})")

    for item in person.get("crew", []):
        key = f"{item['media_type']}:{item['id']}"
        if key not in all_works:
            all_works[key] = {**item, "roles": []}
        all_works[key]["roles"].append(item.get("job", "Crew"))

    # Build watched lookups from watch history
    user_id = current_user.id
    _watched_movie_tmdb = set()
    _watched_movie_imdb = set()
    _watched_series_names: set[str] = set()

    # Movies: collect distinct tmdb_id and imdb_id
    _wm_rows = (await db.execute(
        select(WatchHistory.tmdb_id, WatchHistory.imdb_id)
        .where(WatchHistory.user_id == user_id, WatchHistory.item_type == "movie")
        .distinct()
    )).all()
    for row in _wm_rows:
        if row.tmdb_id:
            _watched_movie_tmdb.add(str(row.tmdb_id))
        if row.imdb_id:
            _watched_movie_imdb.add(row.imdb_id)

    # Shows: collect distinct series_name (case-insensitive)
    _ws_rows = (await db.execute(
        select(WatchHistory.series_name)
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.item_type == "episode",
            WatchHistory.series_name.isnot(None),
        )
        .distinct()
    )).all()
    for row in _ws_rows:
        if row.series_name:
            _watched_series_names.add(row.series_name.lower().strip())

    # Build watchlist lookup sets (local DB) — both IMDB and TMDB
    _wl_imdb_set: set[str] = set()
    _wl_tmdb_set: set[str] = set()
    _wl_rows = (await db.execute(
        select(WatchlistItem.imdb_id, WatchlistItem.tmdb_id).where(
            WatchlistItem.user_id == user_id,
        )
    )).all()
    for row in _wl_rows:
        if row[0]:
            _wl_imdb_set.add(row[0])
        if row[1]:
            _wl_tmdb_set.add(str(row[1]))

    # Cross-reference with library cache
    works = []
    for work in all_works.values():
        tmdb_id_str = str(work.get("id", ""))
        found = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id_str) if tmdb_id_str else None
        _pids = found.get("provider_ids", {}) if found else {}
        work["in_library"] = found is not None
        work["emby_id"] = found.get("emby_id") if found else None
        work["imdb_id"] = _pids.get("Imdb") if found else None
        work["tvdb_id"] = _pids.get("Tvdb") if found else None

        # Determine watched status
        if work.get("media_type") == "movie":
            work["watched"] = (
                tmdb_id_str in _watched_movie_tmdb
                or (work["imdb_id"] and work["imdb_id"] in _watched_movie_imdb)
            )
        elif work.get("media_type") == "tv":
            _wname = (work.get("name") or work.get("title") or "").lower().strip()
            work["watched"] = _wname in _watched_series_names if _wname else False
        else:
            work["watched"] = False

        # Watchlist status (local DB — match by IMDB or TMDB)
        _on_wl = False
        if work.get("imdb_id") and work["imdb_id"] in _wl_imdb_set:
            _on_wl = True
        elif tmdb_id_str and tmdb_id_str in _wl_tmdb_set:
            _on_wl = True
        work["on_watchlist"] = _on_wl

        works.append(work)

    # Sort by release date descending
    works.sort(key=lambda x: x.get("release_date") or "0000", reverse=True)

    return {
        "person": {
            "name": person["name"],
            "profile_path": person.get("profile_path"),
            "known_for": person.get("known_for_department"),
        },
        "works": works,
        "total": len(works),
        "in_library": sum(1 for w in works if w["in_library"]),
        "watched": sum(1 for w in works if w.get("watched")),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Stale Emby ID Repair
# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/library/repair-emby-ids")
async def repair_stale_emby_ids(_user: User = Depends(get_current_user)):
    """Detect and repair stale Emby IDs across all tables after a library rebuild.

    For each row with a non-null emby_item_id / emby_id, checks whether that
    ID still exists in the current library cache.  If not, attempts to
    re-resolve via IMDB → TMDB → title lookup.  Returns a summary of what
    was found and fixed.
    """
    from app.models.schema import (
        QueueItem, Prediction, UniverseItem, WatchHistory, LibraryGap,
    )

    emby = EmbyClient()
    try:
        # Get first user for Emby calls
        async with async_session_ctx() as db:
            user = (await db.execute(
                select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
            )).scalars().first()
        uid = user.emby_user_id if user else None

        # Build a set of all valid emby IDs from library cache
        all_items = await LibraryCache.get_all_items()
        valid_ids = {item["emby_id"] for item in all_items if item.get("emby_id")}

        # Build reverse lookup: provider_id → emby_id
        imdb_to_emby: dict[str, str] = {}
        tmdb_to_emby: dict[str, str] = {}
        title_to_emby: dict[str, str] = {}
        for item in all_items:
            eid = item.get("emby_id")
            if not eid:
                continue
            if item.get("imdb_id"):
                imdb_to_emby[item["imdb_id"]] = eid
            if item.get("tmdb_id"):
                tmdb_to_emby[str(item["tmdb_id"])] = eid
            t = item.get("title", "").strip().lower()
            if t:
                key = t
                if item.get("year"):
                    key = f"{t}:{item['year']}"
                title_to_emby[key] = eid

        stats = {"scanned": 0, "stale": 0, "repaired": 0, "unresolvable": 0}
        details: list[dict] = []

        # Table definitions: (Model, id_col_name, provider_id_cols)
        table_defs = [
            (QueueItem, "emby_item_id", "metadata_json"),
            (Prediction, "emby_item_id", None),
            (UniverseItem, "emby_item_id", None),
            (WatchHistory, "emby_id", None),
            (LibraryGap, "emby_item_id", None),
        ]

        async with async_session_ctx() as db:
            for Model, id_col, meta_col in table_defs:
                col = getattr(Model, id_col)
                rows = (await db.execute(
                    select(Model).where(col.isnot(None))
                )).scalars().all()

                for row in rows:
                    old_id = getattr(row, id_col)
                    if not old_id:
                        continue
                    stats["scanned"] += 1
                    if old_id in valid_ids:
                        continue

                    # Stale — try to resolve new ID
                    stats["stale"] += 1
                    new_id = None
                    source = None

                    # Try IMDB
                    imdb = getattr(row, "imdb_id", None)
                    if not imdb and meta_col and hasattr(row, meta_col):
                        meta = getattr(row, meta_col) or {}
                        ids = meta.get("ids", {})
                        imdb = ids.get("imdb")
                    if imdb and imdb in imdb_to_emby:
                        new_id = imdb_to_emby[imdb]
                        source = "imdb"

                    # Try TMDB
                    if not new_id:
                        tmdb = getattr(row, "tmdb_id", None)
                        if not tmdb and meta_col and hasattr(row, meta_col):
                            meta = getattr(row, meta_col) or {}
                            ids = meta.get("ids", {})
                            tmdb = ids.get("tmdb")
                        if tmdb and str(tmdb) in tmdb_to_emby:
                            new_id = tmdb_to_emby[str(tmdb)]
                            source = "tmdb"

                    # Try title
                    if not new_id:
                        title = getattr(row, "title", None) or getattr(row, "series_name", None) or ""
                        title_lower = title.strip().lower()
                        year = getattr(row, "year", None)
                        if title_lower:
                            key = f"{title_lower}:{year}" if year else title_lower
                            if key in title_to_emby:
                                new_id = title_to_emby[key]
                                source = "title"
                            elif title_lower in title_to_emby:
                                new_id = title_to_emby[title_lower]
                                source = "title"

                    table_name = Model.__tablename__
                    title_display = getattr(row, "title", None) or getattr(row, "series_name", None) or "?"

                    if new_id:
                        setattr(row, id_col, new_id)
                        stats["repaired"] += 1
                        details.append({
                            "table": table_name, "title": title_display,
                            "old_id": old_id, "new_id": new_id, "via": source,
                        })
                    else:
                        stats["unresolvable"] += 1
                        details.append({
                            "table": table_name, "title": title_display,
                            "old_id": old_id, "new_id": None, "via": None,
                        })

            await db.commit()
    finally:
        await emby.close()

    log.info("library.repair_emby_ids", **stats)
    return {"stats": stats, "details": details[:200]}


# ═══════════════════════════════════════════════════════════════════════════

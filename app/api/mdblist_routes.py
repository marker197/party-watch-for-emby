"""Routes extracted from routes.py — mdblist_routes.py."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import QueueItem, Universe, UniverseItem, User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user
from app.api.route_helpers import _first_emby_user_id, _get_mdblist_key, _get_setting, _put_setting

log = structlog.get_logger()

router = APIRouter()




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
        await secure_set("mdblist_api_key", key)
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


# -- Simkl synced list tracking (mirrors MDBList pattern) ------------------

@router.get("/api/simkl-lists/synced")
async def get_simkl_synced(db: AsyncSession = Depends(get_db)):
    """Return Simkl lists that have been imported and are tracked for sync."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    if not raw:
        raw = await _get_setting(db, "simkl_synced_lists", "[]")
        if raw and raw != "[]":
            await r.set("simkl_synced_lists", raw)
    synced = _json.loads(raw) if raw else []
    return {"synced": synced}


@router.post("/api/simkl-lists/track")
async def track_simkl_list(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Add or update a Simkl list in synced tracking after import."""
    import json as _json
    slug = (payload.get("list_slug") or "").strip()
    if not slug:
        raise HTTPException(400, "list_slug required")

    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()
    username = (payload.get("username") or "").strip() or "me"
    matched = payload.get("matched", 0)

    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
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

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.put("/api/simkl-lists/synced/{slug}/auto-sync")
async def toggle_simkl_auto_sync(slug: str, payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Toggle auto-sync for a tracked Simkl list."""
    import json as _json
    enabled = payload.get("enabled", True)
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    synced = _json.loads(raw) if raw else []

    for entry in synced:
        if entry.get("slug") == slug:
            entry["auto_sync"] = enabled
            break
    else:
        raise HTTPException(404, "List not tracked")

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug, "auto_sync": enabled}


@router.delete("/api/simkl-lists/synced/{slug}")
async def remove_simkl_synced(slug: str, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Remove a Simkl list from sync tracking (does NOT delete the Emby playlist)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    synced = _json.loads(raw) if raw else []

    synced = [e for e in synced if e.get("slug") != slug]

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.post("/api/simkl-lists/sync-all")
async def sync_all_simkl_lists(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Re-import all auto-synced Simkl lists."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    if not raw:
        raw = await _get_setting(db, "simkl_synced_lists", "[]")
    synced = _json.loads(raw) if raw else []

    results = []
    for entry in synced:
        if not entry.get("auto_sync", True):
            results.append({"slug": entry["slug"], "status": "skipped", "reason": "auto_sync_off"})
            continue
        try:
            result = await import_simkl_list({
                "list_slug": entry["slug"],
                "playlist_name": entry.get("playlist_name", ""),
                "description": entry.get("description", ""),
                "username": entry.get("username", "me"),
            })
            results.append({"slug": entry["slug"], "status": "ok", "matched": result.get("matched", 0)})
        except Exception as e:
            results.append({"slug": entry["slug"], "status": "error", "message": str(e)[:200]})

    return {"status": "ok", "results": results}


@router.get("/api/simkl-lists/popular")
async def get_simkl_popular_lists():
    """Fetch popular Simkl community lists (public endpoint, no auth needed)."""
    simkl = SimklClient()

    try:
        raw = await simkl.get_popular_lists(limit=25)
    finally:
        await simkl.close()

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


@router.get("/api/simkl-lists/trending")
async def get_simkl_trending_lists():
    """Fetch trending Simkl community lists (public endpoint, no auth needed)."""
    simkl = SimklClient()

    try:
        raw = await simkl.get_trending_lists(limit=25)
    finally:
        await simkl.close()

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


@router.get("/api/simkl-lists/items")
async def get_simkl_list_items_detail(slug: str, username: str = "me"):
    """Fetch items from a Simkl list with in-library/missing status for each item."""
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
        items = await simkl.get_list_items(username, slug)
    finally:
        await simkl.close()

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


@router.get("/api/mdblist/lists/search")
async def search_mdblist_lists(
    q: str = Query(..., min_length=1, max_length=200),
    db: AsyncSession = Depends(get_db),
):
    """Search public MDBList lists by keyword."""
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        raw = await client.search_lists(query=q)
    finally:
        await client.close()

    results = []
    for lst in (raw or []):
        results.append({
            "id": lst.get("id", 0),
            "name": lst.get("name", ""),
            "slug": lst.get("slug", ""),
            "description": lst.get("description") or "",
            "mediatype": lst.get("mediatype", ""),
            "items": lst.get("items", lst.get("item_count", 0)),
            "likes": lst.get("likes", 0),
            "user_name": lst.get("user_name", lst.get("username", "")),
            "dynamic": lst.get("dynamic", False),
        })
    return {"lists": results}


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/mdblist/media-info/{provider}/{media_type}/{media_id}")
async def get_mdblist_media_info(
    provider: str,
    media_type: str,
    media_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get enriched media info from MDBList by provider ID.

    provider: 'imdb', 'tmdb', 'tvdb', 'simkl', 'mdblist'
    media_type: 'movie' or 'show'
    media_id: the provider-specific ID (e.g. 'tt1234567' for imdb)

    Returns ratings from multiple sources, genres, runtime, certification,
    streaming info, and more — all from a single call.
    Results cached in Redis for 24 hours.
    """
    if provider not in ("imdb", "tmdb", "tvdb", "simkl", "mdblist"):
        raise HTTPException(400, f"Unsupported provider: {provider}")
    if media_type not in ("movie", "show"):
        raise HTTPException(400, f"Unsupported media type: {media_type}")

    import json as _j
    from app.utils.redis_cache import get_redis

    cache_key = f"mdblist_media_info:{provider}:{media_type}:{media_id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _j.loads(cached)
    except Exception:
        pass

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        data = await client.get_media_info(provider, media_type, media_id)
    finally:
        await client.close()

    if not data:
        raise HTTPException(404, "Item not found on MDBList")

    # Cache for 24 hours
    try:
        r = await get_redis()
        await r.set(cache_key, _j.dumps(data), ex=86400)
    except Exception:
        pass

    return data


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/mdblist/collection/sync")
async def sync_mdblist_collection(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Push current Emby library contents to MDBList's collection.

    Fetches the full library from LibraryCache, compares against MDBList's
    current collection, and adds/removes differences.  This tells MDBList
    which items you own, improving its recommendations.

    Results cached; safe to call repeatedly — only diffs are sent.
    """
    import json as _j
    from app.utils.library_cache import LibraryCache
    from app.utils.redis_cache import get_redis

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        # 1. Get current library from cache
        library = await LibraryCache.get_all_items()
        if not library:
            raise HTTPException(400, "Library cache is empty — rebuild cache first")

        # Build sets of provider IDs from library
        lib_movies: list[dict] = []
        lib_shows: list[dict] = []
        # Use a composite dedup key: prefer IMDB, fall back to TMDB
        lib_keys: set[str] = set()

        for item in library:
            imdb = item.get("imdb_id")
            tmdb = item.get("tmdb_id")
            tvdb = item.get("tvdb_id")
            if not imdb and not tmdb and not tvdb:
                continue

            ids: dict = {}
            if imdb:
                ids["imdb"] = imdb
            if tmdb:
                ids["tmdb"] = int(tmdb)
            if tvdb:
                ids["tvdb"] = int(tvdb)

            # Dedup key: IMDB preferred, then TMDB
            key = f"imdb:{imdb}" if imdb else f"tmdb:{tmdb}" if tmdb else f"tvdb:{tvdb}"
            lib_keys.add(key)

            entry = {"ids": ids, "_key": key}
            if item.get("item_type") == "movie":
                lib_movies.append(entry)
            else:
                lib_shows.append(entry)

        # 2. Get current MDBList collection
        mdb_collection = await client.get_collection()
        mdb_movies = mdb_collection.get("movies", []) if isinstance(mdb_collection, dict) else []
        mdb_shows = mdb_collection.get("shows", []) if isinstance(mdb_collection, dict) else []

        mdb_keys: set[str] = set()
        for entry in mdb_movies + mdb_shows:
            item = entry.get("movie") or entry.get("show") or entry
            eids = item.get("ids") or {}
            if eids.get("imdb"):
                mdb_keys.add(f"imdb:{eids['imdb']}")
            elif eids.get("tmdb"):
                mdb_keys.add(f"tmdb:{eids['tmdb']}")
            elif eids.get("tvdb"):
                mdb_keys.add(f"tvdb:{eids['tvdb']}")

        # 3. Calculate diffs
        to_add_keys = lib_keys - mdb_keys
        to_remove_keys = mdb_keys - lib_keys

        add_movies = [{"ids": m["ids"]} for m in lib_movies if m["_key"] in to_add_keys]
        add_shows = [{"ids": s["ids"]} for s in lib_shows if s["_key"] in to_add_keys]

        # Build removal items from the keys
        rm_items = []
        for k in to_remove_keys:
            prov, val = k.split(":", 1)
            rm_items.append({"ids": {prov: int(val) if prov != "imdb" else val}})

        stats = {
            "library_movies": len(lib_movies),
            "library_shows": len(lib_shows),
            "mdblist_existing": len(mdb_keys),
            "to_add": len(to_add_keys),
            "to_remove": len(to_remove_keys),
        }

        # 4. Push diffs
        if add_movies or add_shows:
            await client.add_to_collection(
                movies=add_movies or None,
                shows=add_shows or None,
            )
        if rm_items:
            await client.remove_from_collection(movies=rm_items or None)

        log.info("mdblist.collection_sync", **stats)

        # Record last sync time
        try:
            r = await get_redis()
            from datetime import datetime, timezone
            await r.set("mdblist_collection_last_sync",
                        datetime.now(timezone.utc).isoformat())
        except Exception:
            pass

        return {"status": "ok", **stats}
    finally:
        await client.close()


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/mdblist/publish/universe/{universe_id}")
async def publish_universe_to_mdblist(
    universe_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Publish a universe as a static MDBList list.

    Creates or updates the list with items from the universe.
    Stores the MDBList list ID in Redis so future calls update rather than
    duplicate.  List name follows the pattern "🌌 {Universe Name}".
    """
    import json as _j
    from app.utils.redis_cache import get_redis

    universe = (await db.execute(
        select(Universe).where(Universe.id == universe_id)
    )).scalar_one_or_none()
    if not universe:
        raise HTTPException(404, "Universe not found")

    items = (await db.execute(
        select(UniverseItem).where(UniverseItem.universe_id == universe_id)
        .order_by(UniverseItem.release_order)
    )).scalars().all()

    if not items:
        raise HTTPException(400, "Universe has no items")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    display_name = universe.custom_name or universe.name
    list_name = f"🌌 {display_name}"

    try:
        r = await get_redis()

        # Check if we already have a list ID for this universe
        redis_key = f"mdblist_published_list:universe:{universe_id}"
        existing_list_id = await r.get(redis_key)

        list_id = int(existing_list_id) if existing_list_id else None

        # Determine mediatype from items
        has_movies = any(i.item_type == "movie" for i in items)
        has_shows = any(i.item_type == "show" for i in items)
        mediatype = "movie" if has_movies and not has_shows else "show" if has_shows and not has_movies else "movie"

        if not list_id:
            # Create new list
            result = await client.create_static_list(
                name=list_name, mediatype=mediatype, private=False,
            )
            list_id = result.get("id")
            if not list_id:
                raise HTTPException(500, f"Failed to create list: {result}")
            await r.set(redis_key, str(list_id))
            log.info("mdblist.publish_universe.created", universe_id=universe_id, list_id=list_id)

        # Build item list for the static list
        mdb_items = []
        for item in items:
            entry: dict = {}
            if item.imdb_id:
                entry["imdb"] = item.imdb_id
            elif item.tmdb_id:
                entry["tmdb"] = int(item.tmdb_id)
            if entry:
                mdb_items.append(entry)

        if not mdb_items:
            return {"status": "ok", "list_id": list_id, "items": 0, "note": "No items with provider IDs"}

        # Clear existing items and re-add (full sync)
        try:
            existing_items = await client.get_all_list_items(list_id)
            if existing_items:
                rm_items = []
                for ei in existing_items:
                    inner = ei.get("movie") or ei.get("show") or ei
                    ids = inner.get("ids", {})
                    rm_entry: dict = {}
                    if ids.get("imdb"):
                        rm_entry["imdb"] = ids["imdb"]
                    elif ids.get("tmdb"):
                        rm_entry["tmdb"] = ids["tmdb"]
                    if rm_entry:
                        rm_items.append(rm_entry)
                if rm_items:
                    await client.modify_static_list_items(list_id, "remove", rm_items)
        except Exception:
            pass  # list might be empty or new

        await client.modify_static_list_items(list_id, "add", mdb_items)

        # Update description with completion stats
        in_lib = sum(1 for i in items if i.in_library)
        watched = sum(1 for i in items if i.watched)
        desc = f"{display_name} — {in_lib}/{len(items)} in library, {watched}/{len(items)} watched"
        try:
            await client.update_list(list_id, name=list_name)
        except Exception:
            pass

        log.info("mdblist.publish_universe.synced",
                 universe_id=universe_id, list_id=list_id, items=len(mdb_items))

        return {"status": "ok", "list_id": list_id, "items": len(mdb_items)}
    finally:
        await client.close()


@router.post("/api/mdblist/publish/smart-queue/{user_id}")
async def publish_smart_queue_to_mdblist(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Publish the current Smart Queue as a static MDBList list.

    Creates or updates a list named "📋 Smart Queue". Items are the current
    top-20 queue recommendations, refreshed on each call.
    """
    import json as _j
    from app.utils.redis_cache import get_redis

    queue_items = (await db.execute(
        select(QueueItem).where(
            QueueItem.user_id == user_id,
            QueueItem.played_at.is_(None),
        ).order_by(QueueItem.score.desc()).limit(20)
    )).scalars().all()

    if not queue_items:
        raise HTTPException(400, "Smart Queue is empty")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        r = await get_redis()
        redis_key = f"mdblist_published_list:queue:{user_id}"
        existing_list_id = await r.get(redis_key)
        list_id = int(existing_list_id) if existing_list_id else None

        if not list_id:
            result = await client.create_static_list(
                name="📋 Smart Queue", mediatype="movie", private=False,
            )
            list_id = result.get("id")
            if not list_id:
                raise HTTPException(500, f"Failed to create list: {result}")
            await r.set(redis_key, str(list_id))

        # Build items
        mdb_items = []
        for qi in queue_items:
            entry: dict = {}
            if qi.imdb_id:
                entry["imdb"] = qi.imdb_id
            elif qi.tmdb_id:
                entry["tmdb"] = int(qi.tmdb_id)
            if entry:
                mdb_items.append(entry)

        # Clear and re-add
        try:
            existing = await client.get_all_list_items(list_id)
            if existing:
                rm_items = []
                for ei in existing:
                    inner = ei.get("movie") or ei.get("show") or ei
                    ids = inner.get("ids", {})
                    rm_entry: dict = {}
                    if ids.get("imdb"):
                        rm_entry["imdb"] = ids["imdb"]
                    elif ids.get("tmdb"):
                        rm_entry["tmdb"] = ids["tmdb"]
                    if rm_entry:
                        rm_items.append(rm_entry)
                if rm_items:
                    await client.modify_static_list_items(list_id, "remove", rm_items)
        except Exception:
            pass

        if mdb_items:
            await client.modify_static_list_items(list_id, "add", mdb_items)

        log.info("mdblist.publish_queue.synced", user_id=user_id, list_id=list_id, items=len(mdb_items))
        return {"status": "ok", "list_id": list_id, "items": len(mdb_items)}
    finally:
        await client.close()


@router.post("/api/mdblist/publish/scrobble-misses/{user_id}")
async def publish_scrobble_misses_to_mdblist(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Publish scrobble audit misses as a static MDBList list.

    Creates or updates a list named "⚠ Scrobble Misses" containing
    items found played in Emby but not tracked on Simkl.
    """
    import json as _j
    from app.utils.redis_cache import get_redis, cache_get

    # Read cached audit results
    audit = await cache_get(f"scrobble_audit:{user_id}")
    if not audit:
        raise HTTPException(400, "No scrobble audit data — run an audit first")

    missed_movies = audit.get("missed_movies", [])
    missed_episodes = audit.get("missed_episodes", [])
    if not missed_movies and not missed_episodes:
        return {"status": "ok", "items": 0, "note": "No misses to publish"}

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        r = await get_redis()
        redis_key = f"mdblist_published_list:scrobble_misses:{user_id}"
        existing_list_id = await r.get(redis_key)
        list_id = int(existing_list_id) if existing_list_id else None

        if not list_id:
            result = await client.create_static_list(
                name="⚠ Scrobble Misses", mediatype="movie", private=True,
            )
            list_id = result.get("id")
            if not list_id:
                raise HTTPException(500, f"Failed to create list: {result}")
            await r.set(redis_key, str(list_id))

        # Build items from misses
        mdb_items = []
        seen: set[str] = set()
        for m in missed_movies:
            imdb = m.get("imdb_id")
            tmdb = m.get("tmdb_id")
            dedup = imdb or tmdb or ""
            if dedup and dedup not in seen:
                seen.add(dedup)
                entry: dict = {}
                if imdb:
                    entry["imdb"] = imdb
                elif tmdb:
                    entry["tmdb"] = int(tmdb)
                if entry:
                    mdb_items.append(entry)

        for ep in missed_episodes:
            imdb = ep.get("imdb_id") or ep.get("series_imdb")
            if imdb and imdb not in seen:
                seen.add(imdb)
                mdb_items.append({"imdb": imdb})

        # Clear and re-add
        try:
            existing = await client.get_all_list_items(list_id)
            if existing:
                rm_items = []
                for ei in existing:
                    inner = ei.get("movie") or ei.get("show") or ei
                    ids = inner.get("ids", {})
                    rm_entry: dict = {}
                    if ids.get("imdb"):
                        rm_entry["imdb"] = ids["imdb"]
                    elif ids.get("tmdb"):
                        rm_entry["tmdb"] = ids["tmdb"]
                    if rm_entry:
                        rm_items.append(rm_entry)
                if rm_items:
                    await client.modify_static_list_items(list_id, "remove", rm_items)
        except Exception:
            pass

        if mdb_items:
            await client.modify_static_list_items(list_id, "add", mdb_items)

        log.info("mdblist.publish_scrobble_misses.synced",
                 user_id=user_id, list_id=list_id, items=len(mdb_items))
        return {"status": "ok", "list_id": list_id, "items": len(mdb_items)}
    finally:
        await client.close()


# ═══════════════════════════════════════════════════════════════════════════

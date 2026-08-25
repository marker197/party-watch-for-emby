"""Routes extracted from routes.py — universe_routes.py."""

from __future__ import annotations

import asyncio
import re

import structlog
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models.schema import Universe, UniverseItem, User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.security.auth import get_current_user
from app.api.route_helpers import _get_setting, _put_setting
from app.services.universe_discovery.service import UniverseDiscoveryService

log = structlog.get_logger()

router = APIRouter()

universe_svc = UniverseDiscoveryService()


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

    Payload: {"playlist_enabled": bool, "custom_name": str|null, "description": str|null, "quality_pref": "hd"|"4k"|null}
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

    # Quality preference stored in Redis (no migration needed)
    if "quality_pref" in payload:
        from app.utils.redis_cache import get_redis
        r = await get_redis()
        qp = payload["quality_pref"]
        if qp in ("hd", "4k"):
            await r.set(f"universe:{universe_id}:quality_pref", qp)
        else:
            await r.delete(f"universe:{universe_id}:quality_pref")

    # Read back quality_pref
    from app.utils.redis_cache import get_redis
    r = await get_redis()
    qp_val = await r.get(f"universe:{universe_id}:quality_pref")

    return {
        "status": "ok",
        "playlist_enabled": bool(universe.playlist_enabled),
        "custom_name": universe.custom_name,
        "description": universe.description,
        "quality_pref": qp_val or None,
    }


# -- Universe artwork management -------------------------------------------

@router.get("/api/universes/artwork")
async def list_universe_artwork(_user: User = Depends(get_current_user)):
    """Return artwork status for all universes.

    Looks up the Emby playlist by name for each universe and returns
    whether it has a primary image set.
    """
    from app.utils.emby_client import EmbyClient
    from app.utils.redis_cache import get_redis

    universes = await universe_svc.get_universes()
    r = await get_redis()
    emby = None
    try:
        emby = EmbyClient()

        # Fetch all playlists once
        playlists = await emby.get_items(item_type="Playlist", recursive=True)
        playlist_map = {}
        for p in playlists.get("Items", []):
            playlist_map[p.get("Name", "")] = p

        result = []
        for u in universes:
            display_name = u.get("custom_name") or u["name"]
            playlist_name = f"🌌 {display_name}"
            playlist = playlist_map.get(playlist_name)
            emby_id = playlist.get("Id") if playlist else None
            has_image = bool(
                playlist and playlist.get("ImageTags", {}).get("Primary")
            ) if playlist else False

            # Check Redis for custom artwork flag
            has_custom = False
            if r:
                try:
                    has_custom = bool(await r.get(f"universe:{u['id']}:custom_artwork"))
                except Exception:
                    pass

            result.append({
                "id": u["id"],
                "name": u["name"],
                "display_name": display_name,
                "total_items": u["total_items"],
                "in_library": u["in_library"],
                "emby_playlist_id": emby_id,
                "has_image": has_image,
                "has_custom_artwork": has_custom,
            })

        return result
    finally:
        if emby:
            await emby.close()


@router.post("/api/universes/{universe_id}/artwork")
async def upload_universe_artwork(
    universe_id: int,
    file: UploadFile = File(...),
    _user: User = Depends(get_current_user),
):
    """Upload custom artwork for a universe's Emby playlist.

    Accepts .png or .jpg, max 5 MB.  Looks up the playlist by name
    and pushes the image via Emby's image API.
    """
    from app.utils.emby_client import EmbyClient
    from app.utils.redis_cache import get_redis

    # Validate file type
    ct = file.content_type or ""
    if ct not in ("image/png", "image/jpeg", "image/jpg"):
        raise HTTPException(400, "Only .png and .jpg files are accepted")

    # Read and validate size (5 MB max)
    image_bytes = await file.read()
    if len(image_bytes) > 5 * 1024 * 1024:
        raise HTTPException(413, "Image must be under 5 MB")
    if len(image_bytes) < 100:
        raise HTTPException(400, "File appears to be empty")

    # Look up universe
    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            raise HTTPException(404, "Universe not found")
        display_name = universe.custom_name or universe.name

    # Find the Emby playlist
    emby = None
    try:
        emby = EmbyClient()
        playlist_name = f"🌌 {display_name}"
        playlists = await emby.get_items(item_type="Playlist", recursive=True)
        playlist_id = None
        for p in playlists.get("Items", []):
            if p.get("Name") == playlist_name:
                playlist_id = p["Id"]
                break

        if not playlist_id:
            raise HTTPException(
                404,
                f"No Emby playlist found for '{display_name}'. "
                "Enable playlist sync and run a scan first."
            )

        # Upload image
        content_type = "image/png" if ct == "image/png" else "image/jpeg"
        ok = await emby.set_item_image(playlist_id, image_bytes,
                                        content_type=content_type)
        if not ok:
            raise HTTPException(500, "Failed to upload image to Emby")

        # Mark as custom artwork in Redis + invalidate thumbnail cache
        r = await get_redis()
        await r.set(f"universe:{universe_id}:custom_artwork", "1")
        await r.delete(f"universe_artwork_thumb:{universe_id}")

        return {"status": "ok", "emby_playlist_id": playlist_id}
    finally:
        if emby:
            await emby.close()


@router.delete("/api/universes/{universe_id}/artwork")
async def delete_universe_artwork(
    universe_id: int,
    _user: User = Depends(get_current_user),
):
    """Remove custom artwork from a universe's Emby playlist."""
    from app.utils.emby_client import EmbyClient
    from app.utils.redis_cache import get_redis

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            raise HTTPException(404, "Universe not found")
        display_name = universe.custom_name or universe.name

    emby = None
    try:
        emby = EmbyClient()
        playlist_name = f"🌌 {display_name}"
        playlists = await emby.get_items(item_type="Playlist", recursive=True)
        playlist_id = None
        for p in playlists.get("Items", []):
            if p.get("Name") == playlist_name:
                playlist_id = p["Id"]
                break

        if not playlist_id:
            raise HTTPException(404, "No Emby playlist found")

        ok = await emby.delete_item_image(playlist_id)
        if not ok:
            raise HTTPException(500, "Failed to remove image from Emby")

        r = await get_redis()
        await r.delete(f"universe:{universe_id}:custom_artwork")
        await r.delete(f"universe_artwork_thumb:{universe_id}")

        return {"status": "ok"}
    finally:
        if emby:
            await emby.close()


async def _fetch_artwork_thumbnail(
    playlist_id: str, universe_id: int,
) -> tuple[bytes, str] | None:
    """Fetch artwork thumbnail from Redis cache or Emby.

    Returns (image_bytes, content_type) or None.
    Caches in Redis with 1-hour TTL to avoid hammering Emby.
    """
    import base64 as _b64
    import httpx
    from app.config import settings
    from app.utils.redis_cache import get_redis

    cache_key = f"universe_artwork_thumb:{universe_id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            data = _json.loads(cached)
            return _b64.b64decode(data["b64"]), data["ct"]
    except Exception:
        pass

    # Fetch from Emby
    img_url = f"{settings.emby_url}/Items/{playlist_id}/Images/Primary"
    params = {"api_key": settings.emby_api_key, "maxWidth": "300"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(img_url, params=params)
            if resp.status_code != 200:
                return None
            ct = resp.headers.get("content-type", "image/png")
            img_bytes = resp.content

        # Cache for 1 hour
        try:
            r = await get_redis()
            payload = _json.dumps({"b64": _b64.b64encode(img_bytes).decode(), "ct": ct})
            await r.set(cache_key, payload, ex=3600)
        except Exception:
            pass

        return img_bytes, ct
    except Exception:
        return None


@router.get("/api/universes/{universe_id}/artwork/preview")
async def preview_universe_artwork(universe_id: int):
    """Proxy the Emby playlist's primary image, with Redis thumbnail cache."""
    from app.utils.emby_client import EmbyClient

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            raise HTTPException(404, "Universe not found")
        display_name = universe.custom_name or universe.name

    emby = None
    try:
        emby = EmbyClient()
        playlist_name = f"🌌 {display_name}"
        playlists = await emby.get_items(item_type="Playlist", recursive=True)
        playlist_id = None
        for p in playlists.get("Items", []):
            if p.get("Name") == playlist_name:
                playlist_id = p["Id"]
                break

        if not playlist_id:
            raise HTTPException(404, "No playlist found")

        result = await _fetch_artwork_thumbnail(playlist_id, universe_id)
        if not result:
            raise HTTPException(404, "No image found")
        return Response(content=result[0], media_type=result[1])
    finally:
        if emby:
            await emby.close()


@router.post("/api/universes/artwork/previews")
async def batch_artwork_previews(
    request: Request,
    _user: User = Depends(get_current_user),
):
    """Batch-fetch artwork thumbnails for multiple universes in one call.

    Request body: {"universe_ids": [1, 2, 3]}
    Response: {"previews": {"1": {"b64": "...", "ct": "image/jpeg"}, ...}}

    Uses Redis thumbnail cache (1h TTL). Only hits Emby for cache misses.
    """
    import base64 as _b64
    from app.utils.emby_client import EmbyClient

    body = await request.json()
    universe_ids = body.get("universe_ids", [])
    if not universe_ids or not isinstance(universe_ids, list):
        return {"previews": {}}

    # Look up universe names
    async with async_session_ctx() as db:
        rows = (await db.execute(
            select(Universe).where(Universe.id.in_(universe_ids))
        )).scalars().all()
        id_to_name = {
            u.id: u.custom_name or u.name for u in rows
        }

    emby = None
    try:
        emby = EmbyClient()
        playlists = await emby.get_items(item_type="Playlist", recursive=True)
        playlist_map: dict[str, str] = {}
        for p in playlists.get("Items", []):
            playlist_map[p.get("Name", "")] = p.get("Id", "")

        previews: dict[str, dict] = {}
        for uid in universe_ids:
            name = id_to_name.get(uid)
            if not name:
                continue
            playlist_id = playlist_map.get(f"🌌 {name}")
            if not playlist_id:
                continue
            result = await _fetch_artwork_thumbnail(playlist_id, uid)
            if result:
                previews[str(uid)] = {
                    "b64": _b64.b64encode(result[0]).decode(),
                    "ct": result[1],
                }

        return {"previews": previews}
    finally:
        if emby:
            await emby.close()


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
                        "simkl_id": item.simkl_id,
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
                    simkl_id=item_data.get("simkl_id"),
                    imdb_id=item_data.get("imdb_id"),
                    tmdb_id=item_data.get("tmdb_id"),
                    in_library=False,
                    watched=False,
                ))

            created += 1

        await db.commit()

    return {"status": "ok", "created": created, "skipped": skipped}


@router.post("/api/universes/{universe_id}/save-order")
async def save_universe_order(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Persist item order to DB without creating an Emby playlist.

    Payload: {"item_ids": [db_item_id_1, db_item_id_2, ...]}
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

        items = (await db.execute(
            select(UniverseItem).where(UniverseItem.universe_id == universe_id)
        )).scalars().all()
        id_to_item = {i.id: i for i in items}
        updated = 0
        for pos, item_id in enumerate(item_ids):
            item_id_int = int(item_id) if not isinstance(item_id, int) else item_id
            if item_id_int in id_to_item:
                id_to_item[item_id_int].release_order = pos + 1
                updated += 1

        await db.commit()

    log.info("universe.order_saved", universe_id=universe_id, items=updated)
    return {"status": "ok", "items": updated}


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

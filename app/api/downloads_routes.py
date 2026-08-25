"""Routes extracted from routes.py — downloads_routes.py."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, Request

from app.models.schema import User
from app.utils.database import async_session as async_session_ctx
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.security.auth import get_current_user
from app.api.route_helpers import _put_setting

log = structlog.get_logger()

router = APIRouter()



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
    raw_radarr = await secure_get("radarr_servers")
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
    raw_sonarr = await secure_get("sonarr_servers")
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
    raw_sab = await secure_get("sabnzbd_servers")
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


@router.get("/api/download-progress")
async def get_download_progress():
    """Lightweight SABnzbd-only progress snapshot.

    Returns only nzo_id-keyed progress data — no Radarr/Sonarr calls.
    Designed to be polled at 500ms for realtime progress bar updates.
    No caching — SABnzbd on LAN responds in <50ms.
    """
    import json as _json
    r = await get_redis()
    raw_sab = await secure_get("sabnzbd_servers")
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


@router.get("/api/sabnzbd/servers")
async def get_sabnzbd_servers():
    """Read configured SABnzbd servers from Redis."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("sabnzbd_servers")
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
    raw_existing = await secure_get("sabnzbd_servers")
    existing = _json.loads(raw_existing) if raw_existing else []

    for i, srv in enumerate(servers):
        key = srv.get("api_key", "")
        if "****" in key and i < len(existing):
            srv["api_key"] = existing[i].get("api_key", key)

    encoded = _json.dumps(servers)
    await secure_set("sabnzbd_servers", encoded)

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
        raw_existing = await secure_get("sabnzbd_servers")
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

"""Routes extracted from routes.py — arr_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.security.auth import get_current_user
from app.api.route_helpers import _get_setting, _is_masked, _mask_api_key, _put_setting, _resolve_servers

log = structlog.get_logger()

router = APIRouter()



@router.get("/api/radarr/servers")
async def get_radarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Radarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("radarr_servers")
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
    await secure_set("radarr_servers", encoded)
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
    raw = await secure_get("radarr_servers")
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

    # Track manually-sent items so watchlist sync excludes them
    if added:
        exclude_ids = []
        for movie, res in zip(movies, results):
            if res.get("status") == "ok" and movie.get("tmdb_id"):
                exclude_ids.append(str(movie["tmdb_id"]))
        if exclude_ids:
            r = await get_redis()
            await r.sadd("manual_arr_exclude:tmdb", *exclude_ids)

    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(movies),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/sonarr/servers")
async def get_sonarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Sonarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("sonarr_servers")
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
    await secure_set("sonarr_servers", encoded)
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
    raw = await secure_get("sonarr_servers")
    if not raw:
        raise HTTPException(400, "No Sonarr servers configured — add one in Settings")
    servers = _json.loads(raw)
    if server_idx >= len(servers):
        raise HTTPException(400, f"Server index {server_idx} out of range")

    srv = servers[server_idx]
    client = SonarrClient(srv["url"], srv["api_key"], name=srv["name"])
    profile_id = srv.get("quality_profile_id")

    from app.utils.tmdb_client import get_tv_external_ids

    results = []
    for show in shows:
        tvdb_id = show.get("tvdb_id")
        imdb_id = show.get("imdb_id")

        # Filmography sends carry only a TMDB ID for shows that aren't in
        # the library.  Sonarr keys on TVDB, and without one add_series
        # falls back to a title search that blind-picks the first result.
        # Resolve TMDB -> TVDB first so the match is exact.
        if not tvdb_id and show.get("tmdb_id"):
            ext = await get_tv_external_ids(int(show["tmdb_id"]))
            if ext:
                if ext.get("tvdb_id"):
                    tvdb_id = ext["tvdb_id"]
                    log.info("sonarr.tvdb_resolved_from_tmdb",
                             tmdb_id=show["tmdb_id"], tvdb_id=tvdb_id,
                             title=show.get("title"))
                if not imdb_id and ext.get("imdb_id"):
                    imdb_id = ext["imdb_id"]

        if not tvdb_id:
            log.info("sonarr.no_tvdb_using_title_lookup",
                     title=show.get("title"), year=show.get("year"))

        result = await client.add_series(
            tvdb_id=tvdb_id,
            imdb_id=imdb_id,
            title=show.get("title", ""),
            year=show.get("year"),
            quality_profile_id=profile_id,
        )
        # Surface how the series was matched so the UI can warn on a
        # title-only match, which is the fallible path
        result["matched_by"] = (
            "tvdb" if tvdb_id else ("title" if result.get("status") == "ok" else "none")
        )
        results.append(result)

    await client.close()

    added = sum(1 for r in results if r.get("status") == "ok")

    # Track manually-sent items so watchlist sync excludes them
    if added:
        exclude_tmdb = []
        exclude_tvdb = []
        for show, res in zip(shows, results):
            if res.get("status") == "ok":
                if show.get("tmdb_id"):
                    exclude_tmdb.append(str(show["tmdb_id"]))
                if show.get("tvdb_id"):
                    exclude_tvdb.append(str(show["tvdb_id"]))
        r = await get_redis()
        if exclude_tmdb:
            await r.sadd("manual_arr_exclude:tmdb", *exclude_tmdb)
        if exclude_tvdb:
            await r.sadd("manual_arr_exclude:tvdb", *exclude_tvdb)

    # Persist matched_by provenance so filmography UI can warn on
    # title-matched items even after page navigation
    try:
        r = await get_redis()
        for show, res in zip(shows, results):
            if res.get("status") == "ok" and res.get("matched_by") == "title":
                _prov_title = show.get("title") or res.get("title") or ""
                if _prov_title:
                    await r.hset(
                        "sonarr:matched_by_title",
                        _prov_title.lower().strip(),
                        _prov_title,
                    )
    except Exception:
        pass  # best-effort — UI hint only

    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(shows),
        "results": results,
    }


@router.get("/api/sonarr/title-matched")
async def get_title_matched_sonarr(
    _user: User = Depends(get_current_user),
):
    """Return list of shows that were sent to Sonarr via title match only.

    These should be verified in Sonarr since the matched series may not
    be the correct one.
    """
    try:
        r = await get_redis()
        items = await r.hgetall("sonarr:matched_by_title")
        # items is {b"key": b"val"} — decode
        return {
            "items": [
                {"key": k.decode() if isinstance(k, bytes) else k,
                 "title": v.decode() if isinstance(v, bytes) else v}
                for k, v in items.items()
            ],
            "total": len(items),
        }
    except Exception:
        return {"items": [], "total": 0}


@router.delete("/api/sonarr/title-matched")
async def clear_title_matched_sonarr(
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Remove a show from the title-matched warning list after verification.

    Payload: {title: "show name"}
    """
    title = (payload.get("title") or "").lower().strip()
    if not title:
        raise HTTPException(400, "title is required")
    try:
        r = await get_redis()
        await r.hdel("sonarr:matched_by_title", title)
    except Exception:
        pass
    return {"status": "ok"}


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
    raw_radarr = await secure_get("radarr_servers")
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
    raw_sonarr = await secure_get("sonarr_servers")
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


@router.post("/api/watchlist-sync/run")
async def run_watchlist_sync(
    current_user: User = Depends(get_current_user),
):
    """Manually trigger a Radarr/Sonarr ↔ Simkl watchlist sync."""
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
    raw_radarr = await secure_get("radarr_servers")
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
    raw_sonarr = await secure_get("sonarr_servers")
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
    raw_radarr = await secure_get("radarr_servers")
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
    raw_sonarr = await secure_get("sonarr_servers")
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

    # Add new arrivals with timestamp (dedup by id, skip dismissed)
    existing_ids = {(i.get("type"), str(i.get("id", ""))) for i in existing_items}
    # Load dismissed items
    dismissed_key = "recently_arrived_dismissed_v1"
    try:
        raw_dismissed = await r.get(dismissed_key)
        dismissed_set = set(_json.loads(raw_dismissed)) if raw_dismissed else set()
    except Exception:
        dismissed_set = set()
    new_arrival_names: list[str] = []
    for m in arrived_movies:
        key = ("movie", str(m.get("tmdb_id", "")))
        dismiss_key_str = f"movie:{m.get('tmdb_id', '')}"
        if key not in existing_ids and dismiss_key_str not in dismissed_set:
            existing_items.append({**m, "id": m.get("tmdb_id"), "arrived_at": now_ts})
            new_arrival_names.append(m.get("title", "Unknown movie"))
    for s in arrived_shows:
        key = ("show", str(s.get("tvdb_id", "")))
        dismiss_key_str = f"show:{s.get('tvdb_id', '')}"
        if key not in existing_ids and dismiss_key_str not in dismissed_set:
            existing_items.append({**s, "id": s.get("tvdb_id"), "arrived_at": now_ts})
            new_arrival_names.append(s.get("title", "Unknown show"))

    # New arrivals tracked but NOT notified here — the Emby webhook
    # item_added handler sends the notification in real-time instead.

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
    """Remove a single item from the recently arrived list and persist the dismissal."""
    import json as _json
    body = await request.json()
    item_type = body.get("type")  # "movie" or "show"
    item_id = str(body.get("id", ""))

    r = await get_redis()
    arrived_key = "recently_arrived_items_v1"
    dismissed_key = "recently_arrived_dismissed_v1"
    try:
        # Remove from arrived list
        raw = await r.get(arrived_key)
        items = _json.loads(raw) if raw else []
        items = [i for i in items if not (i.get("type") == item_type and str(i.get("id", "")) == item_id)]
        await r.setex(arrived_key, 86400 * 2, _json.dumps(items))
        # Persist dismissal (30 day TTL, same as pending snapshot)
        raw_dismissed = await r.get(dismissed_key)
        dismissed = _json.loads(raw_dismissed) if raw_dismissed else []
        dismiss_entry = f"{item_type}:{item_id}"
        if dismiss_entry not in dismissed:
            dismissed.append(dismiss_entry)
        await r.setex(dismissed_key, 86400 * 30, _json.dumps(dismissed))
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
        # Some Emby clients (macOS, web) ignore StartPositionTicks on
        # the initial PlayNow command.  A follow-up Seek after a short
        # delay forces them to jump to the correct position, then
        # Unpause ensures playback continues automatically.
        if start_position_ticks:
            import asyncio
            await asyncio.sleep(1.5)
            try:
                await emby.send_play_state_command(
                    session_id, "Seek", seek_ticks=int(start_position_ticks),
                )
                await asyncio.sleep(0.3)
                await emby.send_play_state_command(session_id, "Unpause")
            except Exception:
                pass  # best-effort — play already started
        return {"status": "playing", "emby_id": emby_item_id}
    except Exception as e:
        log.warning("play_on_session.failed", error=str(e)[:200])
        return {"status": "error", "message": f"Play failed: {str(e)[:100]}"}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════

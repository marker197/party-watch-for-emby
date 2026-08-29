"""Routes extracted from routes.py — settings_routes.py."""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, File, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import AppSetting, User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user
from app.api.route_helpers import MASKED_SUFFIX, _check_ssl_cert, _first_emby_user_id, _get_setting, _is_masked, _mask_api_key, _put_setting, _resolve_servers, record_job_run

log = structlog.get_logger()

router = APIRouter()



class SettingsRequest(BaseModel):
    simkl_client_id: str = None
    simkl_client_secret: str = None
    emby_url: str = None
    emby_api_key: str = None
    cron_smart_queue: str = None
    cron_ml_retrain: str = None
    cron_universe_scan: str = None
    features: dict = None

class TestConnectionRequest(BaseModel):
    service: str
    client_id: str | None = None
    client_secret: str | None = None
    url: str | None = None
    api_key: str | None = None


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


@router.get("/api/job-history")
async def job_history(
    job_id: str = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    """Return recent job run history from Postgres."""
    from app.models.schema import JobRun
    from app.utils.database import async_session as _async_session
    async with _async_session() as db:
        q = select(JobRun).order_by(JobRun.started_at.desc()).limit(limit)
        if job_id:
            q = q.where(JobRun.job_id == job_id)
        rows = (await db.execute(q)).scalars().all()
    return [
        {
            "id": r.id,
            "job_id": r.job_id,
            "status": r.status,
            "started_at": r.started_at.strftime("%Y-%m-%d %H:%M:%S") if r.started_at else None,
            "duration_s": r.duration_s,
            "error": r.error,
        }
        for r in rows
    ]


@router.delete("/api/job-history")
async def clear_job_history(_user: User = Depends(get_current_user)):
    """Delete all job run history."""
    from app.models.schema import JobRun
    from app.utils.database import async_session as _async_session
    async with _async_session() as db:
        await db.execute(delete(JobRun))
        await db.commit()
    return {"status": "ok"}


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


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/cache/rebuild")
async def rebuild_cache(_user: User = Depends(get_current_user)):
    """Manually trigger library cache rebuild."""
    import time as _time
    t = _time.time()
    try:
        async with EmbyClient() as emby:
            uid = await _first_emby_user_id()
            summary = await LibraryCache.index_library(emby, user_id=uid)
        await record_job_run("library_cache_rebuild", "ok", _time.time() - t)
        return {"status": "rebuilt", **summary}
    except Exception as e:
        await record_job_run("library_cache_rebuild", "error", _time.time() - t, str(e)[:200])
        raise


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


@router.get("/job-history", response_class=HTMLResponse)
async def get_job_history_page():
    """Serve the job run history page."""
    try:
        with open("frontend/templates/job_history.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/watch-party", response_class=HTMLResponse)
async def get_watch_party_page(code: str = None):
    """Serve the watch party chat page."""
    try:
        with open("frontend/templates/watch_party.html", "r") as f:
            html = f.read()
        # Inject party code if provided (sanitised: alphanumeric only)
        if code:
            safe_code = re.sub(r"[^A-Za-z0-9]", "", code)[:12]
            if safe_code:
                html = html.replace("const partyCode = null;", f"const partyCode = '{safe_code}';")
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


def _parse_db_url(url: str) -> tuple[str, str, str, str]:
    """Parse DATABASE_URL into (user, password, host, dbname).

    Handles: postgresql+asyncpg://user:pass@host:port/dbname
    """
    from urllib.parse import urlparse, unquote
    parsed = urlparse(url)
    user = unquote(parsed.username or "embysimkl")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "postgres"
    dbname = (parsed.path or "/embysimkl").lstrip("/")
    return user, password, host, dbname

@router.post("/api/db/backup")
async def create_db_backup(_user: User = Depends(get_current_user)):
    """Create a pg_dump backup and return a download token."""
    import subprocess
    import uuid

    backup_dir = "/app/cache/backups"
    os.makedirs(backup_dir, exist_ok=True)
    backup_id = uuid.uuid4().hex[:12]
    filename = f"emby-simkl-backup-{backup_id}.sql"
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

    filepath = f"/app/cache/backups/emby-simkl-backup-{backup_id}.sql"
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


class SettingsRequest(BaseModel):
    simkl_client_id: str = None
    simkl_client_secret: str = None
    emby_url: str = None
    emby_api_key: str = None
    cron_smart_queue: str = None
    cron_ml_retrain: str = None
    cron_universe_scan: str = None
    features: dict = None



@router.get("/api/settings")
async def read_settings(db: AsyncSession = Depends(get_db)):
    """Read current settings — DB overrides, .env fallbacks."""
    return {
        "simkl_client_id": os.getenv("SIMKL_CLIENT_ID", "")[:8] + "****" if os.getenv("SIMKL_CLIENT_ID") else "",
        "simkl_client_secret": os.getenv("SIMKL_CLIENT_SECRET", "")[:8] + "****" if os.getenv("SIMKL_CLIENT_SECRET") else "",
        "emby_url": os.getenv("EMBY_URL", ""),
        "emby_api_key": os.getenv("EMBY_API_KEY", "")[:8] + "****" if os.getenv("EMBY_API_KEY") else "",
        "cron_smart_queue": await _get_setting(db, "cron_smart_queue", os.getenv("SMART_QUEUE_CRON", "0 2 * * *")),
        "cron_ml_retrain": await _get_setting(db, "cron_ml_retrain", os.getenv("ML_RETRAIN_CRON", "0 4 * * 1")),
        "cron_universe_scan": await _get_setting(db, "cron_universe_scan", os.getenv("UNIVERSE_SCAN_CRON", "0 3 * * 0")),
        "features": {
            "smart_queue": (await _get_setting(db, "feature_smart_queue", os.getenv("ENABLE_SMART_QUEUE", "true"))).lower() == "true",
            "ml_predictor": (await _get_setting(db, "feature_ml_predictor", os.getenv("ENABLE_ML_PREDICTOR", "true"))).lower() == "true",
            "universe_discovery": (await _get_setting(db, "feature_universe_discovery", os.getenv("ENABLE_UNIVERSE_DISCOVERY", "true"))).lower() == "true",
            "watch_party": (await _get_setting(db, "feature_watch_party", os.getenv("ENABLE_WATCH_PARTY", "true"))).lower() == "true",
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

    # Feature toggles — persist to DB and update in-memory settings
    if request.features and isinstance(request.features, dict):
        feature_map = {
            "smart_queue": "enable_smart_queue",
            "ml_predictor": "enable_ml_predictor",
            "universe_discovery": "enable_universe_discovery",
            "watch_party": "enable_watch_party",
        }
        for feature_key, config_attr in feature_map.items():
            if feature_key in request.features:
                val = "true" if request.features[feature_key] else "false"
                await _put_setting(db, f"feature_{feature_key}", val)
                # Update in-memory settings object so /health etc. reflect changes
                if hasattr(settings, config_attr):
                    object.__setattr__(settings, config_attr, request.features[feature_key])
                saved.append(f"feature_{feature_key}")

    await db.commit()

    return {
        "status": "ok",
        "saved": saved,
        "message": f"Saved {len(saved)} setting(s).",
    }


class TestConnectionRequest(BaseModel):
    service: str
    client_id: str | None = None
    client_secret: str | None = None
    url: str | None = None
    api_key: str | None = None


@router.post("/api/settings/test-connection")
async def test_connection(body: TestConnectionRequest, _user: User = Depends(get_current_user)):
    """Test Simkl or Emby connection (uses credentials from .env)."""
    service = body.service
    if service == "simkl":
        # Test Simkl API
        simkl = SimklClient()
        try:
            result = await simkl.get_trending(kind="shows")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await simkl.close()
    
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
    """Clear all stored Simkl OAuth tokens (users must re-link)."""
    users = (await db.execute(select(User))).scalars().all()
    for user in users:
        user.simkl_access_token = None
        user.simkl_token_expires = None
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


@router.get("/api/connection-status")
async def connection_status():
    """Return cached heartbeat results for Emby, Simkl, and Radarr."""
    import json as _json
    r = await get_redis()
    result = {}
    for svc in ("emby", "simkl"):
        raw = await r.get(f"heartbeat:{svc}")
        if raw:
            result[svc] = _json.loads(raw)
        else:
            result[svc] = {"status": "unknown", "checked_at": None}
    # Radarr — may have 0, 1, or 2 servers
    radarr_list = []
    raw_servers = await secure_get("radarr_servers")
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
    raw_sonarr = await secure_get("sonarr_servers")
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
    raw_sab = await secure_get("sabnzbd_servers")
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
        mdb_key = await secure_get("mdblist_api_key")
        if mdb_key:
            result["mdblist"] = {"status": "unknown", "checked_at": None}
    # Integration provider
    raw_prov = await r.get("integration_provider")
    result["integration_provider"] = (raw_prov if isinstance(raw_prov, str) else raw_prov.decode()) if raw_prov else "simkl"
    return result


@router.post("/api/connection-status/refresh")
async def refresh_connection_status(_user: User = Depends(get_current_user)):
    """Force an immediate heartbeat check for all services."""
    from app.main import run_heartbeat
    await run_heartbeat()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/notifications/config")
async def get_notification_config(db: AsyncSession = Depends(get_db)):
    """Return notification config. Gotify tokens are masked."""
    import json as _json
    from app.utils.notification_client import DEFAULT_EVENTS, EVENT_TYPES
    raw = await secure_get("notifications_config")
    if not raw:
        raw = await _get_setting(db, "notifications_config", "")
    if not raw:
        return {"services": [], "events": dict(DEFAULT_EVENTS), "event_types": EVENT_TYPES}
    try:
        config = _json.loads(raw)
        for svc in config.get("services", []):
            if svc.get("token"):
                svc["token"] = _mask_api_key(svc["token"])
        config["event_types"] = EVENT_TYPES
        return config
    except Exception:
        return {"services": [], "events": dict(DEFAULT_EVENTS), "event_types": EVENT_TYPES}


@router.put("/api/notifications/config")
async def save_notification_config(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Save notification config (services + event toggles)."""
    import json as _json
    services = payload.get("services", [])[:5]  # max 5 services

    # Resolve masked tokens from existing config
    try:
        existing_raw = await secure_get("notifications_config")
        if existing_raw:
            existing = _json.loads(existing_raw)
            existing_tokens = {}
            for svc in existing.get("services", []):
                if svc.get("url") and svc.get("token"):
                    existing_tokens[svc["url"]] = svc["token"]
            for svc in services:
                token = svc.get("token", "")
                if token and _is_masked(token):
                    real = existing_tokens.get(svc.get("url", ""), "")
                    if real:
                        svc["token"] = real
                    else:
                        svc.pop("token", None)
    except Exception:
        pass

    clean = []
    for svc in services:
        if svc.get("url"):
            clean.append({
                "name": svc.get("name", "Webhook"),
                "type": svc.get("type", "webhook"),
                "url": svc["url"].rstrip("/"),
                "token": svc.get("token", ""),
                "enabled": svc.get("enabled", True),
            })

    events = payload.get("events", {})
    from app.utils.notification_client import DEFAULT_EVENTS
    clean_events = {}
    for key in DEFAULT_EVENTS:
        clean_events[key] = bool(events.get(key, DEFAULT_EVENTS[key]))

    config = {"services": clean, "events": clean_events}
    encoded = _json.dumps(config)
    await secure_set("notifications_config", encoded)
    await _put_setting(db, "notifications_config", encoded)
    await db.commit()
    return {"status": "ok", "services": len(clean)}


@router.post("/api/notifications/test")
async def test_notification(
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Send a test notification to a single service."""
    from app.utils.notification_client import test_service
    svc_type = payload.get("type", "webhook")
    url = payload.get("url", "")
    token = payload.get("token", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # Resolve masked token
    if token and _is_masked(token):
        import json as _json
        try:
            existing_raw = await secure_get("notifications_config")
            if existing_raw:
                existing = _json.loads(existing_raw)
                for svc in existing.get("services", []):
                    if svc.get("url", "").rstrip("/") == url.rstrip("/") and svc.get("token"):
                        token = svc["token"]
                        break
        except Exception:
            pass
    service = {"type": svc_type, "url": url, "token": token, "name": "Test"}
    result = await test_service(service)
    return result


# ═══════════════════════════════════════════════════════════════════════════



@router.get("/api/tmdb/key")
async def get_tmdb_key(db: AsyncSession = Depends(get_db)):
    """Return whether a TMDB API key is configured (never returns the key itself)."""
    r = await get_redis()
    raw = await secure_get("tmdb_api_key")
    if not raw:
        raw = await _get_setting(db, "tmdb_api_key", "")
        if raw:
            await secure_set("tmdb_api_key", raw)
    return {"configured": bool(raw)}


@router.put("/api/tmdb/key")
async def save_tmdb_key(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save or clear the TMDB API key."""
    import json as _json
    key = (payload.get("api_key") or "").strip()
    r = await get_redis()
    if key:
        await secure_set("tmdb_api_key", key)
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

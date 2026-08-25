"""Emby-Simkl Suite — application bootstrap.

Starts:
  - FastAPI REST server on port 8000
  - Socket.IO WebSocket server on same ASGI app
  - APScheduler background tasks (smart queue, ML retrain, universe scan)
  - Database init
"""

from __future__ import annotations

import asyncio
import json as _json
import time
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import socketio
import structlog
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, text as sa_text
from fastapi.responses import HTMLResponse as _HTMLResponse

from app.config import settings
from app.utils.logging import setup_logging, security_log
from app.utils.database import init_db, get_db, async_session
from app.utils.redis_cache import close_redis
from app.utils.secure_redis import secure_get, secure_set, migrate_plaintext_secrets
from app.api.auth_routes import router as auth_router
from app.api.queue_routes import router as queue_router
from app.api.ml_routes import router as ml_router
from app.api.scrobble_audit_routes import router as scrobble_audit_router
from app.api.universe_routes import router as universe_router
from app.api.party_routes import router as party_router
from app.api.webhook_routes import router as webhook_router
from app.api.settings_routes import router as settings_router
from app.api.bias_routes import router as bias_router
from app.api.arr_routes import router as arr_router
from app.api.watch_history_routes import router as watch_history_router
from app.api.downloads_routes import router as downloads_router
from app.api.media_routes import router as media_router
from app.api.mdblist_routes import router as mdblist_router
from app.api.ratings_routes import router as ratings_router
from app.api.import_routes import router as import_router
from app.api.item_detail_routes import router as item_detail_router
from app.api.duplicates_routes import router as duplicates_router
from app.api.library_health_routes import router as library_health_router
from app.api.cross_sync_routes import router as cross_sync_router
from app.api.monitoring_routes import router as monitoring_router
from app.api.phase5_routes import router as phase5_router
from app.services.watch_party.service import sio as watch_party_sio
from app.middleware.rate_limit import limiter
from app.services.monitoring.health import monitor

log = structlog.get_logger()

scheduler = AsyncIOScheduler()


# ---------------------------------------------------------------------------
# Security middleware for audit logging (pure ASGI — does not break sub-app mounts)
# ---------------------------------------------------------------------------

class SecurityAuditMiddleware:
    """Log all HTTP requests for security audit trail.

    Also injects:
      - Security response headers (M3)
      - CSRF double-submit cookie protection (M11)

    Implemented as a pure ASGI middleware instead of BaseHTTPMiddleware
    because BaseHTTPMiddleware breaks Starlette sub-app mounts (the
    Socket.IO ASGIApp mounted at /ws would 404 on every request).
    """

    # Paths exempt from CSRF (webhooks, health, auth device-code polling, image proxy)
    _CSRF_EXEMPT = frozenset({"/health", "/api/auth/poll",
                              "/api/auth/device-code"})
    _CSRF_EXEMPT_PREFIXES = ("/webhook/", "/api/emby/image/", "/api/remote-play")

    # Security headers injected on every HTTP response
    _SECURITY_HEADERS = [
        (b"x-content-type-options", b"nosniff"),
        (b"x-frame-options", b"DENY"),
        (b"x-xss-protection", b"1; mode=block"),
        (b"referrer-policy", b"strict-origin-when-cross-origin"),
        (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
        (b"content-security-policy",
         b"default-src 'self'; "
         b"script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://cdn.socket.io; "
         b"style-src 'self' 'unsafe-inline'; "
         b"img-src 'self' data: blob:; "
         b"connect-src 'self' ws: wss:; "
         b"font-src 'self'; "
         b"frame-ancestors 'none'"),
    ]

    def __init__(self, app):
        self.app = app

    def _is_csrf_exempt(self, path: str) -> bool:
        if path in self._CSRF_EXEMPT:
            return True
        return any(path.startswith(p) for p in self._CSRF_EXEMPT_PREFIXES)

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        path = scope.get("path", "")
        method = scope.get("method", "WEBSOCKET" if scope["type"] == "websocket" else "")
        client = scope.get("client") or ("unknown", 0)
        client_ip = client[0] if client else "unknown"

        # ── CSRF double-submit cookie check ────────────────────────
        # State-changing methods require X-CSRF-Token header to match
        # the csrf_token cookie.  GET/HEAD/OPTIONS are safe methods.
        if (scope["type"] == "http"
                and method in ("POST", "PUT", "DELETE", "PATCH")
                and not self._is_csrf_exempt(path)):
            headers_raw = dict(scope.get("headers", []))
            cookie_header = headers_raw.get(b"cookie", b"").decode()
            csrf_cookie = ""
            for pair in cookie_header.split(";"):
                pair = pair.strip()
                if pair.startswith("csrf_token="):
                    csrf_cookie = pair.split("=", 1)[1]
                    break
            csrf_header = headers_raw.get(b"x-csrf-token", b"").decode()
            if not csrf_cookie or csrf_cookie != csrf_header:
                import json as _cjson
                body = _cjson.dumps({"detail": "CSRF token missing or invalid"}).encode()
                await send({
                    "type": "http.response.start",
                    "status": 403,
                    "headers": [
                        (b"content-type", b"application/json"),
                        *self._SECURITY_HEADERS,
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                security_log.warning("csrf_rejected", path=path, client=client_ip)
                return

        status_code = 0

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
                # Inject security headers
                hdrs = list(message.get("headers", []))
                hdrs.extend(self._SECURITY_HEADERS)
                # Always set CSRF cookie (overwrites any stale HttpOnly
                # cookie from earlier deployments)
                if scope["type"] == "http":
                    import secrets as _secrets
                    # Read existing token from request cookie if present
                    req_headers = dict(scope.get("headers", []))
                    cookie_hdr = req_headers.get(b"cookie", b"").decode()
                    existing_token = ""
                    for pair in cookie_hdr.split(";"):
                        pair = pair.strip()
                        if pair.startswith("csrf_token="):
                            existing_token = pair.split("=", 1)[1]
                            break
                    token = existing_token or _secrets.token_hex(32)
                    hdrs.append((
                        b"set-cookie",
                        f"csrf_token={token}; Path=/; SameSite=Lax".encode(),
                    ))
                message = {**message, "headers": hdrs}
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
            duration = time.time() - start_time
            if status_code >= 400:
                security_log.warning("http_response_error",
                    method=method,
                    path=path,
                    status_code=status_code,
                    duration_ms=duration * 1000,
                )
        except Exception as e:
            security_log.error("http_request_exception",
                method=method,
                path=path,
                error=str(e),
            )
            raise


# ---------------------------------------------------------------------------
# Lifespan: startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    log.info("suite.starting", features={
        "smart_queue": settings.enable_smart_queue,
        "ml_predictor": settings.enable_ml_predictor,
        "universe_discovery": settings.enable_universe_discovery,
        "watch_party": settings.enable_watch_party,
    })

    # Database
    await init_db()
    log.info("suite.db_ready")

    # One-time: update alembic_version after migration squash
    try:
        async with async_session() as db:
            result = await db.execute(sa_text("SELECT version_num FROM alembic_version LIMIT 1"))
            row = result.first()
            if row and row[0] not in ("001_initial", "002_rewatch", "003_watch_history", "004_watch_history_genres", "005_watch_history_progress", "006_dedup_watch_history", "007_simkl", "008_user_rating", "009_episode_ratings", "010_watchlist_items", "011_dismissed_issues", "012_watch_history_null_dedup"):
                # Pre-squash revision — jump to current head
                await db.execute(sa_text("UPDATE alembic_version SET version_num = '007_simkl'"))
                await db.commit()
                log.info("suite.alembic_version_updated", old=row[0], new="007_simkl")
            # Let Alembic CMD run any pending upgrades (001→002→003→004→005)
    except Exception as e:
        log.warning("suite.alembic_version_check_skipped", error=str(e))

    # Load schedule overrides from DB (overwrite config defaults)
    await _load_schedule_overrides()

    # Seed Redis with durable settings from DB (survives Redis restarts)
    await _seed_redis_from_db()

    # Load wizard-saved Emby credentials from DB into in-memory settings
    # (covers the case where .env has no EMBY_URL but the wizard saved one)
    if not settings.emby_url or not settings.emby_api_key:
        try:
            from app.models.schema import AppSetting as _AppSetting
            async with async_session() as db:
                for key in ("emby_url", "emby_api_key"):
                    row = (await db.execute(
                        select(_AppSetting).where(_AppSetting.key == key)
                    )).scalar_one_or_none()
                    if row and row.value:
                        setattr(settings, key, row.value)
                        log.info("suite.loaded_from_db", key=key)
        except Exception as e:
            log.warning("suite.emby_creds_load_skipped", error=str(e)[:200])

    # Load wizard-saved Simkl client ID from DB if not in env
    if not settings.simkl_client_id:
        try:
            from app.models.schema import AppSetting as _AppSetting2
            async with async_session() as db:
                row = (await db.execute(
                    select(_AppSetting2).where(_AppSetting2.key == "simkl_client_id")
                )).scalar_one_or_none()
                if row and row.value:
                    settings.simkl_client_id = row.value
                    log.info("suite.loaded_from_db", key="simkl_client_id")
        except Exception as e:
            log.warning("suite.simkl_creds_load_skipped", error=str(e)[:200])

    # Encrypt any plaintext secrets already in Redis (one-time migration)
    try:
        migrated = await migrate_plaintext_secrets()
        if migrated:
            log.info("suite.secrets_encrypted", count=migrated)
    except Exception as e:
        log.warning("suite.secret_migration_failed", error=str(e))

    # One-time migration: move dismissed lists and drift data from Redis to DB
    await _migrate_redis_to_db()

    # One-time: invalidate watch stats cache after dedup migration so
    # corrected totals (without duplicate runtime inflation) show immediately.
    try:
        _r = await get_redis()
        # Flush stale job_completions so old cron toasts don't burst on first load
        await _r.delete("job_completions")
        if not await _r.get("dedup_stats_invalidated"):
            keys = await _r.keys("watch_stats_v5:*")
            if keys:
                await _r.delete(*keys)
                log.info("suite.dedup_stats_cache_cleared", keys=len(keys))
            await _r.set("dedup_stats_invalidated", "1")
    except Exception:
        pass

    # Scheduler
    _register_jobs()
    scheduler.start()
    log.info("suite.scheduler_started")

    # Run initial heartbeat so connection status is available immediately
    try:
        await run_heartbeat()
    except Exception:
        log.warning("suite.initial_heartbeat_failed")

    # First-run watchlist sync — ensures missing Radarr/Sonarr items are
    # on the Simkl/MDBList watchlist from the moment the container starts
    try:
        from app.services.watchlist_sync.service import WatchlistSyncService
        _wls_startup = WatchlistSyncService()
        await _wls_startup.run_for_all_users()
    except Exception:
        log.warning("suite.initial_watchlist_sync_failed")

    yield  # app is running

    # Shutdown
    scheduler.shutdown(wait=False)
    await close_redis()
    log.info("suite.shutdown")


# ---------------------------------------------------------------------------
# FastAPI app with security hardening
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Emby-Simkl Suite",
    version="1.0.0",
    description="Smart Queue · ML Predictor · Universe Discovery · Watch Party · Monitoring",
    lifespan=lifespan,
)

# Security middleware in order:

# 1. Trusted host middleware
# Home deployments are accessed via LAN IP (e.g. http://192.168.1.50:8000),
# so default to allowing any host. Set ALLOWED_HOSTS in .env to restrict
# (comma-separated, e.g. "localhost,192.168.1.50") if the suite is ever
# exposed beyond the home network.
def _get_allowed_hosts() -> list[str]:
    hosts = os.environ.get("ALLOWED_HOSTS", "*").split(",")
    return [h.strip() for h in hosts if h.strip()]

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=_get_allowed_hosts(),
)

# 2. GZIP compression

# 3. CORS with restricted origins (SECURITY FIX: No more allow_origins=["*"])
def _get_allowed_origins() -> list[str]:
    """Get allowed CORS origins from environment.

    Always includes moz-extension:// and chrome-extension:// so the
    Emby Remote Play browser extension can reach the API.
    """
    allowed = os.environ.get("ALLOWED_ORIGINS", "http://localhost:8000").split(",")
    origins = [o.strip() for o in allowed if o.strip()]
    # Browser extension origins use random UUIDs — can't be predicted,
    # so we must use allow_origin_regex below instead.
    return origins


# Extension origins (moz-extension://{uuid}, chrome-extension://{id})
# can't be enumerated, so use a regex alongside the explicit list.
_EXTENSION_ORIGIN_RE = r"^(moz-extension|chrome-extension)://.*$"

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_origin_regex=_EXTENSION_ORIGIN_RE,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Length", "Content-Type"],
    max_age=600,
)

# 4. Custom security audit middleware (pure ASGI — preserves sub-app mounts)
app.add_middleware(SecurityAuditMiddleware)

# 5. Rate limiting middleware (slowapi)
app.state.limiter = limiter
app.add_exception_handler(429, lambda r, e: JSONResponse(
    status_code=429,
    content={"error": "Rate limit exceeded", "retry_after": "60"},
))

# 6. Log validation errors so 422s are diagnosable from server logs
@app.exception_handler(RequestValidationError)
async def _validation_error_handler(request: Request, exc: RequestValidationError):
    log.warning(
        "request_validation_error",
        path=request.url.path,
        method=request.method,
        errors=exc.errors(),
    )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

# REST routes (split across domain-specific modules)
app.include_router(auth_router)
app.include_router(queue_router)
app.include_router(ml_router)
app.include_router(scrobble_audit_router)
app.include_router(universe_router)
app.include_router(party_router)
app.include_router(webhook_router)
app.include_router(settings_router)
app.include_router(bias_router)
app.include_router(arr_router)
app.include_router(watch_history_router)
app.include_router(downloads_router)
app.include_router(media_router)
app.include_router(mdblist_router)
app.include_router(ratings_router)
app.include_router(import_router)
app.include_router(item_detail_router)
app.include_router(duplicates_router)
app.include_router(library_health_router)
app.include_router(cross_sync_router)
# Monitoring routes (health checks, metrics)
app.include_router(monitoring_router)
# Social Watching, Library Health, Bulk Actions
app.include_router(phase5_router)


# ---------------------------------------------------------------------------
# Dashboard (served at /)
# ---------------------------------------------------------------------------

# Static assets (provider icons, etc.)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")


@app.get("/")
async def dashboard(request: Request):
    # Check if first-run setup is needed (no Emby creds or no provider chosen)
    from app.utils.redis_cache import get_redis
    from fastapi.responses import RedirectResponse
    try:
        if not settings.emby_url or not settings.emby_api_key:
            return RedirectResponse(url="/setup", status_code=302)

        r = await get_redis()
        provider = await r.get("integration_provider")
        if not provider:
            # Check DB as fallback
            from app.models.schema import AppSetting
            async with async_session() as db:
                row = (await db.execute(
                    select(AppSetting).where(AppSetting.key == "integration_provider")
                )).scalar_one_or_none()
            if not row:
                return RedirectResponse(url="/setup", status_code=302)
    except Exception:
        pass  # If check fails, show dashboard normally

    try:
        with open("frontend/templates/dashboard.html", "r") as f:
            html = f.read()

        # Inject feature toggle states (replaces Jinja2 placeholders)
        toggles = {
            "SMART_QUEUE": settings.enable_smart_queue,
            "ML_PREDICTOR": settings.enable_ml_predictor,
            "UNIVERSE_DISCOVERY": settings.enable_universe_discovery,
        }
        for key, enabled in toggles.items():
            html = html.replace(f"__BADGE_{key}__", "badge-on" if enabled else "badge-off")
            html = html.replace(f"__STATUS_{key}__", "ON" if enabled else "OFF")
        html = html.replace("__WATCH_PARTY_BOOL__", "true" if settings.enable_watch_party else "false")

        return _HTMLResponse(html)
    except FileNotFoundError:
        return _HTMLResponse("<h1>Dashboard not found</h1>", status_code=500)


# ---------------------------------------------------------------------------
# Combine Socket.IO + FastAPI into a single ASGI app
# ---------------------------------------------------------------------------

# The python-socketio recommended pattern: Socket.IO is the outer ASGI app
# and FastAPI is the fallback for non-socket requests.  This avoids the
# app.mount() approach which breaks when any middleware wraps the app.
#
# Socket.IO handles requests to /ws/socket.io/* and passes everything
# else through to FastAPI.  The Dockerfile CMD uses `app.main:app` so
# we reassign `app` here — the FastAPI instance stays alive via
# `other_asgi_app` and its lifespan (DB, scheduler) still runs.

_fastapi_app = app
app = socketio.ASGIApp(
    watch_party_sio,
    other_asgi_app=_fastapi_app,
    socketio_path="/ws/socket.io",
)


# ---------------------------------------------------------------------------
# Scheduler jobs
# ---------------------------------------------------------------------------

async def _load_schedule_overrides():
    """Read schedule cron overrides from app_settings DB table.

    If rows exist, override the in-memory settings values so that
    _register_jobs() picks them up. Falls back silently to .env defaults.
    """
    from app.models.schema import AppSetting
    try:
        async with async_session() as db:
            rows = (await db.execute(
                select(AppSetting).where(AppSetting.key.in_([
                    "cron_smart_queue", "cron_ml_retrain", "cron_universe_scan",
                ]))
            )).scalars().all()
        overrides = {r.key: r.value for r in rows}
        if "cron_smart_queue" in overrides:
            settings.smart_queue_cron = overrides["cron_smart_queue"]
        if "cron_ml_retrain" in overrides:
            settings.ml_retrain_cron = overrides["cron_ml_retrain"]
        if "cron_universe_scan" in overrides:
            settings.universe_scan_cron = overrides["cron_universe_scan"]
        if overrides:
            log.info("suite.schedule_overrides_loaded", keys=list(overrides.keys()))
    except Exception as e:
        log.warning("suite.schedule_overrides_skipped", error=str(e)[:200])


async def _seed_redis_from_db():
    """Seed Redis with durable settings from DB if Redis keys are missing.

    Covers radarr_servers, sonarr_servers, and auto_send_settings — these are
    persisted to app_settings on save but the heartbeat and connection-status
    endpoints read from Redis for speed.  After a Redis restart the keys vanish;
    this restores them from the authoritative DB copy.
    """
    from app.models.schema import AppSetting
    from app.utils.redis_cache import get_redis
    from app.utils.secure_redis import SECRET_KEYS
    _KEYS = ("radarr_servers", "sonarr_servers", "sabnzbd_servers", "auto_send_settings",
             "emby_url", "emby_api_key")
    try:
        r = await get_redis()
        async with async_session() as db:
            for key in _KEYS:
                existing = await r.get(key)
                if existing:
                    continue  # Redis already has it
                row = (await db.execute(
                    select(AppSetting).where(AppSetting.key == key)
                )).scalar_one_or_none()
                if row and row.value:
                    if key in SECRET_KEYS:
                        await secure_set(key, row.value)
                    else:
                        await r.set(key, row.value)
                    log.info("suite.redis_seeded_from_db", key=key)
    except Exception as e:
        log.warning("suite.redis_seed_skipped", error=str(e)[:200])


async def _migrate_redis_to_db():
    """One-time migration: copy scrobble dismissed lists and ML drift data
    from Redis into app_settings DB table so they survive rebuilds.

    Only runs if the DB key doesn't already exist (idempotent).
    """
    from app.models.schema import AppSetting
    from app.utils.redis_cache import get_redis
    try:
        r = await get_redis()
        async with async_session() as db:
            for pattern, db_prefix in [
                ("scrobble_audit_dismissed:*", "scrobble_dismissed:"),
                ("ml_drift:*", "ml_drift:"),
            ]:
                cursor = 0
                while True:
                    cursor, keys = await r.scan(cursor=cursor, match=pattern, count=100)
                    for redis_key in keys:
                        key_str = redis_key if isinstance(redis_key, str) else redis_key.decode()
                        suffix = key_str.split(":")[-1]
                        db_key = f"{db_prefix}{suffix}"
                        existing = (await db.execute(
                            select(AppSetting).where(AppSetting.key == db_key)
                        )).scalar_one_or_none()
                        if existing:
                            continue
                        raw = await r.get(redis_key)
                        if raw:
                            val = raw if isinstance(raw, str) else raw.decode()
                            db.add(AppSetting(
                                key=db_key, value=val,
                                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
                            ))
                            log.info("suite.migrated_redis_to_db", redis_key=key_str, db_key=db_key)
                    if not cursor:
                        break
            await db.commit()
    except Exception as e:
        log.warning("suite.redis_to_db_migration_skipped", error=str(e)[:200])


def _parse_cron(expr: str) -> dict:
    """Parse '0 2 * * *' into APScheduler CronTrigger kwargs."""
    parts = expr.split()
    if len(parts) != 5:
        return {"hour": 2, "minute": 0}
    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


async def _tracked_job(job_id: str, func):
    """Run a scheduled job and record last-run status to Redis."""
    import json as _json
    from app.utils.redis_cache import get_redis
    start = time.time()
    status = "ok"
    error_msg = None
    try:
        await func()
    except Exception as e:
        status = "error"
        error_msg = str(e)[:200]
        log.exception("scheduler.job_failed", job=job_id)
    elapsed = round(time.time() - start, 1)
    try:
        r = await get_redis()
        run_data = {
            "last_run": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "duration_s": elapsed,
            "error": error_msg,
        }
        await r.set(f"scheduler:status:{job_id}", _json.dumps(run_data))
        # Note: no longer pushing to job_completions Redis list.
        # Cron job toasts were stacking overnight and flooding the
        # dashboard on first morning load.  The scheduler status grid
        # already surfaces last-run / error info per job.
    except Exception:
        pass


# Map of job_id → cron expression for display
_job_crons: dict[str, str] = {}


def reschedule_job(job_id: str, cron_expr: str):
    """Reschedule a running APScheduler job with a new cron expression.

    Called from PUT /api/settings when the user saves new schedules.
    """
    parts = cron_expr.split()
    if len(parts) != 5:
        log.warning("reschedule.invalid_cron", job=job_id, cron=cron_expr)
        return
    trigger = CronTrigger(
        minute=parts[0], hour=parts[1], day=parts[2],
        month=parts[3], day_of_week=parts[4],
    )
    try:
        scheduler.reschedule_job(job_id, trigger=trigger)
        _job_crons[job_id] = cron_expr
        log.info("scheduler.job_rescheduled", job=job_id, cron=cron_expr)
    except Exception as e:
        log.warning("reschedule.failed", job=job_id, error=str(e))


async def run_heartbeat():
    """Ping Emby, Simkl, and Radarr — store results in Redis."""
    from app.utils.redis_cache import get_redis
    r = await get_redis()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # --- Emby ---
    from app.utils.emby_client import EmbyClient
    emby = EmbyClient()
    try:
        info = await emby.get_system_info()
        await r.set("heartbeat:emby", _json.dumps({
            "status": "ok", "checked_at": now,
            "server_name": info.get("ServerName", ""),
            "version": info.get("Version", ""),
        }), ex=600)
    except Exception as e:
        await r.set("heartbeat:emby", _json.dumps({
            "status": "error", "checked_at": now,
            "message": str(e)[:200],
        }), ex=600)
    finally:
        await emby.close()

    # --- Simkl (only if configured) ---
    if settings.simkl_client_id:
        from app.utils.simkl_client import SimklClient
        simkl = SimklClient()
        try:
            await simkl.get_trending(kind="shows")
            await r.set("heartbeat:simkl", _json.dumps({
                "status": "ok", "checked_at": now,
            }), ex=600)
        except Exception as e:
            await r.set("heartbeat:simkl", _json.dumps({
                "status": "error", "checked_at": now,
                "message": str(e)[:200],
            }), ex=600)
        finally:
            await simkl.close()

    # --- Radarr (0..N servers from Redis config) ---
    raw_servers = await secure_get("radarr_servers")
    if raw_servers:
        from app.utils.radarr_client import RadarrClient
        servers = _json.loads(raw_servers)
        for i, srv in enumerate(servers):
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", f"Radarr {i+1}"))
                result = await client.test_connection()
                await client.close()
                await r.set(f"heartbeat:radarr:{i}", _json.dumps({
                    "status": result.get("status", "error"),
                    "checked_at": now,
                    "version": result.get("version", ""),
                    "message": result.get("message", ""),
                }), ex=600)
            except Exception as e:
                await r.set(f"heartbeat:radarr:{i}", _json.dumps({
                    "status": "error", "checked_at": now,
                    "message": str(e)[:200],
                }), ex=600)

    # --- Sonarr (0..N servers from Redis config) ---
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        from app.utils.sonarr_client import SonarrClient
        sonarr_servers = _json.loads(raw_sonarr)
        for i, srv in enumerate(sonarr_servers):
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", f"Sonarr {i+1}"))
                result = await client.test_connection()
                await client.close()
                await r.set(f"heartbeat:sonarr:{i}", _json.dumps({
                    "status": result.get("status", "error"),
                    "checked_at": now,
                    "version": result.get("version", ""),
                    "message": result.get("message", ""),
                }), ex=600)
            except Exception as e:
                await r.set(f"heartbeat:sonarr:{i}", _json.dumps({
                    "status": "error", "checked_at": now,
                    "message": str(e)[:200],
                }), ex=600)

    # --- SABnzbd (0..N servers from Redis config) ---
    raw_sab = await secure_get("sabnzbd_servers")
    if raw_sab:
        from app.utils.sabnzbd_client import SabnzbdClient
        sab_servers = _json.loads(raw_sab)
        for i, srv in enumerate(sab_servers):
            try:
                client = SabnzbdClient(srv["url"], srv["api_key"], name=srv.get("name", f"SABnzbd {i+1}"))
                result = await client.test_connection()
                await client.close()
                await r.set(f"heartbeat:sabnzbd:{i}", _json.dumps({
                    "status": result.get("status", "error"),
                    "checked_at": now,
                    "version": result.get("version", ""),
                    "message": result.get("message", ""),
                }), ex=600)
            except Exception as e:
                await r.set(f"heartbeat:sabnzbd:{i}", _json.dumps({
                    "status": "error", "checked_at": now,
                    "message": str(e)[:200],
                }), ex=600)

    # --- MDBList (optional, only if API key configured) ---
    mdb_key = await secure_get("mdblist_api_key")
    if mdb_key:
        from app.utils.mdblist_client import MDBListClient
        mdb_key_str = mdb_key if isinstance(mdb_key, str) else mdb_key.decode()
        client = MDBListClient(api_key=mdb_key_str)
        try:
            result = await client.test_connection()
            await r.set("heartbeat:mdblist", _json.dumps({
                "status": result.get("status", "error"),
                "checked_at": now,
                "username": result.get("username", ""),
                "plan": result.get("plan", ""),
                "message": result.get("message", ""),
            }), ex=600)
        except Exception as e:
            await r.set("heartbeat:mdblist", _json.dumps({
                "status": "error", "checked_at": now,
                "message": str(e)[:200],
            }), ex=600)
        finally:
            await client.close()


def _register_jobs():
    if settings.enable_smart_queue:
        from app.services.smart_queue.service import SmartQueueService
        _sq_svc = SmartQueueService()
        cron = settings.smart_queue_cron
        _job_crons["smart_queue"] = cron

        async def _run_smart_queue(_fn=_sq_svc.run_for_all_users):
            await _tracked_job("smart_queue", _fn)

        scheduler.add_job(
            _run_smart_queue,
            CronTrigger(**_parse_cron(cron)),
            id="smart_queue",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="smart_queue", cron=cron)

    # Rewatch Recommender — daily at 2:15 AM (right after smart queue)
    from app.services.rewatch.service import RewatchRecommender
    _rw_svc = RewatchRecommender()
    rw_cron = "15 2 * * *"
    _job_crons["rewatch_rebuild"] = rw_cron

    async def _run_rewatch_rebuild(_fn=_rw_svc.run_for_all_users):
        await _tracked_job("rewatch_rebuild", _fn)

    scheduler.add_job(
        _run_rewatch_rebuild,
        CronTrigger(**_parse_cron(rw_cron)),
        id="rewatch_rebuild",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="rewatch_rebuild", cron=rw_cron)

    if settings.enable_ml_predictor:
        from app.services.ml_predictor.service import MLPredictorService
        _ml_svc = MLPredictorService()
        cron = settings.ml_retrain_cron
        _job_crons["ml_retrain"] = cron

        async def _run_ml_retrain(_fn=_ml_svc.train_for_all_users):
            await _tracked_job("ml_retrain", _fn)

        scheduler.add_job(
            _run_ml_retrain,
            CronTrigger(**_parse_cron(cron)),
            id="ml_retrain",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="ml_retrain", cron=cron)

    if settings.enable_universe_discovery:
        from app.services.universe_discovery.service import UniverseDiscoveryService
        _ud_svc = UniverseDiscoveryService()
        cron = settings.universe_scan_cron
        _job_crons["universe_scan"] = cron

        async def _run_universe_scan(_fn=_ud_svc.run_scan):
            await _tracked_job("universe_scan", _fn)

        scheduler.add_job(
            _run_universe_scan,
            CronTrigger(**_parse_cron(cron)),
            id="universe_scan",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="universe_scan", cron=cron)

    # Rating Bias Detector — weekly on Tuesday at 5 AM
    if settings.enable_rating_bias_detector:
        from app.services.rating_bias_detector.service import RatingBiasDetectorService
        _bd_svc = RatingBiasDetectorService()
        cron = "0 5 * * 1"
        _job_crons["bias_analysis"] = cron

        async def _run_bias_analysis(_fn=_bd_svc.analyze_for_all_users):
            await _tracked_job("bias_analysis", _fn)

        scheduler.add_job(
            _run_bias_analysis,
            CronTrigger(hour=5, minute=0, day_of_week="1"),
            id="bias_analysis",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="bias_analysis", cron=cron)

    # Watchlist Sync — every 30 minutes
    # Scans Radarr/Sonarr for missing items, adds to Simkl watchlist,
    # refreshes Airing Soon so new watchlisted premieres appear.
    from app.services.watchlist_sync.service import WatchlistSyncService
    _wls_svc = WatchlistSyncService()
    _job_crons["watchlist_sync"] = "*/30 * * * *"

    async def _run_watchlist_sync(_fn=_wls_svc.run_for_all_users):
        await _tracked_job("watchlist_sync", _fn)

    scheduler.add_job(
        _run_watchlist_sync,
        IntervalTrigger(minutes=30),
        id="watchlist_sync",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="watchlist_sync", interval="30m")

    # MDBList Sync — daily at 3:15 AM (after watchlist sync at 2:30 AM)
    # Re-imports all auto-synced MDBList lists into Emby playlists.
    mdblist_cron = "15 3 * * *"
    _job_crons["mdblist_sync"] = mdblist_cron

    async def _run_mdblist_sync():
        async def _do():
            from app.api.mdblist_routes import sync_all_mdblist_lists
            from app.utils.database import async_session as _async_session
            async with _async_session() as db:
                await sync_all_mdblist_lists(db)
        await _tracked_job("mdblist_sync", _do)

    scheduler.add_job(
        _run_mdblist_sync,
        CronTrigger(**_parse_cron(mdblist_cron)),
        id="mdblist_sync",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="mdblist_sync", cron=mdblist_cron)

    # Simkl List Sync — daily at 3:20 AM (right after MDBList sync)
    simkl_list_cron = "20 3 * * *"
    _job_crons["simkl_list_sync"] = simkl_list_cron

    async def _run_simkl_list_sync():
        async def _do():
            from app.api.mdblist_routes import sync_all_simkl_lists
            from app.utils.database import async_session as _async_session
            async with _async_session() as db:
                await sync_all_simkl_lists(db)
        await _tracked_job("simkl_list_sync", _do)

    scheduler.add_job(
        _run_simkl_list_sync,
        CronTrigger(**_parse_cron(simkl_list_cron)),
        id="simkl_list_sync",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="simkl_list_sync", cron=simkl_list_cron)

    # Premiere notifications — daily at 8 AM (notify about today's premieres/finales)
    premiere_notify_cron = "0 8 * * *"
    _job_crons["premiere_notify"] = premiere_notify_cron

    async def _run_premiere_notify():
        async def _do():
            from app.utils.notification_client import notify, _load_config
            from app.models.schema import User
            # Only run if premiere notifications are enabled
            config = await _load_config()
            if not config.get("events", {}).get("premiere", False):
                return
            if not config.get("services"):
                return
            from app.services.airing_alerts.service import AiringAlertsService
            from app.utils.database import async_session as _async_session
            async with _async_session() as db:
                user = (await db.execute(
                    select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
                )).scalars().first()
            if not user:
                return
            svc = AiringAlertsService()
            result = await svc.get_airing_soon(user, days=1)
            items = result.get("items", [])
            today_premieres = [e for e in items if e.get("is_premiere") and e.get("days_until_air", 99) == 0]
            today_finales = [e for e in items if e.get("is_finale") and e.get("days_until_air", 99) == 0]
            parts = []
            if today_premieres:
                names = [e.get("title", "?") for e in today_premieres[:3]]
                parts.append("Premieres: " + ", ".join(names)
                             + (f" +{len(today_premieres)-3}" if len(today_premieres) > 3 else ""))
            if today_finales:
                names = [e.get("title", "?") for e in today_finales[:3]]
                parts.append("Finales: " + ", ".join(names)
                             + (f" +{len(today_finales)-3}" if len(today_finales) > 3 else ""))
            if parts:
                notify("premiere", "📅 Airing Today", " · ".join(parts))
        await _tracked_job("premiere_notify", _do)

    scheduler.add_job(
        _run_premiere_notify,
        CronTrigger(**_parse_cron(premiere_notify_cron)),
        id="premiere_notify",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="premiere_notify", cron=premiere_notify_cron)

    # Library cache rebuild — daily at 1:30 AM (before smart queue at 2 AM)
    async def rebuild_library_cache():
        from app.utils.library_cache import LibraryCache
        from app.utils.emby_client import EmbyClient
        from app.models.schema import User
        from app.utils.database import async_session as _async_session
        emby = EmbyClient()
        async with _async_session() as db:
            user = (await db.execute(
                select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
            )).scalars().first()
        uid = user.emby_user_id if user else None
        summary = await LibraryCache.index_library(emby, user_id=uid)
        log.info("scheduler.library_cache_rebuilt", **summary)

    cron = "30 1 * * *"
    _job_crons["library_cache_rebuild"] = cron

    async def _run_cache_rebuild(_fn=rebuild_library_cache):
        await _tracked_job("library_cache_rebuild", _fn)

    scheduler.add_job(
        _run_cache_rebuild,
        CronTrigger(hour=1, minute=30),
        id="library_cache_rebuild",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="library_cache_rebuild", cron=cron)

    # Library Health scan — weekly on Wednesdays at 4:30 AM
    from app.services.library_health_service import LibraryHealthService
    _lh_svc = LibraryHealthService()

    async def _run_library_health_scan():
        async def _do():
            from app.utils.database import async_session as _async_session
            from app.models.schema import User
            async with _async_session() as db:
                users = (await db.execute(
                    select(User).where(User.simkl_access_token.isnot(None))
                )).scalars().all()
            for u in users:
                try:
                    await _lh_svc.scan(u)
                    log.info("scheduler.library_health_scanned", user_id=u.id)
                except Exception as e:
                    log.warning("scheduler.library_health_failed",
                                user_id=u.id, error=str(e)[:120])
        await _tracked_job("library_health_scan", _do)

    lh_cron = "30 4 * * 3"
    _job_crons["library_health_scan"] = lh_cron
    scheduler.add_job(
        _run_library_health_scan,
        CronTrigger(hour=4, minute=30, day_of_week="3"),
        id="library_health_scan",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="library_health_scan", cron=lh_cron)

    # SSL certificate check — daily at 6 AM (only runs if SSL_DOMAIN is set)
    if settings.ssl_domain:
        async def check_ssl_certificate():
            from app.api.route_helpers import _check_ssl_cert
            from app.utils.redis_cache import get_redis
            result = await _check_ssl_cert(settings.ssl_domain)
            r = await get_redis()
            await r.set("ssl:cert_status", _json.dumps(result))
            if result.get("status") in ("critical", "expired"):
                log.warning("ssl.cert_expiring", domain=settings.ssl_domain,
                            days_left=result.get("days_left"), status=result["status"])
            else:
                log.info("ssl.cert_checked", domain=settings.ssl_domain,
                         days_left=result.get("days_left"), status=result.get("status"))

        cron = "0 6 * * *"
        _job_crons["ssl_cert_check"] = cron

        async def _run_ssl_check(_fn=check_ssl_certificate):
            await _tracked_job("ssl_cert_check", _fn)

        scheduler.add_job(
            _run_ssl_check,
            CronTrigger(hour=6, minute=0),
            id="ssl_cert_check",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="ssl_cert_check", cron=cron, domain=settings.ssl_domain)

    # Connection heartbeat — every 5 minutes
    scheduler.add_job(
        run_heartbeat,
        "interval",
        minutes=5,
        id="heartbeat",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="heartbeat", interval="5m")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        app,  # Socket.IO outer app; FastAPI as fallback via other_asgi_app
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )

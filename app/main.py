"""Emby-Trakt Suite — application bootstrap.

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
from datetime import datetime

import socketio
import structlog
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.utils.logging import setup_logging, security_log
from app.utils.database import init_db, get_db, async_session
from app.utils.redis_cache import close_redis
from app.api.routes import router
from app.api.monitoring_routes import router as monitoring_router
from app.api.rate_limit_admin_routes import router as rate_limit_router
from app.api.phase5_routes import router as phase5_router
from app.services.watch_party.service import sio as watch_party_sio
from app.middleware.rate_limit import limiter
from app.services.monitoring.health import monitor
from app.services.rate_limit_service import RateLimitService

log = structlog.get_logger()

scheduler = AsyncIOScheduler()


# ---------------------------------------------------------------------------
# Security middleware for audit logging (pure ASGI — does not break sub-app mounts)
# ---------------------------------------------------------------------------

class SecurityAuditMiddleware:
    """Log all HTTP requests for security audit trail.

    Implemented as a pure ASGI middleware instead of BaseHTTPMiddleware
    because BaseHTTPMiddleware breaks Starlette sub-app mounts (the
    Socket.IO ASGIApp mounted at /ws would 404 on every request).
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        path = scope.get("path", "")
        method = scope.get("method", "WEBSOCKET" if scope["type"] == "websocket" else "")
        client = scope.get("client") or ("unknown", 0)
        client_ip = client[0] if client else "unknown"

        status_code = 0

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
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
    
    # Initialize rate limit configurations
    async with async_session() as db:
        await RateLimitService.initialize_defaults(db)
    log.info("suite.rate_limits_initialized")

    # Load schedule overrides from DB (overwrite config defaults)
    await _load_schedule_overrides()

    # Seed Redis with durable settings from DB (survives Redis restarts)
    await _seed_redis_from_db()

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
    # on the Trakt watchlist from the moment the container starts
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
    title="Emby-Trakt Suite",
    version="0.4.0",
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
app.add_exception_handler(429, lambda r, e: {"error": "Rate limit exceeded", "retry_after": "60"})

# REST routes
app.include_router(router)
# Monitoring routes (health checks, metrics)
app.include_router(monitoring_router)
# Rate limiting admin routes
app.include_router(rate_limit_router)
# Social Watching, Library Health, Metadata Enrichment, Bulk Actions
app.include_router(phase5_router)


# ---------------------------------------------------------------------------
# Dashboard (served at /)
# ---------------------------------------------------------------------------

templates = Jinja2Templates(directory="frontend/templates")

# Static assets (provider icons, etc.)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")


@app.get("/")
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "features": {
            "smart_queue": settings.enable_smart_queue,
            "ml_predictor": settings.enable_ml_predictor,
            "universe_discovery": settings.enable_universe_discovery,
            "watch_party": settings.enable_watch_party,
        },
    })


@app.get("/rate-limiting")
async def rate_limiting_dashboard(request: Request):
    """Rate limiting configuration dashboard."""
    return templates.TemplateResponse("rate_limiting.html", {"request": request})


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
    _KEYS = ("radarr_servers", "sonarr_servers", "auto_send_settings")
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
                    await r.set(key, row.value)
                    log.info("suite.redis_seeded_from_db", key=key)
    except Exception as e:
        log.warning("suite.redis_seed_skipped", error=str(e)[:200])


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
            "last_run": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "duration_s": elapsed,
            "error": error_msg,
        }
        await r.set(f"scheduler:status:{job_id}", _json.dumps(run_data))
        # Push completion event for dashboard toast notifications
        await r.lpush("job_completions", _json.dumps({
            "job": job_id, **run_data,
        }))
        await r.ltrim("job_completions", 0, 19)  # keep max 20
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
    """Ping Emby, Trakt, and Radarr — store results in Redis."""
    from app.utils.redis_cache import get_redis
    r = await get_redis()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # --- Emby ---
    try:
        from app.utils.emby_client import EmbyClient
        emby = EmbyClient()
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

    # --- Trakt ---
    try:
        from app.utils.trakt_client import TraktClient
        trakt = TraktClient()
        await trakt.get_trending(kind="shows")
        await trakt.close()
        await r.set("heartbeat:trakt", _json.dumps({
            "status": "ok", "checked_at": now,
        }), ex=600)
    except Exception as e:
        await r.set("heartbeat:trakt", _json.dumps({
            "status": "error", "checked_at": now,
            "message": str(e)[:200],
        }), ex=600)

    # --- Radarr (0..N servers from Redis config) ---
    raw_servers = await r.get("radarr_servers")
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
    raw_sonarr = await r.get("sonarr_servers")
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

    # Watchlist Sync — daily at 2:30 AM (after smart queue at 2 AM)
    # Scans Radarr/Sonarr for missing items, adds to Trakt watchlist,
    # refreshes Airing Soon so new watchlisted premieres appear.
    from app.services.watchlist_sync.service import WatchlistSyncService
    _wls_svc = WatchlistSyncService()
    wls_cron = "30 2 * * *"
    _job_crons["watchlist_sync"] = wls_cron

    async def _run_watchlist_sync(_fn=_wls_svc.run_for_all_users):
        await _tracked_job("watchlist_sync", _fn)

    scheduler.add_job(
        _run_watchlist_sync,
        CronTrigger(**_parse_cron(wls_cron)),
        id="watchlist_sync",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="watchlist_sync", cron=wls_cron)

    # Library cache rebuild — daily at 1:30 AM (before smart queue at 2 AM)
    async def rebuild_library_cache():
        from app.utils.library_cache import LibraryCache
        from app.utils.emby_client import EmbyClient
        from app.models.schema import User
        from app.utils.database import async_session as _async_session
        emby = EmbyClient()
        async with _async_session() as db:
            user = (await db.execute(
                select(User).where(User.trakt_access_token.isnot(None)).order_by(User.id)
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

    # SSL certificate check — daily at 6 AM (only runs if SSL_DOMAIN is set)
    if settings.ssl_domain:
        async def check_ssl_certificate():
            from app.api.routes import _check_ssl_cert
            from app.utils.redis_cache import get_redis as _get_redis
            result = await _check_ssl_cert(settings.ssl_domain)
            r = await _get_redis()
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

"""Emby-Trakt Suite — application bootstrap.

Starts:
  - FastAPI REST server on port 8000
  - Socket.IO WebSocket server on same ASGI app
  - APScheduler background tasks (smart queue, ML retrain, universe scan)
  - Database init
"""

from __future__ import annotations

import asyncio
import time
import os
from contextlib import asynccontextmanager
from datetime import datetime

import socketio
import structlog
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
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
# Security middleware for audit logging
# ---------------------------------------------------------------------------

async def security_audit_middleware(request: Request, call_next) -> Response:
    """Log all requests for security audit trail."""
    start_time = time.time()
    
    security_log.info("http_request_received",
        method=request.method,
        path=request.url.path,
        client_ip=request.client.host if request.client else "unknown",
    )
    
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        
        if response.status_code >= 400:
            security_log.warning("http_response_error",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=duration * 1000,
            )
        
        return response
    except Exception as e:
        security_log.error("http_request_exception",
            method=request.method,
            path=request.url.path,
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

    # Scheduler
    _register_jobs()
    scheduler.start()
    log.info("suite.scheduler_started")

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
    """Get allowed CORS origins from environment."""
    allowed = os.environ.get("ALLOWED_ORIGINS", "http://localhost:8000").split(",")
    return [o.strip() for o in allowed if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Length", "Content-Type"],
    max_age=600,
)

# 4. Custom security audit middleware
app.add_middleware(BaseHTTPMiddleware, dispatch=security_audit_middleware)

# 5. Rate limiting middleware (slowapi)
app.state.limiter = limiter
app.add_exception_handler(429, lambda r, e: {"error": "Rate limit exceeded", "retry_after": "60"})

# REST routes
app.include_router(router)
# Monitoring routes (health checks, metrics)
app.include_router(monitoring_router)
# Rate limiting admin routes
app.include_router(rate_limit_router)
# Phase 5 routes (Social Watching, Library Health, Metadata Enrichment, Bulk Actions)
app.include_router(phase5_router)


# ---------------------------------------------------------------------------
# Dashboard (served at /)
# ---------------------------------------------------------------------------

templates = Jinja2Templates(directory="frontend/templates")


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
# Mount Socket.IO for Watch Party
# ---------------------------------------------------------------------------

# Mount Socket.IO as a sub-app under FastAPI so the FastAPI lifespan
# (DB init, scheduler) still runs. Client path stays /ws/socket.io
sio_asgi = socketio.ASGIApp(watch_party_sio, socketio_path="/socket.io")
app.mount("/ws", sio_asgi)


# ---------------------------------------------------------------------------
# Scheduler jobs
# ---------------------------------------------------------------------------

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
        await r.set(f"scheduler:status:{job_id}", _json.dumps({
            "last_run": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "duration_s": elapsed,
            "error": error_msg,
        }))
    except Exception:
        pass


# Map of job_id → cron expression for display
_job_crons: dict[str, str] = {}


def _register_jobs():
    if settings.enable_smart_queue:
        from app.services.smart_queue.service import SmartQueueService
        _sq_svc = SmartQueueService()
        cron = settings.smart_queue_cron
        _job_crons["smart_queue"] = cron
        scheduler.add_job(
            lambda _fn=_sq_svc.run_for_all_users: _tracked_job("smart_queue", _fn),
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
        scheduler.add_job(
            lambda _fn=_ml_svc.train_for_all_users: _tracked_job("ml_retrain", _fn),
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
        scheduler.add_job(
            lambda _fn=_ud_svc.run_scan: _tracked_job("universe_scan", _fn),
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
        scheduler.add_job(
            lambda _fn=_bd_svc.analyze_for_all_users: _tracked_job("bias_analysis", _fn),
            CronTrigger(hour=5, minute=0, day_of_week="1"),
            id="bias_analysis",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="bias_analysis", cron=cron)

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
    scheduler.add_job(
        lambda _fn=rebuild_library_cache: _tracked_job("library_cache_rebuild", _fn),
        CronTrigger(hour=1, minute=30),
        id="library_cache_rebuild",
        replace_existing=True,
    )
    log.info("scheduler.job_added", job="library_cache_rebuild", cron=cron)

    # SSL certificate check — daily at 6 AM (only runs if SSL_DOMAIN is set)
    if settings.ssl_domain:
        async def check_ssl_certificate():
            import json as _json
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
        scheduler.add_job(
            lambda _fn=check_ssl_certificate: _tracked_job("ssl_cert_check", _fn),
            CronTrigger(hour=6, minute=0),
            id="ssl_cert_check",
            replace_existing=True,
        )
        log.info("scheduler.job_added", job="ssl_cert_check", cron=cron, domain=settings.ssl_domain)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        app,  # FastAPI is the outer app; Socket.IO mounted at /ws
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )

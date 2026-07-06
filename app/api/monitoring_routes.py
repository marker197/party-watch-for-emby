"""Monitoring endpoints for health checks and metrics.

Endpoints:
  GET /health - Quick health status
  GET /health/detailed - Full metrics report
  GET /health/alerts - Recent alerts only
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis_async
import structlog

from app.utils.database import get_db
from app.utils.redis_cache import get_redis
from app.services.monitoring.health import monitor

log = structlog.get_logger()
router = APIRouter(prefix="/health", tags=["monitoring"])


@router.get("", name="Quick Health Check")
async def quick_health():
    """Quick health status (minimal overhead)."""
    return {
        "status": "ok",
        "version": "0.4.0",
        "database_size_gb": monitor.db_size_bytes / 1_000_000_000,
        "request_count": monitor.request_count,
        "error_count": monitor.error_count,
        "redis_memory_mb": monitor.redis_memory_bytes / 1_000_000,
    }


@router.get("/detailed", name="Detailed Health Report")
async def detailed_health(
    db: AsyncSession = Depends(get_db),
    redis_client: redis_async.Redis = Depends(get_redis),
):
    """Full health report with all metrics and alerts."""
    try:
        report = await monitor.get_full_health_report(db, redis_client)
        log.info("health_report_generated", status=report["status"])
        return report
    except Exception as e:
        log.error("health_check_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@router.get("/alerts", name="Recent Alerts")
async def recent_alerts():
    """Get recent alerts only."""
    return {
        "alert_count": len(monitor.alerts),
        "recent_alerts": monitor.alerts[-20:],  # Last 20 alerts
        "alert_types": list(set(a["type"] for a in monitor.alerts)) if monitor.alerts else [],
    }


@router.get("/ready", name="Readiness Check")
async def readiness_check(db: AsyncSession = Depends(get_db)):
    """Kubernetes/Docker readiness probe."""
    try:
        # Quick DB connectivity check
        from sqlalchemy import text
        await db.execute(text("SELECT 1"))
        return {"ready": True}
    except Exception as e:
        log.error("readiness_check_failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/live", name="Liveness Check")
async def liveness_check():
    """Kubernetes/Docker liveness probe."""
    return {"alive": True, "uptime_seconds": monitor.check_uptime()["uptime"]["seconds"]}


__all__ = ["router"]

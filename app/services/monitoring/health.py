"""Health monitoring and alerting service.

Monitors:
  - Database size (alerts if > 1GB)
  - Redis memory usage
  - Application uptime
  - Error rates
  - Request latencies
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis_async

log = structlog.get_logger()


class HealthMonitor:
    """Monitor system health metrics and alert on thresholds."""
    
    # Database size threshold: 1GB
    DB_SIZE_THRESHOLD = 1_000_000_000  # bytes
    
    # Redis memory threshold: 400MB
    REDIS_MEMORY_THRESHOLD = 400_000_000  # bytes
    
    # Alert thresholds
    ERROR_RATE_THRESHOLD = 0.05  # 5% errors
    P95_LATENCY_THRESHOLD = 5.0  # 5 seconds
    
    def __init__(self):
        self.db_size_bytes = 0
        self.redis_memory_bytes = 0
        self.error_count = 0
        self.request_count = 0
        self.latencies = []
        self.start_time = datetime.now()
        self.alerts = []
    
    async def check_database_size(self, db: AsyncSession) -> Dict:
        """Check PostgreSQL database size."""
        try:
            result = await db.execute(
                text("""
                    SELECT pg_database_size(current_database()) as size_bytes
                """)
            )
            size_bytes = result.scalar()
            self.db_size_bytes = size_bytes
            
            alert = None
            if size_bytes > self.DB_SIZE_THRESHOLD:
                alert = {
                    "type": "database_size",
                    "severity": "warning",
                    "timestamp": datetime.now().isoformat(),
                    "message": f"Database size ({size_bytes / 1_000_000_000:.2f}GB) exceeds threshold ({self.DB_SIZE_THRESHOLD / 1_000_000_000:.2f}GB)",
                    "size_bytes": size_bytes,
                    "threshold_bytes": self.DB_SIZE_THRESHOLD,
                }
                self.alerts.append(alert)
                log.warning("database_size_alert", **alert)
            
            return {
                "database": {
                    "size_bytes": size_bytes,
                    "size_gb": size_bytes / 1_000_000_000,
                    "threshold_gb": self.DB_SIZE_THRESHOLD / 1_000_000_000,
                    "status": "warning" if size_bytes > self.DB_SIZE_THRESHOLD else "ok",
                    "alert": alert,
                }
            }
        except Exception as e:
            log.error("database_size_check_failed", error=str(e))
            return {"database": {"error": str(e), "status": "unknown"}}
    
    async def check_redis_memory(self, redis_client: redis_async.Redis) -> Dict:
        """Check Redis memory usage."""
        try:
            info = await redis_client.info("memory")
            used_memory = info.get("used_memory", 0)
            max_memory = info.get("maxmemory", 0)
            
            self.redis_memory_bytes = used_memory
            
            alert = None
            if used_memory > self.REDIS_MEMORY_THRESHOLD:
                alert = {
                    "type": "redis_memory",
                    "severity": "warning",
                    "timestamp": datetime.now().isoformat(),
                    "message": f"Redis memory ({used_memory / 1_000_000:.1f}MB) exceeds threshold ({self.REDIS_MEMORY_THRESHOLD / 1_000_000:.1f}MB)",
                    "used_memory_mb": used_memory / 1_000_000,
                    "threshold_mb": self.REDIS_MEMORY_THRESHOLD / 1_000_000,
                    "max_memory_mb": max_memory / 1_000_000 if max_memory else None,
                }
                self.alerts.append(alert)
                log.warning("redis_memory_alert", **alert)
            
            return {
                "redis": {
                    "used_memory_bytes": used_memory,
                    "used_memory_mb": used_memory / 1_000_000,
                    "max_memory_mb": max_memory / 1_000_000 if max_memory else None,
                    "threshold_mb": self.REDIS_MEMORY_THRESHOLD / 1_000_000,
                    "status": "warning" if used_memory > self.REDIS_MEMORY_THRESHOLD else "ok",
                    "alert": alert,
                }
            }
        except Exception as e:
            log.error("redis_memory_check_failed", error=str(e))
            return {"redis": {"error": str(e), "status": "unknown"}}
    
    def check_uptime(self) -> Dict:
        """Calculate application uptime."""
        uptime = datetime.now() - self.start_time
        return {
            "uptime": {
                "seconds": uptime.total_seconds(),
                "human": str(uptime).split('.')[0],
                "started_at": self.start_time.isoformat(),
            }
        }
    
    def check_error_rate(self) -> Dict:
        """Calculate error rate."""
        if self.request_count == 0:
            error_rate = 0
        else:
            error_rate = self.error_count / self.request_count
        
        alert = None
        if error_rate > self.ERROR_RATE_THRESHOLD:
            alert = {
                "type": "error_rate",
                "severity": "warning",
                "timestamp": datetime.now().isoformat(),
                "message": f"Error rate ({error_rate * 100:.1f}%) exceeds threshold ({self.ERROR_RATE_THRESHOLD * 100:.1f}%)",
                "error_rate": error_rate,
                "error_count": self.error_count,
                "total_requests": self.request_count,
            }
            self.alerts.append(alert)
            log.warning("error_rate_alert", **alert)
        
        return {
            "errors": {
                "error_count": self.error_count,
                "total_requests": self.request_count,
                "error_rate": error_rate,
                "threshold": self.ERROR_RATE_THRESHOLD,
                "status": "warning" if error_rate > self.ERROR_RATE_THRESHOLD else "ok",
                "alert": alert,
            }
        }
    
    def check_latency(self) -> Dict:
        """Calculate request latency percentiles."""
        if not self.latencies:
            return {
                "latency": {
                    "p50_ms": 0,
                    "p95_ms": 0,
                    "p99_ms": 0,
                    "requests_tracked": 0,
                    "status": "ok",
                    "alert": None,
                }
            }
        
        sorted_latencies = sorted(self.latencies)
        length = len(sorted_latencies)
        p50_idx = int(length * 0.50)
        p95_idx = int(length * 0.95)
        p99_idx = int(length * 0.99)
        
        p50 = sorted_latencies[p50_idx] * 1000 if p50_idx < length else 0
        p95 = sorted_latencies[p95_idx] * 1000 if p95_idx < length else 0
        p99 = sorted_latencies[p99_idx] * 1000 if p99_idx < length else 0
        
        alert = None
        if p95 > self.P95_LATENCY_THRESHOLD * 1000:
            alert = {
                "type": "high_latency",
                "severity": "info",
                "timestamp": datetime.now().isoformat(),
                "message": f"P95 latency ({p95:.1f}ms) exceeds threshold ({self.P95_LATENCY_THRESHOLD * 1000:.1f}ms)",
                "p95_ms": p95,
                "threshold_ms": self.P95_LATENCY_THRESHOLD * 1000,
            }
            log.info("high_latency_observed", **alert)
        
        return {
            "latency": {
                "p50_ms": p50,
                "p95_ms": p95,
                "p99_ms": p99,
                "requests_tracked": len(self.latencies),
                "status": "ok" if p95 <= self.P95_LATENCY_THRESHOLD * 1000 else "info",
                "alert": alert,
            }
        }
    
    async def get_full_health_report(self, db: AsyncSession, redis_client: redis_async.Redis) -> Dict:
        """Get comprehensive health report."""
        db_health = await self.check_database_size(db)
        redis_health = await self.check_redis_memory(redis_client)
        uptime = self.check_uptime()
        errors = self.check_error_rate()
        latency = self.check_latency()
        
        overall_status = "ok"
        if any(
            v.get("status") == "warning"
            for report in [db_health, redis_health, errors, latency]
            for v in report.values()
            if isinstance(v, dict)
        ):
            overall_status = "warning"
        
        return {
            "status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "alerts": self.alerts[-10:],  # Last 10 alerts
            **db_health,
            **redis_health,
            **uptime,
            **errors,
            **latency,
        }
    
    def record_request(self, latency_seconds: float, is_error: bool = False):
        """Record request metrics."""
        self.request_count += 1
        self.latencies.append(latency_seconds)
        
        # Keep only last 1000 latencies to avoid memory bloat
        if len(self.latencies) > 1000:
            self.latencies = self.latencies[-1000:]
        
        if is_error:
            self.error_count += 1


# Global monitor instance
monitor = HealthMonitor()

__all__ = ["monitor", "HealthMonitor"]

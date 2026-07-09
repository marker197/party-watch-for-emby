# Monitoring, Alerting & Database Management

This guide covers:
- Health monitoring endpoints
- Alert thresholds and management
- Database size monitoring
- Rate limiting configuration
- Performance tuning

## Health Monitoring Endpoints

### Quick Health Check (Lightweight)

```bash
curl http://localhost:8000/health

# Response:
{
  "status": "ok",
  "version": "0.4.0",
  "database_size_gb": 0.15,
  "request_count": 2847,
  "error_count": 3,
  "redis_memory_mb": 45.2
}
```

**Use for**: Kubernetes liveness probes, simple status checks  
**Overhead**: Minimal (~5ms)

### Detailed Health Report

```bash
curl http://localhost:8000/health/detailed

# Response includes:
{
  "status": "ok",  # or "warning" if any alerts
  "timestamp": "2026-06-30T12:34:56.789Z",
  "alerts": [ ... ],
  "database": {
    "size_bytes": 157286400,
    "size_gb": 0.15,
    "threshold_gb": 1.0,
    "status": "ok",
    "alert": null
  },
  "redis": {
    "used_memory_mb": 45.2,
    "max_memory_mb": 256.0,
    "threshold_mb": 400.0,
    "status": "ok",
    "alert": null
  },
  "uptime": {
    "seconds": 86400,
    "human": "1 day, 0:00:00",
    "started_at": "2026-06-29T12:34:56.789Z"
  },
  "errors": {
    "error_count": 3,
    "total_requests": 2847,
    "error_rate": 0.001,
    "threshold": 0.05,
    "status": "ok",
    "alert": null
  },
  "latency": {
    "p50_ms": 45.2,
    "p95_ms": 234.5,
    "p99_ms": 567.8,
    "requests_tracked": 2847,
    "status": "ok",
    "alert": null
  }
}
```

**Use for**: Full diagnostic checks, dashboards  
**Overhead**: ~100-200ms (includes DB query)

### Recent Alerts

```bash
curl http://localhost:8000/health/alerts

# Response:
{
  "alert_count": 2,
  "recent_alerts": [
    {
      "type": "database_size",
      "severity": "warning",
      "timestamp": "2026-06-30T10:15:22.123Z",
      "message": "Database size (1.25GB) exceeds threshold (1.0GB)",
      "size_bytes": 1342177280,
      "threshold_bytes": 1000000000
    },
    {
      "type": "redis_memory",
      "severity": "warning",
      "timestamp": "2026-06-30T09:45:10.456Z",
      "message": "Redis memory (425.3MB) exceeds threshold (400.0MB)",
      "used_memory_mb": 425.3,
      "threshold_mb": 400.0
    }
  ],
  "alert_types": ["database_size", "redis_memory"]
}
```

### Readiness Check (Kubernetes)

```bash
curl http://localhost:8000/health/ready

# 200 OK if database is connected
# 503 if database unreachable
```

### Liveness Check (Kubernetes)

```bash
curl http://localhost:8000/health/live

# Always returns 200 if container is running
```

## Alert Thresholds

Default thresholds (configurable in `app/services/monitoring/health.py`):

| Metric | Threshold | Severity | Action |
|--------|-----------|----------|--------|
| Database Size | 1.0 GB | Warning | Archive old data, optimize, or upgrade storage |
| Redis Memory | 400 MB | Warning | Evict less-used keys (LRU policy enabled) |
| Error Rate | 5% (errors/requests) | Warning | Check logs, investigate failures |
| P95 Latency | 5000 ms | Info | Optimize slow endpoints, check server load |

### Adjusting Thresholds

Edit `app/services/monitoring/health.py`:

```python
class HealthMonitor:
    DB_SIZE_THRESHOLD = 2_000_000_000        # 2GB instead of 1GB
    REDIS_MEMORY_THRESHOLD = 500_000_000     # 500MB instead of 400MB
    ERROR_RATE_THRESHOLD = 0.10              # 10% instead of 5%
    P95_LATENCY_THRESHOLD = 10.0             # 10 seconds instead of 5
```

Then rebuild:
```bash
docker-compose up -d --build
```

## Database Size Management

### Why Database Grows

1. **Watch History**: Every video watched creates records
2. **Ratings & Predictions**: ML model generates predictions for all items
3. **Cache**: Smart Queue recommendations cached for quick access
4. **Logs**: Audit trail and operation logs

### Typical Growth Rates

For a single Emby server with 1-5 users:

```
Initial (empty):     5 MB
After 1 week:        25-50 MB
After 1 month:       75-150 MB
After 6 months:      200-400 MB
After 1 year:        400-800 MB
```

### Monitoring Database Size

**Check current size:**
```bash
docker-compose exec postgres psql -U ${DB_USER} -d ${DB_NAME} -c "SELECT pg_size_pretty(pg_database_size('${DB_NAME}'));"
```

**Check size per table:**
```bash
docker-compose exec postgres psql -U ${DB_USER} -d ${DB_NAME} -c "
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

### Reducing Database Size

#### 1. Archive Old Watch History
```sql
-- Move watches older than 6 months to archive table
INSERT INTO watch_history_archive 
SELECT * FROM watch_history 
WHERE watched_at < NOW() - INTERVAL '6 months';

DELETE FROM watch_history 
WHERE watched_at < NOW() - INTERVAL '6 months';
```

#### 2. Cleanup Old Predictions
```sql
-- Remove predictions older than 3 months
DELETE FROM ml_predictions 
WHERE created_at < NOW() - INTERVAL '3 months';
```

#### 3. Vacuum Database
```bash
docker-compose exec postgres psql -U ${DB_USER} -d ${DB_NAME} -c "VACUUM FULL ANALYZE;"
```

#### 4. Disable Features You Don't Use

Edit `.env`:
```bash
ENABLE_ML_PREDICTOR=false      # Disables ML predictions (saves ~30% space)
ENABLE_UNIVERSE_DISCOVERY=false # Disables universe matching (saves ~10% space)
```

### Database Size Notifications

The monitoring system alerts when database exceeds 1GB:

```json
{
  "type": "database_size",
  "severity": "warning",
  "message": "Database size (1.25GB) exceeds threshold (1.0GB)",
  "action_required": true
}
```

**Recommended Actions**:
1. Review and archive old data
2. Increase monitoring frequency
3. Consider upgrading to larger disk if growth continues
4. Implement automatic cleanup policies

## Rate Limiting

### Configured Limits

By default, rate limiting is configured but not enforced (library integrated). To enable:

**Edit `app/api/routes.py` and add:**

```python
from app.middleware.rate_limit import limiter, LIMITS

@router.get("/queue/{user_id}")
@limiter.limit(LIMITS["read"])  # 150 requests/minute
async def get_queue(user_id: int, request: Request):
    # endpoint code
    pass

@router.post("/ml/train/{user_id}")
@limiter.limit(LIMITS["heavy"])  # 5 requests/minute
async def train_ml_model(user_id: int, request: Request):
    # endpoint code
    pass
```

### Rate Limit Responses

**When limit exceeded:**
```json
{
  "error": "Rate limit exceeded",
  "detail": "100 per 1 minute",
  "retry_after": "60"
}

HTTP Status: 429 Too Many Requests
```

### Bypass Rate Limits (For Admin)

If needed for testing:
```python
# Temporarily disable in config
RATE_LIMITING_ENABLED = False
```

## Performance Metrics

Monitor these via `/health/detailed`:

### Latency Percentiles

- **P50 (Median)**: 50% of requests complete within this time
- **P95**: 95% of requests complete within this time (target: <1000ms)
- **P99**: 99% of requests complete within this time

**Interpreting results:**
- P50 = 50ms, P95 = 200ms: Excellent
- P50 = 100ms, P95 = 500ms: Good
- P50 = 200ms, P95 = 2000ms: Acceptable
- P50 > 500ms or P95 > 5000ms: Needs optimization

### Error Rate

```
Error Rate = Errors / Total Requests

< 0.1%:  Excellent
0.1-1%:  Good
1-5%:    Acceptable
> 5%:    Alert - investigate
```

## Monitoring in Production

### Kubernetes Integration

Add probes to deployment manifest:

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: emby-trakt-suite
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8000
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8000
      initialDelaySeconds: 5
      periodSeconds: 5
```

### Docker Compose Health Checks

Already configured in `docker-compose.yml`:

```yaml
services:
  emby-trakt-suite:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```

Check status:
```bash
docker-compose ps
# Shows: emby-trakt-suite    healthy
```

### External Monitoring Tools

**Integration examples:**

**Prometheus:**
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'emby-trakt-suite'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/health/detailed'
```

**Grafana Dashboard:**
- Import metrics from Prometheus
- Alert on database_size > 1GB
- Alert on error_rate > 5%
- Alert on p95_latency > 5000ms

## Troubleshooting

### Health checks failing

**Check what's wrong:**
```bash
curl -v http://localhost:8000/health/detailed

# If database error:
docker-compose logs postgres

# If redis error:
docker-compose logs redis
```

### High error rate

**Investigate:**
```bash
docker-compose logs emby-trakt-suite | grep ERROR

# Check specific endpoint:
curl -v http://localhost:8000/queue/1

# Review full health report for patterns
curl http://localhost:8000/health/alerts
```

### Database growing too fast

**Check what's using space:**
```bash
docker-compose exec postgres psql -U ${DB_USER} -d ${DB_NAME} -c \
  "SELECT * FROM pg_tables WHERE schemaname='public' ORDER BY schemaname, tablename;"
```

**Most common culprits:**
1. `emby_items_cache` - Library cache (can be cleared)
2. `watch_history` - Historical data (can be archived)
3. `ml_predictions` - Old predictions (can be deleted)

---

**Setup Complete**: All monitoring, alerting, and management systems are ready to use ✅  
**Next Step**: Set up external monitoring tools (Prometheus/Grafana) for production

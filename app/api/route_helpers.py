"""Shared helper functions used across multiple route modules.

This module contains no route decorators — only utility functions that
multiple route files and the scheduler in main.py depend on.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import User, AppSetting
from app.utils.database import get_db
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set


async def record_job_run(job_id: str, status: str, duration_s: float, error: str | None = None):
    """Record a manual job run to the job_runs table."""
    try:
        from app.models.schema import JobRun
        from app.utils.database import async_session
        async with async_session() as db:
            db.add(JobRun(
                job_id=job_id, status=status,
                started_at=datetime.now(timezone.utc).replace(tzinfo=None),
                duration_s=round(duration_s, 1), error=error,
            ))
            await db.commit()
    except Exception:
        pass
from app.utils.database import async_session as async_session_ctx

log = structlog.get_logger()


# ── Constants ───────────────────────────────────────────────────────────────

VALID_PROVIDERS = {"simkl", "mdblist", "both", "none"}

MASKED_SUFFIX = "****"

_ITEM_KEY_RE = re.compile(r"^(emby|imdb|tmdb|simkl|tvdb):[A-Za-z0-9_-]+$")


# ── Integration provider helpers ────────────────────────────────────────────

async def _get_integration_provider(db: AsyncSession | None = None) -> str:
    """Return the configured integration provider: 'simkl', 'mdblist', 'both', or 'none'.
    Checks Redis first (fast), falls back to DB, defaults to 'simkl' for legacy installs."""
    r = await get_redis()
    raw = await r.get("integration_provider")
    if raw:
        val = raw if isinstance(raw, str) else raw.decode()
        if val in VALID_PROVIDERS:
            return val
    if db:
        row = (await db.execute(select(AppSetting).where(AppSetting.key == "integration_provider"))).scalar_one_or_none()
        if row and row.value in VALID_PROVIDERS:
            await r.set("integration_provider", row.value)
            return row.value
    # Legacy installs without this setting default to 'simkl' if simkl creds exist
    if settings.simkl_client_id:
        return "simkl"
    return "none"


def _provider_set(provider: str) -> set[str]:
    """Convert provider string to set of active integrations."""
    if provider == "both":
        return {"simkl", "mdblist"}
    if provider in ("simkl", "mdblist"):
        return {provider}
    return set()


async def _get_active_providers(db: AsyncSession | None = None) -> set[str]:
    """Return set of active integration providers, e.g. {'simkl', 'mdblist'}."""
    return _provider_set(await _get_integration_provider(db))


# ── User helpers ────────────────────────────────────────────────────────────

async def _first_emby_user_id() -> str | None:
    """Return the emby_user_id of the first linked user (for user-scoped queries)."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
    return user.emby_user_id if user else None


# ── Item key validation ─────────────────────────────────────────────────────

def _validate_item_key(item_key: str) -> str:
    """Validate item_key path parameter format (provider:value)."""
    from fastapi import HTTPException
    if not _ITEM_KEY_RE.match(item_key):
        raise HTTPException(400, "item_key must be provider:value (e.g. imdb:tt1234567)")
    return item_key


# ── MDBList key helper ──────────────────────────────────────────────────────

async def _get_mdblist_key(db: AsyncSession | None = None) -> str:
    """Return the configured MDBList API key from Redis (fast) or DB fallback."""
    r = await get_redis()
    raw = await secure_get("mdblist_api_key")
    if raw:
        return raw if isinstance(raw, str) else raw.decode()
    if db:
        raw = await _get_setting(db, "mdblist_api_key", "")
        if raw:
            await secure_set("mdblist_api_key", raw)
        return raw or ""
    return ""


# ── Activity log ────────────────────────────────────────────────────────────

async def _activity_log(message: str, category: str = "general"):
    """Append an entry to the activity log in Redis and push to dashboard."""
    import json as _json
    try:
        r = await get_redis()
        entry_dict = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "cat": category,
            "msg": message,
        }
        entry = _json.dumps(entry_dict)
        await r.lpush("activity_log", entry)
        await r.ltrim("activity_log", 0, 99)  # keep last 100
        # Push to any connected dashboard clients
        try:
            from app.services.watch_party.service import sio
            await sio.emit("activity_entry", entry_dict)
        except Exception:
            pass
        # Fire notification if message matches an enabled event type
        _maybe_notify(message)
    except Exception:
        pass  # logging should never crash the request


def _maybe_notify(message: str) -> None:
    """Pattern-match activity log messages to notification event types.
    Fire-and-forget — never blocks, never crashes."""
    import re as _re
    from app.utils.notification_client import notify
    msg = message.strip()
    # Consolidated playback notifications (start/stop with sync status)
    if msg.startswith("Started Watching:"):
        title = msg.split(":", 1)[1].strip()
        title = _re.sub(r'\s*—\s*(Synced|Sync error)$', '', title)
        notify("scrobble", "▶️ Started Watching", title)
    elif msg.startswith("Stopped Watching:"):
        title = msg.split(":", 1)[1].strip()
        title = _re.sub(r'\s*—\s*(Synced|Sync error.*)$', '', title)
        synced = "Synced" in msg
        notify("scrobble", "⏹️ Stopped Watching" + (" ✓" if synced else " ⚠"), title)
    # Legacy patterns (kept for backwards compatibility)
    elif msg.startswith("✓ Simkl scrobbled:") or msg.startswith("✓ Synced to Simkl:"):
        title = msg.split(":", 1)[1].strip() if ":" in msg else msg
        title = _re.sub(r'\s*\(\d+%?\)$', '', title)
        notify("scrobble", "🎬 Simkl Sync", title)
    elif msg.startswith("✓ Synced to MDBList:"):
        title = msg.split(":", 1)[1].strip() if ":" in msg else msg
        title = _re.sub(r'\s*\(\d+%?\)$', '', title)
        notify("scrobble", "🎬 MDBList Sync", title)
    # System errors
    elif "failed" in msg.lower() and ("token" in msg.lower() or "error" in msg.lower()):
        notify("system", "⚠️ System Alert", msg)


# ── Settings DB helpers ─────────────────────────────────────────────────────

async def _get_setting(db: AsyncSession, key: str, env_fallback: str) -> str:
    """Read a setting from DB; fall back to .env value if no DB row."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    return row.value if row else env_fallback


async def _put_setting(db: AsyncSession, key: str, value: str):
    """Upsert a setting into the app_settings table."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    if row:
        row.value = value
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    else:
        db.add(AppSetting(key=key, value=value, updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))


# ── API key masking ─────────────────────────────────────────────────────────

def _mask_api_key(key: str) -> str:
    """Return first 4 chars + **** for display. Never return the full key."""
    if not key:
        return ""
    return key[:4] + MASKED_SUFFIX


def _is_masked(key: str) -> bool:
    """True if the key looks like a masked value (ends with ****)."""
    return key.endswith(MASKED_SUFFIX) and len(key) <= 8


async def _resolve_servers(new_servers: list[dict], storage_key: str) -> list[dict]:
    """For each server in the incoming list, if the api_key is masked,
    look up the real key from the currently stored config."""
    import json as _json
    needs_resolve = any(_is_masked(s.get("api_key", "")) for s in new_servers)
    if not needs_resolve:
        return new_servers
    # Load existing servers to get real keys
    existing = {}
    try:
        r = await get_redis()
        raw = await r.get(storage_key)
        if raw:
            for srv in _json.loads(raw):
                existing[srv.get("url", "")] = srv.get("api_key", "")
    except Exception:
        pass
    resolved = []
    for s in new_servers:
        key = s.get("api_key", "")
        if _is_masked(key):
            # Preserve existing key matched by URL
            real_key = existing.get(s.get("url", "").rstrip("/"), "")
            if real_key:
                s = {**s, "api_key": real_key}
            else:
                continue  # can't resolve — skip this server
        resolved.append(s)
    return resolved


# ── SSL certificate check ──────────────────────────────────────────────────

async def _check_ssl_cert(domain: str) -> dict:
    """Connect to domain over TLS and read certificate expiry."""
    import ssl
    import socket
    import asyncio

    def _fetch_cert(d: str) -> dict:
        ctx = ssl.create_default_context()
        with socket.create_connection((d, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=d) as ssock:
                return ssock.getpeercert()

    try:
        cert = await asyncio.to_thread(_fetch_cert, domain)

        not_after_str = cert.get("notAfter", "")
        not_before_str = cert.get("notBefore", "")
        # e.g. "Sep 28 12:00:00 2026 GMT"
        from datetime import datetime as _dt
        not_after = _dt.strptime(not_after_str, "%b %d %H:%M:%S %Y %Z")
        not_before = _dt.strptime(not_before_str, "%b %d %H:%M:%S %Y %Z")
        days_left = (not_after - _dt.now(timezone.utc).replace(tzinfo=None)).days

        issuer_parts = dict(x[0] for x in cert.get("issuer", ()))
        issuer = issuer_parts.get("organizationName", issuer_parts.get("commonName", "Unknown"))

        subject_parts = dict(x[0] for x in cert.get("subject", ()))
        cn = subject_parts.get("commonName", domain)

        san = [entry[1] for entry in cert.get("subjectAltName", ())]

        status = "ok" if days_left > 30 else "expiring_soon" if days_left > 7 else "critical" if days_left > 0 else "expired"

        return {
            "domain": domain,
            "common_name": cn,
            "issuer": issuer,
            "not_before": not_before.strftime("%Y-%m-%d %H:%M:%S"),
            "not_after": not_after.strftime("%Y-%m-%d %H:%M:%S"),
            "days_left": days_left,
            "status": status,
            "san": san,
            "checked_at": _dt.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "error": None,
        }

    except Exception as e:
        return {
            "domain": domain,
            "status": "error",
            "days_left": None,
            "error": str(e)[:200],
            "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

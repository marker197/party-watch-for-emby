"""Notification service — sends alerts to Discord, Gotify, or generic webhooks.

Fire-and-forget via asyncio.create_task(). Never blocks the caller.
Settings stored in Redis (notifications_config) with DB persistence.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get

log = structlog.get_logger()

# Event types that can be individually toggled
EVENT_TYPES = {
    "scrobble":   "Scrobble complete (Trakt/MDBList sync)",
    "arrival":    "New content arrived in library",
    "premiere":   "Premiere or finale airing today",
    "download":   "Download finished",
    "prediction": "High-score ML prediction found",
    "system":     "System alerts (token failures, errors)",
}

# Defaults — only interesting events are on
DEFAULT_EVENTS = {
    "scrobble":   False,
    "arrival":    True,
    "premiere":   True,
    "download":   True,
    "prediction": False,
    "system":     True,
}

_CONFIG_KEY = "notifications_config"
_http: httpx.AsyncClient | None = None


def _get_http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=10.0)
    return _http


async def _load_config() -> dict:
    """Load notification config from Redis."""
    try:
        raw = await secure_get(_CONFIG_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {"services": [], "events": dict(DEFAULT_EVENTS)}


async def _send_discord(url: str, title: str, message: str, color: int = 0x6C5CE7) -> bool:
    """Send a Discord webhook embed."""
    payload = {
        "embeds": [{
            "title": title,
            "description": message,
            "color": color,
            "footer": {"text": "Emby-Trakt Suite"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }
    resp = await _get_http().post(url, json=payload)
    resp.raise_for_status()
    return True


async def _send_gotify(url: str, token: str, title: str, message: str, priority: int = 5) -> bool:
    """Send a Gotify notification."""
    api_url = f"{url.rstrip('/')}/message"
    params = {"token": token}
    payload = {
        "title": title,
        "message": message,
        "priority": priority,
        "extras": {
            "client::notification": {"click": {"url": ""}},
        },
    }
    resp = await _get_http().post(api_url, json=payload, params=params)
    resp.raise_for_status()
    return True


async def _send_webhook(url: str, title: str, message: str, event_type: str) -> bool:
    """Send a generic JSON webhook POST."""
    payload = {
        "event": event_type,
        "title": title,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "emby-trakt-suite",
    }
    resp = await _get_http().post(url, json=payload)
    resp.raise_for_status()
    return True


async def _dispatch(service: dict, title: str, message: str, event_type: str) -> None:
    """Dispatch a notification to a single service."""
    svc_type = service.get("type", "webhook")
    url = service.get("url", "")
    if not url:
        return

    try:
        if svc_type == "discord":
            await _send_discord(url, title, message)
        elif svc_type == "gotify":
            token = service.get("token", "")
            if not token:
                log.warning("notify.gotify_no_token", name=service.get("name"))
                return
            await _send_gotify(url, token, title, message)
        else:
            await _send_webhook(url, title, message, event_type)
    except Exception as e:
        log.warning("notify.send_failed",
                    service=service.get("name", "?"),
                    type=svc_type,
                    error=str(e)[:120])


async def _do_notify(event_type: str, title: str, message: str) -> None:
    """Internal: load config, check event is enabled, send to all services."""
    config = await _load_config()
    events = config.get("events", DEFAULT_EVENTS)

    if not events.get(event_type, False):
        return  # event type disabled

    services = config.get("services", [])
    if not services:
        return

    tasks = []
    for svc in services:
        if not svc.get("enabled", True):
            continue
        tasks.append(_dispatch(svc, title, message, event_type))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def notify(event_type: str, title: str, message: str) -> None:
    """Fire-and-forget notification. Safe to call from anywhere.

    Args:
        event_type: one of EVENT_TYPES keys
        title: short headline (e.g. "New Arrival")
        message: detail line (e.g. "Interstellar has been downloaded")
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_do_notify(event_type, title, message))
    except RuntimeError:
        pass  # no running loop — skip silently


async def test_service(service: dict) -> dict:
    """Send a test notification to a single service. Returns status dict."""
    try:
        await _dispatch(
            service,
            "🔔 Test Notification",
            "Emby-Trakt Suite notifications are working!",
            "test",
        )
        return {"status": "ok", "message": "Notification sent"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}

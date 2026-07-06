"""Redis connection and caching helpers."""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis
import structlog

from app.config import settings

log = structlog.get_logger()

_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=20,
        )
    return _pool


async def close_redis():
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

async def cache_set(key: str, value: Any, ttl: int = 3600) -> None:
    r = await get_redis()
    await r.set(key, json.dumps(value), ex=ttl)


async def cache_get(key: str) -> Any | None:
    r = await get_redis()
    raw = await r.get(key)
    if raw:
        return json.loads(raw)
    return None


async def cache_delete(key: str) -> None:
    r = await get_redis()
    await r.delete(key)


async def cache_keys(pattern: str) -> list[str]:
    r = await get_redis()
    return [k async for k in r.scan_iter(match=pattern)]


# -- Pub/Sub for watch-party ------------------------------------------------

async def publish(channel: str, data: dict) -> None:
    r = await get_redis()
    await r.publish(channel, json.dumps(data))


async def subscribe(channel: str):
    """Returns an async generator that yields parsed messages."""
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(channel)
    async for msg in pubsub.listen():
        if msg["type"] == "message":
            yield json.loads(msg["data"])

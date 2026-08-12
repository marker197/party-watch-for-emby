"""Shared Emby library cache to reduce redundant API calls.

Maintains a Redis-backed index of the Emby library keyed by provider IDs
(TMDB, IMDB, TVDB) and title for fast lookups from all services.

Cache is rebuilt once daily via scheduled task.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

from app.utils.redis_cache import get_redis

log = structlog.get_logger()

CACHE_KEY_PREFIX = "library:"
CACHE_EXPIRY = 86400  # 24 hours


class LibraryCache:
    """Manage shared Emby library cache."""

    @staticmethod
    def _item_cache_key(provider_id_type: str, provider_id: str) -> str:
        return f"{CACHE_KEY_PREFIX}{provider_id_type}:{provider_id}"

    @staticmethod
    def _title_cache_key(title: str, year: int | None = None) -> str:
        key = f"{CACHE_KEY_PREFIX}title:{title.lower()}"
        if year:
            key += f":{year}"
        return key

    @classmethod
    async def find_by_provider_id(cls, provider_id_type: str, provider_id: str) -> dict | None:
        """Find item in cache by provider ID (Tmdb, Imdb, Tvdb)."""
        try:
            r = await get_redis()
            key = cls._item_cache_key(provider_id_type, provider_id)
            data = await r.get(key)
            if data:
                await cls._record_hit()
                return json.loads(data)
            await cls._record_miss()
        except Exception as e:
            log.warning("library_cache.lookup_error", error=str(e))
        return None

    @classmethod
    async def find_by_title(cls, title: str, year: int | None = None, item_type: str | None = None) -> dict | None:
        """Find item in cache by title (case-insensitive)."""
        try:
            r = await get_redis()
            key = cls._title_cache_key(title, year)
            data = await r.get(key)
            if data:
                item = json.loads(data)
                if item_type and item.get("type", "").lower() != item_type.lower():
                    await cls._record_miss()
                    return None
                await cls._record_hit()
                return item
            await cls._record_miss()
        except Exception as e:
            log.warning("library_cache.title_lookup_error", error=str(e))
        return None

    @classmethod
    async def index_library(cls, emby_client, page_size: int = 100, user_id: str | None = None) -> dict:
        """Scan entire Emby library and cache all items.
        
        Uses user-scoped endpoint when user_id is provided, which ensures
        all user-visible libraries (e.g. '4K Movies' + 'Movies') are included.
        """
        cache_entries = 0
        summary = {"movies": 0, "series": 0, "cached_entries": 0}

        try:
            log.info("library_cache.indexing_start", user_id=user_id or "server-scope")
            start_time = datetime.now(timezone.utc)

            for item_type_label, item_type_query in [("movies", "Movie"), ("series", "Series")]:
                skip = 0
                while True:
                    items = await emby_client.get_library_items(
                        item_type_query, skip=skip, limit=page_size,
                        fields=["ProviderIds", "ProductionYear"],
                        user_id=user_id,
                    )
                    if not items:
                        break

                    for item in items:
                        await cls._cache_item(item, item_type={"movies": "movie", "series": "series"}[item_type_label])
                        cache_entries += 1

                    summary[item_type_label] += len(items)
                    skip += page_size
                    if len(items) < page_size:
                        break

            summary["cached_entries"] = cache_entries
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            log.info(
                "library_cache.indexing_complete",
                elapsed_seconds=elapsed,
                user_id=user_id or "server-scope",
                **summary,
            )

            if cache_entries == 0:
                log.warning(
                    "library_cache.empty_result",
                    hint="user_id may lack library access or Emby returned 0 items",
                    user_id=user_id,
                )

            # Record rebuild stats
            try:
                r = await get_redis()
                await r.set(
                    f"{CACHE_KEY_PREFIX}:stat:last_rebuild",
                    datetime.now(timezone.utc).isoformat(),
                )
                await r.set(f"{CACHE_KEY_PREFIX}:stat:movies", str(summary["movies"]))
                await r.set(f"{CACHE_KEY_PREFIX}:stat:series", str(summary["series"]))
            except Exception:
                pass

            return summary
        except Exception as e:
            log.error("library_cache.indexing_error", error=str(e))
            raise

    @classmethod
    async def _cache_item(cls, item: dict, item_type: str) -> None:
        try:
            r = await get_redis()
            emby_id = item.get("Id")
            title = item.get("Name", "")
            provider_ids = item.get("ProviderIds", {})
            year = item.get("ProductionYear")

            if not emby_id:
                return

            cache_data = json.dumps({
                "emby_id": emby_id,
                "title": title,
                "type": item_type,
                "provider_ids": provider_ids,
                "year": year,
            })

            # Cache by each provider ID
            for ptype, pid in provider_ids.items():
                if pid:
                    await r.setex(cls._item_cache_key(ptype, str(pid)), CACHE_EXPIRY, cache_data)

            # Cache by title
            if title:
                await r.setex(cls._title_cache_key(title, year), CACHE_EXPIRY, cache_data)
                # For series, also store a year-less key so CDN items with
                # mismatched year (e.g. latest season year) can still match
                if year and item_type == "series":
                    await r.setex(cls._title_cache_key(title, None), CACHE_EXPIRY, cache_data)
        except Exception as e:
            log.warning("library_cache.cache_item_error", error=str(e), item_id=item.get("Id"))

    @classmethod
    async def get_all_items(cls) -> list[dict]:
        """Return all unique library items from cache, deduped by emby_id."""
        try:
            r = await get_redis()
            seen_ids: set[str] = set()
            items: list[dict] = []
            async for key in r.scan_iter(match=f"{CACHE_KEY_PREFIX}Imdb:*", count=200):
                raw = await r.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                eid = data.get("emby_id")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    pids = data.get("provider_ids", {})
                    items.append({
                        "emby_id": eid,
                        "title": data.get("title"),
                        "year": data.get("year"),
                        "item_type": data.get("type"),
                        "imdb_id": pids.get("Imdb"),
                        "tmdb_id": pids.get("Tmdb"),
                        "tvdb_id": pids.get("Tvdb"),
                    })
            return items
        except Exception as e:
            log.warning("library_cache.get_all_items_error", error=str(e))
            return []

    @classmethod
    async def clear(cls) -> dict:
        try:
            r = await get_redis()
            deleted = 0
            async for key in r.scan_iter(match=f"{CACHE_KEY_PREFIX}*", count=100):
                await r.delete(key)
                deleted += 1
            log.info("library_cache.cleared", entries_deleted=deleted)
            return {"status": "cleared", "entries_deleted": deleted}
        except Exception as e:
            log.error("library_cache.clear_error", error=str(e))
            raise

    @classmethod
    async def get_stats(cls) -> dict:
        try:
            r = await get_redis()
            total_keys = 0
            async for _ in r.scan_iter(match=f"{CACHE_KEY_PREFIX}*", count=100):
                total_keys += 1

            hits = int(await r.get(f"{CACHE_KEY_PREFIX}:stat:hits") or 0)
            misses = int(await r.get(f"{CACHE_KEY_PREFIX}:stat:misses") or 0)
            total_lookups = hits + misses
            hit_rate = (hits / total_lookups * 100) if total_lookups > 0 else 0

            last_rebuild = await r.get(f"{CACHE_KEY_PREFIX}:stat:last_rebuild")
            movies = int(await r.get(f"{CACHE_KEY_PREFIX}:stat:movies") or 0)
            series = int(await r.get(f"{CACHE_KEY_PREFIX}:stat:series") or 0)
            version = int(await r.get(f"{CACHE_KEY_PREFIX}:stat:version") or 0)
            return {
                "cached_keys": total_keys,
                "movies": movies,
                "series": series,
                "items": movies + series,
                "hit_rate": round(hit_rate, 1),
                "last_rebuild": last_rebuild,
                "version": version,
            }
        except Exception as e:
            return {"cached_keys": 0, "hit_rate": 0, "last_rebuild": None, "error": str(e)}

    @classmethod
    async def _record_hit(cls):
        try:
            r = await get_redis()
            await r.incr(f"{CACHE_KEY_PREFIX}:stat:hits")
        except Exception:
            pass

    @classmethod
    async def _record_miss(cls):
        try:
            r = await get_redis()
            await r.incr(f"{CACHE_KEY_PREFIX}:stat:misses")
        except Exception:
            pass

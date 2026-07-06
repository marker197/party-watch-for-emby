"""Service #1 — Smart Watch Queue.

Daily task that:
1. Pulls user's Trakt watchlist, trending, friends' ratings, calendar
2. Cross-references with Emby library via LibraryCache (fast Redis lookups)
3. Scores & ranks items using learned weights from feedback
4. Creates / updates Emby collections

Phase 2: Feedback loop tracks which recommendations get played and
adjusts source weights per-user over time.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import QueueItem, User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import cache_get, cache_set
from app.utils.database import async_session

log = structlog.get_logger()

# Default weights — overridden per-user by feedback loop
DEFAULT_WEIGHTS = {
    "watchlist": 10.0,
    "trending": 6.0,
    "friend": 8.0,
    "calendar": 7.0,
    "affinity": 5.0,
}


class SmartQueueService:
    def __init__(self):
        self.emby = EmbyClient()

    async def run_for_all_users(self):
        """Main entry point called by scheduler."""
        log.info("smart_queue.run_start")
        async with async_session() as db:
            users = (await db.execute(
                select(User).where(User.trakt_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self._update_user_queue(user)
            except Exception:
                log.exception("smart_queue.user_error", user_id=user.id)

        log.info("smart_queue.run_complete", users_processed=len(users))

    async def _update_user_queue(self, user: User):
        # Phase 1: Token refresh callback
        async def on_token_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await db.commit()

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=on_token_refresh,
        )
        try:
            # Check rate limit budget
            info = trakt.get_rate_limit_info()
            if info["remaining"] < 50:
                log.warning("smart_queue.rate_limit_low", remaining=info["remaining"])
                return

            candidates = await self._gather_candidates(trakt, user)

            # Phase 2: Load learned weights
            weights = await self._load_weights(user.id)
            scored = self._score_candidates(candidates, weights)
            top = sorted(scored, key=lambda c: c["score"], reverse=True)[:30]

            await self._persist_queue(user, top)
            await self._sync_emby_collection(user, top)
            log.info("smart_queue.user_done", user=user.emby_username, items=len(top))
        finally:
            await trakt.close()

    # -----------------------------------------------------------------------
    # Gather candidates from multiple Trakt sources
    # -----------------------------------------------------------------------

    async def _gather_candidates(self, trakt: TraktClient, user: User) -> list[dict]:
        candidates: dict[str, dict] = {}

        # 1. Watchlist items
        watchlist = await trakt.get_watchlist()
        for entry in watchlist:
            item = entry.get("movie") or entry.get("show") or {}
            tid = str(item.get("ids", {}).get("trakt", ""))
            if tid:
                candidates[tid] = {
                    "trakt_id": tid,
                    "title": item.get("title", ""),
                    "year": item.get("year"),
                    "item_type": "movie" if "movie" in entry else "show",
                    "ids": item.get("ids", {}),
                    "source": "watchlist",
                    "source_score": 1.0,  # raw; multiplied by weight later
                }

        # 2. Trending shows + movies
        for kind in ("shows", "movies"):
            trending = await trakt.get_trending(kind=kind, limit=30)
            for rank, entry in enumerate(trending):
                item = entry.get("movie") or entry.get("show") or {}
                tid = str(item.get("ids", {}).get("trakt", ""))
                if tid and tid not in candidates:
                    candidates[tid] = {
                        "trakt_id": tid,
                        "title": item.get("title", ""),
                        "year": item.get("year"),
                        "item_type": "movie" if kind == "movies" else "show",
                        "ids": item.get("ids", {}),
                        "source": "trending",
                        "source_score": 1.0 - rank / 30,
                        "trending_rank": rank + 1,
                    }

        # 3. Calendar (upcoming episodes for shows user follows)
        try:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            calendar = await trakt.get_my_shows(start_date=today, days=14)
            for entry in calendar:
                show = entry.get("show", {})
                tid = str(show.get("ids", {}).get("trakt", ""))
                if tid and tid not in candidates:
                    candidates[tid] = {
                        "trakt_id": tid,
                        "title": show.get("title", ""),
                        "year": show.get("year"),
                        "item_type": "show",
                        "ids": show.get("ids", {}),
                        "source": "calendar",
                        "source_score": 1.0,
                        "air_date": entry.get("first_aired"),
                    }
        except Exception:
            log.warning("smart_queue.calendar_skip", reason="calendar fetch failed")

        # 4. Friends' highly rated
        try:
            friends = await trakt.get_friends()
            for friend in friends[:10]:
                fname = friend.get("user", {}).get("ids", {}).get("slug", "")
                if not fname:
                    continue
                try:
                    friend_ratings = await trakt.get_friend_ratings(fname, kind="all")
                    for r in friend_ratings:
                        if r.get("rating", 0) < 8:
                            continue
                        item = r.get("movie") or r.get("show") or {}
                        tid = str(item.get("ids", {}).get("trakt", ""))
                        if tid and tid not in candidates:
                            candidates[tid] = {
                                "trakt_id": tid,
                                "title": item.get("title", ""),
                                "year": item.get("year"),
                                "item_type": "movie" if "movie" in r else "show",
                                "ids": item.get("ids", {}),
                                "source": "friend",
                                "source_score": 1.0,
                                "friend_rating": r.get("rating"),
                                "friend_name": fname,
                            }
                except Exception:
                    continue
        except Exception:
            log.warning("smart_queue.friends_skip")

        return list(candidates.values())

    # -----------------------------------------------------------------------
    # Score candidates using learned weights
    # -----------------------------------------------------------------------

    def _score_candidates(self, candidates: list[dict], weights: dict) -> list[dict]:
        for c in candidates:
            source = c.get("source", "watchlist")
            weight = weights.get(source, 5.0)
            score = c.get("source_score", 1.0) * weight

            # boost items airing soon
            if c.get("air_date"):
                try:
                    air = datetime.fromisoformat(c["air_date"].replace("Z", "+00:00"))
                    days_away = (air - datetime.utcnow().astimezone()).days
                    if 0 <= days_away <= 3:
                        score += 4.0
                    elif days_away <= 7:
                        score += 2.0
                except Exception:
                    pass

            # boost friend-endorsed items
            if c.get("friend_rating"):
                score += (c["friend_rating"] - 7) * 1.5

            c["score"] = round(score, 2)
        return candidates

    # -----------------------------------------------------------------------
    # Match candidates to Emby library using LibraryCache (Phase 1)
    # -----------------------------------------------------------------------

    async def _find_in_emby(self, candidate: dict) -> str | None:
        """Match a Trakt item to an Emby library item via LibraryCache."""
        ids = candidate.get("ids", {})

        # Try provider IDs via cache first (sub-millisecond)
        for provider_type, trakt_key in [("Tmdb", "tmdb"), ("Imdb", "imdb"), ("Tvdb", "tvdb")]:
            pid = ids.get(trakt_key)
            if pid:
                cached = await LibraryCache.find_by_provider_id(provider_type, str(pid))
                if cached:
                    return cached["emby_id"]

        # Fallback: title search via cache
        title = candidate.get("title", "")
        year = candidate.get("year")
        if title:
            cached = await LibraryCache.find_by_title(title, year=year)
            if cached:
                return cached["emby_id"]

        # Last resort: live Emby search (only if cache misses)
        if title:
            search_type = "Movie" if candidate.get("item_type") == "movie" else "Series"
            search_results = await self.emby.search_items(title, item_type=search_type)
            for emby_item in search_results:
                if emby_item.get("Name", "").lower() == title.lower():
                    return emby_item["Id"]

        return None

    # -----------------------------------------------------------------------
    # Persist queue to database
    # -----------------------------------------------------------------------

    async def _persist_queue(self, user: User, items: list[dict]):
        async with async_session() as db:
            await db.execute(delete(QueueItem).where(QueueItem.user_id == user.id))
            for item in items:
                emby_id = await self._find_in_emby(item)
                if not emby_id:
                    continue
                db.add(QueueItem(
                    user_id=user.id,
                    emby_item_id=emby_id,
                    title=item["title"],
                    item_type=item["item_type"],
                    source=item["source"],
                    score=item["score"],
                    trakt_trending_rank=item.get("trending_rank"),
                    trakt_rating=item.get("friend_rating"),
                    metadata_json=item,
                ))
            await db.commit()

    # -----------------------------------------------------------------------
    # Sync to Emby collection
    # -----------------------------------------------------------------------

    async def _sync_emby_collection(self, user: User, items: list[dict]):
        emby_ids = []
        for item in items:
            eid = await self._find_in_emby(item)
            if eid:
                emby_ids.append(eid)

        if emby_ids:
            # Use playlists (preserves insertion order) instead of collections
            await self.emby.recreate_playlist(
                "🎯 Smart Up Next", emby_ids, user_id=user.emby_user_id,
            )
            log.info("smart_queue.playlist_synced", count=len(emby_ids))

    # ===================================================================
    # PHASE 2: Feedback Loop — learned weights
    # ===================================================================

    async def _load_weights(self, user_id: int) -> dict:
        """Load per-user learned weights from Redis, or return defaults."""
        try:
            raw = await cache_get(f"queue_weights:{user_id}")
            if raw:
                return json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            pass
        return dict(DEFAULT_WEIGHTS)

    async def record_play(self, user_id: int, emby_item_id: str, duration_ticks: int = 0):
        """Called when Emby webhook reports a user played a queue item.

        Marks the item as played, then recalculates source weights
        based on which recommendation sources the user actually acts on.
        """
        async with async_session() as db:
            item = (await db.execute(
                select(QueueItem).where(
                    QueueItem.user_id == user_id,
                    QueueItem.emby_item_id == emby_item_id,
                    QueueItem.played == False,
                )
            )).scalar_one_or_none()

            if not item:
                return  # not a queue recommendation

            item.played = True
            item.played_at = datetime.utcnow()
            item.played_duration_ticks = duration_ticks
            await db.commit()

            log.info(
                "smart_queue.feedback_recorded",
                user_id=user_id,
                title=item.title,
                source=item.source,
            )

        # Recalculate weights
        await self._update_weights(user_id)

    async def _update_weights(self, user_id: int):
        """Adjust source weights based on play rates.

        Logic:
        - Count total recommendations per source
        - Count played recommendations per source
        - play_rate = played / total
        - If play_rate > 0.5 → boost weight by 15%
        - If play_rate < 0.15 → reduce weight by 15%
        - Clamp weights between 2.0 and 20.0
        """
        async with async_session() as db:
            # Get play counts per source (last 90 days of queue items)
            cutoff = datetime.utcnow() - timedelta(days=90)
            rows = (await db.execute(
                select(
                    QueueItem.source,
                    func.count(QueueItem.id).label("total"),
                    func.count(QueueItem.played_at).label("played"),
                )
                .where(
                    QueueItem.user_id == user_id,
                    QueueItem.created_at >= cutoff,
                )
                .group_by(QueueItem.source)
            )).all()

        if not rows:
            return

        weights = await self._load_weights(user_id)

        for source, total, played in rows:
            if total < 3:
                continue  # too few items to learn from
            play_rate = played / total
            old_weight = weights.get(source, 5.0)

            if play_rate > 0.5:
                new_weight = old_weight * 1.15  # +15%
            elif play_rate < 0.15:
                new_weight = old_weight * 0.85  # -15%
            else:
                continue

            weights[source] = round(max(2.0, min(20.0, new_weight)), 2)
            log.info(
                "smart_queue.weight_adjusted",
                user_id=user_id,
                source=source,
                play_rate=f"{play_rate:.0%}",
                old_weight=old_weight,
                new_weight=weights[source],
            )

        # Persist updated weights
        await cache_set(f"queue_weights:{user_id}", json.dumps(weights), ttl=86400 * 365)

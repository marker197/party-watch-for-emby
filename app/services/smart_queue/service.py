"""Service #1 — Smart Watch Queue.

Daily task that:
1. Pulls user's Trakt watchlist, trending, friends' ratings, calendar
2. Cross-references with Emby library via LibraryCache (fast Redis lookups)
3. Scores & ranks items using learned weights from feedback
4. Creates / updates Emby collections

Feedback loop tracks which recommendations get played and
adjusts source weights per-user over time.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
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
    "recommended": 8.0,
    "friend": 8.0,
    "calendar": 7.0,
    "affinity": 5.0,
}

# Source quotas for the 20-item queue
SOURCE_QUOTAS = {
    "watchlist": 7,
    "trending": 7,
    "recommended": 6,
}


class SmartQueueService:
    def __init__(self):
        self.emby = None

    async def _ensure_emby(self):
        """Create a fresh EmbyClient if not already open."""
        if self.emby is None:
            self.emby = EmbyClient()

    async def _close_emby(self):
        """Close the EmbyClient if open."""
        if self.emby is not None:
            try:
                await self.emby.close()
            except Exception:
                pass
            self.emby = None

    async def run_for_all_users(self):
        """Main entry point called by scheduler."""
        log.info("smart_queue.run_start")
        await self._ensure_emby()
        try:
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
        finally:
            await self._close_emby()

    async def _update_user_queue(self, user: User):
        # Token refresh callback
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

            # Prune MOVIE items already in Trakt watched history
            # Shows are NOT filtered here — they use Emby episode awareness instead
            watched_movie_ids = await self._get_watched_trakt_ids(trakt)

            candidates = await self._gather_candidates(trakt, user)

            # Filter out already-watched movies (shows skip this filter)
            before = len(candidates)
            filtered = []
            for c in candidates:
                tid = str(c.get("trakt_id", ""))
                if tid and tid in watched_movie_ids and c.get("item_type") == "movie":
                    log.debug("smart_queue.candidate_filtered",
                              title=c.get("title"), trakt_id=tid, source=c.get("source"))
                else:
                    filtered.append(c)
            candidates = filtered
            if before != len(candidates):
                log.info("smart_queue.filtered_watched",
                         user_id=user.id, removed=before - len(candidates),
                         watched_set_size=len(watched_movie_ids))

            # Pre-resolve Emby IDs and check played status
            candidates = await self._resolve_and_filter_played(candidates, user)

            # Load learned weights and staleness counters
            weights = await self._load_weights(user.id)
            staleness = await self._load_staleness(user.id)
            scored = self._score_candidates(candidates, weights, staleness=staleness)

            # Source-stratified selection: 7 watchlist, 7 trending, 6 recommended
            top = self._stratified_select(scored)

            # Overflow rotation — swap bottom 3 unplayed items for top overflow
            top = await self._rotate_overflow(user.id, top)

            # Remaining candidates (not selected) become overflow for backfill
            top_ids = {c["trakt_id"] for c in top}
            leftover = sorted(
                [c for c in scored if c["trakt_id"] not in top_ids],
                key=lambda c: c["score"], reverse=True,
            )
            overflow = leftover[:30]
            await self._cache_overflow(user.id, overflow)

            # Update staleness counters: increment for items still in queue,
            # remove items no longer present
            new_staleness = {}
            for c in top:
                tid = str(c.get("trakt_id", ""))
                if tid:
                    new_staleness[tid] = staleness.get(tid, 0) + 1
            await self._save_staleness(user.id, new_staleness)

            resolved_ids = await self._persist_queue(user, top)
            await self._sync_emby_collection(user, top, resolved_ids)

            # Auto-send missing items to Radarr/Sonarr if enabled
            await self._auto_send_missing(top, resolved_ids)


            log.info("smart_queue.user_done", user=user.emby_username,
                     items=len(top), overflow=len(overflow))
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

        wl_movies = sum(1 for c in candidates.values() if c["item_type"] == "movie")
        wl_shows = sum(1 for c in candidates.values() if c["item_type"] == "show")
        log.info("smart_queue.watchlist_gathered",
                 total=len(candidates), movies=wl_movies, shows=wl_shows)

        # 2. Trending shows + movies (randomise page for variety)
        trending_page = random.randint(1, 3)
        for kind in ("shows", "movies"):
            trending = await trakt.get_trending(kind=kind, limit=15, page=trending_page)
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
                        "source_score": 1.0 - rank / 15,
                        "trending_rank": rank + 1 + ((trending_page - 1) * 15),
                    }

        # 3. Recommended (personalised based on user's Trakt ratings)
        for kind in ("shows", "movies"):
            try:
                recs = await trakt.get_recommended(kind=kind, limit=15)
                for rank, entry in enumerate(recs):
                    # Recommended endpoint returns items directly (not wrapped)
                    item = entry.get("movie") or entry.get("show") or entry
                    tid = str(item.get("ids", {}).get("trakt", ""))
                    if tid and tid not in candidates:
                        candidates[tid] = {
                            "trakt_id": tid,
                            "title": item.get("title", ""),
                            "year": item.get("year"),
                            "item_type": "movie" if kind == "movies" else "show",
                            "ids": item.get("ids", {}),
                            "source": "recommended",
                            "source_score": 1.0 - rank / 15,
                        }
            except Exception:
                log.warning("smart_queue.recommended_skip", kind=kind)

        # 4. Calendar (upcoming episodes for shows user follows)
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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

        # 5. Friends' highly rated
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

        # Filter out permanently blocked items
        blocked_ids = await self._load_blocklist(user.id)
        if blocked_ids:
            before_block = len(candidates)
            candidates = {tid: c for tid, c in candidates.items() if tid not in blocked_ids}
            removed = before_block - len(candidates)
            if removed:
                log.info("smart_queue.blocklist_filtered", user_id=user.id, removed=removed)

        return list(candidates.values())

    # -----------------------------------------------------------------------
    # Score candidates using learned weights
    # -----------------------------------------------------------------------

    async def _load_staleness(self, user_id: int) -> dict[str, int]:
        """Load per-item staleness counters (trakt_id → consecutive refresh count)."""
        try:
            data = await cache_get(f"queue_staleness:{user_id}")
            if data and isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    async def _load_blocklist(self, user_id: int) -> set[str]:
        """Load permanently blocked Trakt IDs for a user."""
        from app.models.schema import QueueBlocklist
        async with async_session() as db:
            rows = (await db.execute(
                select(QueueBlocklist.trakt_id).where(QueueBlocklist.user_id == user_id)
            )).scalars().all()
            return set(rows)

    async def _save_staleness(self, user_id: int, staleness: dict[str, int]):
        """Persist staleness counters to Redis."""
        await cache_set(f"queue_staleness:{user_id}", staleness, ttl=86400 * 30)

    def _score_candidates(self, candidates: list[dict], weights: dict,
                          staleness: dict[str, int] | None = None) -> list[dict]:
        for c in candidates:
            source = c.get("source", "watchlist")
            weight = weights.get(source, 5.0)
            score = c.get("source_score", 1.0) * weight

            # boost items airing soon
            if c.get("air_date"):
                try:
                    air = datetime.fromisoformat(c["air_date"].replace("Z", "+00:00"))
                    days_away = (air - datetime.now(timezone.utc)).days
                    if 0 <= days_away <= 3:
                        score += 4.0
                    elif days_away <= 7:
                        score += 2.0
                except Exception:
                    pass

            # boost friend-endorsed items
            if c.get("friend_rating"):
                score += (c["friend_rating"] - 7) * 1.5

            # staleness decay — reduce score for items that have sat unplayed
            if staleness:
                days_stale = staleness.get(str(c.get("trakt_id", "")), 0)
                if days_stale > 0:
                    # -1.5 points per day stale, capped at -8
                    penalty = min(days_stale * 1.5, 8.0)
                    score -= penalty

            c["score"] = round(score, 2)
        return candidates

    # -----------------------------------------------------------------------
    # Stratified source selection
    # -----------------------------------------------------------------------

    def _stratified_select(self, candidates: list[dict]) -> list[dict]:
        """Select top items per source quota, then fill any shortfalls.

        Quotas: 7 watchlist, 7 trending, 6 recommended = 20 items.
        Calendar, friend, and affinity items count toward their closest
        quota bucket (calendar/friend fill watchlist slots, etc.).
        If a source doesn't have enough candidates, remaining slots
        are filled from any source by score.
        """
        # Group by source, sorted by score within each group
        by_source: dict[str, list[dict]] = {}
        for c in candidates:
            by_source.setdefault(c.get("source", "watchlist"), []).append(c)
        for lst in by_source.values():
            lst.sort(key=lambda c: c["score"], reverse=True)

        selected: list[dict] = []
        used_ids: set[str] = set()

        # Fill each quota bucket
        for source, quota in SOURCE_QUOTAS.items():
            pool = by_source.get(source, [])
            count = 0
            for c in pool:
                if count >= quota:
                    break
                if c["trakt_id"] not in used_ids:
                    selected.append(c)
                    used_ids.add(c["trakt_id"])
                    count += 1

        target = sum(SOURCE_QUOTAS.values())  # 20

        # Fill remaining slots from any source by score (calendar, friend, etc.)
        if len(selected) < target:
            all_sorted = sorted(candidates, key=lambda c: c["score"], reverse=True)
            for c in all_sorted:
                if len(selected) >= target:
                    break
                if c["trakt_id"] not in used_ids:
                    selected.append(c)
                    used_ids.add(c["trakt_id"])

        # Final sort by score for display order
        selected.sort(key=lambda c: c["score"], reverse=True)

        log.info("smart_queue.stratified_select",
                 total=len(selected),
                 sources={s: sum(1 for c in selected if c.get("source") == s)
                          for s in set(c.get("source", "") for c in selected)})
        return selected

    # -----------------------------------------------------------------------
    # Overflow rotation — swap stale bottom items for fresh overflow
    # -----------------------------------------------------------------------

    async def _rotate_overflow(self, user_id: int, top: list[dict]) -> list[dict]:
        """Replace the bottom 3 unplayed items with top overflow candidates.

        This guarantees that every refresh introduces some fresh content,
        even when the same sources return the same items.
        """
        ROTATE_COUNT = 3

        try:
            overflow = await cache_get(f"queue_overflow:{user_id}")
            if not overflow:
                return top
            # Safety: stale Redis entries from before the double-encoding fix
            # may still be strings — decode them once, then they'll be
            # overwritten with correct format on next _cache_overflow call
            if isinstance(overflow, str):
                import json
                overflow = json.loads(overflow)
        except Exception:
            return top

        # Sort by score ascending — bottom items are rotation candidates
        # Only rotate items that haven't been played
        top_sorted = sorted(top, key=lambda c: c.get("score", 0))
        top_ids = {c["trakt_id"] for c in top}

        rotated_out = []
        swapped_in = []

        for candidate in top_sorted:
            if len(rotated_out) >= ROTATE_COUNT:
                break
            # Don't rotate out items with air dates (time-sensitive)
            if candidate.get("air_date"):
                continue
            # Don't rotate out friend-endorsed items
            if candidate.get("friend_rating"):
                continue
            # Find an overflow item not already in the queue
            for ov in overflow:
                if ov.get("trakt_id") and ov["trakt_id"] not in top_ids:
                    rotated_out.append(candidate["trakt_id"])
                    swapped_in.append(ov)
                    top_ids.discard(candidate["trakt_id"])
                    top_ids.add(ov["trakt_id"])
                    overflow.remove(ov)
                    break

        if not rotated_out:
            return top

        # Build new top list
        new_top = [c for c in top if c["trakt_id"] not in set(rotated_out)]
        new_top.extend(swapped_in)
        new_top.sort(key=lambda c: c.get("score", 0), reverse=True)

        # Update overflow cache (items removed)
        await cache_set(f"queue_overflow:{user_id}", overflow, ttl=86400)

        log.info("smart_queue.overflow_rotation",
                 user_id=user_id,
                 rotated_out=len(rotated_out),
                 swapped_in=[c.get("title", "") for c in swapped_in])

        return new_top

    # -----------------------------------------------------------------------
    # Match candidates to Emby library using LibraryCache
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
            title_matches = [
                item for item in search_results
                if item.get("Name", "").lower() == title.lower()
            ]
            if title_matches:
                if year:
                    # Candidate has a year — only accept an exact year match
                    # to avoid linking e.g. "Resident Evil (2026)" to the
                    # 2002 movie that happens to share the same title
                    year_matches = [
                        item for item in title_matches
                        if item.get("ProductionYear") == year
                    ]
                    if year_matches:
                        return year_matches[0]["Id"]
                    # Wrong year(s) — treat as not in library
                    return None
                else:
                    return title_matches[0]["Id"]

        return None

    # -----------------------------------------------------------------------
    # Persist queue to database
    # -----------------------------------------------------------------------

    async def _persist_queue(self, user: User, items: list[dict]) -> dict[int, str]:
        """Persist queue to DB. Returns {list_index: emby_id} for resolved items.

        Uses pre-resolved Emby IDs from _resolve_and_filter_played when
        available (stored as _resolved_emby_id on each candidate dict).
        Falls back to _find_in_emby for items without a pre-resolved ID.

        Items not found in Emby are still persisted with in_library=False
        so they can be shown in the UI with a 'Send to Radarr/Sonarr' option.
        """
        resolved: dict[int, str] = {}
        in_lib = 0
        async with async_session() as db:
            # Only delete UNPLAYED items — keep played items for feedback history
            await db.execute(
                delete(QueueItem).where(
                    QueueItem.user_id == user.id,
                    QueueItem.played == False,
                )
            )

            # Collect emby IDs of played items so we don't re-insert them
            played_emby_ids: set[str] = set()
            played_rows = (await db.execute(
                select(QueueItem.emby_item_id)
                .where(
                    QueueItem.user_id == user.id,
                    QueueItem.played == True,
                    QueueItem.emby_item_id.isnot(None),
                )
            )).scalars().all()
            played_emby_ids = {eid for eid in played_rows if eid}

            for idx, item in enumerate(items):
                # Use pre-resolved ID if available, otherwise look up
                emby_id = item.get("_resolved_emby_id") or await self._find_in_emby(item)
                in_library = emby_id is not None

                # Skip items that already have a played history record
                if emby_id and emby_id in played_emby_ids:
                    continue

                if in_library:
                    resolved[idx] = emby_id
                    in_lib += 1

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
                    in_library=in_library,
                ))
            await db.commit()

            # Prune played history older than 90 days to keep the table bounded
            history_cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).replace(tzinfo=None)
            await db.execute(
                delete(QueueItem).where(
                    QueueItem.user_id == user.id,
                    QueueItem.played == True,
                    QueueItem.played_at < history_cutoff,
                )
            )
            await db.commit()
        missing = len(items) - in_lib
        log.info("smart_queue.persisted", user_id=user.id,
                 total=len(items), in_library=in_lib, missing=missing)
        return resolved

    # -----------------------------------------------------------------------
    # Sync to Emby collection
    # -----------------------------------------------------------------------

    async def _sync_emby_collection(self, user: User, items: list[dict], resolved_ids: dict[int, str]):
        """Sync the queue to an Emby playlist.

        Uses Series-level Emby IDs for TV shows by default.
        When s01e01_only is enabled, resolves shows to their S01E01 episode ID
        so the playlist links directly to the starting episode.
        """
        s01e01_mode = await self._get_s01e01_setting()

        emby_ids: list[str] = []
        for idx in sorted(resolved_ids):
            emby_id = resolved_ids[idx]
            item = items[idx]
            if s01e01_mode and item.get("item_type") == "show":
                ep_id = await self._resolve_s01e01(emby_id, user.emby_user_id)
                emby_ids.append(ep_id or emby_id)
            else:
                emby_ids.append(emby_id)

        if emby_ids:
            # Use playlists (preserves insertion order) instead of collections
            await self.emby.recreate_playlist(
                "🎯 Smart Up Next", emby_ids, user_id=user.emby_user_id,
            )
            # Log what was added
            type_counts = {}
            for idx in sorted(resolved_ids):
                itype = items[idx].get("item_type", "unknown")
                type_counts[itype] = type_counts.get(itype, 0) + 1
            log.info("smart_queue.playlist_synced",
                     count=len(emby_ids), types=type_counts,
                     s01e01_mode=s01e01_mode)

    async def _get_s01e01_setting(self) -> bool:
        """Read the S01E01 toggle from Redis (queue_settings key)."""
        try:
            data = await cache_get("queue_settings")
            if data and isinstance(data, dict):
                return bool(data.get("s01e01_only", False))
        except Exception:
            pass
        return False

    async def _resolve_s01e01(self, series_emby_id: str, user_id: str) -> str | None:
        """Given a Series Emby ID, find the S01E01 episode ID.

        Queries Emby for episodes under this series, sorted by season number
        then episode number (ParentIndexNumber, IndexNumber), and returns
        the first episode's ID. Returns None if no episodes found.
        """
        try:
            resp = await self.emby.get_items(
                user_id=user_id,
                item_type="Episode",
                parent_id=series_emby_id,
                sort_by="ParentIndexNumber,IndexNumber",
                sort_order="Ascending",
                limit=1,
            )
            episodes = resp.get("Items", [])
            if episodes:
                ep = episodes[0]
                log.debug("smart_queue.s01e01_resolved",
                          series_id=series_emby_id,
                          episode_id=ep["Id"],
                          episode=f"S{ep.get('ParentIndexNumber', '?')}E{ep.get('IndexNumber', '?')}")
                return ep["Id"]
        except Exception as e:
            log.warning("smart_queue.s01e01_resolve_failed",
                        series_id=series_emby_id, error=str(e))
        return None

    # ===================================================================
    # Auto-send missing items to Radarr / Sonarr
    # ===================================================================

    async def _auto_send_missing(self, items: list[dict], resolved_ids: dict[int, str]):
        """If auto-send is enabled, send missing movies to Radarr and/or
        missing shows to Sonarr automatically after queue refresh.

        Reads toggle state from Redis. Defaults to both off.
        Failures are logged but never block the queue refresh.
        """
        try:
            send_settings = await cache_get("auto_send_settings")
            if not send_settings:
                return
            # Safety: stale double-encoded entries are strings
            if isinstance(send_settings, str):
                import json
                send_settings = json.loads(send_settings)
        except Exception:
            return

        radarr_on = send_settings.get("radarr_enabled", False)
        sonarr_on = send_settings.get("sonarr_enabled", False)
        if not radarr_on and not sonarr_on:
            return

        # Collect missing items (not in library = no resolved Emby ID)
        missing_movies = []
        missing_shows = []
        for idx, item in enumerate(items):
            if idx in resolved_ids:
                continue  # already in library
            ids = item.get("ids", {})
            if item.get("item_type") == "movie" and radarr_on:
                missing_movies.append({
                    "tmdb_id": ids.get("tmdb"),
                    "imdb_id": ids.get("imdb"),
                    "title": item.get("title", ""),
                    "year": item.get("year"),
                })
            elif item.get("item_type") == "show" and sonarr_on:
                missing_shows.append({
                    "tvdb_id": ids.get("tvdb"),
                    "imdb_id": ids.get("imdb"),
                    "title": item.get("title", ""),
                    "year": item.get("year"),
                })

        # Send to Radarr
        if missing_movies:
            try:
                from app.utils.radarr_client import RadarrClient
                from app.utils.redis_cache import get_redis
                r = await get_redis()
                raw_servers = await r.get("radarr_servers")
                if raw_servers:
                    servers = json.loads(raw_servers)
                    if servers:
                        srv = servers[0]
                        client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                        sent = 0
                        try:
                            for movie in missing_movies:
                                try:
                                    result = await client.add_movie(
                                        tmdb_id=movie.get("tmdb_id"),
                                        imdb_id=movie.get("imdb_id"),
                                        title=movie.get("title", ""),
                                        year=movie.get("year"),
                                    )
                                    if result.get("status") == "ok":
                                        sent += 1
                                except Exception:
                                    log.debug("auto_send.radarr_item_failed",
                                              title=movie.get("title"))
                        finally:
                            await client.close()
                        log.info("auto_send.radarr_done",
                                 sent=sent, total=len(missing_movies))
            except Exception:
                log.warning("auto_send.radarr_failed")

        # Send to Sonarr
        if missing_shows:
            try:
                from app.utils.sonarr_client import SonarrClient
                from app.utils.redis_cache import get_redis
                r = await get_redis()
                raw_servers = await r.get("sonarr_servers")
                if raw_servers:
                    servers = json.loads(raw_servers)
                    if servers:
                        srv = servers[0]
                        client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                        sent = 0
                        try:
                            for show in missing_shows:
                                try:
                                    result = await client.add_series(
                                        tvdb_id=show.get("tvdb_id"),
                                        imdb_id=show.get("imdb_id"),
                                        title=show.get("title", ""),
                                        year=show.get("year"),
                                    )
                                    if result.get("status") == "ok":
                                        sent += 1
                                except Exception:
                                    log.debug("auto_send.sonarr_item_failed",
                                              title=show.get("title"))
                        finally:
                            await client.close()
                        log.info("auto_send.sonarr_done",
                                 sent=sent, total=len(missing_shows))
            except Exception:
                log.warning("auto_send.sonarr_failed")

    # ===================================================================
    # Feedback Loop — learned weights
    # ===================================================================

    async def _load_weights(self, user_id: int) -> dict:
        """Load per-user learned weights from Redis, or return defaults."""
        try:
            data = await cache_get(f"queue_weights:{user_id}")
            if data and isinstance(data, dict):
                return data
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
            item.played_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).replace(tzinfo=None)
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
        await cache_set(f"queue_weights:{user_id}", weights, ttl=86400 * 365)



    # ===================================================================
    # Overflow cache — pre-scored backfill candidates
    # ===================================================================

    async def _cache_overflow(self, user_id: int, items: list[dict]):
        """Cache overflow candidates (ranked 31-60) for real-time backfill."""
        # Pre-resolve Emby IDs so backfill doesn't need to search later
        resolved = []
        for item in items:
            emby_id = await self._find_in_emby(item)
            if emby_id:
                item["_emby_id"] = emby_id
                resolved.append(item)
        if resolved:
            await cache_set(f"queue_overflow:{user_id}", resolved, ttl=86400)
            log.info("smart_queue.overflow_cached", user_id=user_id, count=len(resolved))

    async def _pop_best_overflow(self, user_id: int, min_score: float) -> dict | None:
        """Pop the highest-scoring overflow item that beats min_score."""
        items = await cache_get(f"queue_overflow:{user_id}")
        if not items:
            return None
        # Safety: stale double-encoded entries are strings
        if isinstance(items, str):
            import json
            items = json.loads(items)

        # Already sorted by score descending from the full run
        best = items[0]
        if best.get("score", 0) >= min_score:
            # Remove it from overflow
            items.pop(0)
            await cache_set(f"queue_overflow:{user_id}", items, ttl=86400)
            return best
        return None

    # ===================================================================
    # Real-time backfill — called from webhook on watched event
    # ===================================================================

    async def remove_and_backfill(self, user_id: int, emby_item_id: str):
        """Remove a watched item from the active queue and backfill with the next best candidate.

        Called from the webhook handler after record_play.
        The item is already marked played=True by record_play() — we keep it
        for feedback history and just backfill the gap.
        Steps:
          1. Verify the item is marked played (don't delete it)
          2. Try overflow pool first (pre-scored, no API calls)
          3. If overflow is empty or too low-scoring, pull fresh trending
          4. Re-sync the Emby playlist (excludes played items)
        """
        await self._ensure_emby()
        try:
            await self._remove_and_backfill_inner(user_id, emby_item_id)
        finally:
            await self._close_emby()

    async def _remove_and_backfill_inner(self, user_id: int, emby_item_id: str):
        async with async_session() as db:
            # Confirm the item exists and is played — we keep it for history
            watched = (await db.execute(
                select(QueueItem).where(
                    QueueItem.user_id == user_id,
                    QueueItem.emby_item_id == emby_item_id,
                    QueueItem.played == True,
                )
            )).scalar_one_or_none()

            if not watched:
                return  # not in queue or not yet marked played

            watched_title = watched.title

            log.info("smart_queue.item_played_kept_for_history",
                     user_id=user_id, title=watched_title)

            # Get remaining ACTIVE (unplayed) queue to find the lowest score
            remaining = (await db.execute(
                select(QueueItem)
                .where(
                    QueueItem.user_id == user_id,
                    QueueItem.played == False,
                )
                .order_by(QueueItem.score.desc())
            )).scalars().all()

        min_score = remaining[-1].score if remaining else 0.0

        # Try overflow first (cheap — no API calls)
        replacement = await self._pop_best_overflow(user_id, min_score)

        # If overflow is empty/exhausted, try fresh trending
        if not replacement:
            replacement = await self._fetch_trending_replacement(user_id, remaining)

        if replacement:
            emby_id = replacement.get("_emby_id") or await self._find_in_emby(replacement)
            if emby_id:
                async with async_session() as db:
                    db.add(QueueItem(
                        user_id=user_id,
                        emby_item_id=emby_id,
                        title=replacement["title"],
                        item_type=replacement["item_type"],
                        source=replacement["source"],
                        score=replacement["score"],
                        trakt_trending_rank=replacement.get("trending_rank"),
                        trakt_rating=replacement.get("friend_rating"),
                        metadata_json=replacement,
                    ))
                    await db.commit()
                log.info("smart_queue.backfill_added",
                         user_id=user_id, title=replacement["title"],
                         score=replacement["score"], source=replacement["source"])

        # Re-sync the Emby playlist with current queue
        await self._resync_playlist_from_db(user_id)

    async def _fetch_trending_replacement(self, user_id: int,
                                          current_items: list) -> dict | None:
        """Pull fresh trending from Trakt and return the best unwatched candidate
        not already in the queue."""
        # Get the user for auth
        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
        if not user or not user.trakt_access_token:
            return None

        # Set of Emby IDs already in queue
        existing_emby_ids = {item.emby_item_id for item in current_items}

        trakt = TraktClient(access_token=user.trakt_access_token)
        try:
            # Get watched history to exclude
            watched_ids = await self._get_watched_trakt_ids(trakt)

            weights = await self._load_weights(user_id)
            for kind in ("shows", "movies"):
                trending = await trakt.get_trending(kind=kind, limit=15)
                for rank, entry in enumerate(trending):
                    item = entry.get("movie") or entry.get("show") or {}
                    tid = str(item.get("ids", {}).get("trakt", ""))
                    if not tid or tid in watched_ids:
                        continue
                    candidate = {
                        "trakt_id": tid,
                        "title": item.get("title", ""),
                        "year": item.get("year"),
                        "item_type": "movie" if kind == "movies" else "show",
                        "ids": item.get("ids", {}),
                        "source": "trending",
                        "source_score": 1.0 - rank / 15,
                        "trending_rank": rank + 1,
                    }
                    # Score it
                    self._score_candidates([candidate], weights)
                    # Check it exists in Emby and isn't already queued
                    emby_id = await self._find_in_emby(candidate)
                    if emby_id and emby_id not in existing_emby_ids:
                        candidate["_emby_id"] = emby_id
                        return candidate
        except Exception:
            log.warning("smart_queue.trending_backfill_failed", user_id=user_id)
        finally:
            await trakt.close()
        return None

    async def _resync_playlist_from_db(self, user_id: int):
        """Rebuild the Emby playlist from current DB queue state."""
        owned = self.emby is None
        await self._ensure_emby()
        try:
            async with async_session() as db:
                user = (await db.execute(
                    select(User).where(User.id == user_id)
                )).scalar_one_or_none()
                if not user:
                    return

                items = (await db.execute(
                    select(QueueItem)
                    .where(
                        QueueItem.user_id == user_id,
                        QueueItem.played == False,
                    )
                    .order_by(QueueItem.score.desc())
                )).scalars().all()

            emby_ids = [item.emby_item_id for item in items
                        if item.emby_item_id and item.in_library is not False]
            if emby_ids:
                await self.emby.recreate_playlist(
                    "🎯 Smart Up Next", emby_ids, user_id=user.emby_user_id,
                )
                log.info("smart_queue.playlist_resynced", user_id=user_id, count=len(emby_ids))
        finally:
            if owned:
                await self._close_emby()

    # ===================================================================
    # Watched history filter
    # ===================================================================

    async def _get_watched_trakt_ids(self, trakt: TraktClient) -> set[str]:
        """Fetch Trakt watched history and return set of watched MOVIE Trakt IDs.

        Only movies are filtered at the Trakt level. Shows use Emby's
        episode-level played status instead (a partially-watched show
        with unwatched episodes should stay in the queue).

        Uses both /users/me/watched (all-time) and /users/me/history (recent)
        to build the most complete set.
        """
        watched_ids: set[str] = set()

        # 1. All-time watched movies
        try:
            watched = await trakt.get_watched(kind="movies")
            for entry in watched:
                item = entry.get("movie") or {}
                tid = str(item.get("ids", {}).get("trakt", ""))
                if tid:
                    watched_ids.add(tid)
            log.info("smart_queue.watched_movies_from_watched",
                     count=len(watched_ids))
        except Exception as e:
            log.warning("smart_queue.watched_fetch_failed", error=str(e)[:120])

        # 2. Recent movie history (catches items that may not appear in watched yet)
        try:
            history = await trakt.get_history(kind="movies", limit=200)
            for entry in history:
                item = entry.get("movie") or {}
                tid = str(item.get("ids", {}).get("trakt", ""))
                if tid:
                    watched_ids.add(tid)
            log.info("smart_queue.watched_movies_total",
                     count=len(watched_ids))
        except Exception as e:
            log.warning("smart_queue.history_fetch_failed", error=str(e)[:120])

        return watched_ids

    async def _resolve_and_filter_played(
        self, candidates: list[dict], user: User,
    ) -> list[dict]:
        """Resolve Emby IDs and filter out fully-played items.

        For each candidate:
        - Look up its Emby library item (Series for shows, Movie for movies)
        - Store the resolved emby_id on the candidate dict (avoids double lookup)
        - Query Emby for UserData played status
        - Movies: filter if Played=true
        - Shows: filter only if UnplayedItemCount=0 (all episodes watched)
        - Items not in library pass through (shown with 'not in library' badge)
        """
        # Step 1: resolve Emby IDs
        for c in candidates:
            emby_id = await self._find_in_emby(c)
            c["_resolved_emby_id"] = emby_id  # None if not in library

        # Step 2: batch-check played status for items with Emby IDs
        in_library_ids = [
            c["_resolved_emby_id"] for c in candidates
            if c["_resolved_emby_id"]
        ]

        played_set: set[str] = set()
        fully_watched_shows: set[str] = set()

        if in_library_ids and user.emby_user_id:
            try:
                # Batch fetch with UserData (includes Played, UnplayedItemCount)
                items = await self.emby.get_user_items_by_ids(
                    user.emby_user_id, in_library_ids,
                )
                for item in items:
                    eid = str(item.get("Id", ""))
                    user_data = item.get("UserData", {})
                    item_type = item.get("Type", "")

                    if item_type == "Series":
                        # Show is fully watched if no unwatched items remain
                        unplayed = user_data.get("UnplayedItemCount", 1)
                        if unplayed == 0:
                            fully_watched_shows.add(eid)
                            log.debug("smart_queue.show_fully_watched",
                                      title=item.get("Name"), emby_id=eid)
                    else:
                        # Movie (or other): check simple Played flag
                        if user_data.get("Played", False):
                            played_set.add(eid)
                            log.debug("smart_queue.movie_played_in_emby",
                                      title=item.get("Name"), emby_id=eid)
            except Exception as e:
                log.warning("smart_queue.emby_played_check_failed",
                            error=str(e)[:120])

        # Step 3: filter
        before = len(candidates)
        result = []
        for c in candidates:
            eid = c.get("_resolved_emby_id")
            if eid and eid in played_set:
                log.info("smart_queue.emby_played_filtered",
                         title=c.get("title"), emby_id=eid)
                continue
            if eid and eid in fully_watched_shows:
                log.info("smart_queue.show_fully_watched_filtered",
                         title=c.get("title"), emby_id=eid)
                continue
            result.append(c)

        removed = before - len(result)
        if removed:
            log.info("smart_queue.emby_played_filter",
                     user_id=user.id, removed=removed,
                     movies=len(played_set), shows=len(fully_watched_shows))

        return result

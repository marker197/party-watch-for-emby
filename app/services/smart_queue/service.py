"""Service #1 — Smart Watch Queue.

Daily task that:
1. Pulls user's Simkl watchlist, trending, friends' ratings, calendar
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
from app.utils.simkl_client import SimklClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import cache_get, cache_set
from app.utils.secure_redis import secure_get, secure_set
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
    "mdb_upnext": 6.0,
}

# Source quota ratios. The queue length is user-configurable (20/30/40/50);
# quotas are derived from these shares so the split stays proportional at
# every size. Every gathered source has a share — calendar, friend and
# mdb_upnext get small ones so a good spread reaches the final queue
# instead of being crowded out by watchlist/trending on score alone.
# Quotas are ceilings, not guarantees: if a source returns nothing, its
# slots fall through to the shortfall fill. Tune these freely — they only
# need to sum to 1.0.
SOURCE_QUOTA_RATIOS = {
    "watchlist": 0.28,
    "trending": 0.26,
    "recommended": 0.22,
    "calendar": 0.10,
    "friend": 0.07,
    "mdb_upnext": 0.07,
}

# Availability bonuses applied at scoring time. Source weights run 5–10 and
# the airing-soon boost is +4.0, so +3.0 is meaningful without being
# decisive: an in-library trending item (6.0 + 3.0) edges out a watchlist
# item that isn't in the library (10.0 + 0.0) only once other factors tip it.
IN_LIBRARY_BONUS = 3.0
IN_ARR_BONUS = 1.0

# Allowed queue sizes (enforced here and in the settings endpoint)
VALID_QUEUE_SIZES = (20, 30, 40, 50)
DEFAULT_QUEUE_SIZE = 20


def build_source_quotas(size: int) -> dict[str, int]:
    """Split `size` across the quota sources by ratio.

    Uses largest-remainder allocation so the parts always sum to exactly
    `size` — a plain round() can drift over or under the target.

        20 -> watchlist 6,  trending 5,  recommended 4,
              calendar 2, friend 1, mdb_upnext 2
        50 -> watchlist 14, trending 13, recommended 11,
              calendar 5, friend 4, mdb_upnext 3
    """
    exact = {s: size * ratio for s, ratio in SOURCE_QUOTA_RATIOS.items()}
    quotas = {s: int(v) for s, v in exact.items()}

    # Hand out the leftover slots to the largest fractional remainders
    remaining = size - sum(quotas.values())
    if remaining > 0:
        by_remainder = sorted(
            exact, key=lambda s: (exact[s] - int(exact[s]), SOURCE_QUOTA_RATIOS[s]),
            reverse=True,
        )
        for s in by_remainder[:remaining]:
            quotas[s] += 1
    return quotas


# Backwards-compatible default table (size 20)
SOURCE_QUOTAS = build_source_quotas(DEFAULT_QUEUE_SIZE)

# Candidate keys for the MDBList /upnext payload. The endpoint's response
# shape isn't formally documented and the web UI splits results into
# "Up Next" and "Upcoming" tabs, so the wrapper key is uncertain. Try the
# in-progress keys first — "upcoming" is release/premiere data, which the
# calendar source already covers, so it's only a last resort.
_UPNEXT_LIST_KEYS = (
    "upnext", "up_next", "next", "shows", "items", "results", "data",
)

# Where the show object may live inside an entry. Up Next rows are
# episode-level ("S02E01 7th Quarter"), so the series may be nested.
_UPNEXT_SHOW_KEYS = ("show", "series", "tv")


def _unwrap_upnext(data) -> list:
    """Normalise the MDBList /upnext response to a list of entries."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in _UPNEXT_LIST_KEYS:
        val = data.get(key)
        if isinstance(val, list) and val:
            return val
    # Last resort: a single list-valued key of dicts, whatever it's called.
    for val in data.values():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val
    return []


def _upnext_show_and_ids(entry) -> tuple[dict, dict]:
    """Return (show_object, ids) for an /upnext entry.

    Entries are episode-level, so series IDs may sit on a nested show
    object, on the entry itself, or be flattened as ``show_tmdb`` style
    keys. Prefer whichever actually carries usable IDs.
    """
    if not isinstance(entry, dict):
        return {}, {}

    for key in _UPNEXT_SHOW_KEYS:
        nested = entry.get(key)
        if isinstance(nested, dict):
            ids = nested.get("ids")
            if isinstance(ids, dict) and ids:
                return nested, ids

    ids = entry.get("ids")
    if isinstance(ids, dict) and ids:
        return entry, ids

    # Flattened provider IDs on the entry (e.g. show_tmdb / imdb_id).
    flat: dict = {}
    for provider in ("simkl", "imdb", "tmdb", "tvdb"):
        for candidate in (
            f"show_{provider}", f"{provider}_id", provider, f"{provider}id",
        ):
            val = entry.get(candidate)
            if val:
                flat[provider] = val
                break

    show = entry
    for key in _UPNEXT_SHOW_KEYS:
        nested = entry.get(key)
        if isinstance(nested, dict):
            show = nested
            break
    return show, flat


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
                    select(User).where(User.simkl_access_token.isnot(None))
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

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )
        try:
            # Check rate limit budget
            info = simkl.get_rate_limit_info()
            if info["remaining"] < 50:
                log.warning("smart_queue.rate_limit_low", remaining=info["remaining"])
                return

            # User-configurable queue length (20/30/40/50) and playlist toggle
            queue_settings = await self._get_queue_settings()
            queue_size = queue_settings["queue_size"]

            # Prune MOVIE items already in Simkl watched history
            # Shows are NOT filtered here — they use Emby episode awareness instead
            watched_movie_ids = await self._get_watched_simkl_ids(simkl)

            candidates = await self._gather_candidates(simkl, user, size=queue_size)

            # Filter out already-watched movies (shows skip this filter)
            before = len(candidates)
            filtered = []
            for c in candidates:
                tid = str(c.get("simkl_id", ""))
                if tid and tid in watched_movie_ids and c.get("item_type") == "movie":
                    log.debug("smart_queue.candidate_filtered",
                              title=c.get("title"), simkl_id=tid, source=c.get("source"))
                else:
                    filtered.append(c)
            candidates = filtered
            if before != len(candidates):
                log.info("smart_queue.filtered_watched",
                         user_id=user.id, removed=before - len(candidates),
                         watched_set_size=len(watched_movie_ids))

            # Pre-resolve Emby IDs and check played status
            candidates = await self._resolve_and_filter_played(candidates, user)

            # Tag Radarr/Sonarr presence so scoring can prefer items that
            # are actually available (in Emby > in *arr > neither)
            radarr_tmdb, sonarr_tvdb = await self._get_arr_id_sets()
            self._tag_arr_presence(candidates, radarr_tmdb, sonarr_tvdb)

            # Load learned weights and staleness counters
            weights = await self._load_weights(user.id)
            staleness = await self._load_staleness(user.id)
            scored = self._score_candidates(candidates, weights, staleness=staleness)

            # Source-stratified selection, sized by the user's queue_size setting
            top = self._stratified_select(scored, size=queue_size)

            # Overflow rotation — swap the stalest bottom items for fresh overflow
            top = await self._rotate_overflow(user.id, top, size=queue_size)

            # Remaining candidates (not selected) become overflow for backfill
            top_ids = {c["simkl_id"] for c in top}
            leftover = sorted(
                [c for c in scored if c["simkl_id"] not in top_ids],
                key=lambda c: c["score"], reverse=True,
            )
            overflow = leftover[:queue_size + 10]
            await self._cache_overflow(user.id, overflow)

            # Update staleness counters: increment for items still in queue,
            # remove items no longer present
            new_staleness = {}
            for c in top:
                tid = str(c.get("simkl_id", ""))
                if tid:
                    new_staleness[tid] = staleness.get(tid, 0) + 1
            await self._save_staleness(user.id, new_staleness)

            resolved_ids = await self._persist_queue(user, top)
            if queue_settings["create_playlist"]:
                await self._sync_emby_collection(user, top, resolved_ids)
            else:
                # Playlist creation disabled — the queue still builds and
                # persists, we just leave Emby alone. Any existing
                # "🎯 Smart Up Next" playlist is left in place, not deleted.
                log.info("smart_queue.playlist_skipped", user_id=user.id,
                         reason="create_playlist disabled")

            # Auto-send missing items to Radarr/Sonarr if enabled
            await self._auto_send_missing(top, resolved_ids)


            log.info("smart_queue.user_done", user=user.emby_username,
                     items=len(top), overflow=len(overflow))
        finally:
            await simkl.close()

    # -----------------------------------------------------------------------
    # Gather candidates from multiple Simkl sources
    # -----------------------------------------------------------------------

    async def _gather_candidates(self, simkl: SimklClient, user: User,
                                 size: int = DEFAULT_QUEUE_SIZE) -> list[dict]:
        candidates: dict[str, dict] = {}

        # Per-source fetch depth. A 50-item queue can't fill an 18-item
        # watchlist quota from a 15-item trending page, so the pools scale
        # with the target. Same number of API calls, larger pages.
        pool_limit = max(15, size)

        # 1. Watchlist items
        watchlist = await simkl.get_watchlist()
        for entry in watchlist:
            item = entry.get("movie") or entry.get("show") or entry
            tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
            if tid:
                candidates[tid] = {
                    "simkl_id": tid,
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
            trending = await simkl.get_trending(kind=kind, limit=pool_limit, page=trending_page)
            for rank, entry in enumerate(trending):
                item = entry.get("movie") or entry.get("show") or entry
                tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                if tid and tid not in candidates:
                    candidates[tid] = {
                        "simkl_id": tid,
                        "title": item.get("title", ""),
                        "year": item.get("year"),
                        "item_type": "movie" if kind == "movies" else "show",
                        "ids": item.get("ids", {}),
                        "source": "trending",
                        "source_score": 1.0 - rank / 15,
                        "trending_rank": rank + 1 + ((trending_page - 1) * 15),
                    }

        # 3. Recommended (personalised based on user's Simkl ratings)
        for kind in ("shows", "movies"):
            try:
                recs = await simkl.get_recommended(kind=kind, limit=pool_limit)
                for rank, entry in enumerate(recs):
                    # Recommended endpoint returns items directly (not wrapped)
                    item = entry.get("movie") or entry.get("show") or entry
                    tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                    if tid and tid not in candidates:
                        candidates[tid] = {
                            "simkl_id": tid,
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
            calendar = await simkl.get_my_shows(start_date=today, days=14)
            for entry in calendar:
                show = entry.get("show", {})
                tid = str(show.get("ids", {}).get("simkl") or show.get("ids", {}).get("simkl_id") or "")
                if tid and tid not in candidates:
                    candidates[tid] = {
                        "simkl_id": tid,
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
            friends = await simkl.get_friends()
            for friend in friends[:10]:
                fname = friend.get("user", {}).get("ids", {}).get("slug", "")
                if not fname:
                    continue
                try:
                    friend_ratings = await simkl.get_friend_ratings(fname, kind="all")
                    for r in friend_ratings:
                        if r.get("rating", 0) < 8:
                            continue
                        item = r.get("movie") or r.get("show") or {}
                        tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                        if tid and tid not in candidates:
                            candidates[tid] = {
                                "simkl_id": tid,
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

        # 6. MDBList "Up Next" — in-progress shows with next unwatched episode
        try:
            from app.utils.mdblist_client import MDBListClient
            mdb_key = await secure_get("mdblist_api_key")
            if mdb_key:
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    upnext_data = await mdb.get_upnext(limit=50)
                    upnext_items = _unwrap_upnext(upnext_data)
                    added = 0
                    for rank, entry in enumerate(upnext_items):
                        show, ids = _upnext_show_and_ids(entry)
                        # Try to match to a Simkl ID for dedup against other sources
                        simkl_id = ids.get("simkl") or ids.get("simkl_id")
                        imdb_id = ids.get("imdb") or ids.get("imdbid")
                        tmdb_id = ids.get("tmdb") or ids.get("tmdbid")
                        # Use IMDB as dedup key if no Simkl ID
                        tid = str(simkl_id or imdb_id or tmdb_id or "")
                        if tid and tid not in candidates:
                            candidates[tid] = {
                                "simkl_id": tid,
                                "title": show.get("title") or show.get("name", ""),
                                "year": show.get("year"),
                                "item_type": "show",
                                "ids": ids,
                                "source": "mdb_upnext",
                                "source_score": 1.0 - rank / 50,
                            }
                            added += 1
                    log.info("smart_queue.mdb_upnext_gathered",
                             count=len(upnext_items), added=added)
                    # Diagnostic: the /upnext response shape is not formally
                    # documented. If we parsed nothing, dump the actual shape
                    # so the next log upload tells us what to key off.
                    if not upnext_items:
                        log.warning(
                            "smart_queue.mdb_upnext_unparsed",
                            payload_type=type(upnext_data).__name__,
                            top_level_keys=(
                                list(upnext_data.keys())[:15]
                                if isinstance(upnext_data, dict) else None
                            ),
                        )
                    elif not added:
                        sample = upnext_items[0]
                        log.warning(
                            "smart_queue.mdb_upnext_no_ids",
                            count=len(upnext_items),
                            entry_keys=(
                                list(sample.keys())[:15]
                                if isinstance(sample, dict) else None
                            ),
                            sample_ids=_upnext_show_and_ids(sample)[1],
                        )
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("smart_queue.mdb_upnext_skip", error=str(e))

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
        """Load per-item staleness counters (simkl_id → consecutive refresh count)."""
        try:
            data = await cache_get(f"queue_staleness:{user_id}")
            if data and isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    async def _load_blocklist(self, user_id: int) -> set[str]:
        """Load permanently blocked Simkl IDs for a user."""
        from app.models.schema import QueueBlocklist
        async with async_session() as db:
            rows = (await db.execute(
                select(QueueBlocklist.simkl_id).where(QueueBlocklist.user_id == user_id)
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

            # availability bonus — prefer things you can actually watch now.
            # In the Emby library beats merely being in Radarr/Sonarr (where
            # it may still be unreleased, unmonitored, or not downloaded).
            if c.get("_resolved_emby_id"):
                score += IN_LIBRARY_BONUS
            elif c.get("_in_arr"):
                score += IN_ARR_BONUS

            # staleness decay — reduce score for items that have sat unplayed
            if staleness:
                days_stale = staleness.get(str(c.get("simkl_id", "")), 0)
                if days_stale > 0:
                    # -1.5 points per day stale, capped at -8
                    penalty = min(days_stale * 1.5, 8.0)
                    score -= penalty

            c["score"] = round(score, 2)
        return candidates

    # -----------------------------------------------------------------------
    # Stratified source selection
    # -----------------------------------------------------------------------

    def _stratified_select(self, candidates: list[dict],
                           size: int = DEFAULT_QUEUE_SIZE) -> list[dict]:
        """Select top items per source quota, then fill any shortfalls.

        Quotas are derived from `size` by ratio — at the default 20 that's
        7 watchlist, 7 trending, 6 recommended. Calendar, friend, affinity
        and mdb_upnext items have no quota of their own; they enter through
        the shortfall fill, which takes the highest-scoring remaining
        candidates from any source once the quota buckets are exhausted.
        """
        quotas = build_source_quotas(size)
        # Group by source, sorted by score within each group
        by_source: dict[str, list[dict]] = {}
        for c in candidates:
            by_source.setdefault(c.get("source", "watchlist"), []).append(c)
        for lst in by_source.values():
            lst.sort(key=lambda c: c["score"], reverse=True)

        selected: list[dict] = []
        used_ids: set[str] = set()

        # Fill each quota bucket
        for source, quota in quotas.items():
            pool = by_source.get(source, [])
            count = 0
            for c in pool:
                if count >= quota:
                    break
                if c["simkl_id"] not in used_ids:
                    selected.append(c)
                    used_ids.add(c["simkl_id"])
                    count += 1

        target = size

        # Fill remaining slots from any source by score (calendar, friend, etc.)
        if len(selected) < target:
            all_sorted = sorted(candidates, key=lambda c: c["score"], reverse=True)
            for c in all_sorted:
                if len(selected) >= target:
                    break
                if c["simkl_id"] not in used_ids:
                    selected.append(c)
                    used_ids.add(c["simkl_id"])

        # Final sort by score for display order
        selected.sort(key=lambda c: c["score"], reverse=True)

        log.info("smart_queue.stratified_select",
                 total=len(selected), target=target,
                 in_library=sum(1 for c in selected if c.get("_resolved_emby_id")),
                 in_arr=sum(1 for c in selected
                            if not c.get("_resolved_emby_id") and c.get("_in_arr")),
                 sources={s: sum(1 for c in selected if c.get("source") == s)
                          for s in set(c.get("source", "") for c in selected)})
        return selected

    # -----------------------------------------------------------------------
    # Overflow rotation — swap stale bottom items for fresh overflow
    # -----------------------------------------------------------------------

    async def _rotate_overflow(self, user_id: int, top: list[dict],
                               size: int = DEFAULT_QUEUE_SIZE) -> list[dict]:
        """Replace the lowest-scoring unplayed items with top overflow.

        This guarantees that every refresh introduces some fresh content,
        even when the same sources return the same items. The count scales
        with queue length so the proportion of churn stays roughly constant
        (3 at size 20, 7 at size 50).
        """
        ROTATE_COUNT = max(3, size // 7)

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
        top_ids = {c["simkl_id"] for c in top}

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
                if ov.get("simkl_id") and ov["simkl_id"] not in top_ids:
                    rotated_out.append(candidate["simkl_id"])
                    swapped_in.append(ov)
                    top_ids.discard(candidate["simkl_id"])
                    top_ids.add(ov["simkl_id"])
                    overflow.remove(ov)
                    break

        if not rotated_out:
            return top

        # Build new top list
        new_top = [c for c in top if c["simkl_id"] not in set(rotated_out)]
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
        """Match a Simkl item to an Emby library item via LibraryCache."""
        ids = candidate.get("ids", {})

        # Try provider IDs via cache first (sub-millisecond)
        for provider_type, simkl_key in [("Tmdb", "tmdb"), ("Imdb", "imdb"), ("Tvdb", "tvdb")]:
            pid = ids.get(simkl_key)
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
            # Year mismatch is common for shows — CDN may report latest season year
            # while Emby stores the series premiere year. Try without year.
            if year:
                cached = await LibraryCache.find_by_title(title, year=None)
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
                    simkl_trending_rank=item.get("trending_rank"),
                    simkl_rating=item.get("friend_rating"),
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

    async def _get_arr_id_sets(self) -> tuple[set[str], set[str]]:
        """Return (radarr TMDB ids, sonarr TVDB ids) as string sets.

        Reads the same `arr_library_ids_v2` cache the dashboard uses
        (populated by GET /api/arr-library, 60s TTL). Returns empty sets
        when the cache is cold or *arr isn't configured — the library
        bonus then simply doesn't apply rather than failing the refresh.
        """
        try:
            cached = await cache_get("arr_library_ids_v2")
            if isinstance(cached, str):
                import json as _json
                cached = _json.loads(cached)
            if not isinstance(cached, dict):
                return set(), set()
            radarr = {str(i) for i in (cached.get("radarr_tmdb") or [])}
            sonarr = {str(i) for i in (cached.get("sonarr_tvdb") or [])}
            return radarr, sonarr
        except Exception:
            return set(), set()

    def _tag_arr_presence(self, candidates: list[dict],
                          radarr_tmdb: set[str], sonarr_tvdb: set[str]) -> None:
        """Mark each candidate with whether it's already in Radarr/Sonarr."""
        if not radarr_tmdb and not sonarr_tvdb:
            return
        for c in candidates:
            ids = c.get("ids", {}) or {}
            tmdb = str(ids.get("tmdb") or "")
            tvdb = str(ids.get("tvdb") or "")
            c["_in_arr"] = bool(
                (tmdb and tmdb in radarr_tmdb) or (tvdb and tvdb in sonarr_tvdb)
            )

    async def _get_queue_settings(self) -> dict:
        """Read the queue_settings blob from Redis, with safe defaults."""
        settings = {
            "s01e01_only": False,
            "queue_size": DEFAULT_QUEUE_SIZE,
            "create_playlist": True,
        }
        try:
            data = await cache_get("queue_settings")
            if isinstance(data, str):
                import json as _json
                data = _json.loads(data)
            if data and isinstance(data, dict):
                settings["s01e01_only"] = bool(data.get("s01e01_only", False))
                settings["create_playlist"] = bool(data.get("create_playlist", True))
                size = data.get("queue_size", DEFAULT_QUEUE_SIZE)
                try:
                    size = int(size)
                except (TypeError, ValueError):
                    size = DEFAULT_QUEUE_SIZE
                if size in VALID_QUEUE_SIZES:
                    settings["queue_size"] = size
        except Exception:
            pass
        return settings

    async def _get_s01e01_setting(self) -> bool:
        """Read the S01E01 toggle from Redis (queue_settings key)."""
        return (await self._get_queue_settings())["s01e01_only"]

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
                raw_servers = await secure_get("radarr_servers")
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
                raw_servers = await secure_get("sonarr_servers")
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
        # Reuse Emby IDs already resolved by _resolve_and_filter_played
        resolved = []
        for item in items:
            emby_id = item.get("_resolved_emby_id")
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
                        simkl_trending_rank=replacement.get("trending_rank"),
                        simkl_rating=replacement.get("friend_rating"),
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
        """Pull fresh trending from Simkl and return the best unwatched candidate
        not already in the queue."""
        # Get the user for auth
        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
        if not user or not user.simkl_access_token:
            return None

        # Set of Emby IDs already in queue
        existing_emby_ids = {item.emby_item_id for item in current_items}

        simkl = SimklClient(access_token=user.simkl_access_token)
        try:
            # Get watched history to exclude
            watched_ids = await self._get_watched_simkl_ids(simkl)

            weights = await self._load_weights(user_id)
            for kind in ("shows", "movies"):
                trending = await simkl.get_trending(kind=kind, limit=15)
                for rank, entry in enumerate(trending):
                    item = entry.get("movie") or entry.get("show") or entry
                    tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                    if not tid or tid in watched_ids:
                        continue
                    candidate = {
                        "simkl_id": tid,
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
            await simkl.close()
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

    async def _get_watched_simkl_ids(self, simkl: SimklClient) -> set[str]:
        """Fetch Simkl watched history and return set of watched MOVIE Simkl IDs.

        Only movies are filtered at the Simkl level. Shows use Emby's
        episode-level played status instead (a partially-watched show
        with unwatched episodes should stay in the queue).

        Uses both /users/me/watched (all-time) and /users/me/history (recent)
        to build the most complete set.
        """
        watched_ids: set[str] = set()

        # 1. All-time watched movies
        try:
            watched = await simkl.get_watched(kind="movies")
            for entry in watched:
                item = entry.get("movie") or {}
                tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                if tid:
                    watched_ids.add(tid)
            log.info("smart_queue.watched_movies_from_watched",
                     count=len(watched_ids))
        except Exception as e:
            log.warning("smart_queue.watched_fetch_failed", error=str(e)[:120])

        # 2. Recent movie history (catches items that may not appear in watched yet)
        try:
            history = await simkl.get_history(kind="movies", limit=200)
            for entry in history:
                item = entry.get("movie") or {}
                tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
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

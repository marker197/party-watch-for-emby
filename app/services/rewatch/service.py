"""
Rewatch Recommender Service

Suggests items the user has already watched and rated highly that are worth
rewatching.  Scoring factors:

  - Base pool: items rated >= threshold (default 8), last watched > N months ago
  - Seasonal boost: genre matched to current month
  - Anniversary boost: watched within ±7 days of today in a prior year
  - Staleness weight: longer since last watch = higher score (capped)
  - Decay for dismissed items

Data sources (all active, merged with deduplication):
  1. MDBList ratings + history (has a rating for every watched item)
  2. Simkl ratings + history (richer metadata when available)
  3. Emby LastPlayedDate + UserRating (local fallback — single date, but PlayCount)
  Dedup priority: Simkl > MDBList > Emby (first seen wins by IMDB ID)

Output: top 30 items persisted to Redis with 24h TTL.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import structlog

from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Seasonal genre mapping
# ---------------------------------------------------------------------------

_SEASONAL_GENRES: dict[int, list[str]] = {
    1:  ["drama", "biography"],                        # January — fresh start
    2:  ["romance", "comedy"],                         # February — Valentine's
    3:  ["adventure", "fantasy"],                       # March — spring
    4:  ["comedy", "family"],                           # April
    5:  ["action", "war"],                              # May — Memorial Day
    6:  ["adventure", "animation"],                     # June — summer
    7:  ["action", "science-fiction", "sci-fi"],        # July — blockbusters
    8:  ["adventure", "comedy"],                        # August
    9:  ["drama", "thriller"],                          # September — fall
    10: ["horror", "thriller", "mystery"],              # October — Halloween
    11: ["war", "history", "western"],                  # November
    12: ["family", "animation", "comedy", "holiday", "christmas", "fantasy"],  # December
}


class RewatchRecommender:
    """Build and serve rewatch suggestions for a user."""

    CACHE_PREFIX = "rewatch"
    CACHE_TTL = 86400  # 24 hours
    MAX_ITEMS = 30

    # Scoring weights
    WEIGHT_RATING = 2.0
    WEIGHT_STALENESS = 1.5
    WEIGHT_SEASONAL = 3.0
    WEIGHT_ANNIVERSARY = 4.0

    # Staleness cap: 5 years max contribution
    MAX_STALENESS_YEARS = 5.0

    def __init__(self):
        self._emby_url = os.getenv("EMBY_URL", "")
        self._emby_key = os.getenv("EMBY_API_KEY", "")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_suggestions(self, user_id: int) -> list[dict]:
        """Return cached suggestions or empty list."""
        r = await get_redis()
        raw = await r.get(f"{self.CACHE_PREFIX}:suggestions:{user_id}")
        if raw:
            return json.loads(raw)
        return []

    async def build_suggestions(self, user_id: int,
                                min_rating: int = 8,
                                min_months: int = 12,
                                seasonal: bool = True) -> list[dict]:
        """Rebuild the suggestion list for *user_id*.

        Merges candidates from all available sources (Simkl + MDBList + Emby),
        deduplicating by item_key.  Simkl is authoritative where overlap exists.
        """
        r = await get_redis()

        # Load settings overrides from Redis
        raw_settings = await r.get(f"{self.CACHE_PREFIX}:settings:{user_id}")
        if raw_settings:
            s = json.loads(raw_settings)
            min_rating = s.get("min_rating", min_rating)
            min_months = s.get("min_months", min_months)
            seasonal = s.get("seasonal", seasonal)

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=min_months * 30)

        # Load dismissed item IDs
        dismissed = await self._load_dismissed(user_id)

        # Collect from all available sources and merge
        all_candidates: list[dict] = []
        sources_used: list[str] = []

        simkl_candidates = await self._candidates_from_simkl(user_id, min_rating, cutoff)
        if simkl_candidates:
            all_candidates.extend(simkl_candidates)
            sources_used.append("simkl")
            log.debug("rewatch.source_simkl", user_id=user_id,
                       count=len(simkl_candidates))

        mdblist_candidates = await self._candidates_from_mdblist(user_id, min_rating, cutoff)
        if mdblist_candidates:
            all_candidates.extend(mdblist_candidates)
            sources_used.append("mdblist")
            log.debug("rewatch.source_mdblist", user_id=user_id,
                       count=len(mdblist_candidates))

        emby_candidates = await self._candidates_from_emby(user_id, min_rating, cutoff)
        if emby_candidates:
            all_candidates.extend(emby_candidates)
            sources_used.append("emby")
            log.debug("rewatch.source_emby", user_id=user_id,
                       count=len(emby_candidates))

        if not all_candidates:
            log.info("rewatch.no_candidates", user_id=user_id)
            await r.set(f"{self.CACHE_PREFIX}:suggestions:{user_id}",
                        json.dumps([]), ex=self.CACHE_TTL)
            return []

        # Deduplicate: prefer Simkl > MDBList > Emby by keeping first seen
        # (Simkl added first).  Match by imdb_id or item_key.
        seen_keys: set[str] = set()
        seen_imdb: set[str] = set()
        candidates: list[dict] = []
        for c in all_candidates:
            ik = c["item_key"]
            iid = c.get("imdb_id", "")
            if ik in seen_keys:
                continue
            if iid and iid in seen_imdb:
                continue
            seen_keys.add(ik)
            if iid:
                seen_imdb.add(iid)
            candidates.append(c)

        # Filter dismissed
        candidates = [c for c in candidates if c["item_key"] not in dismissed]

        # Score
        scored = []
        for c in candidates:
            score = self._score_candidate(c, now, seasonal)
            c["score"] = round(score, 2)
            scored.append(c)

        # Sort and trim
        scored.sort(key=lambda x: x["score"], reverse=True)
        results = scored[:self.MAX_ITEMS]

        # Enrich with poster URLs
        for item in results:
            item["poster_url"] = self._poster_url(item.get("emby_id"))

        # Persist
        await r.set(
            f"{self.CACHE_PREFIX}:suggestions:{user_id}",
            json.dumps(results),
            ex=self.CACHE_TTL,
        )

        source_str = "+".join(sources_used) if sources_used else "none"
        log.info("rewatch.built", user_id=user_id, sources=source_str,
                 candidates=len(candidates), results=len(results))
        return results

    async def get_item_history(self, user_id: int, item_id: str) -> dict:
        """Return full watch history for a single item (hover flyout).

        Tries local watch_history DB first (instant, no API calls).
        Falls back to API sources if DB is empty (pre-backfill).

        Returns: {"watches": [{"date": str, "source": str}],
                  "play_count": int, "note": str|None}
        """
        r = await get_redis()
        cache_key = f"{self.CACHE_PREFIX}:history_v3:{user_id}:{item_id}"
        raw = await r.get(cache_key)
        if raw:
            return json.loads(raw)

        # Try local DB first (populated by webhooks + backfill)
        db_result = await self._history_from_db(user_id, item_id)
        if db_result and db_result.get("watches"):
            await r.set(cache_key, json.dumps(db_result), ex=self.CACHE_TTL)
            return db_result

        # Fallback: API sources (for pre-backfill state)
        simkl_result = await self._history_from_simkl(user_id, item_id)
        mdblist_result = await self._history_from_mdblist(user_id, item_id)
        emby_result = await self._history_from_emby(user_id, item_id)

        log.debug("rewatch.history_sources", user_id=user_id, item_id=item_id,
                  db=False, simkl =bool(simkl_result), mdblist=bool(mdblist_result),
                  emby=bool(emby_result))

        all_watches: list[dict] = []
        if simkl_result and simkl_result.get("watches"):
            all_watches.extend(simkl_result["watches"])
        if mdblist_result and mdblist_result.get("watches"):
            all_watches.extend(mdblist_result["watches"])
        if emby_result and emby_result.get("watches"):
            all_watches.extend(emby_result["watches"])

        deduped = self._deduplicate_watches(all_watches)
        deduped.sort(key=lambda w: w["date"], reverse=True)

        emby_play_count = emby_result.get("play_count", 0) if emby_result else 0
        mdblist_play_count = mdblist_result.get("play_count", 0) if mdblist_result else 0
        best_play_count = max(emby_play_count, mdblist_play_count)
        total_known = len(deduped)
        note = None
        if best_play_count > total_known and total_known > 0:
            extra = best_play_count - total_known
            note = f"{extra} additional watch{'es' if extra != 1 else ''} (dates unknown)"
        elif best_play_count > 1 and total_known == 0:
            note = f"Watched {best_play_count} times (no dates recorded)"
        play_count = max(best_play_count, total_known)

        result = {"watches": deduped, "play_count": play_count, "note": note}
        await r.set(cache_key, json.dumps(result), ex=self.CACHE_TTL)
        return result

    async def _history_from_db(self, user_id: int, item_id: str) -> dict | None:
        """Query local watch_history table for a single item."""
        from app.utils.database import async_session
        from sqlalchemy import select

        try:
            from app.models.schema import WatchHistory
        except ImportError:
            return None

        if ":" not in item_id:
            return None
        provider, value = item_id.split(":", 1)

        try:
            async with async_session() as db:
                q = select(WatchHistory).where(WatchHistory.user_id == user_id)
                if provider == "emby":
                    q = q.where(WatchHistory.emby_id == value)
                elif provider == "imdb":
                    q = q.where(WatchHistory.imdb_id == value)
                elif provider == "tmdb":
                    q = q.where(WatchHistory.tmdb_id == value)
                elif provider == "simkl":
                    q = q.where(WatchHistory.simkl_id == value)
                elif provider == "tvdb":
                    q = q.where(WatchHistory.tvdb_id == value)
                else:
                    return None

                q = q.order_by(WatchHistory.watched_at.desc())
                rows = (await db.execute(q)).scalars().all()

            if not rows:
                return None

            watches = [
                {"date": r.watched_at.strftime("%Y-%m-%d %H:%M") if r.watched_at else "",
                 "source": r.source or "db"}
                for r in rows
            ]
            return {"watches": watches, "play_count": len(watches), "note": None}
        except Exception as e:
            log.debug("rewatch.db_history_failed", user_id=user_id,
                      item_id=item_id, error=str(e)[:120])
            return None

    @staticmethod
    def _deduplicate_watches(watches: list[dict]) -> list[dict]:
        """Remove near-duplicate watch entries (within ±1 day tolerance).

        Keeps the entry with longer date string (more precision).
        """
        if not watches:
            return []

        # Parse dates for comparison
        parsed: list[tuple[datetime, dict]] = []
        for w in watches:
            try:
                date_str = w["date"]
                if len(date_str) > 10:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
                else:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                parsed.append((dt, w))
            except (ValueError, KeyError):
                continue

        # Sort by date
        parsed.sort(key=lambda x: x[0])

        deduped_dts: list[datetime] = []
        deduped: list[dict] = []
        for dt, w in parsed:
            is_dup = False
            for i, kept_dt in enumerate(deduped_dts):
                if abs((dt - kept_dt).total_seconds()) < 86400:
                    # Near-duplicate: keep the one with more precision
                    if len(w["date"]) > len(deduped[i]["date"]):
                        deduped[i] = w
                        deduped_dts[i] = dt
                    is_dup = True
                    break
            if not is_dup:
                deduped.append(w)
                deduped_dts.append(dt)

        return deduped

    async def dismiss(self, user_id: int, item_key: str) -> dict:
        """Dismiss an item from rewatch suggestions (DB-backed)."""
        from app.utils.database import async_session
        from app.models.schema import DismissedRewatchItem
        from sqlalchemy import select

        async with async_session() as db:
            existing = (await db.execute(
                select(DismissedRewatchItem).where(
                    DismissedRewatchItem.user_id == user_id,
                    DismissedRewatchItem.item_key == item_key,
                )
            )).scalar_one_or_none()
            if not existing:
                db.add(DismissedRewatchItem(user_id=user_id, item_key=item_key))
                await db.commit()

        # Remove from cached suggestions
        r = await get_redis()
        raw = await r.get(f"{self.CACHE_PREFIX}:suggestions:{user_id}")
        if raw:
            items = json.loads(raw)
            items = [i for i in items if i.get("item_key") != item_key]
            await r.set(f"{self.CACHE_PREFIX}:suggestions:{user_id}",
                        json.dumps(items), ex=self.CACHE_TTL)

        return {"status": "dismissed", "item_key": item_key}

    async def run_for_all_users(self):
        """Scheduler entry point — rebuild for every linked user."""
        from app.utils.database import async_session
        from app.models.schema import User
        from sqlalchemy import select

        async with async_session() as db:
            users = (await db.execute(
                select(User).where(User.simkl_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self.build_suggestions(user.id)
            except Exception as e:
                log.warning("rewatch.build_failed", user_id=user.id,
                            error=str(e)[:200])

    # ------------------------------------------------------------------
    # Data source: Simkl
    # ------------------------------------------------------------------

    async def _candidates_from_simkl(self, user_id: int, min_rating: int,
                                     cutoff: datetime) -> list[dict]:
        """Pull rated items from Simkl, cross-ref with history for dates."""
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.simkl_client import SimklClient
        from app.utils.library_cache import LibraryCache
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user or not user.simkl_access_token:
            return []

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            
            token_expires=user.simkl_token_expires,
        )

        try:
            # Get all ratings (movies + shows)
            ratings = await simkl.get_user_ratings("movies")
            ratings += await simkl.get_user_ratings("shows")

            # Get history for last-watched dates
            history_movies = await simkl.get_history("movies", limit=10000)
            history_shows = await simkl.get_history("shows", limit=10000)

            # Build last-watched lookup: simkl_id -> most_recent_date
            last_watched: dict[str, str] = {}
            for entry in history_movies + history_shows:
                item = entry.get("movie") or entry.get("show") or entry
                tid = str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or "")
                watched_at = entry.get("watched_at", "")
                if tid and watched_at:
                    if tid not in last_watched or watched_at > last_watched[tid]:
                        last_watched[tid] = watched_at

            candidates = []
            for rated in ratings:
                rating = rated.get("rating", 0)
                if rating < min_rating:
                    continue

                item = rated.get("movie") or rated.get("show") or rated
                ids = item.get("ids", {})
                simkl_id = str(ids.get("simkl") or ids.get("simkl_id") or "")
                imdb_id = ids.get("imdb", "")
                tmdb_id = str(ids.get("tmdb", ""))
                item_type = "movie" if "movie" in rated else "show"
                title = item.get("title", "Unknown")
                year = item.get("year")
                genres = item.get("genres", [])

                # Determine last watched date
                lw = last_watched.get(simkl_id, "")
                if not lw:
                    continue  # Never watched according to history

                try:
                    lw_dt = datetime.fromisoformat(lw.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue

                if lw_dt > cutoff:
                    continue  # Watched too recently

                # Resolve Emby ID from library cache
                emby_id = None
                if imdb_id:
                    cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
                    if cached:
                        emby_id = cached.get("emby_id")
                if not emby_id and tmdb_id:
                    cached = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id)
                    if cached:
                        emby_id = cached.get("emby_id")

                candidates.append({
                    "item_key": f"simkl:{simkl_id}",
                    "title": title,
                    "year": year,
                    "item_type": item_type,
                    "rating": rating,
                    "genres": genres,
                    "last_watched": lw_dt.strftime("%Y-%m-%d"),
                    "last_watched_iso": lw,
                    "simkl_id": simkl_id,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "emby_id": emby_id,
                    "in_library": emby_id is not None,
                    "source": "simkl",
                })

            return candidates
        except Exception as e:
            log.warning("rewatch.simkl_fetch_failed", user_id=user_id,
                        error=str(e)[:200])
            return []
        finally:
            await simkl.close()

    # ------------------------------------------------------------------
    # Data source: MDBList
    # ------------------------------------------------------------------

    @staticmethod
    async def _build_mdblist_client():
        """Build an MDBListClient using the stored API key, or None."""
        from app.utils.mdblist_client import MDBListClient
        r = await get_redis()
        raw = await secure_get("mdblist_api_key")
        if not raw:
            return None
        key = raw if isinstance(raw, str) else raw.decode()
        if not key:
            return None
        return MDBListClient(api_key=key)

    async def _candidates_from_mdblist(self, user_id: int, min_rating: int,
                                       cutoff: datetime) -> list[dict]:
        """Fallback: use MDBList /sync/watched + /sync/ratings when Simkl unavailable."""
        from app.utils.library_cache import LibraryCache

        mdb = await self._build_mdblist_client()
        if not mdb:
            log.debug("rewatch.mdblist_skipped", user_id=user_id, reason="no_api_key")
            return []

        try:
            # Fetch watched history and ratings
            watched_data = await mdb.get_watched()
            ratings_data = await mdb.get_ratings()

            log.debug("rewatch.mdblist_raw", user_id=user_id,
                      watched_movies=len(watched_data.get("movies", [])),
                      watched_shows=len(watched_data.get("shows", [])),
                      rated_movies=len(ratings_data.get("movies", []) if isinstance(ratings_data, dict) else []),
                      rated_shows=len(ratings_data.get("shows", []) if isinstance(ratings_data, dict) else []))

            # ── Build Emby date lookup: imdb→LastPlayedDate ──
            # MDBList last_watched_at is often the sync date (not real watch date).
            # Emby's LastPlayedDate is the actual playback date.
            emby_date_lookup: dict[str, str] = {}  # imdb_id → date string
            try:
                from app.utils.database import async_session
                from app.models.schema import User
                from app.utils.emby_client import EmbyClient
                from sqlalchemy import select
                async with async_session() as db:
                    user = (await db.execute(
                        select(User).where(User.id == user_id)
                    )).scalar_one_or_none()
                if user:
                    emby = EmbyClient()
                    try:
                        for itype in ("Movie", "Series"):
                            resp = await emby.get_items(
                                user_id=user.emby_user_id,
                                item_type=itype,
                                filters="IsPlayed",
                                fields="ProviderIds,UserData,UserDataLastPlayedDate",
                                limit=5000,
                            )
                            items = resp.get("Items", []) if isinstance(resp, dict) else resp
                            for it in items:
                                lpd = (it.get("UserData") or {}).get("LastPlayedDate", "")
                                if not lpd:
                                    continue
                                pids = it.get("ProviderIds", {})
                                imdb = pids.get("Imdb", "")
                                if imdb:
                                    emby_date_lookup[imdb] = lpd
                                tmdb = pids.get("Tmdb", "")
                                if tmdb:
                                    emby_date_lookup[f"tmdb:{tmdb}"] = lpd
                    finally:
                        await emby.close()
                log.debug("rewatch.emby_date_lookup", user_id=user_id,
                          entries=len(emby_date_lookup))
            except Exception as e:
                log.debug("rewatch.emby_date_lookup_failed", error=str(e)[:120])

            # Build ratings lookup: (provider:id) -> rating
            rating_lookup: dict[str, float] = {}
            if isinstance(ratings_data, dict):
                for kind in ("movies", "shows"):
                    for item in ratings_data.get(kind, []):
                        r_val = item.get("rating")
                        if r_val is None:
                            continue
                        # MDBList wraps: {rating, movie: {ids: ...}}
                        inner = item.get("movie") or item.get("show") or item
                        ids = inner.get("ids", {})
                        for prov in ("imdb", "tmdb", "simkl", "mdblist"):
                            pid = ids.get(prov)
                            if pid:
                                rating_lookup[f"{prov}:{pid}"] = float(r_val)

            candidates = []
            _dbg_no_date = 0
            _dbg_too_recent = 0
            _dbg_no_rating = 0
            _dbg_below_min = 0
            _dbg_total = 0
            for kind, item_type in (("movies", "movie"), ("shows", "show")):
                for entry in watched_data.get(kind, []):
                    _dbg_total += 1
                    # MDBList wraps: {last_watched_at, movie: {title, ids, year, ...}}
                    inner = entry.get("movie") or entry.get("show") or entry
                    ids = inner.get("ids", {})
                    title = inner.get("title", "Unknown")
                    year = inner.get("year")
                    genres = [g.lower() for g in inner.get("genres", [])]
                    plays = entry.get("plays", 1)

                    # Determine last watched date — try multiple field names
                    # Date fields live at the wrapper level, not inside movie/show
                    watched_at = (
                        entry.get("watched_at")
                        or entry.get("last_watched_at")
                        or entry.get("updated_at")
                        or entry.get("last_played")
                        or ""
                    )

                    # MDBList last_watched_at is often the sync date, not real watch date.
                    # Cross-reference with Emby's LastPlayedDate for more accurate data.
                    imdb_id_for_lookup = ids.get("imdb", "")
                    tmdb_id_for_lookup = str(ids.get("tmdb", "")) if ids.get("tmdb") else ""

                    emby_lpd = None
                    if imdb_id_for_lookup and imdb_id_for_lookup in emby_date_lookup:
                        emby_lpd = emby_date_lookup[imdb_id_for_lookup]
                    elif tmdb_id_for_lookup and f"tmdb:{tmdb_id_for_lookup}" in emby_date_lookup:
                        emby_lpd = emby_date_lookup[f"tmdb:{tmdb_id_for_lookup}"]

                    # Use Emby date if it's older (more likely the real watch date)
                    if emby_lpd:
                        try:
                            emby_dt = datetime.fromisoformat(
                                str(emby_lpd).replace("Z", "+00:00"))
                            if not watched_at:
                                watched_at = str(emby_lpd)
                            else:
                                mdb_dt = datetime.fromisoformat(
                                    watched_at.replace("Z", "+00:00"))
                                if emby_dt < mdb_dt:
                                    watched_at = str(emby_lpd)
                        except (ValueError, TypeError):
                            pass
                    if not watched_at:
                        _dbg_no_date += 1
                        continue
                    try:
                        lw_dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        _dbg_no_date += 1
                        continue

                    if lw_dt > cutoff:
                        _dbg_too_recent += 1
                        continue  # Watched too recently

                    # Find rating from ratings lookup
                    rating = None
                    imdb_id = ids.get("imdb", "")
                    tmdb_id = str(ids.get("tmdb", "")) if ids.get("tmdb") else ""
                    simkl_id = str(ids.get("simkl") or ids.get("simkl_id") or "") if ids.get("simkl") else ""
                    mdblist_id = str(ids.get("mdblist", "")) if ids.get("mdblist") else ""

                    for key_str in (
                        f"imdb:{imdb_id}" if imdb_id else "",
                        f"tmdb:{tmdb_id}" if tmdb_id else "",
                        f"simkl:{simkl_id}" if simkl_id else "",
                        f"mdblist:{mdblist_id}" if mdblist_id else "",
                    ):
                        if key_str and key_str in rating_lookup:
                            rating = rating_lookup[key_str]
                            break

                    # Fallback: check for rating in watched entry itself
                    if rating is None:
                        rating = entry.get("rating")
                    # Fallback: check for MDBList score (0-100 scale → 1-10)
                    if rating is None and entry.get("score") is not None:
                        try:
                            rating = round(float(entry["score"]) / 10, 1)
                        except (ValueError, TypeError):
                            pass

                    # Fallback: use Emby CommunityRating via library cache
                    if rating is None:
                        cached_item = None
                        if imdb_id:
                            cached_item = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
                        if not cached_item and tmdb_id:
                            cached_item = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id)
                        if cached_item:
                            rating = cached_item.get("community_rating")

                    if rating is None:
                        _dbg_no_rating += 1
                        continue
                    if float(rating) < min_rating:
                        _dbg_below_min += 1
                        continue
                    rating = float(rating)

                    # Resolve Emby ID from library cache
                    emby_id = None
                    if imdb_id:
                        cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
                        if cached:
                            emby_id = cached.get("emby_id")
                    if not emby_id and tmdb_id:
                        cached = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id)
                        if cached:
                            emby_id = cached.get("emby_id")

                    # Build item_key — prefer imdb, fall back to tmdb/mdblist
                    item_key = (
                        f"imdb:{imdb_id}" if imdb_id
                        else f"tmdb:{tmdb_id}" if tmdb_id
                        else f"mdblist:{mdblist_id}" if mdblist_id
                        else f"emby:{emby_id}" if emby_id
                        else None
                    )
                    if not item_key:
                        continue

                    candidates.append({
                        "item_key": item_key,
                        "title": title,
                        "year": year,
                        "item_type": item_type,
                        "rating": round(rating, 1),
                        "genres": genres,
                        "last_watched": lw_dt.strftime("%Y-%m-%d"),
                        "last_watched_iso": watched_at,
                        "simkl_id": simkl_id,
                        "imdb_id": imdb_id,
                        "tmdb_id": tmdb_id,
                        "emby_id": emby_id,
                        "in_library": emby_id is not None,
                        "play_count": plays,
                        "source": "mdblist",
                    })

            log.info("rewatch.mdblist_filter_stages", user_id=user_id,
                     total=_dbg_total, no_date=_dbg_no_date,
                     too_recent=_dbg_too_recent, no_rating=_dbg_no_rating,
                     below_min=_dbg_below_min, passed=len(candidates),
                     min_rating=min_rating,
                     cutoff=cutoff.isoformat(),
                     rating_lookup_size=len(rating_lookup),
                     sample_date_fields=(
                         list(watched_data.get("movies", [{}])[0].keys())[:10]
                         if watched_data.get("movies") else "empty"
                     ))

            return candidates
        except Exception as e:
            log.warning("rewatch.mdblist_fetch_failed", user_id=user_id,
                        error=str(e)[:200])
            return []
        finally:
            await mdb.close()

    # ------------------------------------------------------------------
    # Data source: Emby
    # ------------------------------------------------------------------

    async def _candidates_from_emby(self, user_id: int, min_rating: int,
                                    cutoff: datetime) -> list[dict]:
        """Last resort: use Emby's UserData for LastPlayedDate + UserRating."""
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.emby_client import EmbyClient
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user:
            return []

        emby = EmbyClient()
        try:
            # Fetch played movies
            resp_movies = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Movie",
                filters="IsPlayed",
                fields="ProviderIds,Genres,ProductionYear,CommunityRating,UserData,UserDataLastPlayedDate",
                limit=5000,
            )
            movies = resp_movies.get("Items", []) if isinstance(resp_movies, dict) else resp_movies

            # Fetch played series
            resp_shows = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Series",
                filters="IsPlayed",
                fields="ProviderIds,Genres,ProductionYear,CommunityRating,UserData,UserDataLastPlayedDate",
                limit=5000,
            )
            shows = resp_shows.get("Items", []) if isinstance(resp_shows, dict) else resp_shows

            log.debug("rewatch.emby_items", user_id=user_id,
                      movies=len(movies), shows=len(shows))

            candidates = []
            _emby_no_rating = 0
            _emby_below_min = 0
            _emby_no_date = 0
            _emby_too_recent = 0
            for item in (movies + shows):
                user_data = item.get("UserData", {})
                user_rating = user_data.get("Rating") or item.get("CommunityRating")

                if not user_rating:
                    _emby_no_rating += 1
                    continue
                if user_rating < min_rating:
                    _emby_below_min += 1
                    continue

                last_played = user_data.get("LastPlayedDate", "")
                if not last_played:
                    _emby_no_date += 1
                    continue

                try:
                    lp_dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    _emby_no_date += 1
                    continue

                if lp_dt > cutoff:
                    _emby_too_recent += 1
                    continue

                provider_ids = item.get("ProviderIds", {})
                item_type = "movie" if item.get("Type") == "Movie" else "show"
                genres = [g.lower() for g in item.get("Genres", [])]

                emby_id = item.get("Id")
                imdb_id = provider_ids.get("Imdb", "")
                tmdb_id = str(provider_ids.get("Tmdb", ""))
                item_key = f"emby:{emby_id}" if emby_id else f"imdb:{imdb_id}"

                candidates.append({
                    "item_key": item_key,
                    "title": item.get("Name", "Unknown"),
                    "year": item.get("ProductionYear"),
                    "item_type": item_type,
                    "rating": round(float(user_rating), 1),
                    "genres": genres,
                    "last_watched": lp_dt.strftime("%Y-%m-%d"),
                    "last_watched_iso": last_played,
                    "simkl_id": "",
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "emby_id": emby_id,
                    "in_library": True,
                    "play_count": user_data.get("PlayCount", 1),
                    "source": "emby",
                })

            log.info("rewatch.emby_filter_stages", user_id=user_id,
                     total=len(movies) + len(shows),
                     no_rating=_emby_no_rating, below_min=_emby_below_min,
                     no_date=_emby_no_date, too_recent=_emby_too_recent,
                     passed=len(candidates), min_rating=min_rating,
                     cutoff=cutoff.isoformat())
            return candidates
        except Exception as e:
            log.warning("rewatch.emby_fetch_failed", user_id=user_id,
                        error=str(e)[:200])
            return []
        finally:
            await emby.close()

    # ------------------------------------------------------------------
    # History lookups (for hover flyout)
    # ------------------------------------------------------------------

    async def _history_from_simkl(self, user_id: int, item_id: str) -> dict | None:
        """Fetch all watch dates for a single item from Simkl.

        item_id format: 'simkl:12345' or 'imdb:tt1234567'
        """
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.simkl_client import SimklClient
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user or not user.simkl_access_token:
            return None

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            
            token_expires=user.simkl_token_expires,
        )

        try:
            # Parse item_id to determine type and ID
            # We need to check both movies and shows history
            watches = []
            for kind in ("movies", "shows"):
                history = await simkl.get_history(kind, limit=10000)
                for entry in history:
                    item = entry.get("movie") or entry.get("show") or entry
                    ids = item.get("ids", {})
                    tid = str(ids.get("simkl") or ids.get("simkl_id") or "")
                    iid = ids.get("imdb", "")

                    match = (item_id == f"simkl:{tid}" or
                             item_id == f"imdb:{iid}" or
                             item_id == f"emby:{tid}")  # won't match but safe

                    if match:
                        watched_at = entry.get("watched_at", "")
                        if watched_at:
                            try:
                                dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                watches.append({
                                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                                    "source": "simkl",
                                })
                            except (ValueError, TypeError):
                                pass

            watches.sort(key=lambda w: w["date"], reverse=True)
            return {
                "watches": watches,
                "play_count": len(watches),
            } if watches else None
        except Exception as e:
            log.warning("rewatch.simkl_history_failed", user_id=user_id,
                        item_id=item_id, error=str(e)[:200])
            return None
        finally:
            await simkl.close()

    async def _history_from_mdblist(self, user_id: int, item_id: str) -> dict | None:
        """Fetch watch history from MDBList /sync/watched for a single item.

        MDBList returns watched_at and plays count per item. We scan the
        full watched list and match by provider ID.
        """
        mdb = await self._build_mdblist_client()
        if not mdb:
            return None

        try:
            watched_data = await mdb.get_watched()

            # Parse the item_id to get provider and value
            if ":" not in item_id:
                return None
            provider, value = item_id.split(":", 1)

            for kind in ("movies", "shows"):
                for entry in watched_data.get(kind, []):
                    ids = entry.get("ids", {})

                    # Match by provider ID
                    entry_val = ids.get(provider)
                    if entry_val is not None and str(entry_val) == value:
                        watched_at = entry.get("watched_at") or entry.get("last_watched_at", "")
                        plays = entry.get("plays", 1)

                        watches = []
                        if watched_at:
                            try:
                                dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                watches.append({
                                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                                    "source": "mdblist",
                                })
                            except (ValueError, TypeError):
                                pass

                        return {
                            "watches": watches,
                            "play_count": plays,
                            "note": (
                                f"Watched {plays} time{'s' if plays != 1 else ''} (MDBList)"
                                if plays > 1 and len(watches) <= 1 else None
                            ),
                        }

            return None
        except Exception as e:
            log.warning("rewatch.mdblist_history_failed", user_id=user_id,
                        item_id=item_id, error=str(e)[:200])
            return None
        finally:
            await mdb.close()

    async def _history_from_emby(self, user_id: int, item_id: str) -> dict | None:
        """Emby stores only LastPlayedDate + PlayCount (no individual dates)."""
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.emby_client import EmbyClient
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user:
            return None

        # Extract the emby_id from item_key
        emby_id = None
        if item_id.startswith("emby:"):
            emby_id = item_id.split(":", 1)[1]
        else:
            # Try to resolve from library cache
            from app.utils.library_cache import LibraryCache
            if item_id.startswith("imdb:"):
                cached = await LibraryCache.find_by_provider_id("Imdb", item_id.split(":", 1)[1])
                if cached:
                    emby_id = cached.get("emby_id")
            elif item_id.startswith("simkl:"):
                # Can't resolve simkl ID to emby ID without more data
                return None

        if not emby_id:
            return None

        emby = EmbyClient()
        try:
            item = await emby.get_item_safe(emby_id, user_id=user.emby_user_id)
            if not item:
                return None

            user_data = item.get("UserData", {})
            play_count = user_data.get("PlayCount", 0)
            last_played = user_data.get("LastPlayedDate", "")

            watches = []
            if last_played:
                try:
                    dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                    watches.append({
                        "date": dt.strftime("%Y-%m-%d %H:%M"),
                        "source": "emby",
                    })
                except (ValueError, TypeError):
                    pass

            return {
                "watches": watches,
                "play_count": play_count,
                "note": f"Watched {play_count} time{'s' if play_count != 1 else ''}" if play_count > 1 and len(watches) == 1 else None,
            }
        except Exception as e:
            log.warning("rewatch.emby_history_failed", user_id=user_id,
                        item_id=item_id, error=str(e)[:200])
            return None
        finally:
            await emby.close()

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score_candidate(self, c: dict, now: datetime, seasonal: bool) -> float:
        """Compute recommendation score for a candidate."""
        score = 0.0

        # Rating component: higher rating = higher score
        rating = c.get("rating", 5)
        score += (rating / 10.0) * self.WEIGHT_RATING

        # Staleness component: longer since last watch = higher score (capped)
        try:
            lw = datetime.fromisoformat(c["last_watched_iso"].replace("Z", "+00:00"))
            years_since = (now - lw).days / 365.25
            capped = min(years_since, self.MAX_STALENESS_YEARS)
            score += (capped / self.MAX_STALENESS_YEARS) * self.WEIGHT_STALENESS
        except (ValueError, TypeError, KeyError):
            pass

        # Seasonal boost
        if seasonal:
            month = now.month
            seasonal_genres = _SEASONAL_GENRES.get(month, [])
            item_genres = [g.lower() for g in c.get("genres", [])]
            if any(g in seasonal_genres for g in item_genres):
                score += self.WEIGHT_SEASONAL

        # Anniversary boost: watched within ±7 days of today in a prior year
        try:
            lw = datetime.fromisoformat(c["last_watched_iso"].replace("Z", "+00:00"))
            day_of_year_now = now.timetuple().tm_yday
            day_of_year_watched = lw.timetuple().tm_yday
            diff = abs(day_of_year_now - day_of_year_watched)
            # Handle year wrap (e.g. Dec 30 vs Jan 3)
            diff = min(diff, 365 - diff)
            if diff <= 7 and lw.year < now.year:
                score += self.WEIGHT_ANNIVERSARY
        except (ValueError, TypeError, KeyError):
            pass

        return score

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _poster_url(self, emby_id: str | None) -> str:
        """Construct proxied poster image URL (avoids exposing Emby API key)."""
        if not emby_id:
            return ""
        return f"/api/emby/image/{emby_id}/Primary?maxWidth=300"

    async def _load_dismissed(self, user_id: int) -> set[str]:
        """Load dismissed item keys from DB."""
        from app.utils.database import async_session
        from app.models.schema import DismissedRewatchItem
        from sqlalchemy import select

        try:
            async with async_session() as db:
                rows = (await db.execute(
                    select(DismissedRewatchItem.item_key).where(
                        DismissedRewatchItem.user_id == user_id
                    )
                )).scalars().all()
            return set(rows)
        except Exception:
            return set()

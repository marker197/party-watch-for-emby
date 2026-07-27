"""
Rewatch Recommender Service

Suggests items the user has already watched and rated highly that are worth
rewatching.  Scoring factors:

  - Base pool: items rated >= threshold (default 8), last watched > N months ago
  - Seasonal boost: genre matched to current month
  - Anniversary boost: watched within ±7 days of today in a prior year
  - Staleness weight: longer since last watch = higher score (capped)
  - Decay for dismissed items

Data source priority:
  1. Trakt ratings + history (richest data)
  2. MDBList history (if no Trakt account)
  3. Emby LastPlayedDate + UserRating (fallback — single date, but PlayCount)

Output: top 30 items persisted to Redis with 24h TTL.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import structlog

from app.utils.redis_cache import get_redis

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

        Tries Trakt first, falls back to MDBList, then Emby.
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

        # Try data sources in priority order
        candidates = await self._candidates_from_trakt(user_id, min_rating, cutoff)
        source = "trakt"

        if not candidates:
            candidates = await self._candidates_from_mdblist(user_id, min_rating, cutoff)
            source = "mdblist"

        if not candidates:
            candidates = await self._candidates_from_emby(user_id, min_rating, cutoff)
            source = "emby"

        if not candidates:
            log.info("rewatch.no_candidates", user_id=user_id)
            await r.set(f"{self.CACHE_PREFIX}:suggestions:{user_id}",
                        json.dumps([]), ex=self.CACHE_TTL)
            return []

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

        log.info("rewatch.built", user_id=user_id, source=source,
                 candidates=len(candidates), results=len(results))
        return results

    async def get_item_history(self, user_id: int, item_id: str) -> dict:
        """Lazy-load full watch history for a single item (hover flyout).

        Returns: {"title": str, "watches": [{"date": str, "source": str}],
                  "play_count": int}
        """
        r = await get_redis()
        cache_key = f"{self.CACHE_PREFIX}:history:{user_id}:{item_id}"
        raw = await r.get(cache_key)
        if raw:
            return json.loads(raw)

        result = await self._history_from_trakt(user_id, item_id)

        if not result or len(result.get("watches", [])) <= 1:
            mdb_result = await self._history_from_mdblist(user_id, item_id)
            if mdb_result and len(mdb_result.get("watches", [])) > len(result.get("watches", []) if result else []):
                result = mdb_result

        if not result or not result.get("watches"):
            result = await self._history_from_emby(user_id, item_id)

        if not result:
            result = {"title": "", "watches": [], "play_count": 0}

        await r.set(cache_key, json.dumps(result), ex=self.CACHE_TTL)
        return result

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
                select(User).where(User.trakt_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self.build_suggestions(user.id)
            except Exception as e:
                log.warning("rewatch.build_failed", user_id=user.id,
                            error=str(e)[:200])

    # ------------------------------------------------------------------
    # Data source: Trakt
    # ------------------------------------------------------------------

    async def _candidates_from_trakt(self, user_id: int, min_rating: int,
                                     cutoff: datetime) -> list[dict]:
        """Pull rated items from Trakt, cross-ref with history for dates."""
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.trakt_client import TraktClient
        from app.utils.library_cache import LibraryCache
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user or not user.trakt_access_token:
            return []

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
        )

        try:
            # Get all ratings (movies + shows)
            ratings = await trakt.get_user_ratings("movies")
            ratings += await trakt.get_user_ratings("shows")

            # Get history for last-watched dates
            history_movies = await trakt.get_history("movies", limit=10000)
            history_shows = await trakt.get_history("shows", limit=10000)

            # Build last-watched lookup: trakt_id -> most_recent_date
            last_watched: dict[str, str] = {}
            for entry in history_movies + history_shows:
                item = entry.get("movie") or entry.get("show") or {}
                tid = str(item.get("ids", {}).get("trakt", ""))
                watched_at = entry.get("watched_at", "")
                if tid and watched_at:
                    if tid not in last_watched or watched_at > last_watched[tid]:
                        last_watched[tid] = watched_at

            candidates = []
            for rated in ratings:
                rating = rated.get("rating", 0)
                if rating < min_rating:
                    continue

                item = rated.get("movie") or rated.get("show") or {}
                ids = item.get("ids", {})
                trakt_id = str(ids.get("trakt", ""))
                imdb_id = ids.get("imdb", "")
                tmdb_id = str(ids.get("tmdb", ""))
                item_type = "movie" if "movie" in rated else "show"
                title = item.get("title", "Unknown")
                year = item.get("year")
                genres = item.get("genres", [])

                # Determine last watched date
                lw = last_watched.get(trakt_id, "")
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
                    "item_key": f"trakt:{trakt_id}",
                    "title": title,
                    "year": year,
                    "item_type": item_type,
                    "rating": rating,
                    "genres": genres,
                    "last_watched": lw_dt.strftime("%Y-%m-%d"),
                    "last_watched_iso": lw,
                    "trakt_id": trakt_id,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "emby_id": emby_id,
                    "in_library": emby_id is not None,
                    "source": "trakt",
                })

            return candidates
        except Exception as e:
            log.warning("rewatch.trakt_fetch_failed", user_id=user_id,
                        error=str(e)[:200])
            return []
        finally:
            await trakt.close()

    # ------------------------------------------------------------------
    # Data source: MDBList
    # ------------------------------------------------------------------

    async def _candidates_from_mdblist(self, user_id: int, min_rating: int,
                                       cutoff: datetime) -> list[dict]:
        """Fallback: use MDBList watched history if Trakt not available."""
        # MDBList doesn't store per-user ratings the same way, but
        # we can pull watched history and use Emby ratings as fallback
        return []  # Placeholder — MDBList doesn't have a user history endpoint

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
            movies = await emby.get_items(
                user_id=user.emby_user_id,
                item_types="Movie",
                filters="IsPlayed",
                fields="ProviderIds,Genres,ProductionYear,UserData,UserDataLastPlayedDate",
                limit=5000,
            )

            # Fetch played series
            shows = await emby.get_items(
                user_id=user.emby_user_id,
                item_types="Series",
                filters="IsPlayed",
                fields="ProviderIds,Genres,ProductionYear,UserData,UserDataLastPlayedDate",
                limit=5000,
            )

            candidates = []
            for item in (movies + shows):
                user_data = item.get("UserData", {})
                user_rating = user_data.get("Rating") or item.get("CommunityRating")

                if not user_rating or user_rating < min_rating:
                    continue

                last_played = user_data.get("LastPlayedDate", "")
                if not last_played:
                    continue

                try:
                    lp_dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue

                if lp_dt > cutoff:
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
                    "trakt_id": "",
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "emby_id": emby_id,
                    "in_library": True,
                    "play_count": user_data.get("PlayCount", 1),
                    "source": "emby",
                })

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

    async def _history_from_trakt(self, user_id: int, item_id: str) -> dict | None:
        """Fetch all watch dates for a single item from Trakt.

        item_id format: 'trakt:12345' or 'imdb:tt1234567'
        """
        from app.utils.database import async_session
        from app.models.schema import User
        from app.utils.trakt_client import TraktClient
        from sqlalchemy import select

        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user or not user.trakt_access_token:
            return None

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
        )

        try:
            # Parse item_id to determine type and ID
            # We need to check both movies and shows history
            watches = []
            for kind in ("movies", "shows"):
                history = await trakt.get_history(kind, limit=10000)
                for entry in history:
                    item = entry.get("movie") or entry.get("show") or {}
                    ids = item.get("ids", {})
                    tid = str(ids.get("trakt", ""))
                    iid = ids.get("imdb", "")

                    match = (item_id == f"trakt:{tid}" or
                             item_id == f"imdb:{iid}" or
                             item_id == f"emby:{tid}")  # won't match but safe

                    if match:
                        watched_at = entry.get("watched_at", "")
                        if watched_at:
                            try:
                                dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                watches.append({
                                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                                    "source": "trakt",
                                })
                            except (ValueError, TypeError):
                                pass

            watches.sort(key=lambda w: w["date"], reverse=True)
            return {
                "watches": watches,
                "play_count": len(watches),
            } if watches else None
        except Exception as e:
            log.warning("rewatch.trakt_history_failed", user_id=user_id,
                        item_id=item_id, error=str(e)[:200])
            return None
        finally:
            await trakt.close()

    async def _history_from_mdblist(self, user_id: int, item_id: str) -> dict | None:
        """MDBList doesn't expose per-item watch history — placeholder."""
        return None

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
            elif item_id.startswith("trakt:"):
                # Can't resolve trakt ID to emby ID without more data
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
        """Construct Emby poster image URL."""
        if not emby_id or not self._emby_url:
            return ""
        return f"{self._emby_url}/Items/{emby_id}/Images/Primary?maxWidth=300&api_key={self._emby_key}"

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

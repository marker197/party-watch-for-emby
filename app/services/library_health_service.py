"""Library Health Monitor — finds gaps in your Emby library.

Scans:
  1. Incomplete series — shows in your library with unwatched episodes
  2. Trakt watched, not in library — movies/shows in Trakt history but
     missing from Emby
  3. Highly rated missing sequels — movies you rated 8+ whose Trakt-related
     films aren't in your library

Results are cached in Redis for 6 hours. A manual or scheduled scan
replaces the cache.
"""

from __future__ import annotations

import asyncio
import json as _json
from datetime import datetime, timezone

import structlog

from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.database import async_session
from app.models.schema import User
from sqlalchemy import select

log = structlog.get_logger()

CACHE_KEY = "library_health_v1"
CACHE_TTL = 6 * 3600  # 6 hours


class LibraryHealthService:
    """Async library health scanner."""

    async def get_report(self, user: User) -> dict:
        """Return cached report or empty placeholder."""
        r = await get_redis()
        raw = await r.get(f"{CACHE_KEY}:{user.id}")
        if raw:
            return _json.loads(raw)
        return {"status": "no_report", "message": "Run a scan first."}

    async def scan(self, user: User) -> dict:
        """Run full library health scan and cache results."""
        log.info("library_health.scan_start", user_id=user.id)
        start = datetime.now(timezone.utc)

        report: dict = {
            "incomplete_series": [],
            "watched_not_in_library": {"movies": [], "shows": []},
            "missing_sequels": [],
            "summary": {},
            "scanned_at": start.isoformat(),
        }

        trakt = await self._get_trakt_client(user)
        if not trakt:
            report["error"] = "No Trakt account linked"
            return report

        emby = EmbyClient()
        try:
            # ── 1. Incomplete series ────────────────────────────────────
            report["incomplete_series"] = await self._scan_incomplete_series(
                emby, user.emby_user_id,
            )

            # ── 2. Trakt watched, not in library ───────────────────────
            movies_missing, shows_missing = await self._scan_watched_not_in_library(
                trakt,
            )
            report["watched_not_in_library"]["movies"] = movies_missing[:100]
            report["watched_not_in_library"]["shows"] = shows_missing[:100]

            # ── 3. Missing sequels for highly rated movies ─────────────
            report["missing_sequels"] = await self._scan_missing_sequels(
                trakt,
            )

            # ── Summary ────────────────────────────────────────────────
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            report["summary"] = {
                "incomplete_series": len(report["incomplete_series"]),
                "movies_not_in_library": len(report["watched_not_in_library"]["movies"]),
                "shows_not_in_library": len(report["watched_not_in_library"]["shows"]),
                "missing_sequels": len(report["missing_sequels"]),
                "scan_seconds": round(elapsed, 1),
                "total_issues": (
                    len(report["incomplete_series"])
                    + len(report["watched_not_in_library"]["movies"])
                    + len(report["watched_not_in_library"]["shows"])
                    + len(report["missing_sequels"])
                ),
            }

        except Exception as e:
            log.exception("library_health.scan_failed", user_id=user.id)
            report["error"] = str(e)[:200]
        finally:
            await emby.close()
            await trakt.close()

        # Cache
        try:
            r = await get_redis()
            await r.setex(
                f"{CACHE_KEY}:{user.id}", CACHE_TTL, _json.dumps(report),
            )
        except Exception:
            pass

        log.info(
            "library_health.scan_complete",
            user_id=user.id,
            **report.get("summary", {}),
        )
        return report

    # ── Scan: incomplete series ─────────────────────────────────────────

    async def _scan_incomplete_series(
        self, emby: EmbyClient, emby_user_id: str,
    ) -> list[dict]:
        """Find series in Emby where the user has started but not finished."""
        results: list[dict] = []

        # Fetch all Series with UserData and RecursiveItemCount
        start = 0
        batch = 200
        all_series: list[dict] = []
        while True:
            resp = await emby.get_items(
                user_id=emby_user_id,
                item_type="Series",
                fields="ProviderIds,UserData,RecursiveItemCount",
                sort_by="SortName",
                limit=batch,
                start_index=start,
            )
            items = resp.get("Items", [])
            all_series.extend(items)
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch

        for series in all_series:
            ud = series.get("UserData", {})
            unplayed = ud.get("UnplayedItemCount") or 0
            total_eps = series.get("RecursiveItemCount") or 0

            if total_eps <= 0:
                continue

            played_eps = total_eps - unplayed

            # Skip series the user hasn't started at all
            if played_eps <= 0:
                continue

            # Skip fully watched series
            if unplayed <= 0:
                continue

            # This series has been started but not finished
            completion = round(played_eps / total_eps * 100, 1)

            pids = series.get("ProviderIds", {})
            results.append({
                "title": series.get("Name", ""),
                "year": series.get("ProductionYear"),
                "emby_id": series.get("Id"),
                "imdb_id": pids.get("Imdb"),
                "tvdb_id": pids.get("Tvdb"),
                "played_episodes": played_eps,
                "unplayed_episodes": unplayed,
                "total_episodes": total_eps,
                "completion_pct": completion,
            })

        # Sort: most complete first (closest to finishing)
        results.sort(key=lambda x: x["completion_pct"], reverse=True)
        return results

    # ── Scan: Trakt watched not in library ──────────────────────────────

    async def _scan_watched_not_in_library(
        self, trakt: TraktClient,
    ) -> tuple[list[dict], list[dict]]:
        """Find items in Trakt watched history that aren't in the Emby library."""

        missing_movies: list[dict] = []
        missing_shows: list[dict] = []

        # Movies
        try:
            watched_movies = await trakt.get_watched(kind="movies")
        except Exception as e:
            log.warning("library_health.trakt_movies_failed", error=str(e)[:120])
            watched_movies = []

        for entry in watched_movies:
            movie = entry.get("movie", {})
            ids = movie.get("ids", {})
            title = movie.get("title", "")
            year = movie.get("year")

            # Check library cache
            in_library = await self._is_in_library(ids)
            if not in_library:
                missing_movies.append({
                    "title": title,
                    "year": year,
                    "imdb_id": ids.get("imdb"),
                    "tmdb_id": ids.get("tmdb"),
                    "trakt_id": ids.get("trakt"),
                    "plays": entry.get("plays", 1),
                    "last_watched": entry.get("last_watched_at", ""),
                })

        # Shows
        try:
            watched_shows = await trakt.get_watched(kind="shows")
        except Exception as e:
            log.warning("library_health.trakt_shows_failed", error=str(e)[:120])
            watched_shows = []

        for entry in watched_shows:
            show = entry.get("show", {})
            ids = show.get("ids", {})
            title = show.get("title", "")
            year = show.get("year")

            in_library = await self._is_in_library(ids)
            if not in_library:
                # Count episodes watched
                ep_count = 0
                for season in entry.get("seasons", []):
                    ep_count += len(season.get("episodes", []))

                missing_shows.append({
                    "title": title,
                    "year": year,
                    "imdb_id": ids.get("imdb"),
                    "tmdb_id": ids.get("tmdb"),
                    "tvdb_id": ids.get("tvdb"),
                    "trakt_id": ids.get("trakt"),
                    "plays": entry.get("plays", 1),
                    "episodes_watched": ep_count,
                    "last_watched": entry.get("last_watched_at", ""),
                })

        # Sort by most recently watched
        missing_movies.sort(key=lambda x: x.get("last_watched", ""), reverse=True)
        missing_shows.sort(key=lambda x: x.get("last_watched", ""), reverse=True)

        return missing_movies, missing_shows

    # ── Scan: missing sequels ───────────────────────────────────────────

    async def _scan_missing_sequels(
        self, trakt: TraktClient, min_rating: int = 8, max_lookups: int = 30,
    ) -> list[dict]:
        """For movies you rated 8+, check if Trakt's related movies are in your library."""
        results: list[dict] = []

        try:
            ratings = await trakt.get_user_ratings(kind="movies")
        except Exception as e:
            log.warning("library_health.ratings_failed", error=str(e)[:120])
            return results

        # Filter to highly rated, sort by rating desc
        high_rated = [
            r for r in ratings
            if (r.get("rating") or 0) >= min_rating
        ]
        high_rated.sort(key=lambda x: x.get("rating", 0), reverse=True)

        # Limit lookups to avoid hammering Trakt
        lookups_done = 0
        for entry in high_rated:
            if lookups_done >= max_lookups:
                break

            movie = entry.get("movie", {})
            ids = movie.get("ids", {})
            trakt_id = ids.get("trakt")
            if not trakt_id:
                continue

            try:
                related = await trakt.get_related("movies", str(trakt_id), limit=5)
                lookups_done += 1
            except Exception:
                continue

            for rel in (related or []):
                rel_ids = rel.get("ids", {})
                rel_title = rel.get("title", "")
                rel_year = rel.get("year")

                in_library = await self._is_in_library(rel_ids)
                if not in_library:
                    # Avoid duplicates
                    if any(r["title"] == rel_title and r.get("year") == rel_year for r in results):
                        continue
                    results.append({
                        "title": rel_title,
                        "year": rel_year,
                        "imdb_id": rel_ids.get("imdb"),
                        "tmdb_id": rel_ids.get("tmdb"),
                        "trakt_id": rel_ids.get("trakt"),
                        "related_to": movie.get("title", ""),
                        "your_rating": entry.get("rating"),
                    })

            # Brief pause to respect rate limits
            await asyncio.sleep(0.2)

        # Sort by your rating of the source movie
        results.sort(key=lambda x: x.get("your_rating", 0), reverse=True)
        return results[:50]

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _is_in_library(self, ids: dict) -> bool:
        """Check if an item with these provider IDs exists in the library cache."""
        for provider, key in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
            val = ids.get(key)
            if val:
                match = await LibraryCache.find_by_provider_id(provider, str(val))
                if match and match.get("emby_id"):
                    return True
        return False

    async def _get_trakt_client(self, user: User) -> TraktClient | None:
        """Build an authenticated TraktClient with token refresh callback."""
        if not user.trakt_access_token:
            return None

        async def _on_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(
                    select(User).where(User.id == user.id)
                )).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await db.commit()

        return TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=_on_refresh,
        )

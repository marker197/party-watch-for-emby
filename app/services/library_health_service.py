"""Library Health Monitor — finds gaps in your Emby library.

Scans:
  1. Incomplete series — shows in your library with unwatched episodes
  2. Simkl watched, not in library — movies/shows in Simkl history but
     missing from Emby
  3. Highly rated missing sequels — movies you rated 8+ whose Simkl-related
     films aren't in your library

Results are cached in Redis for 6 hours. A manual or scheduled scan
replaces the cache.
"""

from __future__ import annotations

import asyncio
import json as _json
from datetime import datetime, timezone

import structlog

from app.utils.simkl_client import SimklClient
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
        """Return cached report or empty placeholder, with dismissed items filtered out."""
        r = await get_redis()
        raw = await r.get(f"{CACHE_KEY}:{user.id}")
        if raw:
            report = _json.loads(raw)
            return await self._apply_dismissals(report, user.id)
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

        simkl = await self._get_simkl_client(user)
        if not simkl:
            report["error"] = "No Simkl account linked"
            return report

        emby = EmbyClient()
        try:
            # ── 1. Incomplete series ────────────────────────────────────
            report["incomplete_series"] = await self._scan_incomplete_series(
                emby, user.emby_user_id,
            )

            # ── 2. Simkl watched, not in library ───────────────────────
            movies_missing, shows_missing = await self._scan_watched_not_in_library(
                simkl,
            )
            report["watched_not_in_library"]["movies"] = movies_missing[:100]
            report["watched_not_in_library"]["shows"] = shows_missing[:100]

            # ── 3. Missing sequels for highly rated movies ─────────────
            report["missing_sequels"] = await self._scan_missing_sequels(
                simkl,
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
            await simkl.close()

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
        return await self._apply_dismissals(report, user.id)

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

    # ── Scan: Simkl watched not in library ──────────────────────────────

    async def _scan_watched_not_in_library(
        self, simkl: SimklClient,
    ) -> tuple[list[dict], list[dict]]:
        """Find items in Simkl/MDBList watched history that aren't in the Emby library."""

        missing_movies: list[dict] = []
        missing_shows: list[dict] = []
        seen_ids: set[str] = set()  # dedup across sources

        # ── Simkl watched ──
        try:
            watched_movies = await simkl.get_watched(kind="movies")
        except Exception as e:
            log.warning("library_health.simkl_movies_failed", error=str(e)[:120])
            watched_movies = []

        for entry in watched_movies:
            movie = entry.get("movie") or entry
            ids = movie.get("ids", {})
            title = movie.get("title", "")
            year = movie.get("year")
            dedup_key = ids.get("imdb") or ids.get("tmdb") or title
            if dedup_key in seen_ids:
                continue

            in_library = await self._is_in_library(ids)
            if not in_library:
                seen_ids.add(dedup_key)
                missing_movies.append({
                    "title": title, "year": year,
                    "imdb_id": ids.get("imdb"), "tmdb_id": ids.get("tmdb"),
                    "simkl_id": ids.get("simkl") or ids.get("simkl_id"),
                    "plays": entry.get("plays", 1),
                    "last_watched": entry.get("last_watched_at", ""),
                })

        try:
            watched_shows = await simkl.get_watched(kind="shows")
        except Exception as e:
            log.warning("library_health.simkl_shows_failed", error=str(e)[:120])
            watched_shows = []

        for entry in watched_shows:
            show = entry.get("show") or entry
            ids = show.get("ids", {})
            title = show.get("title", "")
            year = show.get("year")
            dedup_key = ids.get("imdb") or ids.get("tvdb") or title
            if dedup_key in seen_ids:
                continue

            in_library = await self._is_in_library(ids)
            if not in_library:
                ep_count = 0
                for season in entry.get("seasons", []):
                    ep_count += len(season.get("episodes", []))
                seen_ids.add(dedup_key)
                missing_shows.append({
                    "title": title, "year": year,
                    "imdb_id": ids.get("imdb"), "tmdb_id": ids.get("tmdb"),
                    "tvdb_id": ids.get("tvdb"),
                    "simkl_id": ids.get("simkl") or ids.get("simkl_id"),
                    "plays": entry.get("plays", 1),
                    "episodes_watched": ep_count,
                    "last_watched": entry.get("last_watched_at", ""),
                })

        # ── MDBList watched (supplement) ──
        try:
            mdb = await self._get_mdblist_client()
            if mdb:
                try:
                    mdb_watched = await mdb.get_watched()
                    for entry in mdb_watched.get("movies", []):
                        inner = entry.get("movie") or entry
                        ids = inner.get("ids", {})
                        dedup_key = ids.get("imdb") or ids.get("tmdb") or inner.get("title", "")
                        if dedup_key in seen_ids:
                            continue
                        in_library = await self._is_in_library(ids)
                        if not in_library:
                            seen_ids.add(dedup_key)
                            missing_movies.append({
                                "title": inner.get("title", ""), "year": inner.get("year"),
                                "imdb_id": ids.get("imdb"), "tmdb_id": ids.get("tmdb"),
                                "simkl_id": ids.get("simkl") or ids.get("simkl_id"),
                                "plays": entry.get("plays", 1),
                                "last_watched": entry.get("last_watched_at", ""),
                            })
                    for entry in mdb_watched.get("shows", []):
                        inner = entry.get("show") or entry
                        ids = inner.get("ids", {})
                        dedup_key = ids.get("imdb") or ids.get("tvdb") or inner.get("title", "")
                        if dedup_key in seen_ids:
                            continue
                        in_library = await self._is_in_library(ids)
                        if not in_library:
                            seen_ids.add(dedup_key)
                            missing_shows.append({
                                "title": inner.get("title", ""), "year": inner.get("year"),
                                "imdb_id": ids.get("imdb"), "tmdb_id": ids.get("tmdb"),
                                "tvdb_id": ids.get("tvdb"),
                                "simkl_id": ids.get("simkl") or ids.get("simkl_id"),
                                "plays": entry.get("plays", 1),
                                "last_watched": entry.get("last_watched_at", ""),
                            })
                finally:
                    await mdb.close()
        except Exception:
            pass

        missing_movies.sort(key=lambda x: x.get("last_watched", ""), reverse=True)
        missing_shows.sort(key=lambda x: x.get("last_watched", ""), reverse=True)
        return missing_movies, missing_shows

    # ── Scan: missing sequels ───────────────────────────────────────────

    async def _scan_missing_sequels(
        self, simkl: SimklClient, min_rating: int = 7, max_lookups: int = 30,
    ) -> list[dict]:
        """For movies you rated highly, check if Simkl's similar movies are in your library.
        Uses both Simkl and MDBList ratings."""
        results: list[dict] = []
        seen_titles: set[str] = set()

        # ── Gather highly-rated movies from all sources ──
        high_rated: list[dict] = []  # [{title, rating, simkl_id, imdb_id}, ...]

        # Simkl ratings
        try:
            simkl_ratings = await simkl.get_user_ratings(kind="movies")
            for entry in simkl_ratings:
                r_val = entry.get("rating") or 0
                if r_val < min_rating:
                    continue
                inner = entry.get("movie") or entry
                ids = inner.get("ids", {})
                high_rated.append({
                    "title": inner.get("title", ""),
                    "rating": r_val,
                    "simkl_id": str(ids.get("simkl") or ids.get("simkl_id") or ""),
                    "imdb_id": ids.get("imdb", ""),
                    "tmdb_id": str(ids.get("tmdb", "")),
                })
        except Exception as e:
            log.warning("library_health.simkl_ratings_failed", error=str(e)[:120])

        # MDBList ratings (supplement)
        try:
            mdb = await self._get_mdblist_client()
            if mdb:
                try:
                    mdb_ratings = await mdb.get_ratings()
                    if isinstance(mdb_ratings, dict):
                        seen_imdb: set[str] = {h["imdb_id"] for h in high_rated if h["imdb_id"]}
                        for item in mdb_ratings.get("movies", []):
                            r_val = item.get("rating") or 0
                            if r_val < min_rating:
                                continue
                            inner = item.get("movie") or item
                            ids = inner.get("ids", {})
                            imdb = ids.get("imdb", "")
                            if imdb and imdb in seen_imdb:
                                continue
                            high_rated.append({
                                "title": inner.get("title", ""),
                                "rating": r_val,
                                "simkl_id": "",
                                "imdb_id": imdb,
                                "tmdb_id": str(ids.get("tmdb", "")),
                            })
                            if imdb:
                                seen_imdb.add(imdb)
                finally:
                    await mdb.close()
        except Exception:
            pass

        if not high_rated:
            log.info("library_health.no_high_rated", min_rating=min_rating)
            return results

        # Shuffle so each scan surfaces different suggestions
        import random as _random
        _random.shuffle(high_rated)

        log.info("library_health.high_rated_found", count=len(high_rated),
                 min_rating=min_rating)

        # ── Look up similar movies for each highly-rated item ──
        lookups_done = 0
        for entry in high_rated:
            if lookups_done >= max_lookups:
                break

            simkl_id = entry.get("simkl_id")

            # If no simkl_id, try to resolve from IMDB ID
            if not simkl_id and entry.get("imdb_id"):
                try:
                    search_results = await simkl.search_by_id("imdb", entry["imdb_id"])
                    if search_results and isinstance(search_results, list):
                        first = search_results[0]
                        simkl_id = str(
                            first.get("ids", {}).get("simkl")
                            or first.get("ids", {}).get("simkl_id")
                            or ""
                        )
                except Exception:
                    pass

            if not simkl_id:
                continue

            try:
                related = await simkl.get_related("movies", str(simkl_id), limit=5)
                lookups_done += 1
            except Exception:
                continue

            for rel in (related or []):
                rel_ids = rel.get("ids", {})
                rel_title = rel.get("title", "")
                rel_year = rel.get("year")

                if not rel_title:
                    continue

                # Dedup
                dedup = f"{rel_title}:{rel_year}"
                if dedup in seen_titles:
                    continue

                in_library = await self._is_in_library(rel_ids)
                if not in_library:
                    seen_titles.add(dedup)
                    results.append({
                        "title": rel_title,
                        "year": rel_year,
                        "imdb_id": rel_ids.get("imdb"),
                        "tmdb_id": rel_ids.get("tmdb"),
                        "simkl_id": rel_ids.get("simkl") or rel_ids.get("simkl_id"),
                        "related_to": entry.get("title", ""),
                        "your_rating": entry.get("rating"),
                    })

            # Brief pause to respect Simkl rate limits (1 POST/sec)
            await asyncio.sleep(0.3)

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

    async def _get_dismissed_set(self, user_id: int) -> set[tuple[str, str]]:
        """Load the set of (type, id) pairs the user has dismissed from the DB."""
        from app.models.schema import DismissedHealthItem
        async with async_session() as db:
            rows = (await db.execute(
                select(DismissedHealthItem.item_type, DismissedHealthItem.item_id)
                .where(DismissedHealthItem.user_id == user_id)
            )).all()
        return {(r[0], r[1]) for r in rows}

    async def _apply_dismissals(self, report: dict, user_id: int) -> dict:
        """Filter dismissed items from a cached report and recalculate summary counts."""
        dismissed = await self._get_dismissed_set(user_id)
        if not dismissed:
            return report

        def _item_id(item: dict) -> str:
            """Return the best identifier for an item."""
            for key in ("imdb_id", "tmdb_id", "tvdb_id", "simkl_id"):
                v = item.get(key)
                if v:
                    return str(v)
            return ""

        wnl = report.get("watched_not_in_library", {})
        movies = wnl.get("movies", [])
        shows = wnl.get("shows", [])

        movies = [m for m in movies if ("movie", _item_id(m)) not in dismissed]
        shows = [s for s in shows if ("show", _item_id(s)) not in dismissed]

        report["watched_not_in_library"]["movies"] = movies
        report["watched_not_in_library"]["shows"] = shows

        # Filter missing_sequels (Simkl Suggestions) — dismissed as type "movie"
        sequels = report.get("missing_sequels", [])
        sequels = [s for s in sequels if ("movie", _item_id(s)) not in dismissed]
        report["missing_sequels"] = sequels

        # Recalculate summary
        if "summary" in report:
            report["summary"]["movies_not_in_library"] = len(movies)
            report["summary"]["shows_not_in_library"] = len(shows)
            report["summary"]["missing_sequels"] = len(sequels)
            report["summary"]["total_issues"] = (
                report["summary"].get("incomplete_series", 0)
                + len(movies)
                + len(shows)
                + len(sequels)
            )

        return report

    async def _get_simkl_client(self, user: User) -> SimklClient | None:
        """Build an authenticated SimklClient with token refresh callback."""
        if not user.simkl_access_token:
            return None

        return SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    @staticmethod
    async def _get_mdblist_client():
        """Build an MDBListClient using the stored API key, or None."""
        from app.utils.secure_redis import secure_get
        raw = await secure_get("mdblist_api_key")
        if not raw:
            return None
        key = raw if isinstance(raw, str) else raw.decode()
        if not key:
            return None
        from app.utils.mdblist_client import MDBListClient
        return MDBListClient(api_key=key)

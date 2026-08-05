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

import re as _re

log = structlog.get_logger()

CACHE_KEY = "library_health_v1"
CACHE_TTL = 6 * 3600  # 6 hours


def _normalize_title(title: str) -> str:
    """Strip punctuation and extra whitespace for fuzzy title comparison.
    'Fired Up!' → 'fired up', 'Spider-Man: No Way Home' → 'spider man no way home'
    """
    t = _re.sub(r"[^\w\s]", " ", title.lower())
    return " ".join(t.split())


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
                emby, user.emby_user_id, simkl,
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
        simkl: SimklClient | None = None,
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

            # Resolve provider IDs — library cache is the most reliable source
            # (Emby bulk endpoint may not return ProviderIds for all items)
            title_str = series.get("Name", "")
            imdb_id = None
            tvdb_id = None
            tmdb_id = None

            # 1. Try library cache by title (populated during cache indexing)
            try:
                cached = await LibraryCache.find_by_title(title_str)
                if cached:
                    cpids = cached.get("provider_ids", {})
                    imdb_id = cpids.get("Imdb") or cpids.get("imdb")
                    tvdb_id = cpids.get("Tvdb") or cpids.get("tvdb")
                    tmdb_id = cpids.get("Tmdb") or cpids.get("tmdb")
            except Exception:
                pass

            # 2. Supplement from Emby response ProviderIds
            pids = series.get("ProviderIds") or {}
            if not imdb_id:
                imdb_id = pids.get("Imdb") or pids.get("imdb")
            if not tvdb_id:
                tvdb_id = pids.get("Tvdb") or pids.get("tvdb")
            if not tmdb_id:
                tmdb_id = pids.get("Tmdb") or pids.get("tmdb")

            results.append({
                "title": title_str,
                "year": series.get("ProductionYear"),
                "emby_id": series.get("Id"),
                "imdb_id": imdb_id,
                "tvdb_id": tvdb_id,
                "tmdb_id": tmdb_id,
                "played_episodes": played_eps,
                "unplayed_episodes": unplayed,
                "total_episodes": total_eps,
                "completion_pct": completion,
            })

        # ── Resolve missing IMDB IDs via Simkl ──
        if simkl:
            for item in results:
                if item.get("imdb_id"):
                    continue
                try:
                    search_result = None
                    if item.get("tmdb_id"):
                        search_result = await simkl.search_by_id("tmdb", str(item["tmdb_id"]))
                    if not search_result and item.get("tvdb_id"):
                        search_result = await simkl.search_by_id("tvdb", str(item["tvdb_id"]))
                    if search_result and isinstance(search_result, list) and search_result:
                        found_ids = search_result[0].get("ids", {})
                        if found_ids.get("imdb"):
                            item["imdb_id"] = found_ids["imdb"]
                        if not item.get("tvdb_id") and found_ids.get("tvdb"):
                            item["tvdb_id"] = str(found_ids["tvdb"])
                except Exception:
                    pass
                await asyncio.sleep(0.2)  # Rate limit courtesy

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
        seen_imdb_ids: set[str] = set()

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

        # Pre-populate seen sets with source items — prevents suggesting items
        # the user already rated highly (i.e. the source items themselves)
        for hr in high_rated:
            if hr.get("imdb_id"):
                seen_imdb_ids.add(hr["imdb_id"])

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

            # Track how many suggestions this source item contributes
            per_item_count = 0
            max_per_item = 3

            for rel in (related or []):
                if per_item_count >= max_per_item:
                    break

                rel_ids = rel.get("ids", {})
                rel_title = rel.get("title", "")
                rel_year = rel.get("year")

                if not rel_title:
                    continue

                # Skip self-suggestion (recommended item is the source item itself)
                rel_simkl = str(rel_ids.get("simkl") or rel_ids.get("simkl_id") or "")
                if rel_simkl and rel_simkl == simkl_id:
                    continue
                rel_imdb = rel_ids.get("imdb", "")
                if rel_imdb and rel_imdb == entry.get("imdb_id"):
                    continue
                if _normalize_title(rel_title) == _normalize_title(entry.get("title", "")):
                    continue

                # Resolve IMDB/TMDB if missing (Simkl recommendations often
                # only include simkl_id — need full IDs for IMDB links + Radarr)
                if rel_simkl and not rel_ids.get("imdb") and not rel_ids.get("tmdb"):
                    try:
                        detail = await simkl.get_movie_detail(rel_simkl)
                        if detail and isinstance(detail, dict):
                            full_ids = detail.get("ids", {})
                            if full_ids.get("imdb"):
                                rel_ids["imdb"] = full_ids["imdb"]
                                rel_imdb = full_ids["imdb"]
                            if full_ids.get("tmdb"):
                                rel_ids["tmdb"] = full_ids["tmdb"]
                            if full_ids.get("tvdb"):
                                rel_ids["tvdb"] = full_ids["tvdb"]
                    except Exception:
                        pass

                # Re-check self-suggestion after ID resolution
                if rel_imdb and rel_imdb == entry.get("imdb_id"):
                    continue

                # Dedup by normalized title AND by IMDB ID
                norm_dedup = _normalize_title(rel_title)
                dedup = f"{norm_dedup}:{rel_year}"
                if dedup in seen_titles:
                    continue
                if rel_imdb and rel_imdb in seen_imdb_ids:
                    continue

                # Skip items with no IMDB ID — can't link them
                if not rel_imdb:
                    continue

                in_library = await self._is_in_library(rel_ids, title=rel_title, year=rel_year)
                if not in_library:
                    seen_titles.add(dedup)
                    if rel_imdb:
                        seen_imdb_ids.add(rel_imdb)
                    results.append({
                        "title": rel_title,
                        "year": rel_year,
                        "imdb_id": rel_ids.get("imdb"),
                        "tmdb_id": rel_ids.get("tmdb"),
                        "simkl_id": rel_ids.get("simkl") or rel_ids.get("simkl_id"),
                        "related_to": entry.get("title", ""),
                        "your_rating": entry.get("rating"),
                    })
                    per_item_count += 1

            # Brief pause to respect Simkl rate limits (1 POST/sec)
            await asyncio.sleep(0.3)

        results.sort(key=lambda x: x.get("your_rating", 0), reverse=True)
        return results[:50]

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _is_in_library(self, ids: dict, title: str = "", year: int | None = None) -> bool:
        """Check if an item with these provider IDs exists in the library cache."""
        for provider, key in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
            val = ids.get(key)
            if val:
                match = await LibraryCache.find_by_provider_id(provider, str(val))
                if match and match.get("emby_id"):
                    return True
        # Title fallback when IDs don't match (e.g. Simkl recommendations without IMDB/TMDB)
        if title:
            match = await LibraryCache.find_by_title(title, year=year)
            if match and match.get("emby_id"):
                return True
            if year:
                match = await LibraryCache.find_by_title(title, year=None)
                if match and match.get("emby_id"):
                    return True
            # Emby search fallback — cache may not have indexed this item,
            # or title has punctuation differences (e.g. "Fired Up" vs "Fired Up!")
            try:
                emby = EmbyClient()
                try:
                    norm_title = _normalize_title(title)
                    for search_type in ("Movie", "Series"):
                        results = await emby.search_items(title, item_type=search_type)
                        for res in results:
                            if _normalize_title(res.get("Name", "")) == norm_title:
                                return True
                finally:
                    await emby.close()
            except Exception:
                pass
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

"""Airing Soon — Season Finale / Premiere Alerts (shows) + Watchlist Release Alerts (movies).

Cross-references Trakt's calendar endpoints — upcoming episodes for shows the
user follows, season/series premieres, and release dates for watchlisted
movies — with the Emby library so the dashboard can surface a single
"Airing Soon" feed with premiere/finale badges and a days-until-air
countdown, covering both shows and movies.

Reuses TraktClient.get_my_shows (already used by Smart Queue for its 14-day
calendar candidate source) but exposes it, plus get_my_premieres and
get_my_movies, as a distinct, richer feed rather than folding them
anonymously into the queue.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog

from app.models.schema import User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.database import async_session
from sqlalchemy import select

log = structlog.get_logger()

SEASON_INFO_CACHE_TTL = 86400  # 24h — season episode counts rarely change mid-run


class AiringAlertsService:
    async def get_airing_soon(self, user: User, days: int = 14) -> list[dict]:
        """Return upcoming episodes + watchlisted movie releases for `user`,
        sorted by days_until_air.

        Each entry: media_type ("show"/"movie"), title, season/episode
        (shows only), air_date, days_until_air, is_premiere, is_finale
        (shows only), in_library, emby_item_id.
        """
        if not user.trakt_access_token:
            return []

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
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            results = []
            results.extend(await self._get_show_alerts(trakt, today, days, user=user))
            results.extend(await self._get_movie_alerts(trakt, today, days))
            results.sort(key=lambda r: (r["days_until_air"] if r["days_until_air"] is not None else 9999))
            return results
        finally:
            await trakt.close()

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    async def _get_show_alerts(self, trakt: TraktClient, today: str, days: int,
                              user: User | None = None) -> list[dict]:
        try:
            upcoming = await trakt.get_my_shows(start_date=today, days=days)
        except Exception as e:
            log.warning("airing_alerts.my_shows_failed", error=str(e)[:200])
            upcoming = []

        try:
            premieres = await trakt.get_my_premieres(start_date=today, days=days)
        except Exception as e:
            log.warning("airing_alerts.premieres_failed", error=str(e)[:200])
            premieres = []

        # Key by (show trakt id, season, episode) to dedupe between the
        # two calendar calls — premieres also show up in get_my_shows.
        merged: dict[tuple, dict] = {}
        for entry in upcoming:
            self._merge_entry(merged, entry, is_premiere_source=False)
        for entry in premieres:
            self._merge_entry(merged, entry, is_premiere_source=True)

        results = []
        # Track shows with finales for binge planner
        finale_shows: dict[str, dict] = {}  # trakt_id → {days_until, season, emby_item_id}

        for key, entry in merged.items():
            show = entry["show"]
            episode = entry["episode"]
            show_trakt_id = str(show.get("ids", {}).get("trakt", ""))

            days_until = self._days_until(entry.get("first_aired"))

            is_premiere = entry["is_premiere"] or episode.get("number") == 1
            is_finale = False
            if not is_premiere and show_trakt_id:
                is_finale = await self._is_season_finale(trakt, show_trakt_id, episode)

            in_library, emby_item_id = await self._match_in_library(show)
            # Shows already in the library always surface. For shows NOT
            # in the library, only surface premieres (new seasons / series
            # premieres) — these are shows the user follows on Trakt but
            # doesn't have yet, and they want to know about the premiere
            # so they can grab them (same rationale as movie alerts).
            # Regular mid-season episodes for non-library shows are
            # skipped — Smart Queue's calendar source handles discovery.
            if not in_library and not is_premiere:
                continue

            result = {
                "media_type": "show",
                "title": show.get("title", ""),
                "trakt_id": show_trakt_id,
                "tvdb_id": show.get("ids", {}).get("tvdb"),
                "tmdb_id": show.get("ids", {}).get("tmdb"),
                "imdb_id": show.get("ids", {}).get("imdb"),
                "season": episode.get("season"),
                "episode": episode.get("number"),
                "episode_title": episode.get("title"),
                "air_date": entry.get("first_aired"),
                "days_until_air": days_until,
                "is_premiere": is_premiere,
                "is_finale": is_finale,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
                "year": show.get("year"),
                "binge_plan": None,
            }
            results.append(result)

            if is_finale and days_until is not None and emby_item_id:
                finale_shows[show_trakt_id] = {
                    "days_until": days_until,
                    "season": episode.get("season"),
                    "emby_item_id": emby_item_id,
                    "title": show.get("title", ""),
                }

        # ── Binge planner: compute catch-up info for shows with finales ──
        if finale_shows and user and user.emby_user_id:
            binge_plans = await self._compute_binge_plans(
                finale_shows, merged, user.emby_user_id,
            )
            for r in results:
                tid = r.get("trakt_id", "")
                if tid in binge_plans:
                    r["binge_plan"] = binge_plans[tid]

        return results

    @staticmethod
    def _merge_entry(merged: dict, entry: dict, is_premiere_source: bool) -> None:
        show = entry.get("show", {})
        episode = entry.get("episode", {})
        show_trakt_id = str(show.get("ids", {}).get("trakt", ""))
        key = (show_trakt_id, episode.get("season"), episode.get("number"))
        if key not in merged:
            merged[key] = {"show": show, "episode": episode, "first_aired": entry.get("first_aired"),
                           "is_premiere": is_premiere_source}
        elif is_premiere_source:
            merged[key]["is_premiere"] = True

    async def _is_season_finale(self, trakt: TraktClient, show_trakt_id: str, episode: dict) -> bool:
        """An episode is a season finale if its number equals that season's
        total episode_count. Result is cached per-show for 24h since season
        episode counts don't change once a season airs."""
        season_num = episode.get("season")
        ep_num = episode.get("number")
        if season_num is None or ep_num is None:
            return False

        cache_key = f"airing_alerts:seasons:{show_trakt_id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                seasons = json.loads(cached)
            else:
                seasons = await trakt.get_show_seasons(show_trakt_id)
                await r.setex(cache_key, SEASON_INFO_CACHE_TTL, json.dumps(seasons))
        except Exception:
            log.warning("airing_alerts.season_lookup_failed", show_trakt_id=show_trakt_id)
            return False

        for season in seasons or []:
            if season.get("number") == season_num:
                episode_count = season.get("episode_count")
                return bool(episode_count) and ep_num == episode_count

        return False

    # ------------------------------------------------------------------
    # Binge planner
    # ------------------------------------------------------------------

    async def _compute_binge_plans(
        self,
        finale_shows: dict[str, dict],
        merged_entries: dict[tuple, dict],
        emby_user_id: str,
    ) -> dict[str, dict]:
        """For each show with a finale in the window, compute how many
        unwatched episodes the user needs to get through and the daily
        pace required.

        Returns {trakt_id: binge_plan_dict}.
        """
        plans: dict[str, dict] = {}

        # Batch-fetch Emby UserData for all finale shows
        emby_ids = [info["emby_item_id"] for info in finale_shows.values()
                    if info.get("emby_item_id")]
        emby_data: dict[str, dict] = {}
        if emby_ids:
            try:
                emby = EmbyClient()
                items = await emby.get_user_items_by_ids(emby_user_id, emby_ids)
                for item in items:
                    emby_data[str(item.get("Id", ""))] = item
            except Exception:
                log.warning("binge_planner.emby_fetch_failed")
                return plans

        for trakt_id, info in finale_shows.items():
            eid = info["emby_item_id"]
            item = emby_data.get(eid)
            if not item:
                continue

            user_data = item.get("UserData", {})
            unwatched_in_library = user_data.get("UnplayedItemCount")
            if unwatched_in_library is None:
                continue

            # Count episodes airing before the finale (not including the finale)
            finale_season = info["season"]
            episodes_airing_before = 0
            for key, entry in merged_entries.items():
                entry_trakt_id = str(entry["show"].get("ids", {}).get("trakt", ""))
                if entry_trakt_id != trakt_id:
                    continue
                ep = entry["episode"]
                ep_season = ep.get("season")
                ep_num = ep.get("number")
                # Only count episodes from same season that aren't the finale
                if ep_season == finale_season:
                    ep_days = self._days_until(entry.get("first_aired"))
                    if ep_days is not None and ep_days >= 0 and ep_days < info["days_until"]:
                        episodes_airing_before += 1

            total_to_watch = unwatched_in_library + episodes_airing_before
            days_left = max(info["days_until"], 1)  # avoid /0

            if total_to_watch <= 0:
                plans[trakt_id] = {
                    "status": "caught_up",
                    "total_to_watch": 0,
                    "days_until_finale": info["days_until"],
                    "episodes_per_day": 0,
                    "message": "You're all caught up for the finale!",
                }
            else:
                pace = round(total_to_watch / days_left, 1)
                if pace <= 1:
                    difficulty = "easy"
                elif pace <= 2:
                    difficulty = "moderate"
                elif pace <= 4:
                    difficulty = "ambitious"
                else:
                    difficulty = "marathon"

                plans[trakt_id] = {
                    "status": "behind",
                    "total_to_watch": total_to_watch,
                    "unwatched_available": unwatched_in_library,
                    "episodes_airing_before_finale": episodes_airing_before,
                    "days_until_finale": info["days_until"],
                    "episodes_per_day": pace,
                    "difficulty": difficulty,
                    "message": (
                        f"{total_to_watch} episode{'s' if total_to_watch != 1 else ''} "
                        f"in {days_left} day{'s' if days_left != 1 else ''} "
                        f"— {pace}/day ({difficulty})"
                    ),
                }

        return plans

    # ------------------------------------------------------------------
    # Movies (watchlist releases)
    # ------------------------------------------------------------------

    async def _get_movie_alerts(self, trakt: TraktClient, today: str, days: int) -> list[dict]:
        try:
            releases = await trakt.get_my_movies(start_date=today, days=days)
        except Exception as e:
            log.warning("airing_alerts.my_movies_failed", error=str(e)[:200])
            releases = []

        results = []
        seen: set[str] = set()
        for entry in releases:
            movie = entry.get("movie", {})
            movie_trakt_id = str(movie.get("ids", {}).get("trakt", ""))
            if movie_trakt_id and movie_trakt_id in seen:
                continue
            seen.add(movie_trakt_id)

            # Movie calendar entries use "released" (date only), not
            # "first_aired" (datetime) like the show calendar entries.
            release_date = entry.get("released")
            days_until = self._days_until(release_date)

            in_library, emby_item_id = await self._match_in_library(movie)
            # Unlike shows, do NOT filter movies to "already in library" —
            # a watchlist movie with a release date next week can't possibly
            # be in the Emby library yet; that's the whole point of this
            # alert (know it's coming so you can grab it via Radarr). We
            # still surface in_library/emby_item_id so the UI can badge
            # anything you already happen to have (e.g. an early digital
            # release you grabbed ahead of the calendar date).

            # Fetch typed release dates (theatrical / digital)
            theatrical_release, digital_release = await self._get_typed_releases(
                trakt, movie_trakt_id,
            )

            results.append({
                "media_type": "movie",
                "title": movie.get("title", ""),
                "trakt_id": movie_trakt_id,
                "season": None,
                "episode": None,
                "episode_title": None,
                "air_date": release_date,
                "days_until_air": days_until,
                "is_premiere": True,   # release-date alert — always the "premiere" for a movie
                "is_finale": False,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
                "theatrical_release": theatrical_release,
                "digital_release": digital_release,
            })
        return results

    async def _get_typed_releases(
        self, trakt: TraktClient, movie_trakt_id: str,
    ) -> tuple[str | None, str | None]:
        """Return (theatrical_date, digital_date) for a movie.

        Checks US releases first, falls back to GB. Cached 24h per movie
        since release dates don't change often.
        """
        if not movie_trakt_id:
            return None, None

        cache_key = f"airing_alerts:releases:{movie_trakt_id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data.get("theatrical"), data.get("digital")
        except Exception:
            pass

        theatrical = None
        digital = None

        for country in ("us", "gb"):
            try:
                releases = await trakt.get_movie_releases(movie_trakt_id, country=country)
            except Exception:
                log.debug("airing_alerts.releases_fetch_failed",
                          movie_trakt_id=movie_trakt_id, country=country)
                continue

            for rel in releases or []:
                rtype = rel.get("release_type", "")
                rdate = rel.get("release_date")
                if not rdate:
                    continue
                # Theatrical: premiere, limited, or theatrical
                if rtype in ("premiere", "limited", "theatrical") and not theatrical:
                    theatrical = rdate
                elif rtype == "digital" and not digital:
                    digital = rdate

            # If we got at least one date from this country, stop
            if theatrical or digital:
                break

        # Cache result (even if both None — avoids re-fetching for movies
        # that genuinely have no typed releases)
        try:
            await r.setex(cache_key, SEASON_INFO_CACHE_TTL,
                          json.dumps({"theatrical": theatrical, "digital": digital}))
        except Exception:
            pass

        return theatrical, digital

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _days_until(date_str: str | None) -> int | None:
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return (dt.date() - datetime.now(timezone.utc).date()).days
        except Exception:
            return None

    @staticmethod
    async def _match_in_library(item: dict) -> tuple[bool, str | None]:
        """Cross-check a Trakt show/movie object against the Emby library
        via provider IDs, falling back to title+year matching."""
        ids = item.get("ids", {})
        for provider_type, trakt_key in [("Tvdb", "tvdb"), ("Tmdb", "tmdb"), ("Imdb", "imdb")]:
            pid = ids.get(trakt_key)
            if pid:
                cached = await LibraryCache.find_by_provider_id(provider_type, str(pid))
                if cached:
                    return True, cached["emby_id"]

        cached = await LibraryCache.find_by_title(item.get("title", ""), year=item.get("year"))
        if cached:
            return True, cached["emby_id"]

        return False, None


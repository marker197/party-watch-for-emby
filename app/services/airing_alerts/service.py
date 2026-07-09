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
            results.extend(await self._get_show_alerts(trakt, today, days))
            results.extend(await self._get_movie_alerts(trakt, today, days))
            results.sort(key=lambda r: (r["days_until_air"] if r["days_until_air"] is not None else 9999))
            return results
        finally:
            await trakt.close()

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    async def _get_show_alerts(self, trakt: TraktClient, today: str, days: int) -> list[dict]:
        try:
            upcoming = await trakt.get_my_shows(start_date=today, days=days)
        except Exception:
            log.warning("airing_alerts.my_shows_failed")
            upcoming = []

        try:
            premieres = await trakt.get_my_premieres(start_date=today, days=days)
        except Exception:
            log.warning("airing_alerts.premieres_failed")
            premieres = []

        # Key by (show trakt id, season, episode) to dedupe between the
        # two calendar calls — premieres also show up in get_my_shows.
        merged: dict[tuple, dict] = {}
        for entry in upcoming:
            self._merge_entry(merged, entry, is_premiere_source=False)
        for entry in premieres:
            self._merge_entry(merged, entry, is_premiere_source=True)

        results = []
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
            # Only surface shows actually in the library — this is an alert
            # about what's coming for things you can watch, not a discovery
            # feed (Smart Queue's calendar source already covers "shows you
            # follow but don't have yet").
            if not in_library:
                continue

            results.append({
                "media_type": "show",
                "title": show.get("title", ""),
                "trakt_id": show_trakt_id,
                "season": episode.get("season"),
                "episode": episode.get("number"),
                "episode_title": episode.get("title"),
                "air_date": entry.get("first_aired"),
                "days_until_air": days_until,
                "is_premiere": is_premiere,
                "is_finale": is_finale,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
            })
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
    # Movies (watchlist releases)
    # ------------------------------------------------------------------

    async def _get_movie_alerts(self, trakt: TraktClient, today: str, days: int) -> list[dict]:
        try:
            releases = await trakt.get_my_movies(start_date=today, days=days)
        except Exception:
            log.warning("airing_alerts.my_movies_failed")
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
            })
        return results

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


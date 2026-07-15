"""Watchlist Sync Service.

Daily job that reads missing (monitored, no file) movies and series from
the arr-library data (which queries Radarr/Sonarr), compares against the
user's Trakt watchlist, adds any items not already on the watchlist, and
clears the Airing Soon cache so newly-watchlisted shows with upcoming
premieres appear immediately.
"""

from __future__ import annotations

import json as _json

import structlog
from sqlalchemy import select

from app.models.schema import User
from app.utils.database import async_session
from app.utils.redis_cache import get_redis
from app.utils.trakt_client import TraktClient

log = structlog.get_logger()


class WatchlistSyncService:
    """Sync missing Radarr/Sonarr items → Trakt watchlist → Airing Soon."""

    async def run_for_all_users(self):
        log.info("watchlist_sync.run_start")
        async with async_session() as db:
            users = (await db.execute(
                select(User).where(User.trakt_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self._sync_user(user)
            except Exception:
                log.exception("watchlist_sync.user_error", user_id=user.id)

        log.info("watchlist_sync.run_complete", users_processed=len(users))

    async def _sync_user(self, user: User):
        # Token refresh callback
        async def on_token_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(
                    select(User).where(User.id == user.id)
                )).scalar_one()
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
            # 1. Get missing item IDs from Radarr/Sonarr
            missing_tmdb, missing_tvdb = await self._get_arr_missing_ids()

            if not missing_tmdb and not missing_tvdb:
                log.info("watchlist_sync.nothing_missing", user_id=user.id)
                return

            # 2. Fetch current Trakt watchlist
            wl_movies = await trakt.get_watchlist(kind="movies")
            wl_shows = await trakt.get_watchlist(kind="shows")

            # Build sets of IDs already on watchlist
            wl_tmdb_ids: set[int] = set()
            for item in (wl_movies or []):
                movie = item.get("movie") or {}
                tmdb = (movie.get("ids") or {}).get("tmdb")
                if tmdb:
                    wl_tmdb_ids.add(tmdb)

            wl_tvdb_ids: set[int] = set()
            for item in (wl_shows or []):
                show = item.get("show") or {}
                tvdb = (show.get("ids") or {}).get("tvdb")
                if tvdb:
                    wl_tvdb_ids.add(tvdb)

            # 3. Find items NOT on watchlist
            movies_to_add = [
                {"ids": {"tmdb": tmdb}}
                for tmdb in missing_tmdb
                if tmdb not in wl_tmdb_ids
            ]
            shows_to_add = [
                {"ids": {"tvdb": tvdb}}
                for tvdb in missing_tvdb
                if tvdb not in wl_tvdb_ids
            ]

            if not movies_to_add and not shows_to_add:
                log.info("watchlist_sync.all_on_watchlist",
                         user_id=user.id,
                         missing_movies=len(missing_tmdb),
                         missing_shows=len(missing_tvdb))
                return

            # 4. Add to Trakt watchlist
            log.info("watchlist_sync.adding_to_trakt",
                     user_id=user.id,
                     movies=len(movies_to_add),
                     shows=len(shows_to_add))

            result = await trakt.add_to_watchlist(
                movies=movies_to_add or None,
                shows=shows_to_add or None,
            )

            added = result.get("added", {})
            existing = result.get("existing", {})
            log.info("watchlist_sync.added_to_trakt",
                     user_id=user.id,
                     movies_added=added.get("movies", 0),
                     shows_added=added.get("shows", 0),
                     movies_existing=existing.get("movies", 0),
                     shows_existing=existing.get("shows", 0))

            # 5. If any shows were added, refresh Airing Soon cache
            if shows_to_add:
                await self._refresh_airing_soon(user)

        finally:
            await trakt.close()

    @staticmethod
    async def _get_arr_missing_ids() -> tuple[list[int], list[int]]:
        """Get missing TMDB/TVDB IDs by querying Radarr/Sonarr directly.

        Creates fresh clients per call (no stale httpx connections).
        Bypasses the arr-library cache to get a guaranteed-fresh snapshot.
        """
        from app.utils.radarr_client import RadarrClient
        from app.utils.sonarr_client import SonarrClient

        r = await get_redis()
        missing_tmdb: list[int] = []
        missing_tvdb: list[int] = []

        # --- Radarr ---
        raw = await r.get("radarr_servers")
        if raw:
            for srv in _json.loads(raw):
                try:
                    client = RadarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Radarr"),
                    )
                    movies = await client.get_missing_movies()
                    await client.close()
                    for m in movies:
                        tmdb = m.get("tmdbId")
                        if tmdb:
                            missing_tmdb.append(tmdb)
                    log.debug("watchlist_sync.radarr_missing",
                              server=srv.get("name"), count=len(movies))
                except Exception:
                    log.exception("watchlist_sync.radarr_fetch_failed",
                                  server=srv.get("name"))

        # --- Sonarr ---
        raw = await r.get("sonarr_servers")
        if raw:
            for srv in _json.loads(raw):
                try:
                    client = SonarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Sonarr"),
                    )
                    series = await client.get_missing_series()
                    await client.close()
                    for s in series:
                        tvdb = s.get("tvdbId")
                        if tvdb:
                            missing_tvdb.append(tvdb)
                    log.debug("watchlist_sync.sonarr_missing",
                              server=srv.get("name"), count=len(series))
                except Exception:
                    log.exception("watchlist_sync.sonarr_fetch_failed",
                                  server=srv.get("name"))

        # Deduplicate (dual-server setups)
        missing_tmdb = list(set(missing_tmdb))
        missing_tvdb = list(set(missing_tvdb))

        log.info("watchlist_sync.arr_missing_totals",
                 movies=len(missing_tmdb), shows=len(missing_tvdb))

        return missing_tmdb, missing_tvdb

    @staticmethod
    async def _refresh_airing_soon(user: User):
        """Invalidate Airing Soon cache so newly-watchlisted shows appear."""
        try:
            r = await get_redis()
            keys = []
            async for key in r.scan_iter(match="airing_alerts:*"):
                keys.append(key)
            if keys:
                await r.delete(*keys)
            log.info("watchlist_sync.airing_soon_cache_cleared",
                     user_id=user.id, keys_cleared=len(keys))
        except Exception:
            log.warning("watchlist_sync.airing_soon_cache_clear_failed",
                        user_id=user.id)

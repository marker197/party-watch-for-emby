"""Watchlist Sync Service.

Bi-directional sync between Radarr/Sonarr and Trakt watchlist:

1. **Arr → Watchlist**: Reads missing (monitored, no file) movies/series from
   Radarr/Sonarr, compares against the user's Trakt watchlist, adds any items
   not already there.  Clears the Airing Soon cache afterwards so newly-
   watchlisted shows with upcoming premieres appear immediately.

2. **Watchlist → Arr**: Reads the user's Trakt watchlist, compares against all
   items in Radarr/Sonarr, adds any items not already present using the first
   configured server's quality profile and root folder.

Both directions are independently toggleable via the watchlist_sync_settings
stored in Redis/DB.
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

SETTINGS_KEY = "watchlist_sync_settings"


class WatchlistSyncService:
    """Bi-directional sync: Radarr/Sonarr ↔ Trakt watchlist."""

    async def run_for_all_users(self):
        settings = await self._get_settings()
        if not settings.get("arr_to_watchlist") and not settings.get("watchlist_to_arr"):
            log.info("watchlist_sync.both_disabled")
            return

        log.info("watchlist_sync.run_start",
                 arr_to_wl=settings.get("arr_to_watchlist"),
                 wl_to_arr=settings.get("watchlist_to_arr"))

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
        settings = await self._get_settings()

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
            if settings.get("arr_to_watchlist"):
                await self._arr_to_watchlist(user, trakt)

            if settings.get("watchlist_to_arr"):
                await self._watchlist_to_arr(user, trakt)
        finally:
            await trakt.close()

    # ------------------------------------------------------------------
    # Direction 1: Arr → Watchlist
    # ------------------------------------------------------------------

    async def _arr_to_watchlist(self, user: User, trakt: TraktClient):
        """Send missing Radarr/Sonarr items to Trakt watchlist (with dupe check)."""
        missing_tmdb, missing_tvdb = await self._get_arr_missing_ids()

        if not missing_tmdb and not missing_tvdb:
            log.info("watchlist_sync.arr_to_wl.nothing_missing", user_id=user.id)
            return

        # Fetch current Trakt watchlist for dupe check
        wl_movies = await trakt.get_watchlist(kind="movies")
        wl_shows = await trakt.get_watchlist(kind="shows")

        wl_tmdb_ids: set[int] = set()
        for item in (wl_movies or []):
            tmdb = ((item.get("movie") or {}).get("ids") or {}).get("tmdb")
            if tmdb:
                wl_tmdb_ids.add(tmdb)

        wl_tvdb_ids: set[int] = set()
        for item in (wl_shows or []):
            tvdb = ((item.get("show") or {}).get("ids") or {}).get("tvdb")
            if tvdb:
                wl_tvdb_ids.add(tvdb)

        # Filter out items already on watchlist
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
            log.info("watchlist_sync.arr_to_wl.all_on_watchlist",
                     user_id=user.id,
                     missing_movies=len(missing_tmdb),
                     missing_shows=len(missing_tvdb))
            return

        log.info("watchlist_sync.arr_to_wl.adding",
                 user_id=user.id,
                 movies=len(movies_to_add),
                 shows=len(shows_to_add))

        result = await trakt.add_to_watchlist(
            movies=movies_to_add or None,
            shows=shows_to_add or None,
        )

        added = result.get("added", {})
        existing = result.get("existing", {})
        log.info("watchlist_sync.arr_to_wl.done",
                 user_id=user.id,
                 movies_added=added.get("movies", 0),
                 shows_added=added.get("shows", 0),
                 movies_existing=existing.get("movies", 0),
                 shows_existing=existing.get("shows", 0))

        # Refresh Airing Soon cache if any shows were added
        if shows_to_add:
            await self._refresh_airing_soon(user)

    # ------------------------------------------------------------------
    # Direction 2: Watchlist → Arr
    # ------------------------------------------------------------------

    async def _watchlist_to_arr(self, user: User, trakt: TraktClient):
        """Add Trakt watchlist items to Radarr/Sonarr if not already there."""
        from app.utils.radarr_client import RadarrClient
        from app.utils.sonarr_client import SonarrClient

        r = await get_redis()

        # Fetch Trakt watchlist
        wl_movies = await trakt.get_watchlist(kind="movies")
        wl_shows = await trakt.get_watchlist(kind="shows")

        # Build watchlist ID maps: {tmdb_id: movie_data, ...}
        wl_movie_map: dict[int, dict] = {}
        for item in (wl_movies or []):
            movie = item.get("movie") or {}
            tmdb = (movie.get("ids") or {}).get("tmdb")
            if tmdb:
                wl_movie_map[tmdb] = movie

        wl_show_map: dict[int, dict] = {}
        for item in (wl_shows or []):
            show = item.get("show") or {}
            tvdb = (show.get("ids") or {}).get("tvdb")
            if tvdb:
                wl_show_map[tvdb] = show

        if not wl_movie_map and not wl_show_map:
            log.info("watchlist_sync.wl_to_arr.empty_watchlist", user_id=user.id)
            return

        # Get all items currently in Radarr/Sonarr
        radarr_tmdb_ids, sonarr_tvdb_ids = await self._get_arr_all_ids()

        # --- Movies → Radarr ---
        movies_to_add = {
            tmdb: data for tmdb, data in wl_movie_map.items()
            if tmdb not in radarr_tmdb_ids
        }

        movies_added = 0
        movies_failed = 0
        if movies_to_add:
            raw = await r.get("radarr_servers")
            if raw:
                servers = _json.loads(raw)
                if servers:
                    srv = servers[0]  # Use first server
                    client = None
                    try:
                        client = RadarrClient(
                            srv["url"], srv["api_key"],
                            name=srv.get("name", "Radarr"),
                        )
                        profile_id = srv.get("quality_profile_id")
                        for tmdb, movie in movies_to_add.items():
                            try:
                                result = await client.add_movie(
                                    tmdb_id=tmdb,
                                    imdb_id=(movie.get("ids") or {}).get("imdb"),
                                    title=movie.get("title", ""),
                                    year=movie.get("year"),
                                    quality_profile_id=int(profile_id) if profile_id else None,
                                )
                                if result.get("status") == "error":
                                    log.warning("watchlist_sync.wl_to_arr.radarr_add_failed",
                                                tmdb=tmdb, reason=result.get("reason", "")[:120])
                                    movies_failed += 1
                                else:
                                    movies_added += 1
                                    log.info("watchlist_sync.wl_to_arr.radarr_added",
                                             tmdb=tmdb, title=movie.get("title", ""))
                            except Exception as e:
                                # Radarr returns 400 if movie already exists
                                err = str(e)[:120]
                                if "already" in err.lower() or "exist" in err.lower():
                                    log.debug("watchlist_sync.wl_to_arr.radarr_exists",
                                              tmdb=tmdb)
                                else:
                                    log.warning("watchlist_sync.wl_to_arr.radarr_add_error",
                                                tmdb=tmdb, error=err)
                                    movies_failed += 1
                    except Exception:
                        log.exception("watchlist_sync.wl_to_arr.radarr_connect_failed",
                                      server=srv.get("name"))
                    finally:
                        if client:
                            await client.close()

        # --- Shows → Sonarr ---
        shows_to_add = {
            tvdb: data for tvdb, data in wl_show_map.items()
            if tvdb not in sonarr_tvdb_ids
        }

        shows_added = 0
        shows_failed = 0
        if shows_to_add:
            raw = await r.get("sonarr_servers")
            if raw:
                servers = _json.loads(raw)
                if servers:
                    srv = servers[0]  # Use first server
                    client = None
                    try:
                        client = SonarrClient(
                            srv["url"], srv["api_key"],
                            name=srv.get("name", "Sonarr"),
                        )
                        profile_id = srv.get("quality_profile_id")
                        for tvdb, show in shows_to_add.items():
                            try:
                                result = await client.add_series(
                                    tvdb_id=tvdb,
                                    imdb_id=(show.get("ids") or {}).get("imdb"),
                                    title=show.get("title", ""),
                                    year=show.get("year"),
                                    quality_profile_id=int(profile_id) if profile_id else None,
                                )
                                if result.get("status") == "error":
                                    log.warning("watchlist_sync.wl_to_arr.sonarr_add_failed",
                                                tvdb=tvdb, reason=result.get("reason", "")[:120])
                                    shows_failed += 1
                                else:
                                    shows_added += 1
                                    log.info("watchlist_sync.wl_to_arr.sonarr_added",
                                             tvdb=tvdb, title=show.get("title", ""))
                            except Exception as e:
                                err = str(e)[:120]
                                if "already" in err.lower() or "exist" in err.lower():
                                    log.debug("watchlist_sync.wl_to_arr.sonarr_exists",
                                              tvdb=tvdb)
                                else:
                                    log.warning("watchlist_sync.wl_to_arr.sonarr_add_error",
                                                tvdb=tvdb, error=err)
                                    shows_failed += 1
                    except Exception:
                        log.exception("watchlist_sync.wl_to_arr.sonarr_connect_failed",
                                      server=srv.get("name"))
                    finally:
                        if client:
                            await client.close()

        log.info("watchlist_sync.wl_to_arr.done",
                 user_id=user.id,
                 movies_added=movies_added,
                 movies_failed=movies_failed,
                 movies_skipped=len(wl_movie_map) - len(movies_to_add),
                 shows_added=shows_added,
                 shows_failed=shows_failed,
                 shows_skipped=len(wl_show_map) - len(shows_to_add))

        # Invalidate Coming Soon cache so new items appear immediately
        if movies_added or shows_added:
            try:
                await r.delete("availability_monitor_v2")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _get_settings() -> dict:
        """Read watchlist sync settings from Redis (DB fallback)."""
        try:
            r = await get_redis()
            raw = await r.get(SETTINGS_KEY)
            if raw:
                return _json.loads(raw)
        except Exception:
            pass
        # Defaults: both off
        return {"arr_to_watchlist": False, "watchlist_to_arr": False}

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
                client = None
                try:
                    client = RadarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Radarr"),
                    )
                    movies = await client.get_missing_movies()
                    for m in movies:
                        tmdb = m.get("tmdbId")
                        if tmdb:
                            missing_tmdb.append(tmdb)
                    log.debug("watchlist_sync.radarr_missing",
                              server=srv.get("name"), count=len(movies))
                except Exception:
                    log.exception("watchlist_sync.radarr_fetch_failed",
                                  server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        # --- Sonarr ---
        raw = await r.get("sonarr_servers")
        if raw:
            for srv in _json.loads(raw):
                client = None
                try:
                    client = SonarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Sonarr"),
                    )
                    series = await client.get_missing_series()
                    for s in series:
                        tvdb = s.get("tvdbId")
                        if tvdb:
                            missing_tvdb.append(tvdb)
                    log.debug("watchlist_sync.sonarr_missing",
                              server=srv.get("name"), count=len(series))
                except Exception:
                    log.exception("watchlist_sync.sonarr_fetch_failed",
                                  server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        # Deduplicate (dual-server setups)
        missing_tmdb = list(set(missing_tmdb))
        missing_tvdb = list(set(missing_tvdb))

        log.info("watchlist_sync.arr_missing_totals",
                 movies=len(missing_tmdb), shows=len(missing_tvdb))

        return missing_tmdb, missing_tvdb

    @staticmethod
    async def _get_arr_all_ids() -> tuple[set[int], set[int]]:
        """Get ALL TMDB/TVDB IDs across all Radarr/Sonarr servers.

        Unlike _get_arr_missing_ids, this returns every item (not just
        missing ones) — used for dupe-checking before adding watchlist
        items to arr.
        """
        from app.utils.radarr_client import RadarrClient
        from app.utils.sonarr_client import SonarrClient

        r = await get_redis()
        radarr_tmdb: set[int] = set()
        sonarr_tvdb: set[int] = set()

        raw = await r.get("radarr_servers")
        if raw:
            for srv in _json.loads(raw):
                client = None
                try:
                    client = RadarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Radarr"),
                    )
                    movies = await client.get_all_movies()
                    for m in movies:
                        tmdb = m.get("tmdbId")
                        if tmdb:
                            radarr_tmdb.add(tmdb)
                except Exception:
                    log.exception("watchlist_sync.radarr_all_fetch_failed",
                                  server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        raw = await r.get("sonarr_servers")
        if raw:
            for srv in _json.loads(raw):
                client = None
                try:
                    client = SonarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Sonarr"),
                    )
                    series = await client.get_all_series()
                    for s in series:
                        tvdb = s.get("tvdbId")
                        if tvdb:
                            sonarr_tvdb.add(tvdb)
                except Exception:
                    log.exception("watchlist_sync.sonarr_all_fetch_failed",
                                  server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        return radarr_tmdb, sonarr_tvdb

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

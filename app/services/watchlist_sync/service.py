"""Watchlist Sync Service.

Bi-directional sync between Radarr/Sonarr and Simkl/MDBList watchlists:

1. **Arr → Watchlist**: Reads missing (monitored, no file) movies/series from
   Radarr/Sonarr, compares against the user's watchlist(s), adds any items
   not already there.  Clears the Airing Soon cache afterwards so newly-
   watchlisted shows with upcoming premieres appear immediately.

2. **Watchlist → Arr**: Reads the user's watchlist(s), compares against all
   items in Radarr/Sonarr, adds any items not already present using the first
   configured server's quality profile and root folder.

Both directions are independently toggleable via the watchlist_sync_settings
stored in Redis/DB.  Which provider(s) are synced depends on the configured
integration_provider setting (simkl, mdblist, both, or none).
"""

from __future__ import annotations

import json as _json

import structlog
from sqlalchemy import select

from app.models.schema import User
from app.utils.database import async_session
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.utils.simkl_client import SimklClient

log = structlog.get_logger()

SETTINGS_KEY = "watchlist_sync_settings"


class WatchlistSyncService:
    """Bi-directional sync: Radarr/Sonarr ↔ Simkl/MDBList watchlist."""

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_for_all_users(self):
        settings = await self._get_settings()
        if not settings.get("arr_to_watchlist") and not settings.get("watchlist_to_arr"):
            log.info("watchlist_sync.both_disabled")
            return

        providers = await self._get_active_providers()
        if not providers:
            log.info("watchlist_sync.no_active_providers")
            return

        log.info("watchlist_sync.run_start",
                 arr_to_wl=settings.get("arr_to_watchlist"),
                 wl_to_arr=settings.get("watchlist_to_arr"),
                 providers=sorted(providers))

        async with async_session() as db:
            # Users with Simkl tokens OR any linked user when MDBList-only
            if "simkl" in providers:
                users = (await db.execute(
                    select(User).where(User.simkl_access_token.isnot(None))
                )).scalars().all()
            else:
                # MDBList-only mode: use all users (MDBList uses API key, not per-user tokens)
                users = (await db.execute(select(User))).scalars().all()

        if not users:
            log.info("watchlist_sync.no_users")
            return

        for user in users:
            try:
                await self._sync_user(user, providers)
            except Exception:
                log.exception("watchlist_sync.user_error", user_id=user.id)

        log.info("watchlist_sync.run_complete", users_processed=len(users))

    async def _sync_user(self, user: User, providers: set[str] | None = None):
        if providers is None:
            providers = await self._get_active_providers()

        settings = await self._get_settings()

        # ---------- Simkl ----------
        if "simkl" in providers and user.simkl_access_token:
            simkl = await self._build_simkl_client(user)
            try:
                if settings.get("arr_to_watchlist"):
                    await self._arr_to_simkl_watchlist(user, simkl)
                if settings.get("watchlist_to_arr"):
                    await self._simkl_watchlist_to_arr(user, simkl)
            finally:
                await simkl.close()

        # ---------- MDBList ----------
        if "mdblist" in providers:
            mdb = await self._build_mdblist_client()
            if mdb:
                try:
                    if settings.get("arr_to_watchlist"):
                        await self._arr_to_mdblist_watchlist(user, mdb)
                    if settings.get("watchlist_to_arr"):
                        await self._mdblist_watchlist_to_arr(user, mdb)
                finally:
                    await mdb.close()

    # ------------------------------------------------------------------
    # Client builders
    # ------------------------------------------------------------------

    @staticmethod
    async def _build_simkl_client(user: User) -> SimklClient:
        async def on_token_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(
                    select(User).where(User.id == user.id)
                )).scalar_one()
                u.simkl_access_token = access
                
                u.simkl_token_expires = expires
                await db.commit()

        return SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    @staticmethod
    async def _build_mdblist_client():
        """Build an MDBListClient using the stored API key, or None if not configured."""
        from app.utils.mdblist_client import MDBListClient
        r = await get_redis()
        raw = await secure_get("mdblist_api_key")
        key = (raw if isinstance(raw, str) else raw.decode()) if raw else ""
        if not key:
            log.debug("watchlist_sync.mdblist.no_api_key")
            return None
        return MDBListClient(api_key=key)

    # ==================================================================
    # Direction 1: Arr → Watchlist
    # ==================================================================

    async def _arr_to_simkl_watchlist(self, user: User, simkl: SimklClient):
        """Send missing Radarr/Sonarr items to Simkl watchlist (with dupe check)."""
        missing_tmdb, missing_tvdb = await self._get_arr_missing_ids()

        if not missing_tmdb and not missing_tvdb:
            log.info("watchlist_sync.arr_to_simkl.nothing_missing", user_id=user.id)
            return

        # Fetch current Simkl watchlist for dupe check
        wl_movies = await simkl.get_watchlist(kind="movies")
        wl_shows = await simkl.get_watchlist(kind="shows")

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
            log.info("watchlist_sync.arr_to_simkl.all_on_watchlist",
                     user_id=user.id,
                     missing_movies=len(missing_tmdb),
                     missing_shows=len(missing_tvdb))
            return

        log.info("watchlist_sync.arr_to_simkl.adding",
                 user_id=user.id,
                 movies=len(movies_to_add),
                 shows=len(shows_to_add))

        result = await simkl.add_to_watchlist(
            movies=movies_to_add or None,
            shows=shows_to_add or None,
        )

        added = result.get("added", {})
        existing = result.get("existing", {})
        log.info("watchlist_sync.arr_to_simkl.done",
                 user_id=user.id,
                 movies_added=added.get("movies", 0),
                 shows_added=added.get("shows", 0),
                 movies_existing=existing.get("movies", 0),
                 shows_existing=existing.get("shows", 0))

        # Refresh Airing Soon cache if any shows were added
        if shows_to_add:
            await self._refresh_airing_soon(user)

    async def _arr_to_mdblist_watchlist(self, user: User, mdb):
        """Send missing Radarr/Sonarr items to MDBList watchlist (with dupe check)."""
        missing_tmdb, missing_tvdb = await self._get_arr_missing_ids()

        if not missing_tmdb and not missing_tvdb:
            log.info("watchlist_sync.arr_to_mdblist.nothing_missing", user_id=user.id)
            return

        # Fetch current MDBList watchlist for dupe check
        # MDBList returns {"movies": [...], "shows": [...]} or a flat list
        wl_data = await mdb.get_watchlist()

        wl_tmdb_ids: set[int] = set()
        wl_imdb_ids: set[str] = set()

        # MDBList watchlist items have tmdb/imdb at top level
        wl_items = []
        if isinstance(wl_data, dict):
            wl_items = (wl_data.get("movies") or []) + (wl_data.get("shows") or [])
        elif isinstance(wl_data, list):
            wl_items = wl_data

        for item in wl_items:
            tmdb = item.get("tmdb") or item.get("tmdbid")
            imdb = item.get("imdb") or item.get("imdbid")
            if tmdb:
                wl_tmdb_ids.add(int(tmdb))
            if imdb:
                wl_imdb_ids.add(str(imdb))

        # Filter out items already on watchlist (check by TMDB ID)
        movies_to_add = [
            {"tmdb": tmdb}
            for tmdb in missing_tmdb
            if tmdb not in wl_tmdb_ids
        ]

        # For shows, MDBList uses TMDB IDs too — but Sonarr gives us TVDB IDs.
        # We can pass TVDB as imdb won't work. MDBList accepts tmdb for shows.
        # Since we only have tvdb_id from Sonarr, we'll try to look up TMDB
        # from our library cache or just pass what we have.
        # MDBList add_to_watchlist accepts: {"tmdb": N} or {"imdb": "tt..."}
        # It does NOT accept tvdb. So we need TMDB or IMDB for shows.
        # Best effort: check library cache for TMDB ID mapped from TVDB.
        shows_to_add = []
        if missing_tvdb:
            tvdb_to_tmdb = await self._resolve_tvdb_to_tmdb(missing_tvdb)
            for tvdb in missing_tvdb:
                tmdb = tvdb_to_tmdb.get(tvdb)
                if tmdb and tmdb not in wl_tmdb_ids:
                    shows_to_add.append({"tmdb": tmdb})
                elif not tmdb:
                    log.debug("watchlist_sync.arr_to_mdblist.no_tmdb_for_tvdb",
                              tvdb=tvdb)

        if not movies_to_add and not shows_to_add:
            log.info("watchlist_sync.arr_to_mdblist.all_on_watchlist",
                     user_id=user.id,
                     missing_movies=len(missing_tmdb),
                     missing_shows=len(missing_tvdb))
            return

        log.info("watchlist_sync.arr_to_mdblist.adding",
                 user_id=user.id,
                 movies=len(movies_to_add),
                 shows=len(shows_to_add))

        result = await mdb.add_to_watchlist(
            movies=movies_to_add or None,
            shows=shows_to_add or None,
        )

        log.info("watchlist_sync.arr_to_mdblist.done",
                 user_id=user.id,
                 result=str(result)[:200])

        if shows_to_add:
            await self._refresh_airing_soon(user)

    # ==================================================================
    # Direction 2: Watchlist → Arr
    # ==================================================================

    async def _simkl_watchlist_to_arr(self, user: User, simkl: SimklClient):
        """Add Simkl watchlist items to Radarr/Sonarr if not already there."""
        wl_movies = await simkl.get_watchlist(kind="movies")
        wl_shows = await simkl.get_watchlist(kind="shows")

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
            log.info("watchlist_sync.simkl_to_arr.empty_watchlist", user_id=user.id)
            return

        await self._add_to_arr(
            user, wl_movie_map, wl_show_map,
            log_prefix="watchlist_sync.simkl_to_arr",
        )

    async def _mdblist_watchlist_to_arr(self, user: User, mdb):
        """Add MDBList watchlist items to Radarr/Sonarr if not already there."""
        wl_data = await mdb.get_watchlist()

        # Parse MDBList response — items have tmdb/imdb/tvdbid at top level
        wl_items = []
        if isinstance(wl_data, dict):
            wl_items = (wl_data.get("movies") or []) + (wl_data.get("shows") or [])
        elif isinstance(wl_data, list):
            wl_items = wl_data

        wl_movie_map: dict[int, dict] = {}
        wl_show_map: dict[int, dict] = {}

        for item in wl_items:
            mediatype = item.get("mediatype") or item.get("type") or ""
            tmdb = item.get("tmdb") or item.get("tmdbid")
            imdb = item.get("imdb") or item.get("imdbid")
            tvdb = item.get("tvdbid") or item.get("tvdb")
            title = item.get("title") or ""
            year = item.get("year")

            if mediatype == "movie" or (not mediatype and not tvdb):
                if tmdb:
                    wl_movie_map[int(tmdb)] = {
                        "title": title,
                        "year": year,
                        "ids": {"tmdb": int(tmdb), "imdb": imdb or None},
                    }
            elif mediatype == "show" or tvdb:
                if tvdb:
                    wl_show_map[int(tvdb)] = {
                        "title": title,
                        "year": year,
                        "ids": {"tvdb": int(tvdb), "imdb": imdb or None},
                    }
                elif tmdb:
                    # No TVDB ID — try to resolve from TMDB
                    # For now, skip shows without TVDB (Sonarr requires TVDB)
                    log.debug("watchlist_sync.mdblist_to_arr.show_no_tvdb",
                              tmdb=tmdb, title=title)

        if not wl_movie_map and not wl_show_map:
            log.info("watchlist_sync.mdblist_to_arr.empty_watchlist", user_id=user.id)
            return

        await self._add_to_arr(
            user, wl_movie_map, wl_show_map,
            log_prefix="watchlist_sync.mdblist_to_arr",
        )

    # ------------------------------------------------------------------
    # Shared: push watchlist items into Radarr/Sonarr
    # ------------------------------------------------------------------

    async def _add_to_arr(
        self,
        user: User,
        movie_map: dict[int, dict],
        show_map: dict[int, dict],
        log_prefix: str,
    ):
        """Add movies (keyed by TMDB) and shows (keyed by TVDB) to Radarr/Sonarr."""
        from app.utils.radarr_client import RadarrClient
        from app.utils.sonarr_client import SonarrClient

        r = await get_redis()

        # Get all items currently in Radarr/Sonarr
        radarr_tmdb_ids, sonarr_tvdb_ids = await self._get_arr_all_ids()

        # --- Movies → Radarr ---
        movies_to_add = {
            tmdb: data for tmdb, data in movie_map.items()
            if tmdb not in radarr_tmdb_ids
        }

        movies_added = 0
        movies_failed = 0
        if movies_to_add:
            raw = await secure_get("radarr_servers")
            if raw:
                servers = _json.loads(raw)
                if servers:
                    srv = servers[0]
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
                                    log.warning(f"{log_prefix}.radarr_add_failed",
                                                tmdb=tmdb, reason=result.get("reason", "")[:120])
                                    movies_failed += 1
                                else:
                                    movies_added += 1
                                    log.info(f"{log_prefix}.radarr_added",
                                             tmdb=tmdb, title=movie.get("title", ""))
                            except Exception as e:
                                err = str(e)[:120]
                                if "already" in err.lower() or "exist" in err.lower():
                                    log.debug(f"{log_prefix}.radarr_exists", tmdb=tmdb)
                                else:
                                    log.warning(f"{log_prefix}.radarr_add_error",
                                                tmdb=tmdb, error=err)
                                    movies_failed += 1
                    except Exception:
                        log.exception(f"{log_prefix}.radarr_connect_failed",
                                      server=srv.get("name"))
                    finally:
                        if client:
                            await client.close()

        # --- Shows → Sonarr ---
        shows_to_add = {
            tvdb: data for tvdb, data in show_map.items()
            if tvdb not in sonarr_tvdb_ids
        }

        shows_added = 0
        shows_failed = 0
        if shows_to_add:
            raw = await secure_get("sonarr_servers")
            if raw:
                servers = _json.loads(raw)
                if servers:
                    srv = servers[0]
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
                                    log.warning(f"{log_prefix}.sonarr_add_failed",
                                                tvdb=tvdb, reason=result.get("reason", "")[:120])
                                    shows_failed += 1
                                else:
                                    shows_added += 1
                                    log.info(f"{log_prefix}.sonarr_added",
                                             tvdb=tvdb, title=show.get("title", ""))
                            except Exception as e:
                                err = str(e)[:120]
                                if "already" in err.lower() or "exist" in err.lower():
                                    log.debug(f"{log_prefix}.sonarr_exists", tvdb=tvdb)
                                else:
                                    log.warning(f"{log_prefix}.sonarr_add_error",
                                                tvdb=tvdb, error=err)
                                    shows_failed += 1
                    except Exception:
                        log.exception(f"{log_prefix}.sonarr_connect_failed",
                                      server=srv.get("name"))
                    finally:
                        if client:
                            await client.close()

        log.info(f"{log_prefix}.done",
                 user_id=user.id,
                 movies_added=movies_added,
                 movies_failed=movies_failed,
                 movies_skipped=len(movie_map) - len(movies_to_add),
                 shows_added=shows_added,
                 shows_failed=shows_failed,
                 shows_skipped=len(show_map) - len(shows_to_add))

        # Invalidate Coming Soon cache so new items appear immediately
        if movies_added or shows_added:
            try:
                r2 = await get_redis()
                await r2.delete("availability_monitor_v2")
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
    async def _get_active_providers() -> set[str]:
        """Return set of active integration providers, e.g. {'simkl', 'mdblist'}."""
        r = await get_redis()
        raw = await r.get("integration_provider")
        provider = (raw if isinstance(raw, str) else raw.decode()) if raw else "simkl"
        if provider == "both":
            return {"simkl", "mdblist"}
        if provider in ("simkl", "mdblist"):
            return {provider}
        return set()  # "none"

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
        raw = await secure_get("radarr_servers")
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
        raw = await secure_get("sonarr_servers")
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

        raw = await secure_get("radarr_servers")
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

        raw = await secure_get("sonarr_servers")
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
    async def _resolve_tvdb_to_tmdb(tvdb_ids: list[int]) -> dict[int, int]:
        """Best-effort TVDB→TMDB resolution via Sonarr's existing data.

        Queries all Sonarr servers for series that have both tvdbId and tmdbId.
        Returns {tvdb_id: tmdb_id} for any matches found.
        """
        from app.utils.sonarr_client import SonarrClient

        r = await get_redis()
        mapping: dict[int, int] = {}
        tvdb_set = set(tvdb_ids)

        raw = await secure_get("sonarr_servers")
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
                        tmdb = s.get("tmdbId")
                        if tvdb and tmdb and tvdb in tvdb_set:
                            mapping[tvdb] = tmdb
                except Exception:
                    log.debug("watchlist_sync.tvdb_tmdb_resolve_failed",
                              server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        log.debug("watchlist_sync.tvdb_to_tmdb_resolved",
                  requested=len(tvdb_ids), resolved=len(mapping))
        return mapping

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

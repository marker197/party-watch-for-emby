"""Airing Soon — Season Finale / Premiere Alerts (shows) + Release Alerts (movies).

Primary data source is Sonarr (shows) and Radarr (movies) calendars — these
always run regardless of integration provider. When Simkl is active in
settings and the user has a linked account, Simkl's personal calendar is
layered on top for additional coverage (shows the user follows on Simkl but
hasn't added to Sonarr, watchlisted movies not in Radarr, and season finale
detection).

Release dates are sourced with a priority cascade:
  1. Radarr / Sonarr (primary — most accurate for items in arr)
  2. TMDB /movie/{id}/release_dates (second — broad coverage, typed releases)
  3. Simkl /releases/{country} (last resort, only if Simkl is active)
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta

import structlog

from app.models.schema import User
from app.utils.simkl_client import SimklClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.utils.tmdb_client import get_movie_release_dates as _tmdb_release_dates
from app.utils.database import async_session
from sqlalchemy import select

log = structlog.get_logger()

SEASON_INFO_CACHE_TTL = 86400  # 24h — season episode counts rarely change mid-run


class AiringAlertsService:
    async def get_airing_soon(self, user: User, days: int = 14) -> dict:
        """Return upcoming episodes + watchlisted movie releases for `user`,
        sorted by days_until_air, plus upcoming digital/physical releases for
        movies missing in Radarr.

        Sonarr/Radarr calendars are the primary data source. Simkl calendar
        is layered on top only when the integration provider includes Simkl.

        Returns:
            {
                "items": [...],                   # main airing feed
                "upcoming_home_releases": [...],  # Radarr missing movies with
                                                  # digital/physical dates in window
            }
        """
        # Determine whether Simkl is active
        from app.api.route_helpers import _get_active_providers
        providers = await _get_active_providers()
        use_simkl = "simkl" in providers and bool(user.simkl_access_token)

        simkl: SimklClient | None = None
        if use_simkl:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # ── Check Redis response cache first (15 min TTL) ──
            response_cache_key = f"airing_alerts:response:{user.id}:{days}"
            try:
                r = await get_redis()
                cached_response = await r.get(response_cache_key)
                if cached_response:
                    log.info("airing_alerts.cache_hit", user_id=user.id)
                    return json.loads(cached_response)
            except Exception:
                pass

            # Determine server country for release-date lookups
            server_country = await self._get_server_country()

            # Build arr release date index from live calendar data (always primary)
            arr_dates = await self._build_arr_release_index(today, days)

            # ── Fetch show + movie alerts in parallel ──
            show_task = asyncio.create_task(self._get_show_alerts(
                simkl, today, days, user=user, arr_dates=arr_dates,
            ))
            movie_task = asyncio.create_task(self._get_movie_alerts(
                simkl, today, days, country=server_country, arr_dates=arr_dates,
            ))
            show_results, movie_results = await asyncio.gather(
                show_task, movie_task, return_exceptions=True,
            )
            results = []
            if isinstance(show_results, list):
                results.extend(show_results)
            else:
                log.warning("airing_alerts.show_alerts_error",
                            error=str(show_results)[:200])
            if isinstance(movie_results, list):
                results.extend(movie_results)
            else:
                log.warning("airing_alerts.movie_alerts_error",
                            error=str(movie_results)[:200])

            results.sort(key=lambda r: (r["days_until_air"] if r["days_until_air"] is not None else 9999))

            # Upcoming digital/physical releases for missing Radarr movies
            home_releases = await self._get_upcoming_home_releases(today, days)

            # Enrich with streaming provider logos (TMDB)
            await self._enrich_with_providers(results, server_country)
            await self._enrich_with_providers(home_releases, server_country)

            response = {
                "items": results,
                "upcoming_home_releases": home_releases,
            }

            # ── Cache response for 15 min ──
            try:
                await r.setex(response_cache_key, 900, json.dumps(response))
            except Exception:
                pass

            return response
        finally:
            if simkl:
                await simkl.close()

    # ------------------------------------------------------------------
    # Streaming provider enrichment (TMDB)
    # ------------------------------------------------------------------

    @staticmethod
    async def _enrich_with_providers(items: list[dict], country: str) -> None:
        """Add ``streaming_services`` field to items that have a ``tmdb_id``."""
        from app.utils.tmdb_client import get_watch_providers, _get_api_key

        api_key = await _get_api_key()
        if not api_key:
            log.debug("enrich_providers.skipped_no_api_key")
            return

        enriched = 0
        for item in items:
            tmdb_id = item.get("tmdb_id")
            if not tmdb_id:
                continue
            media_type = item.get("media_type", "movie")
            providers = await get_watch_providers(
                tmdb_id,
                media_type="tv" if media_type == "show" else "movie",
                country=country,
            )
            item["streaming_services"] = providers
            if providers:
                enriched += 1
        log.info("enrich_providers.done",
                 total_items=len(items), with_tmdb_id=sum(1 for i in items if i.get("tmdb_id")),
                 enriched=enriched, country=country)

    # ------------------------------------------------------------------
    # Arr release date index
    # ------------------------------------------------------------------

    @staticmethod
    async def _build_arr_release_index(today: str, days: int) -> dict:
        """Build a live index of release dates from Radarr and Sonarr calendars.

        Both arr services have calendar endpoints that return only items with
        releases in the requested date range — no need to pull the full library
        or cache the result.

        Returns:
            {
                "movies": {tmdb_id: {"theatrical": str|None, "digital": str|None, "physical": str|None}},
                "sonarr_calendar": {tvdb_id: [{season, episode, air_date_utc, episode_title}, ...]},
            }
        """
        import json as _json

        r = await get_redis()
        end_date = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")

        result: dict = {"movies": {}, "sonarr_calendar": {}}

        # --- Radarr calendar (upcoming movie releases) ---
        raw = await secure_get("radarr_servers")
        if raw:
            from app.utils.radarr_client import RadarrClient
            for srv in _json.loads(raw):
                client = None
                try:
                    client = RadarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Radarr"),
                    )
                    movies = await client.get_calendar(today, end_date)
                    for m in movies:
                        tmdb = m.get("tmdb_id")
                        if not tmdb:
                            continue
                        theatrical = _normalise_date(m.get("in_cinemas"))
                        digital = _normalise_date(m.get("digital_release"))
                        physical = _normalise_date(m.get("physical_release"))
                        if theatrical or digital or physical:
                            result["movies"][str(tmdb)] = {
                                "theatrical": theatrical,
                                "digital": digital,
                                "physical": physical,
                                "title": m.get("title", ""),
                                "has_file": m.get("has_file", False),
                            }
                    log.debug("arr_release_index.radarr_calendar_loaded",
                              server=srv.get("name"),
                              movies_with_dates=len(result["movies"]))
                except Exception:
                    log.warning("arr_release_index.radarr_calendar_failed",
                                server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        # --- Sonarr calendar (upcoming episodes) ---
        raw = await secure_get("sonarr_servers")
        if raw:
            from app.utils.sonarr_client import SonarrClient
            for srv in _json.loads(raw):
                client = None
                try:
                    client = SonarrClient(
                        srv["url"], srv["api_key"],
                        name=srv.get("name", "Sonarr"),
                    )
                    episodes = await client.get_calendar(today, end_date)
                    for ep in episodes:
                        tvdb = ep.get("tvdb_id")
                        if not tvdb:
                            continue
                        key = str(tvdb)
                        if key not in result["sonarr_calendar"]:
                            result["sonarr_calendar"][key] = []
                        result["sonarr_calendar"][key].append({
                            "season": ep.get("season"),
                            "episode": ep.get("episode"),
                            "air_date_utc": ep.get("air_date_utc"),
                            "episode_title": ep.get("episode_title", ""),
                            "series_title": ep.get("series_title", ""),
                            "season_episode_count": ep.get("season_episode_count"),
                            "finale_type": ep.get("finale_type"),
                        })
                    log.debug("arr_release_index.sonarr_calendar_loaded",
                              server=srv.get("name"),
                              episodes=sum(len(v) for v in result["sonarr_calendar"].values()))
                except Exception:
                    log.warning("arr_release_index.sonarr_calendar_failed",
                                server=srv.get("name"))
                finally:
                    if client:
                        await client.close()

        return result

    # ------------------------------------------------------------------
    # Upcoming home releases (digital/physical) for missing Radarr movies
    # ------------------------------------------------------------------

    async def _get_upcoming_home_releases(self, today: str, days: int) -> list[dict]:
        """Return Radarr movies that are monitored but missing (no file) and
        have a digital or physical release date within the next `days` days.

        These are movies the user has added to Radarr but can't download yet
        because they haven't been released digitally/physically — surfacing
        them lets the user know when they'll become available.
        """
        import json as _json

        r = await get_redis()
        raw = await secure_get("radarr_servers")
        if not raw:
            return []

        from app.utils.radarr_client import RadarrClient

        today_date = datetime.now(timezone.utc).date()
        end_date = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")
        results = []
        seen_tmdb: set[str] = set()

        for srv in _json.loads(raw):
            client = None
            try:
                client = RadarrClient(
                    srv["url"], srv["api_key"],
                    name=srv.get("name", "Radarr"),
                )
                movies = await client.get_calendar(today, end_date)
                for m in movies:
                    tmdb = m.get("tmdb_id")
                    if not tmdb or str(tmdb) in seen_tmdb:
                        continue
                    # Only missing movies (in Radarr but no file)
                    if m.get("has_file"):
                        continue

                    digital = _normalise_date(m.get("digital_release"))
                    physical = _normalise_date(m.get("physical_release"))

                    # Must have at least one home release date in the window
                    digital_in_window = _date_in_window(digital, today_date, days)
                    physical_in_window = _date_in_window(physical, today_date, days)
                    if not digital_in_window and not physical_in_window:
                        continue

                    seen_tmdb.add(str(tmdb))

                    # Pick the nearest home release for sorting
                    nearest_date = digital if digital_in_window else physical
                    if digital_in_window and physical_in_window:
                        nearest_date = min(digital, physical)

                    results.append({
                        "title": m.get("title", ""),
                        "tmdb_id": tmdb,
                        "digital_release": digital,
                        "physical_release": physical,
                        "theatrical_release": _normalise_date(m.get("in_cinemas")),
                        "days_until": self._days_until(nearest_date),
                        "server": srv.get("name", "Radarr"),
                    })
            except Exception:
                log.warning("upcoming_home_releases.radarr_failed",
                            server=srv.get("name"))
            finally:
                if client:
                    await client.close()

        results.sort(key=lambda r: r["days_until"] if r["days_until"] is not None else 9999)
        return results

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    async def _get_show_alerts(self, simkl: SimklClient | None, today: str, days: int,
                              user: User | None = None,
                              arr_dates: dict | None = None) -> list[dict]:
        # ── Simkl calendar (optional — only if enabled) ──
        merged: dict[tuple, dict] = {}
        if simkl:
            # Fetch shows, watchlist, and premieres in parallel
            async def _fetch_upcoming():
                try:
                    return await simkl.get_my_shows(start_date=today, days=days)
                except Exception as e:
                    log.warning("airing_alerts.my_shows_failed", error=str(e)[:200])
                    return []

            async def _fetch_watchlist():
                try:
                    return await simkl.get_watchlist(kind="shows")
                except Exception:
                    return []

            async def _fetch_mdblist_wl():
                try:
                    from app.utils.secure_redis import secure_get
                    mdb_key = await secure_get("mdblist_api_key")
                    if not mdb_key:
                        return []
                    from app.utils.mdblist_client import MDBListClient
                    mdb = MDBListClient(api_key=mdb_key)
                    try:
                        mdb_wl = await mdb.get_watchlist(mediatype="show")
                        return mdb_wl.get("shows", []) if isinstance(mdb_wl, dict) else []
                    finally:
                        await mdb.close()
                except Exception:
                    return []

            upcoming, wl, mdb_wl_shows = await asyncio.gather(
                _fetch_upcoming(), _fetch_watchlist(), _fetch_mdblist_wl(),
            )

            # Build a set of show IDs the user follows (Simkl watchlist + Sonarr)
            # so we can filter global premieres to only relevant shows
            followed_ids: set[str] = set()
            for entry in wl:
                inner = entry.get("show") or entry
                ids = inner.get("ids", {})
                for k in ("simkl", "simkl_id", "imdb", "tmdb", "tvdb"):
                    v = ids.get(k)
                    if v:
                        followed_ids.add(f"{k}:{v}")
            # Also add IDs from get_my_shows (user's "watching" shows)
            for entry in upcoming:
                inner = entry.get("show") or entry
                ids = inner.get("ids", {})
                for k in ("simkl", "simkl_id", "imdb", "tmdb", "tvdb"):
                    v = ids.get(k)
                    if v:
                        followed_ids.add(f"{k}:{v}")

            # Add Sonarr TVDB IDs
            sonarr_cal = (arr_dates or {}).get("sonarr_calendar", {})
            for tvdb_str in sonarr_cal:
                followed_ids.add(f"tvdb:{tvdb_str}")

            # Add MDBList watchlist IDs (already fetched in parallel above)
            for entry in mdb_wl_shows:
                inner = entry.get("show") or entry
                ids = inner.get("ids", {})
                for k in ("imdb", "tmdb", "tvdb"):
                    v = ids.get(k)
                    if v:
                        followed_ids.add(f"{k}:{v}")

            log.debug("airing_alerts.followed_ids", count=len(followed_ids))

            try:
                raw_premieres = await simkl.get_my_premieres(start_date=today, days=days)
                # Filter: only keep premieres for shows the user follows
                premieres = []
                for entry in raw_premieres:
                    show = entry.get("show", {})
                    show_ids = show.get("ids", {})
                    matched = False
                    for k in ("simkl", "simkl_id", "imdb", "tmdb", "tvdb"):
                        v = show_ids.get(k)
                        if v and f"{k}:{v}" in followed_ids:
                            matched = True
                            break
                    if matched:
                        premieres.append(entry)
                log.debug("airing_alerts.premieres_filtered",
                         raw=len(raw_premieres), kept=len(premieres))
            except Exception as e:
                log.warning("airing_alerts.premieres_failed", error=str(e)[:200])
                premieres = []

            # Key by (show simkl id, season, episode) to dedupe between the
            # two calendar calls — premieres also show up in get_my_shows.
            for entry in upcoming:
                self._merge_entry(merged, entry, is_premiere_source=False)
            for entry in premieres:
                self._merge_entry(merged, entry, is_premiere_source=True)

        sonarr_cal = (arr_dates or {}).get("sonarr_calendar", {})

        results = []
        # Track shows with finales for binge planner
        finale_shows: dict[str, dict] = {}  # simkl_id → {days_until, season, emby_item_id}

        # Track which (tvdb, season, episode) combos Simkl already covered
        simkl_covered: set[tuple] = set()

        for key, entry in merged.items():
            show = entry["show"]
            episode = entry["episode"]
            show_simkl_id = str(show.get("ids", {}).get("simkl") or show.get("ids", {}).get("simkl_id") or "")
            show_tvdb_id = show.get("ids", {}).get("tvdb")

            days_until = self._days_until(entry.get("first_aired"))
            air_date = entry.get("first_aired")

            # Cross-reference with Sonarr calendar for more accurate air date
            release_source = "simkl"
            if show_tvdb_id and str(show_tvdb_id) in sonarr_cal:
                sonarr_ep = _find_sonarr_episode(
                    sonarr_cal[str(show_tvdb_id)],
                    episode.get("season"),
                    episode.get("number"),
                )
                if sonarr_ep and sonarr_ep.get("air_date_utc"):
                    sonarr_air = sonarr_ep["air_date_utc"]
                    sonarr_days = self._days_until(sonarr_air)
                    # Prefer Sonarr date if it differs (usually more accurate)
                    if sonarr_days is not None:
                        air_date = sonarr_air
                        days_until = sonarr_days
                        release_source = "sonarr"

            is_premiere = entry["is_premiere"] or episode.get("number") == 1
            is_finale = False
            if not is_premiere and episode.get("number"):
                # Try Sonarr finale detection first (works without Simkl)
                sonarr_finale = False
                if show_tvdb_id and str(show_tvdb_id) in sonarr_cal:
                    sonarr_ep = _find_sonarr_episode(
                        sonarr_cal[str(show_tvdb_id)],
                        episode.get("season"),
                        episode.get("number"),
                    )
                    if sonarr_ep:
                        # Primary: Sonarr's finaleType field ("season" or "series")
                        if sonarr_ep.get("finale_type"):
                            sonarr_finale = True
                        else:
                            # Fallback: episode number matches season total
                            sec = sonarr_ep.get("season_episode_count")
                            if sec and episode.get("number") == sec:
                                sonarr_finale = True
                if sonarr_finale:
                    is_finale = True
                elif show_simkl_id and simkl:
                    is_finale = await self._is_season_finale(simkl, show_simkl_id, episode)

            in_library, emby_item_id = await self._match_in_library(show)
            # Shows already in the library always surface. For shows NOT
            # in the library, only surface premieres (new seasons / series
            # premieres) — these are shows the user follows on Simkl but
            # doesn't have yet, and they want to know about the premiere
            # so they can grab them (same rationale as movie alerts).
            # Regular mid-season episodes for non-library shows are
            # skipped — Smart Queue's calendar source handles discovery.
            if not in_library and not is_premiere:
                # Don't mark as simkl_covered so the Sonarr-only fallback
                # can still pick it up (Sonarr may have better ID matching)
                continue

            # Mark as covered ONLY when we're actually including the episode
            if show_tvdb_id:
                simkl_covered.add((str(show_tvdb_id), episode.get("season"), episode.get("number")))

            result = {
                "media_type": "show",
                "title": show.get("title", ""),
                "simkl_id": show_simkl_id,
                "tvdb_id": show_tvdb_id,
                "tmdb_id": show.get("ids", {}).get("tmdb"),
                "imdb_id": show.get("ids", {}).get("imdb"),
                "season": episode.get("season"),
                "episode": episode.get("number"),
                "episode_title": episode.get("title"),
                "air_date": air_date,
                "days_until_air": days_until,
                "is_premiere": is_premiere,
                "is_finale": is_finale,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
                "year": show.get("year"),
                "binge_plan": None,
                "release_source": release_source,
            }
            results.append(result)

            if is_finale and days_until is not None and emby_item_id:
                finale_shows[show_simkl_id] = {
                    "days_until": days_until,
                    "season": episode.get("season"),
                    "episode": episode.get("number"),
                    "emby_item_id": emby_item_id,
                    "title": show.get("title", ""),
                }

        # ── Sonarr-only episodes: add episodes from Sonarr calendar ──
        # that Simkl didn't return (e.g. shows in Sonarr but not followed
        # on Simkl, or episodes Simkl's calendar missed).
        for tvdb_id_str, episodes in sonarr_cal.items():
            for ep in episodes:
                ep_key = (tvdb_id_str, ep.get("season"), ep.get("episode"))
                if ep_key in simkl_covered:
                    continue  # Already handled via Simkl

                air_date = ep.get("air_date_utc")
                days_until = self._days_until(air_date)
                if days_until is None:
                    continue

                series_title = ep.get("series_title", "")

                # Try to find in library by TVDB ID
                match = await LibraryCache.find_by_provider_id("Tvdb", tvdb_id_str)
                in_library = match is not None
                emby_item_id = match.get("emby_id") if match else None

                # Fallback: use library cache title if Sonarr didn't provide one
                if not series_title and match:
                    series_title = match.get("title", "")

                is_premiere = ep.get("season") == 1 and ep.get("episode") == 1
                if ep.get("episode") == 1:
                    is_premiere = True

                # Finale detection: Sonarr finaleType first, episode count fallback
                is_finale = False
                if not is_premiere and ep.get("episode"):
                    if ep.get("finale_type"):
                        is_finale = True
                    else:
                        sec = ep.get("season_episode_count")
                        if sec and ep.get("episode") == sec:
                            is_finale = True

                results.append({
                    "media_type": "show",
                    "title": series_title,
                    "simkl_id": None,
                    "tvdb_id": int(tvdb_id_str) if tvdb_id_str.isdigit() else None,
                    "tmdb_id": None,
                    "imdb_id": None,
                    "season": ep.get("season"),
                    "episode": ep.get("episode"),
                    "episode_title": ep.get("episode_title", ""),
                    "air_date": air_date,
                    "days_until_air": days_until,
                    "is_premiere": is_premiere,
                    "is_finale": is_finale,
                    "in_library": in_library,
                    "emby_item_id": emby_item_id,
                    "year": None,
                    "binge_plan": None,
                    "release_source": "sonarr",
                })

                # Track Sonarr-only finales for binge planner (key by tvdb: prefix)
                if is_finale and days_until is not None and emby_item_id:
                    finale_key = f"tvdb:{tvdb_id_str}"
                    finale_shows[finale_key] = {
                        "days_until": days_until,
                        "season": ep.get("season"),
                        "episode": ep.get("episode"),
                        "emby_item_id": emby_item_id,
                        "title": series_title,
                    }

        # ── Binge planner: compute catch-up info for shows with finales ──
        if finale_shows and user and user.emby_user_id:
            binge_plans = await self._compute_binge_plans(
                finale_shows, merged, user.emby_user_id,
                sonarr_cal=sonarr_cal,
            )
            for r in results:
                # Match by simkl_id (Simkl path) or tvdb: key (Sonarr-only path)
                tid = r.get("simkl_id", "")
                if tid and tid in binge_plans:
                    r["binge_plan"] = binge_plans[tid]
                elif r.get("tvdb_id"):
                    tvdb_key = f"tvdb:{r['tvdb_id']}"
                    if tvdb_key in binge_plans:
                        r["binge_plan"] = binge_plans[tvdb_key]

        # ── Resolve TBA episode titles from Simkl ──
        await self._resolve_tba_titles(results, simkl, sonarr_cal=sonarr_cal)

        return results

    @staticmethod
    def _merge_entry(merged: dict, entry: dict, is_premiere_source: bool) -> None:
        show = entry.get("show", {})
        episode = entry.get("episode", {})
        show_simkl_id = str(show.get("ids", {}).get("simkl") or show.get("ids", {}).get("simkl_id") or "")
        key = (show_simkl_id, episode.get("season"), episode.get("number"))
        if key not in merged:
            merged[key] = {"show": show, "episode": episode, "first_aired": entry.get("first_aired"),
                           "is_premiere": is_premiere_source}
        elif is_premiere_source:
            merged[key]["is_premiere"] = True

    async def _is_season_finale(self, simkl: SimklClient, show_simkl_id: str, episode: dict) -> bool:
        """An episode is a season finale if its number equals that season's
        total episode_count. Result is cached per-show for 24h since season
        episode counts don't change once a season airs."""
        season_num = episode.get("season")
        ep_num = episode.get("number")
        if season_num is None or ep_num is None:
            return False

        cache_key = f"airing_alerts:seasons:{show_simkl_id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                seasons = json.loads(cached)
            else:
                seasons = await simkl.get_show_seasons(show_simkl_id)
                await r.setex(cache_key, SEASON_INFO_CACHE_TTL, json.dumps(seasons))
        except Exception:
            log.warning("airing_alerts.season_lookup_failed", show_simkl_id=show_simkl_id)
            return False

        for season in seasons or []:
            if season.get("number") == season_num:
                episode_count = season.get("episode_count")
                return bool(episode_count) and ep_num == episode_count

        return False

    # ------------------------------------------------------------------
    # TBA episode title resolution
    # ------------------------------------------------------------------

    async def _resolve_tba_titles(
        self, results: list[dict], simkl: SimklClient | None,
        sonarr_cal: dict | None = None,
    ) -> None:
        """Resolve TBA/placeholder episode titles using MDBList and Simkl.

        Mutates ``results`` in place — sets ``episode_title`` when a
        better title is found.

        Resolution order:
          1. MDBList calendar (``/calendar/events``) — user-specific, cached 6h
          2. Simkl per-show episode list (``get_tv_episodes``) — cached 24h

        Note: Simkl CDN calendar (``/calendar/tv.json``) was evaluated but
        its episode dict only contains season/episode/url — no title field.
        """
        tba_items = [
            r for r in results
            if r.get("media_type") == "show" and _is_tba_title(r.get("episode_title"))
        ]
        if not tba_items:
            return

        log.info("airing_tba_resolve.start", tba_count=len(tba_items))

        # ── Pre-step: Enrich Sonarr-only items missing tmdb_id ──
        # Sonarr-only items have tvdb_id but no tmdb_id/simkl_id — resolve
        # via library cache so MDBList/Simkl lookups can match them.
        enriched = 0
        for item in tba_items:
            if not item.get("tmdb_id") and item.get("tvdb_id"):
                match = await LibraryCache.find_by_provider_id(
                    "Tvdb", str(item["tvdb_id"]),
                )
                if match:
                    pids = match.get("provider_ids", {})
                    for tmdb_key in ("Tmdb", "tmdb", "TMDB"):
                        if pids.get(tmdb_key):
                            item["tmdb_id"] = int(pids[tmdb_key])
                            enriched += 1
                            break
                    for imdb_key in ("Imdb", "imdb", "IMDB"):
                        if not item.get("imdb_id") and pids.get(imdb_key):
                            item["imdb_id"] = pids[imdb_key]
                            break
        if enriched:
            log.debug("airing_tba_resolve.tvdb_enriched", count=enriched)

        # ── Step 1: MDBList calendar (user-specific) ──
        mdb_index = await self._get_mdblist_calendar_index()
        resolved_mdb = 0
        still_tba: list[dict] = []

        for item in tba_items:
            title = self._lookup_mdblist_episode_title(mdb_index, item)
            if title and not _is_tba_title(title):
                item["episode_title"] = title
                resolved_mdb += 1
            else:
                still_tba.append(item)

        # ── Step 2: Simkl per-show episode list (individual lookups) ──
        resolved_eps = 0
        if still_tba and simkl:
            # Group by simkl_id to avoid duplicate fetches.
            # For Sonarr-only items missing simkl_id, resolve via Simkl
            # search using TMDB or IMDB ID.
            by_show: dict[str, list[dict]] = {}
            for item in still_tba:
                sid = item.get("simkl_id")
                if not sid and (item.get("tmdb_id") or item.get("imdb_id")):
                    try:
                        if item.get("imdb_id"):
                            found_list = await simkl.search_by_id("imdb", item["imdb_id"])
                        elif item.get("tmdb_id"):
                            found_list = await simkl.search_by_id("tmdb", str(item["tmdb_id"]))
                        else:
                            found_list = []
                        for found in (found_list if isinstance(found_list, list) else []):
                            if found.get("type") in ("tv", "show", "anime"):
                                sid = str(
                                    found.get("ids", {}).get("simkl")
                                    or found.get("ids", {}).get("simkl_id")
                                    or ""
                                )
                                if sid:
                                    item["simkl_id"] = sid
                                    break
                    except Exception:
                        pass
                if sid:
                    by_show.setdefault(sid, []).append(item)

            for simkl_id, items in by_show.items():
                episodes = await self._get_cached_tv_episodes(simkl, simkl_id)
                for item in items:
                    ep_title = self._find_episode_title(
                        episodes, item.get("season"), item.get("episode"),
                    )
                    if ep_title and not _is_tba_title(ep_title):
                        item["episode_title"] = ep_title
                        resolved_eps += 1

        log.info("airing_tba_resolve.done",
                 mdb_resolved=resolved_mdb, eps_resolved=resolved_eps,
                 remaining_tba=len(tba_items) - resolved_mdb - resolved_eps)

    async def _get_mdblist_calendar_index(self) -> dict[tuple, str]:
        """Fetch MDBList calendar events and build a lookup index for episode titles.

        Returns {(show_tmdb_id, season, episode): episode_title}
        Cached in Redis for 6 hours.
        """
        cache_key = "airing_alerts:mdblist_calendar_index_v2"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                rows = json.loads(cached)
                return {(str(row[0]), row[1], row[2]): row[3] for row in rows}
        except Exception:
            pass

        index: dict[tuple, str] = {}

        try:
            mdb_key = await secure_get("mdblist_api_key")
            if not mdb_key:
                return index

            from app.utils.mdblist_client import MDBListClient
            mdb = MDBListClient(api_key=mdb_key)
            try:
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                end = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
                events = await mdb.get_calendar_events(
                    start=today, end=end, limit=1000, favorite_cast=False,
                )
            finally:
                await mdb.close()

            for ev in events:
                if ev.get("type") != "episode":
                    continue
                show_tmdb = ev.get("show_tmdb")
                season = ev.get("season_number")
                episode_num = ev.get("episode_number")
                ep_title = ev.get("episode_title", "")

                if season is None or episode_num is None or not ep_title:
                    continue

                # Index by TMDB ID (primary)
                if show_tmdb:
                    index[(str(show_tmdb), season, episode_num)] = ep_title
                # Also index by show title (fallback for Sonarr-only items)
                show_title = ev.get("title", "").strip().lower()
                if show_title:
                    index[(show_title, season, episode_num)] = ep_title

        except Exception:
            log.debug("airing_tba_resolve.mdblist_calendar_failed")

        # Cache for 6 hours
        try:
            serialisable = [[k[0], k[1], k[2], v] for k, v in index.items()]
            await r.setex(cache_key, 21600, json.dumps(serialisable))
        except Exception:
            pass

        log.debug("airing_tba_resolve.mdblist_calendar_loaded", entries=len(index),
                  sample_keys=list(index.keys())[:10])
        return index

    @staticmethod
    def _lookup_mdblist_episode_title(
        index: dict[tuple, str], item: dict,
    ) -> str | None:
        """Try to find an episode title in the MDBList calendar index.

        Tries TMDB ID first, then falls back to show title matching.
        """
        season = item.get("season")
        episode = item.get("episode")
        if season is None or episode is None:
            return None

        # Try by TMDB ID (primary)
        tmdb_id = item.get("tmdb_id")
        if tmdb_id:
            title = index.get((str(tmdb_id), season, episode))
            if title:
                return title

        # Fallback: try by show title (for Sonarr-only items)
        show_title = (item.get("title") or "").strip().lower()
        if show_title:
            title = index.get((show_title, season, episode))
            if title:
                return title

        return None

    async def _get_cached_tv_episodes(
        self, simkl: SimklClient, simkl_id: str,
    ) -> list[dict]:
        """Fetch full episode list for a show, cached 24h."""
        cache_key = f"airing_alerts:tv_episodes:{simkl_id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

        try:
            episodes = await simkl.get_tv_episodes(simkl_id)
        except Exception:
            log.debug("airing_tba_resolve.episodes_fetch_failed",
                      simkl_id=simkl_id)
            return []

        episodes = episodes if isinstance(episodes, list) else []

        try:
            await r.setex(cache_key, SEASON_INFO_CACHE_TTL,
                          json.dumps(episodes))
        except Exception:
            pass

        return episodes

    @staticmethod
    def _find_episode_title(
        episodes: list[dict], season: int | None, episode_num: int | None,
    ) -> str | None:
        """Find an episode title from Simkl's full episode list."""
        if season is None or episode_num is None:
            return None
        for ep in episodes:
            if ep.get("season") == season and ep.get("episode") == episode_num:
                return ep.get("title")
        return None

    # ------------------------------------------------------------------
    # Binge planner
    # ------------------------------------------------------------------

    async def _compute_binge_plans(
        self,
        finale_shows: dict[str, dict],
        merged_entries: dict[tuple, dict],
        emby_user_id: str,
        sonarr_cal: dict | None = None,
    ) -> dict[str, dict]:
        """For each show with a finale in the window, compute how many
        unwatched episodes the user needs to get through and the daily
        pace required.

        Returns {show_key: binge_plan_dict} where show_key is either a
        simkl_id or ``tvdb:<id>`` for Sonarr-only shows.
        """
        plans: dict[str, dict] = {}

        # Batch-fetch Emby UserData for all finale shows
        emby_ids = [info["emby_item_id"] for info in finale_shows.values()
                    if info.get("emby_item_id")]
        emby_data: dict[str, dict] = {}
        if emby_ids:
            emby = EmbyClient()
            try:
                items = await emby.get_user_items_by_ids(emby_user_id, emby_ids)
                for item in items:
                    emby_data[str(item.get("Id", ""))] = item
            except Exception:
                log.warning("binge_planner.emby_fetch_failed")
                return plans
            finally:
                await emby.close()

        for show_key, info in finale_shows.items():
            eid = info["emby_item_id"]
            item = emby_data.get(eid)
            if not item:
                continue

            user_data = item.get("UserData", {})
            unwatched_in_library = user_data.get("UnplayedItemCount")
            if unwatched_in_library is None:
                continue

            # Count episodes airing up to and including the finale date,
            # excluding the finale episode itself (handles batch drops where
            # multiple episodes air on the same day as the finale).
            finale_season = info["season"]
            finale_episode = info.get("episode")
            episodes_airing_before = 0
            same_day_count = 0  # episodes airing on the exact same day as finale
            counted_eps: set[tuple] = set()  # (season, episode) to avoid double-counting

            # Source 1: Simkl merged entries
            for key, entry in merged_entries.items():
                entry_simkl_id = str(entry["show"].get("ids", {}).get("simkl") or entry["show"].get("ids", {}).get("simkl_id") or "")
                if entry_simkl_id != show_key:
                    continue
                ep = entry["episode"]
                ep_season = ep.get("season")
                ep_num = ep.get("number")
                if ep_season == finale_season:
                    # Skip the finale episode itself
                    if finale_episode is not None and ep_num == finale_episode:
                        continue
                    ep_days = self._days_until(entry.get("first_aired"))
                    if ep_days is not None and ep_days >= 0 and ep_days <= info["days_until"]:
                        counted_eps.add((ep_season, ep_num))
                        episodes_airing_before += 1
                        if ep_days == info["days_until"]:
                            same_day_count += 1

            # Source 2: Sonarr calendar (catches Sonarr-only shows and
            # any episodes Simkl missed for shows it did cover)
            if sonarr_cal:
                # For tvdb: keyed shows, extract the TVDB ID directly
                tvdb_id_str = None
                if show_key.startswith("tvdb:"):
                    tvdb_id_str = show_key[5:]
                else:
                    # Simkl-keyed show — find its TVDB ID from merged_entries
                    for key, entry in merged_entries.items():
                        entry_simkl_id = str(entry["show"].get("ids", {}).get("simkl") or entry["show"].get("ids", {}).get("simkl_id") or "")
                        if entry_simkl_id == show_key:
                            tvdb = entry["show"].get("ids", {}).get("tvdb")
                            if tvdb:
                                tvdb_id_str = str(tvdb)
                            break

                if tvdb_id_str and tvdb_id_str in sonarr_cal:
                    for sep in sonarr_cal[tvdb_id_str]:
                        ep_season = sep.get("season")
                        ep_num = sep.get("episode")
                        if (ep_season, ep_num) in counted_eps:
                            continue  # Already counted from Simkl
                        if ep_season == finale_season:
                            # Skip the finale episode itself
                            if finale_episode is not None and ep_num == finale_episode:
                                continue
                            ep_days = self._days_until(sep.get("air_date_utc"))
                            if ep_days is not None and ep_days >= 0 and ep_days <= info["days_until"]:
                                counted_eps.add((ep_season, ep_num))
                                episodes_airing_before += 1
                                if ep_days == info["days_until"]:
                                    same_day_count += 1

            # Batch drop: all non-finale episodes air on the same day as
            # the finale — the user can't watch anything until drop day.
            is_batch_drop = (episodes_airing_before > 0
                             and same_day_count == episodes_airing_before)

            if is_batch_drop:
                # Include the finale itself — you need to watch all of them
                total_to_watch = unwatched_in_library + episodes_airing_before + 1
                days_left = max(info["days_until"], 1)

                plans[show_key] = {
                    "status": "batch_drop",
                    "total_to_watch": total_to_watch,
                    "unwatched_available": unwatched_in_library,
                    "episodes_airing_before_finale": episodes_airing_before,
                    "days_until_finale": info["days_until"],
                    "episodes_per_day": total_to_watch,
                    "difficulty": "marathon" if total_to_watch > 4 else (
                        "ambitious" if total_to_watch > 2 else "moderate"
                    ),
                    "message": (
                        f"{total_to_watch} episode{'s' if total_to_watch != 1 else ''} "
                        f"drop in {days_left} day{'s' if days_left != 1 else ''}"
                    ),
                }
            elif (unwatched_in_library + episodes_airing_before) <= 0:
                plans[show_key] = {
                    "status": "caught_up",
                    "total_to_watch": 0,
                    "days_until_finale": info["days_until"],
                    "episodes_per_day": 0,
                    "message": "You're all caught up for the finale!",
                }
            else:
                total_to_watch = unwatched_in_library + episodes_airing_before
                days_left = max(info["days_until"], 1)
                pace = round(total_to_watch / days_left, 1)
                if pace <= 1:
                    difficulty = "easy"
                elif pace <= 2:
                    difficulty = "moderate"
                elif pace <= 4:
                    difficulty = "ambitious"
                else:
                    difficulty = "marathon"

                plans[show_key] = {
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

    async def _get_movie_alerts(self, simkl: SimklClient | None, today: str, days: int,
                               country: str = "us",
                               arr_dates: dict | None = None) -> list[dict]:
        radarr_movies = (arr_dates or {}).get("movies", {})

        # ── Simkl movie calendar (optional) ──
        simkl_releases = []
        if simkl:
            try:
                simkl_releases = await simkl.get_my_movies(start_date=today, days=days)
            except Exception as e:
                log.warning("airing_alerts.my_movies_failed", error=str(e)[:200])

        results = []
        seen_tmdb: set[str] = set()
        seen_simkl: set[str] = set()

        # ── Pass 1: Radarr calendar movies (primary) ──
        today_date = datetime.now(timezone.utc).date()
        for tmdb_id_str, dates in radarr_movies.items():
            # Pick the most relevant upcoming date
            best_date = None
            for date_key in ("digital", "physical", "theatrical"):
                d = dates.get(date_key)
                if d and _date_in_window(d, today_date, days):
                    if best_date is None or d < best_date:
                        best_date = d
            if not best_date:
                continue

            days_until = self._days_until(best_date)
            # Try to find in library by TMDB ID
            match = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id_str)
            in_library = match is not None
            emby_item_id = match.get("emby_id") if match else None
            title = match.get("title", "") if match else ""

            # Enrich title from Radarr calendar data if not in library cache
            if not title:
                title = dates.get("title", "")

            seen_tmdb.add(tmdb_id_str)

            results.append({
                "media_type": "movie",
                "title": title,
                "simkl_id": None,
                "tmdb_id": int(tmdb_id_str) if tmdb_id_str.isdigit() else None,
                "imdb_id": None,
                "season": None,
                "episode": None,
                "episode_title": None,
                "air_date": best_date,
                "days_until_air": days_until,
                "is_premiere": True,
                "is_finale": False,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
                "year": None,
                "theatrical_release": dates.get("theatrical"),
                "digital_release": dates.get("digital"),
                "physical_release": dates.get("physical"),
                "release_source": "radarr",
            })

        # ── Pass 2: Simkl movie calendar (supplements — adds titles, IDs, and
        #    movies not in Radarr) ──
        for entry in simkl_releases:
            movie = entry.get("movie", {})
            movie_simkl_id = str(movie.get("ids", {}).get("simkl") or movie.get("ids", {}).get("simkl_id") or "")
            movie_tmdb_id = movie.get("ids", {}).get("tmdb")

            if movie_simkl_id and movie_simkl_id in seen_simkl:
                continue
            seen_simkl.add(movie_simkl_id)

            # If already covered by Radarr pass, enrich instead of adding duplicate
            if movie_tmdb_id and str(movie_tmdb_id) in seen_tmdb:
                for r in results:
                    if r.get("tmdb_id") and str(r["tmdb_id"]) == str(movie_tmdb_id):
                        if not r["title"]:
                            r["title"] = movie.get("title", "")
                        if not r["simkl_id"]:
                            r["simkl_id"] = movie_simkl_id
                        if not r["imdb_id"]:
                            r["imdb_id"] = movie.get("ids", {}).get("imdb")
                        if not r["year"]:
                            r["year"] = movie.get("year")
                        break
                continue

            release_date = entry.get("released")
            days_until = self._days_until(release_date)

            # Record the TMDB ID so the MDBList pass doesn't re-add this movie
            if movie_tmdb_id:
                seen_tmdb.add(str(movie_tmdb_id))

            in_library, emby_item_id = await self._match_in_library(movie)

            # Fetch typed release dates (theatrical / digital / physical)
            theatrical, digital, physical, release_source = await self._get_release_dates(
                simkl, movie_simkl_id, movie_tmdb_id, country=country,
                arr_movies=radarr_movies,
                imdb_id=movie.get("ids", {}).get("imdb"),
            )

            results.append({
                "media_type": "movie",
                "title": movie.get("title", ""),
                "simkl_id": movie_simkl_id,
                "tmdb_id": movie_tmdb_id,
                "imdb_id": movie.get("ids", {}).get("imdb"),
                "season": None,
                "episode": None,
                "episode_title": None,
                "air_date": release_date,
                "days_until_air": days_until,
                "is_premiere": True,
                "is_finale": False,
                "in_library": in_library,
                "emby_item_id": emby_item_id,
                "year": movie.get("year"),
                "theatrical_release": theatrical,
                "digital_release": digital,
                "physical_release": physical,
                "release_source": release_source,
            })
        # ── Pass 3: MDBList calendar (enriches digital/physical dates for
        #    existing movies, adds movies only in MDBList calendar) ──
        try:
            mdb_key = await secure_get("mdblist_api_key")
            if mdb_key:
                from app.utils.mdblist_client import MDBListClient
                mdb_cal = MDBListClient(api_key=mdb_key)
                try:
                    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    end_str = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")
                    mdb_events = await mdb_cal.get_calendar_events(
                        start=today_str, end=end_str, limit=1000, favorite_cast=False,
                    )
                finally:
                    await mdb_cal.close()

                for ev in mdb_events:
                    if ev.get("type") != "movie":
                        continue
                    ev_tmdb = ev.get("tmdb")
                    ev_date = ev.get("start")
                    ev_release_type = ev.get("release_type", "")
                    if not ev_tmdb or not ev_date:
                        continue

                    ev_tmdb_str = str(ev_tmdb)

                    # Enrich existing results with MDBList calendar dates
                    if ev_tmdb_str in seen_tmdb:
                        for r in results:
                            if r.get("tmdb_id") and str(r["tmdb_id"]) == ev_tmdb_str:
                                if ev_release_type == "digital" and not r.get("digital_release"):
                                    r["digital_release"] = ev_date
                                elif ev_release_type == "physical" and not r.get("physical_release"):
                                    r["physical_release"] = ev_date
                                elif ev_release_type in ("release", "theatrical", "premiere") and not r.get("theatrical_release"):
                                    r["theatrical_release"] = ev_date
                                break
                        continue

                    # New movie not in Radarr or Simkl — add it
                    seen_tmdb.add(ev_tmdb_str)
                    in_lib = False
                    emby_id = None
                    match = await LibraryCache.find_by_provider_id("Tmdb", ev_tmdb_str)
                    if match:
                        in_lib = True
                        emby_id = match.get("emby_id")

                    results.append({
                        "media_type": "movie",
                        "title": ev.get("title", ""),
                        "simkl_id": None,
                        "tmdb_id": ev_tmdb,
                        "imdb_id": None,
                        "season": None,
                        "episode": None,
                        "episode_title": None,
                        "air_date": ev_date,
                        "days_until_air": self._days_until(ev_date),
                        "is_premiere": ev_release_type in ("release", "theatrical", "premiere"),
                        "is_finale": False,
                        "in_library": in_lib,
                        "emby_item_id": emby_id,
                        "year": None,
                        "theatrical_release": ev_date if ev_release_type in ("release", "theatrical", "premiere") else None,
                        "digital_release": ev_date if ev_release_type == "digital" else None,
                        "physical_release": ev_date if ev_release_type == "physical" else None,
                        "release_source": "mdblist_calendar",
                    })
        except Exception:
            log.debug("airing_alerts.mdblist_calendar_movies_failed")

        return results

    async def _get_release_dates(
        self, simkl: SimklClient | None, movie_simkl_id: str,
        tmdb_id: int | None = None,
        country: str = "us",
        arr_movies: dict | None = None,
        imdb_id: str | None = None,
    ) -> tuple[str | None, str | None, str | None, str]:
        """Return (theatrical, digital, physical, source) for a movie.

        Priority: Radarr → TMDB → MDBList → Simkl.
        Cross-verifies dates when multiple sources provide them.
        Cached 24h per movie+country.
        """
        if not movie_simkl_id and not tmdb_id:
            return None, None, None, ""

        cache_id = movie_simkl_id or str(tmdb_id)
        cache_key = f"airing_alerts:releases:{cache_id}:{country}_v3"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                return (data.get("theatrical"), data.get("digital"),
                        data.get("physical"), data.get("source", "cache"))
        except Exception:
            pass

        theatrical = None
        digital = None
        physical = None
        source = ""

        # Track per-source dates for cross-verification logging
        source_dates: dict[str, dict] = {}

        # ── Tier 1: Radarr ──
        if tmdb_id and arr_movies:
            radarr_data = arr_movies.get(str(tmdb_id))
            if radarr_data:
                theatrical = radarr_data.get("theatrical")
                digital = radarr_data.get("digital")
                physical = radarr_data.get("physical")
                if theatrical or digital or physical:
                    source = "radarr"
                    source_dates["radarr"] = {
                        "theatrical": theatrical,
                        "digital": digital,
                        "physical": physical,
                    }

        # ── Tier 2: TMDB (fills any gaps left by Radarr) ──
        if tmdb_id and (not theatrical or not digital or not physical):
            tmdb_theat, tmdb_dig, tmdb_phys = await _tmdb_release_dates(
                tmdb_id, country=country,
            )
            tmdb_any = tmdb_theat or tmdb_dig or tmdb_phys
            if tmdb_any:
                source_dates["tmdb"] = {
                    "theatrical": tmdb_theat,
                    "digital": tmdb_dig,
                    "physical": tmdb_phys,
                }
            if not theatrical and tmdb_theat:
                theatrical = tmdb_theat
                if not source:
                    source = "tmdb"
            if not digital and tmdb_dig:
                digital = tmdb_dig
                if not source:
                    source = "tmdb"
            if not physical and tmdb_phys:
                physical = tmdb_phys
                if not source:
                    source = "tmdb"

        # ── Tier 3: MDBList (fills gaps, provides cross-verification) ──
        if not theatrical or not digital or not physical:
            mdb_theat, mdb_dig, mdb_phys = await self._get_mdblist_releases(
                tmdb_id=tmdb_id, imdb_id=imdb_id,
            )
            mdb_any = mdb_theat or mdb_dig or mdb_phys
            if mdb_any:
                source_dates["mdblist"] = {
                    "theatrical": mdb_theat,
                    "digital": mdb_dig,
                    "physical": mdb_phys,
                }
            if not theatrical and mdb_theat:
                theatrical = mdb_theat
                if not source:
                    source = "mdblist"
            if not digital and mdb_dig:
                digital = mdb_dig
                if not source:
                    source = "mdblist"
            if not physical and mdb_phys:
                physical = mdb_phys
                if not source:
                    source = "mdblist"

        # ── Tier 4: Simkl (last resort for any still-missing dates) ──
        if simkl and movie_simkl_id and (not theatrical or not digital):
            simkl_theatrical, simkl_digital = await self._get_simkl_releases(
                simkl, movie_simkl_id, country,
            )
            simkl_any = simkl_theatrical or simkl_digital
            if simkl_any:
                source_dates["simkl"] = {
                    "theatrical": simkl_theatrical,
                    "digital": simkl_digital,
                    "physical": None,
                }
            if not theatrical and simkl_theatrical:
                theatrical = simkl_theatrical
                if not source:
                    source = "simkl"
            if not digital and simkl_digital:
                digital = simkl_digital
                if not source:
                    source = "simkl"

        # ── Cross-verification: log discrepancies between sources ──
        if len(source_dates) > 1:
            self._log_release_date_discrepancies(
                cache_id, source_dates,
            )

        # Resolve source label when only lower tiers contributed
        if not source and (theatrical or digital or physical):
            source = "mdblist" if "mdblist" in source_dates else "simkl"

        # Cache result (even if all None — avoids re-fetching for movies
        # that genuinely have no typed releases)
        try:
            await r.setex(cache_key, SEASON_INFO_CACHE_TTL,
                          json.dumps({"theatrical": theatrical, "digital": digital,
                                      "physical": physical, "source": source}))
        except Exception:
            pass

        return theatrical, digital, physical, source

    async def _get_mdblist_releases(
        self, tmdb_id: int | None = None, imdb_id: str | None = None,
    ) -> tuple[str | None, str | None, str | None]:
        """Fetch release dates from MDBList — media info + calendar.

        Returns (theatrical, digital, physical).
        Uses 24h Redis cache per movie.

        Two sources are checked:
          1. ``/calendar/events`` — has typed ``release_type`` (release, digital)
          2. ``get_media_info`` — general movie info with release fields
        """
        if not tmdb_id and not imdb_id:
            return None, None, None

        provider = None
        media_id = None
        if imdb_id:
            provider, media_id = "imdb", imdb_id
        elif tmdb_id:
            provider, media_id = "tmdb", str(tmdb_id)

        cache_key = f"airing_alerts:mdblist_releases:{provider}:{media_id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data.get("theatrical"), data.get("digital"), data.get("physical")
        except Exception:
            pass

        theatrical = None
        digital = None
        physical = None

        try:
            mdb_key = await secure_get("mdblist_api_key")
            if not mdb_key:
                return None, None, None

            from app.utils.mdblist_client import MDBListClient

            # ── Source 1: MDBList calendar (typed release dates) ──
            # Check cached calendar index for this movie by TMDB ID
            cal_cache_key = "airing_alerts:mdblist_movie_releases"
            try:
                cal_cached = await r.get(cal_cache_key)
                if cal_cached:
                    movie_index = json.loads(cal_cached)
                else:
                    # Build movie release index from calendar
                    mdb_cal = MDBListClient(api_key=mdb_key)
                    try:
                        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        end = (datetime.now(timezone.utc) + timedelta(days=120)).strftime("%Y-%m-%d")
                        events = await mdb_cal.get_calendar_events(
                            start=today, end=end, limit=1000, favorite_cast=False,
                        )
                    finally:
                        await mdb_cal.close()

                    movie_index = {}
                    for ev in events:
                        if ev.get("type") != "movie":
                            continue
                        ev_tmdb = ev.get("tmdb")
                        if not ev_tmdb:
                            continue
                        key = str(ev_tmdb)
                        release_type = ev.get("release_type", "")
                        ev_date = ev.get("start")
                        if not ev_date:
                            continue

                        if key not in movie_index:
                            movie_index[key] = {}
                        if release_type in ("release", "theatrical", "premiere"):
                            movie_index[key]["theatrical"] = ev_date
                        elif release_type == "digital":
                            movie_index[key]["digital"] = ev_date
                        elif release_type == "physical":
                            movie_index[key]["physical"] = ev_date

                    await r.setex(cal_cache_key, 21600, json.dumps(movie_index))
            except Exception:
                movie_index = {}

            tmdb_str = str(tmdb_id) if tmdb_id else None
            if tmdb_str and tmdb_str in movie_index:
                cal_dates = movie_index[tmdb_str]
                theatrical = cal_dates.get("theatrical")
                digital = cal_dates.get("digital")
                physical = cal_dates.get("physical")

            # ── Source 2: MDBList media info (fills gaps) ──
            if provider and (not theatrical or not digital or not physical):
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    info = await mdb.get_media_info(provider, "movie", media_id)
                finally:
                    await mdb.close()

                if info and isinstance(info, dict):
                    if not theatrical:
                        theatrical = _normalise_date(
                            info.get("released")
                            or info.get("release_date")
                            or info.get("theatrical_release")
                        )
                    if not digital:
                        digital = _normalise_date(
                            info.get("digital_release")
                            or info.get("digital_release_date")
                        )
                    if not physical:
                        physical = _normalise_date(
                            info.get("physical_release")
                            or info.get("physical_release_date")
                        )
        except Exception:
            log.debug("airing_alerts.mdblist_releases_failed",
                      provider=provider, media_id=media_id)

        # Cache even if all None
        try:
            await r.setex(cache_key, SEASON_INFO_CACHE_TTL,
                          json.dumps({"theatrical": theatrical, "digital": digital,
                                      "physical": physical}))
        except Exception:
            pass

        return theatrical, digital, physical

    @staticmethod
    def _log_release_date_discrepancies(
        movie_id: str, source_dates: dict[str, dict],
    ) -> None:
        """Log when multiple sources disagree on release dates."""
        sources = list(source_dates.keys())
        for date_type in ("theatrical", "digital", "physical"):
            values: dict[str, str] = {}
            for src in sources:
                val = source_dates[src].get(date_type)
                if val:
                    values[src] = val

            if len(values) > 1:
                unique = set(values.values())
                if len(unique) > 1:
                    log.info("airing_alerts.release_date_mismatch",
                             movie=movie_id, date_type=date_type,
                             dates=values)

    async def _get_simkl_releases(
        self, simkl: SimklClient, movie_simkl_id: str, country: str,
    ) -> tuple[str | None, str | None]:
        """Fetch theatrical + digital dates from Simkl /releases/{country}.

        Uses server country first, falls back to US.
        """
        theatrical = None
        digital = None

        countries = [country]
        if country != "us":
            countries.append("us")

        for c in countries:
            try:
                releases = await simkl.get_movie_releases(movie_simkl_id, country=c)
            except Exception:
                log.debug("airing_alerts.releases_fetch_failed",
                          movie_simkl_id=movie_simkl_id, country=c)
                continue

            for rel in releases or []:
                rtype = rel.get("release_type", "")
                rdate = rel.get("release_date")
                if not rdate:
                    continue
                if rtype in ("premiere", "limited", "theatrical") and not theatrical:
                    theatrical = rdate
                elif rtype == "digital" and not digital:
                    digital = rdate

            if theatrical or digital:
                break

        return theatrical, digital

    # ------------------------------------------------------------------
    # Server country
    # ------------------------------------------------------------------

    @staticmethod
    async def _get_server_country() -> str:
        """Return the Emby server's MetadataCountryCode, lowercased.

        Cached in Redis for 24h — the server country setting almost never
        changes and we don't want to hit /System/Configuration on every
        Airing Soon refresh.
        """
        cache_key = "airing_alerts:server_country"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                return cached
        except Exception:
            pass

        try:
            emby = EmbyClient()
            try:
                country = await emby.get_metadata_country()
            finally:
                await emby.close()
        except Exception:
            log.warning("airing_alerts.server_country_failed")
            country = "us"

        try:
            await r.setex(cache_key, SEASON_INFO_CACHE_TTL, country)
        except Exception:
            pass

        return country

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
        """Cross-check a Simkl show/movie object against the Emby library
        via provider IDs, falling back to title+year matching."""
        ids = item.get("ids", {})
        for provider_type, simkl_key in [("Tvdb", "tvdb"), ("Tmdb", "tmdb"), ("Imdb", "imdb")]:
            pid = ids.get(simkl_key)
            if pid:
                cached = await LibraryCache.find_by_provider_id(provider_type, str(pid))
                if cached:
                    return True, cached["emby_id"]

        cached = await LibraryCache.find_by_title(item.get("title", ""), year=item.get("year"))
        if cached:
            return True, cached["emby_id"]

        return False, None


# ── Module-level helpers ──────────────────────────────────────────────


def _is_tba_title(title: str | None) -> bool:
    """Detect placeholder episode titles that should be resolved from other sources."""
    if not title or not title.strip():
        return True
    t = title.strip().lower()
    if t in ("tba", "tbd", "to be announced", "to be determined", "n/a"):
        return True
    # "Episode 1", "Episode 12", etc.
    import re
    if re.fullmatch(r"episode\s+\d+", t):
        return True
    return False


def _normalise_date(dt_str: str | None) -> str | None:
    """Convert an ISO datetime (e.g. from Radarr) to a date-only string."""
    if not dt_str:
        return None
    try:
        # Handle both "2026-07-15" and "2026-07-15T00:00:00Z" formats
        return dt_str[:10]
    except Exception:
        return None


def _find_sonarr_episode(
    episodes: list[dict], season: int | None, episode_num: int | None,
) -> dict | None:
    """Find a matching episode in a Sonarr calendar list."""
    if season is None or episode_num is None:
        return None
    for ep in episodes:
        if ep.get("season") == season and ep.get("episode") == episode_num:
            return ep
    return None


def _date_in_window(date_str: str | None, today_date, days: int) -> bool:
    """Check if a date string falls within today .. today+days (inclusive)."""
    if not date_str:
        return False
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        return today_date <= dt <= today_date + timedelta(days=days)
    except Exception:
        return False

"""Missed Scrobble Audit — surface items played in Emby but absent from Simkl.

Compares Emby's per-user played status (UserData.Played) against the Simkl
watched list for both movies and shows.  Items present in Emby as played but
missing from the Simkl watched set are flagged as missed scrobbles.

Supports one-click and bulk backfill via Simkl's POST /sync/history.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from sqlalchemy import select

from app.models.schema import User, AppSetting
from app.utils.simkl_client import SimklClient
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set
from app.utils.database import async_session

log = structlog.get_logger()

AUDIT_CACHE_TTL = 3600  # 1h — avoids hammering both APIs on repeated opens


class ScrobbleAuditService:

    async def run_audit(self, user: User, force: bool = False) -> dict:
        """Compare Emby played items against Simkl and/or MDBList watched history.

        Returns {movies: [...], shows: [...], summary: {...}}.
        Each item includes enough metadata for display + backfill.
        """
        # Determine which providers are active
        providers = await self._get_active_providers()
        has_simkl = "simkl" in providers and user.simkl_access_token
        has_mdblist = "mdblist" in providers and await self._get_mdblist_key()

        if not user.emby_user_id or (not has_simkl and not has_mdblist):
            return {"movies": [], "shows": [], "summary": {"movies": 0, "shows": 0}}

        # Check cache first (skip if force)
        cache_key = f"scrobble_audit:{user.id}"
        if not force:
            try:
                r = await get_redis()
                cached = await r.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception:
                pass

        simkl = await self._make_simkl(user) if has_simkl else None
        mdb = await self._make_mdblist() if has_mdblist else None
        emby = EmbyClient()
        try:
            result = await self._compare(simkl, emby, user, mdb=mdb)
        finally:
            if simkl:
                await simkl.close()
            if mdb:
                await mdb.close()
            await emby.close()

        # Cache result
        try:
            r = await get_redis()
            await r.setex(cache_key, AUDIT_CACHE_TTL, json.dumps(result))
        except Exception:
            pass

        return result

    async def invalidate_cache(self, user_id: int) -> None:
        try:
            r = await get_redis()
            await r.delete(f"scrobble_audit:{user_id}")
        except Exception:
            pass

    async def get_dismissed(self, user_id: int) -> list[str]:
        """Return list of dismissed Emby IDs for a user (DB-backed, Redis-cached)."""
        cache_key = f"scrobble_audit_dismissed:{user_id}"
        db_key = f"scrobble_dismissed:{user_id}"
        # Try Redis cache first
        try:
            r = await get_redis()
            raw = await r.get(cache_key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        # Fall back to DB
        try:
            async with async_session() as db:
                row = (await db.execute(
                    select(AppSetting).where(AppSetting.key == db_key)
                )).scalar_one_or_none()
                dismissed = json.loads(row.value) if row and row.value else []
            # Re-populate Redis cache
            try:
                r = await get_redis()
                await r.set(cache_key, json.dumps(dismissed))
            except Exception:
                pass
            return dismissed
        except Exception:
            pass
        return []

    async def _save_dismissed(self, user_id: int, dismissed: list[str]) -> None:
        """Persist dismissed list to DB and Redis."""
        cache_key = f"scrobble_audit_dismissed:{user_id}"
        db_key = f"scrobble_dismissed:{user_id}"
        encoded = json.dumps(dismissed)
        # Write to DB
        try:
            async with async_session() as db:
                row = (await db.execute(
                    select(AppSetting).where(AppSetting.key == db_key)
                )).scalar_one_or_none()
                if row:
                    row.value = encoded
                    row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                else:
                    db.add(AppSetting(key=db_key, value=encoded,
                                      updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))
                await db.commit()
        except Exception:
            log.warning("scrobble_audit.dismissed_db_write_failed", user_id=user_id)
        # Write to Redis cache
        try:
            r = await get_redis()
            await r.set(cache_key, encoded)
        except Exception:
            pass

    async def dismiss_item(self, user_id: int, emby_id: str) -> dict:
        """Dismiss an item from the audit list (persisted to DB)."""
        dismissed = await self.get_dismissed(user_id)
        if emby_id not in dismissed:
            dismissed.append(emby_id)
        await self._save_dismissed(user_id, dismissed)
        await self.invalidate_cache(user_id)
        return {"dismissed": emby_id}

    async def undismiss_item(self, user_id: int, emby_id: str) -> dict:
        """Re-enable a previously dismissed item."""
        dismissed = await self.get_dismissed(user_id)
        dismissed = [d for d in dismissed if d != emby_id]
        await self._save_dismissed(user_id, dismissed)
        await self.invalidate_cache(user_id)
        return {"undismissed": emby_id}

    async def clear_all_dismissals(self, user_id: int) -> dict:
        """Remove all dismissed items so they reappear in the audit."""
        prev = await self.get_dismissed(user_id)
        count = len(prev)
        await self._save_dismissed(user_id, [])
        await self.invalidate_cache(user_id)
        log.info("scrobble_audit.dismissals_cleared", user_id=user_id, count=count)
        return {"cleared": count}

    async def backfill(self, user: User, items: list[dict]) -> dict:
        """Send items to Simkl watch history.

        Movies: {type: "movie", imdb_id, tmdb_id, title, last_played}
        Shows:  {type: "show", imdb_id, tmdb_id, tvdb_id, title, episodes: [{season, episode, last_played}, ...]}
        """
        if not items:
            return {"added": 0}

        simkl = await self._make_simkl(user)
        try:
            payload = []
            show_groups = {}  # keyed by sorted ids tuple
            for item in items:
                ids = {}
                if item.get("imdb_id"):
                    ids["imdb"] = item["imdb_id"]
                if item.get("tmdb_id"):
                    ids["tmdb"] = int(item["tmdb_id"]) if str(item["tmdb_id"]).isdigit() else item["tmdb_id"]
                if item.get("tvdb_id"):
                    ids["tvdb"] = int(item["tvdb_id"]) if str(item["tvdb_id"]).isdigit() else item["tvdb_id"]
                if not ids:
                    continue

                if item.get("type") == "show" and item.get("episodes"):
                    # Group episodes by show → season for Simkl
                    # Simkl needs ONE entry per show with all seasons/episodes
                    ids_key = tuple(sorted(ids.items()))
                    if ids_key not in show_groups:
                        show_groups[ids_key] = {"ids": ids, "seasons": {}}
                    sg = show_groups[ids_key]
                    for ep in item["episodes"]:
                        s_num = ep.get("season", 0)
                        e_num = ep.get("episode", 0)
                        ep_at = ep.get("last_played") or datetime.now(timezone.utc).isoformat()
                        if s_num not in sg["seasons"]:
                            sg["seasons"][s_num] = []
                        sg["seasons"][s_num].append({
                            "number": e_num,
                            "watched_at": ep_at,
                        })
                else:
                    # Movie (or legacy show entry without episodes)
                    watched_at = item.get("last_played")
                    if not watched_at:
                        watched_at = datetime.now(timezone.utc).isoformat()
                    entry = {
                        "ids": ids,
                        "watched_at": watched_at,
                    }
                    if item.get("type") == "show":
                        entry["_type"] = "show"
                    payload.append(entry)

            # Convert grouped shows into Simkl payload format
            for ids_key, sg in show_groups.items():
                show_entry = {
                    "ids": sg["ids"],
                    "seasons": [
                        {"number": s_num, "episodes": eps}
                        for s_num, eps in sorted(sg["seasons"].items())
                    ],
                    "_type": "show",
                }
                payload.append(show_entry)

            if not payload:
                return {"added": 0}

            # Pre-step: move any plantowatch shows to "watching" status.
            # Simkl won't process episode history for plantowatch shows —
            # it just echoes the status and adds 0 episodes.
            show_entries = [p for p in payload if p.get("_type") == "show"]
            if show_entries:
                status_payload = {
                    "shows": [
                        {"ids": s["ids"], "to": "watching"}
                        for s in show_entries
                    ]
                }
                try:
                    status_result = await simkl._post("/sync/add-to-list", status_payload)
                    log.info("scrobble_audit.backfill_status_change",
                             shows=len(show_entries),
                             result=str(status_result)[:300])
                except Exception as e:
                    log.warning("scrobble_audit.backfill_status_change_failed",
                                error=str(e)[:200])

            result = await simkl.add_to_history(payload)
            added_movies = result.get("added", {}).get("movies", 0)
            added_episodes = result.get("added", {}).get("episodes", 0)
            added_shows = result.get("added", {}).get("shows", 0)
            not_found = result.get("not_found", {})
            total = added_movies + added_episodes + added_shows

            log.info("scrobble_audit.backfill_done", user=user.emby_username,
                     requested=len(items), payload_entries=len(payload), added=total,
                     simkl_response_added=result.get("added"),
                     simkl_response_existing=result.get("existing"),
                     simkl_response_not_found=not_found if not_found else None)

            # Also backfill to MDBList if enabled
            mdblist_result = await self._backfill_mdblist(items)

            # Invalidate audit cache so next view reflects the backfill
            await self.invalidate_cache(user.id)

            # Also flush the Simkl activity-gate cache so the next audit
            # re-fetches from Simkl instead of using stale cached data
            try:
                r = await get_redis()
                prefix = simkl._cache_prefix()
                for cache_key_pattern in (
                    f"simkl_sync_data:{prefix}:*",
                    f"simkl_sync_ts:{prefix}:*",
                    f"simkl_activities_cache:{prefix}",
                ):
                    async for key in r.scan_iter(cache_key_pattern):
                        await r.delete(key)
            except Exception:
                pass

            return {
                "added": total,
                "detail": result.get("added", {}),
                "simkl": {
                    "movies": added_movies,
                    "shows": added_shows,
                    "episodes": added_episodes,
                },
                "mdblist": mdblist_result,
            }
        finally:
            await simkl.close()

    # ------------------------------------------------------------------

    async def _get_played_items(
        self, emby: EmbyClient, user_id: str, item_type: str,
    ) -> list[dict]:
        """Fetch played items from Emby with LastPlayedDate.

        The field name ``UserDataLastPlayedDate`` tells Emby to include
        ``LastPlayedDate`` inside the ``UserData`` block — the generic
        ``UserData`` field name does NOT do this.
        """
        items: list[dict] = []
        start = 0
        batch = 500
        while True:
            resp = await emby.get_items(
                user_id=user_id,
                item_type=item_type,
                fields="ProviderIds,ProductionYear,DateCreated,UserDataLastPlayedDate,UserDataPlayCount",
                filters="IsPlayed",
                sort_by="DatePlayed",
                sort_order="Descending",
                limit=batch,
                start_index=start,
            )
            items.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
        return items

    async def _compare(self, simkl: SimklClient | None, emby: EmbyClient, user: User, mdb=None) -> dict:
        # ── Dismissed items ──
        dismissed = set(await self.get_dismissed(user.id))

        # ── Simkl side: build sets of watched IDs ──
        simkl_movie_ids: set[str] = set()

        def _add_movie_ids(entries: list[dict]) -> None:
            """Add provider IDs from movie/anime entries to the watched set."""
            for entry in entries:
                item = entry.get("movie", {}) if "movie" in entry else entry
                ids = item.get("ids", {})
                for key in ("imdb", "tmdb", "tvdb"):
                    val = ids.get(key)
                    if val:
                        simkl_movie_ids.add(f"{key}:{val}")

        if simkl:
            try:
                simkl_movies = await simkl.get_watched(kind="movies")
                _add_movie_ids(simkl_movies)
            except Exception:
                log.warning("scrobble_audit.simkl_movies_failed")

            # Anime movies — Simkl classifies some films as anime, not movies.
            # Without this check, anime films show as false-positive missed scrobbles.
            try:
                simkl_anime = await simkl.get_watched(kind="anime")
                _add_movie_ids(simkl_anime)
                log.debug("scrobble_audit.simkl_anime_merged",
                          count=len(simkl_anime))
            except Exception:
                log.warning("scrobble_audit.simkl_anime_failed")

        # Simkl shows: build show-level completed set + episode-level set
        # 1. /sync/all-items/shows/completed → fully watched, skip entire show
        # 2. /sync/all-items/shows/watching  → partially watched, check episodes
        simkl_completed_shows: set[str] = set()  # "imdb:ttXXX", "tvdb:123", "title:lowername"
        simkl_watched_eps: set[str] = set()  # "{imdb_or_tvdb}:S{s}E{e}"
        simkl_watching_shows: set[str] = set()  # shows in "watching" status (fallback)

        def _extract_show_keys(entry: dict) -> list[str]:
            """Build provider + title keys for a show entry."""
            show_obj = entry.get("show", {}) if "show" in entry else entry
            show_ids = show_obj.get("ids", {})
            keys: list[str] = []
            for key in ("imdb", "tvdb", "tmdb"):
                val = show_ids.get(key)
                if val:
                    keys.append(f"{key}:{val}")
            title = show_obj.get("title", "")
            if title:
                keys.append(f"title:{title.lower().strip()}")
            return keys

        def _extract_episodes(entry: dict, show_keys: list[str]):
            """Walk seasons→episodes and add to simkl_watched_eps."""
            for season in entry.get("seasons", []):
                s_num = season.get("number", 0)
                for ep in season.get("episodes", []):
                    e_num = ep.get("number", 0)
                    ep_tag = f"S{s_num}E{e_num}"
                    for sk in show_keys:
                        simkl_watched_eps.add(f"{sk}:{ep_tag}")

        if simkl:
            # Phase 1: completed shows — all episodes considered watched
            try:
                simkl_shows = await simkl.get_watched(kind="shows")
                for entry in simkl_shows:
                    keys = _extract_show_keys(entry)
                    for sk in keys:
                        simkl_completed_shows.add(sk)
                    _extract_episodes(entry, keys)
                log.info("scrobble_audit.simkl_shows_parsed",
                         completed_shows=len(simkl_shows),
                         completed_show_keys=len(simkl_completed_shows),
                         episode_keys=len(simkl_watched_eps))
            except Exception:
                log.warning("scrobble_audit.simkl_shows_failed")

            # Phase 2: watching shows — per-episode matching
            try:
                watching_data = await simkl._activity_gated_get(
                    "/sync/all-items/shows/watching",
                    "tv_shows.watching",
                    params={"extended": "full"},
                )
                watching_shows = simkl._unwrap_sync_response(watching_data, "shows")
                eps_before = len(simkl_watched_eps)
                for entry in watching_shows:
                    keys = _extract_show_keys(entry)
                    _extract_episodes(entry, keys)
                    # Fallback: if Simkl doesn't provide episode data for
                    # this show, add it to the watching set so we can skip
                    # it rather than report false positives
                    if not entry.get("seasons"):
                        for sk in keys:
                            simkl_watching_shows.add(sk)
                log.info("scrobble_audit.simkl_watching_parsed",
                         watching_shows=len(watching_shows),
                         new_episode_keys=len(simkl_watched_eps) - eps_before,
                         no_episode_data=len(simkl_watching_shows))
            except Exception as e:
                log.warning("scrobble_audit.simkl_watching_failed",
                            error=str(e)[:120])

        # Episode-level provider IDs from Simkl history — catches numbering
        # mismatches between Emby and Simkl (e.g. The Pitt S2E24 vs S2E14)
        simkl_ep_ids: set[str] = set()
        if simkl:
            try:
                simkl_ep_ids = await simkl.get_watched_episode_ids()
                log.info("scrobble_audit.simkl_episode_ids", count=len(simkl_ep_ids))
            except Exception:
                log.warning("scrobble_audit.simkl_ep_ids_failed")

        # ── MDBList side: merge watched data into the same sets ──
        if mdb:
            try:
                mdb_watched = await mdb.get_watched()
                # Movies
                for entry in mdb_watched.get("movies", []):
                    item = entry.get("movie", {}) if "movie" in entry else entry
                    ids = item.get("ids", {})
                    # Also try flat imdb_id/tmdb_id fields
                    if not ids:
                        ids = {}
                        for k, fk in [("imdb", "imdb_id"), ("tmdb", "tmdb_id"), ("tvdb", "tvdb_id")]:
                            if entry.get(fk):
                                ids[k] = entry[fk]
                    for key in ("imdb", "tmdb", "tvdb", "simkl"):
                        val = ids.get(key)
                        if val:
                            simkl_movie_ids.add(f"{key}:{val}")
                # Shows (episode-level)
                for entry in mdb_watched.get("shows", []):
                    show_obj = entry.get("show", {}) if "show" in entry else entry
                    show_ids = show_obj.get("ids", {})
                    if not show_ids:
                        show_ids = {}
                        for k, fk in [("imdb", "imdb_id"), ("tmdb", "tmdb_id"), ("tvdb", "tvdb_id")]:
                            if entry.get(fk):
                                show_ids[k] = entry[fk]
                    show_keys_mdb: list[str] = []
                    for key in ("imdb", "tvdb", "tmdb", "simkl"):
                        val = show_ids.get(key)
                        if val:
                            show_keys_mdb.append(f"{key}:{val}")
                    for season in entry.get("seasons", []):
                        s_num = season.get("number", 0)
                        for ep in season.get("episodes", []):
                            e_num = ep.get("number", 0)
                            ep_tag = f"S{s_num}E{e_num}"
                            for sk in show_keys_mdb:
                                simkl_watched_eps.add(f"{sk}:{ep_tag}")
                            # Also add to episode-level ID set
                            ep_ids = ep.get("ids", {})
                            for key in ("imdb", "tmdb", "tvdb"):
                                val = ep_ids.get(key)
                                if val:
                                    simkl_ep_ids.add(f"{key}:{val}")
                log.info("scrobble_audit.mdblist_watched_merged",
                         movies=len(mdb_watched.get("movies", [])),
                         shows=len(mdb_watched.get("shows", [])))
            except Exception as e:
                log.warning("scrobble_audit.mdblist_watched_failed", error=str(e)[:120])

        # ── Emby side: played movies ──
        missing_movies = []
        seen_movie_keys: dict[str, int] = {}  # provider key → index in missing_movies

        emby_movies = await self._get_played_items(emby, user.emby_user_id, "Movie")
        for item in emby_movies:
            emby_id = item.get("Id", "")
            if emby_id in dismissed:
                continue

            provider_ids = item.get("ProviderIds", {})
            item_keys = set()
            # Emby ProviderIds keys are inconsistently cased across
            # endpoints — bulk may return "Imdb", search returns "IMDB".
            # Check all three variants to avoid false-positive misses.
            for variants, simkl_key in [
                (("Imdb", "imdb", "IMDB"), "imdb"),
                (("Tmdb", "tmdb", "TMDB"), "tmdb"),
                (("Tvdb", "tvdb", "TVDB"), "tvdb"),
            ]:
                for v in variants:
                    val = provider_ids.get(v)
                    if val:
                        item_keys.add(f"{simkl_key}:{val}")
                        break

            if not item_keys:
                continue  # can't match without IDs

            if not item_keys & simkl_movie_ids:
                # Dedup: multiple resolutions of the same movie share provider IDs.
                # Use IMDB or TMDB as the dedup key; keep the copy with the latest play date.
                dedup_key = (
                    provider_ids.get("Imdb") or provider_ids.get("imdb")
                    or provider_ids.get("IMDB") or provider_ids.get("Tmdb")
                    or provider_ids.get("tmdb") or provider_ids.get("TMDB") or ""
                )
                ud = item.get("UserData", {})
                lp = ud.get("LastPlayedDate")
                dc = item.get("DateCreated")
                entry = {
                    "emby_id": emby_id,
                    "title": item.get("Name", ""),
                    "year": item.get("ProductionYear"),
                    "type": "movie",
                    "imdb_id": provider_ids.get("Imdb") or provider_ids.get("imdb") or provider_ids.get("IMDB"),
                    "tmdb_id": provider_ids.get("Tmdb") or provider_ids.get("tmdb") or provider_ids.get("TMDB"),
                    "tvdb_id": provider_ids.get("Tvdb") or provider_ids.get("tvdb") or provider_ids.get("TVDB"),
                    "last_played": lp or dc,
                    "date_source": "played" if lp else ("added" if dc else None),
                }

                if dedup_key and dedup_key in seen_movie_keys:
                    # Already have this movie — keep the one with the later date
                    existing_idx = seen_movie_keys[dedup_key]
                    existing = missing_movies[existing_idx]
                    if (entry.get("last_played") or "") > (existing.get("last_played") or ""):
                        missing_movies[existing_idx] = entry
                else:
                    if dedup_key:
                        seen_movie_keys[dedup_key] = len(missing_movies)
                    missing_movies.append(entry)

        # ── Emby side: TV shows — episode-level comparison ──
        missing_shows = []

        # Get all series in the library (don't filter by IsPlayed — we want
        # series with *any* played episodes, not just fully-completed ones)
        emby_all_series = await self._get_played_items(emby, user.emby_user_id, "Series")

        # Also include series that are only partially watched (have some
        # played episodes but aren't fully "Played" at series level).
        partially_watched = await self._get_partial_series(emby, user.emby_user_id)
        # Merge, dedup by Id
        seen_series_ids = {s.get("Id") for s in emby_all_series}
        for s in partially_watched:
            if s.get("Id") not in seen_series_ids:
                emby_all_series.append(s)
                seen_series_ids.add(s.get("Id"))

        total_missing_eps = 0
        for series in emby_all_series:
            series_id = series.get("Id", "")
            if series_id in dismissed:
                continue
            series_name = series.get("Name", "")
            series_year = series.get("ProductionYear")
            series_providers = series.get("ProviderIds", {})

            # Build show-level keys for Simkl matching
            show_keys: list[str] = []
            for variants, simkl_key in [
                (("Imdb", "imdb", "IMDB"), "imdb"),
                (("Tmdb", "tmdb", "TMDB"), "tmdb"),
                (("Tvdb", "tvdb", "TVDB"), "tvdb"),
            ]:
                for v in variants:
                    val = series_providers.get(v)
                    if val:
                        show_keys.append(f"{simkl_key}:{val}")
                        break
            # Title-based fallback for when Emby bulk endpoint doesn't
            # return ProviderIds for Series items
            if series_name:
                show_keys.append(f"title:{series_name.lower().strip()}")
            if not show_keys:
                continue

            # Check 0: if the entire show is "completed" on Simkl,
            # all episodes are watched — skip episode-level comparison
            if any(sk in simkl_completed_shows for sk in show_keys):
                continue

            # Check 0b: if the show is "watching" on Simkl but Simkl
            # didn't provide per-episode data, we can't verify individual
            # episodes — skip to avoid false positives
            if any(sk in simkl_watching_shows for sk in show_keys):
                continue

            # Fetch played episodes for this series from Emby
            played_episodes = await self._get_played_episodes(
                emby, user.emby_user_id, series_id
            )
            if not played_episodes:
                continue

            # Compare each played episode against Simkl
            missing_eps = []
            for ep in played_episodes:
                s_num = ep.get("ParentIndexNumber", 0)
                e_num = ep.get("IndexNumber", 0)
                if s_num == 0 and e_num == 0:
                    continue  # specials without numbering — skip
                ep_tag = f"S{s_num}E{e_num}"

                # Check 1: SxxExx match against Simkl watched shows
                found = False
                for sk in show_keys:
                    if f"{sk}:{ep_tag}" in simkl_watched_eps:
                        found = True
                        break

                # Check 2: episode-level provider ID match (handles numbering
                # mismatches like The Pitt S2E24 in Emby = S2E14 on Simkl)
                if not found and simkl_ep_ids:
                    ep_providers = ep.get("ProviderIds", {})
                    for variants, simkl_key in [
                        (("Imdb", "imdb", "IMDB"), "imdb"),
                        (("Tmdb", "tmdb", "TMDB"), "tmdb"),
                        (("Tvdb", "tvdb", "TVDB"), "tvdb"),
                    ]:
                        for v in variants:
                            val = ep_providers.get(v)
                            if val and f"{simkl_key}:{val}" in simkl_ep_ids:
                                found = True
                                break
                        if found:
                            break

                if found:
                    continue

                ep_ud = ep.get("UserData", {})
                lp = ep_ud.get("LastPlayedDate")
                dc = ep.get("DateCreated")
                missing_eps.append({
                    "season": s_num,
                    "episode": e_num,
                    "title": ep.get("Name", ""),
                    "last_played": lp or dc,
                    "date_source": "played" if lp else ("added" if dc else None),
                })

            if missing_eps:
                missing_eps.sort(key=lambda e: (e["season"], e["episode"]))
                total_missing_eps += len(missing_eps)
                missing_shows.append({
                    "emby_id": series_id,
                    "title": series_name,
                    "year": series_year,
                    "type": "show",
                    "imdb_id": series_providers.get("Imdb") or series_providers.get("imdb") or series_providers.get("IMDB"),
                    "tmdb_id": series_providers.get("Tmdb") or series_providers.get("tmdb") or series_providers.get("TMDB"),
                    "tvdb_id": series_providers.get("Tvdb") or series_providers.get("tvdb") or series_providers.get("TVDB"),
                    "episode_count": len(missing_eps),
                    "episodes": missing_eps,
                    # Use latest episode's played date as the show-level date
                    "last_played": max(
                        (e["last_played"] for e in missing_eps if e.get("last_played")),
                        default=None,
                    ),
                    "date_source": "played",
                })

        # Sort by title
        missing_movies.sort(key=lambda x: (x.get("title") or "").lower())
        missing_shows.sort(key=lambda x: (x.get("title") or "").lower())

        # Count unique movies by deduping: each movie contributes up to 3 keys
        # (imdb:X, tmdb:X, tvdb:X). Group by provider and take the largest set.
        _unique_movies = set()
        for k in simkl_movie_ids:
            if k.startswith("imdb:"):
                _unique_movies.add(k)
        if not _unique_movies:
            # Fallback: count tmdb keys if no IMDB keys
            for k in simkl_movie_ids:
                if k.startswith("tmdb:"):
                    _unique_movies.add(k)
        # If still empty, rough estimate: total keys / 3
        unique_movie_count = len(_unique_movies) if _unique_movies else len(simkl_movie_ids) // 3

        # Count unique completed shows from Simkl (dedupe across key types)
        _unique_shows = set()
        for k in simkl_completed_shows:
            if k.startswith("imdb:"):
                _unique_shows.add(k)
        if not _unique_shows:
            for k in simkl_completed_shows:
                if k.startswith("tvdb:"):
                    _unique_shows.add(k)
        unique_show_count = len(_unique_shows) if _unique_shows else len(simkl_completed_shows) // 3

        log.info("scrobble_audit.complete", user=user.emby_username,
                 emby_movies=len(emby_movies), emby_series=len(emby_all_series),
                 simkl_movie_ids=len(simkl_movie_ids),
                 simkl_completed_shows=len(simkl_completed_shows),
                 simkl_watched_eps=len(simkl_watched_eps),
                 simkl_ep_ids=len(simkl_ep_ids),
                 dismissed=len(dismissed),
                 missing_movies=len(missing_movies),
                 missing_shows=len(missing_shows),
                 missing_episodes=total_missing_eps)

        return {
            "movies": missing_movies,
            "shows": missing_shows,
            "summary": {
                "movies": len(missing_movies),
                "shows": len(missing_shows),
                "episodes": total_missing_eps,
                "emby_movies_played": sum(1 for m in emby_movies if m.get("UserData", {}).get("Played")),
                "emby_shows_played": len(emby_all_series),
                "simkl_movies_watched": unique_movie_count,
                "simkl_shows_watched": unique_show_count,
                "dismissed_count": len(dismissed),
            },
        }

    async def _get_played_episodes(
        self, emby: EmbyClient, user_id: str, series_id: str,
    ) -> list[dict]:
        """Fetch played episodes for a specific series, sorted by DatePlayed descending."""
        items: list[dict] = []
        start = 0
        batch = 100
        while True:
            resp = await emby.get_items(
                user_id=user_id,
                item_type="Episode",
                parent_id=series_id,
                fields="ProviderIds,UserDataLastPlayedDate,UserDataPlayCount",
                filters="IsPlayed",
                sort_by="DatePlayed",
                sort_order="Descending",
                limit=batch,
                start_index=start,
            )
            items.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
        return items

    async def _get_partial_series(
        self, emby: EmbyClient, user_id: str,
    ) -> list[dict]:
        """Fetch series that have at least one played episode.

        The ``IsPlayed`` filter only returns fully-completed series.
        ``IsResumable`` only returns series with an episode paused mid-playback.
        Neither catches the common case: some episodes watched, none paused,
        series not fully complete.

        Instead, fetch ALL series and filter client-side to those with
        ``UnplayedItemCount < total`` (i.e. at least one episode played).
        """
        items: list[dict] = []
        start = 0
        batch = 500
        while True:
            resp = await emby.get_items(
                user_id=user_id,
                item_type="Series",
                fields="ProviderIds,ProductionYear,DateCreated,UserDataLastPlayedDate,RecursiveItemCount,UserDataUnplayedItemCount",
                sort_by="DatePlayed",
                sort_order="Descending",
                limit=batch,
                start_index=start,
            )
            for s in resp.get("Items", []):
                ud = s.get("UserData", {})
                total = s.get("RecursiveItemCount", 0)
                unplayed = ud.get("UnplayedItemCount", total)
                # Has at least one played episode
                if total > 0 and unplayed < total:
                    items.append(s)
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
        return items

    @staticmethod
    async def _make_simkl(user: User) -> SimklClient:

        return SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    @staticmethod
    async def _get_active_providers() -> set[str]:
        """Return set of active integration providers."""
        try:
            r = await get_redis()
            raw = await r.get("integration_provider")
            if raw:
                val = raw if isinstance(raw, str) else raw.decode()
                if val == "both":
                    return {"simkl", "mdblist"}
                if val in ("simkl", "mdblist"):
                    return {val}
        except Exception:
            pass
        return {"simkl"}  # legacy default

    @staticmethod
    async def _get_mdblist_key() -> str:
        """Return the configured MDBList API key from Redis."""
        try:
            r = await get_redis()
            raw = await secure_get("mdblist_api_key")
            if raw:
                return raw if isinstance(raw, str) else raw.decode()
        except Exception:
            pass
        return ""

    @staticmethod
    async def _make_mdblist():
        """Create an MDBListClient using the stored API key."""
        from app.utils.mdblist_client import MDBListClient
        r = await get_redis()
        raw = await secure_get("mdblist_api_key")
        if not raw:
            return None
        key = raw if isinstance(raw, str) else raw.decode()
        return MDBListClient(api_key=key)

    async def _backfill_mdblist(self, items: list[dict]) -> dict:
        """Send backfill items to MDBList watched history if MDBList is active.
        Returns dict with movies/shows/episodes counts."""
        providers = await self._get_active_providers()
        if "mdblist" not in providers:
            return {"active": False}
        mdb = await self._make_mdblist()
        if not mdb:
            return {"active": False}
        try:
            movies = []
            shows = []
            for item in items:
                ids = {}
                if item.get("imdb_id"):
                    ids["imdb"] = item["imdb_id"]
                if item.get("tmdb_id"):
                    ids["tmdb"] = int(item["tmdb_id"]) if str(item["tmdb_id"]).isdigit() else item["tmdb_id"]
                if item.get("tvdb_id"):
                    ids["tvdb"] = int(item["tvdb_id"]) if str(item["tvdb_id"]).isdigit() else item["tvdb_id"]
                if not ids:
                    continue

                watched_at = item.get("last_played") or datetime.now(timezone.utc).isoformat()

                if item.get("type") == "show" and item.get("episodes"):
                    for ep in item["episodes"]:
                        ep_at = ep.get("last_played") or watched_at
                        shows.append({
                            "ids": ids,
                            "seasons": [{"number": ep.get("season", 0),
                                         "episodes": [{"number": ep.get("episode", 0),
                                                       "watched_at": ep_at}]}],
                        })
                elif item.get("type") == "movie":
                    movies.append({"ids": ids, "watched_at": watched_at})

            if movies or shows:
                result = await mdb.add_to_watched(
                    movies=movies if movies else None,
                    shows=shows if shows else None,
                )
                updated = result.get("updated", {})
                mdb_result = {
                    "active": True,
                    "movies": updated.get("movies", 0),
                    "shows": updated.get("shows", 0),
                    "episodes": updated.get("episodes", 0),
                }
                log.info("scrobble_audit.mdblist_backfill_done",
                         movies=len(movies), shows=len(shows),
                         result=result)
                return mdb_result
            return {"active": True, "movies": 0, "shows": 0, "episodes": 0}
        except Exception as e:
            log.warning("scrobble_audit.mdblist_backfill_failed", error=str(e)[:120])
            return {"active": True, "error": str(e)[:120]}
        finally:
            await mdb.close()

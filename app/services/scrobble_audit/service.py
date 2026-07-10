"""Missed Scrobble Audit — surface items played in Emby but absent from Trakt.

Compares Emby's per-user played status (UserData.Played) against the Trakt
watched list for both movies and shows.  Items present in Emby as played but
missing from the Trakt watched set are flagged as missed scrobbles.

Supports one-click and bulk backfill via Trakt's POST /sync/history.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from sqlalchemy import select

from app.models.schema import User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.database import async_session

log = structlog.get_logger()

AUDIT_CACHE_TTL = 3600  # 1h — avoids hammering both APIs on repeated opens


class ScrobbleAuditService:

    async def run_audit(self, user: User, force: bool = False) -> dict:
        """Compare Emby played items against Trakt watched history.

        Returns {movies: [...], shows: [...], summary: {...}}.
        Each item includes enough metadata for display + backfill.
        """
        if not user.trakt_access_token or not user.emby_user_id:
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

        trakt = await self._make_trakt(user)
        emby = EmbyClient()
        try:
            result = await self._compare(trakt, emby, user)
        finally:
            await trakt.close()

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

    async def backfill(self, user: User, items: list[dict]) -> dict:
        """Send items to Trakt watch history.

        Each item in `items`: {type: "movie"|"show", imdb_id, tmdb_id, title}
        """
        if not items:
            return {"added": 0}

        trakt = await self._make_trakt(user)
        try:
            payload = []
            for item in items:
                ids = {}
                if item.get("imdb_id"):
                    ids["imdb"] = item["imdb_id"]
                if item.get("tmdb_id"):
                    ids["tmdb"] = int(item["tmdb_id"]) if str(item["tmdb_id"]).isdigit() else item["tmdb_id"]
                if not ids:
                    continue

                # Use actual Emby played date if available, otherwise now
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

            if not payload:
                return {"added": 0}

            result = await trakt.add_to_history(payload)
            added_movies = result.get("added", {}).get("movies", 0)
            added_shows = result.get("added", {}).get("episodes", 0) + result.get("added", {}).get("shows", 0)
            total = added_movies + added_shows

            log.info("scrobble_audit.backfill_done", user=user.emby_username,
                     requested=len(items), added=total)

            # Invalidate audit cache so next view reflects the backfill
            await self.invalidate_cache(user.id)

            return {"added": total, "detail": result.get("added", {})}
        finally:
            await trakt.close()

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

    async def _compare(self, trakt: TraktClient, emby: EmbyClient, user: User) -> dict:
        # ── Trakt side: build sets of watched IDs ──
        trakt_movie_ids: set[str] = set()
        trakt_show_ids: set[str] = set()

        try:
            trakt_movies = await trakt.get_watched(kind="movies")
            for entry in trakt_movies:
                ids = entry.get("movie", {}).get("ids", {})
                for key in ("imdb", "tmdb", "tvdb"):
                    val = ids.get(key)
                    if val:
                        trakt_movie_ids.add(f"{key}:{val}")
        except Exception:
            log.warning("scrobble_audit.trakt_movies_failed")

        try:
            trakt_shows = await trakt.get_watched(kind="shows")
            for entry in trakt_shows:
                ids = entry.get("show", {}).get("ids", {})
                for key in ("imdb", "tmdb", "tvdb"):
                    val = ids.get(key)
                    if val:
                        trakt_show_ids.add(f"{key}:{val}")
        except Exception:
            log.warning("scrobble_audit.trakt_shows_failed")

        # ── Emby side: played movies (using Filters=IsPlayed) ──
        missing_movies = []
        missing_shows = []

        emby_movies = await self._get_played_items(emby, user.emby_user_id, "Movie")
        for item in emby_movies:

            provider_ids = item.get("ProviderIds", {})
            item_keys = set()
            for prov, trakt_key in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
                val = provider_ids.get(prov)
                if val:
                    item_keys.add(f"{trakt_key}:{val}")

            if not item_keys:
                continue  # can't match without IDs

            if not item_keys & trakt_movie_ids:
                ud = item.get("UserData", {})
                lp = ud.get("LastPlayedDate")
                dc = item.get("DateCreated")
                missing_movies.append({
                    "emby_id": item.get("Id", ""),
                    "title": item.get("Name", ""),
                    "year": item.get("ProductionYear"),
                    "type": "movie",
                    "imdb_id": provider_ids.get("Imdb"),
                    "tmdb_id": provider_ids.get("Tmdb"),
                    "tvdb_id": provider_ids.get("Tvdb"),
                    "last_played": lp or dc,
                    "date_source": "played" if lp else ("added" if dc else None),
                })

        emby_series = await self._get_played_items(emby, user.emby_user_id, "Series")
        for item in emby_series:
            ud = item.get("UserData", {})
            # A series is "played" if PlayedPercentage == 100 or Played is True
            # or UnplayedItemCount == 0
            is_played = (
                ud.get("Played", False)
                or ud.get("UnplayedItemCount", 1) == 0
                or (ud.get("PlayedPercentage") or 0) >= 100
            )
            if not is_played:
                continue

            provider_ids = item.get("ProviderIds", {})
            item_keys = set()
            for prov, trakt_key in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
                val = provider_ids.get(prov)
                if val:
                    item_keys.add(f"{trakt_key}:{val}")

            if not item_keys:
                continue

            if not item_keys & trakt_show_ids:
                lp = ud.get("LastPlayedDate")
                dc = item.get("DateCreated")
                missing_shows.append({
                    "emby_id": item.get("Id", ""),
                    "title": item.get("Name", ""),
                    "year": item.get("ProductionYear"),
                    "type": "show",
                    "imdb_id": provider_ids.get("Imdb"),
                    "tmdb_id": provider_ids.get("Tmdb"),
                    "tvdb_id": provider_ids.get("Tvdb"),
                    "last_played": lp or dc,
                    "date_source": "played" if lp else ("added" if dc else None),
                })

        # Sort by title
        missing_movies.sort(key=lambda x: (x.get("title") or "").lower())
        missing_shows.sort(key=lambda x: (x.get("title") or "").lower())

        log.info("scrobble_audit.complete", user=user.emby_username,
                 emby_movies=len(emby_movies), emby_series=len(emby_series),
                 trakt_movie_ids=len(trakt_movie_ids), trakt_show_ids=len(trakt_show_ids),
                 missing_movies=len(missing_movies), missing_shows=len(missing_shows))

        return {
            "movies": missing_movies,
            "shows": missing_shows,
            "summary": {
                "movies": len(missing_movies),
                "shows": len(missing_shows),
                "emby_movies_played": sum(1 for m in emby_movies if m.get("UserData", {}).get("Played")),
                "emby_shows_played": sum(1 for s in emby_series if (
                    s.get("UserData", {}).get("Played")
                    or s.get("UserData", {}).get("UnplayedItemCount", 1) == 0
                )),
                "trakt_movies_watched": len(set(k.split(":")[1] for k in trakt_movie_ids if k.startswith("imdb:")) or trakt_movie_ids),
                "trakt_shows_watched": len(set(k.split(":")[1] for k in trakt_show_ids if k.startswith("imdb:")) or trakt_show_ids),
            },
        }

    @staticmethod
    async def _make_trakt(user: User) -> TraktClient:
        async def on_token_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await db.commit()

        return TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=on_token_refresh,
        )

"""Async Simkl API client.

Drop-in replacement for SimklClient. Every request includes client_id,
app-name, app-version as URL params plus User-Agent header.

Auth: PIN flow (GET /oauth/pin → poll GET /oauth/pin/{user_code}).
Tokens are ~5 years, NO refresh token — just re-auth on 401.
Scrobble: POST /scrobble/{start|pause|stop|checkin}, progress 0-100 (max 2dp).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import structlog

from app.config import settings

log = structlog.get_logger()

BASE_URL = "https://api.simkl.com"
CDN_URL = "https://data.simkl.in"


class SimklClient:
    """Async Simkl API client."""

    def __init__(
        self,
        access_token: str | None = None,
        token_expires: datetime | None = None,
    ):
        self._access_token = access_token
        self._token_expires = token_expires
        self._client = httpx.AsyncClient(timeout=15.0)
        self._client_id = settings.simkl_client_id

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _base_params(self) -> dict:
        return {
            "client_id": self._client_id,
            "app-name": "emby-simkl-suite",
            "app-version": "1.0",
        }

    def _auth_headers(self) -> dict:
        h = {
            "Content-Type": "application/json",
            "User-Agent": "emby-simkl-suite/1.0",
        }
        if self._access_token:
            h["Authorization"] = f"Bearer {self._access_token}"
        return h

    def _token_hint(self) -> str:
        """First 8 + last 4 chars of token for debug logging."""
        t = self._access_token or ""
        if len(t) > 12:
            return f"{t[:8]}…{t[-4:]}"
        return t[:8] if t else "(none)"

    def _parse_401(self, resp: httpx.Response, path: str) -> httpx.HTTPStatusError:
        """Extract Simkl's error body on 401 and log diagnostics."""
        body = {}
        try:
            body = resp.json()
        except Exception:
            pass
        simkl_error = body.get("error", "unknown")
        simkl_msg = body.get("message", "")
        log.warning(
            "simkl.auth_rejected",
            path=path,
            simkl_error=simkl_error,
            simkl_message=simkl_msg[:120],
            token_hint=self._token_hint(),
            www_auth=resp.headers.get("WWW-Authenticate", ""),
        )
        return httpx.HTTPStatusError(
            f"Simkl 401: {simkl_error} — {simkl_msg or 're-auth required'}",
            request=resp.request, response=resp,
        )

    async def _get(
        self, path: str, params: dict | None = None, auth_required: bool = True,
    ) -> Any:
        merged = {**self._base_params(), **(params or {})}
        headers = self._auth_headers() if auth_required else {
            "Content-Type": "application/json",
            "User-Agent": "emby-simkl-suite/1.0",
        }
        resp = await self._client.get(
            f"{BASE_URL}{path}", params=merged, headers=headers,
        )
        if resp.status_code == 401:
            raise self._parse_401(resp, path)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            log.warning("simkl.rate_limited", retry_after=retry_after)
            raise httpx.HTTPStatusError(
                f"Rate limited, retry after {retry_after}s",
                request=resp.request, response=resp,
            )
        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()

    async def _post(
        self, path: str, payload: dict | None = None,
        params: dict | None = None, auth_required: bool = True,
    ) -> Any:
        merged = {**self._base_params(), **(params or {})}
        headers = self._auth_headers() if auth_required else {
            "Content-Type": "application/json",
            "User-Agent": "emby-simkl-suite/1.0",
        }
        resp = await self._client.post(
            f"{BASE_URL}{path}", json=payload or {}, params=merged, headers=headers,
        )
        if resp.status_code == 401:
            raise self._parse_401(resp, path)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            log.warning("simkl.rate_limited", retry_after=retry_after)
            raise httpx.HTTPStatusError(
                f"Rate limited, retry after {retry_after}s",
                request=resp.request, response=resp,
            )
        # 409 = duplicate prevention on scrobble (not an error)
        if resp.status_code == 409:
            return resp.json()
        if resp.status_code == 400:
            body_text = ""
            try:
                body_text = resp.text[:300]
            except Exception:
                pass
            log.warning("simkl.bad_request", path=path,
                        response_body=body_text,
                        payload_keys=list((payload or {}).keys()),
                        token_hint=self._token_hint())
        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()

    async def _delete(self, path: str) -> None:
        merged = self._base_params()
        resp = await self._client.delete(
            f"{BASE_URL}{path}", params=merged, headers=self._auth_headers(),
        )
        resp.raise_for_status()

    async def _get_cdn(self, path: str) -> Any:
        """Fetch from CDN — include client_id/app params per Simkl docs."""
        resp = await self._client.get(
            f"{CDN_URL}{path}",
            params=self._base_params(),
            headers={"User-Agent": "emby-simkl-suite/1.0"},
        )
        resp.raise_for_status()
        return resp.json()

    def get_rate_limit_info(self) -> dict:
        """Stub — Simkl has 10 GET/sec + 1 POST/sec per client_id,
        but no per-request remaining counter in response headers.
        Always returns budget-OK so callers proceed."""
        return {"remaining": 999, "limit": 999}

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Auth — PIN flow
    # ------------------------------------------------------------------

    async def get_pin_code(self) -> dict:
        """Step 1: Request a PIN code for device auth.
        Returns {user_code, verification_url, expires_in, interval}."""
        params = {"client_id": self._client_id}
        resp = await self._client.get(
            f"{BASE_URL}/oauth/pin",
            params=params,
            headers={"User-Agent": "emby-simkl-suite/1.0"},
        )
        resp.raise_for_status()
        return resp.json()

    async def poll_pin_token(self, user_code: str) -> dict | None:
        """Step 2: Poll for token after user enters PIN.
        Returns token dict on success, None if still pending."""
        params = {"client_id": self._client_id}
        resp = await self._client.get(
            f"{BASE_URL}/oauth/pin/{user_code}",
            params=params,
            headers={"User-Agent": "emby-simkl-suite/1.0"},
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("result") == "OK" and data.get("access_token"):
                return data
            return None  # still pending
        return None

    # ------------------------------------------------------------------
    # User info
    # ------------------------------------------------------------------

    async def get_me(self) -> dict:
        """Get authenticated user's profile and settings."""
        return await self._post("/users/settings")

    # ------------------------------------------------------------------
    # Ratings
    # ------------------------------------------------------------------

    async def get_user_ratings(self, kind: str = "all") -> list[dict]:
        """Fetch user's rated items with extended metadata (genres, runtime).
        kind: 'movies', 'shows', 'anime', or 'all'.
        Calls GET /sync/ratings/{type} — rating value is in each item."""
        results = []
        types = ["movies", "shows", "anime"] if kind == "all" else [kind]
        for item_type in types:
            try:
                data = await self._get(
                    f"/sync/ratings/{item_type}/1,2,3,4,5,6,7,8,9,10",
                    params={"extended": "full"},
                )
                items_list = []
                if isinstance(data, list):
                    items_list = data
                elif isinstance(data, dict):
                    # Handle dict wrapper (same as _unwrap_sync_response)
                    items_list = (
                        data.get(item_type)
                        or data.get("items")
                        or data.get("results")
                        or data.get("data")
                    ) or []
                    if not isinstance(items_list, list):
                        log.debug("simkl.ratings_unexpected_format",
                                  type=item_type, keys=list(data.keys())[:10])
                        items_list = []

                for item in items_list:
                    item["_type"] = item_type
                    # Try multiple field names for the user's rating
                    if "rating" not in item or item["rating"] is None:
                        item["rating"] = (
                            item.get("user_rating")
                            or item.get("rate")
                            or 0
                        )
                    # Also check if rating is inside the movie/show wrapper
                    if item["rating"] == 0:
                        inner = item.get("movie") or item.get("show") or {}
                        if isinstance(inner, dict):
                            item["rating"] = (
                                inner.get("user_rating")
                                or inner.get("rating")
                                or inner.get("rate")
                                or 0
                            )
                results.extend(items_list)
                log.debug("simkl.ratings_fetched", type=item_type,
                          count=len(items_list))
            except Exception as e:
                log.warning("simkl.ratings_fetch_failed",
                            type=item_type, error=str(e)[:120])
        return results

    # ------------------------------------------------------------------
    # Watchlist
    # ------------------------------------------------------------------

    async def get_watchlist(self, kind: str = "all") -> list[dict]:
        """Fetch user's watchlist (plantowatch status) with extended metadata."""
        results = []
        types = ["movies", "shows", "anime"] if kind == "all" else [kind]
        for item_type in types:
            try:
                endpoint = f"/sync/all-items/{item_type}/plantowatch"
                activity_key = self._ACTIVITY_KEYS.get(f"{item_type}/plantowatch", "all")
                data = await self._activity_gated_get(
                    endpoint, activity_key,
                    params={"extended": "full"},
                )
                items = self._unwrap_sync_response(data, item_type)
                if items:
                    results.extend(items)
                    first = items[0]
                    first_ids = first.get("ids", {})
                    log.debug("simkl.watchlist_fetched", kind=item_type,
                              count=len(items),
                              first_title=str(first.get("title", "?"))[:40],
                              has_tmdb=bool(first_ids.get("tmdb")),
                              has_imdb=bool(first_ids.get("imdb")))
                else:
                    log.debug("simkl.watchlist_empty", kind=item_type)
            except Exception as e:
                log.warning("simkl.watchlist_fetch_failed", kind=item_type,
                            error=str(e)[:120])
        return results

    async def add_to_watchlist(self, items: list[dict] | None = None,
                              movies: list[dict] | None = None,
                              shows: list[dict] | None = None) -> dict:
        """Add items to watchlist (plantowatch status).
        Accepts either a flat items list or movies=/shows= kwargs."""
        if items:
            payload = self._build_sync_payload(items)
        else:
            payload = {}
            if movies:
                payload["movies"] = movies
            if shows:
                payload["shows"] = shows
        # Set destination status
        for key in ("movies", "shows", "anime"):
            if key in payload:
                for item in payload[key]:
                    item["to"] = "plantowatch"
        return await self._post("/sync/add-to-list", payload)

    async def remove_from_watchlist(self, items: list[dict]) -> dict:
        """Remove items from watchlist."""
        payload = self._build_sync_payload(items)
        for key in ("movies", "shows", "anime"):
            if key in payload:
                for item in payload[key]:
                    item["to"] = "notinteresting"
        return await self._post("/sync/add-to-list", payload)

    # ------------------------------------------------------------------
    # History & watched status
    # ------------------------------------------------------------------

    async def get_history(self, kind: str = "all", limit: int = 100, page: int = 1) -> list[dict]:
        """Fetch watched history (completed items) with extended metadata."""
        results = []
        types = ["movies", "shows", "anime"] if kind == "all" else [kind]
        for item_type in types:
            try:
                endpoint = f"/sync/all-items/{item_type}/completed"
                activity_key = self._ACTIVITY_KEYS.get(f"{item_type}/completed", "all")
                data = await self._activity_gated_get(
                    endpoint, activity_key,
                    params={"extended": "full"},
                )
                items = self._unwrap_sync_response(data, item_type)
                results.extend(items)
            except Exception:
                pass
        return results

    async def get_watched(self, kind: str = "shows") -> list[dict]:
        """Fetch all watched items of a given type with extended metadata."""
        try:
            endpoint = f"/sync/all-items/{kind}/completed"
            activity_key = self._ACTIVITY_KEYS.get(f"{kind}/completed", "all")
            data = await self._activity_gated_get(
                endpoint, activity_key,
                params={"extended": "full"},
            )
            return self._unwrap_sync_response(data, kind)
        except Exception:
            return []

    async def get_watched_episode_ids(self, max_pages: int = 20) -> set[str]:
        """Get set of watched episode IDs (IMDB format).
        Parses episodes from /sync/all-items/shows/completed response.
        Falls back to show-level IMDB IDs if episode data isn't present."""
        watched = set()
        try:
            data = await self._activity_gated_get(
                "/sync/all-items/shows/completed",
                "tv_shows.completed",
                params={"extended": "full"},
            )
            items = self._unwrap_sync_response(data, "shows")
            for show in items:
                show_obj = show.get("show") or show
                show_imdb = show_obj.get("ids", {}).get("imdb")
                # Try to extract per-episode IDs from seasons structure
                seasons = show.get("seasons", [])
                if seasons:
                    for season in seasons:
                        for ep in season.get("episodes", []):
                            ep_imdb = ep.get("ids", {}).get("imdb")
                            if ep_imdb:
                                watched.add(ep_imdb)
                            # Also build SxxExx-style keys for matching
                            s_num = season.get("number", 0)
                            e_num = ep.get("number", 0)
                            if show_imdb and s_num and e_num:
                                watched.add(f"{show_imdb}:S{s_num:02d}E{e_num:02d}")
                # Always include show-level IMDB as fallback
                if show_imdb:
                    watched.add(show_imdb)
            log.debug("simkl.watched_episode_ids", count=len(watched),
                       shows=len(items))
        except Exception as e:
            log.warning("simkl.watched_episode_ids_failed", error=str(e)[:120])
        return watched

    @staticmethod
    def _unwrap_sync_response(data, kind: str) -> list[dict]:
        """Simkl /sync/all-items returns either a bare list or a dict wrapper.
        This normalises both to a list."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            items = (
                data.get(kind)
                or data.get("movies")
                or data.get("shows")
                or data.get("anime")
                or data.get("items")
                or data.get("results")
                or data.get("data")
            )
            if isinstance(items, list):
                return items
            log.warning("simkl.unwrap_unexpected_keys", kind=kind,
                        keys=list(data.keys())[:10])
        return []

    async def check_watched(self, items: list[dict]) -> list[dict]:
        """Bulk lookup: have I watched these items?
        POST /sync/watched with list of items."""
        return await self._post("/sync/watched", items)

    async def add_to_history(self, items: list[dict]) -> dict:
        """Mark items as watched. POST /sync/history."""
        payload = self._build_sync_payload(items)
        return await self._post("/sync/history", payload)

    async def remove_from_history(self, items: list[dict]) -> dict:
        """Remove items from watched history."""
        payload = self._build_sync_payload(items)
        return await self._post("/sync/history/remove", payload)

    # ------------------------------------------------------------------
    # Sync activities (cheap "anything changed?" check)
    # ------------------------------------------------------------------

    async def get_activities(self) -> dict:
        """GET /sync/activities — last-modified timestamps per category."""
        return await self._get("/sync/activities")

    # ------------------------------------------------------------------
    # Activity-gated sync (Simkl compliance: Rule 7)
    # ------------------------------------------------------------------
    # Never call /sync/all-items without first checking /sync/activities.
    # This two-phase pattern avoids unconditional full-dataset fetches
    # that can get a client_id suspended.
    #
    # Flow:
    #   1. GET /sync/activities (cached 60s to avoid repeated calls)
    #   2. Compare relevant timestamp against stored value in Redis
    #   3. If unchanged → return cached response data
    #   4. If changed → fetch fresh, cache response, store new timestamp
    # ------------------------------------------------------------------

    # Map sync endpoints → activity response key paths
    _ACTIVITY_KEYS: dict[str, str] = {
        "movies/completed":    "movies.completed",
        "shows/completed":     "tv_shows.completed",
        "anime/completed":     "anime.completed",
        "movies/plantowatch":  "movies.plantowatch",
        "shows/plantowatch":   "tv_shows.plantowatch",
        "anime/plantowatch":   "anime.plantowatch",
        "movies/watching":     "movies.watching",
        "shows/watching":      "tv_shows.watching",
        "anime/watching":      "anime.watching",
    }

    def _cache_prefix(self) -> str:
        """Stable per-token prefix for Redis keys."""
        import hashlib
        t = self._access_token or ""
        return hashlib.md5(t.encode()).hexdigest()[:12]

    async def _get_activities_cached(self) -> dict:
        """Get activities, cached 60s in Redis to avoid repeated calls
        when multiple sync methods run in the same job."""
        import json as _json
        try:
            from app.utils.redis_cache import get_redis
            r = await get_redis()
            key = f"simkl_activities_cache:{self._cache_prefix()}"
            cached = await r.get(key)
            if cached:
                return _json.loads(cached)
        except Exception:
            pass

        activities = await self.get_activities()

        try:
            from app.utils.redis_cache import get_redis
            r = await get_redis()
            key = f"simkl_activities_cache:{self._cache_prefix()}"
            await r.set(key, _json.dumps(activities), ex=60)
        except Exception:
            pass

        return activities

    @staticmethod
    def _extract_activity_ts(activities: dict, dotted_key: str) -> str | None:
        """Extract a timestamp from nested activities dict.
        e.g. 'movies.completed' → activities['movies']['completed']"""
        parts = dotted_key.split(".")
        obj: Any = activities
        for p in parts:
            if isinstance(obj, dict):
                obj = obj.get(p)
            else:
                return None
        return obj if isinstance(obj, str) else None

    async def _activity_gated_get(
        self, endpoint: str, activity_key: str,
        params: dict | None = None,
    ) -> Any:
        """Fetch a /sync/all-items/* endpoint with activity gating.

        Checks /sync/activities first. If the relevant timestamp hasn't
        changed since last fetch, returns cached data from Redis.
        Otherwise fetches fresh, caches, and stores the new timestamp.
        """
        import json as _json

        prefix = self._cache_prefix()
        data_cache_key = f"simkl_sync_data:{prefix}:{activity_key}"
        ts_store_key = f"simkl_sync_ts:{prefix}:{activity_key}"

        try:
            from app.utils.redis_cache import get_redis
            r = await get_redis()

            # Step 1: check activities
            activities = await self._get_activities_cached()
            current_ts = self._extract_activity_ts(activities, activity_key)

            # Step 2: compare against stored timestamp
            stored_ts = await r.get(ts_store_key)
            if stored_ts:
                stored_ts = stored_ts if isinstance(stored_ts, str) else stored_ts.decode()

            if current_ts and stored_ts and current_ts == stored_ts:
                # Nothing changed — try to return cached data
                cached = await r.get(data_cache_key)
                if cached:
                    log.debug("simkl.activity_gate_cache_hit",
                              endpoint=endpoint, activity_key=activity_key)
                    return _json.loads(cached)

            # Step 3: fetch fresh data
            log.debug("simkl.activity_gate_fetching",
                       endpoint=endpoint, activity_key=activity_key,
                       current_ts=current_ts, stored_ts=stored_ts)
            data = await self._get(endpoint, params=params)

            # Cache response (24h TTL) and store timestamp
            await r.set(data_cache_key, _json.dumps(data), ex=86400)
            if current_ts:
                await r.set(ts_store_key, current_ts, ex=86400)

            return data

        except Exception as e:
            # If Redis fails or activities check fails, fall back to direct fetch
            # but log the issue so we know activity gating is broken
            if "401" in str(e) or "429" in str(e):
                raise  # auth/rate errors should propagate
            log.warning("simkl.activity_gate_fallback",
                        endpoint=endpoint, error=str(e)[:120])
            return await self._get(endpoint, params=params)

    # ------------------------------------------------------------------
    # Trending & discovery (CDN — no auth, no rate limit)
    # ------------------------------------------------------------------

    async def get_trending(self, kind: str = "shows", period: str = "week",
                           limit: int = 100, page: int = 1) -> list[dict]:
        """Fetch trending items from CDN. No auth needed.
        kind: 'tv', 'movies', 'anime'. period: 'today', 'week', 'month'.
        CDN filenames follow pattern: {period}_100.json (e.g. today_100.json).
        limit/page accepted for Trakt-compat but CDN returns a fixed 100-item file."""
        # Map our kind names to Simkl CDN path names
        type_map = {"shows": "tv", "movies": "movies", "anime": "anime"}
        simkl_type = type_map.get(kind, kind)
        try:
            return await self._get_cdn(f"/discover/trending/{simkl_type}/{period}_100.json")
        except Exception as e:
            log.warning("simkl.trending_failed", kind=kind, error=str(e)[:120])
            return []

    async def get_popular(self, kind: str = "shows", limit: int = 20) -> list[dict]:
        """Fetch popular items (uses monthly trending)."""
        return await self.get_trending(kind=kind, period="month")

    # ------------------------------------------------------------------
    # Calendar (CDN — no auth, 33-day rolling)
    # ------------------------------------------------------------------

    async def get_calendar_shows(self) -> list[dict]:
        """CDN calendar: upcoming TV episodes (33-day rolling)."""
        try:
            return await self._get_cdn("/calendar/tv.json")
        except Exception as e:
            log.warning("simkl.calendar_shows_failed", error=str(e)[:120])
            return []

    async def get_calendar_movies(self) -> list[dict]:
        """CDN calendar: upcoming movie releases (33-day rolling)."""
        try:
            return await self._get_cdn("/calendar/movie_release.json")
        except Exception as e:
            log.warning("simkl.calendar_movies_failed", error=str(e)[:120])
            return []

    async def get_calendar_anime(self) -> list[dict]:
        """CDN calendar: upcoming anime episodes."""
        try:
            return await self._get_cdn("/calendar/anime.json")
        except Exception as e:
            log.warning("simkl.calendar_anime_failed", error=str(e)[:120])
            return []

    # ------------------------------------------------------------------
    # TV shows
    # ------------------------------------------------------------------

    async def get_tv_detail(self, simkl_id: str) -> dict:
        """Full show detail record."""
        return await self._get(f"/tv/{simkl_id}", auth_required=False)

    async def get_tv_episodes(self, simkl_id: str) -> list[dict]:
        """Full episode list for a show."""
        return await self._get(f"/tv/episodes/{simkl_id}", auth_required=False)

    async def get_tv_premieres(self, param: str = "new") -> list[dict]:
        """TV premieres — 'new' or 'soon'. No auth. Paginated (default 60, max 20 pages)."""
        all_items: list[dict] = []
        page = 1
        max_pages = 20
        try:
            while page <= max_pages:
                resp = await self._client.get(
                    f"{BASE_URL}/tv/premieres/{param}",
                    params={**self._base_params(), "page": page, "limit": 60},
                    headers={"User-Agent": "emby-simkl-suite/1.0"},
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    all_items.extend(data)
                else:
                    break
                total_pages = int(resp.headers.get("X-Pagination-Page-Count", "1"))
                if page >= total_pages:
                    break
                page += 1
        except Exception:
            pass
        return all_items

    async def get_tv_airing(self, date: str | None = None) -> list[dict]:
        """What's airing today/tomorrow/on a date. No auth."""
        params = {}
        if date:
            params["date"] = date
        try:
            return await self._get("/tv/airing", params=params, auth_required=False)
        except Exception:
            return []

    async def get_show_seasons(self, simkl_id: str) -> list[dict]:
        """Get episode list to derive season info.
        Returns episodes grouped by season."""
        episodes = await self.get_tv_episodes(simkl_id)
        # Group into seasons with episode counts for finale detection
        seasons: dict[int, dict] = {}
        for ep in (episodes if isinstance(episodes, list) else []):
            s = ep.get("season")
            if s is not None:
                if s not in seasons:
                    seasons[s] = {"number": s, "episode_count": 0}
                seasons[s]["episode_count"] += 1
        return list(seasons.values())

    # ------------------------------------------------------------------
    # Movies
    # ------------------------------------------------------------------

    async def get_movie_detail(self, simkl_id: str) -> dict:
        """Full movie detail record (includes release dates, similar movies)."""
        return await self._get(f"/movies/{simkl_id}", auth_required=False)

    async def get_movie_releases(self, simkl_id: str, country: str = "us") -> list[dict]:
        """Get movie release dates by region from detail endpoint."""
        detail = await self.get_movie_detail(simkl_id)
        return detail.get("release_dates", [])

    async def get_similar_movies(self, simkl_id: str) -> list[dict]:
        """Get movie recommendations from detail endpoint."""
        detail = await self.get_movie_detail(simkl_id)
        return detail.get("users_recommendations", [])

    async def get_similar_shows(self, simkl_id: str) -> list[dict]:
        """Get show recommendations from TV detail endpoint."""
        detail = await self.get_tv_detail(simkl_id)
        return detail.get("users_recommendations", [])

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: str = "movie") -> list[dict]:
        """Text search. kind: 'movie', 'tv', 'anime'."""
        type_map = {"movies": "movie", "shows": "tv"}
        simkl_type = type_map.get(kind, kind)
        return await self._get(f"/search/{simkl_type}", params={"q": query}, auth_required=False)

    async def search_by_id(self, id_type: str, id_value: str) -> list[dict]:
        """Lookup by external ID. id_type: 'imdb', 'tmdb', 'tvdb', 'mal', etc."""
        params = {id_type: id_value}
        try:
            return await self._get("/search/id", params=params, auth_required=False)
        except Exception:
            return []

    async def search_random(self, kind: str = "movie", **filters) -> dict:
        """Random item pick. With bearer token, skips already-watched.
        Optional filters: genre, year_from, year_to, type, etc."""
        payload = {"type": kind, **filters}
        return await self._post(
            "/search/random", payload,
            auth_required=bool(self._access_token),
        )

    # ------------------------------------------------------------------
    # Scrobble
    # ------------------------------------------------------------------

    def _build_scrobble_payload(self, item_payload: dict, progress: float) -> dict:
        """Build scrobble body. Caps progress to 2 decimal places."""
        body = dict(item_payload)
        body["progress"] = round(progress, 2)
        return body

    async def scrobble_start(self, item_payload: dict, progress: float) -> dict:
        """POST /scrobble/start — begin or resume playback."""
        body = self._build_scrobble_payload(item_payload, progress)
        return await self._post("/scrobble/start", body)

    async def scrobble_pause(self, item_payload: dict, progress: float) -> dict:
        """POST /scrobble/pause — save progress for cross-device resume."""
        body = self._build_scrobble_payload(item_payload, progress)
        return await self._post("/scrobble/pause", body)

    async def scrobble_stop(self, item_payload: dict, progress: float) -> dict:
        """POST /scrobble/stop — end session. ≥80% marks watched."""
        body = self._build_scrobble_payload(item_payload, progress)
        return await self._post("/scrobble/stop", body)

    async def checkin(self, item_payload: dict, message: str = "") -> dict:
        """POST /scrobble/checkin — fire-and-forget, auto-completes at runtime."""
        return await self._post("/scrobble/checkin", item_payload)

    # ------------------------------------------------------------------
    # Playback (cross-device resume)
    # ------------------------------------------------------------------

    async def get_playback(self, kind: str = "all") -> list[dict]:
        """Get saved paused playbacks."""
        if kind == "all":
            path = "/sync/playback"
        else:
            type_map = {"shows": "episodes", "movies": "movies"}
            path = f"/sync/playback/{type_map.get(kind, kind)}"
        try:
            return await self._get(path)
        except Exception:
            return []

    async def delete_playback(self, playback_id: int) -> None:
        """Remove a saved playback session."""
        await self._delete(f"/sync/playback/{playback_id}")

    # ------------------------------------------------------------------
    # Ratings (write)
    # ------------------------------------------------------------------

    async def add_ratings(self, items: list[dict]) -> dict:
        """Rate items 1-10. POST /sync/ratings."""
        payload = self._build_sync_payload(items)
        return await self._post("/sync/ratings", payload)

    async def remove_ratings(self, items: list[dict]) -> dict:
        """Clear user ratings. POST /sync/ratings/remove."""
        payload = self._build_sync_payload(items)
        return await self._post("/sync/ratings/remove", payload)

    # ------------------------------------------------------------------
    # Community ratings (for watchlist items)
    # ------------------------------------------------------------------

    async def get_watchlist_ratings(self, kind: str = "movies") -> list[dict]:
        """Community ratings for items in user's watchlist."""
        try:
            return await self._get(f"/ratings/{kind}")
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_sync_payload(items: list[dict]) -> dict:
        """Group items into movies/shows/anime arrays for sync endpoints."""
        movies = []
        shows = []
        anime = []
        for item in items:
            # Strip internal routing keys before building payload
            clean = {k: v for k, v in item.items() if not k.startswith("_")}

            if "movie" in item:
                movies.append(clean["movie"] if isinstance(clean.get("movie"), dict) else clean)
            elif "show" in item:
                shows.append(clean)
            elif "anime" in item:
                anime.append(clean)
            elif item.get("_type") in ("movie", "movies") or item.get("type") == "movie":
                movies.append(clean)
            elif item.get("_type") in ("show", "shows"):
                shows.append(clean)
            elif item.get("_type") == "anime":
                anime.append(clean)
            else:
                # Default to movie
                movies.append(clean)
        payload = {}
        if movies:
            payload["movies"] = movies
        if shows:
            payload["shows"] = shows
        if anime:
            payload["anime"] = anime
        return payload

    @staticmethod
    def build_movie_payload(title: str, year: int | None = None,
                            ids: dict | None = None) -> dict:
        """Build a movie object for scrobble/sync calls."""
        movie: dict = {}
        if title:
            movie["title"] = title
        if year:
            movie["year"] = year
        if ids:
            movie["ids"] = ids
        return {"movie": movie}

    @staticmethod
    def build_show_payload(title: str, year: int | None = None,
                           ids: dict | None = None,
                           season: int | None = None,
                           episode: int | None = None) -> dict:
        """Build a show+episode object for scrobble/sync calls."""
        show: dict = {}
        if title:
            show["title"] = title
        if year:
            show["year"] = year
        if ids:
            show["ids"] = ids
        result: dict = {"show": show}
        if season is not None and episode is not None:
            result["episode"] = {"season": season, "number": episode}
        return result

    @staticmethod
    def build_episode_payload(show_ids: dict,
                              season: int, episode: int) -> dict:
        """Build an episode-level payload for scrobble calls."""
        return {
            "show": {"ids": show_ids},
            "episode": {"season": season, "number": episode},
        }

    async def test_connection(self) -> bool:
        """Quick connectivity check — try fetching trending (CDN, no auth)."""
        try:
            data = await self.get_trending(kind="movies", period="today")
            return isinstance(data, list) and len(data) > 0
        except Exception:
            return False

    async def test_auth(self) -> bool:
        """Check if the current token is valid."""
        try:
            me = await self.get_me()
            return bool(me.get("user", {}).get("name") or me.get("account"))
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Compatibility stubs — Trakt methods not available in Simkl
    # Return empty results so callers degrade gracefully.
    # ------------------------------------------------------------------

    async def get_show(self, simkl_id: str, **kw) -> dict:
        """Alias for get_tv_detail."""
        return await self.get_tv_detail(simkl_id)

    async def get_movie(self, simkl_id: str, **kw) -> dict:
        """Alias for get_movie_detail."""
        return await self.get_movie_detail(simkl_id)

    async def get_related(self, kind: str, simkl_id: str, **kw) -> list[dict]:
        """Get related/similar items from Simkl detail endpoint."""
        if kind == "movies":
            return await self.get_similar_movies(simkl_id)
        if kind == "shows":
            return await self.get_similar_shows(simkl_id)
        return []

    async def get_movie_related(self, simkl_id: str, **kw) -> list[dict]:
        """Alias for get_similar_movies."""
        return await self.get_similar_movies(simkl_id)

    async def get_recommended(self, kind: str = "movies", limit: int = 20,
                              **kw) -> list[dict]:
        """Get recommendations using Simkl's random search with bearer token
        (skips already-watched items). Falls back to monthly trending."""
        results = []
        simkl_type = {"movies": "movie", "shows": "tv"}.get(kind, kind)
        try:
            # search/random with bearer skips watched items — call multiple times
            for _ in range(min(limit, 20)):
                item = await self._post(
                    "/search/random",
                    {"type": simkl_type},
                    auth_required=bool(self._access_token),
                )
                if item and isinstance(item, dict):
                    results.append(item)
        except Exception:
            pass
        # If random search gave few results, pad with trending
        if len(results) < limit:
            try:
                trending = await self.get_trending(kind=kind, period="month")
                for t in trending:
                    if len(results) >= limit:
                        break
                    results.append(t)
            except Exception:
                pass
        return results[:limit]

    async def get_item_details(self, kind: str, simkl_id: str, **kw) -> dict:
        """Fetch detail for a movie or show."""
        if kind in ("movies", "movie"):
            return await self.get_movie_detail(simkl_id)
        return await self.get_tv_detail(simkl_id)

    async def get_my_shows(self, **kw) -> list[dict]:
        """Fetch user's actively-watched shows (watching status).
        Cross-references with CDN calendar for airing info when possible."""
        try:
            data = await self._activity_gated_get(
                "/sync/all-items/shows/watching",
                "tv_shows.watching",
                params={"extended": "full"},
            )
            return data if isinstance(data, list) else []
        except Exception:
            return []

    async def get_my_movies(self, **kw) -> list[dict]:
        """Fetch user's plan-to-watch movies."""
        try:
            data = await self._activity_gated_get(
                "/sync/all-items/movies/plantowatch",
                "movies.plantowatch",
                params={"extended": "full"},
            )
            return data if isinstance(data, list) else []
        except Exception:
            return []

    async def get_my_premieres(self, **kw) -> list[dict]:
        """Fetch upcoming TV premieres (new + soon).
        Returns combined list from /tv/premieres/new and /tv/premieres/soon.
        Each entry is reshaped to {show, episode, first_aired} format
        expected by airing_alerts._merge_entry."""
        results = []
        for param in ("new", "soon"):
            try:
                data = await self.get_tv_premieres(param)
                if isinstance(data, list):
                    for item in data:
                        # Simkl premiere endpoints return flat show objects:
                        # {title, ids, year, first_aired, ...}
                        # Reshape to match the structure _merge_entry expects
                        if "show" in item and isinstance(item["show"], dict):
                            # Already in {show, episode} format — pass through
                            results.append(item)
                        else:
                            # Flat object — wrap it
                            results.append({
                                "show": {
                                    "title": item.get("title", ""),
                                    "ids": item.get("ids", {}),
                                    "year": item.get("year"),
                                },
                                "episode": {
                                    "season": 1,
                                    "number": 1,
                                    "title": item.get("episode_title")
                                            or item.get("title", ""),
                                },
                                "first_aired": (
                                    item.get("first_aired")
                                    or item.get("date")
                                    or item.get("air_date")
                                    or ""
                                ),
                            })
            except Exception:
                pass
        return results

    async def get_my_lists(self, **kw) -> list[dict]:
        """Simkl has no user-lists endpoint. Return empty."""
        return []

    async def get_popular_lists(self, **kw) -> list[dict]:
        """Simkl has no public lists. Return empty."""
        return []

    async def get_trending_lists(self, **kw) -> list[dict]:
        """Simkl has no trending lists. Return empty."""
        return []

    async def get_liked_lists(self, **kw) -> list[dict]:
        """Simkl has no liked lists. Return empty."""
        return []

    async def get_list_items(self, list_id: str, **kw) -> list[dict]:
        """Simkl has no public lists. Return empty."""
        return []

    async def get_friends(self, **kw) -> list[dict]:
        """Simkl has no friends API. Return empty."""
        return []

    async def get_friend_ratings(self, **kw) -> list[dict]:
        """Simkl has no friends API. Return empty."""
        return []

    async def get_collaborations(self, **kw) -> list[dict]:
        """Simkl has no collaborations. Return empty."""
        return []

    async def post_comment(self, **kw) -> dict:
        """Simkl has no comments API. Return empty."""
        return {}

    def _update_rate_limit(self, *args, **kw) -> None:
        """No-op — Simkl doesn't expose rate-limit headers like Trakt."""
        pass

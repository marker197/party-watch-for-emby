"""Async MDBList API client.

Full integration client covering:
  - User info / health
  - Lists (user, liked, top, search, items, create, modify)
  - Watchlist (get, add, remove)
  - Sync: watched history, ratings, collection, playback, dropped, last_activities
  - Scrobble (start, pause, stop, clear)
  - Check-in (start, get, update, stop)
  - Up Next
  - Search / media info

Auth: API key (query param) or OAuth Bearer token (header).
API base: https://api.mdblist.com
Docs: https://mdblist.docs.apiary.io
Free tier: 1000 requests/day.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Awaitable

import httpx
import structlog

log = structlog.get_logger()

BASE_URL = "https://api.mdblist.com"


class MDBListClient:
    """Async MDBList client — lists, sync, scrobble, watchlist.

    Supports two auth modes:
      - API key: passed via query param (?apikey=...)
      - OAuth Bearer: passed via Authorization header

    When OAuth tokens are provided, the client uses Bearer auth and handles
    token refresh automatically (similar to TraktClient pattern).
    """

    def __init__(
        self,
        api_key: str | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
        token_expires: datetime | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_refresh_callback: Callable[[str, str, datetime], Awaitable[None]] | None = None,
    ):
        self._key = api_key
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._token_expires = token_expires
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_refresh_callback = token_refresh_callback
        self._refresh_attempted = False

        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=20.0,
        )

        # Rate limiting
        self._rate_limit_remaining = 1000
        self._rate_limit_reset = time.time() + 86400

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def close(self):
        await self._client.aclose()

    # -- Auth helpers -------------------------------------------------------

    def _params(self, extra: dict | None = None) -> dict:
        p = {}
        if self._key and not self._access_token:
            p["apikey"] = self._key
        if extra:
            p.update(extra)
        return p

    def _auth_headers(self) -> dict:
        if self._access_token:
            return {"Authorization": f"Bearer {self._access_token}"}
        return {}

    async def _ensure_token_valid(self) -> None:
        """Proactively refresh OAuth token if close to expiry (within 5 min)."""
        if not self._refresh_token or not self._token_expires:
            return
        if not self._client_id or not self._client_secret:
            return

        now = datetime.now(timezone.utc)
        expires = self._token_expires
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        time_until_expiry = (expires - now).total_seconds()

        if time_until_expiry < 300:
            log.info("mdblist.token_refresh", seconds_until_expiry=time_until_expiry)
            try:
                token_data = await self._do_refresh_token()
                self._access_token = token_data["access_token"]
                self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                self._token_expires = datetime.now(timezone.utc) + timedelta(
                    seconds=token_data.get("expires_in", 2592000)
                )
                if self._token_refresh_callback:
                    await self._token_refresh_callback(
                        self._access_token, self._refresh_token, self._token_expires,
                    )
                log.info("mdblist.token_refreshed", new_expiry=self._token_expires)
            except Exception as e:
                log.error("mdblist.token_refresh_failed", error=str(e))
                raise

    async def _do_refresh_token(self) -> dict:
        """Exchange refresh token for new access token."""
        resp = await self._client.post(
            "/oauth/token/",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        if resp.status_code == 400:
            log.error("mdblist.refresh_token_rejected", status=400,
                      hint="Refresh token stale or revoked — user must re-link MDBList account")
        resp.raise_for_status()
        return resp.json()

    # -- Rate limiting ------------------------------------------------------

    def _update_rate_limit(self, resp: httpx.Response) -> None:
        try:
            remaining = int(resp.headers.get("X-RateLimit-Remaining", self._rate_limit_remaining))
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", self._rate_limit_reset))
            self._rate_limit_remaining = remaining
            self._rate_limit_reset = float(reset_ts)
            if remaining < 50:
                log.warning("mdblist.rate_limit_low", remaining=remaining)
        except (ValueError, TypeError):
            pass

    MAX_RATE_LIMIT_WAIT = 60

    async def _wait_for_rate_limit(self) -> None:
        if self._rate_limit_reset > time.time():
            sleep_time = self._rate_limit_reset - time.time() + 1
            if sleep_time > self.MAX_RATE_LIMIT_WAIT:
                from fastapi import HTTPException
                raise HTTPException(429, f"MDBList daily rate limit exceeded")
            log.warning("mdblist.rate_limit_wait", sleep_seconds=round(sleep_time))
            await asyncio.sleep(sleep_time)

    def get_rate_limit_info(self) -> dict:
        now = time.time()
        return {
            "remaining": self._rate_limit_remaining,
            "reset_timestamp": self._rate_limit_reset,
            "seconds_until_reset": max(0, self._rate_limit_reset - now),
        }

    # -- Error sanitization ---------------------------------------------------

    @staticmethod
    def _sanitize_error(e: httpx.HTTPStatusError) -> httpx.HTTPStatusError:
        """Remove API key from httpx error messages to prevent log leaks."""
        import re as _re
        sanitized_msg = _re.sub(r'apikey=[^&\s\'"]+', 'apikey=***', str(e))
        return httpx.HTTPStatusError(sanitized_msg, request=e.request, response=e.response)

    # -- HTTP helpers -------------------------------------------------------

    async def _get(self, path: str, params: dict | None = None, max_retries: int = 3) -> Any:
        self._refresh_attempted = False
        await self._ensure_token_valid()

        for attempt in range(max_retries):
            try:
                resp = await self._client.get(
                    path, headers=self._auth_headers(), params=self._params(params),
                )
                self._update_rate_limit(resp)

                if resp.status_code == 429:
                    await self._wait_for_rate_limit()
                    continue
                if resp.status_code == 401 and self._refresh_token and not self._refresh_attempted:
                    self._refresh_attempted = True
                    try:
                        token_data = await self._do_refresh_token()
                        self._access_token = token_data["access_token"]
                        self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                        self._token_expires = datetime.now(timezone.utc) + timedelta(
                            seconds=token_data.get("expires_in", 2592000))
                        if self._token_refresh_callback:
                            await self._token_refresh_callback(
                                self._access_token, self._refresh_token, self._token_expires)
                        continue
                    except Exception:
                        pass

                resp.raise_for_status()
                return resp.json()
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise
            except httpx.HTTPStatusError as e:
                if 500 <= e.response.status_code < 600 and attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise self._sanitize_error(e) from None

    async def _post(self, path: str, body: dict | None = None, max_retries: int = 3) -> Any:
        self._refresh_attempted = False
        await self._ensure_token_valid()

        for attempt in range(max_retries):
            try:
                resp = await self._client.post(
                    path, headers=self._auth_headers(), params=self._params(),
                    json=body or {},
                )
                self._update_rate_limit(resp)

                if resp.status_code == 429:
                    await self._wait_for_rate_limit()
                    continue
                if resp.status_code == 401 and self._refresh_token and not self._refresh_attempted:
                    self._refresh_attempted = True
                    try:
                        token_data = await self._do_refresh_token()
                        self._access_token = token_data["access_token"]
                        self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                        if self._token_refresh_callback:
                            await self._token_refresh_callback(
                                self._access_token, self._refresh_token,
                                datetime.now(timezone.utc) + timedelta(seconds=token_data.get("expires_in", 2592000)))
                        continue
                    except Exception:
                        pass

                resp.raise_for_status()
                # Some endpoints return 204 No Content
                if resp.status_code == 204:
                    return {}
                return resp.json()
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise
            except httpx.HTTPStatusError as e:
                if 500 <= e.response.status_code < 600 and attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise self._sanitize_error(e) from None

    async def _patch(self, path: str, body: dict | None = None) -> Any:
        await self._ensure_token_valid()
        resp = await self._client.patch(
            path, headers=self._auth_headers(), params=self._params(),
            json=body or {},
        )
        self._update_rate_limit(resp)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise self._sanitize_error(e) from None
        if resp.status_code == 204:
            return {}
        return resp.json()

    async def _delete(self, path: str, body: dict | None = None) -> Any:
        await self._ensure_token_valid()
        kwargs: dict = {"headers": self._auth_headers(), "params": self._params()}
        if body:
            kwargs["json"] = body
        resp = await self._client.delete(path, **kwargs)
        self._update_rate_limit(resp)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise self._sanitize_error(e) from None
        if resp.status_code == 204:
            return {}
        try:
            return resp.json()
        except Exception:
            return {}

    # ═══════════════════════════════════════════════════════════════════════
    # Health / test
    # ═══════════════════════════════════════════════════════════════════════

    async def test_connection(self) -> dict:
        """Test auth by fetching user info."""
        try:
            data = await self._get("/user")
            return {
                "status": "ok",
                "username": data.get("username", ""),
                "user_id": data.get("user_id"),
                "requests_remaining": data.get("rate_limit_remaining", 0),
                "requests_limit": data.get("api_requests", 0),
                "plan": data.get("plan", "Free"),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                return {"status": "error", "message": "Invalid API key or token"}
            if e.response.status_code == 429:
                return {"status": "error", "message": "Rate limit exceeded"}
            return {"status": "error", "message": f"HTTP {e.response.status_code}"}
        except Exception as e:
            return {"status": "error", "message": str(e)[:200]}

    # ═══════════════════════════════════════════════════════════════════════
    # User info
    # ═══════════════════════════════════════════════════════════════════════

    async def get_user(self) -> dict:
        return await self._get("/user")

    # ═══════════════════════════════════════════════════════════════════════
    # Lists
    # ═══════════════════════════════════════════════════════════════════════

    async def get_my_lists(self) -> list[dict]:
        return await self._get("/lists/user/")

    async def get_list_by_id(self, list_id: int) -> list[dict]:
        return await self._get(f"/lists/{list_id}")

    async def get_liked_lists(self) -> list[dict]:
        try:
            data = await self._get("/lists/liked")
            if isinstance(data, dict) and "lists" in data:
                return data["lists"]
            if isinstance(data, list):
                return data
            return []
        except Exception:
            return []

    async def get_top_lists(self) -> list[dict]:
        try:
            return await self._get("/lists/top")
        except Exception:
            return []

    async def search_lists(self, query: str) -> list[dict]:
        try:
            return await self._get("/lists/search", params={"query": query})
        except Exception:
            return []

    async def create_static_list(self, name: str, mediatype: str = "movie", private: bool = False) -> dict:
        return await self._post("/lists/user/add", {
            "name": name, "mediatype": mediatype, "private": private,
        })

    async def update_list(self, list_id: int, name: str | None = None, private: bool | None = None) -> dict:
        body: dict = {}
        if name is not None:
            body["name"] = name
        if private is not None:
            body["private"] = private
        resp = await self._client.put(
            f"/lists/{list_id}", headers=self._auth_headers(),
            params=self._params(), json=body,
        )
        resp.raise_for_status()
        return resp.json()

    async def delete_list(self, list_id: int) -> dict:
        return await self._delete(f"/lists/{list_id}")

    # -- List items ---------------------------------------------------------

    async def get_list_items(
        self, list_id: int, limit: int = 1000, offset: int = 0,
        append_to_response: str = "description",
    ) -> dict:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if append_to_response:
            params["append_to_response"] = append_to_response
        return await self._get(f"/lists/{list_id}/items", params)

    async def get_all_list_items(self, list_id: int) -> list[dict]:
        all_items: list[dict] = []
        offset = 0
        limit = 1000
        while True:
            data = await self.get_list_items(
                list_id, limit=limit, offset=offset, append_to_response="description",
            )
            movies = data.get("movies") or []
            shows = data.get("shows") or []
            for m in movies:
                m.setdefault("mediatype", "movie")
                all_items.append(m)
            for s in shows:
                s.setdefault("mediatype", "show")
                all_items.append(s)
            batch_count = len(movies) + len(shows)
            if batch_count < limit:
                break
            offset += limit
        all_items.sort(key=lambda x: x.get("rank", 9999))
        return all_items

    async def modify_static_list_items(self, list_id: int, action: str, items: list[dict]) -> dict:
        """Add or remove items from a static list.
        action: 'add' or 'remove'
        items: list of {"imdb": "tt..."} or {"tmdb": 123}
        """
        return await self._post(f"/lists/{list_id}/items/{action}", {"items": items})

    # ═══════════════════════════════════════════════════════════════════════
    # Watchlist
    # ═══════════════════════════════════════════════════════════════════════

    async def get_watchlist(self, mediatype: str | None = None) -> dict:
        """Fetch user's watchlist items.
        Returns {"movies": [...], "shows": [...]}.
        """
        path = f"/watchlist/items/{mediatype}" if mediatype else "/watchlist/items"
        return await self._get(path)

    async def add_to_watchlist(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        """Add items to watchlist.
        movies: [{"tmdb": 630, "imdb": "tt..."}, ...]
        shows:  [{"imdb": "tt..."}, {"tmdb": 1396}, ...]
        """
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        if not body:
            return {"added": {"movies": 0, "shows": 0}}
        return await self._post("/watchlist/items/add", body)

    async def remove_from_watchlist(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        if not body:
            return {"removed": {"movies": 0, "shows": 0}}
        return await self._post("/watchlist/items/remove", body)

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Watched History
    # ═══════════════════════════════════════════════════════════════════════

    async def get_watched(self, since: str | None = None) -> dict:
        """Retrieve watched history. Returns {"movies": [...], "shows": [...]}."""
        params = {}
        if since:
            params["since"] = since
        return await self._get("/sync/watched", params)

    async def add_to_watched(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        """Add items to watched history.
        movies: [{"ids": {"imdb": "tt..."}, "watched_at": "..."}, ...]
        shows:  [{"ids": {"imdb": "tt..."}, "seasons": [...]}, ...]
        """
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        if not body:
            return {}
        return await self._post("/sync/watched", body)

    async def remove_from_watched(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        return await self._post("/sync/watched/remove", body)

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Ratings
    # ═══════════════════════════════════════════════════════════════════════

    async def get_ratings(self, since: str | None = None) -> dict:
        """Retrieve user ratings. Returns {"movies": [...], "shows": [...], "episodes": [...]}."""
        params = {}
        if since:
            params["since"] = since
        return await self._get("/sync/ratings", params)

    async def add_ratings(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        """Add or update ratings.
        movies: [{"ids": {"tmdb": 123}, "rating": 8, "rated_at": "..."}, ...]
        """
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        return await self._post("/sync/ratings", body)

    async def remove_ratings(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        return await self._post("/sync/ratings/remove", body)

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Collection
    # ═══════════════════════════════════════════════════════════════════════

    async def get_collection(self, since: str | None = None) -> dict:
        params = {}
        if since:
            params["since"] = since
        return await self._get("/sync/collection", params)

    async def add_to_collection(
        self, movies: list[dict] | None = None, shows: list[dict] | None = None,
    ) -> dict:
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        return await self._post("/sync/collection", body)

    async def remove_from_collection(
        self, movies: list[dict] | None = None, shows: list[dict] | None = None,
    ) -> dict:
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        return await self._post("/sync/collection/remove", body)

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Playback (resume points)
    # ═══════════════════════════════════════════════════════════════════════

    async def get_playback(self) -> list[dict]:
        """Returns active paused sessions (saved progress)."""
        return await self._get("/sync/playback")

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Dropped
    # ═══════════════════════════════════════════════════════════════════════

    async def get_dropped(self) -> dict:
        return await self._get("/sync/dropped")

    async def add_dropped(self, shows: list[dict]) -> dict:
        return await self._post("/sync/dropped", {"shows": shows})

    async def remove_dropped(self, shows: list[dict]) -> dict:
        return await self._post("/sync/dropped/remove", {"shows": shows})

    # ═══════════════════════════════════════════════════════════════════════
    # Sync — Last Activities
    # ═══════════════════════════════════════════════════════════════════════

    async def get_last_activities(self) -> dict:
        """Returns latest sync activity timestamps for incremental sync decisions."""
        return await self._get("/sync/last_activities")

    # ═══════════════════════════════════════════════════════════════════════
    # Scrobble
    # ═══════════════════════════════════════════════════════════════════════

    async def scrobble_start(self, item_payload: dict, progress: float) -> dict:
        """Start/resume playback tracking.
        item_payload: {"movie": {"ids": {"imdb": "tt..."}}}
        """
        body = {**item_payload, "progress": round(progress, 1), "app_version": "1.0.0"}
        return await self._post("/scrobble/start", body)

    async def scrobble_pause(self, item_payload: dict, progress: float) -> dict:
        body = {**item_payload, "progress": round(progress, 1), "app_version": "1.0.0"}
        return await self._post("/scrobble/pause", body)

    async def scrobble_stop(self, item_payload: dict, progress: float) -> dict:
        """Stop playback. If progress >= 80%, item marked as watched."""
        body = {**item_payload, "progress": round(progress, 1), "app_version": "1.0.0"}
        return await self._post("/scrobble/stop", body)

    async def scrobble_clear(self) -> dict:
        return await self._post("/scrobble/clear", {})

    # ═══════════════════════════════════════════════════════════════════════
    # Check-in
    # ═══════════════════════════════════════════════════════════════════════

    async def checkin_start(self, item_payload: dict) -> dict:
        """Start a manual check-in session."""
        return await self._post("/checkin", item_payload)

    async def checkin_get(self) -> dict | None:
        """Get current active check-in."""
        try:
            return await self._get("/checkin")
        except Exception:
            return None

    async def checkin_update(self, payload: dict) -> dict:
        """Update current check-in (pause/resume/progress)."""
        return await self._patch("/checkin", payload)

    async def checkin_stop(self, payload: dict | None = None) -> dict:
        """Stop current check-in. If progress >= 80%, marked as watched."""
        return await self._delete("/checkin", body=payload)

    # ═══════════════════════════════════════════════════════════════════════
    # Up Next
    # ═══════════════════════════════════════════════════════════════════════

    async def get_upnext(self, limit: int = 20, offset: int = 0) -> dict:
        """Returns in-progress TV shows with next unwatched episode."""
        return await self._get("/upnext", params={"limit": limit, "offset": offset})

    # ═══════════════════════════════════════════════════════════════════════
    # Search / Media Info
    # ═══════════════════════════════════════════════════════════════════════

    async def search(self, query: str, media_type: str = "movie") -> list[dict]:
        """Search MDBList for movies or shows."""
        return await self._get(f"/search/{media_type}", params={"query": query})

    async def get_media_info(self, provider: str, media_type: str, media_id: str) -> dict | None:
        """Get detailed media info by provider ID.
        provider: 'imdb', 'tmdb', 'tvdb', 'trakt', 'mdblist'
        media_type: 'movie' or 'show'
        """
        try:
            return await self._get(f"/{provider}/{media_type}/{media_id}")
        except Exception:
            return None

    async def get_ratings_batch(self, media_type: str, ids: list[dict]) -> list[dict]:
        """Batch ratings lookup.
        media_type: 'movie' or 'show'
        ids: [{"imdb": "tt..."}, {"tmdb": 123}]
        """
        try:
            return await self._post(f"/rating/{media_type}/all", ids)
        except Exception:
            return []

    # ═══════════════════════════════════════════════════════════════════════
    # Trending / Popular (via list endpoints)
    # ═══════════════════════════════════════════════════════════════════════

    async def get_trending_lists(self, limit: int = 20) -> list[dict]:
        """Alias for top lists, since MDBList doesn't have a separate trending endpoint."""
        return await self.get_top_lists()

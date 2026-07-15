"""Async Trakt API client.

Covers the endpoints needed by all four services:
  - OAuth device-code flow
  - Scrobble / checkin
  - User ratings, watchlist, history
  - Trending / popular
  - Calendar (my shows, premieres)
  - Friends + their ratings
  - Related items (for universe discovery)

Features:
  - Automatic token refresh before expiry
  - Rate limit tracking with exponential backoff
  - Retry logic for transient failures
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Awaitable

import httpx
import structlog

from fastapi import HTTPException

from app.config import settings

log = structlog.get_logger()

BASE = "https://api.trakt.tv"
HEADERS_BASE = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": settings.trakt_client_id,
}

# Rate limit defaults (Trakt: 1000 calls/day)
DEFAULT_RATE_LIMIT = 1000
RATE_LIMIT_WINDOW = 86400  # 24 hours in seconds


class TraktClient:
    """One instance per user (carries their access token).
    
    Handles:
      - Automatic token refresh when close to expiry
      - Rate limit tracking via Trakt response headers
      - Exponential backoff on 429 (rate limit exceeded)
      - Retry logic on transient errors
    """

    def __init__(
        self,
        access_token: str | None = None,
        refresh_token: str | None = None,
        token_expires: datetime | None = None,
        token_refresh_callback: Callable[[str, str, datetime], Awaitable[None]] | None = None,
    ):
        self._token = access_token
        self._refresh_token = refresh_token
        self._token_expires = token_expires
        self._token_refresh_callback = token_refresh_callback  # called when tokens refreshed
        
        self._client = httpx.AsyncClient(
            base_url=BASE,
            headers=HEADERS_BASE,
            timeout=30.0,
        )
        
        # Token refresh state — prevents infinite 401-refresh loops
        self._refresh_attempted = False

        # Rate limiting state
        self._rate_limit_remaining = DEFAULT_RATE_LIMIT
        self._rate_limit_reset = time.time() + RATE_LIMIT_WINDOW
        self._retry_after = 0.0  # for 429 backoff

    # -- Token refresh -------------------------------------------------------

    async def _ensure_token_valid(self) -> None:
        """Check if token is expired or close to expiry (within 5 minutes), refresh if needed."""
        if not self._refresh_token or not self._token_expires:
            return  # No refresh token, can't refresh
        
        now = datetime.utcnow()
        time_until_expiry = (self._token_expires - now).total_seconds()
        
        # Refresh if expiry is within 5 minutes
        if time_until_expiry < 300:
            log.info("trakt.token_refresh", seconds_until_expiry=time_until_expiry)
            try:
                token_data = await self.refresh_token(self._refresh_token)
                
                # Update local state
                self._token = token_data["access_token"]
                self._refresh_token = token_data["refresh_token"]
                self._token_expires = datetime.utcnow() + timedelta(
                    seconds=token_data.get("expires_in", 7776000)
                )
                
                # Notify caller to persist to database
                if self._token_refresh_callback:
                    await self._token_refresh_callback(
                        self._token,
                        self._refresh_token,
                        self._token_expires,
                    )
                
                log.info("trakt.token_refreshed", new_expiry=self._token_expires)
            except Exception as e:
                log.error("trakt.token_refresh_failed", error=str(e))
                raise

    async def _try_refresh_on_401(self, path: str) -> bool:
        """Attempt a single token refresh after receiving a 401.

        Returns True if tokens were successfully refreshed (caller should retry),
        False if refresh is not possible or failed (caller should raise).
        Tracks state so only one refresh attempt is made per request cycle.
        """
        if not self._refresh_token:
            log.warning("trakt.401_no_refresh_token", path=path)
            return False
        if self._refresh_attempted:
            # Already tried once this request cycle — don't loop
            return False

        self._refresh_attempted = True
        log.info("trakt.401_refresh_attempt", path=path)
        try:
            token_data = await self.refresh_token(self._refresh_token)
            self._token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_expires = datetime.utcnow() + timedelta(
                seconds=token_data.get("expires_in", 7776000)
            )
            if self._token_refresh_callback:
                await self._token_refresh_callback(
                    self._token, self._refresh_token, self._token_expires
                )
            log.info("trakt.401_refresh_success", new_expiry=self._token_expires)
            return True
        except Exception as e:
            log.error("trakt.401_refresh_failed", path=path, error=str(e))
            return False

    # -- Rate limiting -------------------------------------------------------

    def _update_rate_limit(self, resp: httpx.Response) -> None:
        """Extract rate limit info from Trakt API response headers."""
        # Trakt returns: X-Ratelimit-Limit, X-Ratelimit-Remaining, X-Ratelimit-Reset
        try:
            remaining = int(resp.headers.get("X-Ratelimit-Remaining", self._rate_limit_remaining))
            reset_timestamp = int(resp.headers.get("X-Ratelimit-Reset", self._rate_limit_reset))
            
            self._rate_limit_remaining = remaining
            self._rate_limit_reset = float(reset_timestamp)
            
            if remaining < 100:
                log.warning(
                    "trakt.rate_limit_low",
                    remaining=remaining,
                    reset_at=datetime.fromtimestamp(reset_timestamp),
                )
        except (ValueError, TypeError):
            pass  # Headers missing or invalid

    def get_rate_limit_info(self) -> dict:
        """Return current rate limit status."""
        now = time.time()
        seconds_until_reset = max(0, self._rate_limit_reset - now)
        return {
            "remaining": self._rate_limit_remaining,
            "reset_timestamp": self._rate_limit_reset,
            "seconds_until_reset": seconds_until_reset,
        }

    # -- Exponential backoff for 429 -----------------------------------------

    MAX_RATE_LIMIT_WAIT = 60  # Never sleep more than 60s on a 429

    async def _wait_for_rate_limit_reset(self) -> None:
        """Handle a 429 rate limit response.

        If the reset is within MAX_RATE_LIMIT_WAIT (60s), sleep and let the
        caller retry.  If the reset is further out (e.g. the daily 86400s
        window), raise immediately so the caller fails fast instead of
        hanging for hours.
        """
        if self._rate_limit_reset > time.time():
            sleep_time = self._rate_limit_reset - time.time() + 1
            if sleep_time > self.MAX_RATE_LIMIT_WAIT:
                log.error("trakt.rate_limit_exceeded",
                          sleep_would_be=round(sleep_time),
                          max_wait=self.MAX_RATE_LIMIT_WAIT)
                raise HTTPException(
                    429,
                    f"Trakt daily rate limit exceeded — resets in {int(sleep_time // 3600)}h {int((sleep_time % 3600) // 60)}m",
                )
            log.warning("trakt.rate_limit_wait", sleep_seconds=round(sleep_time))
            await asyncio.sleep(sleep_time)

    # -- Helpers ---------------------------------------------------------

    def _auth_headers(self) -> dict:
        if not self._token:
            return {}
        return {"Authorization": f"Bearer {self._token}"}

    async def _get(
        self,
        path: str,
        params: dict | None = None,
        max_retries: int = 3,
    ) -> Any:
        self._refresh_attempted = False
        await self._ensure_token_valid()
        
        for attempt in range(max_retries):
            try:
                resp = await self._client.get(
                    path, headers=self._auth_headers(), params=params or {},
                )
                
                # Update rate limit info
                self._update_rate_limit(resp)
                
                if resp.status_code == 429:
                    # Rate limited — wait and retry
                    await self._wait_for_rate_limit_reset()
                    continue
                
                if resp.status_code == 401:
                    # Attempt one token refresh and retry
                    refreshed = await self._try_refresh_on_401(path)
                    if refreshed:
                        continue  # retry with new token
                    raise HTTPException(401, "Trakt token expired or invalid — re-link required")
                
                resp.raise_for_status()
                return resp.json()
            
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2 ** attempt
                    log.warning("trakt.timeout_retry", path=path, attempt=attempt, wait_seconds=wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except httpx.HTTPStatusError as e:
                if 500 <= e.response.status_code < 600 and attempt < max_retries - 1:
                    # Server error — retry with backoff
                    wait_time = 2 ** attempt
                    log.warning("trakt.server_error_retry", path=path, status=e.response.status_code, wait_seconds=wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                raise

    async def _post(
        self,
        path: str,
        body: dict | None = None,
        max_retries: int = 3,
    ) -> Any:
        self._refresh_attempted = False
        await self._ensure_token_valid()
        
        for attempt in range(max_retries):
            try:
                resp = await self._client.post(
                    path, headers=self._auth_headers(), json=body or {},
                )
                
                # Update rate limit info
                self._update_rate_limit(resp)
                
                if resp.status_code == 429:
                    await self._wait_for_rate_limit_reset()
                    continue
                
                if resp.status_code == 401:
                    refreshed = await self._try_refresh_on_401(path)
                    if refreshed:
                        continue
                    raise HTTPException(401, "Trakt token expired or invalid — re-link required")
                
                resp.raise_for_status()
                return resp.json()
            
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    log.warning("trakt.timeout_retry", path=path, attempt=attempt, wait_seconds=wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except httpx.HTTPStatusError as e:
                if 500 <= e.response.status_code < 600 and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    log.warning("trakt.server_error_retry", path=path, status=e.response.status_code, wait_seconds=wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                raise

    async def _delete(self, path: str) -> None:
        await self._ensure_token_valid()
        resp = await self._client.delete(path, headers=self._auth_headers())
        self._update_rate_limit(resp)
        resp.raise_for_status()

    async def close(self):
        await self._client.aclose()

    # -- OAuth device-code flow ----------------------------------------------

    async def get_device_code(self) -> dict:
        """Step 1: get device_code + user_code to display."""
        resp = await self._client.post(
            "/oauth/device/code",
            json={"client_id": settings.trakt_client_id},
        )
        resp.raise_for_status()
        return resp.json()
        # Returns: device_code, user_code, verification_url, expires_in, interval

    async def poll_device_token(self, device_code: str) -> dict | None:
        """Step 2: poll until user authorises or timeout."""
        resp = await self._client.post(
            "/oauth/device/token",
            json={
                "code": device_code,
                "client_id": settings.trakt_client_id,
                "client_secret": settings.trakt_client_secret,
            },
        )
        if resp.status_code == 200:
            return resp.json()
            # Returns: access_token, refresh_token, expires_in, created_at, token_type
        return None  # 400 = pending, 404 = not found, 409 = already used, 410 = expired, 418 = denied

    async def refresh_token(self, refresh_token: str) -> dict:
        resp = await self._client.post(
            "/oauth/token",
            json={
                "refresh_token": refresh_token,
                "client_id": settings.trakt_client_id,
                "client_secret": settings.trakt_client_secret,
                "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
                "grant_type": "refresh_token",
            },
        )
        resp.raise_for_status()
        return resp.json()

    # -- User profile --------------------------------------------------------

    async def get_me(self) -> dict:
        return await self._get("/users/me")

    # -- Ratings -------------------------------------------------------------

    async def get_user_ratings(self, kind: str = "all") -> list[dict]:
        """kind: movies | shows | seasons | episodes | all"""
        return await self._get(f"/users/me/ratings/{kind}", params={"extended": "full"})

    # -- Watchlist -----------------------------------------------------------

    async def get_watchlist(self, kind: str = "all") -> list[dict]:
        """Fetch user's Trakt watchlist.

        When kind='all', fetches movies and shows separately and merges
        the results.  This avoids edge-cases where the /all endpoint
        returns inconsistent payloads on some Trakt API versions.
        """
        if kind == "all":
            movies = await self._get("/users/me/watchlist/movies", params={"sort": "added"})
            shows = await self._get("/users/me/watchlist/shows", params={"sort": "added"})
            return (movies or []) + (shows or [])
        return await self._get(f"/users/me/watchlist/{kind}", params={"sort": "added"})

    async def add_to_watchlist(
        self,
        movies: list[dict] | None = None,
        shows: list[dict] | None = None,
    ) -> dict:
        """Add items to the user's Trakt watchlist.

        movies: list of {"ids": {"tmdb": int}} or {"ids": {"imdb": "ttXXX"}}
        shows:  list of {"ids": {"tvdb": int}} or {"ids": {"imdb": "ttXXX"}}
        Returns the Trakt sync response with added/existing counts.
        """
        body: dict = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows
        if not body:
            return {"added": {"movies": 0, "shows": 0}}
        return await self._post("/sync/watchlist", body)

    # -- Watch history -------------------------------------------------------

    async def get_history(self, kind: str = "all", limit: int = 100) -> list[dict]:
        return await self._get(
            f"/users/me/history/{kind}",
            params={"limit": limit},
        )

    async def get_watched_episode_ids(self, max_pages: int = 20) -> set[str]:
        """Paginate through /users/me/history/episodes and return a set of
        episode-level provider IDs like ``imdb:tt1234567``, ``tvdb:12345``.

        Used by the scrobble audit to match episodes that have different
        season/episode numbering between Emby and Trakt.
        """
        ids: set[str] = set()
        page = 1
        per_page = 500
        while page <= max_pages:
            await self._ensure_token_valid()
            resp = await self._client.get(
                "/users/me/history/episodes",
                headers=self._auth_headers(),
                params={"page": page, "limit": per_page},
            )
            self._update_rate_limit(resp)
            if resp.status_code == 429:
                await self._wait_for_rate_limit_reset()
                continue  # retry same page
            if resp.status_code == 401:
                refreshed = await self._try_refresh_on_401("/users/me/history/episodes")
                if refreshed:
                    continue
                break
            if resp.status_code != 200:
                break
            entries = resp.json()
            if not entries:
                break
            for entry in entries:
                ep_ids = entry.get("episode", {}).get("ids", {})
                for key in ("imdb", "tvdb", "tmdb"):
                    val = ep_ids.get(key)
                    if val:
                        ids.add(f"{key}:{val}")
            total_pages = int(resp.headers.get("X-Pagination-Page-Count", page))
            if page >= total_pages:
                break
            page += 1
        return ids

    # -- Watched progress ----------------------------------------------------

    async def get_watched(self, kind: str = "shows") -> list[dict]:
        return await self._get(f"/users/me/watched/{kind}")

    # -- Trending / Popular --------------------------------------------------

    async def get_trending(self, kind: str = "shows", limit: int = 20, page: int = 1) -> list[dict]:
        return await self._get(f"/{kind}/trending", params={"limit": limit, "page": page})

    async def get_popular(self, kind: str = "shows", limit: int = 20) -> list[dict]:
        return await self._get(f"/{kind}/popular", params={"limit": limit})

    async def get_recommended(self, kind: str = "shows", limit: int = 20) -> list[dict]:
        return await self._get(f"/recommendations/{kind}", params={"limit": limit})

    # -- Calendar ------------------------------------------------------------

    async def get_my_shows(self, start_date: str | None = None, days: int = 14) -> list[dict]:
        path = "/calendars/my/shows"
        if start_date:
            path += f"/{start_date}/{days}"
        return await self._get(path)

    async def get_my_movies(self, start_date: str | None = None, days: int = 14) -> list[dict]:
        """Movies with a release date in range that are in the user's
        watchlist/collection — the movie equivalent of get_my_shows."""
        path = "/calendars/my/movies"
        if start_date:
            path += f"/{start_date}/{days}"
        return await self._get(path)

    async def get_my_new_shows(self, start_date: str | None = None, days: int = 30) -> list[dict]:
        path = "/calendars/my/shows/new"
        if start_date:
            path += f"/{start_date}/{days}"
        return await self._get(path)

    async def get_my_premieres(self, start_date: str | None = None, days: int = 30) -> list[dict]:
        path = "/calendars/my/shows/premieres"
        if start_date:
            path += f"/{start_date}/{days}"
        return await self._get(path)

    async def get_show_seasons(self, show_id: str) -> list[dict]:
        """Season list with episode_count, used to detect season finales
        (an episode is a finale if episode.number == season.episode_count)."""
        return await self._get(f"/shows/{show_id}/seasons", params={"extended": "full"})

    async def get_movie_releases(self, movie_id: str, country: str = "us") -> list[dict]:
        """Typed release dates for a movie (theatrical, digital, physical, tv).
        Returns list of {country, certification, release_date, release_type, note}."""
        return await self._get(f"/movies/{movie_id}/releases/{country}")

    # -- Friends -------------------------------------------------------------

    async def get_friends(self) -> list[dict]:
        return await self._get("/users/me/friends")

    async def get_friend_ratings(self, username: str, kind: str = "all") -> list[dict]:
        return await self._get(f"/users/{username}/ratings/{kind}")

    async def get_friend_watched(self, username: str, kind: str = "shows") -> list[dict]:
        return await self._get(f"/users/{username}/watched/{kind}")

    # -- Scrobble / Checkin --------------------------------------------------

    async def scrobble_start(self, item_payload: dict, progress: float) -> dict:
        return await self._post("/scrobble/start", {**item_payload, "progress": progress})

    async def scrobble_pause(self, item_payload: dict, progress: float) -> dict:
        return await self._post("/scrobble/pause", {**item_payload, "progress": progress})

    async def scrobble_stop(self, item_payload: dict, progress: float) -> dict:
        return await self._post("/scrobble/stop", {**item_payload, "progress": progress})

    async def add_to_history(self, items: list[dict]) -> dict:
        """Add items to Trakt watch history via POST /sync/history.

        Each item in `items` should be a dict like:
          {"ids": {"imdb": "tt1234567"}, "watched_at": "2026-07-03T12:00:00.000Z"}
        or for shows:
          {"ids": {"imdb": "tt1234567"}, "episodes": [...]}

        Accepts movies and shows in the same call.
        """
        movies = [i for i in items if i.get("_type") != "show"]
        shows = [i for i in items if i.get("_type") == "show"]
        # Strip internal _type key before sending
        for i in movies + shows:
            i.pop("_type", None)
        payload = {}
        if movies:
            payload["movies"] = movies
        if shows:
            payload["shows"] = shows
        if not payload:
            return {}
        return await self._post("/sync/history", payload)

    async def checkin(self, item_payload: dict, message: str = "") -> dict:
        body = {**item_payload}
        if message:
            body["sharing"] = {"text": message}
        # Cancel any existing checkin first to avoid 409 Conflict
        try:
            await self._delete("/checkin")
        except Exception:
            pass  # 204 No Content or 404 if nothing active — both fine
        return await self._post("/checkin", body)

    # -- Related / connections (for universe discovery) ----------------------

    async def get_related(self, kind: str, trakt_id: str, limit: int = 10) -> list[dict]:
        """Get related movies/shows for universe mapping."""
        return await self._get(
            f"/{kind}/{trakt_id}/related",
            params={"limit": limit},
        )

    async def get_item_details(self, kind: str, trakt_id: str) -> dict:
        return await self._get(f"/{kind}/{trakt_id}", params={"extended": "full"})

    async def get_movie_by_id(self, id_value: str, id_type: str = "imdb") -> dict | None:
        """Lookup a movie by IMDB, TMDB, or Trakt ID.  Returns full metadata.

        Trakt's /movies/{id} path accepts IMDB IDs (tt...) and Trakt slugs
        but NOT bare TMDB numeric IDs.  For TMDB use /search/tmdb/{id}.
        """
        try:
            if id_type == "tmdb" or (id_type != "imdb" and id_value.isdigit()):
                # TMDB numeric ID → use search endpoint to get slug first
                results = await self._get(f"/search/tmdb/{id_value}", params={"type": "movie"})
                if results and isinstance(results, list) and results[0].get("movie"):
                    slug = results[0]["movie"].get("ids", {}).get("slug")
                    if slug:
                        return await self._get(f"/movies/{slug}", params={"extended": "full"})
                    return results[0]["movie"]
                return None
            else:
                # IMDB (tt...) or Trakt slug → direct path lookup
                return await self._get(f"/movies/{id_value}", params={"extended": "full"})
        except Exception:
            return None

    async def get_show_by_id(self, id_value: str, id_type: str = "imdb") -> dict | None:
        """Lookup a show by IMDB, TMDB, or Trakt ID.  Returns full metadata."""
        try:
            if id_type == "tmdb" or (id_type != "imdb" and id_value.isdigit()):
                results = await self._get(f"/search/tmdb/{id_value}", params={"type": "show"})
                if results and isinstance(results, list) and results[0].get("show"):
                    slug = results[0]["show"].get("ids", {}).get("slug")
                    if slug:
                        return await self._get(f"/shows/{slug}", params={"extended": "full"})
                    return results[0]["show"]
                return None
            else:
                return await self._get(f"/shows/{id_value}", params={"extended": "full"})
        except Exception:
            return None

    async def search(self, query: str, kind: str = "movie,show") -> list[dict]:
        return await self._get(f"/search/{kind}", params={"query": query})

    # -- Lists (for curated universe data) -----------------------------------

    async def get_popular_lists(self, limit: int = 20) -> list[dict]:
        return await self._get("/lists/popular", params={"limit": limit})

    async def get_list_items(self, username: str, list_id: str) -> list[dict]:
        return await self._get(f"/users/{username}/lists/{list_id}/items")

    # -- Comments (bonus) ----------------------------------------------------

    async def get_comments(self, kind: str, trakt_id: str) -> list[dict]:
        return await self._get(f"/{kind}/{trakt_id}/comments/newest")

    async def post_comment(self, item_payload: dict, comment: str, spoiler: bool = False) -> dict:
        return await self._post("/comments", {
            **item_payload,
            "comment": comment,
            "spoiler": spoiler,
        })

"""Async MDBList API client.

Used by the MDBList integration to fetch user lists, list items,
and user info.  Auth is via API key appended as a query param.

API base: https://api.mdblist.com
Docs: https://mdblist.docs.apiary.io
Free tier: 1000 requests/day.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()

BASE_URL = "https://api.mdblist.com"


class MDBListClient:
    """Lightweight async MDBList client — lists, items, user info."""

    def __init__(self, api_key: str):
        self._key = api_key
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=20.0,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def close(self):
        await self._client.aclose()

    def _params(self, extra: dict | None = None) -> dict:
        p = {"apikey": self._key}
        if extra:
            p.update(extra)
        return p

    async def _get(self, path: str, params: dict | None = None) -> Any:
        resp = await self._client.get(path, params=self._params(params))
        resp.raise_for_status()
        return resp.json()

    # -- Health / test --------------------------------------------------------

    async def test_connection(self) -> dict:
        """Test the API key by fetching user info.
        Returns {"status": "ok", "username": "...", "requests_remaining": N}
        """
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
                return {"status": "error", "message": "Invalid API key"}
            if e.response.status_code == 429:
                return {"status": "error", "message": "Rate limit exceeded"}
            return {"status": "error", "message": f"HTTP {e.response.status_code}"}
        except Exception as e:
            return {"status": "error", "message": str(e)[:200]}

    # -- User info ------------------------------------------------------------

    async def get_user(self) -> dict:
        """Return authenticated user info."""
        return await self._get("/user")

    # -- Lists ----------------------------------------------------------------

    async def get_my_lists(self) -> list[dict]:
        """Fetch all lists belonging to the authenticated user.
        Returns array of list objects with id, name, slug, description,
        mediatype, items, likes, type (dynamic/static), private, etc.
        """
        return await self._get("/lists/user/")

    async def get_list_by_id(self, list_id: int) -> list[dict]:
        """Fetch list metadata by numeric ID."""
        return await self._get(f"/lists/{list_id}")

    async def get_liked_lists(self) -> list[dict]:
        """Fetch lists the user has liked on MDBList."""
        try:
            data = await self._get("/lists/liked")
            # API may return list of dicts or a wrapper — normalise
            if isinstance(data, dict) and "lists" in data:
                return data["lists"]
            if isinstance(data, list):
                return data
            return []
        except Exception:
            return []

    # -- List items -----------------------------------------------------------

    async def get_list_items(
        self,
        list_id: int,
        limit: int = 1000,
        offset: int = 0,
        append_to_response: str = "description",
    ) -> dict:
        """Fetch items from a list by ID.

        Returns {"movies": [...], "shows": [...]}.
        Each item has: id, rank, title, imdb_id, tvdb_id, ids{}, mediatype,
        release_year, etc.  With append_to_response="description" each item
        also gets a description field.

        Max 1000 items per call — use offset to paginate.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if append_to_response:
            params["append_to_response"] = append_to_response
        return await self._get(f"/lists/{list_id}/items", params)

    async def get_all_list_items(self, list_id: int) -> list[dict]:
        """Fetch ALL items from a list, handling pagination.

        Returns a flat list of items (movies and shows combined),
        each with mediatype preserved.
        """
        all_items: list[dict] = []
        offset = 0
        limit = 1000

        while True:
            data = await self.get_list_items(
                list_id, limit=limit, offset=offset,
                append_to_response="description",
            )
            movies = data.get("movies") or []
            shows = data.get("shows") or []

            for m in movies:
                m.setdefault("mediatype", "movie")
                all_items.append(m)
            for s in shows:
                s.setdefault("mediatype", "show")
                all_items.append(s)

            # Sort by rank to preserve list order
            batch_count = len(movies) + len(shows)
            if batch_count < limit:
                break
            offset += limit

        all_items.sort(key=lambda x: x.get("rank", 9999))
        return all_items

    # -- Top / popular lists --------------------------------------------------

    async def get_top_lists(self) -> list[dict]:
        """Fetch top/popular public lists from MDBList."""
        try:
            return await self._get("/lists/top")
        except Exception:
            return []

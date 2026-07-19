"""Async Radarr v3 API client.

Used by Universe Discovery to send missing movies to Radarr for
automatic download.  Supports up to 2 Radarr instances (e.g. 1080p
and 4K servers).
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()


class RadarrClient:
    """Lightweight Radarr v3 client — add movies, list root folders / profiles."""

    def __init__(self, url: str, api_key: str, name: str = "Radarr"):
        self._base = url.rstrip("/")
        self._key = api_key
        self._name = name
        self._client = httpx.AsyncClient(timeout=15.0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    def _headers(self) -> dict:
        return {"X-Api-Key": self._key}

    async def _get(self, path: str) -> Any:
        resp = await self._client.get(
            f"{self._base}/api/v3{path}", headers=self._headers(),
        )
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, body: dict) -> Any:
        resp = await self._client.post(
            f"{self._base}/api/v3{path}",
            headers=self._headers(),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()

    # -- Health / test --------------------------------------------------------

    async def test_connection(self) -> dict:
        """Test the connection to Radarr.  Returns system status."""
        try:
            resp = await self._client.get(
                f"{self._base}/api/v3/system/status",
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            return {"status": "ok", "version": data.get("version", "unknown")}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # -- Library listing ------------------------------------------------------

    async def get_all_movies(self) -> list[dict]:
        """Return all movies in Radarr with status fields."""
        return await self._get("/movie")

    async def get_missing_movies(self) -> list[dict]:
        """Return monitored movies that don't have a file yet."""
        all_movies = await self.get_all_movies()
        return [
            m for m in all_movies
            if m.get("monitored") and not m.get("hasFile")
        ]

    # -- Lookup ---------------------------------------------------------------

    async def get_root_folders(self) -> list[dict]:
        return await self._get("/rootfolder")

    async def get_quality_profiles(self) -> list[dict]:
        return await self._get("/qualityprofile")

    async def lookup_by_tmdb(self, tmdb_id: int) -> dict | None:
        """Look up a movie in Radarr by TMDB ID."""
        results = await self._get(f"/movie/lookup/tmdb?tmdbId={tmdb_id}")
        if isinstance(results, dict):
            return results
        if isinstance(results, list) and results:
            return results[0]
        return None

    async def lookup_by_imdb(self, imdb_id: str) -> dict | None:
        """Look up a movie in Radarr by IMDB ID."""
        results = await self._get(f"/movie/lookup/imdb?imdbId={imdb_id}")
        if isinstance(results, dict):
            return results
        if isinstance(results, list) and results:
            return results[0]
        return None

    async def lookup_by_term(self, title: str, year: int | None = None) -> dict | None:
        """Look up a movie in Radarr by title search."""
        import urllib.parse
        query = f"{title} {year}" if year else title
        results = await self._get(f"/movie/lookup?term={urllib.parse.quote(query)}")
        if isinstance(results, list) and results:
            # Try to match exact title+year first
            if year:
                for r in results:
                    if r.get("year") == year and r.get("title", "").lower() == title.lower():
                        return r
            return results[0]
        return None

    # -- Add movie ------------------------------------------------------------

    async def add_movie(
        self,
        tmdb_id: int | None = None,
        imdb_id: str | None = None,
        title: str = "",
        year: int | None = None,
        root_folder_path: str | None = None,
        quality_profile_id: int | None = None,
        monitored: bool = True,
        search_now: bool = True,
    ) -> dict:
        """Add a movie to Radarr for download.

        Looks up the movie first by TMDB or IMDB ID to get metadata,
        then posts to Radarr's /movie endpoint.
        """
        # Look up movie metadata
        movie_data = None
        if tmdb_id:
            movie_data = await self.lookup_by_tmdb(tmdb_id)
        if not movie_data and imdb_id:
            movie_data = await self.lookup_by_imdb(imdb_id)
        if not movie_data and title:
            movie_data = await self.lookup_by_term(title, year)

        if not movie_data:
            return {
                "status": "error",
                "reason": f"Movie not found in Radarr lookup (tmdb={tmdb_id}, imdb={imdb_id})",
            }

        # Get defaults if not specified
        if not root_folder_path:
            folders = await self.get_root_folders()
            if folders:
                root_folder_path = folders[0].get("path", "/movies")

        if not quality_profile_id:
            profiles = await self.get_quality_profiles()
            if profiles:
                quality_profile_id = profiles[0].get("id", 1)

        # Build payload
        payload = {
            "title": movie_data.get("title", title),
            "tmdbId": movie_data.get("tmdbId", tmdb_id),
            "year": movie_data.get("year", year),
            "qualityProfileId": quality_profile_id or 1,
            "rootFolderPath": root_folder_path or "/movies",
            "monitored": monitored,
            "addOptions": {
                "searchForMovie": search_now,
            },
        }

        # Preserve images/overview from lookup
        for field in ("images", "overview", "imdbId", "titleSlug", "folder"):
            if field in movie_data:
                payload[field] = movie_data[field]
        if "titleSlug" not in payload:
            slug = (movie_data.get("title") or title or "unknown").lower()
            slug = slug.replace(" ", "-").replace(":", "").replace("'", "")
            payload["titleSlug"] = f"{slug}-{movie_data.get('tmdbId', tmdb_id or 0)}"

        try:
            result = await self._post("/movie", payload)
            log.info(
                "radarr.movie_added",
                server=self._name,
                title=payload["title"],
                tmdb_id=payload.get("tmdbId"),
            )
            return {
                "status": "ok",
                "title": result.get("title", payload["title"]),
                "radarr_id": result.get("id"),
                "server": self._name,
            }
        except httpx.HTTPStatusError as e:
            # 400 often means already exists
            detail = ""
            try:
                detail = e.response.json()
            except Exception:
                detail = e.response.text[:200]
            log.warning(
                "radarr.add_failed",
                server=self._name,
                title=payload["title"],
                status=e.response.status_code,
                detail=detail,
            )
            return {
                "status": "error",
                "title": payload["title"],
                "server": self._name,
                "reason": str(detail),
                "http_status": e.response.status_code,
            }

    # -- Calendar --------------------------------------------------------------

    async def get_calendar(self, start: str, end: str) -> list[dict]:
        """Fetch upcoming movie releases from Radarr's calendar.

        Parameters:
            start: ISO date string (e.g. "2026-07-15")
            end:   ISO date string (e.g. "2026-08-15")

        Returns list of movies with release dates (inCinemas, digitalRelease,
        physicalRelease) that fall within the date range.
        """
        try:
            data = await self._get(f"/calendar?start={start}&end={end}")
            result = []
            for m in (data if isinstance(data, list) else []):
                tmdb = m.get("tmdbId")
                if not tmdb:
                    continue
                result.append({
                    "tmdb_id": tmdb,
                    "title": m.get("title", ""),
                    "in_cinemas": m.get("inCinemas"),
                    "digital_release": m.get("digitalRelease"),
                    "physical_release": m.get("physicalRelease"),
                    "has_file": m.get("hasFile", False),
                })
            return result
        except Exception as e:
            log.warning("radarr.calendar_fetch_failed",
                        server=self._name, error=str(e)[:120])
            return []

    # -- Download queue -------------------------------------------------------

    async def get_download_queue(self) -> list[dict]:
        """Fetch active download queue from Radarr.

        Returns a normalised list of queue records with progress, ETA,
        size info and the TMDB ID so the frontend can match to smart
        queue items.
        """
        try:
            data = await self._get("/queue?page=1&pageSize=50&includeMovie=true")
            records = data.get("records", []) if isinstance(data, dict) else data
            result = []
            for rec in records:
                movie = rec.get("movie") or {}
                result.append({
                    "title": rec.get("title") or movie.get("title", ""),
                    "tmdb_id": movie.get("tmdbId"),
                    "imdb_id": movie.get("imdbId"),
                    "status": rec.get("status", ""),
                    "tracked_status": rec.get("trackedDownloadStatus", ""),
                    "progress": round(100 - (rec.get("sizeleft", 0) / max(rec.get("size", 1), 1)) * 100, 1),
                    "size_mb": round(rec.get("size", 0) / 1_048_576, 1),
                    "sizeleft_mb": round(rec.get("sizeleft", 0) / 1_048_576, 1),
                    "eta": rec.get("estimatedCompletionTime"),
                    "download_id": rec.get("downloadId", ""),
                    "download_client": rec.get("downloadClient", ""),
                    "server": self._name,
                    "type": "movie",
                })
            return result
        except Exception as e:
            log.warning("radarr.queue_fetch_failed", server=self._name, error=str(e)[:120])
            return []

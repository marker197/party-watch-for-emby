"""Async Sonarr v3 API client.

Used by Smart Queue to send missing TV shows to Sonarr for
automatic download.  Supports up to 2 Sonarr instances (e.g. 1080p
and 4K servers).
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()


class SonarrClient:
    """Lightweight Sonarr v3 client — add series, list root folders / profiles."""

    def __init__(self, url: str, api_key: str, name: str = "Sonarr"):
        self._base = url.rstrip("/")
        self._key = api_key
        self._name = name
        self._client = httpx.AsyncClient(timeout=15.0)

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
        """Test the connection to Sonarr.  Returns system status."""
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

    async def get_all_series(self) -> list[dict]:
        """Return all series in Sonarr with episode counts and status."""
        return await self._get("/series")

    async def get_missing_series(self) -> list[dict]:
        """Return monitored series that have missing episodes."""
        all_series = await self.get_all_series()
        missing = []
        for s in all_series:
            if not s.get("monitored"):
                continue
            stats = s.get("statistics") or {}
            total = stats.get("episodeCount", 0)
            on_disk = stats.get("episodeFileCount", 0)
            if total > 0 and on_disk < total:
                missing.append(s)
        return missing

    # -- Lookup ---------------------------------------------------------------

    async def get_root_folders(self) -> list[dict]:
        return await self._get("/rootfolder")

    async def get_quality_profiles(self) -> list[dict]:
        return await self._get("/qualityprofile")

    async def lookup_by_tvdb(self, tvdb_id: int) -> dict | None:
        """Look up a series in Sonarr by TVDB ID."""
        results = await self._get(f"/series/lookup?term=tvdb:{tvdb_id}")
        if isinstance(results, dict):
            return results
        if isinstance(results, list) and results:
            return results[0]
        return None

    async def lookup_by_term(self, title: str, year: int | None = None) -> dict | None:
        """Look up a series in Sonarr by title search."""
        import urllib.parse
        query = f"{title} {year}" if year else title
        results = await self._get(f"/series/lookup?term={urllib.parse.quote(query)}")
        if isinstance(results, list) and results:
            if year:
                for r in results:
                    if r.get("year") == year and r.get("title", "").lower() == title.lower():
                        return r
            return results[0]
        return None

    # -- Add series -----------------------------------------------------------

    async def add_series(
        self,
        tvdb_id: int | None = None,
        imdb_id: str | None = None,
        title: str = "",
        year: int | None = None,
        root_folder_path: str | None = None,
        quality_profile_id: int | None = None,
        monitored: bool = True,
        search_now: bool = True,
        season_folder: bool = True,
    ) -> dict:
        """Add a TV series to Sonarr for download.

        Looks up the series first by TVDB ID or title to get metadata,
        then posts to Sonarr's /series endpoint.
        """
        # Look up series metadata
        series_data = None
        if tvdb_id:
            series_data = await self.lookup_by_tvdb(tvdb_id)
        if not series_data and title:
            series_data = await self.lookup_by_term(title, year)

        if not series_data:
            return {
                "status": "error",
                "reason": f"Series not found in Sonarr lookup (tvdb={tvdb_id}, title={title})",
            }

        # Get defaults if not specified
        if not root_folder_path:
            folders = await self.get_root_folders()
            if folders:
                root_folder_path = folders[0].get("path", "/tv")

        if not quality_profile_id:
            profiles = await self.get_quality_profiles()
            if profiles:
                quality_profile_id = profiles[0].get("id", 1)

        # Build payload
        payload = {
            "title": series_data.get("title", title),
            "tvdbId": series_data.get("tvdbId", tvdb_id),
            "year": series_data.get("year", year),
            "qualityProfileId": quality_profile_id or 1,
            "rootFolderPath": root_folder_path or "/tv",
            "monitored": monitored,
            "seasonFolder": season_folder,
            "addOptions": {
                "searchForMissingEpisodes": search_now,
            },
        }

        # Preserve images/overview from lookup
        for field in ("images", "overview", "imdbId", "titleSlug", "folder", "seasons"):
            if field in series_data:
                payload[field] = series_data[field]
        if "titleSlug" not in payload:
            slug = (series_data.get("title") or title or "unknown").lower()
            slug = slug.replace(" ", "-").replace(":", "").replace("'", "")
            payload["titleSlug"] = slug

        try:
            result = await self._post("/series", payload)
            log.info(
                "sonarr.series_added",
                server=self._name,
                title=payload["title"],
                tvdb_id=payload.get("tvdbId"),
            )
            return {
                "status": "ok",
                "title": result.get("title", payload["title"]),
                "sonarr_id": result.get("id"),
                "server": self._name,
            }
        except httpx.HTTPStatusError as e:
            detail = ""
            try:
                detail = e.response.json()
            except Exception:
                detail = e.response.text[:200]
            log.warning(
                "sonarr.add_failed",
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

    # -- Calendar -------------------------------------------------------------

    async def get_calendar(self, start: str, end: str) -> list[dict]:
        """Fetch upcoming episodes from Sonarr's calendar.

        Parameters:
            start: ISO date string (e.g. "2026-07-15")
            end:   ISO date string (e.g. "2026-08-15")

        Returns normalised list of upcoming episodes with air dates,
        series TVDB ID, season/episode numbers, and file status.
        """
        try:
            data = await self._get(
                f"/calendar?start={start}&end={end}"
                "&includeSeries=true&includeEpisodeFile=false"
            )
            result = []
            for ep in (data if isinstance(data, list) else []):
                series = ep.get("series") or {}
                result.append({
                    "tvdb_id": series.get("tvdbId"),
                    "series_title": series.get("title", ""),
                    "season": ep.get("seasonNumber"),
                    "episode": ep.get("episodeNumber"),
                    "episode_title": ep.get("title", ""),
                    "air_date_utc": ep.get("airDateUtc"),
                    "has_file": ep.get("hasFile", False),
                })
            return result
        except Exception as e:
            log.warning("sonarr.calendar_fetch_failed",
                        server=self._name, error=str(e)[:120])
            return []

    # -- Download queue -------------------------------------------------------

    async def get_download_queue(self) -> list[dict]:
        """Fetch active download queue from Sonarr.

        Returns a normalised list of queue records with progress, ETA,
        size info and the TVDB ID / series title so the frontend can
        match to smart queue items.
        """
        try:
            data = await self._get("/queue?page=1&pageSize=50&includeSeries=true&includeEpisode=true")
            records = data.get("records", []) if isinstance(data, dict) else data
            result = []
            for rec in records:
                series = rec.get("series") or {}
                episode = rec.get("episode") or {}
                ep_label = ""
                if episode.get("seasonNumber") is not None and episode.get("episodeNumber") is not None:
                    ep_label = f"S{episode['seasonNumber']:02d}E{episode['episodeNumber']:02d}"
                result.append({
                    "title": series.get("title", rec.get("title", "")),
                    "episode_title": episode.get("title", ""),
                    "episode_label": ep_label,
                    "tvdb_id": series.get("tvdbId"),
                    "imdb_id": series.get("imdbId"),
                    "status": rec.get("status", ""),
                    "tracked_status": rec.get("trackedDownloadStatus", ""),
                    "progress": round(100 - (rec.get("sizeleft", 0) / max(rec.get("size", 1), 1)) * 100, 1),
                    "size_mb": round(rec.get("size", 0) / 1_048_576, 1),
                    "sizeleft_mb": round(rec.get("sizeleft", 0) / 1_048_576, 1),
                    "eta": rec.get("estimatedCompletionTime"),
                    "download_client": rec.get("downloadClient", ""),
                    "server": self._name,
                    "type": "show",
                })
            return result
        except Exception as e:
            log.warning("sonarr.queue_fetch_failed", server=self._name, error=str(e)[:120])
            return []

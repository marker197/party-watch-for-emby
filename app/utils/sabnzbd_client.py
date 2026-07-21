"""Async SABnzbd API client.

Fetches the download queue for real-time progress, speed, ETA, and
status (Downloading, Paused, Queued, Repairing, Verifying, Extracting,
Moving).  Supports up to 2 SABnzbd instances.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()


class SabnzbdClient:
    """Lightweight SABnzbd client — queue status only."""

    def __init__(self, url: str, api_key: str, name: str = "SABnzbd"):
        self._base = url.rstrip("/")
        self._key = api_key
        self._name = name
        self._client = httpx.AsyncClient(timeout=15.0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def _api(self, mode: str, **params: Any) -> dict:
        params.update({"mode": mode, "apikey": self._key, "output": "json"})
        resp = await self._client.get(f"{self._base}/api", params=params)
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()

    # -- Health / test --------------------------------------------------------

    async def test_connection(self) -> dict:
        """Test connection and return version info."""
        try:
            data = await self._api("version")
            version = data.get("version", "")
            if version:
                return {"status": "ok", "version": version}
            return {"status": "error", "message": "No version returned"}
        except httpx.HTTPStatusError as e:
            return {"status": "error", "message": f"HTTP {e.response.status_code}"}
        except Exception as e:
            return {"status": "error", "message": str(e)[:200]}

    # -- Queue ----------------------------------------------------------------

    async def get_queue(self) -> list[dict]:
        """Fetch the active download queue.

        Returns a list of normalised queue slots keyed by ``nzo_id``
        with real-time progress, speed, ETA, size, and SABnzbd status
        (Downloading, Paused, Queued, Repairing, Verifying, Extracting,
        Moving, Running).
        """
        try:
            data = await self._api("queue", limit=50)
            queue = data.get("queue", {})
            slots = queue.get("slots", [])
            result = []
            for slot in slots:
                pct_str = slot.get("percentage", "0")
                try:
                    pct = float(pct_str)
                except (ValueError, TypeError):
                    pct = 0.0

                size_mb = _parse_size_mb(slot.get("mb", "0"))
                left_mb = _parse_size_mb(slot.get("mbleft", "0"))

                # SABnzbd queue.speed is global (e.g. "12.5 M"),
                # queue.kbpersec is the raw numeric KB/s value.
                raw_speed = queue.get("speed", "")
                raw_kbps = queue.get("kbpersec", "")

                result.append({
                    "nzo_id": slot.get("nzo_id", ""),
                    "filename": slot.get("filename", ""),
                    "status": slot.get("status", ""),
                    "progress": round(pct, 1),
                    "size_mb": round(size_mb, 1),
                    "sizeleft_mb": round(left_mb, 1),
                    "eta": slot.get("eta", "unknown"),
                    "timeleft": slot.get("timeleft", ""),
                    "speed": raw_speed,
                    "speed_kbps": _parse_size_mb(raw_kbps),
                    "server": self._name,
                })
            return result
        except Exception as e:
            log.warning("sabnzbd.queue_fetch_failed",
                        server=self._name, error=str(e)[:120])
            return []

    # -- History (post-processing + completed) --------------------------------

    async def get_history(self, limit: int = 20) -> list[dict]:
        """Fetch recent history items (post-processing and completed).

        SABnzbd moves items from queue to history once downloading finishes.
        History items have statuses like: Repairing, Verifying, Extracting,
        Moving, Running, Completed, Failed.
        """
        try:
            data = await self._api("history", limit=limit)
            history = data.get("history", {})
            slots = history.get("slots", [])
            result = []
            for slot in slots:
                status = slot.get("status", "")
                # Only include items still processing or recently completed
                # Skip old completed/failed items
                if status in ("Completed", "Failed"):
                    # Include completed items from last 5 minutes only
                    try:
                        completed_ts = slot.get("completed", 0)
                        import time
                        if time.time() - completed_ts > 300:
                            continue
                    except (ValueError, TypeError):
                        continue

                size_mb = _parse_size_mb(slot.get("bytes", 0)) / (1024 * 1024) if slot.get("bytes") else 0

                result.append({
                    "nzo_id": slot.get("nzo_id", ""),
                    "filename": slot.get("name", ""),
                    "status": status,
                    "progress": 100.0 if status == "Completed" else 99.0,
                    "size_mb": round(size_mb, 1),
                    "sizeleft_mb": 0.0,
                    "eta": "",
                    "timeleft": "",
                    "speed": "",
                    "speed_kbps": 0,
                    "server": self._name,
                })
            return result
        except Exception as e:
            log.warning("sabnzbd.history_fetch_failed",
                        server=self._name, error=str(e)[:120])
            return []


def _parse_size_mb(val: str | float | int) -> float:
    """Parse SABnzbd's size strings (e.g. '1234.56') to float MB."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

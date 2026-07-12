"""Watch History Stats — aggregate Trakt history into viewable stats.

Provides totals (hours watched), genre breakdowns, peak hours,
movies vs TV split, and per-period summaries.  Results are cached
in Redis for 1 hour to avoid hammering the Trakt API.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import structlog
from sqlalchemy import select

from app.models.schema import User
from app.utils.trakt_client import TraktClient
from app.utils.redis_cache import get_redis
from app.utils.database import async_session

log = structlog.get_logger()

STATS_CACHE_TTL = 3600  # 1 hour


class WatchStatsService:

    async def get_stats(self, user: User) -> dict:
        """Return aggregated watch history stats for a user.

        Pulls full Trakt history (paginated), aggregates into:
        - hours_by_period: {week, month, year, all_time}
        - genre_counts: {genre: count}
        - peak_hours: [0..23] with counts
        - type_split: {movies: N, episodes: N}
        - recent_daily: last 30 days [{date, hours, count}]
        - avg_per_session: average items per day when watching
        """
        if not user.trakt_access_token:
            return {"error": "No Trakt token"}

        cache_key = f"watch_stats_v2:{user.id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

        trakt = await self._make_trakt(user)
        try:
            history = await self._fetch_full_history(trakt)
        finally:
            await trakt.close()

        if not history:
            return {
                "hours_by_period": {"week": 0, "month": 0, "year": 0, "all_time": 0},
                "genre_counts": {},
                "peak_hours": [0] * 24,
                "type_split": {"movies": 0, "episodes": 0},
                "recent_daily": [],
                "avg_per_session": 0,
                "total_items": 0,
            }

        result = self._aggregate(history)

        try:
            r = await get_redis()
            await r.setex(cache_key, STATS_CACHE_TTL, json.dumps(result))
        except Exception:
            pass

        return result

    # ------------------------------------------------------------------

    async def _fetch_full_history(self, trakt: TraktClient) -> list[dict]:
        """Paginate through /users/me/history to get all entries."""
        all_entries: list[dict] = []
        page = 1
        per_page = 500
        max_pages = 40  # safety cap — 20k entries

        while page <= max_pages:
            await trakt._ensure_token_valid()
            resp = await trakt._client.get(
                "/users/me/history",
                headers=trakt._auth_headers(),
                params={"page": page, "limit": per_page, "extended": "full"},
            )
            trakt._update_rate_limit(resp)
            if resp.status_code == 429:
                await trakt._wait_for_rate_limit_reset()
                continue
            if resp.status_code == 401:
                refreshed = await trakt._try_refresh_on_401("/users/me/history")
                if refreshed:
                    continue
                break
            if resp.status_code != 200:
                break
            entries = resp.json()
            if not entries:
                break
            all_entries.extend(entries)
            total_pages = int(resp.headers.get("X-Pagination-Page-Count", page))
            if page >= total_pages:
                break
            page += 1

        log.info("watch_stats.history_fetched", entries=len(all_entries), pages=page)
        return all_entries

    def _aggregate(self, history: list[dict]) -> dict:
        """Crunch history entries into stats."""
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        year_ago = now - timedelta(days=365)

        hours_week = 0.0
        hours_month = 0.0
        hours_year = 0.0
        hours_all = 0.0

        genre_counts: dict[str, int] = defaultdict(int)
        peak_hours = [0] * 24
        type_split = {"movies": 0, "episodes": 0}

        # For recent daily chart — last 30 days
        daily_buckets: dict[str, dict] = {}
        for i in range(30):
            d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_buckets[d] = {"date": d, "minutes": 0.0, "count": 0}

        watching_days: set[str] = set()

        # Year-over-year: {year: {1..12: hours}}
        monthly_by_year: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        # Daily heatmap: last 365 days {date_str: {count, minutes}}
        heatmap_start = now - timedelta(days=364)
        heatmap_buckets: dict[str, dict] = {}
        for i in range(365):
            d = (heatmap_start + timedelta(days=i)).strftime("%Y-%m-%d")
            heatmap_buckets[d] = {"date": d, "count": 0, "minutes": 0.0}

        for entry in history:
            watched_at_str = entry.get("watched_at", "")
            try:
                watched_at = datetime.fromisoformat(
                    watched_at_str.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                continue

            # Determine runtime
            item_type = entry.get("type", "")
            runtime_min = 0
            if item_type == "movie":
                runtime_min = (entry.get("movie") or {}).get("runtime", 0) or 0
                type_split["movies"] += 1
            elif item_type == "episode":
                runtime_min = (entry.get("episode") or {}).get("runtime", 0) or 0
                if not runtime_min:
                    runtime_min = (entry.get("show") or {}).get("runtime", 0) or 0
                type_split["episodes"] += 1

            runtime_hrs = runtime_min / 60.0

            # Accumulate periods
            hours_all += runtime_hrs
            if watched_at >= year_ago:
                hours_year += runtime_hrs
            if watched_at >= month_ago:
                hours_month += runtime_hrs
            if watched_at >= week_ago:
                hours_week += runtime_hrs

            # Peak hours
            peak_hours[watched_at.hour] += 1

            # Genre counts
            genres = []
            if item_type == "movie":
                genres = (entry.get("movie") or {}).get("genres", [])
            elif item_type == "episode":
                genres = (entry.get("show") or {}).get("genres", [])
            for g in genres:
                genre_counts[g] += 1

            # Daily buckets (last 30 days)
            day_key = watched_at.strftime("%Y-%m-%d")
            watching_days.add(day_key)
            if day_key in daily_buckets:
                daily_buckets[day_key]["minutes"] += runtime_min
                daily_buckets[day_key]["count"] += 1

            # Year-over-year monthly accumulation
            monthly_by_year[watched_at.year][watched_at.month] += runtime_hrs

            # Daily heatmap (last 365 days)
            if day_key in heatmap_buckets:
                heatmap_buckets[day_key]["count"] += 1
                heatmap_buckets[day_key]["minutes"] += runtime_min

        # Sort daily buckets chronologically
        recent_daily = sorted(daily_buckets.values(), key=lambda d: d["date"])
        for d in recent_daily:
            d["hours"] = round(d["minutes"] / 60, 1)
            del d["minutes"]

        # Sort genres by count descending
        sorted_genres = dict(
            sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)
        )

        avg_per_session = (
            round(len(history) / len(watching_days), 1)
            if watching_days else 0
        )

        # Build YoY monthly data: [{year, months: [hours for Jan..Dec]}]
        yoy_data = []
        for year in sorted(monthly_by_year.keys()):
            months = [round(monthly_by_year[year].get(m, 0), 1) for m in range(1, 13)]
            yoy_data.append({"year": year, "months": months})

        # Build heatmap: sorted list of {date, count, hours}
        heatmap_data = sorted(heatmap_buckets.values(), key=lambda d: d["date"])
        for d in heatmap_data:
            d["hours"] = round(d["minutes"] / 60, 1)
            del d["minutes"]

        return {
            "hours_by_period": {
                "week": round(hours_week, 1),
                "month": round(hours_month, 1),
                "year": round(hours_year, 1),
                "all_time": round(hours_all, 1),
            },
            "genre_counts": sorted_genres,
            "peak_hours": peak_hours,
            "type_split": type_split,
            "recent_daily": recent_daily,
            "avg_per_session": avg_per_session,
            "total_items": len(history),
            "monthly_by_year": yoy_data,
            "daily_heatmap": heatmap_data,
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

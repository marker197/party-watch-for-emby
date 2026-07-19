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
from app.utils.emby_client import EmbyClient
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

        cache_key = f"watch_stats_v4:{user.id}"
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
                "top_shows": [],
                "top_networks": [],
                "fun_stats": {},
                "top_actors": [],
                "top_directors": [],
                "top_studios": [],
            }

        result = self._aggregate(history)

        # Fetch people/studio data from Emby (single paginated call)
        people_data = await self._fetch_emby_people(user)
        result["top_actors"] = people_data.get("actors", [])
        result["top_directors"] = people_data.get("directors", [])
        result["top_studios"] = people_data.get("studios", [])

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

        # New collectors
        show_episodes: dict[str, int] = defaultdict(int)          # show title → ep count
        show_hours: dict[str, float] = defaultdict(float)         # show title → hours
        network_counts: dict[str, int] = defaultdict(int)         # network → count
        movie_titles: list[dict] = []                             # for longest movie etc
        day_of_week_counts = [0] * 7                              # Mon=0 .. Sun=6
        daily_item_counts: dict[str, int] = defaultdict(int)      # date → count (all time)
        daily_item_titles: dict[str, list] = defaultdict(list)    # date → list of titles
        first_watched_at = None
        latest_watched_at = None

        for entry in history:
            watched_at_str = entry.get("watched_at", "")
            try:
                watched_at = datetime.fromisoformat(
                    watched_at_str.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                continue

            # Track first/latest
            if first_watched_at is None or watched_at < first_watched_at:
                first_watched_at = watched_at
            if latest_watched_at is None or watched_at > latest_watched_at:
                latest_watched_at = watched_at

            # Determine runtime
            item_type = entry.get("type", "")
            runtime_min = 0
            if item_type == "movie":
                movie_data = entry.get("movie") or {}
                runtime_min = movie_data.get("runtime", 0) or 0
                type_split["movies"] += 1
                if movie_data.get("title"):
                    movie_titles.append({
                        "title": movie_data["title"],
                        "year": movie_data.get("year"),
                        "runtime": runtime_min,
                    })
            elif item_type == "episode":
                ep_data = entry.get("episode") or {}
                show_data = entry.get("show") or {}
                runtime_min = ep_data.get("runtime", 0) or 0
                if not runtime_min:
                    runtime_min = show_data.get("runtime", 0) or 0
                type_split["episodes"] += 1
                show_name = show_data.get("title", "Unknown")
                show_episodes[show_name] += 1
                show_hours[show_name] += runtime_min / 60.0
                network = show_data.get("network")
                if network:
                    network_counts[network] += 1

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

            # Day of week (Monday=0)
            day_of_week_counts[watched_at.weekday()] += 1

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
            daily_item_counts[day_key] += 1
            # Track title for binge detail
            if item_type == "movie":
                _title = (entry.get("movie") or {}).get("title", "Unknown")
            elif item_type == "episode":
                _show = (entry.get("show") or {}).get("title", "Unknown")
                _ep = entry.get("episode") or {}
                _sn = _ep.get("season", 0)
                _en = _ep.get("number", 0)
                _title = f"{_show} S{_sn:02d}E{_en:02d}"
            else:
                _title = "Unknown"
            daily_item_titles[day_key].append(_title)
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

        # ── New: Top Shows (by episode count) ──
        top_shows = [
            {"title": t, "episodes": c, "hours": round(show_hours[t], 1)}
            for t, c in sorted(show_episodes.items(), key=lambda x: x[1], reverse=True)[:15]
        ]

        # ── New: Top Networks ──
        top_networks = [
            {"name": n, "count": c}
            for n, c in sorted(network_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]

        # ── New: Fun stats ──
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        fav_day_idx = max(range(7), key=lambda i: day_of_week_counts[i]) if any(day_of_week_counts) else 0

        # Biggest binge day
        biggest_binge_date = None
        biggest_binge_count = 0
        biggest_binge_items: list[str] = []
        for d, c in daily_item_counts.items():
            if c > biggest_binge_count:
                biggest_binge_count = c
                biggest_binge_date = d
                biggest_binge_items = daily_item_titles.get(d, [])

        # Deduplicate binge items (keep order)
        seen: set[str] = set()
        binge_deduped: list[str] = []
        for t in biggest_binge_items:
            if t not in seen:
                seen.add(t)
                binge_deduped.append(t)

        # Watch streak (consecutive days)
        sorted_days = sorted(watching_days)
        current_streak = 0
        longest_streak = 0
        streak_start = None
        streak_end = None
        cur_start = None
        for i, day_str in enumerate(sorted_days):
            if i == 0:
                current_streak = 1
                cur_start = day_str
            else:
                prev = datetime.strptime(sorted_days[i - 1], "%Y-%m-%d")
                curr = datetime.strptime(day_str, "%Y-%m-%d")
                if (curr - prev).days == 1:
                    current_streak += 1
                else:
                    current_streak = 1
                    cur_start = day_str
            if current_streak > longest_streak:
                longest_streak = current_streak
                streak_start = cur_start
                streak_end = day_str

        # Longest movie
        longest_movie = None
        if movie_titles:
            lm = max(movie_titles, key=lambda m: m["runtime"])
            if lm["runtime"] > 0:
                longest_movie = {
                    "title": lm["title"],
                    "year": lm.get("year"),
                    "runtime_min": lm["runtime"],
                }

        # Unique movies / unique shows
        unique_movies = len({m["title"] for m in movie_titles})
        unique_shows = len(show_episodes)

        fun_stats = {
            "favourite_day": day_names[fav_day_idx],
            "day_of_week_counts": {day_names[i]: day_of_week_counts[i] for i in range(7)},
            "biggest_binge": {"date": biggest_binge_date, "count": biggest_binge_count, "items": binge_deduped},
            "longest_streak_days": longest_streak,
            "longest_streak_start": streak_start,
            "longest_streak_end": streak_end,
            "unique_movies": unique_movies,
            "unique_shows": unique_shows,
            "longest_movie": longest_movie,
            "first_watched": first_watched_at.isoformat() if first_watched_at else None,
            "days_tracked": (now - first_watched_at).days if first_watched_at else 0,
        }

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
            "top_shows": top_shows,
            "top_networks": top_networks,
            "fun_stats": fun_stats,
        }

    async def _fetch_emby_people(self, user: User) -> dict:
        """Query Emby for played items' People and Studios metadata."""
        actor_titles: dict[str, set] = defaultdict(set)
        director_titles: dict[str, set] = defaultdict(set)
        studio_counts: dict[str, int] = defaultdict(int)

        if not user.emby_user_id:
            return {"actors": [], "directors": [], "studios": []}

        emby = EmbyClient()
        try:
            start = 0
            batch = 500
            while True:
                resp = await emby.get_items(
                    user_id=user.emby_user_id,
                    fields="People,Studios",
                    filters="IsPlayed",
                    item_type="Movie",
                    recursive=True,
                    limit=batch,
                    start_index=start,
                )
                items = resp.get("Items", [])
                for item in items:
                    title = item.get("Name", "Unknown")
                    year = item.get("ProductionYear")
                    display = f"{title} ({year})" if year else title
                    for person in item.get("People", []):
                        name = person.get("Name")
                        if not name:
                            continue
                        role = person.get("Type", "")
                        if role == "Actor":
                            actor_titles[name].add(display)
                        elif role == "Director":
                            director_titles[name].add(display)
                    for studio in item.get("Studios", []):
                        sname = studio.get("Name") if isinstance(studio, dict) else studio
                        if sname:
                            studio_counts[sname] += 1
                if start + batch >= resp.get("TotalRecordCount", 0):
                    break
                start += batch
        except Exception:
            log.warning("watch_stats.emby_people_failed", user_id=user.id)
        finally:
            await emby.close()

        top_actors = [
            {"name": n, "count": len(t), "titles": sorted(t)}
            for n, t in sorted(actor_titles.items(), key=lambda x: len(x[1]), reverse=True)[:15]
        ]
        top_directors = [
            {"name": n, "count": len(t), "titles": sorted(t)}
            for n, t in sorted(director_titles.items(), key=lambda x: len(x[1]), reverse=True)[:15]
        ]
        top_studios = [
            {"name": n, "count": c}
            for n, c in sorted(studio_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]

        return {"actors": top_actors, "directors": top_directors, "studios": top_studios}

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

"""Watch History Stats — aggregate local watch_history DB into viewable stats.

Queries the PostgreSQL watch_history table directly — no external API calls.
Results are cached in Redis for 10 minutes.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import structlog
from sqlalchemy import select, func, extract, distinct, text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User, WatchHistory
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.database import async_session

log = structlog.get_logger()

STATS_CACHE_TTL = 600  # 10 minutes (was 1 hour with Trakt API)


class WatchStatsService:

    async def get_stats(self, user: User) -> dict:
        """Return aggregated watch history stats from local DB."""
        cache_key = f"watch_stats_v5:{user.id}"
        try:
            r = await get_redis()
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

        async with async_session() as db:
            result = await self._aggregate_from_db(db, user)

        # Fetch people/studio data from Emby (unchanged — still Emby API)
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
    # DB-backed aggregation
    # ------------------------------------------------------------------

    async def _aggregate_from_db(self, db: AsyncSession, user: User) -> dict:
        """Build the full stats dict from watch_history rows."""
        uid = user.id
        base = WatchHistory.user_id == uid
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        year_ago = now - timedelta(days=365)

        # ── Period hours ──
        async def _hours_since(cutoff):
            r = await db.execute(
                select(func.coalesce(func.sum(WatchHistory.runtime_minutes), 0))
                .where(base, WatchHistory.watched_at >= cutoff)
            )
            return round((r.scalar() or 0) / 60, 1)

        hours_week = await _hours_since(week_ago)
        hours_month = await _hours_since(month_ago)
        hours_year = await _hours_since(year_ago)
        total_min = (await db.execute(
            select(func.coalesce(func.sum(WatchHistory.runtime_minutes), 0)).where(base)
        )).scalar() or 0
        hours_all = round(total_min / 60, 1)

        # ── Type split ──
        movies_count = (await db.execute(
            select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "movie")
        )).scalar() or 0
        episodes_count = (await db.execute(
            select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "episode")
        )).scalar() or 0
        total_items = movies_count + episodes_count

        # ── Peak hours ──
        peak_q = (
            select(
                extract("hour", WatchHistory.watched_at).label("hr"),
                func.count(WatchHistory.id).label("cnt"),
            )
            .where(base)
            .group_by("hr")
        )
        peak_raw = {int(r.hr): r.cnt for r in (await db.execute(peak_q)).all()}
        peak_hours = [peak_raw.get(h, 0) for h in range(24)]

        # ── Genre counts ──
        genre_counts = await self._count_genres(db, uid)

        # ── Recent daily (last 30 days) ──
        daily_q = (
            select(
                func.date(WatchHistory.watched_at).label("d"),
                func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("mins"),
                func.count(WatchHistory.id).label("cnt"),
            )
            .where(base, WatchHistory.watched_at >= month_ago)
            .group_by("d")
            .order_by("d")
        )
        daily_map = {str(r.d): {"hours": round(r.mins / 60, 1), "count": r.cnt}
                     for r in (await db.execute(daily_q)).all()}
        recent_daily = []
        for i in range(30):
            d = (now - timedelta(days=29 - i)).strftime("%Y-%m-%d")
            entry = daily_map.get(d, {"hours": 0, "count": 0})
            recent_daily.append({"date": d, **entry})

        # ── Avg items per watching day ──
        watching_days_count = (await db.execute(
            select(func.count(distinct(func.date(WatchHistory.watched_at)))).where(base)
        )).scalar() or 0
        avg_per_session = round(total_items / watching_days_count, 1) if watching_days_count else 0

        # ── Year-over-year monthly ──
        yoy_q = (
            select(
                extract("year", WatchHistory.watched_at).label("yr"),
                extract("month", WatchHistory.watched_at).label("mo"),
                func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("mins"),
            )
            .where(base)
            .group_by("yr", "mo")
            .order_by("yr", "mo")
        )
        yoy_raw = defaultdict(lambda: defaultdict(float))
        for r in (await db.execute(yoy_q)).all():
            yoy_raw[int(r.yr)][int(r.mo)] = round(r.mins / 60, 1)
        yoy_data = [
            {"year": yr, "months": [yoy_raw[yr].get(m, 0) for m in range(1, 13)]}
            for yr in sorted(yoy_raw.keys())
        ]

        # ── Daily heatmap (last 365 days) ──
        heatmap_start = now - timedelta(days=364)
        heatmap_q = (
            select(
                func.date(WatchHistory.watched_at).label("d"),
                func.count(WatchHistory.id).label("cnt"),
                func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("mins"),
            )
            .where(base, WatchHistory.watched_at >= heatmap_start)
            .group_by("d")
            .order_by("d")
        )
        heatmap_map = {}
        for r in (await db.execute(heatmap_q)).all():
            heatmap_map[str(r.d)] = {"count": r.cnt, "hours": round(r.mins / 60, 1)}
        heatmap_data = []
        for i in range(365):
            d = (heatmap_start + timedelta(days=i)).strftime("%Y-%m-%d")
            entry = heatmap_map.get(d, {"count": 0, "hours": 0})
            heatmap_data.append({"date": d, **entry})

        # ── Top shows (by episode count) ──
        top_shows_q = (
            select(
                WatchHistory.series_name,
                func.count(WatchHistory.id).label("eps"),
                func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("mins"),
            )
            .where(base, WatchHistory.item_type == "episode",
                   WatchHistory.series_name.isnot(None))
            .group_by(WatchHistory.series_name)
            .order_by(func.count(WatchHistory.id).desc())
            .limit(15)
        )
        top_shows = [
            {"title": r.series_name, "episodes": r.eps, "hours": round(r.mins / 60, 1)}
            for r in (await db.execute(top_shows_q)).all()
        ]

        # ── Top networks — not available in watch_history, leave empty ──
        # (Emby people endpoint handles actors/directors/studios)
        top_networks = []

        # ── Fun stats ──
        fun_stats = await self._build_fun_stats(db, uid, now, total_items, watching_days_count)

        # ── NEW: Most rewatched (movies with 2+ plays) ──
        most_rewatched_q = (
            select(
                WatchHistory.title,
                WatchHistory.imdb_id,
                func.count(WatchHistory.id).label("plays"),
                func.max(WatchHistory.watched_at).label("last_watched"),
            )
            .where(base, WatchHistory.item_type == "movie")
            .group_by(WatchHistory.title, WatchHistory.imdb_id)
            .having(func.count(WatchHistory.id) > 1)
            .order_by(func.count(WatchHistory.id).desc())
            .limit(10)
        )
        most_rewatched = [
            {"title": r.title, "imdb_id": r.imdb_id, "plays": r.plays,
             "last_watched": r.last_watched.isoformat() if r.last_watched else None}
            for r in (await db.execute(most_rewatched_q)).all()
        ]

        # ── NEW: Source breakdown ──
        source_q = (
            select(
                WatchHistory.source,
                func.count(WatchHistory.id).label("cnt"),
            )
            .where(base)
            .group_by(WatchHistory.source)
        )
        source_breakdown = {r.source: r.cnt for r in (await db.execute(source_q)).all()}

        # ── NEW: First watch vs rewatch ratio ──
        # Count titles with exactly 1 play vs 2+ plays (movies only)
        title_plays_q = (
            select(
                WatchHistory.title,
                func.count(WatchHistory.id).label("plays"),
            )
            .where(base, WatchHistory.item_type == "movie")
            .group_by(WatchHistory.title)
        )
        title_plays = (await db.execute(title_plays_q)).all()
        first_watch_count = sum(1 for r in title_plays if r.plays == 1)
        rewatch_count = sum(1 for r in title_plays if r.plays > 1)

        log.info("watch_stats.aggregated_from_db", user_id=uid,
                 total_items=total_items, genres_counted=len(genre_counts))

        return {
            "hours_by_period": {
                "week": hours_week,
                "month": hours_month,
                "year": hours_year,
                "all_time": hours_all,
            },
            "genre_counts": genre_counts,
            "peak_hours": peak_hours,
            "type_split": {"movies": movies_count, "episodes": episodes_count},
            "recent_daily": recent_daily,
            "avg_per_session": avg_per_session,
            "total_items": total_items,
            "monthly_by_year": yoy_data,
            "daily_heatmap": heatmap_data,
            "top_shows": top_shows,
            "top_networks": top_networks,
            "fun_stats": fun_stats,
            "most_rewatched": most_rewatched,
            "source_breakdown": source_breakdown,
            "rewatch_ratio": {
                "first_watch": first_watch_count,
                "rewatched": rewatch_count,
            },
        }

    async def _count_genres(self, db: AsyncSession, user_id: int) -> dict:
        """Parse comma-separated genres column and count occurrences."""
        rows = (await db.execute(
            select(WatchHistory.genres)
            .where(WatchHistory.user_id == user_id, WatchHistory.genres.isnot(None))
        )).scalars().all()

        counts: dict[str, int] = defaultdict(int)
        for raw in rows:
            for g in raw.split(","):
                g = g.strip()
                if g:
                    counts[g] += 1

        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))

    async def _build_fun_stats(self, db: AsyncSession, uid: int,
                                now: datetime, total_items: int,
                                watching_days_count: int) -> dict:
        """Build fun stats dict from DB queries."""
        base = WatchHistory.user_id == uid

        # Day of week counts
        dow_q = (
            select(
                extract("dow", WatchHistory.watched_at).label("dow"),
                func.count(WatchHistory.id).label("cnt"),
            )
            .where(base)
            .group_by("dow")
        )
        # PostgreSQL dow: 0=Sunday .. 6=Saturday → remap to Monday-based
        pg_dow = {int(r.dow): r.cnt for r in (await db.execute(dow_q)).all()}
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        # pg: 0=Sun,1=Mon..6=Sat → python Mon=0..Sun=6
        py_dow = [0] * 7
        for pg_d, cnt in pg_dow.items():
            py_idx = (pg_d - 1) % 7  # Mon=0, Tue=1, ..., Sun=6
            py_dow[py_idx] = cnt
        fav_day_idx = max(range(7), key=lambda i: py_dow[i]) if any(py_dow) else 0

        # Biggest binge day
        daily_q = (
            select(
                func.date(WatchHistory.watched_at).label("d"),
                func.count(WatchHistory.id).label("cnt"),
            )
            .where(base)
            .group_by("d")
            .order_by(func.count(WatchHistory.id).desc())
            .limit(1)
        )
        binge_row = (await db.execute(daily_q)).first()
        biggest_binge_date = str(binge_row.d) if binge_row else None
        biggest_binge_count = binge_row.cnt if binge_row else 0

        # Binge day items
        binge_items: list[str] = []
        if biggest_binge_date:
            binge_date_parsed = datetime.strptime(biggest_binge_date, "%Y-%m-%d")
            binge_next = binge_date_parsed + timedelta(days=1)
            items_q = (
                select(WatchHistory.title, WatchHistory.series_name,
                       WatchHistory.season_number, WatchHistory.episode_number,
                       WatchHistory.item_type)
                .where(base,
                       WatchHistory.watched_at >= binge_date_parsed,
                       WatchHistory.watched_at < binge_next)
                .order_by(WatchHistory.watched_at)
            )
            seen: set[str] = set()
            for r in (await db.execute(items_q)).all():
                if r.item_type == "episode" and r.series_name:
                    label = f"{r.series_name} S{(r.season_number or 0):02d}E{(r.episode_number or 0):02d}"
                else:
                    label = r.title or "Unknown"
                if label not in seen:
                    seen.add(label)
                    binge_items.append(label)

        # Watch streak (consecutive days)
        streak_q = sa_text(
            "SELECT date_trunc('day', watched_at) AS d "
            "FROM watch_history WHERE user_id = :uid "
            "GROUP BY d ORDER BY d"
        )
        date_rows = (await db.execute(streak_q, {"uid": uid})).scalars().all()
        longest_streak = 0
        streak_start = None
        streak_end = None
        if date_rows:
            dates_list = sorted(set(
                d.date() if hasattr(d, "date") else d for d in date_rows
            ))
            streak = 1
            cur_start = dates_list[0]
            best_start = cur_start
            best_end = cur_start
            for i in range(1, len(dates_list)):
                if (dates_list[i] - dates_list[i - 1]).days == 1:
                    streak += 1
                else:
                    if streak > longest_streak:
                        longest_streak = streak
                        best_start = cur_start
                        best_end = dates_list[i - 1]
                    streak = 1
                    cur_start = dates_list[i]
            if streak > longest_streak:
                longest_streak = streak
                best_start = cur_start
                best_end = dates_list[-1]
            streak_start = best_start.isoformat() if hasattr(best_start, "isoformat") else str(best_start)
            streak_end = best_end.isoformat() if hasattr(best_end, "isoformat") else str(best_end)

        # Unique movies / shows
        unique_movies = (await db.execute(
            select(func.count(distinct(WatchHistory.title)))
            .where(base, WatchHistory.item_type == "movie")
        )).scalar() or 0
        unique_shows = (await db.execute(
            select(func.count(distinct(WatchHistory.series_name)))
            .where(base, WatchHistory.item_type == "episode",
                   WatchHistory.series_name.isnot(None))
        )).scalar() or 0

        # Longest movie
        longest_movie_q = (
            select(WatchHistory.title, WatchHistory.runtime_minutes,
                   extract("year", WatchHistory.watched_at).label("yr"))
            .where(base, WatchHistory.item_type == "movie",
                   WatchHistory.runtime_minutes.isnot(None))
            .order_by(WatchHistory.runtime_minutes.desc())
            .limit(1)
        )
        lm_row = (await db.execute(longest_movie_q)).first()
        longest_movie = None
        if lm_row and lm_row.runtime_minutes:
            longest_movie = {
                "title": lm_row.title,
                "year": int(lm_row.yr) if lm_row.yr else None,
                "runtime_min": lm_row.runtime_minutes,
            }

        # First watched date
        first_q = (
            select(func.min(WatchHistory.watched_at)).where(base)
        )
        first_watched_at = (await db.execute(first_q)).scalar()
        days_tracked = (now - first_watched_at).days if first_watched_at else 0

        return {
            "favourite_day": day_names[fav_day_idx],
            "day_of_week_counts": {day_names[i]: py_dow[i] for i in range(7)},
            "biggest_binge": {
                "date": biggest_binge_date,
                "count": biggest_binge_count,
                "items": binge_items,
            },
            "longest_streak_days": longest_streak,
            "longest_streak_start": streak_start,
            "longest_streak_end": streak_end,
            "unique_movies": unique_movies,
            "unique_shows": unique_shows,
            "longest_movie": longest_movie,
            "first_watched": first_watched_at.isoformat() if first_watched_at else None,
            "days_tracked": days_tracked,
        }

    # ------------------------------------------------------------------
    # Emby people/studios (unchanged — still queries Emby)
    # ------------------------------------------------------------------

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

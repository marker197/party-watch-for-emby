"""Rating Bias Detector Service Implementation.

Analyzes user's Simkl rating history to identify patterns, biases, and blind spots.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import User, UserRating, RatingBias
from app.utils.simkl_client import SimklClient
from app.utils.emby_client import EmbyClient
from app.utils.database import async_session

log = structlog.get_logger()

ALL_GENRES = [
    "action", "adventure", "animation", "anime", "comedy", "crime",
    "documentary", "drama", "family", "fantasy", "history", "horror",
    "music", "mystery", "romance", "science-fiction", "sport",
    "superhero", "thriller", "war", "western",
]

DECADES = [1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020]


class RatingBiasDetectorService:
    """Analyzes rating patterns to identify biases, blind spots, and opportunities."""

    def __init__(self):
        self.emby = EmbyClient()

    async def analyze_for_all_users(self):
        """Scheduler entry point — analyze bias for every linked user."""
        log.info("bias_detector.analyze_start")
        async with async_session() as db:
            users = (await db.execute(
                select(User).where(User.simkl_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self.analyze_user(user)
            except Exception:
                log.exception("bias_detector.analyze_error", user_id=user.id)

        log.info("bias_detector.analyze_complete", users=len(users))

    async def analyze_user(self, user: User) -> dict:
        """Full bias analysis pipeline for a single user."""
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

        try:
            # 1. Fetch ratings from DB (cached by ML predictor)
            async with async_session() as db:
                ratings_rows = (await db.execute(
                    select(UserRating).where(UserRating.user_id == user.id)
                )).scalars().all()

            if len(ratings_rows) < 15:
                log.warning("bias_detector.too_few_ratings", user=user.emby_username, count=len(ratings_rows))
                return {"status": "skipped", "reason": "fewer than 15 ratings"}

            # Convert to dicts for analysis
            ratings = [self._rating_to_dict(r) for r in ratings_rows]

            # Enrich community ratings from MDBList cache/API
            await self._enrich_community_ratings(ratings, ratings_rows)

            # 2. Compute overall stats
            overall_stats = self._compute_overall_stats(ratings)

            # 3. Compute genre biases
            genre_stats = self._compute_genre_stats(ratings)

            # 4. Compute era/decade biases
            era_stats = self._compute_era_stats(ratings)

            # 5. Compute rating curve (distribution)
            rating_curve = self._compute_rating_curve(ratings)

            # 6. Find hidden gems (overrated by community, underrated by user)
            hidden_gems = self._find_hidden_gems(ratings)

            # 7. Find challenge opportunities
            challenges = self._generate_challenges(ratings, genre_stats, era_stats)

            # 8. Identify blind spots
            blind_spots = self._identify_blind_spots(ratings, genre_stats)

            # 9. Find "against your taste" candidates
            against_taste = self._find_against_taste(ratings, genre_stats)

            # 10. Store results in DB
            bias_report = {
                "overall": overall_stats,
                "genre_biases": genre_stats,
                "era_biases": era_stats,
                "rating_curve": rating_curve,
                "hidden_gems": hidden_gems,
                "challenges": challenges,
                "blind_spots": blind_spots,
                "against_taste": against_taste,
            }

            async with async_session() as db:
                # Delete old report
                await db.execute(delete(RatingBias).where(RatingBias.user_id == user.id))

                db.add(RatingBias(
                    user_id=user.id,
                    total_ratings=len(ratings),
                    analysis_json=bias_report,
                ))
                await db.commit()

            result = {
                "status": "analyzed",
                "total_ratings": len(ratings),
                "avg_rating": round(overall_stats["avg_rating"], 2),
                "median_rating": round(overall_stats["median_rating"], 2),
                "bias_genres": len(genre_stats),
                "challenges_found": len(challenges),
                "hidden_gems": len(hidden_gems),
            }
            log.info("bias_detector.analyzed", user=user.emby_username, **result)
            return result

        finally:
            await simkl.close()

    async def get_bias_report(self, user_id: int) -> dict | None:
        """Retrieve full bias report for a user."""
        async with async_session() as db:
            row = (await db.execute(
                select(RatingBias).where(RatingBias.user_id == user_id)
            )).scalar_one_or_none()

        if not row:
            return None

        return {
            "generated_at": row.analyzed_at.isoformat(),
            "total_ratings": row.total_ratings,
            "analysis": row.analysis_json,
        }

    async def get_hidden_gems(self, user_id: int, limit: int = 20) -> list[dict]:
        """Get items user should rate higher based on patterns."""
        report = await self.get_bias_report(user_id)
        if not report:
            return []

        gems = report["analysis"].get("hidden_gems", [])
        return gems[:limit]

    async def get_challenges(self, user_id: int) -> list[dict]:
        """Get rating challenges to help explore blind spots."""
        report = await self.get_bias_report(user_id)
        if not report:
            return []

        return report["analysis"].get("challenges", [])

    # =========================================================================
    # Private Analysis Methods
    # =========================================================================

    def _rating_to_dict(self, rating: UserRating) -> dict:
        """Convert UserRating ORM object to dict."""
        return {
            "title": rating.title,
            "rating": rating.rating,
            "genres": rating.genres or [],
            "year": rating.year or 2000,
            "simkl_rating": rating.simkl_rating or 0,
            "item_type": rating.item_type or "movie",
        }

    async def _enrich_community_ratings(self, ratings: list[dict], db_rows: list) -> None:
        """Populate simkl_rating from MDBList cache/API for items missing it.

        Mutates ``ratings`` in place.  Uses the same Redis cache keys as the
        Smart Queue MDBList enrichment (``mdblist_media_info:imdb:{type}:{id}``,
        24h TTL) so most items are already cached.
        """
        import asyncio
        from app.utils.redis_cache import get_redis
        from app.utils.secure_redis import secure_get

        need: list[tuple[int, str, str]] = []  # (index, imdb_id, media_type)
        for i, (rd, row) in enumerate(zip(ratings, db_rows)):
            if rd.get("simkl_rating"):
                continue
            imdb = getattr(row, "imdb_id", None) or ""
            if not imdb:
                continue
            mtype = "show" if rd.get("item_type") == "show" else "movie"
            need.append((i, imdb, mtype))

        if not need:
            return

        r = await get_redis()
        uncached: list[tuple[int, str, str]] = []

        # Check Redis cache first
        for idx, imdb, mtype in need:
            cache_key = f"mdblist_media_info:imdb:{mtype}:{imdb}"
            try:
                raw = await r.get(cache_key)
                if raw:
                    data = json.loads(raw)
                    cr = self._extract_community_rating(data)
                    if cr:
                        ratings[idx]["simkl_rating"] = cr
                    continue
            except Exception:
                pass
            uncached.append((idx, imdb, mtype))

        # Fetch uncached from MDBList API
        if not uncached:
            return
        mdb_key = await secure_get("mdblist_api_key")
        if not mdb_key:
            return

        from app.utils.mdblist_client import MDBListClient
        sem = asyncio.Semaphore(5)
        client = MDBListClient(api_key=mdb_key)
        try:
            async def _fetch(idx: int, imdb: str, mtype: str):
                async with sem:
                    try:
                        data = await client.get_media_info("imdb", mtype, imdb)
                        if data:
                            cache_key = f"mdblist_media_info:imdb:{mtype}:{imdb}"
                            try:
                                await r.set(cache_key, json.dumps(data), ex=86400)
                            except Exception:
                                pass
                            cr = self._extract_community_rating(data)
                            if cr:
                                ratings[idx]["simkl_rating"] = cr
                    except Exception:
                        pass

            await asyncio.gather(*[_fetch(i, im, mt) for i, im, mt in uncached])
        finally:
            await client.close()

        enriched = sum(1 for rd in ratings if rd.get("simkl_rating"))
        log.debug("bias_detector.community_ratings_enriched",
                  total=len(ratings), enriched=enriched, fetched=len(uncached))

    @staticmethod
    def _extract_community_rating(mdblist_data: dict) -> float | None:
        """Extract IMDb community rating from MDBList media info response."""
        # Check ratings array first (primary)
        for entry in mdblist_data.get("ratings", []):
            if entry.get("source") == "imdb" and entry.get("value"):
                try:
                    return round(float(entry["value"]), 1)
                except (ValueError, TypeError):
                    pass
        # Fallback to top-level field
        val = mdblist_data.get("imdbrating")
        if val:
            try:
                return round(float(val), 1)
            except (ValueError, TypeError):
                pass
        return None

    def _compute_overall_stats(self, ratings: list[dict]) -> dict:
        """Compute overall rating statistics."""
        user_ratings = [r["rating"] for r in ratings]
        return {
            "count": len(ratings),
            "avg_rating": np.mean(user_ratings),
            "median_rating": np.median(user_ratings),
            "std_dev": np.std(user_ratings),
            "min_rating": min(user_ratings),
            "max_rating": max(user_ratings),
            "rating_distribution": {
                f"{i}": sum(1 for r in user_ratings if i <= r < i + 1)
                for i in range(1, 10)
            },
        }

    def _compute_genre_stats(self, ratings: list[dict]) -> dict:
        """Analyze rating patterns by genre."""
        genre_ratings = defaultdict(list)
        genre_community = defaultdict(list)
        genre_counts = defaultdict(int)

        for r in ratings:
            genres = r.get("genres", [])
            if not genres:
                genres = ["unknown"]
            for g in genres:
                g_lower = g.lower()
                genre_ratings[g_lower].append(r["rating"])
                genre_counts[g_lower] += 1
                if r.get("simkl_rating"):
                    genre_community[g_lower].append(r["simkl_rating"])

        stats = {}
        for genre in sorted(genre_ratings.keys()):
            ratings_list = genre_ratings[genre]
            community_list = genre_community.get(genre, [])
            community_avg = round(np.mean(community_list), 2) if community_list else 5.0
            stats[genre] = {
                "count": len(ratings_list),
                "avg": round(np.mean(ratings_list), 2),
                "median": round(np.median(ratings_list), 2),
                "std_dev": round(np.std(ratings_list), 2),
                "min": min(ratings_list),
                "max": max(ratings_list),
                "community_avg": community_avg,
                "bias_score": round(np.mean(ratings_list) - community_avg, 2),
            }

        return stats

    def _compute_era_stats(self, ratings: list[dict]) -> dict:
        """Analyze rating patterns by decade."""
        era_ratings = defaultdict(list)

        for r in ratings:
            year = r.get("year", 2000)
            decade = (year // 10) * 10
            era_ratings[decade].append(r["rating"])

        stats = {}
        for decade in sorted(era_ratings.keys()):
            ratings_list = era_ratings[decade]
            stats[f"{decade}s"] = {
                "count": len(ratings_list),
                "avg": round(np.mean(ratings_list), 2),
                "median": round(np.median(ratings_list), 2),
                "range": f"{min(ratings_list)}-{max(ratings_list)}",
                "bias_score": round(np.mean(ratings_list) - 5.0, 2),
            }

        return stats

    def _compute_rating_curve(self, ratings: list[dict]) -> dict:
        """Compute full rating distribution curve."""
        user_ratings = [r["rating"] for r in ratings]
        curve = {}
        for i in np.arange(1.0, 10.5, 0.5):
            curve[f"{i:.1f}"] = sum(1 for r in user_ratings if abs(r - i) < 0.25)
        return curve

    def _find_hidden_gems(self, ratings: list[dict]) -> list[dict]:
        """Find items rated well below the user's own genre average.

        These are titles the user might want to revisit — they scored
        significantly lower than similar content the user usually enjoys.
        """
        # Build per-genre averages from user's own ratings
        from collections import defaultdict
        genre_totals: dict[str, list[float]] = defaultdict(list)
        for r in ratings:
            for g in r.get("genres", []):
                genre_totals[g].append(r["rating"])
        genre_avg = {g: sum(v) / len(v) for g, v in genre_totals.items() if len(v) >= 3}

        gems = []
        for r in ratings:
            user_rating = r["rating"]
            item_genres = r.get("genres", [])
            if not item_genres:
                continue
            # Average of the user's genre averages for this item's genres
            matching = [genre_avg[g] for g in item_genres if g in genre_avg]
            if not matching:
                continue
            expected = sum(matching) / len(matching)
            gap = round(expected - user_rating, 2)
            # Item rated ≥2 points below what the user typically gives this genre mix
            if gap >= 2.0 and expected >= 7.0:
                gems.append({
                    "title": r["title"],
                    "your_rating": user_rating,
                    "community_rating": round(expected, 1),  # reused field = genre avg
                    "gap": gap,
                    "reason": f"You average {round(expected, 1)} in {', '.join(item_genres[:2])}",
                    "genres": item_genres,
                })

        gems.sort(key=lambda x: x["gap"], reverse=True)
        return gems[:15]

    def _generate_challenges(self, ratings: list[dict], genre_stats: dict, era_stats: dict) -> list[dict]:
        """Generate rating challenges to explore blind spots."""
        challenges = []

        # Challenge 1: Explore lowest-rated genre
        lowest_genre = min(
            genre_stats.items(),
            key=lambda x: x[1]["avg"]
        )
        challenges.append({
            "name": f"Challenge: {lowest_genre[0].title()} Appreciation",
            "description": f"You average {lowest_genre[1]['avg']}/10 in {lowest_genre[0]}. Find one you'll love!",
            "target_genre": lowest_genre[0],
            "difficulty": "medium",
        })

        # Challenge 2: Highest-rated exploration
        highest_genre = max(
            genre_stats.items(),
            key=lambda x: x[1]["avg"]
        )
        challenges.append({
            "name": f"Challenge: Beyond {highest_genre[0].title()}",
            "description": f"You excel at {highest_genre[0]} (avg {highest_genre[1]['avg']}/10). Step outside!",
            "target_genre": "any_but_" + highest_genre[0],
            "difficulty": "hard",
        })

        # Challenge 3: Era exploration
        lowest_era = min(
            era_stats.items(),
            key=lambda x: x[1]["avg"]
        )
        challenges.append({
            "name": f"Challenge: Discover the {lowest_era[0]}",
            "description": f"Your lowest-rated era. Find a {lowest_era[0]} gem you'll rate 8+!",
            "target_era": lowest_era[0],
            "difficulty": "medium",
        })

        return challenges

    def _identify_blind_spots(self, ratings: list[dict], genre_stats: dict) -> list[dict]:
        """Identify genres/eras with very few ratings (blind spots)."""
        blind_spots = []
        all_genre_names = set(g.lower() for g in ALL_GENRES)
        rated_genres = set(genre_stats.keys())
        missing_genres = all_genre_names - rated_genres

        for genre in sorted(missing_genres):
            blind_spots.append({
                "type": "genre",
                "name": genre.title(),
                "status": "never_rated",
                "recommendation": f"Try your first {genre} recommendation!",
            })

        # Also find underexplored genres (only 1-2 ratings)
        for genre, stats in genre_stats.items():
            if stats["count"] <= 2:
                blind_spots.append({
                    "type": "genre",
                    "name": genre.title(),
                    "status": "underexplored",
                    "count": stats["count"],
                    "recommendation": f"Only {stats['count']} ratings. Explore more!",
                })

        return blind_spots[:20]

    def _find_against_taste(self, ratings: list[dict], genre_stats: dict) -> list[dict]:
        """Find items that go against your typical taste for exploration."""
        against_taste = []

        # Find the genre you rate highest and lowest
        if not genre_stats:
            return []

        highest_genre = max(
            genre_stats.items(),
            key=lambda x: x[1]["avg"]
        )
        lowest_genre = min(
            genre_stats.items(),
            key=lambda x: x[1]["avg"]
        )

        avg_rating = np.mean([r["rating"] for r in ratings])

        # Create challenge profiles
        against_taste.append({
            "profile": "opposite_genre",
            "name": f"Go Against Your Taste",
            "description": f"You love {highest_genre[0]} but rate {lowest_genre[0]} lower. Find a {lowest_genre[0]} you'll love!",
            "from_genre": highest_genre[0],
            "to_genre": lowest_genre[0],
            "diversity_gain": highest_genre[1]["avg"] - lowest_genre[1]["avg"],
        })

        # Old content if you prefer new
        has_old = any(r.get("year", 2000) < 1990 for r in ratings)
        avg_year = np.mean([r.get("year", 2000) for r in ratings])
        if avg_year > 2000 and has_old:
            against_taste.append({
                "profile": "era_exploration",
                "name": "Retro Discovery",
                "description": f"You mostly watch recent content (avg {int(avg_year)}). Try a classic!",
                "target_era": "pre-1990",
                "diversity_gain": 2.0,  # estimated learning
            })

        return against_taste

    # =========================================================================
    # Emby Integration (find matches for recommendations)
    # =========================================================================

    async def find_library_matches(self, user_id: int, criteria: str) -> list[dict]:
        """Find Emby library items matching a challenge/gem profile.

        criteria:
            - genre:{genre_name}
            - era:{decade}s
            - against:{challenge_name}
        """
        user = None
        async with async_session() as db:
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        if not user:
            return []

        movies = await self.emby.get_all_movies(user_id=user.emby_user_id)
        series = await self.emby.get_all_series(user_id=user.emby_user_id)
        all_items = movies + series

        matches = []
        if criteria.startswith("genre:"):
            genre = criteria.split(":")[1].lower()
            matches = [
                item for item in all_items
                if genre.lower() in [g.lower() for g in item.get("Genres", [])]
            ]
        elif criteria.startswith("era:"):
            era = criteria.split(":")[1]  # "1990s"
            decade = int(era[:4])
            matches = [
                item for item in all_items
                if decade <= item.get("ProductionYear", 2000) < decade + 10
            ]

        return [
            {
                "id": item["Id"],
                "title": item.get("Name", ""),
                "year": item.get("ProductionYear"),
                "genres": item.get("Genres", []),
            }
            for item in matches
        ][:20]

"""
Library Health Monitor Service
Detects gaps, orphaned episodes, missing sequels, and provides acquisition recommendations.

Feature #9: Library Health Monitor
- Incomplete series detection
- Orphaned episode detection (watched episodes without premiere)
- Missing sequel/related content detection
- Director/actor gap analysis
- Health report generation
- Acquisition recommendations
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, desc

from app.models.schema import User, LibraryGap, LibraryHealthReport, UserRating, UniverseItem
from app.utils.trakt_client import TraktClient
from app.utils.library_cache import LibraryCache
from app.utils.emby_client import EmbyClient

logger = logging.getLogger(__name__)


class LibraryHealthMonitor:
    """Monitor library health and detect gaps."""

    # Priority thresholds
    CRITICAL_PRIORITY_RULES = [
        "orphaned_episodes",  # Watched S2E5 but not S1
        "sequel_missing_recent",  # Watched movie, missing sequel within 1 year
    ]

    HIGH_PRIORITY_RULES = [
        "incomplete_series_95pct",  # Have 95% of series
        "missing_director_recent",  # Missing recent work by director
        "missing_actor_lead",  # Missing lead actor's main role
    ]

    MEDIUM_PRIORITY_RULES = [
        "incomplete_series_70pct",  # Have 70% of series
        "missing_acclaimed_work",  # Director/actor has highly-rated film you don't have
    ]

    def __init__(self, db: Session, trakt_client: TraktClient, emby_client: EmbyClient, cache: LibraryCache):
        self.db = db
        self.trakt = trakt_client
        self.emby = emby_client
        self.cache = cache

    async def detect_incomplete_series(self, user_id: int) -> List[Dict]:
        """
        Find TV series where user has watched some episodes but not all.

        Returns:
            [{
                'title': str,
                'total_seasons': int,
                'total_episodes': int,
                'watched_episodes': int,
                'completion_pct': float,
                'missing_seasons': [1, 2, 5],
                'trakt_id': str,
                'your_rating': float
            }, ...]
        """
        try:
            user = (await self.db.execute(select(User).filter(User.id == user_id))).scalars().first()
            if not user:
                return []

            # Get user's watched history
            watched_items = (await self.db.execute(select(UserRating).filter(UserRating.user_id == user_id))).scalars().all()
            watched_by_title = {}
            for item in watched_items:
                if item.item_type == "episode":
                    show_title = item.title.split(" - Season")[0].strip()
                    if show_title not in watched_by_title:
                        watched_by_title[show_title] = []
                    watched_by_title[show_title].append(item)

            incomplete_series = []

            for show_title, episodes in watched_by_title.items():
                if len(episodes) < 3:  # Ignore if only 1-2 episodes
                    continue

                try:
                    # Get full series from Trakt
                    series = await self.trakt.get_show(show_title)
                    if not series:
                        continue

                    total_episodes = series.get('aired_episodes', 0)
                    if total_episodes == 0:
                        continue

                    watched_count = len(episodes)
                    completion_pct = (watched_count / total_episodes) * 100

                    # Find missing seasons
                    watched_seasons = set()
                    for ep in episodes:
                        # Parse season number from episode title (S01E01 format)
                        if " - Season " in ep.title:
                            try:
                                season_str = ep.title.split(" - Season ")[1].split(":")[0]
                                watched_seasons.add(int(season_str))
                            except:
                                pass

                    total_seasons = series.get('status', 'ended') == 'ended' and len(watched_seasons) or \
                                  await self._estimate_seasons(series)

                    missing_seasons = [s for s in range(1, total_seasons + 1) if s not in watched_seasons]

                    if 0 < completion_pct < 100:
                        incomplete_series.append({
                            "title": show_title,
                            "total_seasons": total_seasons,
                            "total_episodes": total_episodes,
                            "watched_episodes": watched_count,
                            "completion_pct": round(completion_pct, 1),
                            "missing_seasons": missing_seasons[:5],  # Top 5
                            "trakt_id": series.get('ids', {}).get('trakt'),
                            "your_rating": episodes[0].rating if episodes else None,
                            "priority": self._calculate_priority("incomplete_series", completion_pct)
                        })

                except Exception as e:
                    logger.warning(f"Error analyzing series {show_title}: {e}")
                    continue

            return sorted(incomplete_series, key=lambda x: x['completion_pct'], reverse=True)

        except Exception as e:
            logger.error(f"Error detecting incomplete series: {e}")
            return []

    async def find_orphaned_episodes(self, user_id: int) -> List[Dict]:
        """
        Find episodes watched without watching the season premiere.
        Example: Watched S2E5 but not S1E1 or S2E1

        Returns:
            [{
                'title': str,
                'show_title': str,
                'episode_number': str,  # S02E05
                'your_rating': float,
                'status': str,  # 'critical' | 'high' | 'medium'
                'missing_premiere': bool,
                'missing_season_premiere': bool
            }, ...]
        """
        try:
            watched_items = (await self.db.execute(select(UserRating).filter(
                and_(UserRating.user_id == user_id, UserRating.item_type == "episode")
            ))).scalars().all()

            orphaned = []
            series_info = {}  # Cache to avoid repeated lookups

            for watched_ep in watched_items:
                try:
                    # Parse show and episode info
                    if " - " not in watched_ep.title:
                        continue

                    parts = watched_ep.title.split(" - ")
                    show_title = parts[0].strip()

                    # Extract season/episode
                    if "Season" not in watched_ep.title:
                        continue

                    season_ep = watched_ep.title.split("Season")[1].strip()
                    if not season_ep or len(season_ep) < 2:
                        continue

                    try:
                        season_num = int(season_ep.split()[0])
                        ep_num = int(season_ep.split("Episode")[1].split(":")[0].strip()) if "Episode" in season_ep else 0
                    except:
                        continue

                    # Get show info
                    if show_title not in series_info:
                        series_info[show_title] = await self.trakt.get_show(show_title)

                    series = series_info.get(show_title)
                    if not series:
                        continue

                    # Check if series premiere is watched
                    premiere_watched = any(
                        ep.title.startswith(show_title) and "Season 1" in ep.title and "Episode 1" in ep.title
                        for ep in watched_items
                    )

                    # Check if season premiere is watched
                    season_premiere_watched = any(
                        ep.title.startswith(show_title) and f"Season {season_num}" in ep.title and "Episode 1" in ep.title
                        for ep in watched_items
                    )

                    if not premiere_watched or (season_num > 1 and not season_premiere_watched):
                        orphaned.append({
                            "title": f"{show_title} - S{season_num:02d}E{ep_num:02d}",
                            "show_title": show_title,
                            "episode_number": f"S{season_num:02d}E{ep_num:02d}",
                            "your_rating": watched_ep.rating,
                            "status": "critical",
                            "missing_premiere": not premiere_watched,
                            "missing_season_premiere": not season_premiere_watched,
                            "priority": "critical"
                        })

                except Exception as e:
                    logger.warning(f"Error analyzing episode {watched_ep.title}: {e}")
                    continue

            return orphaned

        except Exception as e:
            logger.error(f"Error finding orphaned episodes: {e}")
            return []

    async def find_missing_sequels(self, user_id: int) -> List[Dict]:
        """
        Find sequels/prequels user doesn't have but should based on ownership of related.

        Returns:
            [{
                'title': str,
                'original_title': str,
                'year': int,
                'your_rating': float,
                'missing_title': str,
                'missing_year': int,
                'relation': str,  # 'sequel' | 'prequel' | 'spin-off'
                'priority': str
            }, ...]
        """
        try:
            # Get all user's rated movies
            movies = (await self.db.execute(select(UserRating).filter(
                and_(UserRating.user_id == user_id, UserRating.item_type == "movie")
            ))).scalars().all()

            missing_sequels = []

            for movie in movies:
                try:
                    # Get related movies from Trakt
                    movie_details = await self.trakt.get_movie(movie.trakt_slug or movie.title)
                    if not movie_details:
                        continue

                    # Look for sequels/prequels
                    movie_id = movie_details.get('ids', {}).get('trakt')
                    related = await self.trakt.get_movie_related(movie_id)

                    for rel_movie in related:
                        rel_title = rel_movie.get('title')
                        rel_year = rel_movie.get('year')

                        # Check if user has this related movie
                        has_related = any(m.title == rel_title for m in movies)

                        if not has_related:
                            missing_sequels.append({
                                "title": movie.title,
                                "original_year": movie.year,
                                "your_rating": movie.rating,
                                "missing_title": rel_title,
                                "missing_year": rel_year,
                                "relation": "sequel",
                                "priority": self._calculate_priority("missing_sequel", movie.rating),
                                "trakt_id": rel_movie.get('ids', {}).get('trakt')
                            })

                except Exception as e:
                    logger.warning(f"Error finding sequels for {movie.title}: {e}")
                    continue

            return sorted(missing_sequels, key=lambda x: x['your_rating'], reverse=True)[:20]

        except Exception as e:
            logger.error(f"Error finding missing sequels: {e}")
            return []

    async def generate_health_report(self, user_id: int) -> Dict:
        """
        Generate comprehensive library health analysis.

        Returns:
            {
                'total_items': int,
                'unwatched_items': int,
                'incomplete_series': int,
                'orphaned_episodes': int,
                'related_missing': int,
                'series_completion_pct': float,
                'health_score': float,  # 0-100
                'major_gaps': [{gap}],
                'generated_at': datetime
            }
        """
        try:
            # Get library stats
            all_items = (await self.db.execute(select(UserRating).filter(UserRating.user_id == user_id))).scalars().all()
            total_items = len(all_items)

            if total_items == 0:
                return {"error": "No items in library"}

            # Count unwatched (but that's not tracked in UserRating, so estimate)
            unwatched = 0  # Would need separate tracking

            # Get gaps
            incomplete_series = await self.detect_incomplete_series(user_id)
            incomplete_count = len(incomplete_series)

            orphaned_episodes = await self.find_orphaned_episodes(user_id)
            orphaned_count = len(orphaned_episodes)

            missing_sequels = await self.find_missing_sequels(user_id)
            missing_count = len(missing_sequels)

            # Calculate series completion
            series_items = [i for i in all_items if i.item_type == "episode"]
            series_completion_pct = (
                100.0 - (incomplete_count / len(series_items) * 100)
                if series_items else 0.0
            )

            # Calculate health score (0-100)
            health_score = 100.0 - (
                (incomplete_count / max(len(series_items), 1) * 30) +
                (orphaned_count * 5) +
                (missing_count / max(total_items, 1) * 10)
            )
            health_score = max(0, min(100, health_score))

            report = {
                "total_items": total_items,
                "unwatched_items": unwatched,
                "incomplete_series": incomplete_count,
                "orphaned_episodes": orphaned_count,
                "related_missing": missing_count,
                "series_completion_pct": round(series_completion_pct, 1),
                "health_score": round(health_score, 1),
                "major_gaps": (incomplete_series + orphaned_episodes + missing_sequels)[:10],
                "generated_at": datetime.utcnow().isoformat(),
                "recommendations": self._generate_recommendations(
                    incomplete_count, orphaned_count, missing_count, total_items
                )
            }

            # Store in database
            health_record = LibraryHealthReport(
                user_id=user_id,
                total_items=total_items,
                unwatched_items=unwatched,
                incomplete_series=incomplete_count,
                orphaned_episodes=orphaned_count,
                related_missing=missing_count,
                series_completion_pct=series_completion_pct,
                report_json=report
            )
            self.db.add(health_record)
            await self.db.commit()

            return report

        except Exception as e:
            logger.error(f"Error generating health report: {e}")
            await self.db.rollback()
            return {"error": str(e)}

    async def acquisition_recommendations(self, user_id: int, limit: int = 20) -> List[Dict]:
        """
        Recommend what to acquire to close gaps in library.

        Returns:
            [{
                'title': str,
                'type': str,  # 'sequel' | 'prequel' | 'related_series' | 'director_work'
                'reason': str,
                'priority': str,
                'estimated_cost': float,  # rough estimate
                'trakt_rating': float,
                'why_you_should_get_it': str
            }, ...]
        """
        try:
            recommendations = []

            # Get missing sequels as top recommendations
            missing_sequels = await self.find_missing_sequels(user_id)
            for sequel in missing_sequels[:limit // 3]:
                recommendations.append({
                    "title": sequel['missing_title'],
                    "type": "sequel",
                    "reason": f"Sequel to '{sequel['title']}' which you rated {sequel['your_rating']}/10",
                    "priority": sequel['priority'],
                    "estimated_cost": 4.99,  # rental estimate
                    "trakt_rating": None,
                    "why_you_should_get_it": "You enjoyed the original, sequel continues the story"
                })

            # Get series to complete
            incomplete = await self.detect_incomplete_series(user_id)
            for series in incomplete[:limit // 3]:
                if series['completion_pct'] > 70:
                    recommendations.append({
                        "title": series['title'],
                        "type": "complete_series",
                        "reason": f"Complete series: {series['completion_pct']}% done",
                        "priority": "high",
                        "estimated_cost": 19.99,
                        "trakt_rating": None,
                        "why_you_should_get_it": f"Finish the series you're {series['completion_pct']}% through"
                    })

            return recommendations[:limit]

        except Exception as e:
            logger.error(f"Error in acquisition_recommendations: {e}")
            return []

    def _calculate_priority(self, gap_type: str, metric: float) -> str:
        """Determine gap priority based on type and metric."""
        if gap_type in self.CRITICAL_PRIORITY_RULES:
            return "critical"
        if gap_type in self.HIGH_PRIORITY_RULES:
            return "high"
        if gap_type in self.MEDIUM_PRIORITY_RULES:
            return "medium"
        return "low"

    def _generate_recommendations(self, incomplete: int, orphaned: int, missing: int, total: int) -> List[str]:
        """Generate text recommendations based on health."""
        recommendations = []

        if orphaned > 0:
            recommendations.append(f"⚠️ Fix {orphaned} orphaned episodes - watch series from beginning")

        if incomplete > total * 0.2:
            recommendations.append("📺 Consider completing your series - you have many incomplete shows")

        if missing > 0:
            recommendations.append(f"🎬 {missing} related movies/shows available - check if interested")

        if not recommendations:
            recommendations.append("✅ Your library looks healthy!")

        return recommendations

    async def _estimate_seasons(self, series_data: Dict) -> int:
        """Estimate number of seasons from series data."""
        return series_data.get('seasons_count', 1) or 1

"""
Trakt Social Watching Graph Service
Tracks what friends are watching and calculates influence scores.

Feature #6: Trakt Social Watching Graph
- Real-time friend activity tracking
- Influence scoring (how often you watch what they watch)
- Social sync mode for watch parties
- Friend leaderboards
- Shared library overlap analysis
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, desc

from app.models.schema import User, SocialWatching, QueueItem, UserRating
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient

logger = logging.getLogger(__name__)


class SocialWatchingService:
    """Track friends' watching activity and influence."""

    def __init__(self, db: Session, trakt_client: TraktClient, emby_client: EmbyClient):
        self.db = db
        self.trakt = trakt_client
        self.emby = emby_client

    async def sync_friend_activity(self, user_id: int) -> Dict:
        """
        Poll Trakt for all friends' current watching activity.
        Updates the social_watching table with real-time data.

        Returns:
            {
                'friends_synced': int,
                'now_watching': int,
                'newly_discovered': int,
                'friends': [{username, item, rating, in_library}, ...]
            }
        """
        try:
            user = (await self.db.execute(select(User).filter(User.id == user_id))).scalars().first()
            if not user:
                return {"error": "User not found", "friends_synced": 0}

            # Get user's friends from Trakt
            friends = await self.trakt.get_friends(user.trakt_access_token)
            if not friends:
                return {"friends_synced": 0, "now_watching": 0, "newly_discovered": 0, "friends": []}

            now_watching = 0
            newly_discovered = 0
            synced_friends = []

            for friend in friends:
                friend_username = friend.get('user', {}).get('username')
                if not friend_username:
                    continue

                try:
                    # Get friend's currently watched item
                    watching = await self.trakt.get_currently_watching(friend_username)

                    # Upsert to social_watching table
                    social_record = (await self.db.execute(select(SocialWatching).filter(
                        and_(
                            SocialWatching.user_id == user_id,
                            SocialWatching.friend_trakt_username == friend_username
                        )
                    ))).scalars().first()

                    if not social_record:
                        social_record = SocialWatching(
                            user_id=user_id,
                            friend_trakt_username=friend_username,
                            friend_profile_url=friend.get('user', {}).get('images', {}).get('avatar', {}).get('full')
                        )
                        newly_discovered += 1

                    if watching:
                        # Friend is watching something
                        social_record.is_watching = True
                        social_record.current_item_title = watching.get('item', {}).get('title')
                        social_record.current_item_trakt_id = str(watching.get('item', {}).get('ids', {}).get('trakt'))
                        social_record.item_type = watching.get('type')  # 'movie' or 'episode'
                        social_record.started_at = datetime.now(timezone.utc)
                        social_record.last_seen_at = datetime.now(timezone.utc)
                        social_record.friend_rating = watching.get('rating')

                        # Check if item is in user's library
                        is_in_library = await self._check_item_in_library(
                            watching.get('item', {}).get('title'),
                            watching.get('type')
                        )
                        social_record.in_library = is_in_library

                        now_watching += 1
                    else:
                        # Friend not currently watching
                        social_record.is_watching = False
                        social_record.last_seen_at = datetime.now(timezone.utc)

                    social_record.updated_at = datetime.now(timezone.utc)
                    self.db.add(social_record)
                    synced_friends.append({
                        "username": friend_username,
                        "is_watching": social_record.is_watching,
                        "current_item": social_record.current_item_title,
                        "friend_rating": social_record.friend_rating,
                        "in_library": social_record.in_library,
                        "influence_score": social_record.influence_score
                    })

                except Exception as e:
                    logger.error(f"Error syncing friend {friend_username}: {e}")
                    continue

            await self.db.commit()

            return {
                "friends_synced": len(synced_friends),
                "now_watching": now_watching,
                "newly_discovered": newly_discovered,
                "friends": synced_friends
            }

        except Exception as e:
            logger.error(f"Error in sync_friend_activity: {e}")
            await self.db.rollback()
            return {"error": str(e), "friends_synced": 0}

    async def calculate_influence_score(self, user_id: int, friend_username: str) -> float:
        """
        Calculate influence score: how often you watch what this friend watches.

        Formula:
            influence_score = (items_you_both_watched / friend_total_items_watched) * 100

        Range: 0-100 (100 = you watch everything they watch)

        Returns:
            float: Influence score 0-100
        """
        try:
            # Get friend's rating history from Trakt
            friend_ratings = await self.trakt.get_user_ratings(friend_username)
            if not friend_ratings:
                return 0.0

            friend_items = {r.get('item', {}).get('ids', {}).get('trakt') for r in friend_ratings}
            friend_count = len(friend_items)

            if friend_count == 0:
                return 0.0

            # Get user's rating history
            user = (await self.db.execute(select(User).filter(User.id == user_id))).scalars().first()
            user_ratings = (await self.db.execute(select(UserRating).filter(UserRating.user_id == user_id))).scalars().all()
            user_items = {r.trakt_id for r in user_ratings}

            # Calculate overlap
            overlap = len(user_items.intersection(friend_items))
            influence = (overlap / friend_count) * 100.0

            return min(influence, 100.0)

        except Exception as e:
            logger.error(f"Error calculating influence score: {e}")
            return 0.0

    async def get_friends_watching_now(self, user_id: int, limit: int = 20) -> List[Dict]:
        """
        Get list of friends currently watching content.

        Returns:
            [{
                'friend_username': str,
                'current_item': str,
                'item_type': str,
                'friend_rating': float,
                'in_library': bool,
                'influence_score': float,
                'started_at': datetime
            }, ...]
        """
        try:
            watching = (await self.db.execute(select(SocialWatching).filter(
                and_(
                    SocialWatching.user_id == user_id,
                    SocialWatching.is_watching == True
                )
            ).order_by(desc(SocialWatching.influence_score)).limit(limit))).scalars().all()

            return [
                {
                    "friend_username": w.friend_trakt_username,
                    "current_item": w.current_item_title,
                    "item_type": w.item_type,
                    "friend_rating": w.friend_rating,
                    "in_library": w.in_library,
                    "influence_score": w.influence_score,
                    "started_at": w.started_at.isoformat() if w.started_at else None,
                    "friend_profile_url": w.friend_profile_url
                }
                for w in watching
            ]

        except Exception as e:
            logger.error(f"Error getting friends watching now: {e}")
            return []

    async def create_social_leaderboard(self, user_id: int, limit: int = 20) -> List[Dict]:
        """
        Create leaderboard of friends ranked by influence score.

        Returns:
            [{
                'rank': int,
                'friend_username': str,
                'influence_score': float,
                'shared_items': int,
                'is_watching_now': bool,
                'current_item': str
            }, ...]
        """
        try:
            friends = (await self.db.execute(select(SocialWatching).filter(
                SocialWatching.user_id == user_id
            ).order_by(desc(SocialWatching.influence_score)).limit(limit))).scalars().all()

            leaderboard = []
            for rank, friend in enumerate(friends, 1):
                # Count shared items
                user_ratings = (await self.db.execute(select(UserRating).filter(UserRating.user_id == user_id))).scalars().all()
                user_items = {r.trakt_id for r in user_ratings}
                # Approximate shared count (actual would require friend's full history)
                shared_count = int((friend.influence_score / 100.0) * len(user_items)) if user_items else 0

                leaderboard.append({
                    "rank": rank,
                    "friend_username": friend.friend_trakt_username,
                    "influence_score": round(friend.influence_score, 1),
                    "shared_items": shared_count,
                    "is_watching_now": friend.is_watching,
                    "current_item": friend.current_item_title,
                    "friend_profile_url": friend.friend_profile_url
                })

            return leaderboard

        except Exception as e:
            logger.error(f"Error creating leaderboard: {e}")
            return []

    async def get_library_overlap(self, user_id: int, friend_username: str) -> Dict:
        """
        Analyze shared content between user and friend.

        Returns:
            {
                'overlap_pct': float,
                'shared_items': int,
                'user_only_items': int,
                'friend_only_items': int,
                'shared_items_list': [
                    {'title': str, 'both_rated': bool, 'rating_diff': float}
                ]
            }
        """
        try:
            # Get friend's ratings
            friend_ratings = await self.trakt.get_user_ratings(friend_username)
            friend_items = {r.get('item', {}).get('ids', {}).get('trakt'): r for r in friend_ratings}

            # Get user's ratings
            user = (await self.db.execute(select(User).filter(User.id == user_id))).scalars().first()
            user_ratings = (await self.db.execute(select(UserRating).filter(UserRating.user_id == user_id))).scalars().all()
            user_items = {r.trakt_id: r for r in user_ratings}

            # Calculate overlap
            shared_ids = set(user_items.keys()).intersection(set(friend_items.keys()))
            shared_items_list = []

            for trakt_id in shared_ids:
                user_rating = user_items[trakt_id].rating
                friend_rating = friend_items[trakt_id].get('rating', 0)
                rating_diff = abs(user_rating - (friend_rating or 0))

                shared_items_list.append({
                    "title": user_items[trakt_id].title,
                    "user_rating": user_rating,
                    "friend_rating": friend_rating,
                    "rating_diff": rating_diff
                })

            total_shared = len(user_items) + len(friend_items) - len(shared_ids)
            overlap_pct = (len(shared_ids) / total_shared * 100) if total_shared > 0 else 0

            return {
                "overlap_pct": round(overlap_pct, 1),
                "shared_items": len(shared_ids),
                "user_only_items": len(user_items) - len(shared_ids),
                "friend_only_items": len(friend_items) - len(shared_ids),
                "shared_items_list": sorted(shared_items_list, key=lambda x: x['rating_diff'])[:10]
            }

        except Exception as e:
            logger.error(f"Error calculating library overlap: {e}")
            return {
                "overlap_pct": 0,
                "shared_items": 0,
                "user_only_items": 0,
                "friend_only_items": 0,
                "shared_items_list": []
            }

    async def _check_item_in_library(self, title: str, item_type: str) -> bool:
        """Check if item exists in user's Emby library."""
        try:
            if item_type == "movie":
                items = await self.emby.search_library(title, search_type="Movie")
            else:
                items = await self.emby.search_library(title, search_type="Series")
            return len(items) > 0
        except Exception as e:
            logger.error(f"Error checking library: {e}")
            return False

    async def update_influence_scores_for_user(self, user_id: int) -> Dict:
        """
        Batch update all influence scores for a user's friends.
        Should be run as a scheduled task.

        Returns:
            {'updated': int, 'errors': int}
        """
        try:
            friends = (await self.db.execute(select(SocialWatching).filter(
                SocialWatching.user_id == user_id
            ))).scalars().all()

            updated = 0
            errors = 0

            for friend in friends:
                try:
                    # Calculate influence for this friend
                    influence = await self.calculate_influence_score(user_id, friend.friend_trakt_username)
                    friend.influence_score = influence
                    updated += 1
                except Exception as e:
                    logger.error(f"Error updating influence for {friend.friend_trakt_username}: {e}")
                    errors += 1

            await self.db.commit()
            return {"updated": updated, "errors": errors}

        except Exception as e:
            logger.error(f"Error in update_influence_scores_for_user: {e}")
            await self.db.rollback()
            return {"error": str(e), "updated": 0, "errors": 1}

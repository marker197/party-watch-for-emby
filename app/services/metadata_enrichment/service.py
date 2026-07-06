"""
Metadata Enrichment Service
Enriches Emby library items with Trakt metadata (taglines, themes, quotes, social scores).

Feature #12: Metadata Enrichment
- Pull Trakt ratings and taglines
- Genre/theme tagging from Trakt
- Social score calculation (trending)
- Quote integration
- Batch enrichment processing
- 30-day auto-refresh
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func

from app.models.schema import EnrichedMetadata, User
from app.utils.trakt_client import TraktClient
from app.utils.library_cache import LibraryCache

logger = logging.getLogger(__name__)


class MetadataEnrichmentService:
    """Enrich Emby metadata with Trakt data."""

    def __init__(self, db: Session, trakt_client: TraktClient, cache: LibraryCache):
        self.db = db
        self.trakt = trakt_client
        self.cache = cache

    async def enrich_item(self, emby_item_id: str, title: str, item_type: str = "movie", force: bool = False) -> Dict:
        """
        Enrich a single library item with Trakt metadata.

        Args:
            emby_item_id: Emby item ID
            title: Item title
            item_type: 'movie' or 'episode'
            force: Force refresh even if cached

        Returns:
            {
                'emby_item_id': str,
                'title': str,
                'tagline': str,
                'themes': [str],
                'quotes': [str],
                'social_score': float,  # 0-1 trending
                'trakt_rating': float,
                'trakt_votes': int,
                'enriched': bool,
                'from_cache': bool,
                'expires_at': datetime
            }
        """
        try:
            # Check if already enriched and fresh
            if not force:
                existing = (await self.db.execute(select(EnrichedMetadata).filter(
                    EnrichedMetadata.emby_item_id == emby_item_id
                ))).scalars().first()

                if existing and (not existing.expires_at or existing.expires_at > datetime.utcnow()):
                    # Return cached metadata
                    return self._format_enriched_response(existing, from_cache=True)

            # Fetch from Trakt
            if item_type == "movie":
                trakt_item = await self.trakt.get_movie(title)
            else:
                trakt_item = await self.trakt.get_show(title)

            if not trakt_item:
                return {"error": f"Could not find {title} on Trakt", "enriched": False}

            # Extract metadata
            enriched_data = await self._extract_trakt_metadata(trakt_item)

            # Calculate social score (trending)
            social_score = await self._calculate_social_score(trakt_item)

            # Store in database
            metadata_record = (await self.db.execute(select(EnrichedMetadata).filter(
                EnrichedMetadata.emby_item_id == emby_item_id
            ))).scalars().first()

            if not metadata_record:
                metadata_record = EnrichedMetadata(emby_item_id=emby_item_id)

            metadata_record.trakt_id = trakt_item.get('ids', {}).get('trakt')
            metadata_record.trakt_slug = trakt_item.get('ids', {}).get('slug')
            metadata_record.title = trakt_item.get('title')
            metadata_record.tagline = enriched_data.get('tagline')
            metadata_record.themes = enriched_data.get('themes')
            metadata_record.quotes = enriched_data.get('quotes')
            metadata_record.social_score = social_score
            metadata_record.trakt_rating = trakt_item.get('rating')
            metadata_record.trakt_votes = trakt_item.get('votes', 0)
            metadata_record.themes_from_trakt = True
            metadata_record.enriched_at = datetime.utcnow()
            metadata_record.expires_at = datetime.utcnow() + timedelta(days=30)
            metadata_record.metadata_json = {
                'tagline': enriched_data.get('tagline'),
                'themes': enriched_data.get('themes'),
                'quotes': enriched_data.get('quotes'),
                'social_score': social_score,
                'trakt_rating': trakt_item.get('rating'),
                'trakt_votes': trakt_item.get('votes'),
                'overview': trakt_item.get('overview')
            }

            self.db.add(metadata_record)
            await self.db.commit()

            return self._format_enriched_response(metadata_record, from_cache=False)

        except Exception as e:
            logger.error(f"Error enriching item {emby_item_id}: {e}")
            await self.db.rollback()
            return {"error": str(e), "enriched": False}

    async def batch_enrich_library(self, user_id: int, item_ids: Optional[List[str]] = None) -> Dict:
        """
        Enrich multiple library items at once.

        Args:
            user_id: User ID
            item_ids: List of Emby item IDs to enrich (if None, all in library)

        Returns:
            {
                'total': int,
                'enriched': int,
                'errors': int,
                'skipped': int,  # already enriched and fresh
                'results': [...]
            }
        """
        try:
            total = 0
            enriched = 0
            errors = 0
            skipped = 0
            results = []

            # Get library items from cache
            cache_key = f"library:{user_id}"
            library_data = self.cache.get(cache_key)

            if not library_data:
                return {"error": "Library cache empty", "total": 0, "enriched": 0}

            items_to_process = library_data.get('items', [])[:100]  # Limit to 100 to avoid rate limits

            for item in items_to_process:
                total += 1
                emby_id = item.get('id')
                title = item.get('title')
                item_type = item.get('type')

                # Check if already fresh
                existing = (await self.db.execute(select(EnrichedMetadata).filter(
                    EnrichedMetadata.emby_item_id == emby_id
                ))).scalars().first()

                if existing and existing.expires_at and existing.expires_at > datetime.utcnow():
                    skipped += 1
                    continue

                try:
                    result = await self.enrich_item(emby_id, title, item_type)
                    if result.get('enriched'):
                        enriched += 1
                    else:
                        errors += 1
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error enriching {title}: {e}")
                    errors += 1

            return {
                "total": total,
                "enriched": enriched,
                "errors": errors,
                "skipped": skipped,
                "results": results
            }

        except Exception as e:
            logger.error(f"Error in batch_enrich_library: {e}")
            return {"error": str(e), "total": 0}

    async def get_enriched_metadata(self, emby_item_id: str) -> Optional[Dict]:
        """
        Retrieve enriched metadata for an item.

        Returns:
            {
                'emby_item_id': str,
                'title': str,
                'tagline': str,
                'themes': [str],
                'quotes': [str],
                'social_score': float,
                'trakt_rating': float,
                'trakt_votes': int
            } or None if not enriched
        """
        try:
            metadata = (await self.db.execute(select(EnrichedMetadata).filter(
                EnrichedMetadata.emby_item_id == emby_item_id
            ))).scalars().first()

            if not metadata:
                return None

            return self._format_enriched_response(metadata, from_cache=True)

        except Exception as e:
            logger.error(f"Error getting enriched metadata: {e}")
            return None

    async def get_social_scores(self, limit: int = 20) -> List[Dict]:
        """
        Get items ranked by social score (trending).

        Returns:
            [{
                'title': str,
                'social_score': float,
                'trakt_rating': float,
                'themes': [str]
            }, ...]
        """
        try:
            trending = (await self.db.execute(select(EnrichedMetadata).order_by(
                EnrichedMetadata.social_score.desc()
            ).limit(limit))).scalars().all()

            return [
                {
                    "title": m.title,
                    "emby_item_id": m.emby_item_id,
                    "social_score": round(m.social_score, 3) if m.social_score else 0,
                    "trakt_rating": m.trakt_rating,
                    "themes": m.themes or [],
                    "trakt_votes": m.trakt_votes
                }
                for m in trending
            ]

        except Exception as e:
            logger.error(f"Error getting social scores: {e}")
            return []

    async def refresh_expired_metadata(self, days_old: int = 30) -> Dict:
        """
        Refresh metadata that has expired (older than days_old).

        Returns:
            {'refreshed': int, 'errors': int}
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)

            expired = (await self.db.execute(select(EnrichedMetadata).filter(
                and_(
                    EnrichedMetadata.expires_at < cutoff_date,
                    EnrichedMetadata.metadata_json.is_not(None)
                )
            ).limit(50))).scalars().all()  # Limit to 50 to avoid rate limits

            refreshed = 0
            errors = 0

            for metadata in expired:
                try:
                    # Re-enrich the item
                    await self.enrich_item(
                        metadata.emby_item_id,
                        metadata.title,
                        force=True
                    )
                    refreshed += 1
                except Exception as e:
                    logger.error(f"Error refreshing {metadata.title}: {e}")
                    errors += 1

            return {"refreshed": refreshed, "errors": errors}

        except Exception as e:
            logger.error(f"Error in refresh_expired_metadata: {e}")
            return {"error": str(e), "refreshed": 0}

    async def _extract_trakt_metadata(self, trakt_item: Dict) -> Dict:
        """
        Extract rich metadata from Trakt API response.

        Returns:
            {
                'tagline': str,
                'themes': [str],
                'quotes': [str],
                'genres': [str]
            }
        """
        return {
            "tagline": self._extract_tagline(trakt_item),
            "themes": self._extract_themes(trakt_item),
            "quotes": self._extract_quotes(trakt_item),
            "genres": trakt_item.get('genres', [])
        }

    def _extract_tagline(self, item: Dict) -> str:
        """Extract or generate tagline from Trakt data."""
        # Trakt doesn't provide taglines directly, create from overview
        overview = item.get('overview', '')
        if overview:
            # Take first sentence
            sentences = overview.split('.')
            return sentences[0][:100] if sentences else ''
        return ''

    def _extract_themes(self, item: Dict) -> List[str]:
        """Extract themes/genres from Trakt data."""
        genres = item.get('genres', [])
        # Add computed themes based on genres and rating
        themes = genres.copy()

        rating = item.get('rating', 0)
        if rating >= 8.0:
            themes.append('acclaimed')
        if rating <= 5.0:
            themes.append('underrated')

        return themes[:5]  # Top 5 themes

    def _extract_quotes(self, item: Dict) -> List[str]:
        """Extract notable quotes about the item."""
        # Trakt API doesn't provide quotes directly
        # In production, would call IMDb API or third-party quotes service
        quotes = []

        # Placeholder - these would come from external source
        if item.get('title'):
            quotes.append(f"A notable entry in the {', '.join(item.get('genres', [])[:2])} genre")

        return quotes

    async def _calculate_social_score(self, trakt_item: Dict) -> float:
        """
        Calculate social score (0-1) based on trending/popularity.

        Formula:
            - Base: votes / 10000
            - Rating boost: rating / 10
            - Normalization: clamp to 0-1
        """
        votes = trakt_item.get('votes', 0)
        rating = trakt_item.get('rating', 0)

        # Simple scoring: higher votes and ratings = higher social score
        vote_component = min(votes / 10000, 0.5)  # Max 50%
        rating_component = (rating / 10.0) * 0.5  # Max 50%

        social_score = vote_component + rating_component
        return min(social_score, 1.0)

    def _format_enriched_response(self, metadata: EnrichedMetadata, from_cache: bool = False) -> Dict:
        """Format enriched metadata for API response."""
        return {
            "emby_item_id": metadata.emby_item_id,
            "title": metadata.title,
            "tagline": metadata.tagline,
            "themes": metadata.themes or [],
            "quotes": metadata.quotes or [],
            "social_score": round(metadata.social_score, 3) if metadata.social_score else 0.0,
            "trakt_rating": metadata.trakt_rating,
            "trakt_votes": metadata.trakt_votes,
            "enriched": True,
            "from_cache": from_cache,
            "enriched_at": metadata.enriched_at.isoformat() if metadata.enriched_at else None,
            "expires_at": metadata.expires_at.isoformat() if metadata.expires_at else None
        }

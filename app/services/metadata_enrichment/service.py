"""
Metadata Enrichment Service — Feature #12

Enriches Emby library items with richer Trakt metadata:
- Community ratings (Trakt vs Emby comparison)
- Genres / themes / tone tags from Trakt
- Taglines (first sentence of Trakt overview when Emby lacks one)
- Social/trending scores
- Selective push to Emby: Tags and Taglines (safe, non-destructive)

Enrichment runs per-item or as a batch scan.  Data is cached in the
enriched_metadata table with a 30-day TTL.  Push to Emby is optional
and only modifies Tags and Taglines — never overwrites core fields.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import EnrichedMetadata, User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.database import async_session

log = structlog.get_logger()


class MetadataEnrichmentService:
    """Enrich Emby metadata with Trakt data and optionally push back."""

    def __init__(self):
        self.emby = EmbyClient()

    # ===================================================================
    # Single-item enrichment
    # ===================================================================

    async def enrich_item(
        self,
        trakt: TraktClient,
        emby_item_id: str,
        provider_ids: dict,
        title: str,
        item_type: str = "movie",
        force: bool = False,
    ) -> dict | None:
        """Enrich one item.  Tries Trakt lookup by IMDB → TMDB → slug → title search.

        Returns enriched dict or None on failure.
        """
        async with async_session() as db:
            # Check cache
            if not force:
                existing = (await db.execute(
                    select(EnrichedMetadata).where(
                        EnrichedMetadata.emby_item_id == emby_item_id
                    )
                )).scalar_one_or_none()
                if existing and existing.expires_at and existing.expires_at > datetime.utcnow():
                    return self._format(existing, from_cache=True)

            # Fetch from Trakt
            trakt_data = await self._lookup_trakt(trakt, provider_ids, title, item_type)
            if not trakt_data:
                log.warning("enrichment.trakt_lookup_miss", title=title, emby_id=emby_item_id)
                return None

            # Extract enriched fields
            trakt_rating = trakt_data.get("rating")
            trakt_votes = trakt_data.get("votes", 0)
            genres = trakt_data.get("genres", [])
            overview = trakt_data.get("overview", "")
            tagline = trakt_data.get("tagline", "")
            if not tagline and overview:
                tagline = overview.split(".")[0][:120]

            themes = list(genres)
            if trakt_rating and trakt_rating >= 8.0:
                themes.append("acclaimed")

            social_score = self._calc_social_score(trakt_votes, trakt_rating or 0)

            trakt_ids = trakt_data.get("ids", {})

            # Upsert
            record = (await db.execute(
                select(EnrichedMetadata).where(
                    EnrichedMetadata.emby_item_id == emby_item_id
                )
            )).scalar_one_or_none()

            if not record:
                record = EnrichedMetadata(emby_item_id=emby_item_id)
                db.add(record)

            record.trakt_id = str(trakt_ids.get("trakt", ""))
            record.trakt_slug = trakt_ids.get("slug", "")
            record.title = trakt_data.get("title", title)
            record.tagline = tagline
            record.themes = themes[:8]
            record.quotes = []  # Trakt doesn't expose quotes
            record.social_score = social_score
            record.trakt_rating = trakt_rating
            record.trakt_votes = trakt_votes
            record.themes_from_trakt = True
            record.enriched_at = datetime.utcnow()
            record.expires_at = datetime.utcnow() + timedelta(days=30)
            record.metadata_json = {
                "tagline": tagline,
                "themes": themes[:8],
                "genres": genres,
                "overview": overview[:500],
                "trakt_rating": trakt_rating,
                "trakt_votes": trakt_votes,
                "social_score": social_score,
                "certification": trakt_data.get("certification", ""),
                "runtime": trakt_data.get("runtime"),
                "network": trakt_data.get("network", ""),
                "status": trakt_data.get("status", ""),
                "country": trakt_data.get("country", ""),
                "language": trakt_data.get("language", ""),
                "trakt_ids": trakt_ids,
            }

            await db.commit()
            await db.refresh(record)
            return self._format(record, from_cache=False)

    # ===================================================================
    # Batch enrichment
    # ===================================================================

    async def batch_enrich(
        self, trakt: TraktClient, user_id: str, limit: int = 100,
    ) -> dict:
        """Enrich library items that haven't been enriched or are expired.

        Scans Emby library via LibraryCache, skips already-fresh items.
        Returns summary stats.
        """
        enriched = 0
        skipped = 0
        errors = 0
        items_processed = []

        # Get library items from Emby (movies + series)
        all_items = []
        for item_type in ("Movie", "Series"):
            try:
                resp = await self.emby.get_items(
                    user_id=user_id, item_type=item_type,
                    fields="ProviderIds,ProductionYear,Genres,CommunityRating,Taglines,Tags",
                    limit=limit,
                )
                all_items.extend(resp.get("Items", []))
            except Exception as e:
                log.warning("enrichment.library_fetch_failed", item_type=item_type,
                            error=str(e)[:120])

        log.info("enrichment.batch_start", total_items=len(all_items))

        async with async_session() as db:
            # Get set of already-enriched (fresh AND actually has data) emby IDs
            fresh_ids = set()
            rows = (await db.execute(
                select(EnrichedMetadata.emby_item_id).where(
                    EnrichedMetadata.expires_at > datetime.utcnow(),
                    EnrichedMetadata.trakt_rating.isnot(None),
                )
            )).scalars().all()
            fresh_ids = set(rows)

        for item in all_items:
            emby_id = str(item.get("Id", ""))
            if emby_id in fresh_ids:
                skipped += 1
                continue

            provider_ids = item.get("ProviderIds", {})
            title = item.get("Name", "")
            itype = "show" if item.get("Type") == "Series" else "movie"

            try:
                result = await self.enrich_item(
                    trakt, emby_id, provider_ids, title, itype,
                    force=True,  # always force — skip check is done above
                )
                if result:
                    enriched += 1
                    items_processed.append({
                        "title": title,
                        "emby_id": emby_id,
                        "trakt_rating": result.get("trakt_rating"),
                        "social_score": result.get("social_score"),
                    })
                else:
                    errors += 1
            except Exception:
                errors += 1
                log.warning("enrichment.item_failed", title=title)

        log.info("enrichment.batch_done",
                 enriched=enriched, skipped=skipped, errors=errors)

        return {
            "total": len(all_items),
            "enriched": enriched,
            "skipped": skipped,
            "errors": errors,
            "items": items_processed[:50],  # cap response size
        }

    # ===================================================================
    # Push to Emby — selective, non-destructive
    # ===================================================================

    async def push_to_emby(self, emby_item_id: str, fields: list[str]) -> dict:
        """Push enriched metadata fields to Emby for a single item.

        Supported fields:
          - tags: Adds Trakt genre/theme tags to Emby's Tags (additive)
          - tagline: Sets Emby Taglines if empty or force
          - community_rating: Updates CommunityRating with Trakt's rating

        Returns {pushed: [fields], skipped: [fields], error: str|None}
        """
        async with async_session() as db:
            record = (await db.execute(
                select(EnrichedMetadata).where(
                    EnrichedMetadata.emby_item_id == emby_item_id
                )
            )).scalar_one_or_none()

        if not record or not record.metadata_json:
            return {"pushed": [], "skipped": fields, "error": "Not enriched yet"}

        meta = record.metadata_json
        updates: dict[str, Any] = {}
        pushed = []
        skipped_fields = []

        # Get current Emby item to compare
        try:
            current = await self.emby.get_item_safe(emby_item_id)
        except Exception:
            current = None
        if not current:
            return {"pushed": [], "skipped": fields, "error": "Could not fetch item from Emby"}

        if "tags" in fields:
            trakt_tags = meta.get("themes", []) + meta.get("genres", [])
            trakt_tags = list(dict.fromkeys(trakt_tags))  # dedupe, preserve order
            existing_tags = current.get("Tags", []) or []
            # Additive — merge without duplicates
            merged = list(dict.fromkeys(existing_tags + trakt_tags))
            if merged != existing_tags:
                updates["Tags"] = merged
                pushed.append("tags")
            else:
                skipped_fields.append("tags")

        if "tagline" in fields:
            tagline = meta.get("tagline", "")
            existing_taglines = current.get("Taglines", []) or []
            if tagline and not existing_taglines:
                updates["Taglines"] = [tagline]
                pushed.append("tagline")
            else:
                skipped_fields.append("tagline")

        if "community_rating" in fields:
            trakt_rating = meta.get("trakt_rating")
            if trakt_rating:
                # Trakt is 0-10, Emby CommunityRating is also 0-10
                updates["CommunityRating"] = round(trakt_rating, 1)
                pushed.append("community_rating")
            else:
                skipped_fields.append("community_rating")

        if updates:
            ok = await self.emby.update_item(emby_item_id, updates)
            if not ok:
                return {"pushed": [], "skipped": fields, "error": "Emby update failed"}

        return {"pushed": pushed, "skipped": skipped_fields, "error": None}

    async def batch_push_to_emby(self, fields: list[str], limit: int = 200) -> dict:
        """Push enriched metadata to Emby for all enriched items."""
        async with async_session() as db:
            records = (await db.execute(
                select(EnrichedMetadata).where(
                    EnrichedMetadata.metadata_json.isnot(None)
                ).limit(limit)
            )).scalars().all()

        total = len(records)
        pushed_count = 0
        skipped_count = 0
        errors_count = 0

        for record in records:
            try:
                result = await self.push_to_emby(record.emby_item_id, fields)
                if result.get("pushed"):
                    pushed_count += 1
                elif result.get("error"):
                    errors_count += 1
                else:
                    skipped_count += 1
            except Exception:
                errors_count += 1

        return {
            "total": total,
            "pushed": pushed_count,
            "skipped": skipped_count,
            "errors": errors_count,
            "fields": fields,
        }

    # ===================================================================
    # Comparison report
    # ===================================================================

    async def get_comparison(self, user_id: str, limit: int = 50) -> list[dict]:
        """Compare Trakt vs Emby metadata for enriched items.

        Returns items where Trakt and Emby data differ — useful for
        deciding what to push.
        """
        async with async_session() as db:
            records = (await db.execute(
                select(EnrichedMetadata).where(
                    EnrichedMetadata.metadata_json.isnot(None)
                ).order_by(EnrichedMetadata.trakt_votes.desc())
                .limit(limit)
            )).scalars().all()

        log.info("enrichment.comparison_start",
                 records=len(records),
                 sample_rating=records[0].trakt_rating if records else None,
                 sample_votes=records[0].trakt_votes if records else None,
                 sample_title=records[0].title if records else None)

        comparisons = []
        for record in records:
            try:
                emby_item = await self.emby.get_item_safe(record.emby_item_id)
            except Exception:
                continue
            if not emby_item:
                continue

            meta = record.metadata_json or {}
            emby_rating = emby_item.get("CommunityRating")
            trakt_rating = meta.get("trakt_rating")
            emby_genres = emby_item.get("Genres", [])
            trakt_genres = meta.get("genres", [])
            emby_tags = emby_item.get("Tags", []) or []
            emby_taglines = emby_item.get("Taglines", []) or []
            trakt_tagline = meta.get("tagline", "")

            # Calculate differences
            diffs = []
            if trakt_rating and emby_rating:
                diff = abs(trakt_rating - emby_rating)
                if diff >= 0.5:
                    diffs.append(f"rating: Emby {emby_rating:.1f} vs Trakt {trakt_rating:.1f}")
            elif trakt_rating and not emby_rating:
                diffs.append(f"rating: Emby missing, Trakt {trakt_rating:.1f}")

            new_genres = [g for g in trakt_genres if g not in emby_genres]
            if new_genres:
                diffs.append(f"genres: +{', '.join(new_genres[:3])}")

            if trakt_tagline and not emby_taglines:
                diffs.append("tagline: Emby missing")

            comparisons.append({
                "emby_item_id": record.emby_item_id,
                "title": record.title,
                "emby_rating": emby_rating,
                "trakt_rating": trakt_rating,
                "trakt_votes": meta.get("trakt_votes", 0),
                "social_score": round(record.social_score or 0, 3),
                "emby_genres": emby_genres,
                "trakt_genres": trakt_genres,
                "emby_tags": emby_tags,
                "trakt_themes": meta.get("themes", []),
                "emby_taglines": emby_taglines,
                "trakt_tagline": trakt_tagline,
                "diffs": diffs,
                "has_diffs": len(diffs) > 0,
                "enriched_at": record.enriched_at.isoformat() if record.enriched_at else None,
            })

        # Sort: items with differences first
        comparisons.sort(key=lambda c: (not c["has_diffs"], -(c.get("trakt_votes") or 0)))
        return comparisons

    # ===================================================================
    # Helpers
    # ===================================================================

    async def _lookup_trakt(
        self, trakt: TraktClient, provider_ids: dict,
        title: str, item_type: str,
    ) -> dict | None:
        """Try to find the item on Trakt via provider IDs, then title search."""
        imdb = provider_ids.get("Imdb") or provider_ids.get("imdb")
        tmdb = provider_ids.get("Tmdb") or provider_ids.get("tmdb")

        lookup_fn = trakt.get_movie_by_id if item_type == "movie" else trakt.get_show_by_id

        # Try IMDB first (direct path lookup — most reliable)
        if imdb:
            result = await lookup_fn(imdb, id_type="imdb")
            if result:
                log.info("enrichment.trakt_found", title=title, via="imdb",
                         has_rating=result.get("rating") is not None,
                         rating=result.get("rating"),
                         votes=result.get("votes", 0),
                         keys=list(result.keys())[:10])
                return result

        # Try TMDB (requires search endpoint)
        if tmdb:
            result = await lookup_fn(str(tmdb), id_type="tmdb")
            if result:
                log.info("enrichment.trakt_found", title=title, via="tmdb",
                         has_rating=result.get("rating") is not None,
                         rating=result.get("rating"),
                         votes=result.get("votes", 0),
                         keys=list(result.keys())[:10])
                return result

        # Fallback: title search
        try:
            kind = "movie" if item_type == "movie" else "show"
            results = await trakt.search(title, kind=kind)
            if results:
                item = results[0].get(kind, results[0])
                slug = item.get("ids", {}).get("slug")
                if slug:
                    result = await lookup_fn(slug, id_type="slug")
                    if result:
                        log.info("enrichment.trakt_found", title=title, via="search",
                                 has_rating=result.get("rating") is not None)
                        return result
        except Exception:
            pass

        log.warning("enrichment.trakt_lookup_miss", title=title,
                    has_imdb=bool(imdb), has_tmdb=bool(tmdb))
        return None

    @staticmethod
    def _calc_social_score(votes: int, rating: float) -> float:
        vote_part = min(votes / 10000, 0.5)
        rating_part = (rating / 10.0) * 0.5
        return min(round(vote_part + rating_part, 4), 1.0)

    @staticmethod
    def _format(record: EnrichedMetadata, from_cache: bool = False) -> dict:
        meta = record.metadata_json or {}
        return {
            "emby_item_id": record.emby_item_id,
            "title": record.title,
            "tagline": record.tagline,
            "themes": record.themes or [],
            "social_score": round(record.social_score or 0, 3),
            "trakt_rating": record.trakt_rating,
            "trakt_votes": record.trakt_votes,
            "certification": meta.get("certification", ""),
            "runtime": meta.get("runtime"),
            "network": meta.get("network", ""),
            "status": meta.get("status", ""),
            "genres": meta.get("genres", []),
            "enriched": True,
            "from_cache": from_cache,
            "enriched_at": record.enriched_at.isoformat() if record.enriched_at else None,
            "expires_at": record.expires_at.isoformat() if record.expires_at else None,
        }

"""Routes extracted from routes.py — ratings_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.secure_redis import secure_get
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user, require_user_ownership
from app.middleware.rate_limit import LIMITS, limiter
from app.api.route_helpers import _get_active_providers, _get_mdblist_key

log = structlog.get_logger()

router = APIRouter()



@router.post("/api/rating-sync/{user_id}")
@limiter.limit(LIMITS["heavy"])
async def sync_ratings_between_providers(
    request: Request,
    user_id: int,
    direction: str = Query("mdblist_to_simkl",
                           regex="^(mdblist_to_simkl|simkl_to_mdblist|bidirectional)$"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Sync ratings between MDBList and Simkl.

    Directions:
      - mdblist_to_simkl: push MDBList ratings → Simkl
      - simkl_to_mdblist: push Simkl ratings → MDBList
      - bidirectional: merge both (MDBList wins on conflicts)
    """
    require_user_ownership(current_user.id, user_id, "rating_sync")
    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    # Build clients
    from app.utils.mdblist_client import MDBListClient
    from app.utils.secure_redis import secure_get
    mdb_key = await secure_get("mdblist_api_key")
    mdb = MDBListClient(api_key=mdb_key) if mdb_key else None

    simkl = None
    if user.simkl_access_token:
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    if not mdb and not simkl:
        raise HTTPException(400, "Neither MDBList nor Simkl is configured")

    try:
        result = {"synced_to_simkl": 0, "synced_to_mdblist": 0,
                  "skipped_existing": 0, "errors": 0}

        # ── Fetch ratings from both ──
        mdb_ratings: dict = {}
        simkl_ratings: list[dict] = []

        if mdb:
            try:
                mdb_ratings = await mdb.get_ratings()
            except Exception as e:
                log.warning("rating_sync.mdblist_fetch_failed", **{"error": str(e)[:120]})

        if simkl:
            try:
                simkl_ratings = await simkl.get_user_ratings("all")
            except Exception as e:
                log.warning("rating_sync.simkl_fetch_failed", **{"error": str(e)[:120]})

        # ── Build lookup maps: imdb_id → {rating, item_data} ──
        mdb_by_imdb: dict[str, dict] = {}
        for kind in ("movies", "shows"):
            for item in (mdb_ratings.get(kind, []) if isinstance(mdb_ratings, dict) else []):
                r_val = item.get("rating")
                iid = (item.get("ids") or {}).get("imdb", "")
                if r_val is not None and iid:
                    mdb_by_imdb[iid] = {
                        "rating": int(round(float(r_val))),
                        "item_type": "movie" if kind == "movies" else "show",
                        "ids": item.get("ids", {}),
                        "title": item.get("title", ""),
                    }

        simkl_by_imdb: dict[str, dict] = {}
        for entry in simkl_ratings:
            item_obj = entry.get("movie") or entry.get("show") or entry
            iid = (item_obj.get("ids") or {}).get("imdb", "")
            r_val = entry.get("rating")
            if r_val is not None and iid:
                simkl_by_imdb[iid] = {
                    "rating": int(r_val),
                    "item_type": "movie" if "movie" in entry else "show",
                    "ids": item_obj.get("ids", {}),
                    "title": item_obj.get("title", ""),
                }

        # ── MDBList → Simkl ──
        if direction in ("mdblist_to_simkl", "bidirectional") and simkl and mdb_by_imdb:
            to_push: list[dict] = []
            for imdb_id, mdb_item in mdb_by_imdb.items():
                existing = simkl_by_imdb.get(imdb_id)
                if existing and existing["rating"] == mdb_item["rating"]:
                    result["skipped_existing"] += 1
                    continue
                # Build Simkl-format rating payload
                ids = {"imdb": imdb_id}
                if mdb_item["ids"].get("tmdb"):
                    ids["tmdb"] = int(mdb_item["ids"]["tmdb"])
                entry_obj = {
                    "rating": mdb_item["rating"],
                    "ids": ids,
                }
                if mdb_item["item_type"] == "movie":
                    to_push.append({"movie": entry_obj})
                else:
                    to_push.append({"show": entry_obj})

            if to_push:
                # Batch in chunks of 100
                for i in range(0, len(to_push), 100):
                    chunk = to_push[i:i + 100]
                    try:
                        await simkl.add_ratings(chunk)
                        result["synced_to_simkl"] += len(chunk)
                    except Exception as e:
                        log.warning("rating_sync.simkl_push_error", **{"error": str(e)[:120],
                                              "chunk_size": len(chunk)})
                        result["errors"] += len(chunk)

        # ── Simkl → MDBList ──
        if direction in ("simkl_to_mdblist", "bidirectional") and mdb and simkl_by_imdb:
            movies_to_push: list[dict] = []
            shows_to_push: list[dict] = []
            for imdb_id, simkl_item in simkl_by_imdb.items():
                existing = mdb_by_imdb.get(imdb_id)
                if existing and existing["rating"] == simkl_item["rating"]:
                    result["skipped_existing"] += 1
                    continue
                entry_obj = {
                    "ids": {"imdb": imdb_id},
                    "rating": simkl_item["rating"],
                }
                if simkl_item["item_type"] == "movie":
                    movies_to_push.append(entry_obj)
                else:
                    shows_to_push.append(entry_obj)

            if movies_to_push or shows_to_push:
                try:
                    await mdb.add_ratings(
                        movies=movies_to_push or None,
                        shows=shows_to_push or None,
                    )
                    result["synced_to_mdblist"] += len(movies_to_push) + len(shows_to_push)
                except Exception as e:
                    log.warning("rating_sync.mdblist_push_error", **{"error": str(e)[:120]})
                    result["errors"] += len(movies_to_push) + len(shows_to_push)

        log.info("rating_sync.complete", **{"user_id": user_id, "direction": direction, **result})
        return {"success": True, **result}
    finally:
        if simkl:
            await simkl.close()
        if mdb:
            await mdb.close()


@router.get("/api/rating-sync/{user_id}/status")
async def get_rating_sync_status(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Preview what a rating sync would do without executing it."""
    require_user_ownership(current_user.id, user_id, "rating_sync")
    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    from app.utils.mdblist_client import MDBListClient
    from app.utils.secure_redis import secure_get
    mdb_key = await secure_get("mdblist_api_key")
    mdb = MDBListClient(api_key=mdb_key) if mdb_key else None
    simkl = None
    if user.simkl_access_token:
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        mdb_count = 0
        simkl_count = 0
        overlap = 0

        if mdb:
            try:
                mdb_ratings = await mdb.get_ratings()
                for kind in ("movies", "shows"):
                    mdb_count += len(mdb_ratings.get(kind, [])
                                     if isinstance(mdb_ratings, dict) else [])
            except Exception:
                pass

        if simkl:
            try:
                sr = await simkl.get_user_ratings("all")
                simkl_count = len(sr)
                # Count overlap by IMDB ID
                simkl_imdb = set()
                for entry in sr:
                    item_obj = entry.get("movie") or entry.get("show") or entry
                    iid = (item_obj.get("ids") or {}).get("imdb", "")
                    if iid:
                        simkl_imdb.add(iid)
                if isinstance(mdb_ratings, dict):
                    for kind in ("movies", "shows"):
                        for item in mdb_ratings.get(kind, []):
                            iid = (item.get("ids") or {}).get("imdb", "")
                            if iid in simkl_imdb:
                                overlap += 1
            except Exception:
                pass

        return {
            "mdblist_rated": mdb_count,
            "simkl_rated": simkl_count,
            "overlap": overlap,
            "mdblist_only": mdb_count - overlap,
            "simkl_only": simkl_count - overlap,
            "mdblist_configured": mdb is not None,
            "simkl_configured": simkl is not None,
        }
    finally:
        if simkl:
            await simkl.close()
        if mdb:
            await mdb.close()


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/ratings/unrated/{user_id}")
async def get_unrated_items(
    user_id: int,
    limit: int = Query(5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return recent watch-history items that the user hasn't rated yet.

    Cross-references WatchHistory against UserRating (by imdb_id) and
    DismissedRatingItem to find items eligible for rating prompts.
    Returns movie-level items and series-level items (collapsed from episodes).
    """
    from app.models.schema import WatchHistory, UserRating, DismissedRatingItem
    from sqlalchemy import cast, Date

    # 1) Existing rated imdb_ids for this user
    rated_q = select(UserRating.imdb_id).where(
        UserRating.user_id == user_id,
        UserRating.imdb_id.isnot(None),
    )
    rated_rows = (await db.execute(rated_q)).scalars().all()
    rated_imdb = set(r for r in rated_rows if r)

    # Also include simkl_id-based rated items (older imports without imdb_id)
    rated_simkl_q = select(UserRating.simkl_id).where(
        UserRating.user_id == user_id,
    )
    rated_simkl_rows = (await db.execute(rated_simkl_q)).scalars().all()
    rated_simkl = set(r for r in rated_simkl_rows if r)

    # 2) Dismissed item keys
    dismissed_q = select(DismissedRatingItem.item_key).where(
        DismissedRatingItem.user_id == user_id,
    )
    dismissed_rows = (await db.execute(dismissed_q)).scalars().all()
    dismissed_keys = set(dismissed_rows)

    # 3) Recent watch history (last 90 days, completed items only)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=90)
    wh_q = (
        select(WatchHistory)
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.watched_at >= cutoff,
        )
        .order_by(WatchHistory.watched_at.desc())
        .limit(500)
    )
    wh_rows = (await db.execute(wh_q)).scalars().all()

    # 4) Collapse episodes to series level, movies stay as-is
    # Key: imdb_id (preferred) or normalised title
    seen: dict[str, dict] = {}
    for r in wh_rows:
        # Skip partial watches
        if r.progress is not None and r.progress < 80:
            continue

        if r.item_type == "episode":
            # Use series-level info
            display_title = r.series_name or r.title or ""
            item_type_out = "show"
            # For series, we need the series imdb_id — but WatchHistory stores episode-level IDs
            # Use series_name as fallback key
            item_imdb = None  # episode imdb != series imdb
            item_tmdb = None
            item_simkl = r.simkl_id
            key = f"show:{(display_title).strip().lower()}"
        else:
            display_title = r.title or ""
            item_type_out = "movie"
            item_imdb = r.imdb_id
            item_tmdb = r.tmdb_id
            item_simkl = r.simkl_id
            key = f"imdb:{r.imdb_id}" if r.imdb_id else f"movie:{display_title.strip().lower()}"

        if not display_title.strip():
            continue

        # Skip if already rated
        if item_imdb and item_imdb in rated_imdb:
            continue
        if item_simkl and item_simkl in rated_simkl:
            continue

        # Skip if dismissed
        if key in dismissed_keys:
            continue

        if key in seen:
            continue

        # Resolve emby_id for poster
        emby_id = r.emby_id
        if not emby_id:
            resolved = None
            if item_imdb:
                resolved = await LibraryCache.find_by_provider_id("Imdb", item_imdb)
            if not resolved and item_tmdb:
                resolved = await LibraryCache.find_by_provider_id("Tmdb", item_tmdb)
            if not resolved and display_title:
                resolved = await LibraryCache.find_by_title(display_title)
            if resolved:
                emby_id = resolved.get("emby_id") or resolved.get("Id")

        # For shows, try to get series-level imdb from library cache
        if item_type_out == "show" and not item_imdb and display_title:
            cache_item = await LibraryCache.find_by_title(display_title)
            if cache_item:
                pids = cache_item.get("provider_ids") or cache_item.get("ProviderIds") or {}
                item_imdb = pids.get("Imdb") or pids.get("imdb")
                item_tmdb = pids.get("Tmdb") or pids.get("tmdb")
                if not emby_id:
                    emby_id = cache_item.get("emby_id") or cache_item.get("Id")

        # Re-check rated after resolving series imdb
        if item_imdb and item_imdb in rated_imdb:
            continue
        # Update key with resolved imdb
        if item_imdb and key.startswith("show:"):
            real_key = f"imdb:{item_imdb}"
            if real_key in dismissed_keys:
                continue
            key = real_key

        seen[key] = {
            "item_key": key,
            "title": display_title,
            "item_type": item_type_out,
            "imdb_id": item_imdb,
            "tmdb_id": item_tmdb,
            "simkl_id": item_simkl,
            "emby_id": emby_id,
            "watched_at": r.watched_at.isoformat() if r.watched_at else None,
            "year": None,
        }

        if len(seen) >= limit:
            break

    # Try to enrich year from library cache
    for item in seen.values():
        if item.get("year"):
            continue
        cached = None
        if item.get("imdb_id"):
            cached = await LibraryCache.find_by_provider_id("Imdb", item["imdb_id"])
        if not cached and item.get("tmdb_id"):
            cached = await LibraryCache.find_by_provider_id("Tmdb", item["tmdb_id"])
        if not cached and item.get("title"):
            cached = await LibraryCache.find_by_title(item["title"])
        if cached:
            item["year"] = cached.get("ProductionYear")
            if not item.get("emby_id"):
                item["emby_id"] = cached.get("emby_id") or cached.get("Id")

    return {"items": list(seen.values()), "total_unrated": len(seen)}


@router.post("/api/rate")
async def rate_item(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Rate an item and push to Simkl + MDBList.

    Payload: {user_id, imdb_id, tmdb_id?, rating (1-10), item_type (movie|show), title?}
    """
    user_id = payload.get("user_id")
    imdb_id = payload.get("imdb_id")
    tmdb_id = payload.get("tmdb_id")
    rating = payload.get("rating")
    item_type = payload.get("item_type", "movie")
    title = payload.get("title", "")
    season_number = payload.get("season_number")
    episode_number = payload.get("episode_number")
    series_name = payload.get("series_name", "")

    if not user_id or not rating:
        raise HTTPException(400, "user_id and rating are required")
    if not isinstance(rating, (int, float)) or rating < 1 or rating > 10:
        raise HTTPException(400, "rating must be 1-10")

    # Try to resolve missing IDs from library cache (especially for shows)
    if not imdb_id and not tmdb_id and title:
        cached = await LibraryCache.find_by_title(title)
        if cached:
            pids = cached.get("provider_ids") or cached.get("ProviderIds") or {}
            imdb_id = pids.get("Imdb") or pids.get("imdb")
            tmdb_id = pids.get("Tmdb") or pids.get("tmdb")

    # Fallback: search Emby directly by title if cache missed
    if not imdb_id and not tmdb_id and title:
        _emby_rate = None
        try:
            from app.utils.emby_client import EmbyClient
            _emby_rate = EmbyClient()
            search_type = "Series" if item_type == "show" else "Movie"
            results = await _emby_rate.search_items(title, item_type=search_type)
            if results:
                for result in (results if isinstance(results, list) else results.get("Items", results)):
                    r_pids = result.get("ProviderIds", {})
                    r_title = result.get("Name", "").strip().lower()
                    if r_title == title.strip().lower():
                        imdb_id = r_pids.get("Imdb") or r_pids.get("imdb")
                        tmdb_id = r_pids.get("Tmdb") or r_pids.get("tmdb")
                        if imdb_id or tmdb_id:
                            break
        except Exception as e:
            log_init = structlog.get_logger()
            log_init.debug("rate.emby_search_fallback_failed", error=str(e)[:120])
        finally:
            if _emby_rate:
                await _emby_rate.close()

    if not imdb_id and not tmdb_id:
        raise HTTPException(400, "imdb_id or tmdb_id required (could not resolve from title)")

    rating = int(rating)

    # Verify user exists
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    log = structlog.get_logger()
    results = {"simkl": None, "mdblist": None, "local": None}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # ── Build provider payloads ──────────────────────────────────────
    ids_obj = {}
    if imdb_id:
        ids_obj["imdb"] = imdb_id
    if tmdb_id:
        try:
            ids_obj["tmdb"] = int(tmdb_id)
        except (ValueError, TypeError):
            ids_obj["tmdb"] = tmdb_id

    # For episodes, resolve the SERIES IDs (Simkl needs show-level IDs,
    # not episode IMDB, in the top-level ids object)
    series_ids_obj = {}
    if item_type == "episode" and series_name:
        cached_series = await LibraryCache.find_by_title(series_name)
        if cached_series:
            s_pids = cached_series.get("provider_ids") or cached_series.get("ProviderIds") or {}
            s_imdb = s_pids.get("Imdb") or s_pids.get("imdb") or s_pids.get("IMDB")
            s_tmdb = s_pids.get("Tmdb") or s_pids.get("tmdb")
            if s_imdb:
                series_ids_obj["imdb"] = s_imdb
            if s_tmdb:
                try:
                    series_ids_obj["tmdb"] = int(s_tmdb)
                except (ValueError, TypeError):
                    series_ids_obj["tmdb"] = s_tmdb
        if not series_ids_obj.get("imdb"):
            # Cache may lack IMDB for series — search Emby directly
            _emby_sr = None
            try:
                from app.utils.emby_client import EmbyClient
                _emby_sr = EmbyClient()
                sr_results = await _emby_sr.search_items(series_name, item_type="Series")
                _sr_list = sr_results if isinstance(sr_results, list) else (sr_results or {}).get("Items", sr_results or [])
                for sr in _sr_list:
                    sr_pids = sr.get("ProviderIds", {})
                    sr_title = (sr.get("Name") or "").strip().lower()
                    _sn_lower = series_name.strip().lower()
                    sr_tmdb = sr_pids.get("Tmdb") or sr_pids.get("tmdb")
                    _tmdb_match = (sr_tmdb and series_ids_obj.get("tmdb")
                                   and str(sr_tmdb) == str(series_ids_obj["tmdb"]))
                    if sr_title == _sn_lower or _sn_lower in sr_title or _tmdb_match:
                        s_imdb = sr_pids.get("Imdb") or sr_pids.get("imdb") or sr_pids.get("IMDB")
                        s_tmdb = sr_pids.get("Tmdb") or sr_pids.get("tmdb")
                        if s_imdb:
                            series_ids_obj["imdb"] = s_imdb
                        if s_tmdb:
                            try:
                                series_ids_obj["tmdb"] = int(s_tmdb)
                            except (ValueError, TypeError):
                                series_ids_obj["tmdb"] = s_tmdb
                        if series_ids_obj.get("imdb"):
                            break
            except Exception:
                pass
            finally:
                if _emby_sr:
                    await _emby_sr.close()
        log.debug("rate.series_ids_resolved",
                   series_name=series_name,
                   series_ids=series_ids_obj,
                   episode_ids=ids_obj)

    # ── Check for existing local rating (detect re-rate) ────────────
    from app.models.schema import UserRating

    existing_q = select(UserRating).where(
        UserRating.user_id == user_id,
        UserRating.item_type == item_type,
    )
    if item_type == "episode" and season_number is not None and episode_number is not None:
        existing_q = existing_q.where(
            UserRating.series_name == series_name,
            UserRating.season_number == season_number,
            UserRating.episode_number == episode_number,
        )
    elif imdb_id:
        existing_q = existing_q.where(UserRating.imdb_id == imdb_id)
    elif tmdb_id:
        existing_q = existing_q.where(UserRating.tmdb_id == tmdb_id)

    existing_rating_row = (await db.execute(existing_q.limit(1))).scalar_one_or_none()
    old_rating = existing_rating_row.rating if existing_rating_row else None
    is_rerate = old_rating is not None and int(old_rating) != rating

    if old_rating is not None and int(old_rating) == rating:
        log.info("rating.unchanged_skipped", user_id=user_id, imdb=imdb_id,
                 rating=rating, item_type=item_type)
        return {"ok": True, "rating": rating, "results": {
            "simkl": {"ok": True, "skipped": "unchanged"},
            "mdblist": {"ok": True, "skipped": "unchanged"},
            "local": {"ok": True, "skipped": "unchanged"},
        }}

    if is_rerate:
        log.info("rating.rerate_detected", user_id=user_id, imdb=imdb_id,
                 old=old_rating, new=rating, item_type=item_type)

    # ── Push to providers ──────────────────────────────────────────────
    providers = await _get_active_providers(db)

    # --- Helper: build Simkl remove payload (IDs only, no rating) ---
    def _simkl_remove_payload() -> list[dict]:
        return [{"ids": ids_obj, "_type": "movies" if item_type == "movie" else "shows"}]

    if "simkl" in providers and user.simkl_access_token:
        # Simkl episode re-rating not supported — remove_ratings with a
        # show-nested payload destroys the show-level rating and history.
        # Episode re-rates are handled by MDBList only.
        if is_rerate and item_type == "episode":
            results["simkl"] = {"ok": True, "skipped": "episode_rerate_unsupported"}
        else:
            try:
                simkl = SimklClient(
                    access_token=user.simkl_access_token,
                    token_expires=user.simkl_token_expires,
                )

                # Re-rate for movies/shows: remove then re-add
                if is_rerate:
                    rm_resp = await simkl.remove_ratings(_simkl_remove_payload())
                    log.info("rating.simkl_removed_for_rerate", user_id=user_id,
                             imdb=imdb_id, old_rating=old_rating,
                             response=str(rm_resp)[:200])
                    await asyncio.sleep(1.1)

                # Build payload
                if item_type == "episode" and season_number is not None and episode_number is not None:
                    simkl_item = {
                        "ids": series_ids_obj or ids_obj,
                        "_type": "shows",
                        "seasons": [{
                            "number": season_number,
                            "episodes": [{
                                "number": episode_number,
                                "rating": rating,
                            }],
                        }],
                    }
                else:
                    simkl_item = {
                        "ids": ids_obj,
                        "rating": rating,
                        "rated_at": now_str,
                        "_type": "movies" if item_type == "movie" else "shows",
                    }
                if title:
                    simkl_item["title"] = series_name or title

                resp = await simkl.add_ratings([simkl_item])
                results["simkl"] = {"ok": True, "response": resp}
                log.info("rating.simkl_pushed", user_id=user_id, imdb=imdb_id,
                         rating=rating, rerate=is_rerate)

                await simkl.close()
            except Exception as e:
                results["simkl"] = {"ok": False, "error": str(e)[:200]}
                log.warning("rating.simkl_failed", error=str(e)[:200])

    # ── Push to MDBList ──────────────────────────────────────────────
    if "mdblist" in providers:
        try:
            key = await _get_mdblist_key(db)
            if key:
                from app.utils.mdblist_client import MDBListClient
                mdb = MDBListClient(api_key=key)

                # --- Helper: build MDBList remove payload ---
                if is_rerate:
                    if item_type == "movie":
                        rm_mdb = {"ids": {}}
                        if imdb_id:
                            rm_mdb["ids"]["imdb"] = imdb_id
                        if tmdb_id:
                            try:
                                rm_mdb["ids"]["tmdb"] = int(tmdb_id)
                            except (ValueError, TypeError):
                                pass
                        rm_resp = await mdb.remove_ratings(movies=[rm_mdb])
                    elif item_type == "episode":
                        show_ids = dict(series_ids_obj) if series_ids_obj else {}
                        if not show_ids and imdb_id:
                            show_ids["imdb"] = imdb_id
                        ep_rm_obj: dict = {}
                        if episode_number is not None:
                            ep_rm_obj["number"] = episode_number
                        if imdb_id:
                            ep_rm_obj["ids"] = {"imdb": imdb_id}
                        rm_show = {
                            "ids": show_ids,
                            "seasons": [{
                                "number": season_number if season_number is not None else 1,
                                "episodes": [ep_rm_obj],
                            }],
                        }
                        rm_resp = await mdb.remove_ratings(shows=[rm_show])
                    else:
                        rm_mdb = {"ids": {}}
                        if imdb_id:
                            rm_mdb["ids"]["imdb"] = imdb_id
                        if tmdb_id:
                            try:
                                rm_mdb["ids"]["tmdb"] = int(tmdb_id)
                            except (ValueError, TypeError):
                                pass
                        rm_resp = await mdb.remove_ratings(shows=[rm_mdb])
                    log.info("rating.mdblist_removed_for_rerate", user_id=user_id,
                             imdb=imdb_id, old_rating=old_rating,
                             item_type=item_type,
                             response=str(rm_resp)[:200])

                # --- Add new rating ---
                mdb_item = {"ids": {}, "rating": rating, "rated_at": now_str}
                if imdb_id:
                    mdb_item["ids"]["imdb"] = imdb_id
                if tmdb_id:
                    try:
                        mdb_item["ids"]["tmdb"] = int(tmdb_id)
                    except (ValueError, TypeError):
                        pass

                if item_type == "movie":
                    resp = await mdb.add_ratings(movies=[mdb_item])
                elif item_type == "episode":
                    show_ids = {}
                    if series_ids_obj:
                        show_ids = dict(series_ids_obj)
                    else:
                        show_ids = dict(mdb_item["ids"])

                    ep_obj: dict = {
                        "rating": rating,
                        "rated_at": now_str,
                    }
                    if episode_number is not None:
                        ep_obj["number"] = episode_number
                    if imdb_id:
                        ep_obj["ids"] = {"imdb": imdb_id}

                    show_wrapper = {
                        "ids": show_ids,
                        "seasons": [{
                            "number": season_number if season_number is not None else 1,
                            "episodes": [ep_obj],
                        }],
                    }
                    resp = await mdb.add_ratings(shows=[show_wrapper])
                    log.debug("rating.mdblist_episode_payload",
                              show_ids=show_ids,
                              season=season_number,
                              episode=episode_number,
                              ep_imdb=imdb_id,
                              response=str(resp)[:300])
                else:
                    resp = await mdb.add_ratings(shows=[mdb_item])
                results["mdblist"] = {"ok": True, "response": resp}
                log.info("rating.mdblist_pushed", user_id=user_id, imdb=imdb_id,
                         rating=rating, item_type=item_type, rerate=is_rerate,
                         response=str(resp)[:200])
                await mdb.close()
        except Exception as e:
            results["mdblist"] = {"ok": False, "error": str(e)[:200]}
            log.warning("rating.mdblist_failed", error=str(e)[:200])

    # ── Store locally in UserRating ──────────────────────────────────
    try:
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)

        if existing_rating_row:
            existing_rating_row.rating = rating
            existing_rating_row.rated_at = now_naive
            existing_rating_row.source = "user"
            if imdb_id:
                existing_rating_row.imdb_id = imdb_id
            if tmdb_id:
                existing_rating_row.tmdb_id = tmdb_id
        else:
            new_rating = UserRating(
                user_id=user_id,
                simkl_id=imdb_id or tmdb_id or "",
                title=title,
                item_type=item_type,
                rating=rating,
                rated_at=now_naive,
                source="user",
                imdb_id=imdb_id,
                tmdb_id=tmdb_id,
                season_number=season_number if item_type == "episode" else None,
                episode_number=episode_number if item_type == "episode" else None,
                series_name=series_name if item_type == "episode" else None,
            )
            db.add(new_rating)

        await db.commit()
        results["local"] = {"ok": True}
    except Exception as e:
        await db.rollback()
        results["local"] = {"ok": False, "error": str(e)[:200]}
        log.warning("rating.local_failed", error=str(e)[:200])

    # ── Remove from dismissed list if present ────────────────────────
    from app.models.schema import DismissedRatingItem
    try:
        dismiss_key = f"imdb:{imdb_id}" if imdb_id else f"tmdb:{tmdb_id}"
        dismiss_q = select(DismissedRatingItem).where(
            DismissedRatingItem.user_id == user_id,
            DismissedRatingItem.item_key == dismiss_key,
        )
        dismissed = (await db.execute(dismiss_q)).scalar_one_or_none()
        if dismissed:
            await db.delete(dismissed)
            await db.commit()
    except Exception:
        pass

    any_ok = any(r and r.get("ok") for r in results.values())
    return {"ok": any_ok, "rating": rating, "results": results}


@router.post("/api/ratings/dismiss/{user_id}")
async def dismiss_rating_item(
    user_id: int,
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Dismiss an item from the rating prompt so it doesn't reappear.

    Payload: {item_key: "imdb:tt1234567"}
    """
    from app.models.schema import DismissedRatingItem

    item_key = payload.get("item_key", "").strip()
    if not item_key:
        raise HTTPException(400, "item_key required")

    # Check if already dismissed
    existing = (await db.execute(
        select(DismissedRatingItem).where(
            DismissedRatingItem.user_id == user_id,
            DismissedRatingItem.item_key == item_key,
        )
    )).scalar_one_or_none()

    if not existing:
        db.add(DismissedRatingItem(
            user_id=user_id,
            item_key=item_key,
        ))
        await db.commit()

    return {"ok": True, "item_key": item_key}


@router.get("/api/ratings/user/{user_id}")
async def get_user_ratings(
    user_id: int,
    source: str | None = None,
    limit: int = Query(50, ge=1, le=50000),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return user's ratings, optionally filtered by source (user/imported)."""
    from app.models.schema import UserRating

    q = select(UserRating).where(UserRating.user_id == user_id)
    if source:
        q = q.where(UserRating.source == source)
    q = q.order_by(UserRating.rated_at.desc().nullslast()).limit(limit)

    rows = (await db.execute(q)).scalars().all()
    items = []

    # Resolve emby_ids: try cache first, then batch Emby search for misses
    emby_id_map: dict[int, str] = {}  # rating.id → emby_id
    cache_misses: list = []  # (rating_id, title, item_type)

    for r in rows:
        emby_id = None
        try:
            cached = None
            if r.imdb_id:
                cached = await LibraryCache.find_by_provider_id("Imdb", r.imdb_id)
            if not cached and r.tmdb_id:
                cached = await LibraryCache.find_by_provider_id("Tmdb", r.tmdb_id)
            if not cached and r.title:
                cached = await LibraryCache.find_by_title(r.title)
            # For episodes, also try series_name (library indexes Series, not Episodes)
            if not cached and r.item_type == "episode" and r.series_name:
                cached = await LibraryCache.find_by_title(r.series_name)
            if cached:
                emby_id = cached.get("emby_id") or cached.get("Id")
        except Exception:
            pass
        if emby_id:
            emby_id_map[r.id] = emby_id
        elif r.title or (r.item_type == "episode" and r.series_name):
            cache_misses.append((r.id, r.series_name if r.item_type == "episode" and r.series_name else r.title, r.item_type))

    # For cache misses, do batch Emby searches (max 30 to avoid hammering)
    if cache_misses:
        _emby_ur = None
        try:
            from app.utils.emby_client import EmbyClient
            _emby_ur = EmbyClient()
            searched_titles: set[str] = set()
            for rid, title, itype in cache_misses:
                title_lower = title.strip().lower()
                if title_lower in searched_titles:
                    continue
                searched_titles.add(title_lower)
                try:
                    search_type = "Movie" if itype == "movie" else "Series"
                    results = await _emby_ur.search_items(title, item_type=search_type)
                    for res in results:
                        if res.get("Name", "").strip().lower() == title_lower:
                            eid = res.get("Id")
                            # Apply to all ratings with this title
                            for rid2, t2, _ in cache_misses:
                                if t2.strip().lower() == title_lower:
                                    emby_id_map[rid2] = eid
                            break
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            if _emby_ur:
                await _emby_ur.close()

    # Resolve series_emby_id for episode ratings (for poster fallback)
    series_emby_cache: dict[str, str | None] = {}  # series_name → emby_id
    for r in rows:
        if r.item_type == "episode" and r.series_name and r.series_name not in series_emby_cache:
            try:
                cached = await LibraryCache.find_by_title(r.series_name)
                if not cached:
                    cached = await LibraryCache.find_by_title(r.series_name, item_type="series")
                series_emby_cache[r.series_name] = (
                    cached.get("emby_id") or cached.get("Id") if cached else None
                )
            except Exception:
                series_emby_cache[r.series_name] = None

    # Emby search fallback for series that weren't in the cache
    series_cache_misses = [
        name for name, eid in series_emby_cache.items() if eid is None
    ]
    if series_cache_misses:
        _emby_s = None
        try:
            from app.utils.emby_client import EmbyClient
            _emby_s = EmbyClient()
            for sname in series_cache_misses:
                try:
                    results = await _emby_s.search_items(sname, item_type="Series")
                    for res in results:
                        if res.get("Name", "").strip().lower() == sname.strip().lower():
                            series_emby_cache[sname] = res.get("Id")
                            break
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            if _emby_s:
                await _emby_s.close()

    for r in rows:
        series_eid = None
        if r.item_type == "episode" and r.series_name:
            series_eid = series_emby_cache.get(r.series_name)
        items.append({
            "id": r.id,
            "title": r.title,
            "item_type": r.item_type,
            "rating": r.rating,
            "imdb_id": r.imdb_id,
            "tmdb_id": r.tmdb_id,
            "simkl_id": r.simkl_id,
            "source": r.source or "imported",
            "rated_at": r.rated_at.isoformat() if r.rated_at else None,
            "emby_id": emby_id_map.get(r.id),
            "series_emby_id": series_eid,
            "season_number": r.season_number,
            "episode_number": r.episode_number,
            "series_name": r.series_name,
        })
    return {"items": items, "count": len(items)}


@router.post("/api/ratings/sync/{user_id}")
async def sync_ratings_from_providers(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Pull ratings from Simkl + MDBList and store locally with imdb_id/tmdb_id.

    This ensures the unrated-items endpoint correctly filters out items
    the user has already rated on either provider. Preserves source='user' ratings.
    """
    from app.models.schema import UserRating
    from sqlalchemy import delete

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    log = structlog.get_logger()
    providers = await _get_active_providers(db)
    imported_rows: list[dict] = []
    seen_imdb: set[str] = set()

    # ── Simkl ratings ──
    if "simkl" in providers and user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            raw = await simkl.get_user_ratings(kind="all")
            for entry in raw:
                item = entry.get("movie") or entry.get("show") or entry
                ids = item.get("ids", {})
                imdb = ids.get("imdb", "")
                tmdb = str(ids.get("tmdb", "")) if ids.get("tmdb") else ""
                if imdb:
                    seen_imdb.add(imdb)
                imported_rows.append({
                    "simkl_id": str(ids.get("simkl") or ids.get("simkl_id") or ""),
                    "title": item.get("title", ""),
                    "item_type": "movie" if entry.get("_type", "").startswith("movie") or "movie" in entry else "show",
                    "rating": entry.get("rating", 0),
                    "imdb_id": imdb or None,
                    "tmdb_id": tmdb or None,
                    "rated_at": entry.get("rated_at"),
                })
            await simkl.close()
            log.info("ratings_sync.simkl_fetched", count=len(raw), user_id=user_id)
        except Exception as e:
            log.warning("ratings_sync.simkl_failed", error=str(e)[:200])

    # ── MDBList ratings ──
    if "mdblist" in providers:
        try:
            key = await _get_mdblist_key(db)
            if key:
                from app.utils.mdblist_client import MDBListClient
                mdb = MDBListClient(api_key=key)
                try:
                    mdb_ratings = await mdb.get_all_ratings()
                    if isinstance(mdb_ratings, dict):
                        for kind, item_type in (("movies", "movie"), ("shows", "show")):
                            for item in mdb_ratings.get(kind, []):
                                inner = item.get("movie") or item.get("show") or item
                                rating = item.get("rating")
                                if rating is None:
                                    continue
                                ids = inner.get("ids", {})
                                if not isinstance(ids, dict):
                                    ids = {}
                                imdb = ids.get("imdb", "") or inner.get("imdb_id", "") or ""
                                if not imdb:
                                    continue
                                if imdb in seen_imdb:
                                    continue
                                seen_imdb.add(imdb)
                                tmdb = str(ids.get("tmdb", "")) if ids.get("tmdb") else ""
                                imported_rows.append({
                                    "simkl_id": str(ids.get("simkl") or ""),
                                    "title": inner.get("title", ""),
                                    "item_type": item_type,
                                    "rating": int(round(float(rating))),
                                    "imdb_id": imdb or None,
                                    "tmdb_id": tmdb or None,
                                    "rated_at": item.get("rated_at"),
                                })
                        # ── MDBList episode ratings ──
                        for item in mdb_ratings.get("episodes", []):
                            ep_inner = item.get("episode") or item
                            show_inner = item.get("show") or {}
                            rating = item.get("rating")
                            if rating is None:
                                continue
                            ep_ids = ep_inner.get("ids", {})
                            if not isinstance(ep_ids, dict):
                                ep_ids = {}
                            show_ids = show_inner.get("ids", {}) if isinstance(show_inner.get("ids"), dict) else {}
                            ep_imdb = ep_ids.get("imdb", "") or ""
                            # Use a composite key for episode dedup
                            ep_dedup = ep_imdb or f"ep:{show_inner.get('title', '')}:s{ep_inner.get('season', '')}e{ep_inner.get('number', '')}"
                            if ep_dedup in seen_imdb:
                                continue
                            seen_imdb.add(ep_dedup)
                            tmdb = str(ep_ids.get("tmdb", "")) if ep_ids.get("tmdb") else ""
                            imported_rows.append({
                                "simkl_id": str(ep_ids.get("simkl") or ""),
                                "title": ep_inner.get("title", ""),
                                "item_type": "episode",
                                "rating": int(round(float(rating))),
                                "imdb_id": ep_imdb or None,
                                "tmdb_id": tmdb or None,
                                "rated_at": item.get("rated_at"),
                                "season_number": ep_inner.get("season"),
                                "episode_number": ep_inner.get("number") or ep_inner.get("episode"),
                                "series_name": show_inner.get("title", ""),
                            })
                finally:
                    await mdb.close()
                log.info("ratings_sync.mdblist_fetched", count=len(imported_rows), user_id=user_id)
        except Exception as e:
            log.warning("ratings_sync.mdblist_failed", error=str(e)[:200])

    # ── Persist: delete old imports, keep user-submitted ──
    await db.execute(
        delete(UserRating).where(
            UserRating.user_id == user_id,
            UserRating.source != "user",
        )
    )

    user_rated_q = select(UserRating.imdb_id).where(
        UserRating.user_id == user_id,
        UserRating.source == "user",
        UserRating.imdb_id.isnot(None),
    )
    user_rated_imdb = set(r for r in (await db.execute(user_rated_q)).scalars().all() if r)

    # Also build set of user-submitted episode keys for dedup
    user_ep_q = select(
        UserRating.series_name, UserRating.season_number, UserRating.episode_number
    ).where(
        UserRating.user_id == user_id,
        UserRating.source == "user",
        UserRating.item_type == "episode",
        UserRating.series_name.isnot(None),
    )
    user_rated_episodes = set()
    for row in (await db.execute(user_ep_q)).all():
        if row[0] and row[1] is not None and row[2] is not None:
            user_rated_episodes.add(f"ep:{row[0].lower()}:s{row[1]}e{row[2]}")

    added = 0
    for r in imported_rows:
        if r.get("imdb_id") and r["imdb_id"] in user_rated_imdb:
            continue
        # Check episode-level dedup for items without IMDB
        if r.get("item_type") == "episode" and r.get("series_name") and r.get("season_number") is not None and r.get("episode_number") is not None:
            ep_key = f"ep:{r['series_name'].lower()}:s{r['season_number']}e{r['episode_number']}"
            if ep_key in user_rated_episodes:
                continue
        if not r.get("rating"):
            continue
        db.add(UserRating(
            user_id=user_id,
            simkl_id=r.get("simkl_id") or "",
            title=r["title"],
            item_type=r["item_type"],
            rating=r["rating"],
            source="imported",
            imdb_id=r.get("imdb_id"),
            tmdb_id=r.get("tmdb_id"),
            season_number=r.get("season_number"),
            episode_number=r.get("episode_number"),
            series_name=r.get("series_name"),
            rated_at=(
                datetime.fromisoformat(r["rated_at"].replace("Z", "+00:00"))
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
                if r.get("rated_at") else None
            ),
        ))
        added += 1

    await db.commit()
    log.info("ratings_sync.done", imported=added, user_id=user_id)
    return {"ok": True, "imported": added, "providers": list(providers)}


# ── Rating edit / delete ─────────────────────────────────────────────────

@router.put("/api/ratings/{rating_id}")
async def update_user_rating(
    rating_id: int,
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update a user-submitted rating value."""
    from app.models.schema import UserRating

    row = (await db.execute(
        select(UserRating).where(UserRating.id == rating_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Rating not found")
    if row.user_id != current_user.id:
        raise HTTPException(403, "Not your rating")
    if row.source != "user":
        raise HTTPException(400, "Only user-submitted ratings can be edited")

    new_rating = payload.get("rating")
    if not new_rating or not isinstance(new_rating, (int, float)) or new_rating < 1 or new_rating > 10:
        raise HTTPException(400, "rating must be 1-10")

    new_rating = int(new_rating)
    old_rating = row.rating
    row.rating = new_rating
    row.rated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.commit()

    log = structlog.get_logger()

    # Push updated rating to providers
    providers = await _get_active_providers(db)
    user = (await db.execute(select(User).where(User.id == row.user_id))).scalar_one_or_none()
    ids_obj = {}
    if row.imdb_id:
        ids_obj["imdb"] = row.imdb_id
    if row.tmdb_id:
        try:
            ids_obj["tmdb"] = int(row.tmdb_id)
        except (ValueError, TypeError):
            ids_obj["tmdb"] = row.tmdb_id

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    if ids_obj and "simkl" in providers and user and user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            simkl_item = {
                "ids": ids_obj,
                "rating": new_rating,
                "rated_at": now_str,
                "_type": "movies" if row.item_type == "movie" else "shows",
            }
            await simkl.add_ratings([simkl_item])
            await simkl.close()
        except Exception as e:
            log.warning("rating_update.simkl_failed", error=str(e)[:200])

    log.info("rating.updated", rating_id=rating_id, old=old_rating, new=new_rating)
    return {"ok": True, "rating_id": rating_id, "old_rating": old_rating, "new_rating": new_rating}


@router.delete("/api/ratings/{rating_id}")
async def delete_user_rating(
    rating_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a user-submitted rating."""
    from app.models.schema import UserRating

    row = (await db.execute(
        select(UserRating).where(UserRating.id == rating_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Rating not found")
    if row.user_id != current_user.id:
        raise HTTPException(403, "Not your rating")
    if row.source != "user":
        raise HTTPException(400, "Only user-submitted ratings can be deleted")

    log = structlog.get_logger()

    # Remove from providers
    providers = await _get_active_providers(db)
    user = (await db.execute(select(User).where(User.id == row.user_id))).scalar_one_or_none()
    ids_obj = {}
    if row.imdb_id:
        ids_obj["imdb"] = row.imdb_id
    if row.tmdb_id:
        try:
            ids_obj["tmdb"] = int(row.tmdb_id)
        except (ValueError, TypeError):
            ids_obj["tmdb"] = row.tmdb_id

    if ids_obj and "simkl" in providers and user and user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            simkl_item = {
                "ids": ids_obj,
                "_type": "movies" if row.item_type == "movie" else "shows",
            }
            await simkl.remove_ratings([simkl_item])
            await simkl.close()
        except Exception as e:
            log.warning("rating_delete.simkl_failed", error=str(e)[:200])

    title = row.title
    await db.delete(row)
    await db.commit()
    log.info("rating.deleted", rating_id=rating_id, title=title)
    return {"ok": True, "rating_id": rating_id}


# ═══════════════════════════════════════════════════════════════════════════

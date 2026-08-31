"""Routes extracted from routes.py — watch_history_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import distinct, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user, require_user_ownership
from app.middleware.rate_limit import LIMITS, limiter
from app.api.route_helpers import _validate_item_key
from app.services.watch_stats.service import WatchStatsService

log = structlog.get_logger()

router = APIRouter()

watch_stats_svc = WatchStatsService()


@router.get("/api/stats/person-items")
async def get_person_items(
    name: str,
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return all library items (movies + series) featuring a person.

    Queries Emby by person name — returns everything in the library,
    not just played items.  Used by the stats page hover modals.
    """
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "User not found")

    from app.utils.emby_client import EmbyClient

    emby = EmbyClient()
    try:
        resp = await emby.get_items(
            user_id=user.emby_user_id,
            item_type="Movie,Series",
            fields="ProductionYear",
            recursive=True,
            limit=200,
            extra_params={"Person": name},
        )
        items = resp.get("Items", [])
        titles = []
        for item in items:
            title = item.get("Name", "Unknown")
            year = item.get("ProductionYear")
            display = f"{title} ({year})" if year else title
            titles.append(display)
        return {"name": name, "titles": sorted(titles), "count": len(titles)}
    finally:
        await emby.close()


@router.get("/api/stats/{user_id}")
async def get_watch_stats(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return aggregated watch history stats for a user."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await watch_stats_svc.get_stats(user)


@router.get("/stats", response_class=HTMLResponse)
async def get_stats_page():
    """Serve the Watch Stats page."""
    try:
        with open("frontend/templates/stats.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/rewatch", response_class=HTMLResponse)
async def get_rewatch_page():
    """Serve the Rewatch Recommender page."""
    try:
        with open("frontend/templates/rewatch.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/guide", response_class=HTMLResponse)
async def get_guide_page():
    """Serve the User Guide page."""
    try:
        with open("frontend/templates/guide.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/watch-history", response_class=HTMLResponse)
async def get_watch_history_page():
    """Serve the Watch History timeline page."""
    try:
        with open("frontend/templates/watch_history.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/watchlist", response_class=HTMLResponse)
async def get_watchlist_page():
    """Serve the unified watchlist page."""
    try:
        with open("frontend/templates/watchlist.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/api/watchlist/merged/{user_id}")
async def get_merged_watchlist(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Fetch watchlists from Simkl + MDBList, merge/dedup by IMDB ID,
    and cross-reference against library cache + arr status."""
    import asyncio
    from app.utils.library_cache import LibraryCache

    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    # Fetch from both providers concurrently
    simkl = None
    mdblist = None

    try:
        from app.utils.simkl_client import SimklClient
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )
    except Exception:
        pass

    try:
        from app.utils.mdblist_client import MDBListClient
        from app.utils.redis_cache import get_redis as _get_redis
        from app.utils.secure_redis import secure_get
        _r = await _get_redis()
        raw_key = await secure_get("mdblist_api_key")
        mdb_key = (raw_key if isinstance(raw_key, str) else raw_key.decode()) if raw_key else ""
        if mdb_key:
            mdblist = MDBListClient(api_key=mdb_key)
    except Exception:
        pass

    async def _fetch_simkl():
        if not simkl:
            return []
        try:
            movies = await simkl.get_watchlist(kind="movies")
            await asyncio.sleep(1.1)
            shows = await simkl.get_watchlist(kind="shows")
            return [("simkl", "movie", m) for m in movies] + [("simkl", "show", s) for s in shows]
        except Exception as e:
            log.warning("watchlist.simkl_fetch_failed", error=str(e)[:120])
            return []

    async def _fetch_mdblist():
        if not mdblist:
            return []
        try:
            data = await mdblist.get_watchlist()
            results = []
            for m in (data.get("movies") or []):
                results.append(("mdblist", "movie", m))
            for s in (data.get("shows") or []):
                results.append(("mdblist", "show", s))
            return results
        except Exception as e:
            log.warning("watchlist.mdblist_fetch_failed", error=str(e)[:120])
            return []

    simkl_raw, mdblist_raw = await asyncio.gather(
        _fetch_simkl(), _fetch_mdblist()
    )

    if simkl:
        await simkl.close()
    if mdblist:
        await mdblist.close()

    # Merge by IMDB ID, keeping track of which providers have each item
    seen: dict[str, dict] = {}  # imdb_id -> merged item

    def _unwrap_item(item):
        """Unwrap movie/show sub-objects — MDBList may wrap items as
        {"movie": {...}} or {"show": {...}}, Simkl /sync/all-items
        returns flat objects.  Return the inner item either way."""
        if isinstance(item.get("movie"), dict):
            return item["movie"]
        if isinstance(item.get("show"), dict):
            return item["show"]
        return item

    def _extract_ids(provider, inner):
        ids = inner.get("ids", {})
        if provider == "mdblist":
            return (
                ids.get("imdb") or inner.get("imdb_id") or inner.get("imdb"),
                str(ids.get("tmdb") or inner.get("tmdb_id") or inner.get("tmdb") or ""),
                str(ids.get("tvdb") or inner.get("tvdb_id") or inner.get("tvdb") or ""),
            )
        else:
            return (
                ids.get("imdb"),
                str(ids.get("tmdb") or ""),
                str(ids.get("tvdb") or ""),
            )

    for provider, item_type, item in simkl_raw + mdblist_raw:
        inner = _unwrap_item(item)
        imdb_id, tmdb_id, tvdb_id = _extract_ids(provider, inner)
        title = inner.get("title") or inner.get("name") or item.get("title") or ""
        key = imdb_id or tmdb_id or title
        if not key:
            continue

        if key not in seen:
            year = inner.get("year") or item.get("year") or None
            seen[key] = {
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "tvdb_id": tvdb_id,
                "title": title,
                "year": year,
                "item_type": item_type,
                "providers": [],
                "in_library": False,
                "emby_id": None,
                "poster": None,
            }

        if provider not in seen[key]["providers"]:
            seen[key]["providers"].append(provider)
        if not seen[key]["imdb_id"] and imdb_id:
            seen[key]["imdb_id"] = imdb_id
        if not seen[key]["tmdb_id"] and tmdb_id:
            seen[key]["tmdb_id"] = tmdb_id

    # Cross-reference against library cache
    for key, item in seen.items():
        for pid_type, pid_val in [("Imdb", item["imdb_id"]), ("Tmdb", item["tmdb_id"])]:
            if not pid_val:
                continue
            cached = await LibraryCache.find_by_provider_id(pid_type, str(pid_val))
            if cached:
                item["in_library"] = True
                item["emby_id"] = cached.get("emby_id")
                item["poster"] = cached.get("poster")
                break

    # For items not in library, fetch TMDB poster paths concurrently
    missing_items = [v for v in seen.values() if not v["in_library"] and v.get("tmdb_id")]
    if missing_items:
        from app.utils.tmdb_client import get_full_details

        _tmdb_sem = asyncio.Semaphore(5)  # max 5 concurrent TMDB requests

        async def _enrich_item(item):
            async with _tmdb_sem:
                try:
                    tmdb_id = int(item["tmdb_id"])
                    media_type = "movie" if item["item_type"] == "movie" else "tv"
                    details = await get_full_details(tmdb_id, media_type)
                    if details and details.get("poster_path"):
                        item["tmdb_poster"] = details["poster_path"]
                    if not item.get("title") or item["title"] == "?":
                        item["title"] = details.get("title") or details.get("name") or item.get("title", "")
                    if not item.get("year") and details:
                        rd = details.get("release_date") or details.get("first_air_date") or ""
                        if rd:
                            item["year"] = int(rd[:4]) if rd[:4].isdigit() else None
                except Exception:
                    pass

        await asyncio.gather(*[_enrich_item(item) for item in missing_items])

    items = sorted(seen.values(), key=lambda x: (x.get("title") or "").lower())

    return {
        "items": items,
        "total": len(items),
        "simkl_count": len(simkl_raw),
        "mdblist_count": len(mdblist_raw),
    }


@router.delete("/api/watchlist/remove")
async def remove_from_watchlist(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Remove an item from both Simkl and MDBList watchlists.

    Payload: {imdb_id, tmdb_id?, item_type (movie|show), title?}
    """
    import asyncio

    imdb_id = payload.get("imdb_id")
    tmdb_id = payload.get("tmdb_id")
    item_type = payload.get("item_type", "movie")
    title = payload.get("title", "")

    if not imdb_id and not tmdb_id:
        raise HTTPException(400, "imdb_id or tmdb_id required")

    user = (await db.execute(
        select(User).where(User.id == _user.id)
    )).scalar_one_or_none()

    results = {"simkl": None, "mdblist": None}

    # Build item payloads
    ids_obj = {}
    if imdb_id:
        ids_obj["imdb"] = imdb_id
    if tmdb_id:
        ids_obj["tmdb"] = int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id

    async def _remove_simkl():
        try:
            from app.utils.simkl_client import SimklClient
            client = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            item = {"ids": ids_obj, "title": title, "type": item_type}
            if item_type == "show":
                item["_type"] = "show"
            result = await client.remove_from_watchlist([item])
            await client.close()
            return {"ok": True, "result": result}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}

    async def _remove_mdblist():
        try:
            from app.utils.mdblist_client import MDBListClient
            from app.utils.secure_redis import secure_get
            raw_key = await secure_get("mdblist_api_key")
            mdb_key = (raw_key if isinstance(raw_key, str) else raw_key.decode()) if raw_key else ""
            if not mdb_key:
                return {"ok": False, "error": "MDBList API key not configured"}
            client = MDBListClient(api_key=mdb_key)
            if item_type == "movie":
                result = await client.remove_from_watchlist(movies=[ids_obj])
            else:
                result = await client.remove_from_watchlist(shows=[ids_obj])
                # Mirror "dropped" status to MDBList for shows —
                # Simkl sets "dropped" on remove; keep MDBList in sync.
                try:
                    await client.add_dropped(shows=[ids_obj])
                except Exception:
                    pass
            await client.close()
            return {"ok": True, "result": result}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}

    results["simkl"], results["mdblist"] = await asyncio.gather(
        _remove_simkl(), _remove_mdblist()
    )

    # Add to manual_arr_exclude so watchlist sync won't re-add these
    # items from Radarr/Sonarr on the next cycle
    try:
        r = await get_redis()
        if tmdb_id:
            await r.sadd("manual_arr_exclude:tmdb", str(tmdb_id))
            await r.sadd("watchlist_removed:tmdb", str(tmdb_id))
        if payload.get("tvdb_id"):
            await r.sadd("manual_arr_exclude:tvdb", str(payload["tvdb_id"]))
            await r.sadd("watchlist_removed:tvdb", str(payload["tvdb_id"]))
        if imdb_id:
            await r.sadd("watchlist_removed:imdb", str(imdb_id))
    except Exception:
        pass

    all_ok = results["simkl"].get("ok") and results["mdblist"].get("ok")
    log.info("watchlist.removed", imdb_id=imdb_id, item_type=item_type,
             simkl_ok=results["simkl"].get("ok"),
             mdblist_ok=results["mdblist"].get("ok"))

    return {"status": "ok" if all_ok else "partial", "results": results}


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/continue-watching/{user_id}")
async def get_continue_watching(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Return items the user started watching but hasn't finished.

    Uses Emby's ``Filters=IsResumable`` to find movies and episodes
    with an active playback resume point.  Episodes are grouped by
    their parent series for a cleaner display.
    """
    import json as _json

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "User not found or no Emby user linked")

    # Changed cache key so old stale cache is bypassed
    cache_key = f"continue_watching_v2:{user.id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    emby = EmbyClient()
    try:
        # Fetch all resumable items (movies + episodes with a resume point)
        start = 0
        batch = 500
        all_resumable: list[dict] = []
        while True:
            resp = await emby.get_items(
                user_id=user.emby_user_id,
                fields="ProviderIds,UserData,UserDataLastPlayedDate,RunTimeTicks",
                filters="IsResumable",
                sort_by="DatePlayed",
                sort_order="Descending",
                limit=batch,
                start_index=start,
            )
            all_resumable.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
    finally:
        await emby.close()

    log.info("continue_watching.fetched", resumable_count=len(all_resumable))

    movies: list[dict] = []
    # Group episodes by series
    series_map: dict[str, dict] = {}  # series_id → {info + episodes}

    for item in all_resumable:
        item_type = item.get("Type", "")
        ud = item.get("UserData", {})
        position_ticks = ud.get("PlaybackPositionTicks", 0) or 0
        runtime_ticks = item.get("RunTimeTicks", 0) or 0
        last_played = ud.get("LastPlayedDate")

        # Calculate progress percentage
        progress_pct = round(position_ticks / runtime_ticks * 100, 1) if runtime_ticks > 0 else 0

        # Calculate how long ago
        days_ago = None
        if last_played:
            try:
                lp_dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                days_ago = (datetime.now(timezone.utc) - lp_dt).days
            except (ValueError, AttributeError):
                pass

        if item_type == "Movie":
            movies.append({
                "emby_id": item.get("Id", ""),
                "title": item.get("Name", ""),
                "year": item.get("ProductionYear"),
                "type": "movie",
                "progress_pct": progress_pct,
                "position_ticks": position_ticks,
                "last_played": last_played,
                "days_ago": days_ago,
                "imdb_id": item.get("ProviderIds", {}).get("Imdb"),
            })

        elif item_type == "Episode":
            series_id = item.get("SeriesId", "")
            series_name = item.get("SeriesName", "")
            s_num = item.get("ParentIndexNumber", 0)
            e_num = item.get("IndexNumber", 0)
            ep_name = item.get("Name", "")

            if series_id not in series_map:
                series_map[series_id] = {
                    "emby_id": series_id,
                    "title": series_name,
                    "type": "show",
                    "episodes": [],
                    "last_played": last_played,
                    "days_ago": days_ago,
                }

            series_map[series_id]["episodes"].append({
                "emby_id": item.get("Id", ""),
                "season": s_num,
                "episode": e_num,
                "title": ep_name,
                "progress_pct": progress_pct,
                "position_ticks": position_ticks,
                "last_played": last_played,
            })

            # Update series-level last_played to the most recent episode
            existing_days = series_map[series_id].get("days_ago")
            if days_ago is not None and (existing_days is None or days_ago < existing_days):
                series_map[series_id]["days_ago"] = days_ago
                series_map[series_id]["last_played"] = last_played

    shows = list(series_map.values())
    for show in shows:
        show["episode_count"] = len(show["episodes"])
        show["episodes"].sort(key=lambda e: (e["season"], e["episode"]))
        # Set resume target to the most recently played episode
        most_recent = max(show["episodes"], key=lambda e: e.get("last_played") or "", default=None)
        if most_recent:
            show["resume_emby_id"] = most_recent["emby_id"]
            show["resume_ticks"] = most_recent.get("position_ticks", 0)

    # Combine and sort: oldest first (most abandoned)
    all_items = movies + shows
    all_items.sort(key=lambda x: x.get("days_ago") or 0, reverse=True)

    result = {"items": all_items, "total": len(all_items)}

    try:
        r = await get_redis()
        await r.setex(cache_key, 3600, _json.dumps(result))
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/watch-history/{user_id}")
async def get_watch_history(
    user_id: int,
    limit: int = 50,
    offset: int = 0,
    item_type: str | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return paginated watch history for a user."""
    require_user_ownership(current_user.id, user_id, "watch_history")
    from app.models.schema import WatchHistory
    q = select(WatchHistory).where(WatchHistory.user_id == user_id)
    if item_type:
        q = q.where(WatchHistory.item_type == item_type)
    q = q.order_by(WatchHistory.watched_at.desc()).offset(offset).limit(limit)

    count_q = select(func.count(WatchHistory.id)).where(WatchHistory.user_id == user_id)
    if item_type:
        count_q = count_q.where(WatchHistory.item_type == item_type)

    rows = (await db.execute(q)).scalars().all()
    total = (await db.execute(count_q)).scalar() or 0

    return {
        "items": [
            {
                "id": r.id,
                "emby_id": r.emby_id,
                "item_type": r.item_type,
                "title": r.title,
                "series_name": r.series_name,
                "season_number": r.season_number,
                "episode_number": r.episode_number,
                "imdb_id": r.imdb_id,
                "tmdb_id": r.tmdb_id,
                "watched_at": r.watched_at.isoformat() if r.watched_at else None,
                "runtime_minutes": r.runtime_minutes,
                "source": r.source,
            }
            for r in rows
        ],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/watch-history/{user_id}/by-date")
async def get_watch_history_by_date(
    user_id: int,
    before: str | None = None,
    item_type: str | None = None,
    rating_filter: str | None = None,
    page: int = 1,
    page_size: int = 60,
    days: int = 30,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return watch history grouped by date for the timeline page.

    Items watched multiple times on the same day are collapsed into one
    entry with a play_count.  Dedup uses a normalised key built from
    item_type + title (or series+season+episode for episodes) so that
    rows from different backfill sources (webhook / simkl / emby) that
    describe the same logical watch merge correctly.

    Items without an ``emby_id`` (Simkl backfill) are resolved against
    the Redis library cache so images can be served.
    """
    from app.models.schema import WatchHistory
    from sqlalchemy import cast, Date
    from collections import OrderedDict

    filters = [WatchHistory.user_id == user_id]
    # Parse multi-select type filter (comma-separated, e.g. "movie,show")
    requested_types = set()
    if item_type:
        requested_types = {t.strip() for t in item_type.split(",") if t.strip() in ("movie", "episode", "show")}
    if requested_types and len(requested_types) < 3:
        # Map requested types to DB item_type values
        db_types: set[str] = set()
        if "movie" in requested_types:
            db_types.add("movie")
        if "show" in requested_types or "episode" in requested_types:
            db_types.add("episode")
        if len(db_types) == 1:
            filters.append(WatchHistory.item_type == next(iter(db_types)))
        else:
            filters.append(WatchHistory.item_type.in_(db_types))
    if before:
        try:
            before_date = datetime.strptime(before, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(400, "before must be YYYY-MM-DD")
        filters.append(cast(WatchHistory.watched_at, Date) < before_date)

    # ── Rating filter: join UserRating to filter by score ──────────
    rated_imdb_set: set[str] | None = None
    unrated_mode = False
    if rating_filter:
        from app.models.schema import UserRating as UR
        if rating_filter == "unrated":
            unrated_mode = True
            # Get all rated IMDB IDs so we can exclude them
            rated_q = select(UR.imdb_id).where(
                UR.user_id == user_id,
                UR.imdb_id.isnot(None),
            )
            rated_imdb_set = {r for r in (await db.execute(rated_q)).scalars().all() if r}
        else:
            try:
                rating_val = int(rating_filter)
            except ValueError:
                rating_val = None
            if rating_val and 1 <= rating_val <= 10:
                rated_q = select(UR.imdb_id).where(
                    UR.user_id == user_id,
                    UR.rating == rating_val,
                    UR.imdb_id.isnot(None),
                )
                rated_imdb_set = {r for r in (await db.execute(rated_q)).scalars().all() if r}

    # ── Total items count (distinct items, no rewatches) ────────────
    # Build a dedup expression matching the view's collapse behaviour:
    #   Movies:   distinct by imdb_id (or title fallback)
    #   Shows:    distinct series (by series_name or title)
    #   Episodes: distinct (series+season+episode) combos
    #   All:      movies + episodes deduplied by their own keys
    from sqlalchemy import case, cast as sa_cast, String as SAString

    total_count_filters = [WatchHistory.user_id == user_id]
    if requested_types and len(requested_types) < 3:
        tc_db_types: set[str] = set()
        if "movie" in requested_types:
            tc_db_types.add("movie")
        if "show" in requested_types or "episode" in requested_types:
            tc_db_types.add("episode")
        if len(tc_db_types) == 1:
            total_count_filters.append(WatchHistory.item_type == next(iter(tc_db_types)))
        else:
            total_count_filters.append(WatchHistory.item_type.in_(tc_db_types))

    # Choose the right dedup expression for the active filter
    if requested_types == {"movie"}:
        # Unique movies by imdb_id (or title fallback)
        dedup_expr = func.coalesce(WatchHistory.imdb_id, WatchHistory.title)
    elif requested_types == {"show"}:
        # Unique series (collapsed view)
        dedup_expr = func.coalesce(WatchHistory.series_name, WatchHistory.title)
    elif requested_types == {"episode"}:
        # Unique episodes by series+season+episode
        dedup_expr = func.concat(
            func.coalesce(WatchHistory.imdb_id, WatchHistory.series_name, WatchHistory.title),
            '|', func.coalesce(sa_cast(WatchHistory.season_number, SAString), '-1'),
            '|', func.coalesce(sa_cast(WatchHistory.episode_number, SAString), '-1'),
        )
    else:
        # All / mixed — movies dedup by imdb_id, episodes dedup by series+season+ep
        dedup_expr = case(
            (WatchHistory.item_type == "movie",
             func.concat('mov|', func.coalesce(WatchHistory.imdb_id, WatchHistory.title))),
            else_=func.concat(
                'ep|', func.coalesce(WatchHistory.imdb_id, WatchHistory.series_name, WatchHistory.title),
                '|', func.coalesce(sa_cast(WatchHistory.season_number, SAString), '-1'),
                '|', func.coalesce(sa_cast(WatchHistory.episode_number, SAString), '-1'),
            ),
        )

    total_items = (await db.execute(
        select(func.count(distinct(dedup_expr))).where(*total_count_filters)
    )).scalar() or 0

    # Adjust total for rating filter — use same distinct dedup expression
    if rated_imdb_set is not None and total_items > 0:
        if unrated_mode:
            # Count distinct items that ARE rated (to subtract)
            if rated_imdb_set:
                rated_count = (await db.execute(
                    select(func.count(distinct(dedup_expr))).where(
                        *total_count_filters,
                        WatchHistory.imdb_id.in_(rated_imdb_set),
                    )
                )).scalar() or 0
            else:
                rated_count = 0
            total_items = max(0, total_items - rated_count)
        else:
            # Count only distinct items whose imdb_id IS in rated set
            if rated_imdb_set:
                total_items = (await db.execute(
                    select(func.count(distinct(dedup_expr))).where(
                        *total_count_filters,
                        WatchHistory.imdb_id.in_(rated_imdb_set),
                    )
                )).scalar() or 0
            else:
                total_items = 0

    q = (
        select(WatchHistory)
        .where(*filters)
        .order_by(WatchHistory.watched_at.desc())
        .limit(days * 25)
    )
    rows = (await db.execute(q)).scalars().all()

    # ── Normalised dedup key ────────────────────────────────────────
    # Collapse episodes to series-level cards only when "show" is selected
    # without "episode" (episode = more granular view takes precedence)
    collapse_to_show = ("show" in requested_types and "episode" not in requested_types)

    def _dedup_key(r):
        """Build a stable key that merges rows from different sources."""
        if r.item_type == "episode":
            series = (r.series_name or "").strip().lower()
            if collapse_to_show:
                # Collapse all episodes of the same series into one entry
                return f"show|{series}"
            sn = r.season_number if r.season_number is not None else -1
            en = r.episode_number if r.episode_number is not None else -1
            return f"ep|{series}|{sn}|{en}"
        # movie — prefer imdb_id, fall back to normalised title
        if r.imdb_id:
            return f"mov|imdb:{r.imdb_id}"
        return f"mov|{(r.title or '').strip().lower()}"

    # ── Group by date, dedup within each day ────────────────────────
    day_map: OrderedDict[str, dict] = OrderedDict()
    for r in rows:
        if not r.watched_at:
            continue
        date_str = r.watched_at.strftime("%Y-%m-%d")
        if date_str not in day_map:
            if len(day_map) >= days:
                break
            day_map[date_str] = {}

        key = _dedup_key(r)
        bucket = day_map[date_str]
        if key in bucket:
            bucket[key]["play_count"] += 1
            # Prefer the row that has an emby_id (for images)
            if r.emby_id and not bucket[key]["emby_id"]:
                bucket[key]["emby_id"] = r.emby_id
            # Prefer non-empty title / series_name
            if r.title and not bucket[key]["title"]:
                bucket[key]["title"] = r.title
            if r.series_name and not bucket[key]["series_name"]:
                bucket[key]["series_name"] = r.series_name
            # Keep highest progress
            rp = r.progress if r.progress is not None else 0
            bp = bucket[key]["progress"] if bucket[key]["progress"] is not None else 0
            if rp > bp:
                bucket[key]["progress"] = r.progress
        else:
            bucket[key] = {
                "emby_id": r.emby_id,
                "item_type": "show" if (collapse_to_show and r.item_type == "episode") else r.item_type,
                "title": r.title,
                "series_name": r.series_name,
                "season_number": r.season_number,
                "episode_number": r.episode_number,
                "imdb_id": r.imdb_id,
                "tmdb_id": r.tmdb_id,
                "tvdb_id": r.tvdb_id,
                "progress": r.progress,
                "runtime_minutes": r.runtime_minutes,
                "play_count": 1,
            }

    # ── Filter out items with <2% progress (accidental opens) ───────
    for _date_str, bucket in list(day_map.items()):
        to_remove = [k for k, v in bucket.items()
                     if v.get("progress") is not None and v["progress"] < 2]
        for k in to_remove:
            del bucket[k]

    # ── Apply rating filter (after dedup so we have imdb_ids) ─────
    if rated_imdb_set is not None:
        for _date_str, bucket in list(day_map.items()):
            if unrated_mode:
                # Keep only items whose imdb_id is NOT in the rated set
                to_remove = [k for k, v in bucket.items()
                             if v.get("imdb_id") and v["imdb_id"] in rated_imdb_set]
            else:
                # Keep only items whose imdb_id IS in the rated set
                to_remove = [k for k, v in bucket.items()
                             if not v.get("imdb_id") or v["imdb_id"] not in rated_imdb_set]
            for k in to_remove:
                del bucket[k]
        # Remove empty days
        for _date_str in [d for d, b in day_map.items() if not b]:
            del day_map[_date_str]

    # ── Resolve missing emby_ids from library cache ─────────────────
    items_needing_id: list[dict] = []
    for _date_str, bucket in day_map.items():
        for item in bucket.values():
            if not item["emby_id"]:
                items_needing_id.append(item)

    if items_needing_id:
        for item in items_needing_id:
            resolved = None
            # Try provider IDs first (fast Redis lookup)
            if item.get("imdb_id"):
                resolved = await LibraryCache.find_by_provider_id("Imdb", item["imdb_id"])
            if not resolved and item.get("tmdb_id"):
                resolved = await LibraryCache.find_by_provider_id("Tmdb", item["tmdb_id"])
            if not resolved and item.get("tvdb_id"):
                resolved = await LibraryCache.find_by_provider_id("Tvdb", item["tvdb_id"])
            # Fall back to title search
            if not resolved:
                search_title = item.get("series_name") or item.get("title")
                if search_title:
                    resolved = await LibraryCache.find_by_title(search_title)
            if resolved:
                item["emby_id"] = resolved.get("emby_id") or resolved.get("Id")

    # ── Resolve series emby_id for episode items (poster fallback) ──
    # ── AND series-level IDs for collapsed show items (detail link) ──
    # Collect episode emby_ids that need series resolution
    needs_series: list[dict] = []
    for _date_str, bucket in day_map.items():
        for item in bucket.values():
            if item.get("item_type") in ("episode", "show") and item.get("emby_id"):
                needs_series.append(item)

    if needs_series:
        try:
            emby = EmbyClient()
            try:
                # Step 1: fetch episode items to get SeriesId
                ep_ids = list({it["emby_id"] for it in needs_series if it["emby_id"]})
                ep_items = await emby.get_items_by_ids(ep_ids, user_id=current_user.emby_user_id) if ep_ids else []
                # Map emby_id → SeriesId
                ep_to_series: dict[str, str] = {}
                for ep in ep_items:
                    eid = ep.get("Id")
                    sid = ep.get("SeriesId")
                    if eid and sid:
                        ep_to_series[eid] = sid

                # Step 2: fetch unique series items to get ProviderIds
                series_ids = list(set(ep_to_series.values()))
                series_items = await emby.get_items_by_ids(series_ids, user_id=current_user.emby_user_id) if series_ids else []
                series_map: dict[str, dict] = {}
                for s in series_items:
                    series_map[s.get("Id")] = s

                # Step 3: assign series IDs to items
                for item in needs_series:
                    series_id = ep_to_series.get(item["emby_id"])
                    if not series_id:
                        continue
                    series_item = series_map.get(series_id)
                    if not series_item:
                        continue
                    item["series_emby_id"] = series_id
                    s_pids = series_item.get("ProviderIds") or {}
                    if item.get("item_type") == "show":
                        item["series_imdb_id"] = s_pids.get("Imdb")
                        item["series_tmdb_id"] = s_pids.get("Tmdb")
                        item["series_tvdb_id"] = s_pids.get("Tvdb")
            finally:
                await emby.close()
        except Exception as e:
            log.warning("wh.series_resolve_failed", error=str(e)[:120])

    # ── All-time rewatch counts ─────────────────────────────────────
    # Collect unique identifiers from visible items to query total watches
    all_items_flat = []
    for _ds, bucket in day_map.items():
        for item in bucket.values():
            all_items_flat.append(item)

    # Movies: count by imdb_id across all time
    movie_imdbs = {it["imdb_id"] for it in all_items_flat if it.get("imdb_id") and it["item_type"] == "movie"}
    movie_counts: dict[str, int] = {}
    if movie_imdbs:
        mc_q = (
            select(WatchHistory.imdb_id, func.count(WatchHistory.id))
            .where(
                WatchHistory.user_id == user_id,
                WatchHistory.item_type == "movie",
                WatchHistory.imdb_id.in_(movie_imdbs),
            )
            .group_by(WatchHistory.imdb_id)
        )
        for row in (await db.execute(mc_q)).all():
            movie_counts[row[0]] = row[1]

    # Episodes: count by imdb_id + season + episode across all time
    ep_keys_set: set[tuple] = set()
    for it in all_items_flat:
        if it["item_type"] == "episode" and it.get("imdb_id") and it.get("season_number") is not None and it.get("episode_number") is not None:
            ep_keys_set.add((it["imdb_id"], it["season_number"], it["episode_number"]))
    ep_counts: dict[tuple, int] = {}
    if ep_keys_set:
        # Build OR conditions for each (imdb, season, episode) triple
        from sqlalchemy import and_, or_, tuple_
        ep_imdbs = {k[0] for k in ep_keys_set}
        ec_q = (
            select(
                WatchHistory.imdb_id,
                WatchHistory.season_number,
                WatchHistory.episode_number,
                func.count(WatchHistory.id),
            )
            .where(
                WatchHistory.user_id == user_id,
                WatchHistory.item_type == "episode",
                WatchHistory.imdb_id.in_(ep_imdbs),
            )
            .group_by(WatchHistory.imdb_id, WatchHistory.season_number, WatchHistory.episode_number)
        )
        for row in (await db.execute(ec_q)).all():
            ep_counts[(row[0], row[1], row[2])] = row[3]

    # Shows (collapsed mode): count distinct watched dates for the series
    show_imdbs = {it["imdb_id"] for it in all_items_flat if it.get("imdb_id") and it["item_type"] == "show"}
    show_counts: dict[str, int] = {}
    if show_imdbs:
        # For shows, count total episode watch events (not distinct dates)
        sc_q = (
            select(WatchHistory.imdb_id, func.count(WatchHistory.id))
            .where(
                WatchHistory.user_id == user_id,
                WatchHistory.item_type == "episode",
                WatchHistory.imdb_id.in_(show_imdbs),
            )
            .group_by(WatchHistory.imdb_id)
        )
        for row in (await db.execute(sc_q)).all():
            show_counts[row[0]] = row[1]

    # Attach total_watches to each item
    for it in all_items_flat:
        tw = 1
        if it["item_type"] == "movie" and it.get("imdb_id"):
            tw = movie_counts.get(it["imdb_id"], 1)
        elif it["item_type"] == "episode" and it.get("imdb_id") and it.get("season_number") is not None and it.get("episode_number") is not None:
            tw = ep_counts.get((it["imdb_id"], it["season_number"], it["episode_number"]), 1)
        elif it["item_type"] == "show" and it.get("imdb_id"):
            tw = show_counts.get(it["imdb_id"], 1)
        it["total_watches"] = tw

    # ── Build response ──────────────────────────────────────────────
    result_days = []
    last_date = None
    for date_str, items_dict in day_map.items():
        day_items = [v for v in items_dict.values() if v.get("title") or v.get("series_name")]
        if day_items:
            result_days.append({"date": date_str, "items": day_items})
        last_date = date_str

    next_before = None
    if last_date and len(day_map) >= days:
        next_before = last_date

    total_q = select(func.count(distinct(cast(WatchHistory.watched_at, Date)))).where(
        WatchHistory.user_id == user_id
    )
    total_days = (await db.execute(total_q)).scalar() or 0

    return {
        "days": result_days,
        "next_before": next_before,
        "total_days": total_days,
        "total_items": total_items,
    }


@router.post("/api/mark-watched")
async def mark_watched(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark an item as fully watched on Emby + scrobble stop to Simkl/MDBList."""
    body = await request.json()
    user_id = body.get("user_id")
    emby_item_id = body.get("emby_item_id")
    imdb_id = body.get("imdb_id")
    tmdb_id = body.get("tmdb_id")
    item_type = body.get("item_type", "movie")
    title = body.get("title", "")
    season_number = body.get("season_number")
    episode_number = body.get("episode_number")
    series_name = body.get("series_name", "")

    if not user_id:
        raise HTTPException(400, "user_id required")

    require_user_ownership(current_user.id, int(user_id), "mark_watched")

    user = await db.get(User, int(user_id))
    if not user:
        raise HTTPException(404, "User not found")

    results = {"emby": False, "simkl": False, "mdblist": False}

    # Build provider IDs dict
    ids = {}
    if imdb_id:
        ids["imdb"] = imdb_id
    if tmdb_id:
        try:
            ids["tmdb"] = int(tmdb_id)
        except (ValueError, TypeError):
            pass

    # ── Mark played on Emby ──
    if emby_item_id and user.emby_user_id:
        try:
            async with EmbyClient() as emby:
                await emby.mark_played(user.emby_user_id, emby_item_id)
                results["emby"] = True
        except Exception as e:
            log.warning("mark_watched.emby_failed", error=str(e)[:120])

    # ── Scrobble stop at 100% to Simkl ──
    if user.simkl_access_token and ids:
        try:
            simkl = SimklClient(access_token=user.simkl_access_token)
            try:
                if item_type == "episode" and season_number is not None and episode_number is not None:
                    payload = {
                        "show": {"ids": ids},
                        "episode": {"season": int(season_number), "number": int(episode_number)},
                    }
                else:
                    payload = {"movie": {"ids": ids}}
                await simkl.scrobble_stop(payload, progress=100)
                results["simkl"] = True
            finally:
                await simkl.close()
        except Exception as e:
            log.warning("mark_watched.simkl_failed", error=str(e)[:120])

    # ── Scrobble stop at 100% to MDBList ──
    if ids:
        try:
            from app.utils.mdblist_client import MDBListClient
            from app.utils.secure_redis import secure_get
            mdb_key = await secure_get("mdblist_api_key")
            if mdb_key:
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    if item_type == "episode" and season_number is not None and episode_number is not None:
                        mdb_payload = {
                            "show": {
                                "ids": ids,
                                "season": {
                                    "number": int(season_number),
                                    "episode": {"number": int(episode_number)},
                                },
                            },
                        }
                    else:
                        mdb_payload = {"movie": {"ids": ids}}
                    await mdb.scrobble_stop(mdb_payload, progress=100)
                    results["mdblist"] = True
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("mark_watched.mdblist_failed", error=str(e)[:120])

    # ── Update local WatchHistory progress to 100 ──
    try:
        from app.models.schema import WatchHistory
        wh_filters = [WatchHistory.user_id == int(user_id)]
        if imdb_id:
            wh_filters.append(WatchHistory.imdb_id == imdb_id)
        elif emby_item_id:
            wh_filters.append(WatchHistory.emby_id == emby_item_id)
        else:
            wh_filters.append(WatchHistory.title == title)
        if item_type == "episode" and season_number is not None and episode_number is not None:
            wh_filters.append(WatchHistory.season_number == int(season_number))
            wh_filters.append(WatchHistory.episode_number == int(episode_number))
        wh_q = (
            select(WatchHistory)
            .where(*wh_filters)
            .order_by(WatchHistory.watched_at.desc())
            .limit(1)
        )
        wh_row = (await db.execute(wh_q)).scalar_one_or_none()
        if wh_row:
            wh_row.progress = 100
            await db.commit()
            results["db"] = True
        else:
            results["db"] = False
    except Exception as e:
        log.warning("mark_watched.db_update_failed", error=str(e)[:120])
        results["db"] = False

    log.info("mark_watched.completed", user=user.emby_username, item=title,
             emby=results["emby"], simkl=results["simkl"], mdblist=results["mdblist"],
             db=results.get("db", False))
    return {"status": "ok", "results": results}


@router.get("/api/watch-history/{user_id}/months")
async def get_watch_history_months(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return distinct year-months that have watch history for jump-to-month."""
    from app.models.schema import WatchHistory
    require_user_ownership(current_user.id, user_id, "watch_history_months")

    q = (
        select(
            func.extract("year", WatchHistory.watched_at).label("y"),
            func.extract("month", WatchHistory.watched_at).label("m"),
        )
        .where(WatchHistory.user_id == user_id, WatchHistory.watched_at.isnot(None))
        .group_by("y", "m")
        .order_by(func.extract("year", WatchHistory.watched_at).desc(),
                  func.extract("month", WatchHistory.watched_at).desc())
    )
    rows = (await db.execute(q)).all()
    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    months = []
    for row in rows:
        y, m = int(row.y), int(row.m)
        months.append({
            "label": f"{month_names[m]} {y}",
            "value": f"{y}-{str(m).zfill(2)}",
        })
    return {"months": months}


@router.get("/api/library/random-backdrop")
async def get_random_backdrop(current_user: User = Depends(get_current_user)):
    """Return a random Emby item ID that has a Backdrop image."""
    import random as _random
    from app.utils.redis_cache import cache_keys, cache_get
    try:
        keys = await cache_keys("library:title:*")
        if not keys:
            return {"emby_id": None}
        sample = _random.sample(keys, min(len(keys), 30))
        for key in sample:
            item = await cache_get(key)
            if item and isinstance(item, dict) and item.get("emby_id"):
                return {"emby_id": item["emby_id"]}
        return {"emby_id": None}
    except Exception:
        return {"emby_id": None}


@router.get("/api/watch-history/{user_id}/item/{item_key:path}")
async def get_item_watch_history(
    user_id: int,
    item_key: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return all watch events for a specific item (rewatch flyout).

    item_key formats: 'emby:xxx', 'imdb:ttxxx', 'simkl:123'
    """
    require_user_ownership(current_user.id, user_id, "watch_history_item")
    _validate_item_key(item_key)
    from app.models.schema import WatchHistory
    from sqlalchemy import or_

    provider, value = item_key.split(":", 1)
    filters = [WatchHistory.user_id == user_id]

    if provider == "emby":
        filters.append(WatchHistory.emby_id == value)
    elif provider == "imdb":
        filters.append(WatchHistory.imdb_id == value)
    elif provider == "tmdb":
        filters.append(WatchHistory.tmdb_id == value)
    elif provider == "simkl":
        filters.append(WatchHistory.simkl_id == value)
    elif provider == "tvdb":
        filters.append(WatchHistory.tvdb_id == value)
    else:
        return {"watches": [], "play_count": 0}

    rows = (await db.execute(
        select(WatchHistory).where(*filters).order_by(WatchHistory.watched_at.desc())
    )).scalars().all()

    return {
        "watches": [
            {"date": r.watched_at.strftime("%Y-%m-%d %H:%M") if r.watched_at else "", "source": r.source}
            for r in rows
        ],
        "play_count": len(rows),
    }


@router.get("/api/watch-history/{user_id}/stats")
async def get_watch_history_stats(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aggregated stats from local watch history — no API calls needed."""
    require_user_ownership(current_user.id, user_id, "watch_history_stats")
    from app.models.schema import WatchHistory
    from sqlalchemy import extract, case, distinct

    base = WatchHistory.user_id == user_id

    # Total counts
    total_watches = (await db.execute(select(func.count(WatchHistory.id)).where(base))).scalar() or 0
    total_movies = (await db.execute(
        select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "movie")
    )).scalar() or 0
    total_episodes = (await db.execute(
        select(func.count(WatchHistory.id)).where(base, WatchHistory.item_type == "episode")
    )).scalar() or 0

    # Total hours watched
    total_minutes = (await db.execute(
        select(func.coalesce(func.sum(WatchHistory.runtime_minutes), 0)).where(base)
    )).scalar() or 0
    total_hours = round(total_minutes / 60, 1)

    # Unique titles (movies) and series
    unique_movies = (await db.execute(
        select(func.count(distinct(WatchHistory.title))).where(base, WatchHistory.item_type == "movie")
    )).scalar() or 0
    unique_series = (await db.execute(
        select(func.count(distinct(WatchHistory.series_name))).where(
            base, WatchHistory.item_type == "episode", WatchHistory.series_name.isnot(None)
        )
    )).scalar() or 0

    # Most rewatched movies (top 10)
    most_rewatched_q = (
        select(
            WatchHistory.title,
            WatchHistory.emby_id,
            WatchHistory.imdb_id,
            func.count(WatchHistory.id).label("plays"),
            func.max(WatchHistory.watched_at).label("last_watched"),
        )
        .where(base, WatchHistory.item_type == "movie")
        .group_by(WatchHistory.title, WatchHistory.emby_id, WatchHistory.imdb_id)
        .having(func.count(WatchHistory.id) > 1)
        .order_by(func.count(WatchHistory.id).desc())
        .limit(10)
    )
    most_rewatched = [
        {"title": r.title, "emby_id": r.emby_id, "imdb_id": r.imdb_id,
         "plays": r.plays, "last_watched": r.last_watched.isoformat() if r.last_watched else None}
        for r in (await db.execute(most_rewatched_q)).all()
    ]

    # Most watched series (by episode count)
    most_watched_series_q = (
        select(
            WatchHistory.series_name,
            func.count(WatchHistory.id).label("episodes_watched"),
            func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("total_minutes"),
        )
        .where(base, WatchHistory.item_type == "episode", WatchHistory.series_name.isnot(None))
        .group_by(WatchHistory.series_name)
        .order_by(func.count(WatchHistory.id).desc())
        .limit(10)
    )
    most_watched_series = [
        {"series_name": r.series_name, "episodes_watched": r.episodes_watched,
         "total_hours": round(r.total_minutes / 60, 1)}
        for r in (await db.execute(most_watched_series_q)).all()
    ]

    # Watches per month (last 12 months)
    twelve_months_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365)
    monthly_q = (
        select(
            extract("year", WatchHistory.watched_at).label("year"),
            extract("month", WatchHistory.watched_at).label("month"),
            func.count(WatchHistory.id).label("count"),
            func.coalesce(func.sum(WatchHistory.runtime_minutes), 0).label("minutes"),
        )
        .where(base, WatchHistory.watched_at >= twelve_months_ago)
        .group_by("year", "month")
        .order_by("year", "month")
    )
    monthly = [
        {"year": int(r.year), "month": int(r.month), "count": r.count,
         "hours": round(r.minutes / 60, 1)}
        for r in (await db.execute(monthly_q)).all()
    ]

    # Day-of-week distribution
    dow_q = (
        select(
            extract("dow", WatchHistory.watched_at).label("dow"),
            func.count(WatchHistory.id).label("count"),
        )
        .where(base)
        .group_by("dow")
        .order_by("dow")
    )
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    dow_raw = {int(r.dow): r.count for r in (await db.execute(dow_q)).all()}
    by_day_of_week = [{"day": day_names[i], "count": dow_raw.get(i, 0)} for i in range(7)]

    # Viewing streak (consecutive days)
    from sqlalchemy import text as sa_text_stats
    streak_q = sa_text_stats(
        "SELECT date_trunc('day', watched_at) AS d "
        "FROM watch_history WHERE user_id = :uid "
        "GROUP BY d ORDER BY d"
    )
    date_rows = (await db.execute(streak_q, {"uid": user_id})).scalars().all()
    current_streak = 0
    max_streak = 0
    if date_rows:
        dates_list = sorted(set(d.date() if hasattr(d, "date") else d for d in date_rows))
        if dates_list:
            streak = 1
            for i in range(1, len(dates_list)):
                if (dates_list[i] - dates_list[i-1]).days == 1:
                    streak += 1
                else:
                    max_streak = max(max_streak, streak)
                    streak = 1
            max_streak = max(max_streak, streak)

            # Current streak
            today = datetime.now(timezone.utc).date()
            if dates_list[-1] >= today - timedelta(days=1):
                current_streak = 1
                for i in range(len(dates_list) - 2, -1, -1):
                    if (dates_list[i+1] - dates_list[i]).days == 1:
                        current_streak += 1
                    else:
                        break

    return {
        "total_watches": total_watches,
        "total_movies": total_movies,
        "total_episodes": total_episodes,
        "total_hours": total_hours,
        "unique_movies": unique_movies,
        "unique_series": unique_series,
        "most_rewatched": most_rewatched,
        "most_watched_series": most_watched_series,
        "monthly": monthly,
        "by_day_of_week": by_day_of_week,
        "current_streak": current_streak,
        "max_streak": max_streak,
    }


@router.post("/api/watch-history/{user_id}/backfill")
@limiter.limit(LIMITS["heavy"])
async def backfill_watch_history(
    request: Request,
    user_id: int,
    _user: User = Depends(get_current_user),
):
    """One-time import of watch history from Simkl, MDBList, and Emby.

    Runs in-request (not background) so the caller sees the result.
    Deduplicates via unique constraint — safe to run multiple times.
    """
    require_user_ownership(_user.id, user_id, "watch_history_backfill")

    # Concurrency guard — only one backfill per user at a time
    r = await get_redis()
    lock_key = f"backfill_lock:{user_id}"
    acquired = await r.set(lock_key, "1", ex=600, nx=True)  # 10-min TTL
    if not acquired:
        raise HTTPException(409, "A backfill is already running for this user. Please wait.")

    from app.models.schema import WatchHistory
    from sqlalchemy import or_, and_
    import structlog
    log = structlog.get_logger()

    async with async_session_ctx() as db:
        user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            await r.delete(lock_key)
            raise HTTPException(404, "User not found")

        # Eagerly capture user fields — a rollback will expire the ORM object
        user_db_id = user.id
        user_emby_user_id = user.emby_user_id
        user_simkl_token = user.simkl_access_token
        user_simkl_expires = user.simkl_token_expires

        # ── Clean up duplicates from prior buggy runs ─────────────────
        # NULL emby_id made the unique constraint ineffective.
        # Keep the oldest row per (user_id, item_type, title, watched_at).
        from sqlalchemy import text as sa_text
        cleanup_q = sa_text("""
            DELETE FROM watch_history
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM watch_history
                WHERE user_id = :uid
                GROUP BY user_id, item_type, COALESCE(title, ''), watched_at
            )
            AND user_id = :uid
        """)
        result = await db.execute(cleanup_q, {"uid": user_id})
        dupes_removed = result.rowcount
        if dupes_removed:
            await db.commit()
            log.info("backfill.duplicates_cleaned", user_id=user_id, removed=dupes_removed)

        added = {"simkl": 0, "mdblist": 0, "emby": 0}
        skipped = {"simkl": 0, "mdblist": 0, "emby": 0}

        # ── 1. Simkl (richest — individual timestamps) ────────────────
        if user_simkl_token:
            try:
                from app.utils.simkl_client import SimklClient
                simkl = SimklClient(
                    access_token=user_simkl_token,
                    token_expires=user_simkl_expires,
                )
                try:
                    # Pre-load existing keys into a set for fast dedup
                    existing_q = select(
                        WatchHistory.item_type, WatchHistory.title, WatchHistory.watched_at
                    ).where(WatchHistory.user_id == user_id)
                    existing_rows = (await db.execute(existing_q)).all()
                    existing_keys = {
                        (r.item_type, r.title or "", r.watched_at)
                        for r in existing_rows
                    }
                    log.debug("backfill.existing_loaded", count=len(existing_keys))

                    for kind in ("movies", "shows"):
                        # get_history returns all items at once (no server-side pagination)
                        history = await simkl.get_history(kind)
                        if not history:
                            log.debug("backfill.simkl_empty", kind=kind)
                            continue

                        log.debug("backfill.simkl_fetched", kind=kind,
                                  items=len(history))

                        kind_added = 0
                        kind_skipped = 0
                        batch = []

                        if kind == "movies":
                            for entry in history:
                                watched_at = entry.get("last_watched_at") or entry.get("watched_at") or ""
                                if not watched_at:
                                    continue
                                try:
                                    dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                    dt_naive = dt.replace(tzinfo=None)
                                except (ValueError, TypeError):
                                    continue

                                item = entry.get("movie") or entry
                                ids = item.get("ids", {})
                                title = item.get("title", "")
                                key = ("movie", title, dt_naive)
                                if key in existing_keys:
                                    kind_skipped += 1
                                    continue
                                existing_keys.add(key)
                                batch.append(WatchHistory(
                                    user_id=user_db_id,
                                    item_type="movie",
                                    title=title,
                                    imdb_id=ids.get("imdb") or None,
                                    tmdb_id=str(ids.get("tmdb")) if ids.get("tmdb") else None,
                                    simkl_id=str(ids.get("simkl")) if ids.get("simkl") else None,
                                    tvdb_id=None,
                                    watched_at=dt_naive,
                                    runtime_minutes=item.get("runtime"),
                                    source="backfill_simkl",
                                ))
                        else:
                            # Shows — extract individual episodes from seasons
                            for entry in history:
                                show = entry.get("show") or entry
                                show_ids = show.get("ids", {})
                                show_title = show.get("title", "")
                                show_watched = entry.get("last_watched_at") or ""

                                seasons = entry.get("seasons", [])
                                if seasons:
                                    # Per-episode timestamps
                                    for season in seasons:
                                        s_num = season.get("number")
                                        for ep in season.get("episodes", []):
                                            ep_watched = ep.get("watched_at") or ep.get("last_watched_at") or show_watched
                                            if not ep_watched:
                                                continue
                                            try:
                                                dt = datetime.fromisoformat(ep_watched.replace("Z", "+00:00"))
                                                dt_naive = dt.replace(tzinfo=None)
                                            except (ValueError, TypeError):
                                                continue

                                            ep_title = ep.get("title") or f"S{s_num or 0:02d}E{ep.get('number', 0):02d}"
                                            ep_ids = ep.get("ids", {})
                                            key = ("episode", ep_title, dt_naive)
                                            if key in existing_keys:
                                                kind_skipped += 1
                                                continue
                                            existing_keys.add(key)
                                            batch.append(WatchHistory(
                                                user_id=user_db_id,
                                                item_type="episode",
                                                title=ep_title,
                                                series_name=show_title,
                                                season_number=s_num,
                                                episode_number=ep.get("number"),
                                                imdb_id=show_ids.get("imdb") or None,
                                                tmdb_id=str(show_ids.get("tmdb")) if show_ids.get("tmdb") else None,
                                                simkl_id=str(ep_ids.get("simkl")) if ep_ids.get("simkl") else None,
                                                tvdb_id=str(show_ids.get("tvdb")) if show_ids.get("tvdb") else None,
                                                watched_at=dt_naive,
                                                runtime_minutes=ep.get("runtime") or show.get("runtime"),
                                                source="backfill_simkl",
                                            ))
                                elif show_watched:
                                    # No season data — single entry for the show
                                    try:
                                        dt = datetime.fromisoformat(show_watched.replace("Z", "+00:00"))
                                        dt_naive = dt.replace(tzinfo=None)
                                    except (ValueError, TypeError):
                                        continue
                                    key = ("show", show_title, dt_naive)
                                    if key in existing_keys:
                                        kind_skipped += 1
                                        continue
                                    existing_keys.add(key)
                                    batch.append(WatchHistory(
                                        user_id=user_db_id,
                                        item_type="show",
                                        title=show_title,
                                        imdb_id=show_ids.get("imdb") or None,
                                        tmdb_id=str(show_ids.get("tmdb")) if show_ids.get("tmdb") else None,
                                        simkl_id=str(show_ids.get("simkl")) if show_ids.get("simkl") else None,
                                        tvdb_id=str(show_ids.get("tvdb")) if show_ids.get("tvdb") else None,
                                        watched_at=dt_naive,
                                        source="backfill_simkl",
                                    ))

                        if batch:
                            db.add_all(batch)
                            await db.commit()
                            kind_added += len(batch)

                        added["simkl"] += kind_added
                        skipped["simkl"] += kind_skipped
                        log.info("backfill.simkl_kind_done", kind=kind,
                                 added=kind_added, skipped=kind_skipped)
                finally:
                    await simkl.close()
            except Exception as e:
                log.warning("backfill.simkl_failed", error=str(e)[:200])
                await db.rollback()

        # ── 2. MDBList (last watched date + plays count) ──────────────
        # Re-load existing keys (Simkl may have added new ones)
        existing_rows = (await db.execute(
            select(WatchHistory.item_type, WatchHistory.title, WatchHistory.watched_at)
            .where(WatchHistory.user_id == user_id)
        )).all()
        existing_keys = {(r.item_type, r.title or "", r.watched_at) for r in existing_rows}

        try:
            r = await get_redis()
            raw_key = await secure_get("mdblist_api_key")
            if raw_key:
                from app.utils.mdblist_client import MDBListClient
                key = raw_key if isinstance(raw_key, str) else raw_key.decode()
                mdb = MDBListClient(api_key=key)
                try:
                    watched_data = await mdb.get_watched()
                    batch = []
                    for kind, wh_type in (("movies", "movie"), ("shows", "show")):
                        for entry in watched_data.get(kind, []):
                            watched_at = entry.get("watched_at") or entry.get("last_watched_at", "")
                            if not watched_at:
                                continue
                            try:
                                dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
                                dt_naive = dt.replace(tzinfo=None)
                            except (ValueError, TypeError):
                                continue

                            title = entry.get("title", "")
                            it = wh_type if wh_type == "movie" else "episode"
                            k = (it, title, dt_naive)
                            if k in existing_keys:
                                skipped["mdblist"] += 1
                                continue
                            existing_keys.add(k)

                            ids = entry.get("ids", {})
                            batch.append(WatchHistory(
                                user_id=user_db_id,
                                item_type=it,
                                title=title,
                                imdb_id=ids.get("imdb") or None,
                                tmdb_id=str(ids.get("tmdb")) if ids.get("tmdb") else None,
                                simkl_id=str(ids.get("simkl")) if ids.get("simkl") else None,
                                tvdb_id=str(ids.get("tvdb")) if ids.get("tvdb") else None,
                                watched_at=dt_naive,
                                source="backfill_mdblist",
                            ))
                    if batch:
                        mdb_added = 0
                        for item in batch:
                            try:
                                db.add(item)
                                await db.flush()
                                mdb_added += 1
                            except Exception:
                                await db.rollback()
                        if mdb_added:
                            await db.commit()
                        added["mdblist"] = mdb_added
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("backfill.mdblist_failed", error=str(e)[:200])
            await db.rollback()

        # ── 3. Emby (LastPlayedDate only, one date per item) ──────────
        if user_emby_user_id:
            try:
                emby = EmbyClient()
                try:
                    for emby_type in ("Movie", "Episode"):
                        start = 0
                        page_size = 500
                        while True:
                            resp = await emby.get_items(
                                user_id=user_emby_user_id,
                                item_type=emby_type,
                                filters="IsPlayed",
                                fields="ProviderIds,UserData,UserDataLastPlayedDate,RunTimeTicks,SeriesName,ParentIndexNumber,IndexNumber",
                                limit=page_size,
                                start_index=start,
                            )
                            items = resp.get("Items", []) if isinstance(resp, dict) else resp
                            if not items:
                                break

                            batch = []
                            for item in items:
                                ud = item.get("UserData", {})
                                last_played = ud.get("LastPlayedDate", "")
                                if not last_played:
                                    continue
                                try:
                                    dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                                    dt_naive = dt.replace(tzinfo=None)
                                except (ValueError, TypeError):
                                    continue

                                title = item.get("Name", "")
                                it = "episode" if emby_type == "Episode" else "movie"
                                k = (it, title, dt_naive)
                                if k in existing_keys:
                                    skipped["emby"] += 1
                                    continue
                                existing_keys.add(k)

                                pids = item.get("ProviderIds", {})
                                runtime_ticks = item.get("RunTimeTicks", 0) or 0
                                runtime_min = int(runtime_ticks / 600_000_000) if runtime_ticks else None

                                batch.append(WatchHistory(
                                    user_id=user_db_id,
                                    emby_id=item.get("Id"),
                                    item_type=it,
                                    title=title,
                                    series_name=item.get("SeriesName") if emby_type == "Episode" else None,
                                    season_number=item.get("ParentIndexNumber") if emby_type == "Episode" else None,
                                    episode_number=item.get("IndexNumber") if emby_type == "Episode" else None,
                                    imdb_id=pids.get("Imdb") or None,
                                    tmdb_id=str(pids.get("Tmdb")) if pids.get("Tmdb") else None,
                                    tvdb_id=str(pids.get("Tvdb")) if pids.get("Tvdb") else None,
                                    watched_at=dt_naive,
                                    runtime_minutes=runtime_min,
                                    source="backfill_emby",
                                ))

                            if batch:
                                db.add_all(batch)
                                await db.commit()
                                added["emby"] += len(batch)

                            if len(items) < page_size:
                                break
                            start += page_size
                finally:
                    await emby.close()
            except Exception as e:
                log.warning("backfill.emby_failed", error=str(e)[:200])
                await db.rollback()

    total_added = sum(added.values())
    total_skipped = sum(skipped.values())
    log.info("backfill.complete", user_id=user_id, added=added, skipped=skipped,
             duplicates_cleaned=dupes_removed)

    # Invalidate stats cache so new data shows immediately
    try:
        r = await get_redis()
        await r.delete(f"watch_stats_v5:{user_id}")
    except Exception:
        pass

    # Release concurrency lock
    await r.delete(lock_key)

    return {
        "status": "ok",
        "added": added,
        "skipped_duplicates": skipped,
        "duplicates_cleaned": dupes_removed,
        "total_added": total_added,
        "total_skipped": total_skipped,
    }


@router.post("/api/watch-history/{user_id}/backfill-genres")
async def backfill_watch_history_genres(
    user_id: int,
    _user: User = Depends(get_current_user),
):
    """Populate the genres column for existing watch_history rows from Emby.

    Queries Emby for each unique emby_id that has no genres set,
    then batch-updates the rows.  Safe to run multiple times.
    """
    require_user_ownership(_user.id, user_id, "watch_history_genres_backfill")
    import structlog
    log = structlog.get_logger()
    from app.models.schema import WatchHistory

    async with async_session_ctx() as db:
        user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user or not user.emby_user_id:
            raise HTTPException(404, "User not found or no Emby user linked")

        # Find rows missing genres
        from sqlalchemy import or_
        missing_q = (
            select(distinct(WatchHistory.emby_id))
            .where(
                WatchHistory.user_id == user_id,
                WatchHistory.emby_id.isnot(None),
                or_(WatchHistory.genres.is_(None), WatchHistory.genres == ""),
            )
        )
        missing_ids = [r for r in (await db.execute(missing_q)).scalars().all() if r]

        if not missing_ids:
            return {"status": "ok", "updated": 0, "total_updated": 0, "message": "All rows already have genres"}

        log.info("genre_backfill.start", user_id=user_id, missing_items=len(missing_ids))

        emby = EmbyClient()
        updated = 0
        try:
            # Batch fetch from Emby in chunks of 50
            for i in range(0, len(missing_ids), 50):
                chunk = missing_ids[i:i + 50]
                try:
                    items = await emby.get_items_by_ids(
                        item_ids=chunk,
                        user_id=user.emby_user_id,
                    )
                except Exception as e:
                    log.warning("genre_backfill.emby_batch_failed", error=str(e)[:120])
                    continue

                for item in items:
                    emby_id = item.get("Id")
                    genres_list = item.get("Genres", [])
                    if not emby_id or not genres_list:
                        continue
                    genres_str = ",".join(genres_list)

                    from sqlalchemy import update as sa_update
                    await db.execute(
                        sa_update(WatchHistory)
                        .where(
                            WatchHistory.user_id == user_id,
                            WatchHistory.emby_id == emby_id,
                            or_(WatchHistory.genres.is_(None), WatchHistory.genres == ""),
                        )
                        .values(genres=genres_str)
                    )
                    updated += 1

                await db.commit()
        finally:
            await emby.close()

        # Also try to fill rows without emby_id using library cache title match
        no_emby_q = (
            select(distinct(WatchHistory.title))
            .where(
                WatchHistory.user_id == user_id,
                WatchHistory.emby_id.is_(None),
                or_(WatchHistory.genres.is_(None), WatchHistory.genres == ""),
                WatchHistory.title.isnot(None),
            )
        )
        no_emby_titles = [r for r in (await db.execute(no_emby_q)).scalars().all() if r]
        title_updated = 0

        if no_emby_titles:
            emby2 = EmbyClient()
            try:
                for title in no_emby_titles[:200]:  # cap to avoid hammering
                    try:
                        results = await emby2.search_items(
                            term=title,
                            item_type="Movie",
                        )
                        if results:
                            genres_list = results[0].get("Genres", [])
                            if genres_list:
                                genres_str = ",".join(genres_list)
                                from sqlalchemy import update as sa_update
                                await db.execute(
                                    sa_update(WatchHistory)
                                    .where(
                                        WatchHistory.user_id == user_id,
                                        WatchHistory.title == title,
                                        or_(WatchHistory.genres.is_(None), WatchHistory.genres == ""),
                                    )
                                    .values(genres=genres_str)
                                )
                                title_updated += 1
                    except Exception:
                        continue
                await db.commit()
            finally:
                await emby2.close()

        # Invalidate stats cache
        try:
            r = await get_redis()
            await r.delete(f"watch_stats_v5:{user_id}")
        except Exception:
            pass

        log.info("genre_backfill.complete", user_id=user_id,
                 by_emby_id=updated, by_title=title_updated)
        return {
            "status": "ok",
            "updated_by_emby_id": updated,
            "updated_by_title": title_updated,
            "total_updated": updated + title_updated,
        }


# ═══════════════════════════════════════════════════════════════════════════

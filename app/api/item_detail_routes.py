"""Routes extracted from routes.py — item_detail_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user
from app.api.route_helpers import _get_active_providers, _get_mdblist_key

log = structlog.get_logger()

router = APIRouter()



@router.get("/item/{imdb_id}")
async def item_detail_page(imdb_id: str):
    """Render the item detail HTML page."""
    # Allow '_' as a placeholder when the real lookup is by tmdb_id/emby_id query param.
    # For actual IMDB IDs, validate the tt+digits pattern to prevent XSS.
    if imdb_id != "_" and not re.fullmatch(r"tt\d{7,10}", imdb_id):
        return HTMLResponse("<h1>Invalid item ID</h1>", status_code=400)
    try:
        with open("frontend/templates/item_detail.html", "r") as f:
            html = f.read()
        html = html.replace("{{ imdb_id }}", imdb_id)
        return HTMLResponse(html)
    except FileNotFoundError:
        return HTMLResponse("<h1>Page not found</h1>", status_code=404)


@router.get("/api/item/detail")
async def get_item_detail(
    imdb_id: str | None = None,
    tmdb_id: str | None = None,
    emby_id: str | None = None,
    media_type: str = "movie",
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aggregate item detail from Emby, MDBList, TMDB, and local DB."""
    import asyncio
    from app.utils.tmdb_client import get_full_details as tmdb_full_details
    from app.utils.tmdb_client import get_watch_providers as tmdb_providers

    if not imdb_id and not tmdb_id and not emby_id:
        raise HTTPException(400, "At least one of imdb_id, tmdb_id, or emby_id required")

    # Normalize empty strings to None
    imdb_id = imdb_id or None
    tmdb_id = tmdb_id or None
    emby_id = emby_id or None

    user_id = current_user.id
    result: dict = {"imdb_id": imdb_id, "tmdb_id": tmdb_id, "emby_id": emby_id, "media_type": media_type}

    # ── Resolve IDs from library cache if we only have one ──
    if emby_id and not imdb_id:
        cached = await LibraryCache.find_by_provider_id("Emby", emby_id)
        if not cached:
            # Try direct lookup by emby_id as the cache key
            from app.utils.redis_cache import cache_get
            cached = await cache_get(f"library:id:{emby_id}")
        if cached:
            imdb_id = imdb_id or (cached.get("provider_ids") or {}).get("Imdb")
            tmdb_id = tmdb_id or (cached.get("provider_ids") or {}).get("Tmdb")
            result["imdb_id"] = imdb_id
            result["tmdb_id"] = tmdb_id

    if imdb_id and not emby_id:
        cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
        if cached:
            emby_id = cached.get("emby_id") or cached.get("Id")
            tmdb_id = tmdb_id or (cached.get("provider_ids") or {}).get("Tmdb")
            result["emby_id"] = emby_id
            result["tmdb_id"] = tmdb_id

    # ── Parallel fetch from all sources ──
    emby_user_guid = current_user.emby_user_id  # Emby needs the GUID, not DB integer id

    async def fetch_emby():
        if not emby_id:
            return None
        try:
            emby = EmbyClient()
            item = await emby.get_item(emby_id, user_id=emby_user_guid)
            await emby.close()
            return item
        except Exception as e:
            log.debug("item_detail.emby_failed", error=str(e)[:120])
            return None

    async def fetch_mdblist():
        if not imdb_id:
            return None
        try:
            mdb_key = await _get_mdblist_key(db)
            if not mdb_key:
                return None
            from app.utils.mdblist_client import MDBListClient
            mdb = MDBListClient(api_key=mdb_key)
            mdb_type = "movie" if media_type == "movie" else "show"
            info = await mdb.get_media_info("imdb", mdb_type, imdb_id)
            await mdb.close()
            return info
        except Exception as e:
            log.debug("item_detail.mdblist_failed", error=str(e)[:120])
            return None

    async def fetch_tmdb():
        tid = tmdb_id or None
        if not tid and imdb_id:
            # Try to resolve tmdb_id from imdb_id via library cache
            cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
            if cached:
                tid = (cached.get("provider_ids") or {}).get("Tmdb")
        if not tid:
            return None
        try:
            tmdb_type = "movie" if media_type == "movie" else "tv"
            return await tmdb_full_details(int(tid), media_type=tmdb_type)
        except Exception as e:
            log.debug("item_detail.tmdb_failed", error=str(e)[:120])
            return None

    async def fetch_tmdb_providers():
        tid = tmdb_id or None
        if not tid and imdb_id:
            cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
            if cached:
                tid = (cached.get("provider_ids") or {}).get("Tmdb")
        if not tid:
            return []
        try:
            tmdb_type = "movie" if media_type == "movie" else "tv"
            return await tmdb_providers(int(tid), media_type=tmdb_type, country="GB")
        except Exception:
            return []

    async def fetch_user_rating():
        if not imdb_id:
            return None
        try:
            from app.models.schema import UserRating
            q = select(UserRating).where(
                UserRating.user_id == user_id,
                UserRating.imdb_id == imdb_id,
            ).order_by(UserRating.rated_at.desc()).limit(1)
            row = (await db.execute(q)).scalar_one_or_none()
            return {"rating": row.rating, "source": row.source, "rated_at": str(row.rated_at)} if row else None
        except Exception:
            return None

    async def fetch_watch_history():
        if not imdb_id:
            return []
        try:
            from app.models.schema import WatchHistory
            q = (
                select(WatchHistory)
                .where(WatchHistory.user_id == user_id, WatchHistory.imdb_id == imdb_id)
                .order_by(WatchHistory.watched_at.desc())
                .limit(20)
            )
            rows = (await db.execute(q)).scalars().all()
            return [{"watched_at": str(r.watched_at), "progress": r.progress} for r in rows]
        except Exception:
            return []

    emby_data, mdb_data, tmdb_data, providers, user_rating, history = await asyncio.gather(
        fetch_emby(), fetch_mdblist(), fetch_tmdb(),
        fetch_tmdb_providers(), fetch_user_rating(), fetch_watch_history(),
    )

    # ── Second pass: resolve imdb_id from TMDB when MDBList missed ──
    if not mdb_data and not imdb_id and tmdb_data:
        resolved_imdb = tmdb_data.get("imdb_id")
        if not resolved_imdb and media_type != "movie" and tmdb_id:
            # TV shows: use external_ids endpoint
            from app.utils.tmdb_client import get_tv_external_ids
            ext = await get_tv_external_ids(int(tmdb_id))
            if ext:
                resolved_imdb = ext.get("imdb_id")
        if resolved_imdb:
            imdb_id = resolved_imdb
            result["imdb_id"] = imdb_id
            # Re-fetch MDBList + user rating now that we have imdb_id
            mdb_data, user_rating = await asyncio.gather(
                fetch_mdblist(), fetch_user_rating(),
            )

    # ── Merge into unified response ──

    # Emby data
    if emby_data:
        result["title"] = emby_data.get("Name")
        result["overview"] = emby_data.get("Overview")
        result["year"] = emby_data.get("ProductionYear")
        result["genres"] = emby_data.get("Genres", [])
        result["certification"] = emby_data.get("OfficialRating")
        result["community_rating"] = emby_data.get("CommunityRating")
        result["taglines"] = emby_data.get("Taglines", [])
        result["studios"] = [s.get("Name") for s in (emby_data.get("Studios") or [])]
        runtime_ticks = emby_data.get("RunTimeTicks")
        result["runtime_minutes"] = int(runtime_ticks / 600_000_000) if runtime_ticks else None
        # People from Emby
        people = emby_data.get("People", [])
        result["emby_cast"] = [
            {"name": p.get("Name"), "role": p.get("Role"), "type": p.get("Type"), "emby_id": p.get("Id")}
            for p in people
        ]
        # Provider IDs
        pids = emby_data.get("ProviderIds") or {}
        result["imdb_id"] = result.get("imdb_id") or pids.get("Imdb")
        result["tmdb_id"] = result.get("tmdb_id") or pids.get("Tmdb")
        result["tvdb_id"] = pids.get("Tvdb")
        # UserData (played status, play count)
        ud = emby_data.get("UserData") or {}
        result["is_played"] = ud.get("Played", False)
        result["emby_play_count"] = ud.get("PlayCount", 0)
        result["in_library"] = True
    else:
        result["in_library"] = False

    # TMDB data — richer cast with photos, budget, revenue
    if tmdb_data:
        result["title"] = result.get("title") or tmdb_data.get("title")
        result["overview"] = result.get("overview") or tmdb_data.get("overview")
        result["tagline"] = tmdb_data.get("tagline")
        result["release_date"] = tmdb_data.get("release_date")
        result["runtime_minutes"] = result.get("runtime_minutes") or tmdb_data.get("runtime")
        result["budget"] = tmdb_data.get("budget")
        result["revenue"] = tmdb_data.get("revenue")
        result["status"] = tmdb_data.get("status")
        result["genres"] = result.get("genres") or tmdb_data.get("genres", [])
        result["poster_path"] = tmdb_data.get("poster_path")
        result["backdrop_path"] = tmdb_data.get("backdrop_path")
        result["production_companies"] = tmdb_data.get("production_companies", [])
        result["production_countries"] = tmdb_data.get("production_countries", [])
        result["spoken_languages"] = tmdb_data.get("spoken_languages", [])
        result["keywords"] = tmdb_data.get("keywords", [])
        result["tmdb_cast"] = tmdb_data.get("cast", [])
        result["tmdb_crew"] = tmdb_data.get("crew", [])
        result["tmdb_vote_average"] = tmdb_data.get("vote_average")
        result["tmdb_vote_count"] = tmdb_data.get("vote_count")
        result["number_of_seasons"] = tmdb_data.get("number_of_seasons")
        result["number_of_episodes"] = tmdb_data.get("number_of_episodes")
        result["networks"] = tmdb_data.get("networks", [])
        result["belongs_to_collection"] = tmdb_data.get("belongs_to_collection")

    # MDBList ratings
    if mdb_data:
        result["title"] = result.get("title") or mdb_data.get("title")
        result["overview"] = result.get("overview") or mdb_data.get("description")
        result["year"] = result.get("year") or mdb_data.get("year")
        # Extract all rating sources
        ratings = {}
        for r_item in (mdb_data.get("ratings") or []):
            src = r_item.get("source")
            if src:
                ratings[src.lower()] = {
                    "value": r_item.get("value"),
                    "score": r_item.get("score"),
                    "votes": r_item.get("votes") or r_item.get("vote_count"),
                }
        # Also check top-level score fields
        if mdb_data.get("score"):
            ratings["mdblist"] = {"value": mdb_data["score"], "votes": mdb_data.get("score_average_count")}
        if mdb_data.get("imdbrating"):
            ratings.setdefault("imdb", {})["value"] = mdb_data["imdbrating"]
            ratings["imdb"]["votes"] = mdb_data.get("imdbvotes")
        if mdb_data.get("traktrating"):
            ratings.setdefault("trakt", {})["value"] = mdb_data["traktrating"]
            ratings["trakt"]["votes"] = mdb_data.get("traktvotes")
        if mdb_data.get("tmdbrating"):
            ratings.setdefault("tmdb", {})["value"] = mdb_data["tmdbrating"]
            ratings["tmdb"]["votes"] = mdb_data.get("tmdbvotes")
        if mdb_data.get("letterboxdrating"):
            ratings.setdefault("letterboxd", {})["value"] = mdb_data["letterboxdrating"]
            ratings["letterboxd"]["votes"] = mdb_data.get("letterboxdvotes")
        if mdb_data.get("tomatoesrating"):
            ratings.setdefault("tomatoes", {})["value"] = mdb_data["tomatoesrating"]
            ratings["tomatoes"]["votes"] = mdb_data.get("tomatoes_audience_count") or mdb_data.get("tomatoesvotes")
        if mdb_data.get("tomatoesaudience"):
            ratings["popcorn"] = {"value": mdb_data["tomatoesaudience"], "votes": mdb_data.get("tomatoes_audience_count")}
        if mdb_data.get("metacritic"):
            ratings.setdefault("metacritic", {})["value"] = mdb_data["metacritic"]
            ratings["metacritic"]["votes"] = mdb_data.get("metacriticvotes")
        result["ratings"] = ratings
        result["mdb_certification"] = mdb_data.get("certification")
        result["trailer"] = mdb_data.get("trailer")

    # Watch providers
    result["watch_providers"] = providers or []

    # TMDB recommendations — enrich with library status
    recs_raw = (tmdb_data or {}).get("recommendations", [])
    recs_out: list[dict] = []
    for rec in recs_raw:
        rec_tmdb = rec.get("id")
        rec_in_lib = False
        rec_imdb = None
        rec_emby = None
        if rec_tmdb:
            cached_rec = await LibraryCache.find_by_provider_id("Tmdb", str(rec_tmdb))
            if cached_rec:
                rec_in_lib = True
                rec_imdb = (cached_rec.get("provider_ids") or {}).get("Imdb")
                rec_emby = cached_rec.get("emby_id") or cached_rec.get("Id")
        rec["in_library"] = rec_in_lib
        rec["imdb_id"] = rec_imdb
        rec["emby_id"] = rec_emby
        recs_out.append(rec)
    result["recommendations"] = recs_out

    # Watchlist status — local DB lookup (synced from providers)
    result["on_watchlist"] = False
    if imdb_id or tmdb_id:
        from app.models.schema import WatchlistItem
        from sqlalchemy import or_ as _or
        _wl_conds = []
        if imdb_id:
            _wl_conds.append(WatchlistItem.imdb_id == imdb_id)
        if tmdb_id:
            _wl_conds.append(WatchlistItem.tmdb_id == str(tmdb_id))
        _wl_row = (await db.execute(
            select(WatchlistItem.id).where(
                WatchlistItem.user_id == user_id,
                _or(*_wl_conds),
            ).limit(1)
        )).first()
        result["on_watchlist"] = _wl_row is not None

    # User data
    result["user_id"] = user_id
    result["user_rating"] = user_rating
    result["watch_history"] = history

    return result


# ═══════════════════════════════════════════════════════════════════════


@router.get("/api/item/episodes")
async def get_item_episodes(
    emby_id: str | None = None,
    imdb_id: str | None = None,
    current_user: User = Depends(get_current_user),
):
    """Return seasons and episodes for a series with watched status."""
    import asyncio

    # Resolve emby_id from imdb_id if needed
    if not emby_id and imdb_id:
        cached = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
        if cached:
            emby_id = cached.get("emby_id") or cached.get("Id")
    if not emby_id:
        return {"seasons": [], "error": "Item not in library"}

    emby_user_guid = current_user.emby_user_id
    emby = EmbyClient()
    try:
        # Fetch seasons
        seasons_resp = await emby.get_items(
            user_id=emby_user_guid,
            item_type="Season",
            parent_id=emby_id,
            fields="UserData",
            recursive=False,
            sort_by="SortName",
        )
        seasons_raw = seasons_resp.get("Items", [])

        # Fetch episodes for all seasons in parallel
        async def _get_eps(season: dict) -> dict:
            sid = season.get("Id")
            eps_resp = await emby.get_items(
                user_id=emby_user_guid,
                item_type="Episode",
                parent_id=sid,
                fields="UserData,RunTimeTicks,Overview",
                recursive=False,
                sort_by="SortName",
            )
            eps = []
            for ep in eps_resp.get("Items", []):
                ud = ep.get("UserData", {})
                runtime_ticks = ep.get("RunTimeTicks")
                eps.append({
                    "emby_id": ep.get("Id"),
                    "name": ep.get("Name"),
                    "season_number": ep.get("ParentIndexNumber"),
                    "episode_number": ep.get("IndexNumber"),
                    "overview": (ep.get("Overview") or "")[:200],
                    "runtime_minutes": int(runtime_ticks / 600_000_000) if runtime_ticks else None,
                    "played": ud.get("Played", False),
                    "play_count": ud.get("PlayCount", 0),
                })
            s_ud = season.get("UserData", {})
            return {
                "season_number": season.get("IndexNumber"),
                "name": season.get("Name", ""),
                "emby_id": sid,
                "episode_count": len(eps),
                "played_count": sum(1 for e in eps if e["played"]),
                "episodes": eps,
            }

        results = await asyncio.gather(*[_get_eps(s) for s in seasons_raw])
        # Sort by season number, filter out Specials (season 0) at the end
        results = sorted(results, key=lambda s: (s["season_number"] or 999))
        return {"seasons": results}
    except Exception as e:
        log.warning("item_detail.episodes_failed", error=str(e)[:120])
        return {"seasons": [], "error": str(e)[:120]}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════


@router.post("/api/watchlist/toggle")
async def toggle_watchlist(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Add or remove an item from the user's watchlist on active providers."""
    imdb_id = payload.get("imdb_id")
    tmdb_id = payload.get("tmdb_id")
    item_type = payload.get("item_type", "movie")  # "movie" or "show"
    action = payload.get("action", "add")  # "add" or "remove"

    if not imdb_id and not tmdb_id:
        raise HTTPException(400, "imdb_id or tmdb_id required")

    ids_dict = {}
    if imdb_id:
        ids_dict["imdb"] = imdb_id
    if tmdb_id:
        ids_dict["tmdb"] = tmdb_id

    results: dict = {"action": action, "simkl": None, "mdblist": None}
    providers = await _get_active_providers(db)

    # Simkl
    if "simkl" in providers and current_user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=current_user.simkl_access_token,
                token_expires=current_user.simkl_token_expires,
            )
            try:
                item_payload = [{"ids": ids_dict}]
                if action == "add":
                    r = await simkl.add_to_watchlist(items=item_payload)
                else:
                    r = await simkl.remove_from_watchlist(item_payload)
                results["simkl"] = "ok"
            finally:
                await simkl.close()
        except Exception as e:
            results["simkl"] = str(e)[:100]

    # MDBList
    if "mdblist" in providers:
        try:
            mdb_key = await _get_mdblist_key(db)
            if mdb_key:
                from app.utils.mdblist_client import MDBListClient
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    mdb_item = {}
                    if imdb_id:
                        mdb_item["imdb"] = imdb_id
                    if tmdb_id:
                        mdb_item["tmdb"] = tmdb_id
                    if action == "add":
                        if item_type == "show":
                            await mdb.add_to_watchlist(shows=[mdb_item])
                        else:
                            await mdb.add_to_watchlist(movies=[mdb_item])
                    else:
                        if item_type == "show":
                            await mdb.remove_from_watchlist(shows=[mdb_item])
                            # Mirror "dropped" status to MDBList for shows
                            try:
                                await mdb.add_dropped(shows=[mdb_item])
                            except Exception:
                                pass
                        else:
                            await mdb.remove_from_watchlist(movies=[mdb_item])
                    results["mdblist"] = "ok"
                finally:
                    await mdb.close()
        except Exception as e:
            results["mdblist"] = str(e)[:100]

    # Persist locally
    from app.models.schema import WatchlistItem
    _now = datetime.now(timezone.utc).replace(tzinfo=None)
    if action == "add":
        existing = (await db.execute(
            select(WatchlistItem).where(
                WatchlistItem.user_id == current_user.id,
                WatchlistItem.imdb_id == imdb_id,
            )
        )).scalar_one_or_none()
        if not existing:
            db.add(WatchlistItem(
                user_id=current_user.id,
                imdb_id=imdb_id,
                tmdb_id=str(tmdb_id) if tmdb_id else None,
                title=payload.get("title"),
                item_type=item_type,
                source="user",
                added_at=_now,
                synced_at=_now,
            ))
            await db.commit()
    else:
        await db.execute(
            WatchlistItem.__table__.delete().where(
                WatchlistItem.user_id == current_user.id,
                WatchlistItem.imdb_id == imdb_id,
            )
        )
        await db.commit()

    return results


# ═══════════════════════════════════════════════════════════════════════════

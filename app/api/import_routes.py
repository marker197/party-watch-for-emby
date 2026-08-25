"""Routes extracted from routes.py — import_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.utils.redis_cache import get_redis
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user, require_user_ownership
from app.api.route_helpers import _activity_log, _get_active_providers, _get_mdblist_key

log = structlog.get_logger()

router = APIRouter()



@router.post("/api/import/trakt/parse")
async def trakt_import_parse(
    request: Request,
    _user: User = Depends(get_current_user),
):
    """Accept a Trakt export zip, parse it, cache parsed data, return summary."""
    import zipfile, io, json as _json, uuid

    form = await request.form()
    upload = form.get("file")
    if not upload:
        return JSONResponse({"error": "No file uploaded"}, status_code=400)

    raw = await upload.read()
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except Exception:
        return JSONResponse({"error": "Invalid zip file"}, status_code=400)

    names = zf.namelist()

    def _load_multi(prefix: str) -> list:
        """Load paginated files like ratings-movies-1.json, ratings-movies-2.json, ..."""
        items = []
        # Try exact name first (e.g. ratings-shows.json)
        if f"{prefix}.json" in names:
            try:
                data = _json.loads(zf.read(f"{prefix}.json"))
                if isinstance(data, list):
                    items.extend(data)
            except Exception:
                pass
        # Then paginated files
        for i in range(1, 100):
            fname = f"{prefix}-{i}.json"
            if fname not in names:
                break
            try:
                data = _json.loads(zf.read(fname))
                if isinstance(data, list):
                    items.extend(data)
            except Exception:
                pass
        return items

    # Parse all importable data
    parsed = {
        "ratings_movies": _load_multi("ratings-movies"),
        "ratings_shows": _load_multi("ratings-shows"),
        "ratings_episodes": _load_multi("ratings-episodes"),
        "watched_movies": _load_multi("watched-movies"),
        "watched_shows": _load_multi("watched-shows"),
        "watched_history": _load_multi("watched-history"),
        "watchlist": _load_multi("lists-watchlist"),
    }

    # Build summary
    summary = {}
    for key, items in parsed.items():
        summary[key] = len(items)

    # Cache parsed data in Redis (15 min TTL) so push doesn't re-parse
    import_id = uuid.uuid4().hex[:12]
    try:
        r = await get_redis()
        await r.set(
            f"trakt_import:{import_id}",
            _json.dumps(parsed),
            ex=900,
        )
    except Exception as e:
        return JSONResponse({"error": f"Cache failed: {str(e)[:100]}"}, status_code=500)

    return {
        "import_id": import_id,
        "summary": summary,
        "total_items": sum(summary.values()),
    }


@router.post("/api/import/trakt/push")
async def trakt_import_push(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Push parsed Trakt data to local DB + Simkl + MDBList.
    Expects JSON: {import_id, push_ratings, push_watched, push_watchlist, user_id}
    """
    import json as _json

    body = await request.json()
    import_id = body.get("import_id")
    user_id = body.get("user_id") or current_user.id
    push_ratings = body.get("push_ratings", True)
    push_watched = body.get("push_watched", True)
    push_watchlist = body.get("push_watchlist", True)

    require_user_ownership(current_user.id, user_id, "trakt_import_push")

    # Load cached parsed data
    r = await get_redis()
    raw = await r.get(f"trakt_import:{import_id}")
    if not raw:
        return JSONResponse({"error": "Import expired — please re-upload the zip"}, status_code=410)

    parsed = _json.loads(raw)
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return JSONResponse({"error": "User not found"}, status_code=404)

    # Get active providers
    providers = await _get_active_providers()
    has_simkl = "simkl" in providers and user.simkl_access_token
    has_mdblist = "mdblist" in providers and bool(await _get_mdblist_key())

    results = {"ratings": {}, "watched": {}, "watchlist": {}, "errors": []}
    BATCH = 50  # items per API call

    # Helper: extract IDs from Trakt item
    def _ids(item: dict, media_key: str = "") -> dict:
        """Extract imdb/tmdb/tvdb from nested Trakt item."""
        obj = item.get(media_key, item) if media_key else item
        raw_ids = obj.get("ids", {})
        out = {}
        if raw_ids.get("imdb"):
            out["imdb"] = raw_ids["imdb"]
        if raw_ids.get("tmdb"):
            out["tmdb"] = int(raw_ids["tmdb"])
        if raw_ids.get("tvdb"):
            out["tvdb"] = int(raw_ids["tvdb"])
        return out

    # Helper: safe title
    def _title(item: dict, media_key: str = "") -> str:
        obj = item.get(media_key, item) if media_key else item
        return obj.get("title", "Unknown")

    # Helper: emit import progress via Socket.IO + activity log
    async def _progress(msg: str, phase: str = "", pct: int = 0):
        try:
            from app.services.watch_party.service import sio
            await sio.emit("import_progress", {"msg": msg, "phase": phase, "pct": pct})
        except Exception:
            pass
        await _activity_log(msg, category="general")

    # ── 1. RATINGS ──────────────────────────────────────────────────────
    if push_ratings:
        await _progress("Processing ratings…", "ratings", 10)
        all_ratings = []
        for item in parsed.get("ratings_movies", []):
            ids = _ids(item, "movie")
            if ids:
                all_ratings.append({
                    "ids": ids, "rating": item.get("rating"),
                    "rated_at": item.get("rated_at", ""),
                    "_type": "movies", "title": _title(item, "movie"),
                    "year": item.get("movie", {}).get("year"),
                    "item_type": "movie",
                })
        for item in parsed.get("ratings_shows", []):
            ids = _ids(item, "show")
            if ids:
                all_ratings.append({
                    "ids": ids, "rating": item.get("rating"),
                    "rated_at": item.get("rated_at", ""),
                    "_type": "shows", "title": _title(item, "show"),
                    "year": item.get("show", {}).get("year"),
                    "item_type": "show",
                })

        # Store in local DB
        from app.models.schema import UserRating
        db_count = 0
        for r_item in all_ratings:
            imdb = r_item["ids"].get("imdb", "")
            tmdb = str(r_item["ids"].get("tmdb", "")) if r_item["ids"].get("tmdb") else ""
            existing = (await db.execute(
                select(UserRating).where(
                    UserRating.user_id == user_id,
                    UserRating.imdb_id == imdb,
                )
            )).scalar_one_or_none() if imdb else None
            if not existing:
                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                db.add(UserRating(
                    user_id=user_id,
                    title=r_item["title"],
                    item_type=r_item["item_type"],
                    rating=r_item["rating"],
                    imdb_id=imdb or None,
                    tmdb_id=tmdb or None,
                    source="imported",
                    created_at=now_naive,
                ))
                db_count += 1
        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            results["errors"].append(f"DB ratings: {str(e)[:100]}")
        results["ratings"]["db"] = db_count

        # Push to Simkl (batch 50, 1/sec)
        if has_simkl and all_ratings:
            await _progress("Pushing ratings to Simkl…", "ratings", 30)
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            simkl_ok = 0
            total_rating_batches = max(1, (len(all_ratings) + BATCH - 1) // BATCH)
            for i in range(0, len(all_ratings), BATCH):
                batch = all_ratings[i:i + BATCH]
                items = []
                for r_item in batch:
                    items.append({
                        "ids": r_item["ids"],
                        "rating": r_item["rating"],
                        "_type": r_item["_type"],
                    })
                try:
                    await simkl.add_ratings(items)
                    simkl_ok += len(batch)
                    batch_num = i // BATCH + 1
                    pct = 30 + int(40 * batch_num / total_rating_batches)
                    await _progress(f"Simkl ratings: batch {batch_num}/{total_rating_batches}", "ratings", pct)
                    if i + BATCH < len(all_ratings):
                        await asyncio.sleep(1)
                except Exception as e:
                    results["errors"].append(f"Simkl ratings batch {i//BATCH+1}: {str(e)[:100]}")
            results["ratings"]["simkl"] = simkl_ok

        # Push to MDBList
        if has_mdblist and all_ratings:
            key = await _get_mdblist_key()
            from app.utils.mdblist_client import MDBListClient
            mdb = MDBListClient(api_key=key)
            movie_ratings = [
                {"ids": r["ids"], "rating": r["rating"], "rated_at": r.get("rated_at", "")}
                for r in all_ratings if r["_type"] == "movies"
            ]
            show_ratings = [
                {"ids": r["ids"], "rating": r["rating"], "rated_at": r.get("rated_at", "")}
                for r in all_ratings if r["_type"] == "shows"
            ]
            try:
                if movie_ratings:
                    await mdb.add_ratings(movies=movie_ratings)
                if show_ratings:
                    await mdb.add_ratings(shows=show_ratings)
                results["ratings"]["mdblist"] = len(movie_ratings) + len(show_ratings)
            except Exception as e:
                results["errors"].append(f"MDBList ratings: {str(e)[:100]}")
            await mdb.close()

        await _progress(f"📦 Trakt import: {len(all_ratings)} ratings processed", "ratings", 100)

    # ── 2. WATCHED HISTORY ──────────────────────────────────────────────
    if push_watched:
        watched_movies = parsed.get("watched_movies", [])
        watched_shows = parsed.get("watched_shows", [])

        # Store in local DB (watch_history table)
        from app.models.schema import WatchHistory
        from sqlalchemy import cast, Date as SADate
        wh_count = 0
        wh_ep_count = 0

        await _progress("Importing movie watch history to DB…", "watched", 5)
        for item in watched_movies:
            ids = _ids(item, "movie")
            title = _title(item, "movie")
            watched_at_str = item.get("last_watched_at", "")
            try:
                watched_at = datetime.fromisoformat(watched_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                watched_at = datetime.now(timezone.utc).replace(tzinfo=None)
            # Check for same-day duplicate
            existing = (await db.execute(
                select(WatchHistory).where(
                    WatchHistory.user_id == user_id,
                    WatchHistory.imdb_id == ids.get("imdb", ""),
                    cast(WatchHistory.watched_at, SADate) == watched_at.date(),
                )
            )).scalar_one_or_none() if ids.get("imdb") else None
            if not existing:
                db.add(WatchHistory(
                    user_id=user_id,
                    emby_id=None,
                    item_type="movie",
                    title=title,
                    imdb_id=ids.get("imdb") or None,
                    tmdb_id=str(ids.get("tmdb", "")) or None,
                    watched_at=watched_at,
                    progress=100,
                    source="trakt_import",
                ))
                wh_count += 1
        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            results["errors"].append(f"DB watched movies: {str(e)[:100]}")

        # Store show episodes in local DB
        await _progress("Importing show watch history to DB…", "watched", 15)
        for show in watched_shows:
            show_ids = _ids(show, "show")
            show_title = _title(show, "show")
            show_imdb = show_ids.get("imdb") or None
            show_tmdb = str(show_ids.get("tmdb", "")) if show_ids.get("tmdb") else None
            show_tvdb = str(show_ids.get("tvdb", "")) if show_ids.get("tvdb") else None
            for season in show.get("seasons", []):
                season_num = season.get("number", 0)
                for episode in season.get("episodes", []):
                    ep_num = episode.get("number", 0)
                    watched_at_str = episode.get("last_watched_at", "")
                    try:
                        watched_at = datetime.fromisoformat(watched_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
                    except Exception:
                        watched_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    ep_title = f"{show_title} S{season_num:02d}E{ep_num:02d}"
                    # Dedup by show imdb + season + episode + date
                    if show_imdb:
                        existing = (await db.execute(
                            select(WatchHistory).where(
                                WatchHistory.user_id == user_id,
                                WatchHistory.imdb_id == show_imdb,
                                WatchHistory.season_number == season_num,
                                WatchHistory.episode_number == ep_num,
                                cast(WatchHistory.watched_at, SADate) == watched_at.date(),
                            )
                        )).scalar_one_or_none()
                    else:
                        existing = None
                    if not existing:
                        db.add(WatchHistory(
                            user_id=user_id,
                            emby_id=None,
                            item_type="episode",
                            title=ep_title,
                            series_name=show_title,
                            season_number=season_num,
                            episode_number=ep_num,
                            imdb_id=show_imdb,
                            tmdb_id=show_tmdb,
                            tvdb_id=show_tvdb,
                            watched_at=watched_at,
                            progress=100,
                            source="trakt_import",
                        ))
                        wh_ep_count += 1
        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            results["errors"].append(f"DB watched shows: {str(e)[:100]}")

        results["watched"]["db"] = wh_count
        results["watched"]["db_episodes"] = wh_ep_count

        # Push movies to Simkl history
        if has_simkl and watched_movies:
            await _progress("Pushing watched movies to Simkl…", "watched", 30)
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            simkl_ok = 0
            total_movie_batches = max(1, (len(watched_movies) + BATCH - 1) // BATCH)
            for i in range(0, len(watched_movies), BATCH):
                batch = watched_movies[i:i + BATCH]
                items = []
                for item in batch:
                    ids = _ids(item, "movie")
                    if ids:
                        items.append({
                            "ids": ids,
                            "_type": "movies",
                            "watched_at": item.get("last_watched_at", ""),
                        })
                try:
                    if items:
                        await simkl.add_to_history(items)
                        simkl_ok += len(items)
                    batch_num = i // BATCH + 1
                    pct = 30 + int(30 * batch_num / total_movie_batches)
                    await _progress(f"Simkl movies: batch {batch_num}/{total_movie_batches}", "watched", pct)
                    if i + BATCH < len(watched_movies):
                        await asyncio.sleep(1)
                except Exception as e:
                    results["errors"].append(f"Simkl history batch {i//BATCH+1}: {str(e)[:100]}")

            # Push shows to Simkl history (with season/episode structure)
            await _progress("Pushing watched shows to Simkl…", "watched", 60)
            total_shows = max(1, len(watched_shows))
            for idx, show in enumerate(watched_shows):
                ids = _ids(show, "show")
                if not ids:
                    continue
                seasons = show.get("seasons", [])
                if seasons:
                    show_item = {
                        "_type": "show",
                        "ids": ids,
                        "seasons": seasons,
                    }
                    try:
                        await simkl.add_to_history([show_item])
                        simkl_ok += 1
                        await asyncio.sleep(1)
                    except Exception as e:
                        results["errors"].append(f"Simkl show {_title(show, 'show')}: {str(e)[:80]}")
                if (idx + 1) % 10 == 0 or idx == len(watched_shows) - 1:
                    pct = 60 + int(20 * (idx + 1) / total_shows)
                    await _progress(f"Simkl shows: {idx + 1}/{total_shows}", "watched", pct)

            results["watched"]["simkl"] = simkl_ok

        # Push to MDBList history
        if has_mdblist and watched_movies:
            await _progress("Pushing watched history to MDBList…", "watched", 85)
            key = await _get_mdblist_key()
            from app.utils.mdblist_client import MDBListClient
            mdb = MDBListClient(api_key=key)
            mdb_movies = []
            for item in watched_movies:
                ids = _ids(item, "movie")
                if ids:
                    mdb_movies.append({
                        "ids": ids,
                        "watched_at": item.get("last_watched_at", ""),
                    })
            try:
                # MDBList batch — send all at once
                if mdb_movies:
                    await mdb.add_to_watched(movies=mdb_movies)
                results["watched"]["mdblist"] = len(mdb_movies)
            except Exception as e:
                results["errors"].append(f"MDBList history: {str(e)[:100]}")
            await mdb.close()

        await _progress(
            f"📦 Trakt import: {len(watched_movies)} movies, {wh_ep_count} episodes from {len(watched_shows)} shows processed",
            "watched", 100,
        )

    # ── 3. WATCHLIST ────────────────────────────────────────────────────
    if push_watchlist:
        watchlist = parsed.get("watchlist", [])
        await _progress("Pushing watchlist…", "watchlist", 10)

        if has_simkl and watchlist:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            wl_items = []
            for item in watchlist:
                item_type = item.get("type", "movie")
                ids = _ids(item, item_type)
                if ids:
                    wl_items.append({
                        "ids": ids,
                        "_type": "movies" if item_type == "movie" else "shows",
                        "to": "plantowatch",
                    })
            try:
                if wl_items:
                    await simkl.add_to_watchlist(items=wl_items)
                results["watchlist"]["simkl"] = len(wl_items)
            except Exception as e:
                results["errors"].append(f"Simkl watchlist: {str(e)[:100]}")

        # Push to MDBList watchlist
        if has_mdblist and watchlist:
            await _progress("Pushing watchlist to MDBList…", "watchlist", 50)
            key = await _get_mdblist_key()
            from app.utils.mdblist_client import MDBListClient
            mdb = MDBListClient(api_key=key)
            mdb_wl_movies = []
            mdb_wl_shows = []
            for item in watchlist:
                item_type = item.get("type", "movie")
                ids = _ids(item, item_type)
                if not ids:
                    continue
                # MDBList expects flat id dicts: {"imdb": "tt...", "tmdb": 630}
                flat = {}
                if ids.get("imdb"):
                    flat["imdb"] = ids["imdb"]
                if ids.get("tmdb"):
                    flat["tmdb"] = ids["tmdb"]
                if not flat:
                    continue
                if item_type == "movie":
                    mdb_wl_movies.append(flat)
                else:
                    mdb_wl_shows.append(flat)
            try:
                if mdb_wl_movies or mdb_wl_shows:
                    await mdb.add_to_watchlist(
                        movies=mdb_wl_movies or None,
                        shows=mdb_wl_shows or None,
                    )
                results["watchlist"]["mdblist"] = len(mdb_wl_movies) + len(mdb_wl_shows)
            except Exception as e:
                results["errors"].append(f"MDBList watchlist: {str(e)[:100]}")
            await mdb.close()

        await _progress(f"📦 Trakt import: {len(watchlist)} watchlist items processed", "watchlist", 100)

    # Clean up cached data
    try:
        await r.delete(f"trakt_import:{import_id}")
    except Exception:
        pass

    # Invalidate activity-gate caches so fresh data shows on next sync
    if has_simkl:
        try:
            import hashlib
            prefix = hashlib.md5(user.simkl_access_token.encode()).hexdigest()[:12]
            keys = await r.keys(f"simkl_sync_*:{prefix}:*")
            if keys:
                await r.delete(*keys)
        except Exception:
            pass

    await _progress("✓ Trakt import complete", "done", 100)

    return {"ok": True, "results": results}


# ═══════════════════════════════════════════════════════════════════════

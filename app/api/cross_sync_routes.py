"""Routes extracted from routes.py — cross_sync_routes.py."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.utils.redis_cache import get_redis
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user
from app.api.route_helpers import _get_active_providers, _get_mdblist_key

log = structlog.get_logger()

router = APIRouter()



@router.get("/api/mdblist/sync-status")
async def mdblist_sync_status(db: AsyncSession = Depends(get_db)):
    """Compare Simkl watched history against MDBList to show what's missing.
    Returns counts and sample items for movies and shows."""
    import json as _json

    providers = await _get_active_providers(db)
    if "mdblist" not in providers:
        raise HTTPException(400, "MDBList is not an active provider")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    # Get the first linked user
    user = (await db.execute(
        select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
    )).scalars().first()
    if not user:
        raise HTTPException(400, "No linked Simkl user found")

    from app.utils.mdblist_client import MDBListClient

    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )
    mdb = MDBListClient(api_key=key)

    try:
        # Fetch Simkl watched movies
        simkl_movies = await simkl.get_watched(kind="movies")
        # Fetch MDBList watched
        mdb_watched = await mdb.get_watched()

        # Build MDBList watched ID sets
        mdb_movie_ids: set[str] = set()
        for entry in mdb_watched.get("movies", []):
            ids = entry.get("movie", {}).get("ids", {})
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
                v = ids.get(k)
                if v:
                    mdb_movie_ids.add(f"{k}:{v}")

        mdb_show_keys: set[str] = set()
        for entry in mdb_watched.get("shows", []):
            ids = entry.get("show", {}).get("ids", {})
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
                v = ids.get(k)
                if v:
                    mdb_show_keys.add(f"{k}:{v}")

        # Find Simkl movies not in MDBList
        missing_movies = []
        for entry in simkl_movies:
            movie = entry.get("movie", {})
            ids = movie.get("ids", {})
            item_keys = set()
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
                v = ids.get(k)
                if v:
                    item_keys.add(f"{k}:{v}")
            if not item_keys & mdb_movie_ids:
                missing_movies.append({
                    "title": movie.get("title", ""),
                    "year": movie.get("year"),
                    "ids": ids,
                    "last_watched_at": entry.get("last_watched_at"),
                })

        # Find Simkl shows not in MDBList (show-level only)
        simkl_shows = await simkl.get_watched(kind="shows")
        missing_shows = []
        for entry in simkl_shows:
            show = entry.get("show", {})
            ids = show.get("ids", {})
            item_keys = set()
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
                v = ids.get(k)
                if v:
                    item_keys.add(f"{k}:{v}")
            if not item_keys & mdb_show_keys:
                missing_shows.append({
                    "title": show.get("title", ""),
                    "year": show.get("year"),
                    "ids": ids,
                    "last_watched_at": entry.get("last_watched_at"),
                })

        return {
            "simkl_movies": len(simkl_movies),
            "simkl_shows": len(simkl_shows),
            "mdblist_movies": len(mdb_watched.get("movies", [])),
            "mdblist_shows": len(mdb_watched.get("shows", [])),
            "missing_movies": len(missing_movies),
            "missing_shows": len(missing_shows),
            "sample_movies": missing_movies[:20],
            "sample_shows": missing_shows[:20],
        }
    finally:
        await simkl.close()
        await mdb.close()


@router.post("/api/mdblist/sync-from-simkl")
async def sync_simkl_to_mdblist(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Incremental sync of Simkl watched history into MDBList.

    On first run, pushes everything. On subsequent runs, only pushes items
    watched after the last successful sync timestamp (stored in Redis).
    Pass {"full": true} in the body to force a full re-sync.
    """
    import json as _json

    providers = await _get_active_providers(db)
    if "mdblist" not in providers:
        raise HTTPException(400, "MDBList is not an active provider")

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    user = (await db.execute(
        select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
    )).scalars().first()
    if not user:
        raise HTTPException(400, "No linked Simkl user found")

    # Check for force-full flag
    force_full = False
    try:
        body = await request.json()
        force_full = body.get("full", False)
    except Exception:
        pass

    # Load last sync timestamp from Redis
    r = await get_redis()
    last_sync_ts = None
    if not force_full:
        raw = await r.get("mdblist_sync_last_completed")
        if raw:
            last_sync_ts = raw if isinstance(raw, str) else raw.decode()

    from app.utils.mdblist_client import MDBListClient

    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )
    mdb = MDBListClient(api_key=key)

    sync_started_at = datetime.now(timezone.utc).isoformat()

    try:
        # Fetch full Simkl watched history
        simkl_movies = await simkl.get_watched(kind="movies")
        simkl_shows = await simkl.get_watched(kind="shows")

        # Build MDBList movie payloads — filter by last_watched_at if delta sync
        mdb_movies = []
        skipped_movies = 0
        for entry in simkl_movies:
            watched_at = entry.get("last_watched_at", "")
            if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                skipped_movies += 1
                continue
            movie = entry.get("movie", {}) if "movie" in entry else entry
            ids = movie.get("ids", {})
            mdb_ids = {}
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
                if ids.get(k):
                    mdb_ids[k] = ids[k]
            if mdb_ids:
                mdb_movies.append({
                    "ids": mdb_ids,
                    "watched_at": watched_at or datetime.now(timezone.utc).isoformat(),
                })

        mdb_shows = []
        # Parse episode-level data from already-fetched simkl_shows
        # (Simkl's /sync/all-items/shows/completed includes seasons/episodes)
        from collections import defaultdict
        show_eps: dict[str, dict] = {}
        total_eps_fetched = 0
        skipped_eps = 0

        for entry in simkl_shows:
            show = entry.get("show", {}) if "show" in entry else entry
            show_ids = show.get("ids", {})
            show_key = str(show_ids.get("simkl") or show_ids.get("simkl_id") or "") or str(show_ids.get("imdb", ""))
            if not show_key:
                continue

            # Show-level last_watched_at for delta sync filtering
            show_watched_at = entry.get("last_watched_at", "")

            seasons = entry.get("seasons", [])
            if not seasons:
                # No season data — skip (Simkl may not include episode-level
                # detail depending on the response). The show-level entry
                # is still useful for movie-style "mark whole show watched".
                continue

            if show_key not in show_eps:
                mdb_ids = {}
                for k in ("imdb", "tmdb", "tvdb", "simkl"):
                    if show_ids.get(k):
                        mdb_ids[k] = show_ids[k]
                show_eps[show_key] = {"ids": mdb_ids, "seasons": defaultdict(list)}

            for season in seasons:
                s_num = season.get("number", 0)
                for ep in season.get("episodes", []):
                    e_num = ep.get("number", 0)
                    watched_at = ep.get("watched_at") or show_watched_at or ""

                    if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                        skipped_eps += 1
                        continue

                    show_eps[show_key]["seasons"][s_num].append({
                        "number": e_num,
                        "watched_at": watched_at,
                    })
                    total_eps_fetched += 1

        log.info("mdblist_sync.episodes_parsed",
                 shows=len(show_eps), episodes=total_eps_fetched,
                 skipped_eps=skipped_eps, skipped_movies=skipped_movies,
                 mode="delta" if last_sync_ts else "full")

        # Convert grouped data to MDBList payload
        for show_key, show_data in show_eps.items():
            if not show_data["ids"]:
                continue
            mdb_seasons = []
            for s_num, eps in sorted(show_data["seasons"].items()):
                seen = set()
                deduped = []
                for ep in eps:
                    if ep["number"] not in seen:
                        seen.add(ep["number"])
                        deduped.append(ep)
                mdb_seasons.append({"number": s_num, "episodes": deduped})
            if mdb_seasons:
                mdb_shows.append({"ids": show_data["ids"], "seasons": mdb_seasons})

        # Send to MDBList in batches
        results = {"movies": 0, "shows": 0, "episodes": 0}
        batch_size = 100

        for i in range(0, len(mdb_movies), batch_size):
            batch = mdb_movies[i:i + batch_size]
            try:
                resp_data = await mdb.add_to_watched(movies=batch)
                results["movies"] += resp_data.get("updated", {}).get("movies", 0)
            except Exception as e:
                log.warning("mdblist_sync.movie_batch_failed", batch=i, error=str(e)[:120])

        for i in range(0, len(mdb_shows), batch_size):
            batch = mdb_shows[i:i + batch_size]
            try:
                resp_data = await mdb.add_to_watched(shows=batch)
                results["shows"] += resp_data.get("updated", {}).get("seasons", 0)
                results["episodes"] += resp_data.get("updated", {}).get("episodes", 0)
            except Exception as e:
                log.warning("mdblist_sync.show_batch_failed", batch=i, error=str(e)[:120])

        # Also sync ratings (only new ones since last sync)
        ratings_result = {"movies": 0, "episodes": 0}
        try:
            simkl_ratings = await simkl.get_user_ratings(kind="movies")
            mdb_rate_movies = []
            for entry in simkl_ratings:
                rated_at = entry.get("rated_at", "")
                if last_sync_ts and rated_at and rated_at <= last_sync_ts:
                    continue
                movie = entry.get("movie", {})
                ids = movie.get("ids", {})
                mdb_ids = {}
                for k in ("imdb", "tmdb", "tvdb", "simkl"):
                    if ids.get(k):
                        mdb_ids[k] = ids[k]
                if mdb_ids and entry.get("rating"):
                    mdb_rate_movies.append({
                        "ids": mdb_ids,
                        "rating": entry["rating"],
                        "rated_at": rated_at or datetime.now(timezone.utc).isoformat(),
                    })
            if mdb_rate_movies:
                for i in range(0, len(mdb_rate_movies), batch_size):
                    batch = mdb_rate_movies[i:i + batch_size]
                    try:
                        resp_data = await mdb.add_ratings(movies=batch)
                        ratings_result["movies"] += resp_data.get("updated", {}).get("movies", 0)
                    except Exception as e:
                        log.warning("mdblist_sync.rating_batch_failed", batch=i, error=str(e)[:120])
        except Exception as e:
            log.warning("mdblist_sync.ratings_failed", error=str(e)[:120])

        # Store sync timestamp on success
        await r.set("mdblist_sync_last_completed", sync_started_at)

        log.info("mdblist_sync.complete",
                 movies_synced=results["movies"],
                 shows_synced=results["shows"],
                 episodes_synced=results["episodes"],
                 ratings_movies=ratings_result["movies"],
                 mode="delta" if last_sync_ts else "full",
                 skipped_movies=skipped_movies,
                 skipped_eps=skipped_eps)

        return {
            "status": "ok",
            "mode": "delta" if last_sync_ts else "full",
            "watched": results,
            "ratings": ratings_result,
            "totals": {
                "simkl_movies": len(simkl_movies),
                "simkl_shows": len(simkl_shows),
                "pushed_movies": len(mdb_movies),
                "pushed_shows": len(mdb_shows),
                "skipped_movies": skipped_movies,
                "skipped_episodes": skipped_eps,
            },
        }
    finally:
        await simkl.close()
        await mdb.close()


@router.post("/api/mdblist/sync-to-simkl")
async def sync_mdblist_to_simkl(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Import MDBList watched history and ratings into Simkl.

    On first run, pushes everything. On subsequent runs, only pushes items
    watched/rated after the last sync timestamp (stored in Redis).
    Pass {"full": true} in body to force a full re-sync.
    """
    import json as _json

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    user = (await db.execute(
        select(User).where(User.id == _user.id)
    )).scalar_one_or_none()
    if not user or not user.simkl_access_token:
        raise HTTPException(400, "Simkl not linked")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    force_full = body.get("full", False)

    from app.utils.mdblist_client import MDBListClient
    mdb = MDBListClient(api_key=key)
    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )

    try:
        r = await get_redis()

        # Delta sync: read last sync timestamp
        last_sync_ts = None
        if not force_full:
            raw = await r.get("mdblist_to_simkl_last_sync")
            if raw:
                last_sync_ts = raw if isinstance(raw, str) else raw.decode()

        sync_start = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # ── 1. Fetch MDBList watched history ──
        mdb_watched = await mdb.get_watched()

        # ── 2. Build Simkl history payload ──
        history_payload: list[dict] = []
        skipped = 0

        # Movies
        for entry in mdb_watched.get("movies", []):
            inner = entry.get("movie") or entry
            watched_at = entry.get("last_watched_at") or entry.get("watched_at") or ""
            if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                skipped += 1
                continue
            ids = inner.get("ids", {})
            simkl_ids = {}
            for k in ("imdb", "tmdb", "tvdb"):
                if ids.get(k):
                    simkl_ids[k] = ids[k]
            if not simkl_ids:
                continue
            history_payload.append({
                "ids": simkl_ids,
                "watched_at": watched_at or sync_start,
                "_type": "movie",
            })

        # Shows (with episode-level data)
        for entry in mdb_watched.get("shows", []):
            inner = entry.get("show") or entry
            watched_at = entry.get("last_watched_at") or entry.get("watched_at") or ""
            if last_sync_ts and watched_at and watched_at <= last_sync_ts:
                skipped += 1
                continue
            ids = inner.get("ids", {})
            simkl_ids = {}
            for k in ("imdb", "tmdb", "tvdb"):
                if ids.get(k):
                    simkl_ids[k] = ids[k]
            if not simkl_ids:
                continue
            # Check if entry has season/episode data
            seasons = entry.get("seasons", [])
            if seasons:
                history_payload.append({
                    "ids": simkl_ids,
                    "seasons": seasons,
                    "_type": "show",
                })
            else:
                history_payload.append({
                    "ids": simkl_ids,
                    "watched_at": watched_at or sync_start,
                    "_type": "show",
                })

        log.info("mdblist_to_simkl.history_built",
                 movies=sum(1 for p in history_payload if p.get("_type") == "movie"),
                 shows=sum(1 for p in history_payload if p.get("_type") == "show"),
                 skipped=skipped,
                 mode="delta" if last_sync_ts else "full")

        # Push history to Simkl
        history_result = {}
        if history_payload:
            history_result = await simkl.add_to_history(history_payload)

        # ── 3. Fetch MDBList ratings and push to Simkl ──
        mdb_ratings = await mdb.get_ratings()
        ratings_payload: list[dict] = []

        if isinstance(mdb_ratings, dict):
            for kind in ("movies", "shows"):
                for item in mdb_ratings.get(kind, []):
                    rating_val = item.get("rating")
                    if not rating_val:
                        continue
                    inner = item.get("movie") or item.get("show") or item
                    ids = inner.get("ids", {})
                    simkl_ids = {}
                    for k in ("imdb", "tmdb", "tvdb"):
                        if ids.get(k):
                            simkl_ids[k] = ids[k]
                    if not simkl_ids:
                        continue
                    ratings_payload.append({
                        "ids": simkl_ids,
                        "rating": int(round(float(rating_val))),
                        "_type": "movie" if kind == "movies" else "show",
                    })

        ratings_result = {}
        if ratings_payload:
            ratings_result = await simkl.add_ratings(ratings_payload)

        log.info("mdblist_to_simkl.ratings_pushed", count=len(ratings_payload),
                 result=ratings_result.get("added", {}))

        # Save sync timestamp
        await r.set("mdblist_to_simkl_last_sync", sync_start)

        added = history_result.get("added", {})
        return {
            "mode": "full" if force_full or not last_sync_ts else "delta",
            "history": {
                "movies_pushed": added.get("movies", 0),
                "shows_pushed": added.get("shows", 0),
                "episodes_pushed": added.get("episodes", 0),
                "skipped": skipped,
                "total_payload": len(history_payload),
            },
            "ratings": {
                "pushed": len(ratings_payload),
                "added": ratings_result.get("added", {}),
            },
        }
    finally:
        await simkl.close()
        await mdb.close()


# ═══════════════════════════════════════════════════════════════════════════

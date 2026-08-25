"""Routes extracted from routes.py — duplicates_routes.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.security.auth import get_current_user

log = structlog.get_logger()

router = APIRouter()



@router.get("/duplicates")
async def duplicates_page():
    """Serve the duplicate/conflict detector page."""
    with open("frontend/templates/duplicates.html", "r") as f:
        return HTMLResponse(f.read())


@router.get("/api/duplicates/scan")
async def scan_duplicates(
    include_dismissed: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Scan library and watch history for duplicates and conflicts.

    Issues the user has dismissed are filtered out unless
    ``include_dismissed=true``.  Dismissal exists because orphaned
    history for deliberately-removed media is an expected finding, not
    a problem — otherwise it reappears on every scan indefinitely.
    """
    from app.models.schema import WatchHistory, DismissedIssue
    issues = []

    # ── Fetch all library items from Emby (not cache — cache dedupes by
    #    provider key so duplicates sharing an IMDB ID overwrite each other).
    _dup_fields = "ProviderIds,ProductionYear,MediaSources,SeriesName"
    async with EmbyClient() as emby:
        all_movies = await emby.get_all_movies()
        all_series = await emby.get_all_series()
        # Fetch virtual folders then query each library individually so every
        # item is tagged with its Emby library name (path matching fails on
        # SMB shares where the mount path differs from the Locations array).
        vfolders = await emby.get_virtual_folders()
        _dup_items: list[dict] = []  # items enriched with _library_name
        for vf in vfolders:
            lib_name = vf.get("name", "Unknown")
            lib_id = vf.get("item_id")
            if not lib_id:
                continue
            _s = 0
            while True:
                _resp = await emby.get_items(
                    parent_id=lib_id, fields=_dup_fields,
                    limit=500, start_index=_s,
                )
                batch = _resp.get("Items", [])
                for it in batch:
                    it["_library_name"] = lib_name
                _dup_items.extend(batch)
                if _s + 500 >= _resp.get("TotalRecordCount", 0):
                    break
                _s += 500

    # Build lookup maps from the global fetches (for orphan/metadata checks)
    library_imdb_map: dict[str, list] = {}       # imdb_id -> [items]
    library_emby_ids: set[str] = set()            # all emby IDs in library
    library_series_titles: set[str] = set()       # lowercase series titles
    library_emby_to_imdb: dict[str, str] = {}     # emby_id -> imdb_id

    for item in all_movies + all_series:
        eid = item.get("Id")
        pids = item.get("ProviderIds") or {}
        iid = pids.get("Imdb")
        name = item.get("Name", "")
        if eid:
            library_emby_ids.add(eid)
            if iid:
                library_emby_to_imdb[eid] = iid
        if iid:
            library_imdb_map.setdefault(iid, []).append(item)
        if item.get("Type") == "Series" and name:
            library_series_titles.add(name.lower().strip())

    # Build IMDB map from per-library fetch for duplicate display
    # Only consider Movie and Series — episodes/seasons are not meaningful duplicates
    dup_imdb_map: dict[str, list] = {}
    for item in _dup_items:
        if item.get("Type") not in ("Movie", "Series"):
            continue
        iid = (item.get("ProviderIds") or {}).get("Imdb")
        if iid:
            dup_imdb_map.setdefault(iid, []).append(item)

    # ── 1. Duplicate library items (same IMDB ID, different Emby IDs)
    def _res_tier(item: dict) -> str:
        """Derive resolution label from MediaSources."""
        ms = (item.get("MediaSources") or [None])[0]
        if not ms:
            return "Unknown"
        width = 0
        for stream in ms.get("MediaStreams", []):
            if stream.get("Type") == "Video":
                width = stream.get("Width", 0)
                break
        if width >= 3800:
            return "4K"
        if width >= 1900:
            return "Full HD"
        if width >= 1200:
            return "HD"
        if width > 0:
            return "SD"
        return "Unknown"

    def _file_size_mb(item: dict) -> float | None:
        ms = (item.get("MediaSources") or [None])[0]
        if not ms:
            return None
        size = ms.get("Size")
        return round(size / (1024 * 1024), 1) if size else None

    for imdb_id, items in dup_imdb_map.items():
        if len(items) > 1:
            first_type = items[0].get("Type", "")
            item_type = "movie" if first_type == "Movie" else "series"

            # Build display title: for Episodes, prepend SeriesName
            def _display_title(item: dict) -> str:
                name = item.get("Name", "Unknown")
                series = item.get("SeriesName")
                if item.get("Type") == "Episode" and series:
                    return f"{series} — {name}"
                return name

            enriched = []
            for i in items:
                _pids = i.get("ProviderIds") or {}
                enriched.append({
                    "emby_id": i.get("Id"),
                    "title": _display_title(i),
                    "year": i.get("ProductionYear"),
                    "item_type": item_type,
                    "library": i.get("_library_name", "Unknown"),
                    "resolution": _res_tier(i),
                    "size_mb": _file_size_mb(i),
                    # Exposed so the re-link dialog can prefill current values
                    "imdb_id": _pids.get("Imdb"),
                    "tmdb_id": _pids.get("Tmdb"),
                    "tvdb_id": _pids.get("Tvdb"),
                })
            res_tiers = {e["resolution"] for e in enriched}
            same_resolution = len(res_tiers) == 1 and "Unknown" not in res_tiers
            group_title = _display_title(items[0])
            issues.append({
                "type": "duplicate_library",
                "severity": "warning",
                # Stable identity for dismissal — survives Emby ID changes
                "issue_key": f"dup:{imdb_id}",
                "title": group_title,
                "imdb_id": imdb_id,
                "details": f"{len(items)} copies in library",
                "items": enriched,
                "same_resolution": same_resolution,
            })

    # ── 2. Orphaned watch history
    user_id = current_user.id

    # 2a. Movies: check by emby_id first, then IMDB fallback
    orphaned_movies = (await db.execute(
        select(
            WatchHistory.emby_id,
            WatchHistory.imdb_id,
            WatchHistory.title,
            func.max(WatchHistory.watched_at).label("last_watched"),
        )
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.item_type == "movie",
        )
        .group_by(WatchHistory.emby_id, WatchHistory.imdb_id, WatchHistory.title)
    )).all()

    seen_movie_titles: set[str] = set()
    for row in orphaned_movies:
        # Check if item still in library by emby_id or IMDB
        in_library = False
        if row.emby_id and row.emby_id in library_emby_ids:
            in_library = True
        elif row.imdb_id and row.imdb_id in library_imdb_map:
            in_library = True
        if not in_library:
            title = row.title or "Unknown"
            if title.lower() in seen_movie_titles:
                continue
            seen_movie_titles.add(title.lower())
            issues.append({
                "type": "orphaned_history",
                "severity": "info",
                "issue_key": f"orphan:movie:{row.imdb_id or title.lower()}",
                "title": title,
                "imdb_id": row.imdb_id,
                "emby_id": row.emby_id,
                "item_type": "movie",
                "details": f"Movie watched but no longer in library (last: {row.last_watched.strftime('%Y-%m-%d') if row.last_watched else '?'})",
            })

    # 2b. Episodes: group by series_name, check if series still in library
    #     Uses library_series_titles set (case-insensitive) for reliable matching.
    #     Skips episodes where series_name is NULL (can't determine orphan status).
    orphaned_eps = (await db.execute(
        select(
            WatchHistory.series_name,
            func.count(WatchHistory.id).label("ep_count"),
            func.max(WatchHistory.watched_at).label("last_watched"),
        )
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.item_type == "episode",
            WatchHistory.series_name.isnot(None),
            WatchHistory.series_name != "",
        )
        .group_by(WatchHistory.series_name)
    )).all()

    for row in orphaned_eps:
        show_name = row.series_name.strip()
        if show_name.lower() in library_series_titles:
            continue  # series still in library
        ep_label = f"{row.ep_count} episode{'s' if row.ep_count != 1 else ''}"
        issues.append({
            "type": "orphaned_history",
            "severity": "info",
            "issue_key": f"orphan:series:{show_name.lower()}",
            "title": show_name,
            "imdb_id": None,
            "item_type": "episode",
            "episode_count": row.ep_count,
            "details": f"{ep_label} watched but series no longer in library (last: {row.last_watched.strftime('%Y-%m-%d') if row.last_watched else '?'})",
        })

    # ── 3. Watch history entries with no IMDB ID (movies only) —
    #        Use emby_id to look up the real IMDB from Emby and backfill
    no_imdb = (await db.execute(
        select(
            WatchHistory.title,
            WatchHistory.emby_id,
            WatchHistory.item_type,
            func.count(WatchHistory.id).label("count"),
        )
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.imdb_id.is_(None),
            WatchHistory.item_type == "movie",
        )
        .group_by(WatchHistory.title, WatchHistory.emby_id, WatchHistory.item_type)
    )).all()

    backfilled_titles: set[str] = set()
    for row in no_imdb:
        title = row.title or "Unknown"
        # Try to resolve IMDB from emby_id first (most reliable)
        resolved_imdb = None
        if row.emby_id:
            resolved_imdb = library_emby_to_imdb.get(row.emby_id)
        if not resolved_imdb:
            # Fallback: check library cache by title
            cached = await LibraryCache.find_by_title(title)
            resolved_imdb = (cached.get("provider_ids") or {}).get("Imdb") if cached else None
        if resolved_imdb:
            # Backfill the watch history records
            stmt = WatchHistory.__table__.update().where(
                WatchHistory.user_id == user_id,
                WatchHistory.imdb_id.is_(None),
                WatchHistory.item_type == "movie",
            )
            if row.emby_id:
                stmt = stmt.where(WatchHistory.emby_id == row.emby_id)
            else:
                stmt = stmt.where(WatchHistory.title == title)
            await db.execute(stmt.values(imdb_id=resolved_imdb))
            await db.commit()
            backfilled_titles.add(title.lower())
        else:
            if title.lower() not in backfilled_titles:
                issues.append({
                    "type": "missing_metadata",
                    "severity": "warning",
                    "issue_key": f"meta:{row.emby_id or title.lower()}",
                    "title": title,
                    "emby_id": row.emby_id,
                    "item_type": row.item_type,
                    "details": f"No IMDB ID — {row.count} watch record(s) may not link properly",
                })

    # ── Filter out dismissed issues ────────────────────────────────────
    # Done at the end rather than inline so each detection block stays
    # independent of the dismissal mechanism.
    dismissed_rows = (await db.execute(
        select(DismissedIssue.issue_type, DismissedIssue.issue_key)
        .where(DismissedIssue.user_id == current_user.id)
    )).all()
    dismissed_set = {(r[0], r[1]) for r in dismissed_rows}

    hidden = 0
    if dismissed_set and not include_dismissed:
        kept = []
        for i in issues:
            if (i.get("type"), i.get("issue_key")) in dismissed_set:
                hidden += 1
                continue
            kept.append(i)
        issues = kept
    elif include_dismissed:
        for i in issues:
            i["dismissed"] = (i.get("type"), i.get("issue_key")) in dismissed_set

    # Sort: warnings first, then info
    severity_order = {"error": 0, "warning": 1, "info": 2}
    issues.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 9))

    return {
        "issues": issues,
        "total": len(issues),
        "duplicates": sum(1 for i in issues if i["type"] == "duplicate_library"),
        "orphaned": sum(1 for i in issues if i["type"] == "orphaned_history"),
        "missing_meta": sum(1 for i in issues if i["type"] == "missing_metadata"),
        "hidden": hidden,
        "dismissed_total": len(dismissed_set),
    }


@router.delete("/api/duplicates/resolve")
async def resolve_duplicate(
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Delete a duplicate library item by emby_id."""
    emby_id = payload.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id is required")

    async with EmbyClient() as emby:
        # Verify item exists before deleting
        try:
            item = await emby.get_item(emby_id)
        except Exception:
            raise HTTPException(404, "Item not found in Emby")
        title = item.get("Name", "Unknown")
        await emby.delete_item(emby_id)

    log.info("duplicates.item_deleted", emby_id=emby_id, title=title)
    return {"status": "ok", "deleted": emby_id, "title": title}


@router.post("/api/duplicates/relink")
async def relink_duplicate(
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Reassign provider IDs on a duplicate library item.

    Non-destructive alternative to /api/duplicates/resolve.  Where two
    copies collide because one has been matched to the wrong provider
    entry, this points the mis-matched copy at the correct IDs (or
    clears them) instead of deleting the file.

    Payload:
      emby_id  (required) — the copy to re-link
      imdb_id  — new IMDB ID, or "" / null to clear
      tmdb_id  — new TMDB ID, or "" / null to clear
      tvdb_id  — new TVDB ID, or "" / null to clear
      clear    — bool, drop all existing provider IDs first
      refresh  — bool (default true), queue a metadata refresh after

    At least one ID must be supplied unless clear=true.
    """
    emby_id = payload.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id is required")

    clear = bool(payload.get("clear"))
    do_refresh = payload.get("refresh", True)

    # Only include keys the caller actually sent — an absent key is left
    # untouched, whereas an explicit empty value removes it.
    provider_ids: dict = {}
    for field, emby_key in (("imdb_id", "Imdb"),
                            ("tmdb_id", "Tmdb"),
                            ("tvdb_id", "Tvdb")):
        if field in payload:
            val = payload.get(field)
            provider_ids[emby_key] = str(val).strip() if val else None

    if not clear and not any(v for v in provider_ids.values()):
        raise HTTPException(
            400, "Supply at least one provider ID, or set clear=true"
        )

    # Basic shape validation — a malformed IMDB ID silently orphans the item
    imdb_new = provider_ids.get("Imdb")
    if imdb_new and not re.fullmatch(r"tt\d{7,10}", imdb_new):
        raise HTTPException(
            400, f"Invalid IMDB ID format: {imdb_new} (expected ttNNNNNNN)"
        )
    for key in ("Tmdb", "Tvdb"):
        val = provider_ids.get(key)
        if val and not val.isdigit():
            raise HTTPException(400, f"Invalid {key} ID: {val} (expected digits)")

    async with EmbyClient() as emby:
        item = await emby.get_item_safe(emby_id)
        if not item:
            raise HTTPException(404, "Item not found in Emby")

        title = item.get("Name", "Unknown")
        before = dict(item.get("ProviderIds") or {})

        ok = await emby.set_provider_ids(
            emby_id, provider_ids, replace=clear,
        )
        if not ok:
            raise HTTPException(502, "Emby rejected the provider ID update")

        if do_refresh:
            await emby.refresh_item(emby_id)

        after = await emby.get_item_safe(emby_id)
        applied = dict((after or {}).get("ProviderIds") or {})

    # Library cache maps provider ID -> item.  Drop the entries for both
    # the old and new IDs so lookups don't resolve to the stale pairing.
    # Targeted deletes only — a full clear() would force a rebuild.
    try:
        from app.utils.redis_cache import cache_delete
        stale_keys = set()
        for pid_type, pid in list(before.items()) + list(applied.items()):
            if pid:
                stale_keys.add(LibraryCache._item_cache_key(pid_type, str(pid)))
        for key in stale_keys:
            await cache_delete(key)
        log.debug("duplicates.cache_keys_dropped", count=len(stale_keys))
    except Exception as e:
        log.warning("duplicates.cache_invalidate_failed", error=str(e)[:200])

    log.info("duplicates.item_relinked", emby_id=emby_id, title=title,
             before=before, after=applied, cleared=clear,
             refreshed=bool(do_refresh))

    return {
        "status": "ok",
        "emby_id": emby_id,
        "title": title,
        "before": before,
        "after": applied,
        "refreshed": bool(do_refresh),
    }


# ── Issue dismissal ─────────────────────────────────────────────────────

@router.post("/api/duplicates/dismiss")
async def dismiss_issue(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Permanently hide a scan issue from future scans.

    Payload: {issue_type, issue_key, title?, note?}

    Orphaned history for media deliberately removed from the library is
    an expected finding rather than a fault, so it needs a way to be
    acknowledged once instead of resurfacing on every scan.
    """
    from app.models.schema import DismissedIssue

    issue_type = (payload.get("issue_type") or "").strip()
    issue_key = (payload.get("issue_key") or "").strip()
    if not issue_type or not issue_key:
        raise HTTPException(400, "issue_type and issue_key are required")

    valid = {"orphaned_history", "missing_metadata", "duplicate_library"}
    if issue_type not in valid:
        raise HTTPException(400, f"Unknown issue_type: {issue_type}")

    existing = (await db.execute(
        select(DismissedIssue).where(
            DismissedIssue.user_id == current_user.id,
            DismissedIssue.issue_type == issue_type,
            DismissedIssue.issue_key == issue_key,
        )
    )).scalar_one_or_none()

    if existing:
        return {"status": "ok", "already_dismissed": True, "issue_key": issue_key}

    row = DismissedIssue(
        user_id=current_user.id,
        issue_type=issue_type,
        issue_key=issue_key[:512],
        title=(payload.get("title") or None),
        note=(payload.get("note") or None),
        dismissed_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    db.add(row)
    try:
        await db.commit()
    except Exception:
        # Unique constraint race — another tab dismissed the same issue
        await db.rollback()
        return {"status": "ok", "already_dismissed": True, "issue_key": issue_key}

    log.info("duplicates.issue_dismissed", user_id=current_user.id,
             issue_type=issue_type, issue_key=issue_key)
    return {"status": "ok", "issue_key": issue_key, "title": row.title}


@router.get("/api/duplicates/dismissed")
async def list_dismissed_issues(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List every dismissed issue so the user can undo one."""
    from app.models.schema import DismissedIssue

    rows = (await db.execute(
        select(DismissedIssue)
        .where(DismissedIssue.user_id == current_user.id)
        .order_by(DismissedIssue.dismissed_at.desc())
    )).scalars().all()

    return {
        "items": [
            {
                "id": r.id,
                "issue_type": r.issue_type,
                "issue_key": r.issue_key,
                "title": r.title,
                "note": r.note,
                "dismissed_at": r.dismissed_at.isoformat() if r.dismissed_at else None,
            }
            for r in rows
        ],
        "total": len(rows),
    }


@router.delete("/api/duplicates/dismiss")
async def undismiss_issue(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Un-hide a dismissed issue so it reappears on the next scan.

    Accepts either {id} or {issue_type, issue_key}.
    """
    from app.models.schema import DismissedIssue

    stmt = select(DismissedIssue).where(DismissedIssue.user_id == current_user.id)
    if payload.get("id"):
        stmt = stmt.where(DismissedIssue.id == int(payload["id"]))
    elif payload.get("issue_type") and payload.get("issue_key"):
        stmt = stmt.where(
            DismissedIssue.issue_type == payload["issue_type"],
            DismissedIssue.issue_key == payload["issue_key"],
        )
    else:
        raise HTTPException(400, "Supply id, or issue_type + issue_key")

    row = (await db.execute(stmt)).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Dismissed issue not found")

    title = row.title
    await db.delete(row)
    await db.commit()

    log.info("duplicates.issue_undismissed", user_id=current_user.id,
             issue_key=row.issue_key)
    return {"status": "ok", "title": title}


@router.post("/api/duplicates/dismiss-bulk")
async def dismiss_issues_bulk(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Dismiss multiple scan issues at once.

    Payload: {items: [{issue_type, issue_key, title?}, ...]}
    """
    from app.models.schema import DismissedIssue

    items = payload.get("items") or []
    if not items or len(items) > 500:
        raise HTTPException(400, "Supply 1-500 items")

    valid = {"orphaned_history", "missing_metadata", "duplicate_library"}
    added = 0
    skipped = 0

    for item in items:
        issue_type = (item.get("issue_type") or "").strip()
        issue_key = (item.get("issue_key") or "").strip()
        if not issue_type or not issue_key or issue_type not in valid:
            skipped += 1
            continue

        existing = (await db.execute(
            select(DismissedIssue).where(
                DismissedIssue.user_id == current_user.id,
                DismissedIssue.issue_type == issue_type,
                DismissedIssue.issue_key == issue_key,
            )
        )).scalar_one_or_none()

        if existing:
            skipped += 1
            continue

        db.add(DismissedIssue(
            user_id=current_user.id,
            issue_type=issue_type,
            issue_key=issue_key[:512],
            title=(item.get("title") or None),
            dismissed_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ))
        added += 1

    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(500, "Bulk dismiss failed")

    log.info("duplicates.bulk_dismissed", user_id=current_user.id,
             added=added, skipped=skipped)
    return {"status": "ok", "added": added, "skipped": skipped}


@router.post("/api/duplicates/undismiss-bulk")
async def undismiss_issues_bulk(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Restore multiple dismissed issues.

    Payload: {ids: [1, 2, 3]}  — DismissedIssue row IDs.
    """
    from app.models.schema import DismissedIssue

    ids = payload.get("ids") or []
    if not ids or len(ids) > 500:
        raise HTTPException(400, "Supply 1-500 ids")

    rows = (await db.execute(
        select(DismissedIssue).where(
            DismissedIssue.user_id == current_user.id,
            DismissedIssue.id.in_([int(i) for i in ids]),
        )
    )).scalars().all()

    removed = len(rows)
    for row in rows:
        await db.delete(row)

    await db.commit()
    log.info("duplicates.bulk_undismissed", user_id=current_user.id, removed=removed)
    return {"status": "ok", "removed": removed}


@router.get("/api/duplicates/suggest-relink/{emby_id}")
async def suggest_relink(
    emby_id: str,
    _user: User = Depends(get_current_user),
):
    """Suggest correct provider IDs for a mis-matched duplicate.

    Looks up the item's title+year in TMDB and returns the best match's
    provider IDs so the relink dialog can pre-fill them.
    """
    from app.utils.tmdb_client import search_tmdb

    async with EmbyClient() as emby:
        item = await emby.get_item_safe(emby_id)
    if not item:
        raise HTTPException(404, "Item not found in Emby")

    title = item.get("Name", "")
    year = item.get("ProductionYear")
    item_type = item.get("Type", "")  # Movie | Series
    media_type = "movie" if item_type == "Movie" else "tv"

    if not title:
        return {"suggestions": []}

    results = await search_tmdb(title, media_type=media_type, year=year)
    top = (results or [])[:5]

    # Fetch external IDs concurrently instead of sequentially
    import asyncio
    from app.utils.tmdb_client import get_external_ids

    async def _safe_ext(tmdb_id):
        try:
            return await get_external_ids(tmdb_id, media_type)
        except Exception:
            return {}

    ext_results = await asyncio.gather(
        *[_safe_ext(r.get("id")) for r in top]
    )

    suggestions = []
    for r, ext in zip(top, ext_results):
        tmdb_id = r.get("id")
        r_title = r.get("title") or r.get("name") or ""
        r_year = (r.get("release_date") or r.get("first_air_date") or "")[:4]

        suggestions.append({
            "tmdb_id": str(tmdb_id) if tmdb_id else None,
            "imdb_id": ext.get("imdb_id"),
            "tvdb_id": str(ext["tvdb_id"]) if ext.get("tvdb_id") else None,
            "title": r_title,
            "year": r_year,
        })

    return {"suggestions": suggestions, "emby_title": title, "emby_year": year}


# ── History re-link (orphaned / missing metadata) ───────────────────────

@router.post("/api/duplicates/relink-history")
async def relink_history(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Repoint watch history rows at correct provider IDs.

    Orphaned and missing-metadata issues are the opposite case to a
    duplicate: the *Emby item* is fine (or gone), and it's the watch
    history rows that carry the wrong or missing IDs.  So this edits
    the DB rather than Emby.

    Payload: {
      scope: "movie" | "series"     — which history rows to target
      match_title: str              — required for series scope
      match_emby_id: str            — preferred selector for movie scope
      match_imdb_id: str            — fallback selector for movie scope
      imdb_id / tmdb_id             — new values to write
      new_series_name               — series scope only, for renames
    }
    """
    from app.models.schema import WatchHistory

    scope = (payload.get("scope") or "movie").strip()
    if scope not in ("movie", "series"):
        raise HTTPException(400, "scope must be 'movie' or 'series'")

    new_imdb = (payload.get("imdb_id") or "").strip() or None
    new_tmdb = (payload.get("tmdb_id") or "").strip() or None
    new_emby = (payload.get("new_emby_id") or "").strip() or None
    new_series_name = (payload.get("new_series_name") or "").strip() or None

    if new_imdb and not re.fullmatch(r"tt\d{7,10}", new_imdb):
        raise HTTPException(400, f"Invalid IMDB ID format: {new_imdb}")
    if new_tmdb and not new_tmdb.isdigit():
        raise HTTPException(400, f"Invalid TMDB ID: {new_tmdb}")
    if not (new_imdb or new_tmdb or new_emby or new_series_name):
        raise HTTPException(400, "Nothing to apply — supply an ID or a new series name")

    stmt = WatchHistory.__table__.update().where(
        WatchHistory.user_id == current_user.id,
    )

    if scope == "movie":
        stmt = stmt.where(WatchHistory.item_type == "movie")
        emby_id = (payload.get("match_emby_id") or "").strip()
        imdb_match = (payload.get("match_imdb_id") or "").strip()
        title_match = (payload.get("match_title") or "").strip()

        # The scan groups orphaned movies by (emby_id, imdb_id, title) but
        # then dedups the *display* down to one card per title.  Matching
        # on emby_id alone therefore updates only one of several row
        # groups, and the untouched siblings resurface on the next scan
        # looking like the re-link never saved.  OR the selectors together
        # so every row behind the card is updated in one go.
        selectors = []
        if emby_id:
            selectors.append(WatchHistory.emby_id == emby_id)
        if imdb_match:
            selectors.append(WatchHistory.imdb_id == imdb_match)
        if title_match:
            selectors.append(func.lower(WatchHistory.title) == title_match.lower())
        if not selectors:
            raise HTTPException(
                400, "Supply match_emby_id, match_imdb_id or match_title"
            )
        stmt = stmt.where(or_(*selectors))
    else:
        title_match = (payload.get("match_title") or "").strip()
        if not title_match:
            raise HTTPException(400, "match_title is required for series scope")
        stmt = stmt.where(
            WatchHistory.item_type == "episode",
            func.lower(WatchHistory.series_name) == title_match.lower(),
        )

    values: dict = {}
    if new_imdb:
        values["imdb_id"] = new_imdb
    if new_tmdb:
        values["tmdb_id"] = new_tmdb
    if new_emby:
        values["emby_id"] = new_emby
    if scope == "series" and new_series_name:
        values["series_name"] = new_series_name

    result = await db.execute(stmt.values(**values))
    await db.commit()
    updated = result.rowcount or 0

    # ── Verify: will this actually clear the orphan? ────────────────────
    # Orphan status is decided by library membership, not by the IDs on
    # the history row.  Re-linking IDs for something genuinely deleted
    # from the library changes nothing about the finding, so say so
    # rather than letting the user re-link repeatedly in confusion.
    in_library = False
    try:
        if new_imdb:
            in_library = bool(await LibraryCache.find_by_provider_id("Imdb", new_imdb))
        if not in_library and new_tmdb:
            in_library = bool(await LibraryCache.find_by_provider_id("Tmdb", new_tmdb))
        if not in_library and new_emby:
            async with EmbyClient() as _emby:
                in_library = bool(await _emby.get_item_safe(new_emby))
        if not in_library and scope == "series":
            _t = (payload.get("new_series_name") or payload.get("match_title") or "")
            if _t:
                in_library = bool(await LibraryCache.find_by_title(_t))
    except Exception as e:
        log.warning("duplicates.relink_verify_failed", error=str(e)[:200])

    log.info("duplicates.history_relinked", user_id=current_user.id,
             scope=scope, updated=updated, values=values,
             in_library=in_library)

    return {
        "status": "ok",
        "scope": scope,
        "updated": updated,
        "applied": values,
        "in_library": in_library,
        # True when the rows were updated but the media still isn't in
        # the library — the issue will reappear, and dismissing is the
        # appropriate action
        "still_orphaned": bool(updated and not in_library),
    }


@router.get("/api/duplicates/library-search")
async def duplicates_library_search(
    q: str,
    item_type: str = "Movie",
    _user: User = Depends(get_current_user),
):
    """Search the Emby library so a history row can be pointed at a real item.

    The genuine fix for an orphan is usually that the media is still
    present but under a different Emby ID (re-added, re-imported, moved
    library).  Typing IDs by hand can't discover that — this can.
    """
    q = (q or "").strip()
    if len(q) < 2:
        raise HTTPException(400, "Search term must be at least 2 characters")

    if item_type not in ("Movie", "Series"):
        item_type = "Movie"

    async with EmbyClient() as emby:
        items = await emby.search_items(q, item_type=item_type)

    results = []
    for i in items[:15]:
        pids = i.get("ProviderIds") or {}
        results.append({
            "emby_id": i.get("Id"),
            "title": i.get("Name"),
            "year": i.get("ProductionYear"),
            "type": i.get("Type"),
            "imdb_id": pids.get("Imdb"),
            "tmdb_id": pids.get("Tmdb"),
        })

    return {"results": results, "total": len(results)}

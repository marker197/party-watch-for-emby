"""Routes extracted from routes.py — webhook_routes.py."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import QueueItem, Universe, User
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.simkl_client import SimklClient
from app.api.route_helpers import _activity_log, _get_active_providers, _get_mdblist_key, _maybe_notify
from app.services.scrobble_audit.service import ScrobbleAuditService
from app.services.smart_queue.service import SmartQueueService

log = structlog.get_logger()

router = APIRouter()

scrobble_audit_svc = ScrobbleAuditService()
smart_queue_svc = SmartQueueService()


@router.post("/webhook/sonarr")
@router.post("/webhook/sonarr/")
async def sonarr_webhook(request: Request):
    """Receive Sonarr webhooks for import/grab/series events.

    On 'Download' (import complete):
      - Stores imported episode info in Redis keyed by TVDB ID + SxxExx
      - Airing Soon card reads this to show 'Imported' badge instead of 'In Sonarr'

    On 'Grab':
      - Logs the grab event to the activity log

    On 'SeriesAdd':
      - Logs the new series event
    """
    import json as _json

    try:
        payload = await request.json()
    except Exception:
        return {"status": "error", "message": "invalid JSON"}

    event_type = payload.get("eventType", "")
    series = payload.get("series", {})
    episodes = payload.get("episodes", [])

    series_title = series.get("title", "Unknown")
    tvdb_id = series.get("tvdbId")

    log.info("webhook.sonarr", event_type=event_type, series=series_title,
             tvdb_id=tvdb_id, episodes=len(episodes))

    if event_type == "Test":
        await _activity_log(f"📡 Sonarr test webhook received", category="webhook")
        return {"status": "ok", "event": "Test"}

    if event_type == "Download":
        # Import complete — store each imported episode in Redis
        r = await get_redis()
        imported_count = 0
        for ep in episodes:
            s_num = ep.get("seasonNumber", 0)
            e_num = ep.get("episodeNumber", 0)
            ep_title = ep.get("title", "")

            if tvdb_id and s_num and e_num:
                # Key format: sonarr_imported:{tvdb_id}:S{s}E{e}
                redis_key = f"sonarr_imported:{tvdb_id}:S{s_num}E{e_num}"
                import_data = _json.dumps({
                    "series": series_title,
                    "season": s_num,
                    "episode": e_num,
                    "episode_title": ep_title,
                    "quality": ep.get("quality", ""),
                    "imported_at": datetime.now(timezone.utc).isoformat(),
                })
                # TTL 30 days — airing soon card only shows ~30 days ahead
                await r.setex(redis_key, 30 * 86400, import_data)
                imported_count += 1

                await _activity_log(
                    f"📥 Sonarr imported: {series_title} S{s_num:02d}E{e_num:02d}"
                    + (f" — {ep_title}" if ep_title else ""),
                    category="webhook",
                )

        log.info("webhook.sonarr_import_stored", series=series_title,
                 tvdb_id=tvdb_id, episodes_imported=imported_count)
        return {"status": "ok", "event": event_type, "imported": imported_count}

    if event_type == "Grab":
        ep_list = ", ".join(
            f"S{ep.get('seasonNumber', 0):02d}E{ep.get('episodeNumber', 0):02d}"
            for ep in episodes
        )
        await _activity_log(
            f"🎣 Sonarr grabbed: {series_title} {ep_list}",
            category="webhook",
        )
        return {"status": "ok", "event": event_type}

    if event_type == "SeriesAdd":
        await _activity_log(
            f"📺 Sonarr series added: {series_title}",
            category="webhook",
        )
        return {"status": "ok", "event": event_type}

    # Any other event — just log it
    await _activity_log(
        f"📡 Sonarr webhook: {event_type} — {series_title}",
        category="webhook",
    )
    return {"status": "ok", "event": event_type}


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/webhook/emby")
@router.post("/")
async def emby_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """Receive Emby webhooks for real-time events.

    Emby sends webhooks as either:
      - application/json (raw JSON body)
      - multipart/form-data or form-urlencoded with a 'data' field containing JSON

    Also registered at POST / as a fallback since Emby may be configured
    with just the root URL.

    Event types:
      - PlaybackStart: user started playing an item
      - PlaybackStop: user stopped playing an item
      - ItemMarkedPlayed: user marked item as watched

    On PlaybackStop / ItemMarkedPlayed:
      1. Record feedback for Smart Queue scoring
      2. Scrobble to Simkl watch history (if user has linked Simkl account)
    """
    import json as _json

    # Parse payload from whatever format Emby sends
    content_type = request.headers.get("content-type", "")
    payload = {}

    try:
        if "application/json" in content_type:
            payload = await request.json()
        elif "form" in content_type or "multipart" in content_type:
            form = await request.form()
            raw = form.get("data", "{}")
            payload = _json.loads(raw) if isinstance(raw, str) else {}
        else:
            # Try JSON first, fall back to reading body as text
            body = await request.body()
            if body:
                try:
                    payload = _json.loads(body)
                except (ValueError, _json.JSONDecodeError):
                    payload = {}
    except Exception:
        return {"status": "ignored", "reason": "unparseable_body"}

    if not payload:
        return {"status": "ignored", "reason": "empty_payload"}

    # Emby uses "Event" (not "EventType") with lowercase dot-notation
    # e.g. "playback.stop", "item.markplayed", "system.webhooktest"
    event_type = payload.get("Event", "") or payload.get("EventType", "")
    item_data = payload.get("Item", {})
    user_data = payload.get("User", {})
    session_data = payload.get("Session", {})

    item_name = item_data.get("Name", "")
    item_type_raw = item_data.get("Type", "")
    emby_item_id = item_data.get("Id", "")
    emby_user_id = user_data.get("Id", "")
    emby_username = user_data.get("Name", "")

    # Unified display name for activity logs:
    #   Movies: "Movie Title"
    #   Episodes: "Series Name : Episode Name : S1E1"
    if item_type_raw == "Episode":
        _sn = item_data.get("SeriesName", "")
        _snum = item_data.get("ParentIndexNumber", "")
        _enum = item_data.get("IndexNumber", "")
        _ep_tag = f"S{_snum}E{_enum}" if _snum and _enum else ""
        parts = [p for p in (_sn, item_name, _ep_tag) if p]
        display_name = " : ".join(parts) if parts else item_name
    else:
        display_name = item_name

    # Test webhooks and events without an item are acknowledged but not processed
    if not emby_item_id:
        return {"status": "ok", "event": event_type, "note": "no item data"}

    # Library-level events (library.new, item.added, item.removed) don't require a user
    event_lower = event_type.lower()
    is_library_event = event_lower in ("library.new", "librarynew",
                                        "item.added", "itemadded")
    is_library_removed = event_lower in ("library.deleted", "librarydeleted",
                                          "item.removed", "itemremoved")

    if not emby_user_id and not is_library_event and not is_library_removed:
        return {"status": "ok", "event": event_type, "note": "no user data"}

    # Find our user (may be None for library events)
    user = None
    if emby_user_id:
        user = (await db.execute(
            select(User).where(User.emby_user_id == emby_user_id)
        )).scalar_one_or_none()

    if not user and not is_library_event and not is_library_removed:
        await _activity_log(
            f"Webhook ignored: unknown Emby user {emby_username} ({emby_user_id})",
            category="webhook",
        )
        return {"status": "ignored", "reason": "unknown_user"}

    simkl_synced = False

    # -- Helper: build a Simkl client with auto-refresh for this user ---------
    async def _get_simkl_client():
        return SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    # -- Helper: build Simkl scrobble payload from webhook item data ----------
    async def _build_scrobble_payload():
        provider_ids = item_data.get("ProviderIds", {})
        simkl_ids = {}
        if provider_ids.get("Imdb"):
            simkl_ids["imdb"] = provider_ids["Imdb"]
        if provider_ids.get("Tmdb"):
            simkl_ids["tmdb"] = int(provider_ids["Tmdb"])
        if provider_ids.get("Tvdb"):
            simkl_ids["tvdb"] = int(provider_ids["Tvdb"])
        if not simkl_ids:
            return None

        if item_type_raw == "Movie":
            return {"movie": {"ids": simkl_ids}}
        elif item_type_raw == "Episode":
            # Try to get series-level provider IDs from the webhook payload
            series_ids = {}
            series_provider = item_data.get("SeriesProviderIds", {})
            if series_provider.get("Imdb"):
                series_ids["imdb"] = series_provider["Imdb"]
            if series_provider.get("Tmdb"):
                series_ids["tmdb"] = int(series_provider["Tmdb"])
            if series_provider.get("Tvdb"):
                series_ids["tvdb"] = int(series_provider["Tvdb"])

            # Fallback: resolve series IDs via library cache / Emby API
            if not series_ids:
                resolved = await _resolve_series_ids()
                if resolved.get("imdb"):
                    series_ids["imdb"] = resolved["imdb"]
                if resolved.get("tmdb"):
                    series_ids["tmdb"] = int(resolved["tmdb"])
                if resolved.get("tvdb"):
                    series_ids["tvdb"] = int(resolved["tvdb"])

            episode_obj = {
                "season": item_data.get("ParentIndexNumber", 1),
                "number": item_data.get("IndexNumber", 1),
            }

            if series_ids:
                return {"show": {"ids": series_ids}, "episode": episode_obj}
            else:
                # Last resort: episode-level IDs (may not work on all providers)
                episode_obj["ids"] = simkl_ids
                return {"episode": episode_obj}
        return None

    # -- Helper: get MDBList client for scrobble if enabled --------------------
    async def _get_mdblist_client_for_scrobble():
        """Build an MDBListClient using the stored API key, if MDBList is active."""
        providers = await _get_active_providers()
        if "mdblist" not in providers:
            return None
        key = await _get_mdblist_key()
        if not key:
            return None
        from app.utils.mdblist_client import MDBListClient
        return MDBListClient(api_key=key)

    # -- Helper: resolve series-level provider IDs for an episode ---------------
    async def _resolve_series_ids() -> dict:
        """Get series-level provider IDs for the current episode item.
        Three fallback levels:
          1. SeriesProviderIds from the webhook payload (fastest)
          2. Library cache lookup by SeriesName
          3. Emby API lookup by SeriesId (network call, last resort)
        Returns dict like {"imdb": "tt...", "tmdb": 12345, "tvdb": 67890} or {}.
        """
        # Level 1: SeriesProviderIds from webhook
        series_provider = item_data.get("SeriesProviderIds", {})
        result = {}
        for key in ("Imdb", "Tmdb", "Tvdb"):
            val = series_provider.get(key)
            if val:
                result[key.lower()] = int(val) if key != "Imdb" else val
        if result:
            return result

        # Level 2: Library cache by series name
        series_name = item_data.get("SeriesName", "")
        if series_name:
            cached = await LibraryCache.find_by_title(series_name, item_type="Series")
            if cached:
                cpids = cached.get("provider_ids", {})
                for key in ("Imdb", "Tmdb", "Tvdb"):
                    val = cpids.get(key)
                    if val:
                        result[key.lower()] = int(val) if key != "Imdb" else val
                if result:
                    log.debug("webhook.series_ids_from_cache", series=series_name, ids=result)
                    return result

        # Level 3: Emby API lookup by SeriesId
        series_emby_id = item_data.get("SeriesId")
        if series_emby_id:
            try:
                async with EmbyClient() as emby:
                    series_item = await emby.get_item_safe(series_emby_id)
                    if series_item:
                        spids = series_item.get("ProviderIds", {})
                        for key in ("Imdb", "Tmdb", "Tvdb"):
                            val = spids.get(key)
                            if val:
                                result[key.lower()] = int(val) if key != "Imdb" else val
                        if result:
                            log.debug("webhook.series_ids_from_emby", series=series_name,
                                      series_emby_id=series_emby_id, ids=result)
                            return result
            except Exception as e:
                log.debug("webhook.series_id_emby_lookup_failed",
                          series_emby_id=series_emby_id, error=str(e)[:80])

        return result

    # -- Helper: build MDBList scrobble payload --------------------------------
    async def _build_mdblist_scrobble_payload():
        """Build MDBList-compatible scrobble payload from webhook item data.
        Supports movies and TV episodes.
        Movie IDs accepted: imdb, tmdb, simkl, kitsu, mdblist (NOT tvdb).
        Show/episode IDs accepted: imdb, tmdb, simkl, tvdb, mdblist.
        Episode payload uses MDBList's nested format:
          {"show": {"ids": {...}, "season": {"number": N, "episode": {"number": M}}}}
        """
        provider_ids = item_data.get("ProviderIds", {})

        if item_type_raw == "Movie":
            mdb_ids = {}
            if provider_ids.get("Imdb"):
                mdb_ids["imdb"] = provider_ids["Imdb"]
            if provider_ids.get("Tmdb"):
                mdb_ids["tmdb"] = int(provider_ids["Tmdb"])
            # Note: tvdb is NOT supported by MDBList scrobble for movies
            if not mdb_ids:
                return None
            return {"movie": {"ids": mdb_ids}}

        elif item_type_raw == "Episode":
            show_ids = await _resolve_series_ids()

            if not show_ids:
                return None

            season_num = item_data.get("ParentIndexNumber", 1)
            episode_num = item_data.get("IndexNumber", 1)

            return {
                "show": {
                    "ids": show_ids,
                    "season": {
                        "number": season_num,
                        "episode": {"number": episode_num},
                    },
                },
            }

        return None

    # -- Helper: scrobble to MDBList (fire-and-forget, non-blocking) -----------
    async def _mdblist_scrobble(action: str, progress: float):
        """Send a scrobble event to MDBList if enabled. Never raises.
        Fires for movies and TV episodes.
        """
        try:
            mdb = await _get_mdblist_client_for_scrobble()
            if not mdb:
                return
            payload = await _build_mdblist_scrobble_payload()
            if not payload:
                return
            log.debug("webhook.mdblist_scrobble_payload",
                      action=action, progress=round(progress, 1),
                      payload=payload, item_type=item_type_raw)
            try:
                if action == "start":
                    await mdb.scrobble_start(payload, progress=progress)
                elif action == "pause":
                    await mdb.scrobble_pause(payload, progress=progress)
                elif action == "stop":
                    result = await mdb.scrobble_stop(payload, progress=progress)
                    return result
                elif action == "resume":
                    await mdb.scrobble_start(payload, progress=progress)
            finally:
                await mdb.close()
        except Exception as e:
            import re
            err_str = re.sub(r'apikey=[^&\s\'"]+', 'apikey=***', str(e)[:200])
            # Try to extract response body for 400 errors
            resp_body = ""
            status_code = ""
            if hasattr(e, 'response') and e.response is not None:
                try:
                    status_code = str(e.response.status_code)
                    resp_body = e.response.text[:200]
                except Exception:
                    pass
            log.warning(f"webhook.mdblist_scrobble_{action}_failed",
                        error=err_str, response_body=resp_body)
            # Include status + body in activity log so it's visible on dashboard
            detail = f" [{status_code}]" if status_code else ""
            if resp_body:
                detail += f" {resp_body[:120]}"
            await _activity_log(f"⚠ MDBList {action} failed: {display_name}{detail}", category="simkl")

    # -- Helper: add to MDBList watched history --------------------------------
    async def _mdblist_add_to_history():
        """Add item to MDBList watched history if enabled. Never raises."""
        try:
            mdb = await _get_mdblist_client_for_scrobble()
            if not mdb:
                return
            provider_ids = item_data.get("ProviderIds", {})
            ids: dict = {}
            if provider_ids.get("Imdb"):
                ids["imdb"] = provider_ids["Imdb"]
            if provider_ids.get("Tmdb"):
                ids["tmdb"] = int(provider_ids["Tmdb"])
            if provider_ids.get("Tvdb"):
                ids["tvdb"] = int(provider_ids["Tvdb"])
            if not ids:
                return
            watched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            try:
                if item_type_raw == "Movie":
                    await mdb.add_to_watched(
                        movies=[{"ids": ids, "watched_at": watched_at}],
                    )
                elif item_type_raw == "Episode":
                    series_ids = await _resolve_series_ids()
                    show_ids = series_ids or ids
                    season_num = item_data.get("ParentIndexNumber", 1)
                    episode_num = item_data.get("IndexNumber", 1)
                    await mdb.add_to_watched(
                        shows=[{
                            "ids": show_ids,
                            "seasons": [{"number": season_num, "episodes": [{"number": episode_num, "watched_at": watched_at}]}],
                        }],
                    )
                log.debug("webhook.mdblist_history_synced",
                         type=item_type_raw.lower(), title=display_name)
            finally:
                await mdb.close()
        except Exception as e:
            log.warning("webhook.mdblist_history_failed", error=str(e)[:120])

    # -- Helper: extract playback position ticks from webhook payload ---------
    def _get_position_ticks():
        """Emby sends position in various locations depending on event type."""
        # Try Session.PlayState.PositionTicks (most common)
        pos = session_data.get("PlayState", {}).get("PositionTicks", 0)
        if pos:
            return pos
        # Try root-level PlaybackPositionTicks
        pos = payload.get("PlaybackPositionTicks", 0)
        if pos:
            return pos
        # Try PlaybackInfo
        pos = payload.get("PlaybackInfo", {}).get("PositionTicks", 0)
        return pos

    # -- Helper: calculate playback progress as 0-100 -------------------------
    def _calc_progress():
        pos = _get_position_ticks()
        duration = item_data.get("RunTimeTicks", 0)
        if duration > 0 and pos > 0:
            return min(99.9, max(1.0, pos / duration * 100))
        # Simkl rejects progress < 1% with 422, so default to 1% minimum
        return 1.0

    # ── Match Emby event names ───────────────────────────────────────────────
    # Emby uses lowercase dot-notation (playback.start) but some builds use
    # PascalCase. Normalise to lowercase for matching (already set above).

    is_play_start = event_lower in ("playback.start", "playbackstart")
    is_play_stop = event_lower in ("playback.stop", "playbackstop")
    is_play_pause = event_lower in ("playback.pause", "playbackpause")
    is_play_unpause = event_lower in ("playback.unpause", "playbackunpause",
                                       "playback.resume", "playbackresume")
    is_mark_played = event_lower in ("item.markplayed", "item.markedplayed",
                                      "itemmarkplayed", "itemmarkedplayed")
    is_watched = is_play_stop or is_mark_played

    # ── Pause/unpause suppression ───────────────────────────────────────────
    # Three layers of dedup for pause/unpause events:
    #   1. Watch party seek: seek_all() fires pause→seek→resume per session
    #   2. Init burst: Emby fires rapid pause/unpause during playback start
    #      (buffering, player initialisation). Suppressed for 10s after start.
    #   3. Same-event debounce: duplicate pause or unpause for the same
    #      user+item within 5s is suppressed.
    if is_play_pause or is_play_unpause:
        session_id = session_data.get("Id", "")
        try:
            r = await get_redis()
            # Layer 1: watch party seek
            if session_id and await r.get(f"party_seek_suppress:{session_id}"):
                return {"status": "suppressed", "reason": "party_seek_in_progress"}
            # Layer 2: init burst (set by playback.start above)
            if user and await r.get(f"scrobble_init_suppress:{user.id}:{emby_item_id}"):
                return {"status": "suppressed", "reason": "init_burst"}
            # Layer 3: same-event debounce (5s window)
            if user:
                evt_key = "pause" if is_play_pause else "unpause"
                dedup_key = f"scrobble_dedup:{user.id}:{emby_item_id}:{evt_key}"
                if await r.get(dedup_key):
                    return {"status": "suppressed", "reason": "debounce"}
                await r.set(dedup_key, "1", ex=5)
        except Exception:
            pass

    # ── Helper: invalidate Continue Watching cache ─────────────────────────
    async def _invalidate_continue_watching():
        """Delete the Continue Watching Redis cache for this user so
        the next dashboard load fetches fresh data from Emby."""
        try:
            r = await get_redis()
            key = f"continue_watching_v2:{user.id}"
            deleted = await r.delete(key)
            if deleted:
                log.debug("webhook.continue_watching_cache_invalidated", user=user.id)
        except Exception:
            pass  # non-critical

    # ── playback.start → Simkl scrobble/start ("Watching…") ─────────────────
    if is_play_start:
        # Invalidate continue watching cache — a new resume point is being created
        await _invalidate_continue_watching()

        # Set init-burst suppression flag — Emby fires rapid pause/unpause
        # webhooks during playback initialisation (buffering, seeking).
        # Suppress those for 10 seconds after the start event.
        if user:
            try:
                r = await get_redis()
                await r.set(
                    f"scrobble_init_suppress:{user.id}:{emby_item_id}",
                    "1", ex=10,
                )
            except Exception:
                pass

        sync_ok = True
        if user.simkl_access_token:
            try:
                simkl = await _get_simkl_client()
                scrobble = await _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    await simkl.scrobble_start(scrobble, progress=progress)
                    simkl_synced = True
            except Exception as e:
                sync_ok = False
                log.warning("webhook.simkl_scrobble_start_failed", error=str(e))
        # MDBList scrobble start (background — errors logged separately)
        asyncio.create_task(_mdblist_scrobble("start", _calc_progress()))
        # One consolidated activity log line
        await _activity_log(
            f"Started Watching: {display_name}" + (" — Synced" if sync_ok else " — Sync error"),
            category="play-start",
        )
        return {"status": "received", "event": event_type, "simkl_synced": simkl_synced}

    # ── playback.pause → Simkl scrobble/pause ───────────────────────────────
    if is_play_pause:
        if user.simkl_access_token:
            progress = _calc_progress()
            # Simkl rejects pause at >80% progress (considers it watched).
            # Skip the scrobble — the stop event that follows will sync history.
            if progress <= 80:
                try:
                    simkl = await _get_simkl_client()
                    scrobble = await _build_scrobble_payload()
                    if scrobble:
                        await simkl.scrobble_pause(scrobble, progress=progress)
                        simkl_synced = True
                except Exception as e:
                    err_str = str(e)
                    if "422" not in err_str:
                        log.warning("webhook.simkl_scrobble_pause_failed", error=err_str)
        # MDBList scrobble pause (background)
        asyncio.create_task(_mdblist_scrobble("pause", _calc_progress()))
        # One consolidated activity log line
        await _activity_log(f"{display_name}: Paused", category="playback")
        return {"status": "received", "event": event_type, "simkl_synced": simkl_synced}

    # ── playback.unpause → Simkl scrobble/start (resume) ────────────────────
    if is_play_unpause:
        if user.simkl_access_token:
            try:
                simkl = await _get_simkl_client()
                scrobble = await _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    await simkl.scrobble_start(scrobble, progress=progress)
                    simkl_synced = True
            except Exception as e:
                log.warning("webhook.simkl_scrobble_resume_failed", error=str(e))
        # MDBList scrobble resume (background)
        asyncio.create_task(_mdblist_scrobble("resume", _calc_progress()))
        # One consolidated activity log line
        await _activity_log(f"{display_name}: Continued", category="playback")
        return {"status": "received", "event": event_type, "simkl_synced": simkl_synced}

    # ── playback.stop / item.markplayed → Simkl watch history ───────────────
    if is_watched:
        # Invalidate continue watching cache — item finished or resume point changed
        await _invalidate_continue_watching()

        # Extract playback duration from session data
        duration_ticks = session_data.get("PlayState", {}).get("PositionTicks", 0)

        # Record feedback for Smart Queue
        await smart_queue_svc.record_play(
            user_id=user.id,
            emby_item_id=emby_item_id,
            duration_ticks=duration_ticks,
        )

        # Remove watched item from queue and backfill with next best
        try:
            await smart_queue_svc.remove_and_backfill(
                user_id=user.id,
                emby_item_id=emby_item_id,
            )
        except Exception as e:
            log.warning("webhook.backfill_failed", error=str(e)[:120])

        # ── Send scrobble/stop to clear Simkl "watching" state ──────────
        # Only for actual playback stops (not manual mark-as-played).
        # If progress > 80%, Simkl auto-adds to history (action=scrobble)
        # and we skip the manual add_to_history to avoid duplicates.
        scrobble_already_added = False
        simkl_sync_error = ""
        if is_play_stop and user.simkl_access_token:
            try:
                simkl = await _get_simkl_client()
                scrobble = await _build_scrobble_payload()
                if scrobble:
                    progress = _calc_progress()
                    result = await simkl.scrobble_stop(scrobble, progress=progress)
                    action = result.get("action", "") if isinstance(result, dict) else ""
                    if action == "scrobble":
                        scrobble_already_added = True
                        simkl_synced = True
            except Exception as e:
                err_str = str(e)
                if "409" in err_str:
                    scrobble_already_added = True
                    simkl_synced = True
                elif "422" not in err_str:
                    log.warning("webhook.simkl_scrobble_stop_failed", error=err_str)
                    simkl_sync_error = err_str[:80]

        # Scrobble to Simkl watch history if user has a token
        if user.simkl_access_token and not scrobble_already_added:
            try:
                simkl = await _get_simkl_client()

                # Build Simkl item from provider IDs in the webhook payload
                provider_ids = item_data.get("ProviderIds", {})
                simkl_ids = {}
                if provider_ids.get("Imdb"):
                    simkl_ids["imdb"] = provider_ids["Imdb"]
                if provider_ids.get("Tmdb"):
                    simkl_ids["tmdb"] = int(provider_ids["Tmdb"])
                if provider_ids.get("Tvdb"):
                    simkl_ids["tvdb"] = int(provider_ids["Tvdb"])

                if simkl_ids:
                    watched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

                    if item_type_raw in ("Movie",):
                        history_item = {
                            "ids": simkl_ids,
                            "watched_at": watched_at,
                        }
                        await simkl.add_to_history([history_item])
                        simkl_synced = True
                        log.info("webhook.simkl_history_synced",
                                 type="movie", ids=simkl_ids, user=user.id)

                    elif item_type_raw in ("Episode",):
                        series_ids = await _resolve_series_ids()

                        episode = {
                            "watched_at": watched_at,
                            "ids": simkl_ids,
                        }
                        season_num = item_data.get("ParentIndexNumber")
                        episode_num = item_data.get("IndexNumber")
                        if season_num is not None:
                            episode["season"] = season_num
                        if episode_num is not None:
                            episode["number"] = episode_num

                        show_item = {
                            "_type": "show",
                            "ids": series_ids or simkl_ids,
                            "seasons": [{
                                "number": season_num or 1,
                                "episodes": [episode],
                            }],
                        }
                        await simkl.add_to_history([show_item])
                        simkl_synced = True
                        log.info("webhook.simkl_history_synced",
                                 type="episode", ids=series_ids or simkl_ids,
                                 ep_ids=simkl_ids, user=user.id)

            except Exception as e:
                log.error("webhook.simkl_sync_failed", error=str(e), user=user.id)
                simkl_sync_error = str(e)[:80]

            # Invalidate scrobble audit cache so newly synced items
            # don't appear as missed on the next audit view
            if simkl_synced:
                await scrobble_audit_svc.invalidate_cache(user.id)

        # ── MDBList: scrobble stop + history sync ─────────────────────────
        if is_play_stop:
            asyncio.create_task(_mdblist_scrobble("stop", _calc_progress()))
        # Always try MDBList history (independent of Simkl scrobble state)
        asyncio.create_task(_mdblist_add_to_history())

        # ── One consolidated activity log line ────────────────────────────
        if simkl_sync_error:
            await _activity_log(
                f"Stopped Watching: {display_name} — Sync error: {simkl_sync_error}",
                category="play-stop",
            )
        elif simkl_synced:
            await _activity_log(
                f"Stopped Watching: {display_name} — Synced",
                category="play-stop",
            )
        else:
            await _activity_log(
                f"Stopped Watching: {display_name}",
                category="play-stop",
            )

        # ── Persistent watch history (local DB) ──────────────────────────
        # Record every PlaybackStop regardless of progress (the history
        # page shows partial watches too, with a % badge).
        # ItemMarkPlayed is skipped: Emby fires it alongside PlaybackStop,
        # and the two arrive near-simultaneously causing duplicate rows.
        should_record = is_play_stop
        wh_progress = None
        if is_play_stop:
            try:
                wh_progress = int(_calc_progress())
            except Exception:
                wh_progress = None

        if should_record:
            try:
                from app.models.schema import WatchHistory
                from sqlalchemy import cast, Date as SADate
                provider_ids = item_data.get("ProviderIds", {})
                runtime_ticks = item_data.get("RunTimeTicks", 0) or 0
                runtime_min = int(runtime_ticks / 600_000_000) if runtime_ticks else None

                wh_item_type = "episode" if item_type_raw == "Episode" else "movie"
                wh_series = item_data.get("SeriesName") if item_type_raw == "Episode" else None
                wh_season = item_data.get("ParentIndexNumber") if item_type_raw == "Episode" else None
                wh_episode = item_data.get("IndexNumber") if item_type_raw == "Episode" else None

                # For episodes, get series-level provider IDs
                wh_imdb = provider_ids.get("Imdb", "") or ""
                wh_tmdb = str(provider_ids.get("Tmdb", "")) if provider_ids.get("Tmdb") else ""
                wh_tvdb = str(provider_ids.get("Tvdb", "")) if provider_ids.get("Tvdb") else ""
                wh_simkl = ""

                if item_type_raw == "Episode":
                    series_ids = await _resolve_series_ids()
                    wh_imdb = wh_imdb or str(series_ids.get("imdb", ""))
                    wh_tmdb = wh_tmdb or str(series_ids.get("tmdb", ""))
                    wh_tvdb = wh_tvdb or str(series_ids.get("tvdb", ""))

                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)

                # Genres: from item or its series (for episodes)
                wh_genres_list = item_data.get("Genres") or []
                if not wh_genres_list and item_type_raw == "Episode":
                    wh_genres_list = item_data.get("SeriesGenres") or []
                wh_genres = ",".join(wh_genres_list) if wh_genres_list else None

                # ── Same-day dedup: update existing row if this item
                #    was already recorded today, instead of inserting a
                #    duplicate.  One row per (user, item, calendar day).
                existing_today = None
                if emby_item_id:
                    existing_today = (await db.execute(
                        select(WatchHistory).where(
                            WatchHistory.user_id == user.id,
                            WatchHistory.emby_id == emby_item_id,
                            cast(WatchHistory.watched_at, SADate) == now_naive.date(),
                        )
                    )).scalar_one_or_none()

                if existing_today:
                    # Update timestamp and progress on the existing row
                    existing_today.watched_at = now_naive
                    if wh_progress is not None:
                        existing_today.progress = wh_progress
                    # Fill in any provider IDs that were missing
                    if not existing_today.imdb_id and (wh_imdb or None):
                        existing_today.imdb_id = wh_imdb or None
                    if not existing_today.tmdb_id and (wh_tmdb or None):
                        existing_today.tmdb_id = wh_tmdb or None
                    if not existing_today.tvdb_id and (wh_tvdb or None):
                        existing_today.tvdb_id = wh_tvdb or None
                    if not existing_today.genres and wh_genres:
                        existing_today.genres = wh_genres
                    await db.commit()
                    log.debug("webhook.watch_history_updated", user_id=user.id,
                              title=display_name, item_type=wh_item_type,
                              progress=wh_progress)
                else:
                    entry = WatchHistory(
                        user_id=user.id,
                        emby_id=emby_item_id,
                        item_type=wh_item_type,
                        title=item_name,
                        series_name=wh_series,
                        season_number=wh_season,
                        episode_number=wh_episode,
                        imdb_id=wh_imdb or None,
                        tmdb_id=wh_tmdb or None,
                        simkl_id=wh_simkl or None,
                        tvdb_id=wh_tvdb or None,
                        watched_at=now_naive,
                        runtime_minutes=runtime_min,
                        genres=wh_genres,
                        progress=wh_progress,
                        source="webhook",
                    )
                    db.add(entry)
                    await db.commit()
                    log.debug("webhook.watch_history_recorded", user_id=user.id,
                              title=display_name, item_type=wh_item_type,
                              progress=wh_progress)
                # Invalidate stats cache so next load reflects the new watch
                try:
                    _r = await get_redis()
                    await _r.delete(f"watch_stats_v5:{user.id}")
                except Exception:
                    pass
            except Exception as e:
                await db.rollback()
                # IntegrityError from unique constraint = duplicate, not an error
                if "uq_watch_history_user_item_time" in str(e):
                    log.debug("webhook.watch_history_duplicate", title=display_name)
                else:
                    log.warning("webhook.watch_history_failed", error=str(e)[:200])

    # ── library.new / item.added → check smart queue for missing items ─────
    if is_library_event and item_type_raw in ("Movie", "Episode", "Series"):
        try:
            # Extract provider IDs from the new item
            provider_ids = item_data.get("ProviderIds", {})
            tmdb_id = provider_ids.get("Tmdb")
            imdb_id = provider_ids.get("Imdb")
            tvdb_id = provider_ids.get("Tvdb")

            # Skip cache updates for unpack/extraction events (no real item yet)
            _is_unpack = "unpack" in item_name.lower() or "unpack" in display_name.lower() or "unpack" in (item_data.get("Path") or "").lower()

            # Immediately update library cache for Movies and Series
            # so all features (Library Health, Universe Discovery, etc.)
            # see the new item without waiting for the nightly rebuild.
            # Only when we have provider IDs (real item, not unpack stub).
            already_cached = True  # default: don't notify unless confirmed new
            has_provider_ids = any(provider_ids.get(k) for k in ("Tmdb", "Imdb", "Tvdb"))
            if item_type_raw in ("Movie", "Series") and emby_item_id and has_provider_ids and not _is_unpack:
                try:
                    cache_type = "movie" if item_type_raw == "Movie" else "series"
                    # Check if already cached (avoid double-counting on duplicate webhooks)
                    already_cached = False
                    for _pid_type in ("Tmdb", "Imdb", "Tvdb"):
                        _pid_val = provider_ids.get(_pid_type)
                        if _pid_val:
                            existing = await LibraryCache.find_by_provider_id(_pid_type, str(_pid_val))
                            if existing:
                                already_cached = True
                                break
                    await LibraryCache._cache_item(item_data, item_type=cache_type)
                    if not already_cached:
                        _r = await get_redis()
                        stat_key = f"library::stat:{'movies' if item_type_raw == 'Movie' else 'series'}"
                        await _r.incr(stat_key)
                        # Bump version so dashboard knows to refresh library counts
                        await _r.incr("library::stat:version")
                    log.info("webhook.library_cache_updated",
                             title=item_name, type=cache_type,
                             emby_id=emby_item_id, new=not already_cached)
                except Exception as _ce:
                    log.debug("webhook.library_cache_update_failed",
                              error=str(_ce)[:120])

            # For episodes, also extract the series-level IDs from SeriesId
            # so we can match the queue item (which tracks the series, not
            # individual episodes)
            series_provider_ids = {}
            series_emby_id = item_data.get("SeriesId")
            if item_type_raw == "Episode" and series_emby_id:
                try:
                    emby = EmbyClient()
                    series_item = await emby.get_items_by_ids([series_emby_id])
                    if series_item:
                        series_provider_ids = series_item[0].get("ProviderIds", {})
                except Exception:
                    log.debug("webhook.series_lookup_failed", series_id=series_emby_id)
                finally:
                    try:
                        await emby.close()
                    except Exception:
                        pass

            # Determine which IDs and queue item_type to match
            if item_type_raw == "Movie":
                match_type = "movie"
                match_ids = {"tmdb": tmdb_id, "imdb": imdb_id}
                resolved_emby_id = emby_item_id
            else:
                # Episode or Series → match against show queue items
                match_type = "show"
                if item_type_raw == "Episode" and series_provider_ids:
                    match_ids = {
                        "tmdb": series_provider_ids.get("Tmdb"),
                        "imdb": series_provider_ids.get("Imdb"),
                        "tvdb": series_provider_ids.get("Tvdb"),
                    }
                    resolved_emby_id = series_emby_id or emby_item_id
                elif item_type_raw == "Series":
                    match_ids = {"tmdb": tmdb_id, "imdb": imdb_id, "tvdb": tvdb_id}
                    resolved_emby_id = emby_item_id
                else:
                    # Episode without series lookup fallback
                    match_ids = {"tmdb": tmdb_id, "imdb": imdb_id, "tvdb": tvdb_id}
                    resolved_emby_id = emby_item_id

            has_ids = any(v for v in match_ids.values())

            if has_ids:
                # Find any missing queue items that match
                missing_items = (await db.execute(
                    select(QueueItem).where(
                        QueueItem.in_library == False,
                        QueueItem.item_type == match_type,
                    )
                )).scalars().all()

                promoted = 0
                for qi in missing_items:
                    meta = qi.metadata_json or {}
                    ids = meta.get("ids", {})
                    match = False
                    for id_key, id_val in match_ids.items():
                        if id_val and str(ids.get(id_key, "")) == str(id_val):
                            match = True
                            break

                    if match:
                        qi.emby_item_id = resolved_emby_id
                        qi.in_library = True
                        promoted += 1
                        log.info("webhook.queue_item_promoted",
                                 title=qi.title, emby_id=resolved_emby_id,
                                 item_type=match_type)

                if promoted:
                    await db.commit()
                    # Invalidate availability cache — the item just arrived
                    try:
                        _r = await get_redis()
                        await _r.delete("availability_monitor_v2")
                    except Exception:
                        pass
                    # Re-sync playlist for each affected user
                    affected_users = {qi.user_id for qi in missing_items if qi.in_library}
                    for uid in affected_users:
                        try:
                            await smart_queue_svc._resync_playlist_from_db(uid)
                        except Exception:
                            log.warning("webhook.playlist_resync_failed", user_id=uid)

                    if not _is_unpack:
                        _lib_cat = "library-movie" if item_type_raw == "Movie" else "library-episode"
                        await _activity_log(
                            f"📥 Library added: {display_name} — promoted {promoted} queue item(s) to in-library",
                            category=_lib_cat,
                        )
                else:
                    if not _is_unpack:
                        _lib_cat = "library-movie" if item_type_raw == "Movie" else "library-episode"
                        await _activity_log(
                            f"📥 Library added: {display_name} ({item_type_raw}) — not in smart queue",
                            category=_lib_cat,
                        )
            else:
                if not _is_unpack:
                    _lib_cat = "library-movie" if item_type_raw == "Movie" else "library-episode"
                    await _activity_log(
                        f"📥 Library added: {display_name} ({item_type_raw}) — no provider IDs to match",
                        category=_lib_cat,
                    )

            # ── Notify on any new library item ────────────────────────────
            # Fire for every library.new/item.added event regardless of
            # whether the item was already in the cache (covers quality
            # upgrades, re-downloads, and direct Radarr/Sonarr imports).
            # Short-lived Redis dedup key prevents duplicate notifications
            # when Emby fires multiple webhooks for the same item.
            if not _is_unpack:
                _notify_dedup_key = f"notify_dedup:library:{emby_item_id}"
                try:
                    _r = await get_redis()
                    _already_notified = await _r.get(_notify_dedup_key)
                    if not _already_notified:
                        await _r.set(_notify_dedup_key, "1", ex=60)
                        from app.utils.notification_client import notify
                        notify("download", "📥 New Arrival", display_name or "Unknown")
                except Exception:
                    # Redis unavailable — send anyway, risk of duplicate is minor
                    from app.utils.notification_client import notify
                    notify("download", "📥 New Arrival", display_name or "Unknown")

            # ── Update Recently Arrived from webhook ──────────────────────
            # Check if this item was in the pending snapshot and surface it
            # as arrived immediately, rather than waiting for the next poll.
            try:
                import json as _json
                _r = await get_redis()
                raw_prev = await _r.get("recently_arrived_pending_v1")
                if raw_prev:
                    prev = _json.loads(raw_prev)
                    arrived_item = None

                    if item_type_raw == "Movie" and tmdb_id:
                        prev_movie_ids = {str(m) for m in prev.get("movies", [])}
                        if str(tmdb_id) in prev_movie_ids:
                            arrived_item = {
                                "title": item_name,
                                "year": item_data.get("ProductionYear"),
                                "tmdb_id": tmdb_id,
                                "type": "movie",
                                "id": tmdb_id,
                                "arrived_at": datetime.now(timezone.utc).isoformat() + "Z",
                            }
                    elif item_type_raw in ("Series", "Episode") and tvdb_id:
                        series_tvdb = tvdb_id
                        if item_type_raw == "Episode" and series_provider_ids:
                            series_tvdb = series_provider_ids.get("Tvdb") or tvdb_id
                        prev_show_eps = {}
                        for s in prev.get("shows", []):
                            if isinstance(s, dict):
                                prev_show_eps[str(s.get("id", ""))] = s.get("eps", 0)
                        if str(series_tvdb) in prev_show_eps:
                            series_name = item_data.get("SeriesName") or item_name
                            arrived_item = {
                                "title": series_name,
                                "year": item_data.get("ProductionYear"),
                                "tvdb_id": series_tvdb,
                                "type": "show",
                                "id": series_tvdb,
                                "new_episodes": 1,
                                "arrived_at": datetime.now(timezone.utc).isoformat() + "Z",
                            }

                    if arrived_item:
                        # Append to arrived items list (dedup by type+id)
                        arrived_key = "recently_arrived_items_v1"
                        raw_arr = await _r.get(arrived_key)
                        existing = _json.loads(raw_arr) if raw_arr else []
                        existing_ids = {(i.get("type"), str(i.get("id", ""))) for i in existing}
                        item_key = (arrived_item["type"], str(arrived_item["id"]))

                        if item_key not in existing_ids:
                            existing.append(arrived_item)
                            await _r.setex(arrived_key, 86400 * 2, _json.dumps(existing))
                            log.info("webhook.recently_arrived_added",
                                     title=arrived_item["title"],
                                     type=arrived_item["type"])

                        # Clear the result cache so the dashboard picks it up
                        await _r.delete("recently_arrived_result_v1")

            except Exception as e:
                log.debug("webhook.recently_arrived_update_failed",
                          error=str(e)[:120])

        except Exception as e:
            log.warning("webhook.item_added_handler_failed", error=str(e)[:120])

        return {"status": "received", "event": event_type}

    # ── library.deleted / item.removed → remove from Simkl watchlist ─────
    if is_library_removed and item_type_raw in ("Movie", "Series"):
        _is_unpack_rm = "unpack" in item_name.lower() or "unpack" in display_name.lower() or "unpack" in (item_data.get("Path") or "").lower()
        if _is_unpack_rm:
            return {"status": "ignored", "event": event_type, "note": "unpack stub removal"}
        try:
            provider_ids = item_data.get("ProviderIds", {})
            tmdb_id = provider_ids.get("Tmdb")
            imdb_id = provider_ids.get("Imdb")
            tvdb_id = provider_ids.get("Tvdb")

            if tmdb_id or imdb_id or tvdb_id:
                # Remove from Simkl watchlist for all linked users
                async with async_session_ctx() as _db:
                    linked_users = (await _db.execute(
                        select(User).where(User.simkl_access_token.isnot(None))
                    )).scalars().all()

                removed_for: list[str] = []
                for lu in linked_users:
                    simkl = None
                    try:
                        simkl = SimklClient(
                            access_token=lu.simkl_access_token,
                            token_expires=lu.simkl_token_expires,
                        )

                        if item_type_raw == "Movie":
                            ids = {}
                            if tmdb_id:
                                ids["tmdb"] = int(tmdb_id)
                            if imdb_id:
                                ids["imdb"] = imdb_id
                            result = await simkl.remove_from_watchlist(
                                [{"ids": ids}]
                            )
                            deleted = (result.get("deleted") or {}).get("movies", 0)
                        else:
                            ids = {}
                            if tvdb_id:
                                ids["tvdb"] = int(tvdb_id)
                            if imdb_id:
                                ids["imdb"] = imdb_id
                            result = await simkl.remove_from_watchlist(
                                [{"ids": ids}]
                            )
                            deleted = (result.get("deleted") or {}).get("shows", 0)

                        if deleted:
                            removed_for.append(lu.emby_username or str(lu.id))
                            log.info("webhook.simkl_watchlist_removed",
                                     title=item_name, user=lu.id, deleted=deleted)
                    except Exception as e:
                        log.debug("webhook.simkl_watchlist_remove_failed",
                                  user=lu.id, error=str(e)[:120])
                    finally:
                        if simkl:
                            await simkl.close()

                if removed_for:
                    await _activity_log(
                        f"🗑️ Library removed: {display_name} — removed from Simkl watchlist for {', '.join(removed_for)}",
                        category="simkl",
                    )
                else:
                    await _activity_log(
                        f"🗑️ Library removed: {display_name} — not on any user's Simkl watchlist",
                        category="library",
                    )
            else:
                await _activity_log(
                    f"🗑️ Library removed: {display_name} ({item_type_raw}) — no provider IDs",
                    category="library",
                )
        except Exception as e:
            log.warning("webhook.item_removed_handler_failed", error=str(e)[:120])

        return {"status": "received", "event": event_type}

    if not is_watched and not is_library_removed:
        # Unmatched event — log for debugging
        await _activity_log(
            f"📡 Unhandled webhook: {event_type} — {display_name}",
            category="webhook",
        )

    return {"status": "received", "event": event_type, "simkl_synced": simkl_synced}


# -- Activity log (Redis-backed, last 100 entries) ---------------------------

# ═══════════════════════════════════════════════════════════════════════════

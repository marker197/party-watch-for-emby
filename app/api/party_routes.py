"""Routes extracted from routes.py — party_routes.py."""

from __future__ import annotations

import asyncio

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import QueueItem, User, WatchParty, WatchPartyParticipant
from app.utils.database import async_session as async_session_ctx, get_db
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.security.auth import get_current_user
from app.services.watch_party.service import WatchPartyService

log = structlog.get_logger()

router = APIRouter()

watch_party_svc = WatchPartyService()


class CreatePartyRequest(BaseModel):
    host_user_id: int
    emby_item_id: str | None = None

class JoinPartyRequest(BaseModel):
    code: str
    user_id: int


class CreatePartyRequest(BaseModel):
    host_user_id: int
    emby_item_id: str | None = None


class JoinPartyRequest(BaseModel):
    code: str
    user_id: int


@router.post("/party/create")
async def create_party(body: CreatePartyRequest, _user: User = Depends(get_current_user)):
    try:
        return await watch_party_svc.create_party(body.host_user_id, body.emby_item_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/party/join")
async def join_party(body: JoinPartyRequest, _user: User = Depends(get_current_user)):
    result = await watch_party_svc.join_party(body.code, body.user_id)
    if not result:
        raise HTTPException(404, "Party not found or has ended")
    return result


@router.post("/party/{code}/end")
async def end_party(code: str, _user: User = Depends(get_current_user)):
    await watch_party_svc.end_party(code)
    return {"status": "ended"}


@router.post("/party/{code}/start")
async def start_party_playback(code: str, _user: User = Depends(get_current_user)):
    """Start playback on all participants' Emby sessions simultaneously."""
    return await watch_party_svc.start_playback(code)


@router.get("/party/{code}/sessions")
async def list_party_sessions(code: str):
    """List active Emby sessions for party participants (device picker)."""
    return await watch_party_svc.list_sessions_for_party(code)


@router.post("/party/{code}/start-selected")
async def start_selected_playback(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Start playback on specific devices only.

    Payload: {"session_ids": ["sid1", "sid2"], "emby_item_id": "optional_override",
              "start_position_ticks": 0}
    """
    session_ids = payload.get("session_ids", [])
    item_id = payload.get("emby_item_id")
    start_ticks = int(payload.get("start_position_ticks", 0))
    if not session_ids:
        raise HTTPException(400, "No sessions selected")
    return await watch_party_svc.start_playback_on_sessions(
        code, session_ids, item_id, start_position_ticks=start_ticks,
    )


@router.post("/party/{code}/pause")
async def pause_party_playback(code: str, _user: User = Depends(get_current_user)):
    """Toggle pause/play on all participants' Emby sessions."""
    return await watch_party_svc.pause_all(code)


@router.post("/party/{code}/seek")
async def seek_party_playback(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Seek all participants to a specific position.

    Payload: {"position_ticks": int}
    """
    position_ticks = payload.get("position_ticks", 0)
    return await watch_party_svc.seek_all(code, position_ticks)


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/party/{code}/pick-together")
async def start_pick_together(code: str, payload: dict = None, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Start a Pick Together voting round for a watch party.

    Pulls top candidates from each participant's smart queue (in-library only),
    dedupes, and stores the voting state in Redis.  Emits pick_started to all
    members via Socket.IO.

    Optional payload: {"candidate_count": 8}
    """
    from app.services.watch_party.service import sio

    r = await get_redis()
    state = await r.hgetall(f"party:{code}")
    if not state:
        raise HTTPException(404, "Party not found")

    party_id = int(state["id"])
    candidate_count = (payload or {}).get("candidate_count", 8)
    if candidate_count < 4:
        candidate_count = 4
    if candidate_count > 20:
        candidate_count = 20

    # Collect participant user IDs
    participants = (await db.execute(
        select(WatchPartyParticipant.user_id)
        .where(WatchPartyParticipant.party_id == party_id)
    )).scalars().all()

    if not participants:
        raise HTTPException(400, "No participants in party")

    # Pull top queue items from each participant (in-library only, unplayed)
    seen_titles: set[str] = set()
    candidates: list[dict] = []

    for uid in participants:
        items = (await db.execute(
            select(QueueItem)
            .where(
                QueueItem.user_id == uid,
                QueueItem.played == False,
                QueueItem.in_library == True,
                QueueItem.emby_item_id.isnot(None),
            )
            .order_by(QueueItem.score.desc())
            .limit(candidate_count * 2)  # over-fetch to account for dedup
        )).scalars().all()

        for item in items:
            key = (item.title or "").lower().strip()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            candidates.append({
                "emby_item_id": item.emby_item_id,
                "title": item.title,
                "type": item.item_type,
                "year": (item.metadata_json or {}).get("year"),
                "score": round(item.score, 2) if item.score else 0,
                "source": item.source,
                "votes": 0,
                "voters": [],
            })

    # Sort by score descending, take top N
    candidates.sort(key=lambda c: c["score"], reverse=True)
    candidates = candidates[:candidate_count]

    if not candidates:
        raise HTTPException(400, "No queue items available — run a queue refresh first")

    # Store voting state in Redis
    import json as _json
    pick_state = {
        "candidates": candidates,
        "phase": "voting",  # voting | countdown | done
        "winner_idx": None,
    }
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Broadcast to room
    await sio.emit("pick_started", {"candidates": candidates}, room=code)

    log.info("theater_mode.pick_started", code=code,
             candidates=len(candidates), participants=len(participants))
    return {"status": "ok", "candidates": candidates}


@router.get("/party/{code}/pick-status")
async def get_pick_status(code: str):
    """Get current Pick Together voting state."""
    import json as _json
    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        return {"status": "none"}
    return _json.loads(raw)


@router.post("/party/{code}/vote")
async def cast_vote(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Cast a vote for a candidate.

    Payload: {"candidate_idx": 0, "user_id": 1}
    """
    import json as _json
    from app.services.watch_party.service import sio

    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        raise HTTPException(404, "No active Pick Together session")

    pick_state = _json.loads(raw)
    if pick_state.get("phase") != "voting":
        raise HTTPException(400, "Voting is not active")

    idx = payload.get("candidate_idx")
    user_id = payload.get("user_id")
    if idx is None or not isinstance(idx, int):
        raise HTTPException(400, "candidate_idx required")

    candidates = pick_state["candidates"]
    if idx < 0 or idx >= len(candidates):
        raise HTTPException(400, "Invalid candidate index")

    # Remove previous vote by this user (one vote per user)
    for c in candidates:
        if user_id in c.get("voters", []):
            c["voters"].remove(user_id)
            c["votes"] = len(c["voters"])

    # Cast new vote
    candidates[idx].setdefault("voters", []).append(user_id)
    candidates[idx]["votes"] = len(candidates[idx]["voters"])

    pick_state["candidates"] = candidates
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Broadcast vote update (strip voter IDs for privacy, just send counts)
    vote_summary = [{"title": c["title"], "votes": c["votes"], "idx": i}
                    for i, c in enumerate(candidates)]
    await sio.emit("vote_update", {"votes": vote_summary}, room=code)

    return {"status": "ok", "votes": vote_summary}


@router.post("/party/{code}/pick-winner")
async def confirm_pick_winner(code: str, payload: dict = None, _user: User = Depends(get_current_user)):
    """Host confirms the winner (auto-selects top vote, or override).

    Payload: {"winner_idx": 0}  (optional — defaults to highest vote)
    Sets the party item to the winner. Host then uses device picker + start.
    """
    import json as _json
    from app.services.watch_party.service import sio

    r = await get_redis()
    raw = await r.get(f"party_pick:{code}")
    if not raw:
        raise HTTPException(404, "No active Pick Together session")

    pick_state = _json.loads(raw)
    candidates = pick_state["candidates"]

    # Determine winner
    winner_idx = (payload or {}).get("winner_idx")
    if winner_idx is None:
        # Auto-pick highest votes, tie-break by score
        best_idx = 0
        best_votes = candidates[0].get("votes", 0)
        best_score = candidates[0].get("score", 0)
        for i, c in enumerate(candidates):
            v = c.get("votes", 0)
            s = c.get("score", 0)
            if v > best_votes or (v == best_votes and s > best_score):
                best_idx = i
                best_votes = v
                best_score = s
        winner_idx = best_idx

    if winner_idx < 0 or winner_idx >= len(candidates):
        raise HTTPException(400, "Invalid winner index")

    winner = candidates[winner_idx]
    pick_state["phase"] = "done"
    pick_state["winner_idx"] = winner_idx
    await r.set(f"party_pick:{code}", _json.dumps(pick_state), ex=3600)

    # Update party item to the winner
    emby_item_id = winner.get("emby_item_id", "")
    winner_title = winner.get("title", "")
    display_title = f"Pick Together Lobby - {winner_title}" if winner_title else "Pick Together Lobby"
    if emby_item_id:
        await r.hset(f"party:{code}", mapping={
            "item": emby_item_id,
            "title": display_title,
        })

    # Update DB record so recent parties list shows the item played
    state = await r.hgetall(f"party:{code}")
    party_id = int(state.get("id", 0))
    if party_id:
        async with async_session_ctx() as db_sess:
            from sqlalchemy import update as sa_update
            await db_sess.execute(
                sa_update(WatchParty)
                .where(WatchParty.id == party_id)
                .values(title=display_title, emby_item_id=emby_item_id)
            )
            await db_sess.commit()

    # Broadcast winner — no countdown here, host uses device picker next
    await sio.emit("pick_winner", {
        "winner": winner,
        "winner_idx": winner_idx,
    }, room=code)

    log.info("theater_mode.winner_selected", code=code,
             title=winner.get("title"), votes=winner.get("votes", 0))
    return {"status": "ok", "winner": winner}


@router.post("/party/{code}/start-with-countdown")
async def start_with_countdown(code: str, payload: dict, _user: User = Depends(get_current_user)):
    """Start playback on selected devices after a 20-second countdown.

    Payload: {"session_ids": ["sid1", "sid2"]}
    Broadcasts countdown to all party members via Socket.IO, then starts
    playback on the specified sessions.
    """
    import asyncio
    from app.services.watch_party.service import sio

    session_ids = payload.get("session_ids", [])
    if not session_ids:
        raise HTTPException(400, "No sessions selected")

    r = await get_redis()
    state = await r.hgetall(f"party:{code}")
    if not state:
        raise HTTPException(404, "Party not found")

    item_id = state.get("item", "")
    title = state.get("title", "")

    # Broadcast countdown start to all members
    await sio.emit("countdown_started", {
        "title": title,
        "countdown_seconds": 20,
    }, room=code)

    async def _countdown_then_play():
        for remaining in range(19, -1, -1):
            await asyncio.sleep(1)
            await sio.emit("countdown_tick", {"remaining": remaining}, room=code)

        # Countdown finished — start playback on selected devices
        result = await watch_party_svc.start_playback_on_sessions(
            code, session_ids, item_id, start_position_ticks=0,
        )
        await sio.emit("countdown_play", {
            "started": result.get("started", 0),
        }, room=code)

    asyncio.create_task(_countdown_then_play())

    log.info("theater_mode.countdown_started", code=code,
             title=title, devices=len(session_ids))
    return {"status": "ok", "countdown_seconds": 20}



@router.post("/api/pick-together/solo")
async def solo_pick_together(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Standalone Pick Together (no party needed).

    For in-the-room use: pulls candidates from a user's queue for group
    decision-making on a single device.

    Payload: {"user_id": 1, "candidate_count": 8}
    """
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(400, "user_id required")

    candidate_count = payload.get("candidate_count", 8)
    if candidate_count < 4:
        candidate_count = 4
    if candidate_count > 20:
        candidate_count = 20

    items = (await db.execute(
        select(QueueItem)
        .where(
            QueueItem.user_id == user_id,
            QueueItem.played == False,
            QueueItem.in_library == True,
            QueueItem.emby_item_id.isnot(None),
        )
        .order_by(QueueItem.score.desc())
        .limit(candidate_count)
    )).scalars().all()

    if not items:
        raise HTTPException(400, "No queue items available — run a queue refresh first")

    candidates = [
        {
            "emby_item_id": i.emby_item_id,
            "title": i.title,
            "type": i.item_type,
            "year": (i.metadata_json or {}).get("year"),
            "score": round(i.score, 2) if i.score else 0,
            "source": i.source,
            "votes": 0,
        }
        for i in items
    ]

    return {"status": "ok", "candidates": candidates}


@router.get("/party/{code}")
async def get_party(code: str):
    result = await watch_party_svc.get_party(code)
    if not result:
        raise HTTPException(404, "Party not found")
    return result


@router.get("/parties")
async def list_parties():
    return await watch_party_svc.list_active_parties()


@router.get("/parties/recent")
async def list_recent_parties(limit: int = Query(10, ge=1, le=50)):
    """Return recently ended parties for the watch party lobby."""
    return await watch_party_svc.list_recent_parties(limit)


# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/watch-party/server-sessions")
async def watch_party_server_sessions(db: AsyncSession = Depends(get_db)):
    """Return all active controllable Emby sessions grouped by server user.

    Used by the one-click Watch Party launcher on the Continue Watching
    panel.  Returns every user who has at least one remote-controllable
    device online, with their devices listed underneath.
    """
    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
    except Exception as e:
        log.warning("watch_party.server_sessions_failed", error=str(e)[:200])
        raise HTTPException(502, "Failed to fetch Emby sessions")
    finally:
        await emby.close()

    # Build a mapping of emby_user_id → DB user for display names + DB IDs
    db_users = (await db.execute(select(User))).scalars().all()
    by_emby_id = {u.emby_user_id: u for u in db_users}

    # Group sessions by UserId
    user_sessions: dict[str, dict] = {}  # emby_user_id → {info + devices}
    for s in all_sessions:
        uid = s.get("UserId")
        if not uid:
            continue
        if not s.get("SupportsRemoteControl", False):
            continue

        if uid not in user_sessions:
            db_user = by_emby_id.get(uid)
            user_sessions[uid] = {
                "emby_user_id": uid,
                "db_user_id": db_user.id if db_user else None,
                "username": s.get("UserName") or (db_user.emby_username if db_user else "Unknown"),
                "devices": [],
            }

        user_sessions[uid]["devices"].append({
            "session_id": s.get("Id"),
            "device_name": s.get("DeviceName", "Unknown"),
            "client": s.get("Client", ""),
            "now_playing": s.get("NowPlayingItem", {}).get("Name"),
        })

    return {"users": list(user_sessions.values())}


# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/emby/play")
async def emby_direct_play(payload: dict, _user: User = Depends(get_current_user)):
    """Start playback of an Emby item on a specific session.

    Payload: {"session_id": "abc", "item_id": "123", "start_position_ticks": 0}
    Used by the solo Pick Together flow to play directly on a device.
    """
    session_id = payload.get("session_id")
    item_id = payload.get("item_id")
    start_ticks = int(payload.get("start_position_ticks", 0))

    if not session_id or not item_id:
        raise HTTPException(400, "session_id and item_id required")

    emby = EmbyClient()
    try:
        await emby.play_item_on_session(session_id, item_id, start_position_ticks=start_ticks)
        return {"status": "ok"}
    except Exception as e:
        log.warning("emby_direct_play.failed", session_id=session_id, error=str(e)[:200])
        raise HTTPException(502, f"Playback failed: {str(e)[:100]}")
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════

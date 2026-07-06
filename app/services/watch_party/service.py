"""Service #4 — Multi-User Watch Party Sync.

Manages watch parties:
  - Party CRUD (create / join / leave / end)
  - Real-time playback sync via WebSocket
  - Trakt checkin on start, scrobble on finish
  - State persistence in Redis for fast reads, Postgres for history
"""

from __future__ import annotations

import asyncio
import random
import string
from datetime import datetime
from typing import Any

import socketio
import structlog
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import WatchParty, WatchPartyParticipant, User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.database import async_session

log = structlog.get_logger()

# Socket.IO server — attached to the main ASGI app later
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)


def _generate_code(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


class WatchPartyService:
    def __init__(self):
        self.emby = EmbyClient()

    # -----------------------------------------------------------------------
    # Party lifecycle (called from REST API)
    # -----------------------------------------------------------------------

    async def create_party(self, host_user_id: int, emby_item_id: str) -> dict:
        """Create a new watch party and return its join code.
        
        NEW: Automatically checkin to Trakt when party starts.
        """
        code = _generate_code()

        async with async_session() as db:
            host_user = (await db.execute(
                select(User).where(User.id == host_user_id)
            )).scalar_one_or_none()

        # resolve item title from Emby (tolerates builds where /Items/{id} 404s)
        item = await self.emby.get_item_safe(
            emby_item_id,
            user_id=host_user.emby_user_id if host_user else None,
        )
        if not item:
            raise ValueError(f"Emby item '{emby_item_id}' not found — use the search box to pick a title")
        title = item.get("Name", "Unknown")

        async with async_session() as db:
            party = WatchParty(
                code=code,
                host_user_id=host_user_id,
                emby_item_id=emby_item_id,
                title=title,
                status="waiting",
            )
            db.add(party)
            await db.flush()

            db.add(WatchPartyParticipant(
                party_id=party.id,
                user_id=host_user_id,
            ))
            await db.commit()

            party_id = party.id

        # cache active state in Redis
        r = await get_redis()
        await r.hset(f"party:{code}", mapping={
            "id": str(party_id),
            "host": str(host_user_id),
            "item": emby_item_id,
            "title": title,
            "status": "waiting",
            "position": "0",
            "created_at": datetime.utcnow().isoformat(),
        })
        await r.expire(f"party:{code}", 86400)
        
        # NEW: Trakt checkin when party created
        if host_user and host_user.trakt_access_token:
            await self._trakt_checkin(host_user, item)

        log.info("watch_party.created", code=code, title=title, host_id=host_user_id)
        return {"code": code, "party_id": party_id, "title": title}

    async def join_party(self, code: str, user_id: int) -> dict | None:
        """Join an existing party by code.

        Returns the user's active Emby session count so the UI can warn if
        the user has no devices visible (e.g. wrong user selected).
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return None

        party_id = int(state["id"])

        async with async_session() as db:
            existing = (await db.execute(
                select(WatchPartyParticipant).where(
                    WatchPartyParticipant.party_id == party_id,
                    WatchPartyParticipant.user_id == user_id,
                )
            )).scalar_one_or_none()

            if not existing:
                db.add(WatchPartyParticipant(party_id=party_id, user_id=user_id))
                await db.commit()

            # Count participants — move to "active" once host + guest are in
            count = (await db.execute(
                select(func.count(WatchPartyParticipant.id))
                .where(WatchPartyParticipant.party_id == party_id)
            )).scalar() or 0

            # Check if this user has active Emby sessions
            user = (await db.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()

        active_sessions = 0
        emby_username = ""
        if user and user.emby_user_id:
            emby_username = user.emby_username or user.emby_user_id
            try:
                sessions = await self.emby.get_sessions()
                active_sessions = sum(
                    1 for s in sessions
                    if s.get("UserId") == user.emby_user_id
                    and s.get("SupportsRemoteControl", True)
                )
            except Exception:
                pass

        # Transition from waiting → active when 2+ participants
        current_status = state.get("status", "waiting")
        if current_status == "waiting" and count >= 2:
            await r.hset(f"party:{code}", "status", "active")
            current_status = "active"

        # notify room
        await sio.emit("user_joined", {"user_id": user_id, "username": emby_username}, room=code)

        log.info("watch_party.joined", code=code, user_id=user_id,
                 emby_username=emby_username, active_sessions=active_sessions,
                 status=current_status)
        return {
            "code": code,
            "title": state.get("title", ""),
            "status": current_status,
            "position": int(state.get("position", 0)),
            "active_sessions": active_sessions,
            "emby_username": emby_username,
        }

    async def end_party(self, code: str):
        """End a watch party: stop playback on all devices, scrobble to Trakt."""
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return

        party_id = int(state["id"])
        position = int(state.get("position", 0))

        async with async_session() as db:
            party = (await db.execute(
                select(WatchParty).where(WatchParty.id == party_id)
            )).scalar_one_or_none()

            # Collect participant emby_user_ids for stop commands
            participants = []
            participant_emby_ids: set[str] = set()
            if party:
                participants = (await db.execute(
                    select(WatchPartyParticipant).where(WatchPartyParticipant.party_id == party_id)
                )).scalars().all()

                for participant in participants:
                    user = (await db.execute(
                        select(User).where(User.id == participant.user_id)
                    )).scalar_one_or_none()

                    if user:
                        if user.emby_user_id:
                            participant_emby_ids.add(user.emby_user_id)

                        # Trakt scrobble
                        if user.trakt_access_token:
                            await self._trakt_scrobble_stop(user, party.emby_item_id, position)

            # Update database
            await db.execute(
                update(WatchParty).where(WatchParty.id == party_id).values(
                    status="ended", ended_at=datetime.utcnow()
                )
            )
            await db.commit()

        # Stop playback on all participant Emby sessions
        stopped = 0
        if participant_emby_ids:
            try:
                sessions = await self.emby.get_sessions()
                for session in sessions:
                    if session.get("UserId", "") not in participant_emby_ids:
                        continue
                    sid = session.get("Id", "")
                    if not sid or not session.get("NowPlayingItem"):
                        continue
                    try:
                        await self.emby.send_play_command(sid, "Stop")
                        stopped += 1
                    except Exception:
                        pass
            except Exception as e:
                log.warning("watch_party.stop_sessions_failed", error=str(e))

        await r.delete(f"party:{code}")
        await sio.emit("party_ended", {}, room=code)
        log.info("watch_party.ended", code=code,
                 scrobbled=len(participants), stopped=stopped)

    async def get_party(self, code: str) -> dict | None:
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return None

        party_id = int(state.get("id", 0))
        participants = []
        try:
            async with async_session() as db:
                rows = (await db.execute(
                    select(User.emby_username, User.trakt_username)
                    .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                    .where(WatchPartyParticipant.party_id == party_id)
                )).all()
                participants = [
                    r.emby_username or r.trakt_username or "Unknown"
                    for r in rows
                ]
        except Exception:
            pass

        return {
            "code": code,
            "party_id": party_id,
            "title": state.get("title", ""),
            "status": state.get("status", "waiting"),
            "position": int(state.get("position", 0)),
            "host_user_id": int(state.get("host", 0)),
            "emby_item_id": state.get("item", ""),
            "participants": participants,
        }

    async def list_active_parties(self) -> list[dict]:
        r = await get_redis()
        keys = [k async for k in r.scan_iter(match="party:*")]
        parties = []
        for key in keys:
            state = await r.hgetall(key)
            code = key.split(":", 1)[1]
            if state.get("status") != "ended":
                # Fetch participant names from DB
                participants = []
                party_id = state.get("id")
                if party_id:
                    try:
                        async with async_session() as db:
                            rows = (await db.execute(
                                select(User.emby_username, User.trakt_username)
                                .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                                .where(WatchPartyParticipant.party_id == int(party_id))
                            )).all()
                            participants = [
                                r.emby_username or r.trakt_username or "Unknown"
                                for r in rows
                            ]
                    except Exception:
                        pass
                parties.append({
                    "code": code,
                    "title": state.get("title", ""),
                    "status": state.get("status", ""),
                    "participants": participants,
                })
        return parties

    async def list_recent_parties(self, limit: int = 10) -> list[dict]:
        """Return recently ended parties from the database."""
        async with async_session() as db:
            rows = (await db.execute(
                select(WatchParty)
                .where(WatchParty.status == "ended")
                .order_by(WatchParty.ended_at.desc())
                .limit(limit)
            )).scalars().all()

            result = []
            for party in rows:
                participants = (await db.execute(
                    select(User.emby_username, User.trakt_username)
                    .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                    .where(WatchPartyParticipant.party_id == party.id)
                )).all()
                result.append({
                    "code": party.code,
                    "title": party.title,
                    "status": party.status,
                    "ended_at": party.ended_at.isoformat() if party.ended_at else None,
                    "participants": [
                        r.emby_username or r.trakt_username or "Unknown"
                        for r in participants
                    ],
                })
        return result

    # -----------------------------------------------------------------------
    # Trakt Integration (NEW in Phase 2)
    # -----------------------------------------------------------------------

    async def _trakt_checkin(self, user: User, emby_item: dict) -> None:
        """Checkin to Trakt when party starts."""
        if not user.trakt_access_token:
            return
        
        try:
            trakt = TraktClient(
                access_token=user.trakt_access_token,
                refresh_token=user.trakt_refresh_token,
                token_expires=user.trakt_token_expires,
                token_refresh_callback=self._make_token_callback(user),
            )
            
            # Build Trakt item payload from Emby item
            item_type = emby_item.get("Type", "Movie").lower()
            trakt_id = None
            
            # Extract Trakt ID from Emby's provider IDs
            provider_ids = emby_item.get("ProviderIds", {})
            if item_type == "movie":
                trakt_id = provider_ids.get("Tmdb")
                if trakt_id:
                    payload = {"movie": {"ids": {"tmdb": int(trakt_id)}}}
            else:  # episode or series
                trakt_id = provider_ids.get("Tvdb")
                if trakt_id:
                    payload = {"show": {"ids": {"tvdb": int(trakt_id)}}}
            
            if not trakt_id:
                log.warning("watch_party.trakt_checkin_failed", reason="no_trakt_id", item=emby_item.get("Name"))
                return
            
            # Checkin (optional message)
            message = f"Watching {emby_item.get('Name', 'unknown')} in a watch party!"
            await trakt.checkin(payload, message=message)
            log.info("watch_party.trakt_checkin", user_id=user.id, item=emby_item.get("Name"))
        
        except Exception as e:
            log.error("watch_party.trakt_checkin_error", user_id=user.id, error=str(e))

    async def _trakt_scrobble_stop(self, user: User, emby_item_id: str, position_ticks: int) -> None:
        """Send scrobble-stop to Trakt when party ends."""
        if not user.trakt_access_token:
            return
        
        try:
            trakt = TraktClient(
                access_token=user.trakt_access_token,
                refresh_token=user.trakt_refresh_token,
                token_expires=user.trakt_token_expires,
                token_refresh_callback=self._make_token_callback(user),
            )
            
            # Get item details from Emby
            item = await self.emby.get_item_safe(emby_item_id, user_id=user.emby_user_id)
            if not item:
                return
            provider_ids = item.get("ProviderIds", {})
            item_type = item.get("Type", "Movie").lower()
            
            # Build scrobble payload
            if item_type == "movie":
                tmdb_id = provider_ids.get("Tmdb")
                if tmdb_id:
                    payload = {"movie": {"ids": {"tmdb": int(tmdb_id)}}}
            else:
                tvdb_id = provider_ids.get("Tvdb")
                if tvdb_id:
                    payload = {"show": {"ids": {"tvdb": int(tvdb_id)}}}
                else:
                    return
            
            # Calculate progress (0-100)
            duration_ticks = item.get("RunTimeTicks") or 0
            if duration_ticks > 0 and position_ticks > 0:
                progress = min(100.0, (position_ticks / duration_ticks * 100))
            else:
                # If no position tracked, assume fully watched (party ended = watched)
                progress = 90.0
            
            # Scrobble
            await trakt.scrobble_stop(payload, progress=progress)
            log.info("watch_party.trakt_scrobble", user_id=user.id, progress=f"{progress:.1f}%")
        
        except Exception as e:
            log.error("watch_party.trakt_scrobble_error", user_id=user.id, error=str(e))

    def _make_token_callback(self, user: User):
        """Create a token refresh callback for a user."""
        async def callback(access_token: str, refresh_token: str, expires: datetime) -> None:
            async with async_session() as db:
                db.merge(user)
                user.trakt_access_token = access_token
                user.trakt_refresh_token = refresh_token
                user.trakt_token_expires = expires
                await db.commit()
        return callback

    async def list_sessions_for_party(self, code: str) -> dict:
        """Return active Emby sessions for all participants in a party.

        Used by the UI to let the host pick which devices to play on,
        and to identify duplicate items (different quality versions).
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return {"status": "error", "reason": "party_not_found"}

        party_id = int(state["id"])
        emby_item_id = state.get("item", "")

        async with async_session() as db:
            rows = (await db.execute(
                select(User.emby_user_id, User.emby_username)
                .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                .where(WatchPartyParticipant.party_id == party_id)
            )).all()
            participant_map = {
                row.emby_user_id: row.emby_username
                for row in rows if row.emby_user_id
            }

        sessions = await self.emby.get_sessions()
        devices = []
        for session in sessions:
            uid = session.get("UserId", "")
            if uid not in participant_map:
                continue
            if not session.get("SupportsRemoteControl", True):
                continue
            devices.append({
                "session_id": session.get("Id", ""),
                "device_name": session.get("DeviceName", "Unknown"),
                "client": session.get("Client", ""),
                "user": participant_map.get(uid, uid),
                "now_playing": session.get("NowPlayingItem", {}).get("Name", ""),
            })

        return {"status": "ok", "devices": devices, "emby_item_id": emby_item_id}

    async def start_playback_on_sessions(
        self, code: str, session_ids: list[str], emby_item_id: str | None = None,
    ) -> dict:
        """Start playback on specific sessions only (user-selected devices).

        If emby_item_id is provided, use that instead of the party default
        (allows picking a specific quality version).
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return {"status": "error", "reason": "party_not_found"}

        item_id = emby_item_id or state.get("item", "")
        if not item_id:
            return {"status": "error", "reason": "no_item"}

        started = []
        failed = []
        for sid in session_ids:
            try:
                await self.emby.play_item_on_session(sid, item_id, start_position_ticks=0)
                started.append(sid)
            except Exception as e:
                failed.append({"session_id": sid, "error": str(e)})

        await r.hset(f"party:{code}", "status", "playing")
        # If a different item was chosen, update the party item
        if emby_item_id:
            await r.hset(f"party:{code}", "item", emby_item_id)

        return {"status": "ok", "started": len(started), "failed": failed}

    # -----------------------------------------------------------------------
    # Emby Playback Control (NEW in Phase 2)
    # -----------------------------------------------------------------------

    async def start_playback(self, code: str) -> dict:
        """Start playback on all participants' Emby sessions simultaneously.

        Finds each participant's active Emby session, then sends a PlayNow
        command for the party's item to each one.  Sets party status to
        'playing'.
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return {"status": "error", "reason": "party_not_found"}

        party_id = int(state["id"])
        emby_item_id = state.get("item", "")
        if not emby_item_id:
            return {"status": "error", "reason": "no_item"}

        # Fetch participants → emby_user_id mapping
        async with async_session() as db:
            rows = (await db.execute(
                select(User.emby_user_id)
                .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                .where(WatchPartyParticipant.party_id == party_id)
            )).all()
            participant_emby_ids = {row.emby_user_id for row in rows if row.emby_user_id}

        if not participant_emby_ids:
            return {"status": "error", "reason": "no_participants"}

        # Get all active Emby sessions
        sessions = await self.emby.get_sessions()

        started = []
        failed = []
        for session in sessions:
            session_user_id = session.get("UserId", "")
            session_id = session.get("Id", "")
            device_name = session.get("DeviceName", "")

            # Only target sessions belonging to party participants
            if session_user_id not in participant_emby_ids:
                continue
            # Skip sessions that can't play media (server-side sessions, API clients)
            if not session.get("SupportsRemoteControl", True):
                continue

            try:
                await self.emby.play_item_on_session(
                    session_id, emby_item_id, start_position_ticks=0,
                )
                started.append({"user": session_user_id, "device": device_name})
                log.info("watch_party.playback_started",
                         code=code, session=session_id, device=device_name)
            except Exception as e:
                failed.append({"user": session_user_id, "error": str(e)})
                log.error("watch_party.playback_start_failed",
                          code=code, session=session_id, error=str(e))

        # Update party status to playing
        await r.hset(f"party:{code}", "status", "playing")

        return {
            "status": "ok",
            "started": started,
            "failed": failed,
        }

    async def pause_all(self, code: str) -> dict:
        """Send explicit Pause or Unpause to all participants' Emby sessions.

        Uses the party's Redis status to decide the desired state, then sends
        the explicit command (not PlayPause toggle) so all devices end up in
        the same state regardless of their current individual state.
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return {"status": "error"}

        party_id = int(state["id"])
        current_status = state.get("status", "")

        # Decide target: if currently playing → pause; otherwise → unpause
        want_paused = current_status != "paused"
        command = "Pause" if want_paused else "Unpause"

        async with async_session() as db:
            rows = (await db.execute(
                select(User.emby_user_id)
                .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                .where(WatchPartyParticipant.party_id == party_id)
            )).all()
            participant_emby_ids = {row.emby_user_id for row in rows if row.emby_user_id}

        sessions = await self.emby.get_sessions()
        toggled = 0
        for session in sessions:
            if session.get("UserId", "") not in participant_emby_ids:
                continue
            sid = session.get("Id", "")
            if not sid:
                continue
            # Only target sessions that are actively playing something
            if not session.get("NowPlayingItem"):
                continue
            try:
                await self.emby.send_play_command(sid, command)
                toggled += 1
            except Exception:
                pass

        new_status = "paused" if want_paused else "playing"
        await r.hset(f"party:{code}", "status", new_status)

        return {"status": "ok", "new_status": new_status, "toggled": toggled}

    async def seek_all(self, code: str, position_ticks: int) -> dict:
        """Push a seek position to all participants' Emby sessions.

        Called when the host seeks — sends the same position to every
        participant's active session so everyone stays in sync.
        """
        r = await get_redis()
        state = await r.hgetall(f"party:{code}")
        if not state:
            return {"status": "error", "reason": "party_not_found"}

        party_id = int(state["id"])

        async with async_session() as db:
            rows = (await db.execute(
                select(User.emby_user_id)
                .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                .where(WatchPartyParticipant.party_id == party_id)
            )).all()
            participant_emby_ids = {row.emby_user_id for row in rows if row.emby_user_id}

        sessions = await self.emby.get_sessions()
        seeked = 0
        for session in sessions:
            if session.get("UserId", "") not in participant_emby_ids:
                continue
            sid = session.get("Id", "")
            if not sid:
                continue
            try:
                await self.emby.send_play_state_command(
                    sid, "Seek", seek_ticks=position_ticks,
                )
                seeked += 1
            except Exception:
                pass

        # Update position in Redis
        await r.hset(f"party:{code}", "position", str(position_ticks))

        return {"status": "ok", "seeked": seeked, "position_ticks": position_ticks}

    async def sync_playback_to_emby(self, code: str, event: str, position_ticks: int) -> None:
        """Send playback command to Emby sessions for all party participants.

        Uses the User.emby_user_id (not the DB primary key) to match against
        Emby session UserId strings.  Sends explicit Pause/Unpause (not the
        PlayPause toggle) so devices always end up in the intended state.
        """
        try:
            r = await get_redis()
            state = await r.hgetall(f"party:{code}")
            if not state:
                return

            party_id = int(state["id"])

            async with async_session() as db:
                # Join to User to get emby_user_id for session matching
                rows = (await db.execute(
                    select(User.emby_user_id)
                    .join(WatchPartyParticipant, WatchPartyParticipant.user_id == User.id)
                    .where(WatchPartyParticipant.party_id == party_id)
                )).all()
                participant_emby_ids = {row.emby_user_id for row in rows if row.emby_user_id}

            if not participant_emby_ids:
                return

            sessions = await self.emby.get_sessions()

            for session in sessions:
                emby_uid = session.get("UserId", "")
                if emby_uid not in participant_emby_ids:
                    continue
                session_id = session.get("Id")
                if not session_id:
                    continue

                try:
                    if event == "pause":
                        await self.emby.send_play_command(session_id, "Pause")
                    elif event == "play":
                        await self.emby.send_play_command(session_id, "Unpause")
                    elif event == "seek":
                        await self.emby.send_play_state_command(
                            session_id, "Seek",
                            seek_ticks=position_ticks,
                        )

                    log.debug(
                        "watch_party.emby_sync",
                        session_id=session_id,
                        emby_user_id=emby_uid,
                        event=event,
                    )
                except Exception as e:
                    log.warning(
                        "watch_party.emby_sync_failed",
                        emby_user_id=emby_uid,
                        error=str(e),
                    )

        except Exception as e:
            log.error("watch_party.sync_playback_error", code=code, error=str(e))


# ---------------------------------------------------------------------------
# Socket.IO event handlers
# ---------------------------------------------------------------------------

@sio.event
async def connect(sid, environ):
    log.debug("ws.connect", sid=sid)


@sio.event
async def disconnect(sid):
    log.debug("ws.disconnect", sid=sid)


@sio.event
async def join_room(sid, data):
    """Client sends: {code: "ABC123", user_id: 42}"""
    code = data.get("code", "")
    user_id = data.get("user_id")
    sio.enter_room(sid, code)
    await sio.emit("room_joined", {"sid": sid, "user_id": user_id}, room=code)
    log.info("ws.join_room", sid=sid, code=code)


@sio.event
async def leave_room(sid, data):
    code = data.get("code", "")
    sio.leave_room(sid, code)
    await sio.emit("room_left", {"sid": sid}, room=code)


@sio.event
async def playback_event(sid, data):
    """Sync playback state across all party members.

    data: {code, event: "play"|"pause"|"seek", position_ticks: int}
    
    NEW: Also syncs to Emby playback sessions for all participants.
    """
    code = data.get("code", "")
    event = data.get("event", "")
    position = data.get("position_ticks", 0)

    # update Redis state
    r = await get_redis()
    status_map = {"play": "playing", "pause": "paused", "stop": "ended"}
    new_status = status_map.get(event, "playing")

    await r.hset(f"party:{code}", mapping={
        "status": new_status,
        "position": str(position),
    })

    # NEW: Sync to Emby sessions
    watch_party_svc = WatchPartyService()
    asyncio.create_task(watch_party_svc.sync_playback_to_emby(code, event, position))

    # broadcast to everyone else in the room (WebSocket)
    await sio.emit(
        "sync_playback",
        {"event": event, "position_ticks": position, "from_sid": sid},
        room=code,
        skip_sid=sid,
    )

    log.debug("ws.playback_event", code=code, event=event, position=position)


@sio.event
async def chat_message(sid, data):
    """In-party chat message: {code, user_id, text}."""
    code = data.get("code", "")
    await sio.emit("chat_message", data, room=code, skip_sid=sid)


@sio.event
async def reaction(sid, data):
    """Emoji reaction: {code, user_id, emoji, position_ticks}."""
    code = data.get("code", "")
    await sio.emit("reaction", data, room=code, skip_sid=sid)

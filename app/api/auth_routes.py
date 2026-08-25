"""Routes extracted from routes.py — auth_routes.py."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import AppSetting, User
from app.utils.database import get_db
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_set
from app.utils.simkl_client import SimklClient
from app.security.auth import get_current_user, issue_tokens
from app.middleware.rate_limit import LIMITS, limiter
from app.api.route_helpers import VALID_PROVIDERS, _get_active_providers, _get_integration_provider, _provider_set, _put_setting

log = structlog.get_logger()

router = APIRouter()



class LinkRequest(BaseModel):
    emby_user_id: str
    emby_username: str = ""

class LinkPollRequest(BaseModel):
    emby_user_id: str
    device_code: str


@router.get("/api/integration-provider")
async def get_integration_provider(db: AsyncSession = Depends(get_db)):
    """Return the current integration provider setting."""
    provider = await _get_integration_provider(db)
    return {"provider": provider, "active": list(_provider_set(provider))}


@router.put("/api/integration-provider")
async def set_integration_provider(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Set the integration provider: 'simkl', 'mdblist', 'both', or 'none'."""
    provider = payload.get("provider", "").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise HTTPException(400, f"Invalid provider. Must be one of: {', '.join(sorted(VALID_PROVIDERS))}")

    r = await get_redis()
    await r.set("integration_provider", provider)
    # Inline upsert (can't use _put_setting — defined later in file)
    row = (await db.execute(select(AppSetting).where(AppSetting.key == "integration_provider"))).scalar_one_or_none()
    if row:
        row.value = provider
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    else:
        db.add(AppSetting(key="integration_provider", value=provider, updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))
    await db.commit()

    log.info("integration_provider.changed", provider=provider)
    return {"status": "ok", "provider": provider, "active": list(_provider_set(provider))}


@router.get("/api/integration-provider/setup-required")
async def check_setup_required(db: AsyncSession = Depends(get_db)):
    """Check if first-run setup is needed (no provider configured yet)."""
    row = (await db.execute(
        select(AppSetting).where(AppSetting.key == "integration_provider")
    )).scalar_one_or_none()
    return {"setup_required": row is None}


# ═══════════════════════════════════════════════════════════════════════════



@router.post("/api/setup/test-emby")
async def setup_test_emby(payload: dict):
    """Test Emby connection with user-provided URL + API key (no auth required)."""
    import httpx

    url = (payload.get("emby_url") or "").strip().rstrip("/")
    key = (payload.get("emby_api_key") or "").strip()

    if not url or not key:
        raise HTTPException(400, "Both emby_url and emby_api_key are required.")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                f"{url}/System/Info/Public",
                params={"api_key": key},
            )
            r.raise_for_status()
            info = r.json()
            return {
                "status": "ok",
                "server_name": info.get("ServerName", ""),
                "version": info.get("Version", ""),
            }
    except httpx.ConnectError:
        raise HTTPException(502, f"Cannot reach {url} — check the URL and port.")
    except httpx.TimeoutException:
        raise HTTPException(504, f"Connection to {url} timed out.")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(401, "API key rejected by Emby — check the key.")
        raise HTTPException(502, f"Emby returned HTTP {e.response.status_code}.")
    except Exception as e:
        raise HTTPException(502, f"Connection failed: {str(e)[:200]}")


@router.post("/api/setup/emby-users")
async def setup_emby_users(payload: dict):
    """Fetch the list of Emby users so the wizard can offer a picker.

    No auth required — this is part of the first-run setup flow.
    """
    import httpx

    url = (payload.get("emby_url") or "").strip().rstrip("/")
    key = (payload.get("emby_api_key") or "").strip()

    if not url or not key:
        raise HTTPException(400, "emby_url and emby_api_key are required.")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{url}/Users",
                params={"api_key": key},
            )
            resp.raise_for_status()
            emby_users = resp.json()

        users = []
        for u in emby_users:
            is_admin = False
            policy = u.get("Policy") or {}
            if isinstance(policy, dict):
                is_admin = policy.get("IsAdministrator", False)
            users.append({
                "id": u["Id"],
                "name": u.get("Name", "Unknown"),
                "is_admin": is_admin,
            })
        # Admins first, then alphabetical
        users.sort(key=lambda x: (not x["is_admin"], x["name"].lower()))
        return {"users": users}
    except httpx.ConnectError:
        raise HTTPException(502, f"Cannot reach {url}.")
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch users: {str(e)[:200]}")


@router.post("/api/setup/save")
async def setup_save_all(payload: dict, db: AsyncSession = Depends(get_db)):
    """Save all first-run wizard settings: Emby creds, provider, integration keys.

    Persists to both Redis and the DB (AppSetting table) so values survive
    container and Redis restarts. Also updates the in-memory settings object
    so EmbyClient picks up the new URL/key without a restart.
    """
    from app.config import settings
    from app.utils.secure_redis import secure_set

    emby_url = (payload.get("emby_url") or "").strip().rstrip("/")
    emby_api_key = (payload.get("emby_api_key") or "").strip()
    provider = (payload.get("provider") or "simkl").strip().lower()
    simkl_client_id = (payload.get("simkl_client_id") or "").strip()
    mdblist_client_id = (payload.get("mdblist_client_id") or "").strip()
    mdblist_client_secret = (payload.get("mdblist_client_secret") or "").strip()

    # User selected in wizard — emby_user_id + emby_username
    selected_user_id = (payload.get("emby_user_id") or "").strip()
    selected_username = (payload.get("emby_username") or "").strip()

    if not emby_url or not emby_api_key:
        raise HTTPException(400, "Emby URL and API key are required.")

    if provider not in VALID_PROVIDERS:
        raise HTTPException(400, f"Invalid provider: {provider}")

    r = await get_redis()

    # Helper to upsert an AppSetting row
    async def _upsert(key: str, value: str):
        row = (await db.execute(
            select(AppSetting).where(AppSetting.key == key)
        )).scalar_one_or_none()
        if row:
            row.value = value
            row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        else:
            db.add(AppSetting(
                key=key, value=value,
                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            ))

    # 1. Emby credentials — save to Redis + DB + update in-memory
    await r.set("emby_url", emby_url)
    await r.set("emby_api_key", emby_api_key)
    await _upsert("emby_url", emby_url)
    await _upsert("emby_api_key", emby_api_key)

    settings.emby_url = emby_url
    settings.emby_api_key = emby_api_key

    # 2. Integration provider
    await r.set("integration_provider", provider)
    await _upsert("integration_provider", provider)

    # 3. Simkl client ID (if provided)
    if simkl_client_id:
        await secure_set("simkl_client_id", simkl_client_id)
        await _upsert("simkl_client_id", simkl_client_id)
        settings.simkl_client_id = simkl_client_id

    # 4. MDBList credentials (if provided)
    if mdblist_client_id:
        await secure_set("mdblist_api_key", mdblist_client_id)
        await _upsert("mdblist_api_key", mdblist_client_id)
    if mdblist_client_secret:
        await secure_set("mdblist_client_secret", mdblist_client_secret)
        await _upsert("mdblist_client_secret", mdblist_client_secret)

    await db.commit()

    # 5. Create the selected user (or fall back to first admin from Emby)
    #    get_current_user falls back to User.id=1 on LAN installs
    try:
        existing = (await db.execute(select(User).limit(1))).scalar_one_or_none()
        if not existing:
            if selected_user_id:
                # Wizard sent the user's choice
                db.add(User(
                    emby_user_id=selected_user_id,
                    emby_username=selected_username or "Admin",
                ))
                await db.commit()
                log.info("setup.default_user_created",
                         emby_user=selected_username)
            else:
                # Fallback: fetch users and pick the first admin
                import httpx
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"{emby_url}/Users",
                        params={"api_key": emby_api_key},
                    )
                    resp.raise_for_status()
                    emby_users = resp.json()
                    if emby_users:
                        # Prefer admin user
                        pick = emby_users[0]
                        for u in emby_users:
                            policy = u.get("Policy") or {}
                            if isinstance(policy, dict) and policy.get("IsAdministrator"):
                                pick = u
                                break
                        db.add(User(
                            emby_user_id=pick["Id"],
                            emby_username=pick.get("Name", "Admin"),
                        ))
                        await db.commit()
                        log.info("setup.default_user_created",
                                 emby_user=pick.get("Name"))
    except Exception as e:
        log.warning("setup.default_user_skipped", error=str(e)[:200])

    log.info("setup.wizard_complete",
             provider=provider,
             emby_url=emby_url,
             simkl=bool(simkl_client_id),
             mdblist=bool(mdblist_client_id))

    return {"status": "ok", "provider": provider}



# ═══════════════════════════════════════════════════════════════════════════


class LinkRequest(BaseModel):
    emby_user_id: str
    emby_username: str = ""


class LinkPollRequest(BaseModel):
    emby_user_id: str
    device_code: str


@router.post("/auth/simkl/device-code")
@limiter.limit(LIMITS["auth"])
async def simkl_device_code(request: Request, db: AsyncSession = Depends(get_db)):
    """Start Simkl device-code flow.  Returns user_code + verification_url."""
    body = await request.json()
    emby_user_id = body.get("emby_user_id", "").strip()
    emby_username = body.get("emby_username", "").strip()
    if not emby_user_id:
        raise HTTPException(400, "emby_user_id is required")

    user = (await db.execute(
        select(User).where(User.emby_user_id == emby_user_id)
    )).scalar_one_or_none()

    if not user:
        user = User(emby_user_id=emby_user_id, emby_username=emby_username)
        db.add(user)
        await db.commit()
        await db.refresh(user)

    simkl = SimklClient()
    try:
        result = await simkl.get_pin_code()
    finally:
        await simkl.close()

    return {
        "user_code": result["user_code"],
        "verification_url": result["verification_url"],
        "device_code": result["user_code"],   # Simkl polls with user_code, not device_code
        "expires_in": result["expires_in"],
        "interval": result["interval"],
    }


@router.post("/auth/simkl/poll")
@limiter.limit(LIMITS["auth"])
async def simkl_poll(request: Request, db: AsyncSession = Depends(get_db)):
    """Poll for completed Simkl authorisation."""
    body = await request.json()
    device_code = body.get("device_code", "").strip()
    emby_user_id = body.get("emby_user_id", "").strip()
    if not device_code or not emby_user_id:
        raise HTTPException(400, "device_code and emby_user_id are required")

    simkl = SimklClient()
    try:
        token_data = await simkl.poll_pin_token(device_code)
    finally:
        await simkl.close()

    if not token_data:
        return {"status": "pending"}

    user = (await db.execute(
        select(User).where(User.emby_user_id == emby_user_id)
    )).scalar_one_or_none()

    if not user:
        raise HTTPException(404, "User not found — call device-code first")

    user.simkl_access_token = token_data["access_token"]
    token_expires_in = token_data.get("expires_in", 157680000)  # 5yr default per Simkl docs
    user.simkl_token_expires = (datetime.now(timezone.utc) + timedelta(seconds=token_expires_in)).replace(tzinfo=None)

    # Log token info for debugging auth issues
    tok = token_data["access_token"]
    tok_hint = f"{tok[:8]}…{tok[-4:]}" if len(tok) > 12 else tok[:8]
    log.info("simkl_poll.token_received", **{"token_hint": tok_hint, "expires_in": token_expires_in,
                       "user_id": user.id, "emby_user_id": emby_user_id})

    # fetch simkl username
    authed = SimklClient(access_token=token_data["access_token"])
    try:
        me = await authed.get_me()
        user.simkl_username = me.get("user", {}).get("username", "")
    finally:
        await authed.close()

    await db.commit()

    # Post-commit verification: re-read from DB to confirm persistence
    await db.refresh(user)
    stored_hint = ""
    if user.simkl_access_token:
        st = user.simkl_access_token
        stored_hint = f"{st[:8]}…{st[-4:]}" if len(st) > 12 else st[:8]
    log.info("simkl_poll.token_persisted", **{"stored_hint": stored_hint, "match": stored_hint == tok_hint,
                       "expires": str(user.simkl_token_expires)})

    # ✅ SECURITY: Issue JWT tokens to user
    tokens = await issue_tokens(user)

    return {
        "status": "linked",
        "simkl_username": user.simkl_username,
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "token_type": tokens.token_type,
        "expires_in": tokens.expires_in,
    }


@router.get("/auth/users")
async def list_users(db: AsyncSession = Depends(get_db)):
    users = (await db.execute(select(User))).scalars().all()
    now = datetime.now(timezone.utc)
    result = []
    for u in users:
        expires = u.simkl_token_expires
        token_info = {}
        if expires:
            # DB-stored expires may be naive (pre-timezone-aware migration)
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            delta = expires - now
            total_secs = int(delta.total_seconds())
            if total_secs > 0:
                days = total_secs // 86400
                hours = (total_secs % 86400) // 3600
                minutes = (total_secs % 3600) // 60
                token_info = {
                    "token_expires": expires.isoformat(),
                    "token_days_left": days,
                    "token_hours_left": hours,
                    "token_minutes_left": minutes,
                    "token_status": "ok" if days > 7 else "expiring_soon" if days >= 1 else "expiring_today",
                }
            else:
                token_info = {
                    "token_expires": expires.isoformat(),
                    "token_days_left": 0,
                    "token_hours_left": 0,
                    "token_minutes_left": 0,
                    "token_status": "expired",
                }
        result.append({
            "id": u.id,
            "emby_user_id": u.emby_user_id,
            "emby_username": u.emby_username,
            "simkl_username": u.simkl_username,
            "linked": bool(u.simkl_access_token),
            **token_info,
        })
    return result


@router.get("/auth/emby-users")
async def list_all_emby_users(db: AsyncSession = Depends(get_db)):
    """Return all Emby server users, auto-creating DB records for any missing.

    The watch party page needs every Emby user in the dropdown, not just
    those who have been through the Simkl link flow.
    """
    emby = EmbyClient()
    try:
        emby_users = await emby.get_users()
    except Exception as e:
        await emby.close()
        raise HTTPException(502, f"Could not reach Emby server: {e}")
    await emby.close()

    # Load existing DB users keyed by emby_user_id
    existing = (await db.execute(select(User))).scalars().all()
    by_emby_id = {u.emby_user_id: u for u in existing}

    created = 0
    for eu in emby_users:
        eid = eu.get("Id", "")
        if not eid:
            continue
        if eid not in by_emby_id:
            new_user = User(
                emby_user_id=eid,
                emby_username=eu.get("Name", ""),
            )
            db.add(new_user)
            by_emby_id[eid] = new_user
            created += 1
        else:
            # Update username if it changed on the Emby side
            db_user = by_emby_id[eid]
            emby_name = eu.get("Name", "")
            if emby_name and db_user.emby_username != emby_name:
                db_user.emby_username = emby_name

    if created:
        await db.commit()
        # Refresh to get auto-generated IDs
        for u in by_emby_id.values():
            await db.refresh(u)

    now = datetime.now(timezone.utc)
    result = []
    for u in by_emby_id.values():
        token_info = {}
        if u.simkl_token_expires:
            _exp = u.simkl_token_expires
            if _exp.tzinfo is None:
                _exp = _exp.replace(tzinfo=timezone.utc)
            delta = _exp - now
            total_secs = int(delta.total_seconds())
            if total_secs > 0:
                days = total_secs // 86400
                hours = (total_secs % 86400) // 3600
                minutes = (total_secs % 3600) // 60
                token_info = {
                    "token_status": "ok" if days > 7 else "expiring_soon" if days >= 1 else "expiring_today",
                    "token_days_left": days,
                    "token_hours_left": hours,
                    "token_minutes_left": minutes,
                }
            else:
                token_info = {
                    "token_status": "expired",
                    "token_days_left": 0,
                    "token_hours_left": 0,
                    "token_minutes_left": 0,
                }
        result.append({
            "id": u.id,
            "emby_user_id": u.emby_user_id,
            "emby_username": u.emby_username,
            "simkl_username": u.simkl_username,
            "linked": bool(u.simkl_access_token),
            **token_info,
        })

    return result


# ═══════════════════════════════════════════════════════════════════════════

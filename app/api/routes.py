"""REST API routes for the Emby-Simkl Suite."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func, distinct, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models.schema import User, QueueItem, Prediction, MLModel, Universe, UniverseItem, AppSetting, WatchPartyParticipant, WatchParty
from app.utils.database import get_db
from app.utils.simkl_client import SimklClient
from app.utils.library_cache import LibraryCache
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import get_redis
from app.utils.secure_redis import secure_get, secure_set

# ✅ SECURITY: Import auth module
from app.security.auth import get_current_user, require_user_ownership, issue_tokens

from app.services.smart_queue.service import SmartQueueService
from app.middleware.rate_limit import limiter, LIMITS
from fastapi.responses import Response
import re

# ── Pydantic models for validated payloads ──────────────────────────────
class RewatchSettings(BaseModel):
    """Validated rewatch recommender settings."""
    min_rating: int = Field(default=8, ge=1, le=10)
    min_months: int = Field(default=12, ge=1, le=120)
    seasonal: bool = True


# ── item_key format validation ──────────────────────────────────────────
_ITEM_KEY_RE = re.compile(r"^(emby|imdb|tmdb|simkl|tvdb):[A-Za-z0-9_-]+$")


def _validate_item_key(item_key: str) -> str:
    """Validate item_key path parameter format (provider:value)."""
    if not _ITEM_KEY_RE.match(item_key):
        raise HTTPException(400, "item_key must be provider:value (e.g. imdb:tt1234567)")
    return item_key
from app.services.ml_predictor.service import MLPredictorService
from app.services.universe_discovery.service import UniverseDiscoveryService
from app.services.watch_party.service import WatchPartyService
from app.utils.database import async_session as async_session_ctx


async def _first_emby_user_id() -> str | None:
    """Return the emby_user_id of the first linked user (for user-scoped queries)."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
    return user.emby_user_id if user else None
from app.services.rating_bias_detector.service import RatingBiasDetectorService
from app.services.airing_alerts.service import AiringAlertsService
from app.services.scrobble_audit.service import ScrobbleAuditService
from app.services.watch_stats.service import WatchStatsService

smart_queue_svc = SmartQueueService()
ml_predictor_svc = MLPredictorService()
universe_svc = UniverseDiscoveryService()
watch_party_svc = WatchPartyService()
bias_detector_svc = RatingBiasDetectorService()
airing_alerts_svc = AiringAlertsService()
scrobble_audit_svc = ScrobbleAuditService()
watch_stats_svc = WatchStatsService()

log = structlog.get_logger()

router = APIRouter()


# ═══════════════════════════════════════════════════════════════════════════
# Health
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/health")
async def health():
    cache_stats = await LibraryCache.get_stats()
    return {
        "status": "ok",
        "features": {
            "smart_queue": settings.enable_smart_queue,
            "ml_predictor": settings.enable_ml_predictor,
            "universe_discovery": settings.enable_universe_discovery,
            "watch_party": settings.enable_watch_party,
        },
        "library_cache": cache_stats,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Integration Provider Selection
# ═══════════════════════════════════════════════════════════════════════════

VALID_PROVIDERS = {"simkl", "mdblist", "both", "none"}

async def _get_integration_provider(db: AsyncSession | None = None) -> str:
    """Return the configured integration provider: 'simkl', 'mdblist', 'both', or 'none'.
    Checks Redis first (fast), falls back to DB, defaults to 'simkl' for legacy installs."""
    r = await get_redis()
    raw = await r.get("integration_provider")
    if raw:
        val = raw if isinstance(raw, str) else raw.decode()
        if val in VALID_PROVIDERS:
            return val
    if db:
        row = (await db.execute(select(AppSetting).where(AppSetting.key == "integration_provider"))).scalar_one_or_none()
        if row and row.value in VALID_PROVIDERS:
            await r.set("integration_provider", row.value)
            return row.value
    # Legacy installs without this setting default to 'simkl' if simkl creds exist
    if settings.simkl_client_id:
        return "simkl"
    return "none"


def _provider_set(provider: str) -> set[str]:
    """Convert provider string to set of active integrations."""
    if provider == "both":
        return {"simkl", "mdblist"}
    if provider in ("simkl", "mdblist"):
        return {provider}
    return set()


async def _get_active_providers(db: AsyncSession | None = None) -> set[str]:
    """Return set of active integration providers, e.g. {'simkl', 'mdblist'}."""
    return _provider_set(await _get_integration_provider(db))


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
# Setup Wizard — zero-config first-run endpoints
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
# Auth — Simkl device-code OAuth
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
# Feature #1 — Smart Watch Queue
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/queue/refresh")
async def refresh_queue(_user: User = Depends(get_current_user)):
    """Manually trigger a Smart Queue update for all users."""
    asyncio.create_task(smart_queue_svc.run_for_all_users())
    return {"status": "refresh_started"}


@router.get("/queue/{user_id}")
async def get_queue(
    user_id: int,
    limit: int = Query(20, ge=1, le=1000),  # ✅ SECURITY: Input validation
    current_user: User = Depends(get_current_user),  # ✅ SECURITY: Authentication
    db: AsyncSession = Depends(get_db),
):
    # ✅ SECURITY: Authorization check
    require_user_ownership(current_user.id, user_id, "watch_queue")
    
    items = (await db.execute(
        select(QueueItem)
        .where(
            QueueItem.user_id == user_id,
            QueueItem.played == False,
        )
        .order_by(QueueItem.score.desc())
        .limit(limit)
    )).scalars().all()

    ratings: dict[str, dict] = {}
    emby = EmbyClient()
    try:
        lib_ids = [i.emby_item_id for i in items if i.emby_item_id]
        if lib_ids:
            for it in await emby.get_items_by_ids(lib_ids):
                ratings[str(it.get("Id"))] = {
                    "community_rating": it.get("CommunityRating"),
                    "official_rating": it.get("OfficialRating"),
                    "date_created": it.get("DateCreated"),
                }
    except Exception:
        pass
    finally:
        await emby.close()

    return [
        {
            "emby_item_id": i.emby_item_id,
            "title": i.title,
            "type": i.item_type,
            "source": i.source,
            "score": i.score,
            "trending_rank": i.simkl_trending_rank,
            "played": i.played,
            "played_at": i.played_at.isoformat() if i.played_at else None,
            "in_library": i.in_library if i.in_library is not None else True,
            "community_rating": ratings.get(str(i.emby_item_id), {}).get("community_rating"),
            "official_rating": ratings.get(str(i.emby_item_id), {}).get("official_rating"),
            "date_created": ratings.get(str(i.emby_item_id), {}).get("date_created"),
            "tmdb_id": (i.metadata_json or {}).get("ids", {}).get("tmdb"),
            "imdb_id": (i.metadata_json or {}).get("ids", {}).get("imdb"),
            "tvdb_id": (i.metadata_json or {}).get("ids", {}).get("tvdb"),
            "simkl_id": (i.metadata_json or {}).get("simkl_id"),
            "year": (i.metadata_json or {}).get("year"),
        }
        for i in items
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue Blocklist
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/queue/block")
async def block_queue_item(
    payload: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Permanently dismiss a smart queue item so it never reappears."""
    from app.models.schema import QueueBlocklist

    user_id = payload.get("user_id")
    simkl_id = str(payload.get("simkl_id", ""))
    title = payload.get("title", "")
    item_type = payload.get("item_type", "")

    if not simkl_id or not user_id:
        raise HTTPException(400, "user_id and simkl_id required")
    require_user_ownership(current_user.id, user_id, "queue_block")

    # Insert into blocklist (ignore duplicate)
    existing = (await db.execute(
        select(QueueBlocklist).where(
            QueueBlocklist.user_id == user_id,
            QueueBlocklist.simkl_id == simkl_id,
        )
    )).scalar_one_or_none()
    if not existing:
        db.add(QueueBlocklist(
            user_id=user_id, simkl_id=simkl_id,
            title=title, item_type=item_type,
        ))
        await db.flush()

    # Find and remove matching queue item
    queue_items = (await db.execute(
        select(QueueItem).where(QueueItem.user_id == user_id)
    )).scalars().all()
    removed = False
    for qi in queue_items:
        qi_simkl = str((qi.metadata_json or {}).get("simkl_id", ""))
        if qi_simkl == simkl_id:
            await db.delete(qi)
            removed = True
    await db.commit()

    # Backfill from overflow and re-sync playlist
    if removed:
        try:
            # Pop best overflow candidate
            replacement = await smart_queue_svc._pop_best_overflow(user_id, 0.0)
            if replacement:
                emby_id = await smart_queue_svc._find_in_emby(replacement)
                async with async_session_ctx() as db2:
                    db2.add(QueueItem(
                        user_id=user_id,
                        emby_item_id=emby_id,
                        title=replacement["title"],
                        item_type=replacement["item_type"],
                        source=replacement["source"],
                        score=replacement["score"],
                        simkl_trending_rank=replacement.get("trending_rank"),
                        simkl_rating=replacement.get("friend_rating"),
                        metadata_json=replacement,
                        in_library=emby_id is not None,
                    ))
                    await db2.commit()
                log.info("queue.backfill_after_block",
                         user_id=user_id, title=replacement["title"])
            # Re-sync Emby playlist
            await smart_queue_svc._resync_playlist_from_db(user_id)
        except Exception as e:
            log.warning("queue.backfill_failed", error=str(e)[:120])

    log.info("queue.item_blocked", user_id=user_id, simkl_id=simkl_id, title=title)
    return {"status": "ok", "blocked": simkl_id, "title": title}


@router.get("/api/queue/blocklist/{user_id}")
async def get_queue_blocklist(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return all permanently blocked queue items for a user."""
    from app.models.schema import QueueBlocklist
    require_user_ownership(current_user.id, user_id, "queue_blocklist")

    items = (await db.execute(
        select(QueueBlocklist)
        .where(QueueBlocklist.user_id == user_id)
        .order_by(QueueBlocklist.blocked_at.desc())
    )).scalars().all()

    return {
        "count": len(items),
        "items": [
            {
                "id": i.id,
                "simkl_id": i.simkl_id,
                "title": i.title,
                "item_type": i.item_type,
                "blocked_at": i.blocked_at.isoformat() if i.blocked_at else None,
            }
            for i in items
        ],
    }


@router.delete("/api/queue/block/{block_id}")
async def unblock_queue_item(
    block_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove an item from the blocklist so it can appear in future queues."""
    from app.models.schema import QueueBlocklist

    item = (await db.execute(
        select(QueueBlocklist).where(QueueBlocklist.id == block_id)
    )).scalar_one_or_none()
    if not item:
        raise HTTPException(404, "blocklist entry not found")
    require_user_ownership(current_user.id, item.user_id, "queue_unblock")

    title = item.title
    await db.delete(item)
    await db.commit()
    log.info("queue.item_unblocked", block_id=block_id, title=title)
    return {"status": "ok", "unblocked": title}


@router.get("/api/sonarr/imported")
async def get_sonarr_imported():
    """Return all Sonarr-imported episodes stored in Redis.
    Used by Airing Soon card to show 'Imported' badge.
    Returns {"{tvdb_id}:S{s}E{e}": {...}, ...}
    """
    r = await get_redis()
    cursor = b"0"
    imported: dict = {}
    while True:
        cursor, keys = await r.scan(cursor, match="sonarr_imported:*", count=200)
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            val = await r.get(key)
            if val:
                val_str = val if isinstance(val, str) else val.decode()
                # Strip prefix to get "tvdb_id:SxEx"
                short_key = key_str.replace("sonarr_imported:", "")
                try:
                    imported[short_key] = _json.loads(val_str)
                except Exception:
                    imported[short_key] = {"raw": val_str}
        if cursor == b"0" or cursor == 0:
            break
    return imported


@router.get("/api/airing-soon/{user_id}")
async def get_airing_soon(
    user_id: int,
    days: int = Query(30, ge=1, le=90),
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Upcoming episodes (for shows already in the library) with premiere/
    finale badges and a days-until-air countdown."""
    require_user_ownership(current_user.id, user_id, "airing_soon")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "user not found")

    try:
        result = await airing_alerts_svc.get_airing_soon(user, days=days)
    except Exception:
        log.exception("airing_soon.fetch_failed", user_id=user_id)
        raise HTTPException(502, "failed to fetch airing calendar from Simkl")

    alerts = result.get("items", [])
    home_releases = result.get("upcoming_home_releases", [])

    # Fire watchlist sync in background — ensures missing Radarr/Sonarr
    # items are on the Simkl/MDBList watchlist so they appear in Airing Soon.
    # Throttled: runs at most once every 30 minutes per user.
    r = await get_redis()
    throttle_key = f"watchlist_sync:last_run:{user_id}"
    already_ran = await r.get(throttle_key)
    if not already_ran:
        from app.services.watchlist_sync.service import WatchlistSyncService
        _wls = WatchlistSyncService()

        async def _bg_sync():
            try:
                await r.setex(throttle_key, 1800, "1")  # 30 min throttle
                await _wls._sync_user(user)
            except Exception:
                log.warning("watchlist_sync.background_failed", user_id=user_id)

        background_tasks.add_task(_bg_sync)

    return {
        "count": len(alerts),
        "items": alerts,
        "upcoming_home_releases": home_releases,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Feature #2 — ML Rating Predictor
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/ml/train/{user_id}")
async def train_model(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    """Trigger model training for a specific user."""
    require_user_ownership(current_user.id, user_id, "ml_training")  # ✅ SECURITY
    
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    if not user.simkl_access_token:
        raise HTTPException(400, "User not linked to Simkl")
    result = await ml_predictor_svc.train_for_user(user)
    return result


@router.get("/ml/predictions/{user_id}")
async def get_predictions(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),  # ✅ SECURITY: Input validation
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
):
    require_user_ownership(current_user.id, user_id, "predictions")  # ✅ SECURITY
    return await ml_predictor_svc.get_predictions(user_id, limit)


@router.get("/ml/model/{user_id}")
async def get_model_info(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    require_user_ownership(current_user.id, user_id, "model_info")  # ✅ SECURITY
    
    model = (await db.execute(
        select(MLModel).where(MLModel.user_id == user_id, MLModel.is_active == True)
    )).scalar_one_or_none()
    if not model:
        return {"status": "no_model"}

    # Genre insights from the latest bias analysis, if one exists
    genre_insights = None
    try:
        from app.models.schema import RatingBias
        bias = (await db.execute(
            select(RatingBias).where(RatingBias.user_id == user_id)
        )).scalar_one_or_none()
        if bias and bias.analysis_json:
            genre_stats = bias.analysis_json.get("genre_biases", {})
            top = sorted(genre_stats.items(), key=lambda kv: kv[1].get("count", 0), reverse=True)[:6]
            genre_insights = {
                genre: {
                    "delta": s.get("bias_score", 0.0),
                    "user_avg": s.get("avg", 0.0),
                    "simkl_avg": round(s.get("avg", 0.0) - s.get("bias_score", 0.0), 1),
                }
                for genre, s in top
            }
    except Exception:
        genre_insights = None

    import math
    def _finite(v):
        return v if (v is not None and isinstance(v, (int, float)) and math.isfinite(v)) else None
    mae_val = _finite(model.mae)
    r2_val = _finite(model.r2)

    return {
        "version": model.version,
        "training_samples": model.training_samples,
        "mae": mae_val,
        "r2": r2_val,
        "accuracy": r2_val,
        "feature_count": model.feature_count,
        "genre_insights": genre_insights,
        "trained_at": model.trained_at.isoformat() if model.trained_at else None,
    }


@router.get("/ml/drift/{user_id}")
async def get_rating_drift(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Return taste drift data — how feature importances shifted over training runs."""
    require_user_ownership(current_user.id, user_id, "drift")
    return await ml_predictor_svc.get_drift(user_id)


# ═══════════════════════════════════════════════════════════════════════════
# Missed Scrobble Audit
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/scrobble-audit/{user_id}")
async def scrobble_audit(
    user_id: int,
    force: bool = False,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Emby played items vs Simkl history — surface missed scrobbles."""
    require_user_ownership(current_user.id, user_id, "scrobble_audit")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await scrobble_audit_svc.run_audit(user, force=force)


@router.post("/api/scrobble-audit/{user_id}/backfill")
async def scrobble_backfill(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backfill selected items to Simkl history.

    Body: {items: [{type, imdb_id, tmdb_id, title}, ...]}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_backfill")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    body = await request.json()
    items = body.get("items", [])
    if not items:
        raise HTTPException(400, "No items provided")
    return await scrobble_audit_svc.backfill(user, items)


@router.post("/api/scrobble-audit/{user_id}/backfill-all")
async def scrobble_backfill_all(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backfill ALL missed scrobbles to Simkl history."""
    require_user_ownership(current_user.id, user_id, "scrobble_backfill_all")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    audit = await scrobble_audit_svc.run_audit(user)
    all_items = audit.get("movies", []) + audit.get("shows", [])
    if not all_items:
        return {"added": 0, "message": "Nothing to backfill"}
    return await scrobble_audit_svc.backfill(user, all_items)


@router.post("/api/scrobble-audit/{user_id}/dismiss")
async def scrobble_dismiss(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    """Dismiss an item from the scrobble audit list.

    Body: {emby_id: "..."}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_dismiss")
    body = await request.json()
    emby_id = body.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id required")
    return await scrobble_audit_svc.dismiss_item(user_id, emby_id)


@router.post("/api/scrobble-audit/{user_id}/undismiss")
async def scrobble_undismiss(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    """Re-enable a previously dismissed audit item.

    Body: {emby_id: "..."}
    """
    require_user_ownership(current_user.id, user_id, "scrobble_undismiss")
    body = await request.json()
    emby_id = body.get("emby_id")
    if not emby_id:
        raise HTTPException(400, "emby_id required")
    return await scrobble_audit_svc.undismiss_item(user_id, emby_id)


@router.post("/api/scrobble-audit/{user_id}/clear-dismissals")
async def scrobble_clear_dismissals(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Remove all dismissed items so they reappear in the scrobble audit."""
    require_user_ownership(current_user.id, user_id, "scrobble_clear_dismissals")
    return await scrobble_audit_svc.clear_all_dismissals(user_id)


# ═══════════════════════════════════════════════════════════════════════════
# Feature #3 — Shared Universe Discovery
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/universes/scan")
async def scan_universes(_user: User = Depends(get_current_user)):
    """Trigger a full universe scan."""
    asyncio.create_task(universe_svc.run_scan())
    return {"status": "scan_started"}


@router.get("/api/universes")
async def list_universes():
    return await universe_svc.get_universes()


@router.post("/api/universes")
async def create_universe(payload: dict, _user: User = Depends(get_current_user)):
    """Create a new custom universe.

    Payload: {"name": "...", "description": "..."}
    """
    name = (payload.get("name") or "").strip()
    if not name:
        return {"status": "error", "reason": "name_required"}

    description = (payload.get("description") or "").strip() or None

    # Generate slug from name
    import re
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        return {"status": "error", "reason": "invalid_name"}

    async with async_session_ctx() as db:
        # Check for duplicate name or slug
        existing = (await db.execute(
            select(Universe).where(
                (Universe.name == name) | (Universe.slug == slug)
            )
        )).scalar_one_or_none()
        if existing:
            return {"status": "error", "reason": "universe_already_exists"}

        universe = Universe(
            name=name,
            slug=slug,
            description=description,
            total_items=0,
            is_custom=True,
        )
        db.add(universe)
        await db.commit()
        await db.refresh(universe)

    return {
        "status": "ok",
        "id": universe.id,
        "name": universe.name,
        "slug": universe.slug,
    }


@router.delete("/api/universes/{universe_id}")
async def delete_universe(universe_id: int, _user: User = Depends(get_current_user)):
    """Delete an entire universe and all its items."""
    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        name = universe.name
        await db.delete(universe)  # cascade deletes items
        await db.commit()
    return {"status": "ok", "removed": name}


@router.put("/api/universes/{universe_id}/settings")
async def update_universe_settings(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Update universe display settings (playlist toggle, custom name, description).

    Payload: {"playlist_enabled": bool, "custom_name": str|null, "description": str|null, "quality_pref": "hd"|"4k"|null}
    """
    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        if "playlist_enabled" in payload:
            universe.playlist_enabled = bool(payload["playlist_enabled"])
        if "custom_name" in payload:
            val = (payload["custom_name"] or "").strip() or None
            universe.custom_name = val
        if "description" in payload:
            universe.description = (payload["description"] or "").strip() or None

        await db.commit()

    # Quality preference stored in Redis (no migration needed)
    if "quality_pref" in payload:
        from app.utils.redis_cache import get_redis
        r = await get_redis()
        qp = payload["quality_pref"]
        if qp in ("hd", "4k"):
            await r.set(f"universe:{universe_id}:quality_pref", qp)
        else:
            await r.delete(f"universe:{universe_id}:quality_pref")

    # Read back quality_pref
    from app.utils.redis_cache import get_redis
    r = await get_redis()
    qp_val = await r.get(f"universe:{universe_id}:quality_pref")

    return {
        "status": "ok",
        "playlist_enabled": bool(universe.playlist_enabled),
        "custom_name": universe.custom_name,
        "description": universe.description,
        "quality_pref": qp_val or None,
    }


@router.get("/api/universes/export")
async def export_universes():
    """Export all universes and their items as JSON for backup/transfer."""
    async with async_session_ctx() as db:
        result = await db.execute(
            select(Universe).options(selectinload(Universe.items)).order_by(Universe.name)
        )
        universes = result.scalars().all()

        export = []
        for u in universes:
            items = sorted(u.items, key=lambda i: (i.release_order or 0))
            export.append({
                "name": u.name,
                "slug": u.slug,
                "description": u.description,
                "is_custom": u.is_custom,
                "playlist_enabled": u.playlist_enabled,
                "custom_name": u.custom_name,
                "items": [
                    {
                        "title": item.title,
                        "year": item.year,
                        "item_type": item.item_type,
                        "release_order": item.release_order,
                        "chronological_order": item.chronological_order,
                        "simkl_id": item.simkl_id,
                        "imdb_id": item.imdb_id,
                        "tmdb_id": item.tmdb_id,
                    }
                    for item in items
                ],
            })

    return {"universes": export, "count": len(export)}


@router.post("/api/universes/import")
async def import_universes(request: Request, _user: User = Depends(get_current_user)):
    """Import universes from JSON. Skips universes that already exist (by slug).

    Accepts either raw JSON body or multipart form upload with field 'file'.
    """
    import json as _json

    content_type = request.headers.get("content-type", "")
    if "multipart" in content_type:
        form = await request.form()
        upload = form.get("file")
        if not upload:
            return {"status": "error", "reason": "no_file"}
        raw = await upload.read()
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            return {"status": "error", "reason": "invalid_json"}
    else:
        data = await request.json()

    universe_list = data.get("universes", [])
    if not universe_list:
        return {"status": "error", "reason": "no_universes_in_payload"}

    created = 0
    skipped = 0

    async with async_session_ctx() as db:
        for u_data in universe_list:
            name = (u_data.get("name") or "").strip()
            slug = (u_data.get("slug") or "").strip()
            if not name or not slug:
                skipped += 1
                continue

            existing = (await db.execute(
                select(Universe).where(Universe.slug == slug)
            )).scalar_one_or_none()
            if existing:
                skipped += 1
                continue

            universe = Universe(
                name=name,
                slug=slug,
                description=u_data.get("description"),
                total_items=len(u_data.get("items", [])),
                is_custom=u_data.get("is_custom", False),
                playlist_enabled=u_data.get("playlist_enabled", False),
                custom_name=u_data.get("custom_name"),
            )
            db.add(universe)
            await db.flush()

            for item_data in u_data.get("items", []):
                db.add(UniverseItem(
                    universe_id=universe.id,
                    title=item_data.get("title", "Unknown"),
                    year=item_data.get("year"),
                    item_type=item_data.get("item_type", "movie"),
                    release_order=item_data.get("release_order", 0),
                    chronological_order=item_data.get("chronological_order", 0),
                    simkl_id=item_data.get("simkl_id"),
                    imdb_id=item_data.get("imdb_id"),
                    tmdb_id=item_data.get("tmdb_id"),
                    in_library=False,
                    watched=False,
                ))

            created += 1

        await db.commit()

    return {"status": "ok", "created": created, "skipped": skipped}


@router.post("/api/universes/{universe_id}/save-order")
async def save_universe_order(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Persist item order to DB without creating an Emby playlist.

    Payload: {"item_ids": [db_item_id_1, db_item_id_2, ...]}
    """
    item_ids = payload.get("item_ids", [])
    if not item_ids:
        return {"status": "error", "reason": "no_item_ids"}

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        items = (await db.execute(
            select(UniverseItem).where(UniverseItem.universe_id == universe_id)
        )).scalars().all()
        id_to_item = {i.id: i for i in items}
        updated = 0
        for pos, item_id in enumerate(item_ids):
            item_id_int = int(item_id) if not isinstance(item_id, int) else item_id
            if item_id_int in id_to_item:
                id_to_item[item_id_int].release_order = pos + 1
                updated += 1

        await db.commit()

    log.info("universe.order_saved", universe_id=universe_id, items=updated)
    return {"status": "ok", "items": updated}


@router.post("/api/universes/{universe_id}/reorder")
async def reorder_universe(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Reorder items within a universe, persist to DB, and recreate Emby playlist.

    Payload: {"item_ids": [db_item_id_1, db_item_id_2, ...]}
    The order of IDs is the new watch order.  Updates release_order on
    each UniverseItem row so the order survives scans and restarts.
    """
    item_ids = payload.get("item_ids", [])
    if not item_ids:
        return {"status": "error", "reason": "no_item_ids"}

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        # Update release_order on each item to match the new order
        items = (await db.execute(
            select(UniverseItem).where(UniverseItem.universe_id == universe_id)
        )).scalars().all()
        id_to_item = {i.id: i for i in items}
        emby_ids = []
        for pos, item_id in enumerate(item_ids):
            item_id_int = int(item_id) if not isinstance(item_id, int) else item_id
            if item_id_int in id_to_item:
                id_to_item[item_id_int].release_order = pos + 1
                if id_to_item[item_id_int].emby_item_id:
                    emby_ids.append(id_to_item[item_id_int].emby_item_id)

        await db.commit()

        first_user = (await db.execute(
            select(User).order_by(User.id)
        )).scalars().first()
        emby_user_id = first_user.emby_user_id if first_user else None

    # Recreate Emby playlist with new order
    if emby_ids:
        emby = EmbyClient()
        display_name = universe.custom_name or universe.name
        playlist_name = f"🌌 {display_name}"
        try:
            playlist_id = await emby.recreate_playlist(
                playlist_name, emby_ids, user_id=emby_user_id,
            )
            if playlist_id and universe.description:
                await emby.set_playlist_overview(
                    playlist_id, universe.description,
                    user_id=emby_user_id,
                )
        finally:
            await emby.close()
    else:
        playlist_id = None

    log.info("universe.reordered", universe_id=universe_id, items=len(item_ids))
    return {"status": "ok", "playlist_id": playlist_id, "items": len(item_ids)}


@router.post("/api/universes/{universe_id}/items")
async def add_universe_item(universe_id: int, payload: dict, _user: User = Depends(get_current_user)):
    """Add a custom item to a universe.

    Payload: {"title": "...", "year": 2024, "imdb_id": "tt1234567", "item_type": "movie"}
    """
    title = (payload.get("title") or "").strip()
    if not title:
        return {"status": "error", "reason": "title_required"}

    year = payload.get("year")
    imdb_id = (payload.get("imdb_id") or "").strip() or None
    tmdb_id = (payload.get("tmdb_id") or "").strip() or None
    item_type = payload.get("item_type", "movie")

    async with async_session_ctx() as db:
        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if not universe:
            return {"status": "error", "reason": "universe_not_found"}

        # Determine next release_order
        max_order = (await db.execute(
            select(func.max(UniverseItem.release_order)).where(
                UniverseItem.universe_id == universe_id
            )
        )).scalar() or 0

        new_item = UniverseItem(
            universe_id=universe_id,
            title=title,
            year=int(year) if year else None,
            imdb_id=imdb_id,
            tmdb_id=tmdb_id,
            item_type=item_type,
            release_order=max_order + 1,
            chronological_order=max_order + 1,
            in_library=False,
            watched=False,
        )
        db.add(new_item)
        universe.total_items = (universe.total_items or 0) + 1
        await db.commit()
        await db.refresh(new_item)

    # Trigger a library match for the new item
    asyncio.create_task(universe_svc.run_scan())

    return {
        "status": "ok",
        "item_id": new_item.id,
        "title": new_item.title,
        "message": f"Added '{title}' — library match running in background",
    }


@router.delete("/api/universes/{universe_id}/items/{item_id}")
async def remove_universe_item(universe_id: int, item_id: int, _user: User = Depends(get_current_user)):
    """Remove a custom item from a universe."""
    async with async_session_ctx() as db:
        item = (await db.execute(
            select(UniverseItem).where(
                UniverseItem.id == item_id,
                UniverseItem.universe_id == universe_id,
            )
        )).scalar_one_or_none()
        if not item:
            return {"status": "error", "reason": "item_not_found"}

        title = item.title
        await db.delete(item)

        universe = (await db.execute(
            select(Universe).where(Universe.id == universe_id)
        )).scalar_one_or_none()
        if universe and universe.total_items:
            universe.total_items = max(0, universe.total_items - 1)

        await db.commit()
    return {"status": "ok", "removed": title}


@router.get("/api/universes/auto-discover/setting")
async def get_auto_discover_setting(db: AsyncSession = Depends(get_db)):
    """Return auto-discovery setting: enabled | disabled | unset (never configured)."""
    val = await _get_setting(db, "universe_auto_discover", "")
    if val == "":
        return {"status": "unset"}
    return {"status": val}


@router.put("/api/universes/auto-discover/setting")
async def set_auto_discover_setting(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Set auto-discovery to 'enabled' or 'disabled'."""
    enabled = payload.get("enabled", False)
    val = "enabled" if enabled else "disabled"
    await _put_setting(db, "universe_auto_discover", val)
    await db.commit()
    return {"status": val}


# ═══════════════════════════════════════════════════════════════════════════
# Feature #4 — Watch Party
# ═══════════════════════════════════════════════════════════════════════════

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
# Theater Mode — Pick Together
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
# Emby Webhook receiver
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# Webhook — Sonarr (import complete / grab / series added)
# ═══════════════════════════════════════════════════════════════════════════

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
# Webhook — Emby (playback, scrobble, mark played)
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

async def _activity_log(message: str, category: str = "general"):
    """Append an entry to the activity log in Redis and push to dashboard."""
    import json as _json
    try:
        r = await get_redis()
        entry_dict = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "cat": category,
            "msg": message,
        }
        entry = _json.dumps(entry_dict)
        await r.lpush("activity_log", entry)
        await r.ltrim("activity_log", 0, 99)  # keep last 100
        # Push to any connected dashboard clients
        try:
            from app.services.watch_party.service import sio
            await sio.emit("activity_entry", entry_dict)
        except Exception:
            pass
        # Fire notification if message matches an enabled event type
        _maybe_notify(message)
    except Exception:
        pass  # logging should never crash the request


def _maybe_notify(message: str) -> None:
    """Pattern-match activity log messages to notification event types.
    Fire-and-forget — never blocks, never crashes."""
    import re as _re
    from app.utils.notification_client import notify
    msg = message.strip()
    # Consolidated playback notifications (start/stop with sync status)
    if msg.startswith("Started Watching:"):
        title = msg.split(":", 1)[1].strip()
        title = _re.sub(r'\s*—\s*(Synced|Sync error)$', '', title)
        notify("scrobble", "▶️ Started Watching", title)
    elif msg.startswith("Stopped Watching:"):
        title = msg.split(":", 1)[1].strip()
        title = _re.sub(r'\s*—\s*(Synced|Sync error.*)$', '', title)
        synced = "Synced" in msg
        notify("scrobble", "⏹️ Stopped Watching" + (" ✓" if synced else " ⚠"), title)
    # Legacy patterns (kept for backwards compatibility)
    elif msg.startswith("✓ Simkl scrobbled:") or msg.startswith("✓ Synced to Simkl:"):
        title = msg.split(":", 1)[1].strip() if ":" in msg else msg
        title = _re.sub(r'\s*\(\d+%?\)$', '', title)
        notify("scrobble", "🎬 Simkl Sync", title)
    elif msg.startswith("✓ Synced to MDBList:"):
        title = msg.split(":", 1)[1].strip() if ":" in msg else msg
        title = _re.sub(r'\s*\(\d+%?\)$', '', title)
        notify("scrobble", "🎬 MDBList Sync", title)
    # System errors
    elif "failed" in msg.lower() and ("token" in msg.lower() or "error" in msg.lower()):
        notify("system", "⚠️ System Alert", msg)



# ═══════════════════════════════════════════════════════════════════════════
# Scheduler status
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/scheduler/status")
async def scheduler_status():
    """Return last-run status for each scheduled job."""
    import json as _json
    from app.main import _job_crons
    r = await get_redis()
    jobs = {}
    for job_id, cron in _job_crons.items():
        raw = await r.get(f"scheduler:status:{job_id}")
        if raw:
            data = _json.loads(raw)
        else:
            data = {"last_run": None, "status": "pending", "duration_s": None, "error": None}
        data["cron"] = cron
        jobs[job_id] = data
    return jobs


# ═══════════════════════════════════════════════════════════════════════════
# Job completion events (toast notifications)
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/dashboard-poll")
async def dashboard_poll(
    category: str = Query(default=None),
):
    """Consolidated polling endpoint for dashboard.

    Returns health, activity, and job-completion events in a single response,
    replacing three separate polled endpoints.
    """
    import json as _json
    r = await get_redis()

    # --- Health ---
    cache_stats = await LibraryCache.get_stats()

    # --- Activity ---
    fetch_count = 99 if category else 29
    raw_activity = await r.lrange("activity_log", 0, fetch_count)
    entries = []
    limit = 30
    for item in raw_activity:
        try:
            entry = _json.loads(item)
            if category and entry.get("cat") != category:
                continue
            entries.append(entry)
            if len(entries) >= limit:
                break
        except Exception:
            pass

    # --- Job completions (consuming) ---
    job_events = []
    while True:
        raw = await r.rpop("job_completions")
        if raw is None:
            break
        try:
            job_events.append(_json.loads(raw))
        except Exception:
            pass

    return {
        "health": {
            "status": "ok",
            "features": {
                "smart_queue": settings.enable_smart_queue,
                "ml_predictor": settings.enable_ml_predictor,
                "universe_discovery": settings.enable_universe_discovery,
                "watch_party": settings.enable_watch_party,
            },
            "library_cache": cache_stats,
        },
        "activity": entries,
        "job_completions": job_events,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SSL Certificate Status
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/ssl/status")
async def ssl_status():
    """Return SSL certificate expiry info.

    Reads the latest result written by the scheduled ssl_cert_check job.
    If SSL_DOMAIN is not set, returns disabled status.
    """
    import json as _json
    domain = settings.ssl_domain
    if not domain:
        return {"enabled": False, "message": "SSL_DOMAIN not set in .env"}

    r = await get_redis()
    raw = await r.get("ssl:cert_status")
    if raw:
        data = _json.loads(raw)
        data["enabled"] = True
        return data

    # No cached result yet — do a live check
    result = await _check_ssl_cert(domain)
    result["enabled"] = True
    return result


async def _check_ssl_cert(domain: str) -> dict:
    """Connect to domain over TLS and read certificate expiry."""
    import ssl
    import socket
    import asyncio

    def _fetch_cert(d: str) -> dict:
        ctx = ssl.create_default_context()
        with socket.create_connection((d, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=d) as ssock:
                return ssock.getpeercert()

    try:
        cert = await asyncio.to_thread(_fetch_cert, domain)

        not_after_str = cert.get("notAfter", "")
        not_before_str = cert.get("notBefore", "")
        # e.g. "Sep 28 12:00:00 2026 GMT"
        from datetime import datetime as _dt
        not_after = _dt.strptime(not_after_str, "%b %d %H:%M:%S %Y %Z")
        not_before = _dt.strptime(not_before_str, "%b %d %H:%M:%S %Y %Z")
        days_left = (not_after - _dt.now(timezone.utc).replace(tzinfo=None)).days

        issuer_parts = dict(x[0] for x in cert.get("issuer", ()))
        issuer = issuer_parts.get("organizationName", issuer_parts.get("commonName", "Unknown"))

        subject_parts = dict(x[0] for x in cert.get("subject", ()))
        cn = subject_parts.get("commonName", domain)

        san = [entry[1] for entry in cert.get("subjectAltName", ())]

        status = "ok" if days_left > 30 else "expiring_soon" if days_left > 7 else "critical" if days_left > 0 else "expired"

        return {
            "domain": domain,
            "common_name": cn,
            "issuer": issuer,
            "not_before": not_before.strftime("%Y-%m-%d %H:%M:%S"),
            "not_after": not_after.strftime("%Y-%m-%d %H:%M:%S"),
            "days_left": days_left,
            "status": status,
            "san": san,
            "checked_at": _dt.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "error": None,
        }

    except Exception as e:
        return {
            "domain": domain,
            "status": "error",
            "days_left": None,
            "error": str(e)[:200],
            "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Library Cache management
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/cache/rebuild")
async def rebuild_cache(_user: User = Depends(get_current_user)):
    """Manually trigger library cache rebuild."""
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        summary = await LibraryCache.index_library(emby, user_id=uid)
    return {"status": "rebuilt", **summary}


@router.get("/cache/stats")
async def cache_stats():
    return await LibraryCache.get_stats()


@router.post("/cache/clear")
async def clear_cache(_user: User = Depends(get_current_user)):
    return await LibraryCache.clear()


@router.get("/api/libraries")
async def list_libraries():
    """Return Emby library folders (virtual folders)."""
    async with EmbyClient() as emby:
        return await emby.get_virtual_folders()


@router.get("/api/libraries/stats")
async def library_stats():
    """Return media libraries (no collections/playlists) with item counts."""
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        folders = await emby.get_virtual_folders()
        # Only media libraries — filter out boxsets, playlists, music, etc.
        media_types = {"movies", "tvshows"}
        results = []
        for f in folders:
            ct = f.get("collection_type", "")
            if ct not in media_types:
                continue
            # Count items — Movie for movies, Series for tvshows (not seasons/episodes)
            item_type = "Movie" if ct == "movies" else "Series"
            try:
                resp = await emby.get_items(
                    user_id=uid,
                    parent_id=f.get("item_id"),
                    item_type=item_type,
                    fields="",
                    limit=0,
                )
                count = resp.get("TotalRecordCount", 0)
            except Exception:
                count = 0
            results.append({
                "name": f.get("name", ""),
                "collection_type": ct,
                "item_count": count,
            })
    return results


@router.get("/api/library/search")
async def library_search(q: str = Query(..., min_length=2, max_length=100)):
    """Search Emby library by title (used by the watch party item picker).

    Returns resolution/quality info so users can distinguish 1080p from 4K
    when duplicates exist.
    """
    async with EmbyClient() as emby:
        uid = await _first_emby_user_id()
        resp = await emby.get_items(
            user_id=uid,
            search_term=q,
            item_type=None,
            fields="ProviderIds,Genres,Overview,People,Studios,RunTimeTicks,MediaSources",
            limit=20,
        )
    results = []
    for it in resp.get("Items", []):
        if it.get("Type") not in ("Movie", "Series", "Episode"):
            continue

        # Extract resolution/quality from MediaSources
        quality = ""
        media_sources = it.get("MediaSources") or []
        if media_sources:
            ms = media_sources[0]
            # Video stream resolution
            for stream in ms.get("MediaStreams", []):
                if stream.get("Type") == "Video":
                    w = stream.get("Width", 0)
                    h = stream.get("Height", 0)
                    if w >= 3840 or h >= 2160:
                        quality = "4K"
                    elif w >= 1920 or h >= 1080:
                        quality = "1080p"
                    elif w >= 1280 or h >= 720:
                        quality = "720p"
                    elif w > 0:
                        quality = f"{h}p"
                    # Add HDR if present
                    if stream.get("VideoRangeType") in ("HDR10", "HDR10Plus", "DolbyVision", "HLG"):
                        quality += " HDR"
                    elif stream.get("VideoRange") == "HDR":
                        quality += " HDR"
                    break
            # Add container/codec info
            container = ms.get("Container", "")
            if container:
                quality += f" ({container})" if quality else container

        results.append({
            "id": it.get("Id"),
            "title": it.get("Name"),
            "year": it.get("ProductionYear"),
            "type": it.get("Type"),
            "quality": quality,
        })

    return results[:15]


# ═══════════════════════════════════════════════════════════════════════════
# HTML Pages
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/setup", response_class=HTMLResponse)
async def get_setup_page():
    """Serve the first-run integration provider setup page."""
    try:
        with open("frontend/templates/setup.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Setup page not found</h1>"


@router.get("/lists", response_class=HTMLResponse)
@router.get("/universes", response_class=HTMLResponse)
async def get_universes_page():
    """Serve the lists page (renamed from universes)."""
    try:
        with open("frontend/templates/universes.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/predictions", response_class=HTMLResponse)
async def get_predictions_page():
    """Serve the ML predictions chart page."""
    try:
        with open("frontend/templates/predictions.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/settings", response_class=HTMLResponse)
async def get_settings_page():
    """Serve the settings configuration page."""
    try:
        with open("frontend/templates/settings.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/watch-party", response_class=HTMLResponse)
async def get_watch_party_page(code: str = None):
    """Serve the watch party chat page."""
    try:
        with open("frontend/templates/watch_party.html", "r") as f:
            html = f.read()
        # Inject party code if provided (sanitised: alphanumeric only)
        if code:
            safe_code = re.sub(r"[^A-Za-z0-9]", "", code)[:12]
            if safe_code:
                html = html.replace("const partyCode = null;", f"const partyCode = '{safe_code}';")
        return html
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/bias", response_class=HTMLResponse)
async def get_bias_page():
    """Serve the Rating Bias Detector analysis page."""
    try:
        with open("frontend/templates/bias_detector.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


# ═══════════════════════════════════════════════════════════════════════════
# Rating Bias Detector API
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/bias/analyze/{user_id}")
async def analyze_bias(user_id: int, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Trigger bias analysis for a user."""
    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    result = await bias_detector_svc.analyze_user(user)
    return result


@router.get("/bias/report/{user_id}")
async def get_bias_report(user_id: int):
    """Get full bias report for a user."""
    report = await bias_detector_svc.get_bias_report(user_id)
    if not report:
        raise HTTPException(status_code=404, detail="Bias report not found. Run analysis first.")
    return report


@router.get("/bias/hidden-gems/{user_id}")
async def get_hidden_gems(user_id: int, limit: int = 20):
    """Get hidden gems (items user should rate higher based on patterns)."""
    gems = await bias_detector_svc.get_hidden_gems(user_id, limit)
    return {"gems": gems}


@router.get("/bias/challenges/{user_id}")
async def get_challenges(user_id: int):
    """Get rating challenges to explore blind spots."""
    challenges = await bias_detector_svc.get_challenges(user_id)
    return {"challenges": challenges}


@router.get("/bias/library-matches/{user_id}")
async def get_library_matches(user_id: int, criteria: str):
    """Find Emby library items matching a challenge/gem profile.
    
    criteria: genre:ACTION, era:1990s, against:diversity
    """
    matches = await bias_detector_svc.find_library_matches(user_id, criteria)
    return {"matches": matches}


# ═══════════════════════════════════════════════════════════════════════════
# Database Backup / Restore
# ═══════════════════════════════════════════════════════════════════════════

def _parse_db_url(url: str) -> tuple[str, str, str, str]:
    """Parse DATABASE_URL into (user, password, host, dbname).

    Handles: postgresql+asyncpg://user:pass@host:port/dbname
    """
    from urllib.parse import urlparse, unquote
    parsed = urlparse(url)
    user = unquote(parsed.username or "embysimkl")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "postgres"
    dbname = (parsed.path or "/embysimkl").lstrip("/")
    return user, password, host, dbname

@router.post("/api/db/backup")
async def create_db_backup(_user: User = Depends(get_current_user)):
    """Create a pg_dump backup and return a download token."""
    import subprocess
    import uuid

    backup_dir = "/app/cache/backups"
    os.makedirs(backup_dir, exist_ok=True)
    backup_id = uuid.uuid4().hex[:12]
    filename = f"emby-simkl-backup-{backup_id}.sql"
    filepath = os.path.join(backup_dir, filename)

    # Parse connection details from DATABASE_URL
    # Format: postgresql+asyncpg://user:pass@host:port/dbname
    db_url = os.environ.get("DATABASE_URL", "")
    db_user, db_pass, db_host, db_name = _parse_db_url(db_url)

    env = {**os.environ, "PGPASSWORD": db_pass}
    try:
        result = subprocess.run(
            ["pg_dump", "-h", db_host, "-U", db_user, "-d", db_name, "-f", filepath],
            capture_output=True, text=True, env=env, timeout=120,
        )
    except FileNotFoundError:
        return {"status": "error", "reason": "pg_dump not found — rebuild the container image to install postgresql-client"}

    if result.returncode != 0:
        return {"status": "error", "reason": result.stderr[:300]}

    size_bytes = os.path.getsize(filepath)
    return {
        "status": "ok",
        "backup_id": backup_id,
        "filename": filename,
        "size_bytes": size_bytes,
    }


@router.get("/api/db/backup/{backup_id}")
async def download_db_backup(backup_id: str, _user: User = Depends(get_current_user)):
    """Download a previously created backup file."""
    import re
    from fastapi.responses import FileResponse

    # SECURITY: backup_id must be hex-only (generated by uuid4().hex[:12])
    if not re.fullmatch(r"[a-f0-9]{1,24}", backup_id):
        raise HTTPException(400, "Invalid backup ID")

    filepath = f"/app/cache/backups/emby-simkl-backup-{backup_id}.sql"
    if not os.path.isfile(filepath):
        raise HTTPException(404, "Backup not found — create one first")
    return FileResponse(
        filepath,
        media_type="application/sql",
        filename=os.path.basename(filepath),
    )


@router.post("/api/db/restore")
async def restore_db_backup(request: Request, _user: User = Depends(get_current_user)):
    """Restore a database from an uploaded .sql backup.

    Accepts multipart form upload with fields:
      - 'file': the .sql backup file (max 50 MB)
      - 'confirm': must be the string "RESTORE" to proceed

    WARNING: This overwrites all current data.
    """
    import subprocess
    import re as _re

    MAX_RESTORE_SIZE = 50 * 1024 * 1024  # 50 MB

    form = await request.form()

    # Require explicit confirmation
    confirm = form.get("confirm", "")
    if confirm != "RESTORE":
        raise HTTPException(400, "Confirmation required: include form field confirm=RESTORE")

    upload = form.get("file")
    if not upload:
        raise HTTPException(400, "No file uploaded")

    # Validate filename extension
    filename = getattr(upload, "filename", "") or ""
    if not filename.lower().endswith(".sql"):
        raise HTTPException(400, "Only .sql files are accepted")

    # Read with size limit — stream in chunks to avoid OOM
    chunks = []
    total_size = 0
    while True:
        chunk = await upload.read(1024 * 64)  # 64 KB chunks
        if not chunk:
            break
        total_size += len(chunk)
        if total_size > MAX_RESTORE_SIZE:
            raise HTTPException(413, f"File too large — maximum {MAX_RESTORE_SIZE // (1024*1024)} MB")
        chunks.append(chunk)

    contents = b"".join(chunks)

    if total_size == 0:
        raise HTTPException(400, "Uploaded file is empty")

    # Basic content validation — must look like a pg_dump SQL file
    # Check first 4 KB for SQL-like content
    head = contents[:4096].decode("utf-8", errors="replace")

    # Reject if it looks like binary (not text)
    non_printable = sum(1 for c in head if ord(c) < 32 and c not in '\n\r\t')
    if non_printable > len(head) * 0.1:
        raise HTTPException(400, "File does not appear to be a text SQL file")

    # Must contain at least one pg_dump indicator
    pg_dump_markers = ("pg_dump", "SET statement_timeout", "CREATE TABLE", "COPY ", "INSERT INTO", "ALTER TABLE")
    has_marker = any(marker in head for marker in pg_dump_markers)
    if not has_marker:
        raise HTTPException(400, "File does not appear to be a valid pg_dump backup — no recognisable SQL statements found")

    # Reject dangerous statements that shouldn't be in a data restore
    dangerous_patterns = [
        r'\bCREATE\s+ROLE\b', r'\bCREATE\s+USER\b', r'\bALTER\s+ROLE\b',
        r'\bDROP\s+DATABASE\b', r'\bCREATE\s+DATABASE\b',
        r'\bCOPY\b.*\bFROM\s+PROGRAM\b', r'\bCREATE\s+EXTENSION\b.*\buntrusted\b',
    ]
    full_text = contents.decode("utf-8", errors="replace")
    for pat in dangerous_patterns:
        if _re.search(pat, full_text, _re.IGNORECASE):
            raise HTTPException(
                400,
                f"File contains disallowed SQL statement matching: {pat} — "
                "only data restore files from this application's pg_dump are accepted",
            )

    restore_path = "/app/cache/backups/restore_upload.sql"
    os.makedirs("/app/cache/backups", exist_ok=True)
    with open(restore_path, "wb") as f:
        f.write(contents)

    db_url = os.environ.get("DATABASE_URL", "")
    db_user, db_pass, db_host, db_name = _parse_db_url(db_url)

    env = {**os.environ, "PGPASSWORD": db_pass}
    try:
        result = subprocess.run(
            ["psql", "-h", db_host, "-U", db_user, "-d", db_name, "-f", restore_path],
            capture_output=True, text=True, env=env, timeout=120,
        )
    except FileNotFoundError:
        os.remove(restore_path)
        return {"status": "error", "reason": "psql not found — rebuild the container image to install postgresql-client"}

    os.remove(restore_path)

    if result.returncode != 0:
        return {"status": "error", "reason": result.stderr[:300]}

    log.warning("security.db_restored", user_id=_user.id, file_size=total_size)
    return {"status": "ok", "message": "Database restored. Restart the container for changes to take full effect."}


# ═══════════════════════════════════════════════════════════════════════════
# Radarr Integration
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/radarr/servers")
async def get_radarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Radarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("radarr_servers")
    if not raw:
        raw = (await _get_setting(db, "radarr_servers", ""))
    if not raw:
        return {"servers": []}
    try:
        servers = _json.loads(raw)
        for s in servers:
            s["api_key"] = _mask_api_key(s.get("api_key", ""))
        return {"servers": servers}
    except Exception:
        return {"servers": []}


@router.put("/api/radarr/servers")
async def save_radarr_servers(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save Radarr server configs to DB + Redis.

    Payload: {"servers": [{"name": "...", "url": "...", "api_key": "..."}, ...]}
    Max 2 servers. If api_key is masked (unchanged from GET), the stored key is preserved.
    """
    import json as _json
    servers = payload.get("servers", [])[:2]
    servers = await _resolve_servers(servers, "radarr_servers")
    clean = []
    for s in servers:
        if s.get("url") and s.get("api_key"):
            srv = {
                "name": s.get("name", "Radarr"),
                "url": s["url"].rstrip("/"),
                "api_key": s["api_key"],
            }
            if s.get("quality_profile_id"):
                srv["quality_profile_id"] = int(s["quality_profile_id"])
                srv["quality_profile_name"] = s.get("quality_profile_name", "")
            clean.append(srv)
    encoded = _json.dumps(clean)
    r = await get_redis()
    await secure_set("radarr_servers", encoded)
    await _put_setting(db, "radarr_servers", encoded)
    await db.commit()
    # Invalidate download-queue cache so the next poll picks up changes
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass
    return {"status": "ok", "servers": len(clean)}


@router.post("/api/radarr/test")
async def test_radarr_connection(payload: dict, _user: User = Depends(get_current_user)):
    """Test a Radarr server connection. Returns quality profiles on success."""
    from app.utils.radarr_client import RadarrClient
    url = payload.get("url", "")
    api_key = payload.get("api_key", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # If key is masked, resolve from stored config
    if not api_key or _is_masked(api_key):
        resolved = await _resolve_servers([{"url": url, "api_key": api_key or "x****"}], "radarr_servers")
        api_key = resolved[0]["api_key"] if resolved else ""
    if not api_key:
        return {"status": "error", "message": "API key required"}
    client = RadarrClient(url, api_key)
    result = await client.test_connection()
    if result.get("status") == "ok":
        try:
            profiles = await client.get_quality_profiles()
            result["quality_profiles"] = [
                {"id": p.get("id"), "name": p.get("name")}
                for p in profiles
            ]
        except Exception:
            result["quality_profiles"] = []
    await client.close()
    return result


@router.post("/api/radarr/add")
async def add_to_radarr(payload: dict, _user: User = Depends(get_current_user)):
    """Add movies to a Radarr server.

    Payload: {
      "server_index": 0,
      "movies": [{"tmdb_id": 123, "imdb_id": "tt...", "title": "...", "year": 2024}, ...]
    }
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient

    server_idx = payload.get("server_index", 0)
    movies = payload.get("movies", [])
    if not movies:
        raise HTTPException(400, "No movies provided")

    r = await get_redis()
    raw = await secure_get("radarr_servers")
    if not raw:
        raise HTTPException(400, "No Radarr servers configured — add one in Settings")
    servers = _json.loads(raw)
    if server_idx >= len(servers):
        raise HTTPException(400, f"Server index {server_idx} out of range")

    srv = servers[server_idx]
    client = RadarrClient(srv["url"], srv["api_key"], name=srv["name"])
    profile_id = srv.get("quality_profile_id")

    results = []
    for movie in movies:
        result = await client.add_movie(
            tmdb_id=movie.get("tmdb_id"),
            imdb_id=movie.get("imdb_id"),
            title=movie.get("title", ""),
            year=movie.get("year"),
            quality_profile_id=profile_id,
        )
        results.append(result)

    await client.close()

    added = sum(1 for r in results if r.get("status") == "ok")
    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(movies),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Sonarr Integration
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/sonarr/servers")
async def get_sonarr_servers(db: AsyncSession = Depends(get_db)):
    """Return configured Sonarr servers (Redis → DB fallback). API keys masked."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("sonarr_servers")
    if not raw:
        raw = (await _get_setting(db, "sonarr_servers", ""))
    if not raw:
        return {"servers": []}
    try:
        servers = _json.loads(raw)
        for s in servers:
            s["api_key"] = _mask_api_key(s.get("api_key", ""))
        return {"servers": servers}
    except Exception:
        return {"servers": []}


@router.put("/api/sonarr/servers")
async def save_sonarr_servers(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save Sonarr server configs to DB + Redis.

    Payload: {"servers": [{"name": "...", "url": "...", "api_key": "..."}, ...]}
    Max 2 servers. If api_key is masked (unchanged from GET), the stored key is preserved.
    """
    import json as _json
    servers = payload.get("servers", [])[:2]
    servers = await _resolve_servers(servers, "sonarr_servers")
    clean = []
    for s in servers:
        if s.get("url") and s.get("api_key"):
            srv = {
                "name": s.get("name", "Sonarr"),
                "url": s["url"].rstrip("/"),
                "api_key": s["api_key"],
            }
            if s.get("quality_profile_id"):
                srv["quality_profile_id"] = int(s["quality_profile_id"])
                srv["quality_profile_name"] = s.get("quality_profile_name", "")
            clean.append(srv)
    encoded = _json.dumps(clean)
    r = await get_redis()
    await secure_set("sonarr_servers", encoded)
    await _put_setting(db, "sonarr_servers", encoded)
    await db.commit()
    # Invalidate download-queue cache so the next poll picks up changes
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass
    return {"status": "ok", "servers": len(clean)}


@router.post("/api/sonarr/test")
async def test_sonarr_connection(payload: dict, _user: User = Depends(get_current_user)):
    """Test a Sonarr server connection. Returns quality profiles on success."""
    from app.utils.sonarr_client import SonarrClient
    url = payload.get("url", "")
    api_key = payload.get("api_key", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # If key is masked, resolve from stored config
    if not api_key or _is_masked(api_key):
        resolved = await _resolve_servers([{"url": url, "api_key": api_key or "x****"}], "sonarr_servers")
        api_key = resolved[0]["api_key"] if resolved else ""
    if not api_key:
        return {"status": "error", "message": "API key required"}
    client = SonarrClient(url, api_key)
    result = await client.test_connection()
    if result.get("status") == "ok":
        try:
            profiles = await client.get_quality_profiles()
            result["quality_profiles"] = [
                {"id": p.get("id"), "name": p.get("name")}
                for p in profiles
            ]
        except Exception:
            result["quality_profiles"] = []
    await client.close()
    return result


@router.post("/api/sonarr/add")
async def add_to_sonarr(payload: dict, _user: User = Depends(get_current_user)):
    """Add TV series to a Sonarr server.

    Payload: {
      "server_index": 0,
      "shows": [{"tvdb_id": 123, "imdb_id": "tt...", "title": "...", "year": 2024}, ...]
    }
    """
    import json as _json
    from app.utils.sonarr_client import SonarrClient

    server_idx = payload.get("server_index", 0)
    shows = payload.get("shows", [])
    if not shows:
        raise HTTPException(400, "No shows provided")

    r = await get_redis()
    raw = await secure_get("sonarr_servers")
    if not raw:
        raise HTTPException(400, "No Sonarr servers configured — add one in Settings")
    servers = _json.loads(raw)
    if server_idx >= len(servers):
        raise HTTPException(400, f"Server index {server_idx} out of range")

    srv = servers[server_idx]
    client = SonarrClient(srv["url"], srv["api_key"], name=srv["name"])
    profile_id = srv.get("quality_profile_id")

    from app.utils.tmdb_client import get_tv_external_ids

    results = []
    for show in shows:
        tvdb_id = show.get("tvdb_id")
        imdb_id = show.get("imdb_id")

        # Filmography sends carry only a TMDB ID for shows that aren't in
        # the library.  Sonarr keys on TVDB, and without one add_series
        # falls back to a title search that blind-picks the first result.
        # Resolve TMDB -> TVDB first so the match is exact.
        if not tvdb_id and show.get("tmdb_id"):
            ext = await get_tv_external_ids(int(show["tmdb_id"]))
            if ext:
                if ext.get("tvdb_id"):
                    tvdb_id = ext["tvdb_id"]
                    log.info("sonarr.tvdb_resolved_from_tmdb",
                             tmdb_id=show["tmdb_id"], tvdb_id=tvdb_id,
                             title=show.get("title"))
                if not imdb_id and ext.get("imdb_id"):
                    imdb_id = ext["imdb_id"]

        if not tvdb_id:
            log.info("sonarr.no_tvdb_using_title_lookup",
                     title=show.get("title"), year=show.get("year"))

        result = await client.add_series(
            tvdb_id=tvdb_id,
            imdb_id=imdb_id,
            title=show.get("title", ""),
            year=show.get("year"),
            quality_profile_id=profile_id,
        )
        # Surface how the series was matched so the UI can warn on a
        # title-only match, which is the fallible path
        result["matched_by"] = (
            "tvdb" if tvdb_id else ("title" if result.get("status") == "ok" else "none")
        )
        results.append(result)

    await client.close()

    added = sum(1 for r in results if r.get("status") == "ok")
    return {
        "status": "ok",
        "server": srv["name"],
        "added": added,
        "total": len(shows),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Auto-Send Toggles
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/auto-send")
async def get_auto_send_settings(db: AsyncSession = Depends(get_db)):
    """Read auto-send toggle state (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("auto_send_settings")
    if not raw:
        raw = await _get_setting(db, "auto_send_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    # Defaults: both off
    return {"radarr_enabled": False, "sonarr_enabled": False}


@router.put("/api/auto-send")
async def update_auto_send_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save auto-send toggle state to DB + Redis.

    Payload: {"radarr_enabled": true/false, "sonarr_enabled": true/false}
    """
    import json as _json
    r = await get_redis()
    auto_settings = {
        "radarr_enabled": bool(payload.get("radarr_enabled", False)),
        "sonarr_enabled": bool(payload.get("sonarr_enabled", False)),
    }
    encoded = _json.dumps(auto_settings)
    await r.set("auto_send_settings", encoded)
    await _put_setting(db, "auto_send_settings", encoded)
    await db.commit()
    log.info("auto_send.settings_saved", **auto_settings)
    return {"status": "ok", **auto_settings}


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue Settings
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/queue-settings")
async def get_queue_settings(db: AsyncSession = Depends(get_db)):
    """Read smart queue settings (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("queue_settings")
    if not raw:
        raw = await _get_setting(db, "queue_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    return {"s01e01_only": False}


@router.put("/api/queue-settings")
async def update_queue_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save smart queue settings to DB + Redis."""
    import json as _json
    r = await get_redis()
    queue_settings = {
        "s01e01_only": bool(payload.get("s01e01_only", False)),
    }
    encoded = _json.dumps(queue_settings)
    await r.set("queue_settings", encoded)
    await _put_setting(db, "queue_settings", encoded)
    await db.commit()
    log.info("queue_settings.saved", **queue_settings)
    return {"status": "ok", **queue_settings}


# ═══════════════════════════════════════════════════════════════════════════
# Settings API
# ═══════════════════════════════════════════════════════════════════════════

class SettingsRequest(BaseModel):
    simkl_client_id: str = None
    simkl_client_secret: str = None
    emby_url: str = None
    emby_api_key: str = None
    cron_smart_queue: str = None
    cron_ml_retrain: str = None
    cron_universe_scan: str = None
    features: dict = None


MASKED_SUFFIX = "****"


def _mask_api_key(key: str) -> str:
    """Return first 4 chars + **** for display. Never return the full key."""
    if not key:
        return ""
    return key[:4] + MASKED_SUFFIX


def _is_masked(key: str) -> bool:
    """True if the key looks like a masked value (ends with ****)."""
    return key.endswith(MASKED_SUFFIX) and len(key) <= 8


async def _resolve_servers(new_servers: list[dict], storage_key: str) -> list[dict]:
    """For each server in the incoming list, if the api_key is masked,
    look up the real key from the currently stored config."""
    import json as _json
    needs_resolve = any(_is_masked(s.get("api_key", "")) for s in new_servers)
    if not needs_resolve:
        return new_servers
    # Load existing servers to get real keys
    existing = {}
    try:
        r = await get_redis()
        raw = await r.get(storage_key)
        if raw:
            for srv in _json.loads(raw):
                existing[srv.get("url", "")] = srv.get("api_key", "")
    except Exception:
        pass
    resolved = []
    for s in new_servers:
        key = s.get("api_key", "")
        if _is_masked(key):
            # Preserve existing key matched by URL
            real_key = existing.get(s.get("url", "").rstrip("/"), "")
            if real_key:
                s = {**s, "api_key": real_key}
            else:
                continue  # can't resolve — skip this server
        resolved.append(s)
    return resolved


async def _get_setting(db: AsyncSession, key: str, env_fallback: str) -> str:
    """Read a setting from DB; fall back to .env value if no DB row."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    return row.value if row else env_fallback


async def _put_setting(db: AsyncSession, key: str, value: str):
    """Upsert a setting into the app_settings table."""
    row = (await db.execute(select(AppSetting).where(AppSetting.key == key))).scalar_one_or_none()
    if row:
        row.value = value
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    else:
        db.add(AppSetting(key=key, value=value, updated_at=datetime.now(timezone.utc).replace(tzinfo=None)))


@router.get("/api/settings")
async def read_settings(db: AsyncSession = Depends(get_db)):
    """Read current settings — DB overrides, .env fallbacks."""
    return {
        "simkl_client_id": os.getenv("SIMKL_CLIENT_ID", "")[:8] + "****" if os.getenv("SIMKL_CLIENT_ID") else "",
        "simkl_client_secret": os.getenv("SIMKL_CLIENT_SECRET", "")[:8] + "****" if os.getenv("SIMKL_CLIENT_SECRET") else "",
        "emby_url": os.getenv("EMBY_URL", ""),
        "emby_api_key": os.getenv("EMBY_API_KEY", "")[:8] + "****" if os.getenv("EMBY_API_KEY") else "",
        "cron_smart_queue": await _get_setting(db, "cron_smart_queue", os.getenv("SMART_QUEUE_CRON", "0 2 * * *")),
        "cron_ml_retrain": await _get_setting(db, "cron_ml_retrain", os.getenv("ML_RETRAIN_CRON", "0 4 * * 1")),
        "cron_universe_scan": await _get_setting(db, "cron_universe_scan", os.getenv("UNIVERSE_SCAN_CRON", "0 3 * * 0")),
        "features": {
            "smart_queue": (await _get_setting(db, "feature_smart_queue", os.getenv("ENABLE_SMART_QUEUE", "true"))).lower() == "true",
            "ml_predictor": (await _get_setting(db, "feature_ml_predictor", os.getenv("ENABLE_ML_PREDICTOR", "true"))).lower() == "true",
            "universe_discovery": (await _get_setting(db, "feature_universe_discovery", os.getenv("ENABLE_UNIVERSE_DISCOVERY", "true"))).lower() == "true",
            "watch_party": (await _get_setting(db, "feature_watch_party", os.getenv("ENABLE_WATCH_PARTY", "true"))).lower() == "true",
        }
    }


@router.put("/api/settings")
async def update_settings(request: SettingsRequest, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Persist schedule settings to DB and reschedule live APScheduler jobs."""
    import json as _json
    from app.main import reschedule_job

    saved = []

    # Scheduler cron settings — persist and reschedule
    cron_map = {
        "cron_smart_queue": ("smart_queue", request.cron_smart_queue),
        "cron_ml_retrain": ("ml_retrain", request.cron_ml_retrain),
        "cron_universe_scan": ("universe_scan", request.cron_universe_scan),
    }
    for db_key, (job_id, cron_val) in cron_map.items():
        if cron_val:
            await _put_setting(db, db_key, cron_val)
            reschedule_job(job_id, cron_val)
            saved.append(db_key)

    # Feature toggles — persist to DB and update in-memory settings
    if request.features and isinstance(request.features, dict):
        feature_map = {
            "smart_queue": "enable_smart_queue",
            "ml_predictor": "enable_ml_predictor",
            "universe_discovery": "enable_universe_discovery",
            "watch_party": "enable_watch_party",
        }
        for feature_key, config_attr in feature_map.items():
            if feature_key in request.features:
                val = "true" if request.features[feature_key] else "false"
                await _put_setting(db, f"feature_{feature_key}", val)
                # Update in-memory settings object so /health etc. reflect changes
                if hasattr(settings, config_attr):
                    object.__setattr__(settings, config_attr, request.features[feature_key])
                saved.append(f"feature_{feature_key}")

    await db.commit()

    return {
        "status": "ok",
        "saved": saved,
        "message": f"Saved {len(saved)} setting(s).",
    }


class TestConnectionRequest(BaseModel):
    service: str
    client_id: str | None = None
    client_secret: str | None = None
    url: str | None = None
    api_key: str | None = None


@router.post("/api/settings/test-connection")
async def test_connection(body: TestConnectionRequest, _user: User = Depends(get_current_user)):
    """Test Simkl or Emby connection (uses credentials from .env)."""
    service = body.service
    if service == "simkl":
        # Test Simkl API
        simkl = SimklClient()
        try:
            result = await simkl.get_trending(kind="shows")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await simkl.close()
    
    elif service == "emby":
        # Test Emby API
        emby = EmbyClient()
        try:
            info = await emby.get_system_info()
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            await emby.close()

    return {"status": "error", "message": f"Unknown service: {service}"}


@router.post("/api/settings/reset-oauth")
async def reset_oauth(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Clear all stored Simkl OAuth tokens (users must re-link)."""
    users = (await db.execute(select(User))).scalars().all()
    for user in users:
        user.simkl_access_token = None
        user.simkl_token_expires = None
    await db.commit()
    return {"status": "ok", "message": f"OAuth tokens cleared for {len(users)} user(s). Re-link on the Link page."}


@router.post("/api/settings/factory-reset")
async def factory_reset(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Delete all users (cascades to ratings, predictions, queue) and clear the library cache.
    Requires body: {"confirm": "FACTORY_RESET"}
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if body.get("confirm") != "FACTORY_RESET":
        raise HTTPException(400, "Confirmation required: send {\"confirm\": \"FACTORY_RESET\"}")

    from sqlalchemy import delete as sa_delete
    users = (await db.execute(select(User))).scalars().all()
    count = len(users)
    for user in users:
        await db.delete(user)
    await db.commit()
    try:
        await LibraryCache.clear()
    except Exception:
        pass
    log.warning("security.factory_reset", users_deleted=count)
    return {"status": "ok", "message": f"Factory reset complete. Removed {count} user(s) and cleared cache."}


# ═══════════════════════════════════════════════════════════════════════════
# Connection Status (heartbeat-based)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/connection-status")
async def connection_status():
    """Return cached heartbeat results for Emby, Simkl, and Radarr."""
    import json as _json
    r = await get_redis()
    result = {}
    for svc in ("emby", "simkl"):
        raw = await r.get(f"heartbeat:{svc}")
        if raw:
            result[svc] = _json.loads(raw)
        else:
            result[svc] = {"status": "unknown", "checked_at": None}
    # Radarr — may have 0, 1, or 2 servers
    radarr_list = []
    raw_servers = await secure_get("radarr_servers")
    if raw_servers:
        servers = _json.loads(raw_servers)
        for i, _srv in enumerate(servers):
            raw_hb = await r.get(f"heartbeat:radarr:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"Radarr {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"Radarr {i+1}")}
            radarr_list.append(hb)
    result["radarr"] = radarr_list
    # Sonarr — may have 0, 1, or 2 servers
    sonarr_list = []
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        sonarr_servers = _json.loads(raw_sonarr)
        for i, _srv in enumerate(sonarr_servers):
            raw_hb = await r.get(f"heartbeat:sonarr:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"Sonarr {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"Sonarr {i+1}")}
            sonarr_list.append(hb)
    result["sonarr"] = sonarr_list
    # SABnzbd — may have 0, 1, or 2 servers
    sab_list = []
    raw_sab = await secure_get("sabnzbd_servers")
    if raw_sab:
        sab_servers = _json.loads(raw_sab)
        for i, _srv in enumerate(sab_servers):
            raw_hb = await r.get(f"heartbeat:sabnzbd:{i}")
            if raw_hb:
                hb = _json.loads(raw_hb)
                hb["name"] = _srv.get("name", f"SABnzbd {i+1}")
            else:
                hb = {"status": "unknown", "checked_at": None, "name": _srv.get("name", f"SABnzbd {i+1}")}
            sab_list.append(hb)
    result["sabnzbd"] = sab_list
    # MDBList (optional — only present if API key is configured)
    raw_mdb = await r.get("heartbeat:mdblist")
    if raw_mdb:
        result["mdblist"] = _json.loads(raw_mdb)
    else:
        # Check if key is configured at all
        mdb_key = await secure_get("mdblist_api_key")
        if mdb_key:
            result["mdblist"] = {"status": "unknown", "checked_at": None}
    # Integration provider
    raw_prov = await r.get("integration_provider")
    result["integration_provider"] = (raw_prov if isinstance(raw_prov, str) else raw_prov.decode()) if raw_prov else "simkl"
    return result


@router.post("/api/connection-status/refresh")
async def refresh_connection_status(_user: User = Depends(get_current_user)):
    """Force an immediate heartbeat check for all services."""
    from app.main import run_heartbeat
    await run_heartbeat()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════
# Watch History Stats
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# Continue Watching Audit
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
# Radarr/Sonarr Availability Monitor
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/availability")
async def get_availability():
    """Check download status of items in Radarr/Sonarr.

    Cross-references queue items marked as not-in-library with their
    status in Radarr/Sonarr: monitored, downloading, available.
    Does NOT cache results when any server is unreachable so a
    subsequent request can pick up the missing server.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    # Check cache
    cache_key = "availability_monitor_v2"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    r = await get_redis()
    movies_status: list[dict] = []
    shows_status: list[dict] = []
    any_server_failed = False
    failed_servers: list[str] = []

    # --- Radarr movies ---
    raw_radarr = await secure_get("radarr_servers")
    if raw_radarr:
        radarr_servers = _json.loads(raw_radarr)
        for srv in radarr_servers:
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                all_movies = await client.get_all_movies()
                # Fetch download queue for accurate status
                dl_queue = await client.get_download_queue()
                await client.close()
                dl_movie_ids = {d.get("tmdb_id") for d in dl_queue if d.get("tmdb_id")}

                for movie in all_movies:
                    if not movie.get("monitored", False):
                        continue
                    has_file = movie.get("hasFile", False)
                    tmdb_id = movie.get("tmdbId")
                    status = "available" if has_file else "monitored"

                    # Check real download queue for active download
                    if not has_file and tmdb_id in dl_movie_ids:
                        status = "downloading"

                    movies_status.append({
                        "title": movie.get("title", ""),
                        "year": movie.get("year"),
                        "tmdb_id": tmdb_id,
                        "imdb_id": movie.get("imdbId"),
                        "status": status,
                        "has_file": has_file,
                        "server": srv.get("name", "Radarr"),
                        "size_on_disk": movie.get("sizeOnDisk", 0),
                    })
            except Exception as e:
                any_server_failed = True
                failed_servers.append(srv.get("name", "Radarr"))
                log.warning("availability.radarr_failed", server=srv.get("name"), error=str(e)[:120])

    # --- Sonarr series ---
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        sonarr_servers = _json.loads(raw_sonarr)
        for srv in sonarr_servers:
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                all_series = await client.get_all_series()
                # Fetch download queue for accurate status
                dl_queue = await client.get_download_queue()
                await client.close()
                dl_tvdb_ids = {d.get("tvdb_id") for d in dl_queue if d.get("tvdb_id")}

                for series in all_series:
                    if not series.get("monitored", False):
                        continue
                    stats = series.get("statistics") or {}
                    ep_file_count = stats.get("episodeFileCount", 0)
                    ep_count = stats.get("episodeCount", 0)
                    tvdb_id = series.get("tvdbId")

                    if ep_count == 0:
                        continue

                    if ep_file_count >= ep_count:
                        status = "available"
                    elif tvdb_id in dl_tvdb_ids:
                        status = "downloading"
                    elif ep_file_count > 0:
                        status = "partial"
                    else:
                        status = "monitored"

                    shows_status.append({
                        "title": series.get("title", ""),
                        "year": series.get("year"),
                        "tvdb_id": tvdb_id,
                        "imdb_id": series.get("imdbId"),
                        "status": status,
                        "episodes_on_disk": ep_file_count,
                        "episodes_total": ep_count,
                        "server": srv.get("name", "Sonarr"),
                        "size_on_disk": stats.get("sizeOnDisk", 0) or series.get("sizeOnDisk", 0),
                    })
            except Exception as e:
                any_server_failed = True
                failed_servers.append(srv.get("name", "Sonarr"))
                log.warning("availability.sonarr_failed", server=srv.get("name"), error=str(e)[:120])

    # Filter to show only items that aren't fully available yet
    pending_movies = [m for m in movies_status if m["status"] != "available"]
    pending_shows = [s for s in shows_status if s["status"] != "available"]

    result = {
        "movies": {
            "pending": pending_movies,
            "available_count": len(movies_status) - len(pending_movies),
            "total_monitored": len(movies_status),
        },
        "shows": {
            "pending": pending_shows,
            "available_count": len(shows_status) - len(pending_shows),
            "total_monitored": len(shows_status),
        },
        "partial": any_server_failed,
        "failed_servers": failed_servers,
    }

    # Only cache if ALL servers responded — partial results should be
    # retried on the next request so the waking server gets picked up.
    if not any_server_failed:
        try:
            r = await get_redis()
            await r.setex(cache_key, 300, _json.dumps(result))
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Download Queue Progress (Radarr/Sonarr active downloads)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/download-queue")
async def get_download_queue():
    """Fetch active download queue from all Radarr/Sonarr servers.

    Returns items currently being downloaded with progress, ETA, and
    size info.  Keyed by tmdb_id (movies) and tvdb_id (shows) so the
    frontend can match them to smart queue cards.

    No caching — SABnzbd/Radarr/Sonarr on LAN respond in <100ms.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    r = await get_redis()

    downloads: list[dict] = []

    # --- Radarr queues ---
    raw_radarr = await secure_get("radarr_servers")
    if raw_radarr:
        for srv in _json.loads(raw_radarr):
            client = None
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                items = await client.get_download_queue()
                downloads.extend(items)
            except Exception as e:
                log.warning("download_queue.radarr_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # --- Sonarr queues ---
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        for srv in _json.loads(raw_sonarr):
            client = None
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                items = await client.get_download_queue()
                downloads.extend(items)
            except Exception as e:
                log.warning("download_queue.sonarr_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # --- SABnzbd enrichment ---
    # Build a lookup of nzo_id → SABnzbd slot data from all configured
    # SABnzbd instances, then overlay real-time progress onto the
    # Radarr/Sonarr items matched by downloadId.
    sab_lookup: dict[str, dict] = {}
    raw_sab = await secure_get("sabnzbd_servers")
    if raw_sab:
        from app.utils.sabnzbd_client import SabnzbdClient
        for srv in _json.loads(raw_sab):
            client = None
            try:
                client = SabnzbdClient(srv["url"], srv["api_key"], name=srv.get("name", "SABnzbd"))
                slots = await client.get_queue()
                for slot in slots:
                    nzo = slot.get("nzo_id")
                    if nzo:
                        sab_lookup[nzo] = slot
                # Also fetch history for post-processing states
                history = await client.get_history(limit=10)
                for slot in history:
                    nzo = slot.get("nzo_id")
                    if nzo and nzo not in sab_lookup:
                        sab_lookup[nzo] = slot
            except Exception as e:
                log.warning("download_queue.sabnzbd_failed", server=srv.get("name"), error=str(e)[:120])
            finally:
                if client:
                    await client.close()

    # Merge: replace Radarr/Sonarr progress with SABnzbd real-time data
    if sab_lookup:
        for dl in downloads:
            did = dl.get("download_id", "")
            sab = sab_lookup.get(did)
            if sab:
                dl["progress"] = sab["progress"]
                dl["size_mb"] = sab["size_mb"]
                dl["sizeleft_mb"] = sab["sizeleft_mb"]
                dl["sab_status"] = sab["status"]
                dl["sab_eta"] = sab["timeleft"]
                dl["sab_speed"] = sab["speed"]

    result = {"downloads": downloads, "count": len(downloads)}

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Realtime Download Progress (SABnzbd only — lightweight)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/download-progress")
async def get_download_progress():
    """Lightweight SABnzbd-only progress snapshot.

    Returns only nzo_id-keyed progress data — no Radarr/Sonarr calls.
    Designed to be polled at 500ms for realtime progress bar updates.
    No caching — SABnzbd on LAN responds in <50ms.
    """
    import json as _json
    r = await get_redis()
    raw_sab = await secure_get("sabnzbd_servers")
    if not raw_sab:
        return {"slots": {}, "count": 0}

    from app.utils.sabnzbd_client import SabnzbdClient

    slots: dict[str, dict] = {}
    for srv in _json.loads(raw_sab):
        client = None
        try:
            client = SabnzbdClient(srv["url"], srv["api_key"], name=srv.get("name", "SABnzbd"))
            for slot in await client.get_queue():
                nzo = slot.get("nzo_id")
                if nzo:
                    slots[nzo] = {
                        "progress": slot["progress"],
                        "speed": slot["speed"],
                        "eta": slot["timeleft"],
                        "status": slot["status"],
                        "sizeleft_mb": slot["sizeleft_mb"],
                    }
            # Also fetch history for post-processing states
            for slot in await client.get_history(limit=10):
                nzo = slot.get("nzo_id")
                if nzo and nzo not in slots:
                    slots[nzo] = {
                        "progress": slot["progress"],
                        "speed": "",
                        "eta": "",
                        "status": slot["status"],
                        "sizeleft_mb": 0,
                    }
        except Exception as e:
            log.warning("download_progress.sabnzbd_failed",
                        server=srv.get("name"), error=str(e)[:120])
        finally:
            if client:
                await client.close()

    return {"slots": slots, "count": len(slots)}


# ═══════════════════════════════════════════════════════════════════════════
# SABnzbd Server Management
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/sabnzbd/servers")
async def get_sabnzbd_servers():
    """Read configured SABnzbd servers from Redis."""
    import json as _json
    r = await get_redis()
    raw = await secure_get("sabnzbd_servers")
    servers = _json.loads(raw) if raw else []
    # Mask API keys
    masked = []
    for srv in servers:
        masked.append({
            **srv,
            "api_key": srv["api_key"][:4] + "****" if len(srv.get("api_key", "")) > 4 else "****",
        })
    return {"servers": masked}


@router.put("/api/sabnzbd/servers")
async def save_sabnzbd_servers(request: Request, _user: User = Depends(get_current_user)):
    """Save SABnzbd server configs (max 2) to Redis + DB."""
    import json as _json
    body = await request.json()
    servers = body.get("servers", [])[:2]

    r = await get_redis()

    # Resolve masked keys — if a key looks masked, keep the existing one
    raw_existing = await secure_get("sabnzbd_servers")
    existing = _json.loads(raw_existing) if raw_existing else []

    for i, srv in enumerate(servers):
        key = srv.get("api_key", "")
        if "****" in key and i < len(existing):
            srv["api_key"] = existing[i].get("api_key", key)

    encoded = _json.dumps(servers)
    await secure_set("sabnzbd_servers", encoded)

    # Persist to DB (survives Redis restarts)
    async with async_session_ctx() as db:
        await _put_setting(db, "sabnzbd_servers", encoded)

    # Invalidate download-queue cache so the next poll picks up the new server
    try:
        await r.delete("download_queue_cache_v1")
    except Exception:
        pass

    return {"status": "ok", "servers": len(servers)}


@router.post("/api/sabnzbd/test")
async def test_sabnzbd(request: Request, _user: User = Depends(get_current_user)):
    """Test connection to a SABnzbd server."""
    import json as _json
    from app.utils.sabnzbd_client import SabnzbdClient
    body = await request.json()
    url = body.get("url", "").strip()
    api_key = body.get("api_key", "").strip()
    if not url or not api_key:
        return {"status": "error", "message": "URL and API key required"}

    # Resolve masked key — if the frontend sent a masked value,
    # find the real key from the saved server config by matching URL
    if "****" in api_key:
        r = await get_redis()
        raw_existing = await secure_get("sabnzbd_servers")
        if raw_existing:
            for srv in _json.loads(raw_existing):
                if srv.get("url", "").rstrip("/") == url.rstrip("/"):
                    api_key = srv.get("api_key", api_key)
                    break

    client = None
    try:
        client = SabnzbdClient(url, api_key)
        result = await client.test_connection()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}
    finally:
        if client:
            await client.close()


# ═══════════════════════════════════════════════════════════════════════════
# Notifications (Discord / Gotify / Webhook)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/notifications/config")
async def get_notification_config(db: AsyncSession = Depends(get_db)):
    """Return notification config. Gotify tokens are masked."""
    import json as _json
    from app.utils.notification_client import DEFAULT_EVENTS, EVENT_TYPES
    raw = await secure_get("notifications_config")
    if not raw:
        raw = await _get_setting(db, "notifications_config", "")
    if not raw:
        return {"services": [], "events": dict(DEFAULT_EVENTS), "event_types": EVENT_TYPES}
    try:
        config = _json.loads(raw)
        for svc in config.get("services", []):
            if svc.get("token"):
                svc["token"] = _mask_api_key(svc["token"])
        config["event_types"] = EVENT_TYPES
        return config
    except Exception:
        return {"services": [], "events": dict(DEFAULT_EVENTS), "event_types": EVENT_TYPES}


@router.put("/api/notifications/config")
async def save_notification_config(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Save notification config (services + event toggles)."""
    import json as _json
    services = payload.get("services", [])[:5]  # max 5 services

    # Resolve masked tokens from existing config
    try:
        existing_raw = await secure_get("notifications_config")
        if existing_raw:
            existing = _json.loads(existing_raw)
            existing_tokens = {}
            for svc in existing.get("services", []):
                if svc.get("url") and svc.get("token"):
                    existing_tokens[svc["url"]] = svc["token"]
            for svc in services:
                token = svc.get("token", "")
                if token and _is_masked(token):
                    real = existing_tokens.get(svc.get("url", ""), "")
                    if real:
                        svc["token"] = real
                    else:
                        svc.pop("token", None)
    except Exception:
        pass

    clean = []
    for svc in services:
        if svc.get("url"):
            clean.append({
                "name": svc.get("name", "Webhook"),
                "type": svc.get("type", "webhook"),
                "url": svc["url"].rstrip("/"),
                "token": svc.get("token", ""),
                "enabled": svc.get("enabled", True),
            })

    events = payload.get("events", {})
    from app.utils.notification_client import DEFAULT_EVENTS
    clean_events = {}
    for key in DEFAULT_EVENTS:
        clean_events[key] = bool(events.get(key, DEFAULT_EVENTS[key]))

    config = {"services": clean, "events": clean_events}
    encoded = _json.dumps(config)
    await secure_set("notifications_config", encoded)
    await _put_setting(db, "notifications_config", encoded)
    await db.commit()
    return {"status": "ok", "services": len(clean)}


@router.post("/api/notifications/test")
async def test_notification(
    payload: dict,
    _user: User = Depends(get_current_user),
):
    """Send a test notification to a single service."""
    from app.utils.notification_client import test_service
    svc_type = payload.get("type", "webhook")
    url = payload.get("url", "")
    token = payload.get("token", "")
    if not url:
        return {"status": "error", "message": "URL required"}
    # Resolve masked token
    if token and _is_masked(token):
        import json as _json
        try:
            existing_raw = await secure_get("notifications_config")
            if existing_raw:
                existing = _json.loads(existing_raw)
                for svc in existing.get("services", []):
                    if svc.get("url", "").rstrip("/") == url.rstrip("/") and svc.get("token"):
                        token = svc["token"]
                        break
        except Exception:
            pass
    service = {"type": svc_type, "url": url, "token": token, "name": "Test"}
    result = await test_service(service)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Watchlist Sync (Radarr/Sonarr → Simkl Watchlist)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/watchlist-sync/run")
async def run_watchlist_sync(
    current_user: User = Depends(get_current_user),
):
    """Manually trigger a Radarr/Sonarr ↔ Simkl watchlist sync."""
    from app.services.watchlist_sync.service import WatchlistSyncService
    svc = WatchlistSyncService()
    try:
        await svc._sync_user(current_user)
        return {"status": "ok"}
    except Exception as e:
        log.exception("watchlist_sync.manual_failed", user_id=current_user.id)
        raise HTTPException(500, f"Watchlist sync failed: {e}")


@router.get("/api/watchlist-sync/settings")
async def get_watchlist_sync_settings(db: AsyncSession = Depends(get_db)):
    """Read watchlist sync toggle state (Redis → DB fallback)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("watchlist_sync_settings")
    if not raw:
        raw = await _get_setting(db, "watchlist_sync_settings", "")
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    return {"arr_to_watchlist": False, "watchlist_to_arr": False}


@router.put("/api/watchlist-sync/settings")
async def update_watchlist_sync_settings(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save watchlist sync toggle state to DB + Redis.

    Payload: {"arr_to_watchlist": true/false, "watchlist_to_arr": true/false}
    """
    import json as _json
    r = await get_redis()
    sync_settings = {
        "arr_to_watchlist": bool(payload.get("arr_to_watchlist", False)),
        "watchlist_to_arr": bool(payload.get("watchlist_to_arr", False)),
    }
    encoded = _json.dumps(sync_settings)
    await r.set("watchlist_sync_settings", encoded)
    await _put_setting(db, "watchlist_sync_settings", encoded)
    await db.commit()
    log.info("watchlist_sync.settings_saved", **sync_settings)
    return {"status": "ok", **sync_settings}


# ═══════════════════════════════════════════════════════════════════════════
# TMDB API Key
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/tmdb/key")
async def get_tmdb_key(db: AsyncSession = Depends(get_db)):
    """Return whether a TMDB API key is configured (never returns the key itself)."""
    r = await get_redis()
    raw = await secure_get("tmdb_api_key")
    if not raw:
        raw = await _get_setting(db, "tmdb_api_key", "")
        if raw:
            await secure_set("tmdb_api_key", raw)
    return {"configured": bool(raw)}


@router.put("/api/tmdb/key")
async def save_tmdb_key(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save or clear the TMDB API key."""
    import json as _json
    key = (payload.get("api_key") or "").strip()
    r = await get_redis()
    if key:
        await secure_set("tmdb_api_key", key)
        await _put_setting(db, "tmdb_api_key", key)
        await db.commit()
        # Clear any cached empty provider results from before the key was set
        try:
            cached_keys = []
            cursor = b"0"
            while True:
                cursor, keys = await r.scan(cursor, match="tmdb_providers:*", count=100)
                cached_keys.extend(keys)
                if cursor == b"0" or cursor == 0:
                    break
            if cached_keys:
                await r.delete(*cached_keys)
                log.info("tmdb.cache_cleared", keys_removed=len(cached_keys))
        except Exception:
            pass
        return {"status": "ok", "configured": True}
    else:
        await r.delete("tmdb_api_key")
        await _put_setting(db, "tmdb_api_key", "")
        await db.commit()
        return {"status": "ok", "configured": False}


@router.post("/api/tmdb/test")
async def test_tmdb_key(payload: dict, _user: User = Depends(get_current_user)):
    """Test a TMDB API key by fetching a known movie."""
    import httpx
    key = (payload.get("api_key") or "").strip()
    if not key:
        raise HTTPException(400, "api_key required")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.themoviedb.org/3/movie/550",
                params={"api_key": key},
            )
            if resp.status_code == 401:
                return {"status": "error", "message": "Invalid API key"}
            resp.raise_for_status()
            data = resp.json()
            return {"status": "ok", "message": f"Connected — {data.get('title', 'OK')}"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════════════════
# Simkl Personal Lists → Emby Playlists
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/simkl-lists")
async def get_simkl_lists():
    """Fetch all Simkl lists available to the user: personal, liked, and collaborations."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.simkl_access_token:
            raise HTTPException(400, "No Simkl-linked user found")

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        my_lists = await simkl.get_my_lists()
        liked_lists = await simkl.get_liked_lists()
        collab_lists = await simkl.get_collaborations()
    finally:
        await simkl.close()

    results = []
    seen_slugs = set()

    def _add(lst, owner):
        ids = lst.get("ids", {})
        slug = ids.get("slug", "")
        if slug in seen_slugs:
            return
        seen_slugs.add(slug)
        u = lst.get("user", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": slug,
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "privacy": lst.get("privacy", "private"),
            "likes": lst.get("likes", 0),
            "owner": owner,
            "user_name": u.get("username", ""),
        })

    for lst in (my_lists or []):
        _add(lst, "self")

    for entry in (liked_lists or []):
        # Liked lists response wraps list in a "list" key
        lst = entry.get("list", entry)
        _add(lst, "liked")

    for lst in (collab_lists or []):
        _add(lst, "collaboration")

    return {"lists": results}


@router.post("/api/simkl-lists/import")
async def import_simkl_list(payload: dict, _user: User = Depends(get_current_user)):
    """Import a Simkl list into an Emby playlist.

    Payload: {"list_slug": "...", "playlist_name": "...", "username": "..."}
    username defaults to "me" for the user's own lists.
    Resolves list items against LibraryCache, creates an Emby playlist
    with matched items in list order.
    """
    list_slug = (payload.get("list_slug") or "").strip()
    if not list_slug:
        raise HTTPException(400, "list_slug required")
    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()
    username = (payload.get("username") or "").strip() or "me"

    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.simkl_access_token:
            raise HTTPException(400, "No Simkl-linked user found")

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        # Fetch items — the endpoint returns items under /users/{username}/lists/{slug}/items
        items = await simkl.get_list_items(username, list_slug)
    finally:
        await simkl.close()

    if not items:
        return {"status": "ok", "matched": 0, "unmatched": 0, "message": "List is empty"}

    emby = EmbyClient()
    emby_ids = []
    unmatched = []

    try:
        for entry in items:
            # Each entry has a type key ("movie", "show") and the item data under that key
            item_type = entry.get("type", "")
            item_data = entry.get(item_type, {}) if item_type else {}
            ids = item_data.get("ids", {})
            title = item_data.get("title", "Unknown")

            # Try to resolve via LibraryCache using provider IDs
            match = None

            # Try IMDB
            if ids.get("imdb"):
                match = await LibraryCache.find_by_provider_id("Imdb", ids["imdb"])

            # Try TMDB
            if not match and ids.get("tmdb"):
                match = await LibraryCache.find_by_provider_id("Tmdb", str(ids["tmdb"]))

            # Try TVDB (shows)
            if not match and ids.get("tvdb"):
                match = await LibraryCache.find_by_provider_id("Tvdb", str(ids["tvdb"]))

            if match and match.get("emby_id"):
                emby_ids.append(match["emby_id"])
            else:
                unmatched.append({"title": title, "year": item_data.get("year")})

        # Create Emby playlist
        playlist_id = None
        if emby_ids:
            emby_user_id = (await _first_emby_user_id()) or None
            final_name = playlist_name or f"📋 {list_slug}"
            playlist_id = await emby.recreate_playlist(
                final_name, emby_ids, user_id=emby_user_id,
            )
            # Set Overview (description) on the playlist item
            if playlist_id and description:
                await emby.set_playlist_overview(
                    playlist_id, description,
                    user_id=emby_user_id,
                )
            log.info("simkl_list.imported", slug=list_slug, name=final_name,
                     matched=len(emby_ids), unmatched=len(unmatched))
    finally:
        await emby.close()

    return {
        "status": "ok",
        "matched": len(emby_ids),
        "unmatched": len(unmatched),
        "unmatched_items": unmatched[:20],  # cap to avoid huge responses
        "playlist_id": playlist_id,
    }



# ═══════════════════════════════════════════════════════════════════════════
# MDBList Integration
# ═══════════════════════════════════════════════════════════════════════════


async def _get_mdblist_key(db: AsyncSession | None = None) -> str:
    """Return the configured MDBList API key from Redis (fast) or DB fallback."""
    r = await get_redis()
    raw = await secure_get("mdblist_api_key")
    if raw:
        return raw if isinstance(raw, str) else raw.decode()
    if db:
        raw = await _get_setting(db, "mdblist_api_key", "")
        if raw:
            await secure_set("mdblist_api_key", raw)
        return raw or ""
    return ""


@router.get("/api/mdblist/key")
async def get_mdblist_key(db: AsyncSession = Depends(get_db)):
    """Return whether an MDBList API key is configured."""
    key = await _get_mdblist_key(db)
    return {"configured": bool(key)}


@router.put("/api/mdblist/key")
async def save_mdblist_key(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Save or clear the MDBList API key."""
    key = (payload.get("api_key") or "").strip()
    r = await get_redis()
    if key:
        await secure_set("mdblist_api_key", key)
        await _put_setting(db, "mdblist_api_key", key)
        await db.commit()
        return {"status": "ok", "configured": True}
    else:
        await r.delete("mdblist_api_key")
        await _put_setting(db, "mdblist_api_key", "")
        await db.commit()
        return {"status": "ok", "configured": False}


@router.post("/api/mdblist/test")
async def test_mdblist_key(payload: dict, _user: User = Depends(get_current_user)):
    """Test an MDBList API key."""
    from app.utils.mdblist_client import MDBListClient
    key = (payload.get("api_key") or "").strip()
    if not key:
        raise HTTPException(400, "api_key required")
    client = MDBListClient(key)
    try:
        result = await client.test_connection()
        if result["status"] == "ok":
            return {
                "status": "ok",
                "message": (
                    f"Connected — {result['username']} "
                    f"({result['plan']}, "
                    f"{result['requests_remaining']}/{result['requests_limit']} requests left)"
                ),
            }
        return {"status": "error", "message": result.get("message", "Unknown error")}
    finally:
        await client.close()


@router.get("/api/mdblist/lists")
async def get_mdblist_lists(db: AsyncSession = Depends(get_db)):
    """Fetch all lists available to the MDBList user.
    Returns own lists (dynamic, static, private) and liked lists.
    """
    from app.utils.mdblist_client import MDBListClient
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    client = MDBListClient(key)
    try:
        my_lists = await client.get_my_lists()
        liked_lists = await client.get_liked_lists()
    finally:
        await client.close()

    # Normalise into a unified format
    results = []
    seen_ids = set()

    for lst in (my_lists or []):
        lid = lst.get("id")
        if lid in seen_ids:
            continue
        seen_ids.add(lid)
        results.append({
            "id": lid,
            "name": lst.get("name", ""),
            "slug": lst.get("slug", ""),
            "description": lst.get("description") or "",
            "mediatype": lst.get("mediatype", ""),
            "items": lst.get("items", 0),
            "likes": lst.get("likes", 0),
            "type": lst.get("type", "static"),
            "dynamic": lst.get("dynamic", False),
            "private": lst.get("private", False),
            "owner": "self",
            "user_name": lst.get("user_name", ""),
        })

    for lst in (liked_lists or []):
        if not isinstance(lst, dict):
            continue
        lid = lst.get("id")
        if not lid or lid in seen_ids:
            continue
        seen_ids.add(lid)
        results.append({
            "id": lid,
            "name": lst.get("name", ""),
            "slug": lst.get("slug", ""),
            "description": lst.get("description") or "",
            "mediatype": lst.get("mediatype", ""),
            "items": lst.get("items", 0),
            "likes": lst.get("likes", 0),
            "type": lst.get("type", "static"),
            "dynamic": lst.get("dynamic", False),
            "private": lst.get("private", False),
            "owner": "liked",
            "user_name": lst.get("user_name", ""),
        })

    return {"lists": results}


@router.post("/api/mdblist/import")
async def import_mdblist_list(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Import an MDBList list into an Emby playlist.

    Payload: {"list_id": 123, "playlist_name": "...", "description": "..."}
    Resolves list items against LibraryCache, creates an Emby playlist
    with matched items in list order.
    """
    from app.utils.mdblist_client import MDBListClient

    list_id = payload.get("list_id")
    if not list_id:
        raise HTTPException(400, "list_id required")
    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()

    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    client = MDBListClient(key)
    try:
        items = await client.get_all_list_items(int(list_id))
    finally:
        await client.close()

    if not items:
        return {"status": "ok", "matched": 0, "unmatched": 0, "message": "List is empty"}

    emby = EmbyClient()
    emby_ids = []
    unmatched = []

    try:
        for entry in items:
            ids = entry.get("ids") or {}
            imdb_id = entry.get("imdb_id") or ids.get("imdb")
            tmdb_id = ids.get("tmdb") or entry.get("id")
            tvdb_id = entry.get("tvdb_id") or ids.get("tvdb")
            title = entry.get("title", "Unknown")
            mediatype = entry.get("mediatype", "movie")

            match = None

            # Try IMDB
            if imdb_id:
                match = await LibraryCache.find_by_provider_id("Imdb", str(imdb_id))

            # Try TMDB
            if not match and tmdb_id:
                match = await LibraryCache.find_by_provider_id("Tmdb", str(tmdb_id))

            # Try TVDB (shows)
            if not match and tvdb_id:
                match = await LibraryCache.find_by_provider_id("Tvdb", str(tvdb_id))

            if match and match.get("emby_id"):
                emby_ids.append(match["emby_id"])
            else:
                unmatched.append({
                    "title": title,
                    "year": entry.get("release_year"),
                    "type": mediatype,
                })

        # Create Emby playlist
        playlist_id = None
        if emby_ids:
            emby_user_id = (await _first_emby_user_id()) or None
            final_name = playlist_name or f"📋 MDB: {list_id}"
            playlist_id = await emby.recreate_playlist(
                final_name, emby_ids, user_id=emby_user_id,
            )
            if playlist_id and description:
                await emby.set_playlist_overview(
                    playlist_id, description,
                    user_id=emby_user_id,
                )
            log.info("mdblist.imported", list_id=list_id, name=final_name,
                     matched=len(emby_ids), unmatched=len(unmatched))
    finally:
        await emby.close()

    # Track the import for auto-sync
    import json as _json
    r = await get_redis()
    synced_key = "mdblist_synced_lists"
    raw = await r.get(synced_key)
    synced = _json.loads(raw) if raw else []

    # Update or add entry
    entry_found = False
    for entry in synced:
        if entry.get("list_id") == int(list_id):
            entry["playlist_name"] = playlist_name or entry.get("playlist_name", "")
            entry["description"] = description or entry.get("description", "")
            entry["last_synced"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            entry["matched"] = len(emby_ids)
            entry_found = True
            break

    if not entry_found:
        synced.append({
            "list_id": int(list_id),
            "playlist_name": playlist_name or f"📋 MDB: {list_id}",
            "description": description,
            "last_synced": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "matched": len(emby_ids),
            "auto_sync": True,
        })

    await r.set(synced_key, _json.dumps(synced))
    # Also persist to DB so it survives Redis restart
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()

    return {
        "status": "ok",
        "matched": len(emby_ids),
        "unmatched": len(unmatched),
        "unmatched_items": unmatched[:20],
        "playlist_id": playlist_id,
    }


# -- Simkl synced list tracking (mirrors MDBList pattern) ------------------

@router.get("/api/simkl-lists/synced")
async def get_simkl_synced(db: AsyncSession = Depends(get_db)):
    """Return Simkl lists that have been imported and are tracked for sync."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    if not raw:
        raw = await _get_setting(db, "simkl_synced_lists", "[]")
        if raw and raw != "[]":
            await r.set("simkl_synced_lists", raw)
    synced = _json.loads(raw) if raw else []
    return {"synced": synced}


@router.post("/api/simkl-lists/track")
async def track_simkl_list(payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Add or update a Simkl list in synced tracking after import."""
    import json as _json
    slug = (payload.get("list_slug") or "").strip()
    if not slug:
        raise HTTPException(400, "list_slug required")

    playlist_name = (payload.get("playlist_name") or "").strip()
    description = (payload.get("description") or "").strip()
    username = (payload.get("username") or "").strip() or "me"
    matched = payload.get("matched", 0)

    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    synced = _json.loads(raw) if raw else []

    entry_found = False
    for entry in synced:
        if entry.get("slug") == slug:
            entry["playlist_name"] = playlist_name or entry.get("playlist_name", "")
            entry["description"] = description or entry.get("description", "")
            entry["username"] = username
            entry["last_synced"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            entry["matched"] = matched
            entry_found = True
            break

    if not entry_found:
        synced.append({
            "slug": slug,
            "playlist_name": playlist_name or f"📋 {slug}",
            "description": description,
            "username": username,
            "last_synced": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "matched": matched,
            "auto_sync": True,
        })

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.put("/api/simkl-lists/synced/{slug}/auto-sync")
async def toggle_simkl_auto_sync(slug: str, payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Toggle auto-sync for a tracked Simkl list."""
    import json as _json
    enabled = payload.get("enabled", True)
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    synced = _json.loads(raw) if raw else []

    for entry in synced:
        if entry.get("slug") == slug:
            entry["auto_sync"] = enabled
            break
    else:
        raise HTTPException(404, "List not tracked")

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug, "auto_sync": enabled}


@router.delete("/api/simkl-lists/synced/{slug}")
async def remove_simkl_synced(slug: str, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Remove a Simkl list from sync tracking (does NOT delete the Emby playlist)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    synced = _json.loads(raw) if raw else []

    synced = [e for e in synced if e.get("slug") != slug]

    await r.set("simkl_synced_lists", _json.dumps(synced))
    await _put_setting(db, "simkl_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "slug": slug}


@router.post("/api/simkl-lists/sync-all")
async def sync_all_simkl_lists(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Re-import all auto-synced Simkl lists."""
    import json as _json
    r = await get_redis()
    raw = await r.get("simkl_synced_lists")
    if not raw:
        raw = await _get_setting(db, "simkl_synced_lists", "[]")
    synced = _json.loads(raw) if raw else []

    results = []
    for entry in synced:
        if not entry.get("auto_sync", True):
            results.append({"slug": entry["slug"], "status": "skipped", "reason": "auto_sync_off"})
            continue
        try:
            result = await import_simkl_list({
                "list_slug": entry["slug"],
                "playlist_name": entry.get("playlist_name", ""),
                "description": entry.get("description", ""),
                "username": entry.get("username", "me"),
            })
            results.append({"slug": entry["slug"], "status": "ok", "matched": result.get("matched", 0)})
        except Exception as e:
            results.append({"slug": entry["slug"], "status": "error", "message": str(e)[:200]})

    return {"status": "ok", "results": results}


@router.get("/api/simkl-lists/popular")
async def get_simkl_popular_lists():
    """Fetch popular Simkl community lists (public endpoint, no auth needed)."""
    simkl = SimklClient()

    try:
        raw = await simkl.get_popular_lists(limit=25)
    finally:
        await simkl.close()

    results = []
    for entry in (raw or []):
        lst = entry.get("list", entry)
        u = lst.get("user", {})
        ids = lst.get("ids", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": ids.get("slug", ""),
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "likes": lst.get("likes", 0) if "likes" in lst else entry.get("like_count", 0),
            "user_name": u.get("username", ""),
        })
    return {"lists": results}


@router.get("/api/simkl-lists/trending")
async def get_simkl_trending_lists():
    """Fetch trending Simkl community lists (public endpoint, no auth needed)."""
    simkl = SimklClient()

    try:
        raw = await simkl.get_trending_lists(limit=25)
    finally:
        await simkl.close()

    results = []
    for entry in (raw or []):
        lst = entry.get("list", entry)
        u = lst.get("user", {})
        ids = lst.get("ids", {})
        results.append({
            "name": lst.get("name", ""),
            "slug": ids.get("slug", ""),
            "item_count": lst.get("item_count", 0),
            "description": lst.get("description") or "",
            "likes": lst.get("likes", 0) if "likes" in lst else entry.get("like_count", 0),
            "user_name": u.get("username", ""),
        })
    return {"lists": results}


@router.get("/api/simkl-lists/items")
async def get_simkl_list_items_detail(slug: str, username: str = "me"):
    """Fetch items from a Simkl list with in-library/missing status for each item."""
    async with async_session_ctx() as db:
        user = (await db.execute(
            select(User).where(User.simkl_access_token.isnot(None)).order_by(User.id)
        )).scalars().first()
        if not user or not user.simkl_access_token:
            raise HTTPException(400, "No Simkl-linked user found")

        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )

    try:
        items = await simkl.get_list_items(username, slug)
    finally:
        await simkl.close()

    results = []
    for entry in (items or []):
        item_type = entry.get("type", "")
        item_data = entry.get(item_type, {}) if item_type else {}
        ids = item_data.get("ids", {})
        title = item_data.get("title", "Unknown")
        year = item_data.get("year")

        # Resolve against library cache
        match = None
        if ids.get("imdb"):
            match = await LibraryCache.find_by_provider_id("Imdb", ids["imdb"])
        if not match and ids.get("tmdb"):
            match = await LibraryCache.find_by_provider_id("Tmdb", str(ids["tmdb"]))
        if not match and ids.get("tvdb"):
            match = await LibraryCache.find_by_provider_id("Tvdb", str(ids["tvdb"]))

        in_library = bool(match and match.get("emby_id"))
        results.append({
            "title": title,
            "year": year,
            "type": item_type,
            "in_library": in_library,
            "imdb_id": ids.get("imdb"),
            "tmdb_id": ids.get("tmdb"),
            "tvdb_id": ids.get("tvdb"),
        })

    in_lib = sum(1 for r in results if r["in_library"])
    return {"items": results, "total": len(results), "in_library": in_lib, "missing": len(results) - in_lib}


@router.get("/api/mdblist/synced")
async def get_mdblist_synced(db: AsyncSession = Depends(get_db)):
    """Return the list of MDBList lists that have been imported and are tracked for auto-sync."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    if not raw:
        raw = await _get_setting(db, "mdblist_synced_lists", "[]")
        if raw and raw != "[]":
            await r.set("mdblist_synced_lists", raw)
    synced = _json.loads(raw) if raw else []
    return {"synced": synced}


@router.put("/api/mdblist/synced/{list_id}/auto-sync")
async def toggle_mdblist_auto_sync(list_id: int, payload: dict, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Toggle auto-sync on/off for a synced MDBList list."""
    import json as _json
    enabled = payload.get("enabled", True)
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    synced = _json.loads(raw) if raw else []

    for entry in synced:
        if entry.get("list_id") == list_id:
            entry["auto_sync"] = enabled
            break
    else:
        raise HTTPException(404, "List not tracked")

    await r.set("mdblist_synced_lists", _json.dumps(synced))
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "list_id": list_id, "auto_sync": enabled}


@router.delete("/api/mdblist/synced/{list_id}")
async def remove_mdblist_synced(list_id: int, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Remove a list from auto-sync tracking (does NOT delete the Emby playlist)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    synced = _json.loads(raw) if raw else []

    synced = [e for e in synced if e.get("list_id") != list_id]

    await r.set("mdblist_synced_lists", _json.dumps(synced))
    await _put_setting(db, "mdblist_synced_lists", _json.dumps(synced))
    await db.commit()
    return {"status": "ok", "list_id": list_id}


@router.post("/api/mdblist/sync-all")
async def sync_all_mdblist_lists(db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Re-import all auto-synced MDBList lists (used by the daily cron and manual refresh)."""
    import json as _json
    r = await get_redis()
    raw = await r.get("mdblist_synced_lists")
    if not raw:
        raw = await _get_setting(db, "mdblist_synced_lists", "[]")
    synced = _json.loads(raw) if raw else []

    key = await _get_mdblist_key(db)
    if not key:
        return {"status": "skipped", "reason": "no_api_key"}

    results = []
    for entry in synced:
        if not entry.get("auto_sync", True):
            results.append({"list_id": entry["list_id"], "status": "skipped", "reason": "auto_sync_off"})
            continue
        try:
            result = await import_mdblist_list(
                {
                    "list_id": entry["list_id"],
                    "playlist_name": entry.get("playlist_name", ""),
                    "description": entry.get("description", ""),
                },
                db,
            )
            results.append({"list_id": entry["list_id"], "status": "ok", "matched": result.get("matched", 0)})
        except Exception as e:
            results.append({"list_id": entry["list_id"], "status": "error", "message": str(e)[:200]})

    return {"status": "ok", "results": results}


@router.get("/api/mdblist/items")
async def get_mdblist_list_items_detail(list_id: int, db: AsyncSession = Depends(get_db)):
    """Fetch items from an MDBList list with in-library/missing status for each item."""
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        items = await client.get_all_list_items(list_id)
    finally:
        await client.close()

    results = []
    for item in items:
        imdb_id = item.get("imdb_id") or ""
        tvdb_id = item.get("tvdb_id")
        ids = item.get("ids") or {}
        tmdb_id = ids.get("tmdb") or item.get("tmdb_id")
        title = item.get("title", "Unknown")
        year = item.get("release_year") or item.get("year")
        mediatype = item.get("mediatype", "movie")

        # Resolve against library cache
        match = None
        if imdb_id:
            match = await LibraryCache.find_by_provider_id("Imdb", imdb_id)
        if not match and tmdb_id:
            match = await LibraryCache.find_by_provider_id("Tmdb", str(tmdb_id))
        if not match and tvdb_id:
            match = await LibraryCache.find_by_provider_id("Tvdb", str(tvdb_id))

        in_library = bool(match and match.get("emby_id"))
        results.append({
            "title": title,
            "year": year,
            "type": "show" if mediatype == "show" else "movie",
            "in_library": in_library,
            "imdb_id": imdb_id or None,
            "tmdb_id": tmdb_id,
            "tvdb_id": tvdb_id,
        })

    in_lib = sum(1 for r in results if r["in_library"])
    return {"items": results, "total": len(results), "in_library": in_lib, "missing": len(results) - in_lib}


@router.get("/api/mdblist/top")
async def get_mdblist_top_lists(db: AsyncSession = Depends(get_db)):
    """Fetch top/popular public lists from MDBList."""
    key = await _get_mdblist_key(db)
    if not key:
        raise HTTPException(400, "MDBList API key not configured")

    from app.utils.mdblist_client import MDBListClient
    client = MDBListClient(api_key=key)

    try:
        raw = await client.get_top_lists()
    finally:
        await client.close()

    results = []
    for lst in (raw or []):
        results.append({
            "name": lst.get("name", ""),
            "list_id": lst.get("id", 0),
            "item_count": lst.get("items", lst.get("item_count", 0)),
            "likes": lst.get("likes", 0),
            "user_name": lst.get("user_name", lst.get("username", "")),
            "mediatype": lst.get("mediatype", ""),
            "dynamic": lst.get("dynamic", False),
        })
    return {"lists": results}


# ═══════════════════════════════════════════════════════════════════════════
# Smart Queue History & Feedback
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/queue-history/{user_id}")
async def get_queue_history(
    user_id: int,
    days: int = Query(90, ge=7, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Return queue recommendation vs play stats over time.

    Shows play rates by source, current scoring weights, and recent
    recommendation items with their played/ignored status.
    """
    from app.utils.redis_cache import cache_get

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(tzinfo=None)

    # Per-source stats
    rows = (await db.execute(
        select(
            QueueItem.source,
            func.count(QueueItem.id).label("total"),
            func.count(QueueItem.played_at).label("played"),
        )
        .where(
            QueueItem.user_id == user_id,
            QueueItem.created_at >= cutoff,
        )
        .group_by(QueueItem.source)
    )).all()

    source_stats = []
    for source, total, played in rows:
        play_rate = round(played / total, 3) if total > 0 else 0
        source_stats.append({
            "source": source,
            "recommended": total,
            "played": played,
            "play_rate": play_rate,
        })

    # Weekly breakdown for chart
    weekly_rows = (await db.execute(
        select(
            func.date_trunc("week", QueueItem.created_at).label("week"),
            QueueItem.source,
            func.count(QueueItem.id).label("total"),
            func.count(QueueItem.played_at).label("played"),
        )
        .where(
            QueueItem.user_id == user_id,
            QueueItem.created_at >= cutoff,
        )
        .group_by("week", QueueItem.source)
        .order_by("week")
    )).all()

    weekly_data = []
    for week, source, total, played in weekly_rows:
        weekly_data.append({
            "week": week.strftime("%Y-%m-%d") if week else None,
            "source": source,
            "recommended": total,
            "played": played,
        })

    # Current weights
    weights = {}
    try:
        raw = await cache_get(f"queue_weights:{user_id}")
        if raw:
            weights = raw if isinstance(raw, dict) else {}
    except Exception:
        pass

    # Recent items (last 30)
    recent_items = (await db.execute(
        select(QueueItem)
        .where(QueueItem.user_id == user_id)
        .order_by(QueueItem.created_at.desc())
        .limit(30)
    )).scalars().all()

    recent = [{
        "title": item.title,
        "source": item.source,
        "score": round(item.score, 2) if item.score else 0,
        "played": item.played,
        "played_at": item.played_at.isoformat() if item.played_at else None,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "in_library": item.in_library,
    } for item in recent_items]

    return {
        "source_stats": source_stats,
        "weekly": weekly_data,
        "weights": weights,
        "recent_items": recent,
        "period_days": days,
    }



# ═══════════════════════════════════════════════════════════════════════════
# Arr Library Check — items already in Radarr / Sonarr
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/arr-library")
async def get_arr_library():
    """Return sets of TMDB/TVDB IDs for items already in Radarr/Sonarr.

    Used by the frontend to show 'In Radarr' / 'In Sonarr' instead of
    the send button, and by the watchlist sync job to find missing items.
    Cached 60s to avoid hammering the *arr APIs.
    """
    import json as _json
    from app.utils.redis_cache import cache_get, cache_set

    cache_key = "arr_library_ids_v2"
    try:
        cached = await cache_get(cache_key)
        if cached:
            data = _json.loads(cached) if isinstance(cached, str) else cached
            return data
    except Exception:
        pass

    radarr_tmdb: list[int] = []
    sonarr_tvdb: list[int] = []
    radarr_server_names: dict[int, str] = {}
    sonarr_server_names: dict[int, str] = {}
    radarr_missing_tmdb: list[int] = []
    sonarr_missing_tvdb: list[int] = []

    r = await get_redis()

    # --- Radarr ---
    raw_radarr = await secure_get("radarr_servers")
    if raw_radarr:
        from app.utils.radarr_client import RadarrClient
        for srv in _json.loads(raw_radarr):
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                movies = await client.get_all_movies()
                await client.close()
                for m in movies:
                    tmdb = m.get("tmdbId")
                    if tmdb:
                        radarr_tmdb.append(tmdb)
                        radarr_server_names[tmdb] = srv.get("name", "Radarr")
                        if m.get("monitored") and not m.get("hasFile"):
                            radarr_missing_tmdb.append(tmdb)
            except Exception as e:
                log.warning("arr_library.radarr_failed", server=srv.get("name"), error=str(e)[:120])

    # --- Sonarr ---
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        from app.utils.sonarr_client import SonarrClient
        for srv in _json.loads(raw_sonarr):
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                series = await client.get_all_series()
                await client.close()
                for s in series:
                    tvdb = s.get("tvdbId")
                    if tvdb:
                        sonarr_tvdb.append(tvdb)
                        sonarr_server_names[tvdb] = srv.get("name", "Sonarr")
                        if s.get("monitored"):
                            stats = s.get("statistics") or {}
                            total = stats.get("episodeCount", 0)
                            on_disk = stats.get("episodeFileCount", 0)
                            if total > 0 and on_disk < total:
                                sonarr_missing_tvdb.append(tvdb)
            except Exception as e:
                log.warning("arr_library.sonarr_failed", server=srv.get("name"), error=str(e)[:120])

    # Deduplicate missing IDs (dual-server setups)
    radarr_missing_tmdb = list(set(radarr_missing_tmdb))
    sonarr_missing_tvdb = list(set(sonarr_missing_tvdb))

    result = {
        "radarr_tmdb": radarr_tmdb,
        "sonarr_tvdb": sonarr_tvdb,
        "radarr_names": radarr_server_names,
        "sonarr_names": sonarr_server_names,
        "radarr_missing_tmdb": radarr_missing_tmdb,
        "sonarr_missing_tvdb": sonarr_missing_tvdb,
    }

    # Cache for 60 seconds
    try:
        await cache_set(cache_key, _json.dumps(result), ttl=60)
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Remote Play — browser extension integration
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/remote-play/libraries")
async def remote_play_libraries(db: AsyncSession = Depends(get_db)):
    """Return Emby library folders for the extension options page.

    Lists movies/tvshows libraries so the user can set priority order.
    """
    emby = EmbyClient()
    try:
        folders = await emby.get_virtual_folders()
        media_folders = [
            f for f in folders
            if f.get("collection_type") in ("movies", "tvshows", "mixed", "")
        ]
        return {"libraries": media_folders}
    except Exception as e:
        log.warning("remote_play.libraries_failed", error=str(e)[:200])
        raise HTTPException(502, "failed to fetch Emby libraries")
    finally:
        await emby.close()


@router.get("/api/remote-play/sessions/{user_id}")
async def remote_play_sessions(user_id: int, db: AsyncSession = Depends(get_db)):
    """Return active controllable Emby sessions for a user."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found or no Emby account linked")

    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
        user_sessions = []
        for s in all_sessions:
            if s.get("UserId") != user.emby_user_id:
                continue
            if not s.get("SupportsRemoteControl", False):
                continue
            user_sessions.append({
                "session_id": s.get("Id"),
                "device_name": s.get("DeviceName", "Unknown"),
                "client": s.get("Client", ""),
                "now_playing": s.get("NowPlayingItem", {}).get("Name"),
            })
        return {"sessions": user_sessions}
    except Exception as e:
        log.warning("remote_play.sessions_failed", error=str(e)[:200])
        raise HTTPException(502, "failed to fetch Emby sessions")
    finally:
        await emby.close()


@router.post("/api/remote-play")
async def remote_play(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Resolve a media item from provider IDs and start playback on Emby.

    Called by the browser extension.  Accepts IDs from Simkl, IMDB, or TMDB,
    resolves to an Emby library item, finds an active session for the user,
    and sends a play command.
    """
    body = await request.json()
    user_id = body.get("user_id")
    media_type = body.get("media_type", "movie")
    ids = body.get("ids", {})
    season = body.get("season")
    episode = body.get("episode")
    session_id = body.get("session_id")
    library_priority = body.get("library_priority", [])

    if not user_id:
        raise HTTPException(400, "user_id required")
    if not ids:
        raise HTTPException(400, "at least one ID required (imdb_id, tmdb_id, simkl_slug)")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found or no Emby account linked")

    # ── Step 1: Resolve to Emby library item ──

    matches = []

    for provider_type, id_key in [
        ("Imdb", "imdb_id"),
        ("Tmdb", "tmdb_id"),
        ("Tvdb", "tvdb_id"),
    ]:
        pid = ids.get(id_key)
        if pid:
            cached = await LibraryCache.find_by_provider_id(provider_type, str(pid))
            if cached:
                matches.append(cached)

    # Simkl slug → resolve via Simkl API to get provider IDs
    if not matches and ids.get("simkl_slug") and user.simkl_access_token:
        try:
            simkl = SimklClient(access_token=user.simkl_access_token)
            kind = "movie" if media_type == "movie" else "show"
            results = await simkl.search(query=ids["simkl_slug"], kind=kind)
            await simkl.close()
            if results:
                item_data = results[0].get(kind, {})
                simkl_ids = item_data.get("ids", {})
                for ptype, tkey in [("Imdb", "imdb"), ("Tmdb", "tmdb"), ("Tvdb", "tvdb")]:
                    pid = simkl_ids.get(tkey)
                    if pid:
                        cached = await LibraryCache.find_by_provider_id(ptype, str(pid))
                        if cached:
                            matches.append(cached)
                            break
        except Exception:
            log.debug("remote_play.simkl_resolve_failed", slug=ids.get("simkl_slug"))

    # Title fallback
    if not matches and ids.get("title"):
        cached = await LibraryCache.find_by_title(
            ids["title"], year=ids.get("year"),
        )
        if cached:
            matches.append(cached)

    if not matches:
        return {"status": "not_in_library", "message": "Item not found in Emby library"}

    # Dedupe and pick best match based on library priority
    seen_ids: set[str] = set()
    unique_matches = []
    for m in matches:
        if m["emby_id"] not in seen_ids:
            seen_ids.add(m["emby_id"])
            unique_matches.append(m)

    if len(unique_matches) > 1 and library_priority:
        emby = EmbyClient()
        try:
            for m in unique_matches:
                item_detail = await emby.get_item(m["emby_id"], user_id=user.emby_user_id)
                m["_parent_id"] = item_detail.get("ParentId", "")
        except Exception:
            pass
        finally:
            await emby.close()

        def priority_key(m):
            pid = m.get("_parent_id", "")
            try:
                return library_priority.index(pid)
            except ValueError:
                return 999

        unique_matches.sort(key=priority_key)

    emby_item = unique_matches[0]

    # ── Step 2: For shows, resolve to specific episode ──

    play_item_id = emby_item["emby_id"]

    if media_type == "show" and season is not None and episode is not None:
        emby = EmbyClient()
        try:
            episodes = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Episode",
                parent_id=emby_item["emby_id"],
                fields="ProviderIds,ProductionYear",
                sort_by="ParentIndexNumber,IndexNumber",
            )
            for ep in episodes.get("Items", []):
                if (ep.get("ParentIndexNumber") == season
                        and ep.get("IndexNumber") == episode):
                    play_item_id = ep["Id"]
                    break
            else:
                return {
                    "status": "episode_not_found",
                    "message": f"S{season:02d}E{episode:02d} not found in library",
                    "series_found": emby_item["title"],
                }
        except Exception as e:
            log.warning("remote_play.episode_resolve_failed", error=str(e)[:200])
            return {"status": "error", "message": "Failed to resolve episode"}
        finally:
            await emby.close()
    elif media_type == "show" and season is None:
        # No specific episode — play next unwatched
        emby = EmbyClient()
        try:
            next_up = await emby.get_items(
                user_id=user.emby_user_id,
                item_type="Episode",
                parent_id=emby_item["emby_id"],
                filters="IsUnplayed",
                sort_by="ParentIndexNumber,IndexNumber",
                limit=1,
            )
            next_items = next_up.get("Items", [])
            if next_items:
                play_item_id = next_items[0]["Id"]
        except Exception:
            pass
        finally:
            await emby.close()

    # ── Step 3: Find active session ──

    emby = EmbyClient()
    try:
        all_sessions = await emby.get_sessions()
    except Exception as e:
        await emby.close()
        log.warning("remote_play.sessions_failed", error=str(e)[:200])
        return {"status": "error", "message": "Failed to connect to Emby"}

    user_sessions = [
        s for s in all_sessions
        if s.get("UserId") == user.emby_user_id
        and s.get("SupportsRemoteControl", False)
    ]

    if not user_sessions:
        await emby.close()
        return {
            "status": "no_active_session",
            "message": "No controllable Emby session found — open Emby on a device first",
        }

    target_session = None
    if session_id:
        target_session = next((s for s in user_sessions if s.get("Id") == session_id), None)
        if not target_session:
            await emby.close()
            return {"status": "session_not_found", "message": "Requested session no longer active"}
    elif len(user_sessions) == 1:
        target_session = user_sessions[0]
    else:
        playing = [s for s in user_sessions if s.get("NowPlayingItem")]
        if len(playing) == 1:
            target_session = playing[0]
        else:
            await emby.close()
            return {
                "status": "multiple_sessions",
                "message": "Multiple Emby sessions found — pick one",
                "sessions": [
                    {
                        "session_id": s.get("Id"),
                        "device_name": s.get("DeviceName", "Unknown"),
                        "client": s.get("Client", ""),
                        "now_playing": s.get("NowPlayingItem", {}).get("Name"),
                    }
                    for s in user_sessions
                ],
            }

    # ── Step 4: Send play command ──

    try:
        await emby.play_item_on_session(
            session_id=target_session["Id"],
            item_id=play_item_id,
            controlling_user_id=user.emby_user_id,
        )
        return {
            "status": "playing",
            "title": emby_item.get("title", ""),
            "emby_id": play_item_id,
            "device": target_session.get("DeviceName", ""),
        }
    except Exception as e:
        log.warning("remote_play.play_failed", error=str(e)[:200])
        return {"status": "error", "message": f"Play command failed: {str(e)[:100]}"}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════
# Recently Arrived — items that moved from monitored/downloading → available
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/recently-arrived")
async def get_recently_arrived():
    """Surface items that recently became available in the library.

    Compares current Radarr/Sonarr available items against a previously
    stored snapshot of pending items.  Items that were pending last check
    but are now available (have files) are returned as "recently arrived".
    The snapshot is updated each call so the next call shows only new arrivals.
    """
    import json as _json
    from app.utils.radarr_client import RadarrClient
    from app.utils.sonarr_client import SonarrClient

    r = await get_redis()

    # Check short-lived result cache (5 min)
    result_cache_key = "recently_arrived_result_v1"
    try:
        cached = await r.get(result_cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    # Load previously-pending snapshot
    prev_key = "recently_arrived_pending_v1"
    try:
        raw_prev = await r.get(prev_key)
        prev_pending = _json.loads(raw_prev) if raw_prev else {"movies": [], "shows": []}
    except Exception:
        prev_pending = {"movies": [], "shows": []}

    prev_movie_ids = {str(m) for m in prev_pending.get("movies", [])}
    # Shows: track {tvdb_id: ep_file_count} to detect new episodes
    prev_show_eps = {}
    for s in prev_pending.get("shows", []):
        if isinstance(s, dict):
            prev_show_eps[str(s.get("id", ""))] = s.get("eps", 0)
        else:
            # Legacy format: just a tvdb_id string
            prev_show_eps[str(s)] = 0

    current_pending_movies: list[str] = []
    current_pending_shows: list[dict] = []
    arrived_movies: list[dict] = []
    arrived_shows: list[dict] = []

    # --- Radarr ---
    raw_radarr = await secure_get("radarr_servers")
    if raw_radarr:
        for srv in _json.loads(raw_radarr):
            try:
                client = RadarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Radarr"))
                all_movies = await client.get_all_movies()
                await client.close()
                for movie in all_movies:
                    if not movie.get("monitored", False):
                        continue
                    tmdb_id = str(movie.get("tmdbId", ""))
                    if not tmdb_id:
                        continue
                    has_file = movie.get("hasFile", False)
                    if has_file:
                        # Was it pending last time?
                        if tmdb_id in prev_movie_ids:
                            arrived_movies.append({
                                "title": movie.get("title", ""),
                                "year": movie.get("year"),
                                "tmdb_id": movie.get("tmdbId"),
                                "type": "movie",
                            })
                    else:
                        current_pending_movies.append(tmdb_id)
            except Exception as e:
                log.warning("recently_arrived.radarr_failed", error=str(e)[:120])

    # --- Sonarr ---
    raw_sonarr = await secure_get("sonarr_servers")
    if raw_sonarr:
        for srv in _json.loads(raw_sonarr):
            try:
                client = SonarrClient(srv["url"], srv["api_key"], name=srv.get("name", "Sonarr"))
                all_series = await client.get_all_series()
                await client.close()
                for series in all_series:
                    if not series.get("monitored", False):
                        continue
                    tvdb_id = str(series.get("tvdbId", ""))
                    if not tvdb_id:
                        continue
                    stats = series.get("statistics") or {}
                    ep_file_count = stats.get("episodeFileCount", 0)
                    ep_count = stats.get("episodeCount", 0)
                    if ep_count == 0:
                        continue
                    fully_available = ep_file_count >= ep_count

                    # Check if new episodes arrived since last snapshot
                    prev_eps = prev_show_eps.get(tvdb_id)
                    if prev_eps is not None and ep_file_count > prev_eps:
                        arrived_shows.append({
                            "title": series.get("title", ""),
                            "year": series.get("year"),
                            "tvdb_id": series.get("tvdbId"),
                            "type": "show",
                            "episodes_on_disk": ep_file_count,
                            "episodes_total": ep_count,
                            "new_episodes": ep_file_count - prev_eps,
                        })

                    # Track shows that still need episodes
                    if not fully_available:
                        current_pending_shows.append({"id": tvdb_id, "eps": ep_file_count})
            except Exception as e:
                log.warning("recently_arrived.sonarr_failed", error=str(e)[:120])

    # Save current pending as the new snapshot (30 day TTL)
    try:
        await r.setex(prev_key, 86400 * 30, _json.dumps({
            "movies": current_pending_movies,
            "shows": current_pending_shows,
        }))
    except Exception:
        pass

    # ── Merge with existing arrived items (for 24hr window) ──
    arrived_key = "recently_arrived_items_v1"
    now_ts = datetime.now(timezone.utc).isoformat() + "Z"
    try:
        raw_existing = await r.get(arrived_key)
        existing_items = _json.loads(raw_existing) if raw_existing else []
    except Exception:
        existing_items = []

    # Filter out items older than 24 hours
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat() + "Z"
    existing_items = [i for i in existing_items if i.get("arrived_at", "") > cutoff]

    # Add new arrivals with timestamp (dedup by id, skip dismissed)
    existing_ids = {(i.get("type"), str(i.get("id", ""))) for i in existing_items}
    # Load dismissed items
    dismissed_key = "recently_arrived_dismissed_v1"
    try:
        raw_dismissed = await r.get(dismissed_key)
        dismissed_set = set(_json.loads(raw_dismissed)) if raw_dismissed else set()
    except Exception:
        dismissed_set = set()
    new_arrival_names: list[str] = []
    for m in arrived_movies:
        key = ("movie", str(m.get("tmdb_id", "")))
        dismiss_key_str = f"movie:{m.get('tmdb_id', '')}"
        if key not in existing_ids and dismiss_key_str not in dismissed_set:
            existing_items.append({**m, "id": m.get("tmdb_id"), "arrived_at": now_ts})
            new_arrival_names.append(m.get("title", "Unknown movie"))
    for s in arrived_shows:
        key = ("show", str(s.get("tvdb_id", "")))
        dismiss_key_str = f"show:{s.get('tvdb_id', '')}"
        if key not in existing_ids and dismiss_key_str not in dismissed_set:
            existing_items.append({**s, "id": s.get("tvdb_id"), "arrived_at": now_ts})
            new_arrival_names.append(s.get("title", "Unknown show"))

    # New arrivals tracked but NOT notified here — the Emby webhook
    # item_added handler sends the notification in real-time instead.

    # Persist with 48hr TTL (items self-expire at 24hr via filter above)
    try:
        await r.setex(arrived_key, 86400 * 2, _json.dumps(existing_items))
    except Exception:
        pass

    result = {
        "arrived_movies": [i for i in existing_items if i.get("type") == "movie"],
        "arrived_shows": [i for i in existing_items if i.get("type") == "show"],
        "total": len(existing_items),
    }

    # Cache result for 5 min
    try:
        await r.setex(result_cache_key, 300, _json.dumps(result))
    except Exception:
        pass

    return result


@router.post("/api/recently-arrived/dismiss")
async def dismiss_recently_arrived(_user: User = Depends(get_current_user)):
    """Clear the recently arrived list by resetting the pending snapshot."""
    r = await get_redis()
    await r.delete("recently_arrived_result_v1")
    await r.delete("recently_arrived_pending_v1")
    await r.delete("recently_arrived_items_v1")
    return {"status": "cleared"}


@router.post("/api/recently-arrived/dismiss-item")
async def dismiss_arrived_item(request: Request, _user: User = Depends(get_current_user)):
    """Remove a single item from the recently arrived list and persist the dismissal."""
    import json as _json
    body = await request.json()
    item_type = body.get("type")  # "movie" or "show"
    item_id = str(body.get("id", ""))

    r = await get_redis()
    arrived_key = "recently_arrived_items_v1"
    dismissed_key = "recently_arrived_dismissed_v1"
    try:
        # Remove from arrived list
        raw = await r.get(arrived_key)
        items = _json.loads(raw) if raw else []
        items = [i for i in items if not (i.get("type") == item_type and str(i.get("id", "")) == item_id)]
        await r.setex(arrived_key, 86400 * 2, _json.dumps(items))
        # Persist dismissal (30 day TTL, same as pending snapshot)
        raw_dismissed = await r.get(dismissed_key)
        dismissed = _json.loads(raw_dismissed) if raw_dismissed else []
        dismiss_entry = f"{item_type}:{item_id}"
        if dismiss_entry not in dismissed:
            dismissed.append(dismiss_entry)
        await r.setex(dismissed_key, 86400 * 30, _json.dumps(dismissed))
        # Invalidate result cache
        await r.delete("recently_arrived_result_v1")
    except Exception:
        pass

    return {"status": "dismissed"}


@router.post("/api/play-on-session")
async def play_on_session(request: Request, db: AsyncSession = Depends(get_db), _user: User = Depends(get_current_user)):
    """Start playing an Emby item by its ID on a specific session.

    Unlike /api/remote-play (which resolves from provider IDs), this
    takes a direct emby_item_id and session_id for the continue watching
    play button.
    """
    body = await request.json()
    user_id = body.get("user_id")
    emby_item_id = body.get("emby_item_id")
    session_id = body.get("session_id")
    start_position_ticks = body.get("start_position_ticks", 0)

    if not all([user_id, emby_item_id, session_id]):
        raise HTTPException(400, "user_id, emby_item_id, and session_id required")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.emby_user_id:
        raise HTTPException(404, "user not found")

    emby = EmbyClient()
    try:
        await emby.play_item_on_session(
            session_id=session_id,
            item_id=emby_item_id,
            start_position_ticks=start_position_ticks,
            controlling_user_id=user.emby_user_id,
        )
        # Some Emby clients (macOS, web) ignore StartPositionTicks on
        # the initial PlayNow command.  A follow-up Seek after a short
        # delay forces them to jump to the correct position, then
        # Unpause ensures playback continues automatically.
        if start_position_ticks:
            import asyncio
            await asyncio.sleep(1.5)
            try:
                await emby.send_play_state_command(
                    session_id, "Seek", seek_ticks=int(start_position_ticks),
                )
                await asyncio.sleep(0.3)
                await emby.send_play_state_command(session_id, "Unpause")
            except Exception:
                pass  # best-effort — play already started
        return {"status": "playing", "emby_id": emby_item_id}
    except Exception as e:
        log.warning("play_on_session.failed", error=str(e)[:200])
        return {"status": "error", "message": f"Play failed: {str(e)[:100]}"}
    finally:
        await emby.close()


# ═══════════════════════════════════════════════════════════════════════════
# Watch Party Quick-Start — server-wide session enumeration
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
# Simkl Playback Sync — compare Simkl resume points with Emby
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/playback-sync/{user_id}")
async def get_playback_sync(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compare Simkl in-progress playback items with Emby resume points.

    Returns items that exist on Simkl's playback list, enriched with
    Emby resume data if available.  Surfaces mismatches (Simkl has a
    resume point but Emby doesn't, or vice versa) and stale entries
    (paused > 30 days ago).
    """
    import json as _json

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.simkl_access_token:
        raise HTTPException(404, "User not found or no Simkl account linked")
    require_user_ownership(current_user.id, user_id, "playback_sync")

    # Cache for 10 min
    cache_key = f"playback_sync_v1:{user.id}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            return _json.loads(cached)
    except Exception:
        pass

    # Fetch Simkl playback progress
    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )

    try:
        simkl_playback = await simkl.get_playback()
    except Exception as e:
        log.warning("playback_sync.simkl_fetch_failed", error=str(e)[:120])
        raise HTTPException(502, f"Failed to fetch Simkl playback: {str(e)[:100]}")

    if not simkl_playback:
        result = {"items": [], "total": 0}
        try:
            r = await get_redis()
            await r.setex(cache_key, 600, _json.dumps(result))
        except Exception:
            pass
        return result

    # Fetch Emby resumable items for cross-reference
    emby = EmbyClient()
    emby_resume: dict[str, dict] = {}  # provider_id → emby data
    try:
        resp = await emby.get_items(
            user_id=user.emby_user_id,
            fields="ProviderIds,UserData,RunTimeTicks",
            filters="IsResumable",
            limit=500,
        )
        for item in resp.get("Items", []):
            pids = item.get("ProviderIds", {})
            ud = item.get("UserData", {})
            runtime = item.get("RunTimeTicks", 0) or 0
            pos = ud.get("PlaybackPositionTicks", 0) or 0
            progress = round(pos / runtime * 100, 1) if runtime > 0 else 0

            entry = {
                "emby_id": item.get("Id"),
                "emby_progress": progress,
                "emby_title": item.get("Name", ""),
            }
            for key in ("Imdb", "Tmdb", "Tvdb"):
                if pids.get(key):
                    emby_resume[f"{key.lower()}:{pids[key]}"] = entry
    except Exception as e:
        log.warning("playback_sync.emby_fetch_failed", error=str(e)[:120])
    finally:
        await emby.close()

    # Build comparison items
    items: list[dict] = []
    now = datetime.now(timezone.utc)

    for pb in simkl_playback:
        pb_id = pb.get("id")
        pb_type = pb.get("type", "")
        progress = pb.get("progress", 0)
        paused_at = pb.get("paused_at", "")

        # Extract title and IDs
        media = pb.get(pb_type, {})
        title = media.get("title", "")
        ids = media.get("ids", {})

        # For episodes, include show + episode info
        ep_label = ""
        if pb_type == "episode":
            show = pb.get("show", {})
            title = show.get("title", title)
            ep_title = media.get("title", "")
            season = media.get("season", 0)
            number = media.get("number", 0)
            ep_label = f"S{season:02d}E{number:02d}" + (f" — {ep_title}" if ep_title else "")
            # Use show IDs for matching
            ids = show.get("ids", ids)

        # Calculate days since paused
        days_stale = None
        if paused_at:
            try:
                pa_dt = datetime.fromisoformat(paused_at.replace("Z", "+00:00"))
                days_stale = (now.astimezone() - pa_dt).days if pa_dt.tzinfo else (now - pa_dt.replace(tzinfo=None)).days
            except Exception:
                pass

        # Try to match with Emby resume
        emby_match = None
        for id_type in ("imdb", "tmdb", "tvdb"):
            id_val = ids.get(id_type)
            if id_val:
                key = f"{id_type}:{id_val}"
                if key in emby_resume:
                    emby_match = emby_resume[key]
                    break

        item_entry = {
            "simkl_playback_id": pb_id,
            "type": pb_type,
            "title": title,
            "episode": ep_label,
            "simkl_progress": round(progress, 1),
            "paused_at": paused_at,
            "days_stale": days_stale,
            "simkl_ids": {k: v for k, v in ids.items() if v},
        }

        if emby_match:
            item_entry["emby_id"] = emby_match["emby_id"]
            item_entry["emby_progress"] = emby_match["emby_progress"]
            diff = abs(progress - emby_match["emby_progress"])
            item_entry["progress_diff"] = round(diff, 1)
            item_entry["synced"] = diff < 5  # within 5% = synced
        else:
            item_entry["emby_id"] = None
            item_entry["emby_progress"] = None
            item_entry["progress_diff"] = None
            item_entry["synced"] = False

        items.append(item_entry)

    # Sort: unsynced first, then by staleness
    items.sort(key=lambda x: (x["synced"], -(x["days_stale"] or 0)))

    result = {"items": items, "total": len(items)}

    try:
        r = await get_redis()
        await r.setex(cache_key, 600, _json.dumps(result))
    except Exception:
        pass

    return result


@router.delete("/api/playback-sync/{user_id}/{playback_id}")
async def delete_simkl_playback(
    user_id: int,
    playback_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a stale playback entry from Simkl."""
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.simkl_access_token:
        raise HTTPException(404, "User not found or no Simkl account linked")
    require_user_ownership(current_user.id, user_id, "playback_sync")

    simkl = SimklClient(
        access_token=user.simkl_access_token,
        token_expires=user.simkl_token_expires,
    )

    await simkl.delete_playback(playback_id)

    # Invalidate cache
    try:
        r = await get_redis()
        await r.delete(f"playback_sync_v1:{user.id}")
    except Exception:
        pass

    return {"status": "deleted", "playback_id": playback_id}


# ═══════════════════════════════════════════════════════════════════════════
# Library Health Monitor
# ═══════════════════════════════════════════════════════════════════════════

from app.services.library_health_service import LibraryHealthService

_library_health_svc = LibraryHealthService()


@router.get("/library-health", response_class=HTMLResponse)
async def get_library_health_page():
    """Serve the Library Health Monitor page."""
    try:
        with open("frontend/templates/library_health.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Page not found</h1>"


@router.get("/api/library-health/{user_id}")
async def get_library_health_report(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return cached library health report for a user."""
    require_user_ownership(current_user.id, user_id, "library_health")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _library_health_svc.get_report(user)


@router.post("/api/library-health/{user_id}/scan")
async def scan_library_health(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Run a full library health scan for a user."""
    require_user_ownership(current_user.id, user_id, "library_health")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _library_health_svc.scan(user)


@router.post("/api/library-health/{user_id}/dismiss")
async def dismiss_library_health_item(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dismiss a 'watched not in library' item so it no longer appears in reports."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    body = await request.json()
    item_type = body.get("type", "")  # "movie" or "show"
    item_id = str(body.get("id", ""))  # imdb/tmdb/tvdb/simkl ID
    if not item_id or not item_type:
        raise HTTPException(400, "type and id required")

    # Upsert: only insert if not already dismissed
    existing = (await db.execute(
        select(DismissedHealthItem).where(
            DismissedHealthItem.user_id == user_id,
            DismissedHealthItem.item_type == item_type,
            DismissedHealthItem.item_id == item_id,
        )
    )).scalar_one_or_none()
    if not existing:
        db.add(DismissedHealthItem(user_id=user_id, item_type=item_type, item_id=item_id))
        await db.commit()

    count = (await db.execute(
        select(func.count()).select_from(DismissedHealthItem).where(
            DismissedHealthItem.user_id == user_id
        )
    )).scalar()
    return {"status": "dismissed", "total_dismissed": count}


@router.get("/api/library-health/{user_id}/dismissed")
async def get_dismissed_library_health_items(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the list of dismissed library health items for a user."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    rows = (await db.execute(
        select(DismissedHealthItem).where(DismissedHealthItem.user_id == user_id)
    )).scalars().all()
    return {"dismissed": [
        {"type": r.item_type, "id": r.item_id, "dismissed_at": r.dismissed_at.isoformat() if r.dismissed_at else None}
        for r in rows
    ]}


@router.post("/api/library-health/{user_id}/undismiss-all")
async def undismiss_all_library_health(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Clear all dismissed library health items for a user."""
    from app.models.schema import DismissedHealthItem
    require_user_ownership(current_user.id, user_id, "library_health")
    await db.execute(
        DismissedHealthItem.__table__.delete().where(
            DismissedHealthItem.user_id == user_id
        )
    )
    await db.commit()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# DB Inspector — browse app_settings from the browser
# ---------------------------------------------------------------------------

@router.get("/api/db/app-settings")
async def db_app_settings(prefix: str = "", db: AsyncSession = Depends(get_db)):
    """List app_settings rows, optionally filtered by key prefix.

    GET /api/db/app-settings              → all rows
    GET /api/db/app-settings?prefix=scrobble  → keys starting with 'scrobble'
    GET /api/db/app-settings?prefix=ml_drift  → drift snapshots
    """
    if prefix:
        rows = (await db.execute(
            select(AppSetting).where(AppSetting.key.like(f"{prefix}%"))
        )).scalars().all()
    else:
        rows = (await db.execute(select(AppSetting))).scalars().all()
    result = []
    for r in rows:
        try:
            val = _json.loads(r.value) if r.value else None
        except Exception:
            val = r.value
        result.append({
            "key": r.key,
            "value": val,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        })
    return {"settings": result}


@router.get("/api/debug-mode")
async def get_debug_mode():
    """Return current log level / debug state."""
    import logging as _logging
    current = _logging.getLogger().level
    return {"debug": current <= _logging.DEBUG, "level": _logging.getLevelName(current)}


@router.put("/api/debug-mode")
async def set_debug_mode(payload: dict, _user: User = Depends(get_current_user)):
    """Toggle debug logging at runtime. No restart needed."""
    import logging as _logging
    import structlog as _sl

    enabled = payload.get("enabled", False)
    new_level = _logging.DEBUG if enabled else _logging.INFO

    root = _logging.getLogger()
    root.setLevel(new_level)
    for h in root.handlers:
        h.setLevel(new_level)

    # Also reconfigure structlog's filtering threshold
    _sl.configure(
        wrapper_class=_sl.make_filtering_bound_logger(new_level),
    )

    # Persist preference so it survives page reload (not container restart)
    r = await get_redis()
    await r.set("debug_mode", "1" if enabled else "0")

    level_name = "DEBUG" if enabled else "INFO"
    log.info("settings.debug_mode_changed", level=level_name)
    return {"debug": enabled, "level": level_name}


# ═══════════════════════════════════════════════════════════════════════════
# Simkl ↔ MDBList Cross-Sync
# ═══════════════════════════════════════════════════════════════════════════

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
# Direct Emby Playback (Theater Mode solo play)
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
# Rewatch Recommender
# ═══════════════════════════════════════════════════════════════════════════

from app.services.rewatch.service import RewatchRecommender

_rewatch_svc = RewatchRecommender()


@router.get("/api/rewatch/{user_id}")
async def get_rewatch_suggestions(
    user_id: int,
    page: int = 1,
    page_size: int = 30,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return cached rewatch suggestions for a user (paginated)."""
    require_user_ownership(current_user.id, user_id, "rewatch")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    all_items = await _rewatch_svc.get_suggestions(user_id)
    total = len(all_items)
    start = (page - 1) * page_size
    end = start + page_size
    items = all_items[start:end]
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "items": items,
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.post("/api/rewatch/{user_id}/refresh")
async def refresh_rewatch(
    user_id: int,
    page: int = 1,
    page_size: int = 30,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Force rebuild rewatch suggestions (clears cache first)."""
    require_user_ownership(_user.id, user_id, "rewatch_refresh")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    all_items = await _rewatch_svc.build_suggestions(user_id)
    total = len(all_items)
    start = (page - 1) * page_size
    end = start + page_size
    items = all_items[start:end]
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "items": items,
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "status": "rebuilt",
    }


@router.post("/api/rewatch/{user_id}/dismiss/{item_key:path}")
async def dismiss_rewatch_item(
    user_id: int,
    item_key: str,
    _user: User = Depends(get_current_user),
):
    """Dismiss a rewatch suggestion permanently."""
    require_user_ownership(_user.id, user_id, "rewatch_dismiss")
    return await _rewatch_svc.dismiss(user_id, item_key)


@router.get("/api/rewatch/{user_id}/history/{item_key:path}")
async def get_rewatch_item_history(
    user_id: int,
    item_key: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Lazy-load watch history for hover flyout."""
    require_user_ownership(current_user.id, user_id, "rewatch_history")
    _validate_item_key(item_key)
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    return await _rewatch_svc.get_item_history(user_id, item_key)


@router.get("/api/rewatch/{user_id}/settings")
async def get_rewatch_settings(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Read rewatch recommender settings for a user."""
    require_user_ownership(current_user.id, user_id, "rewatch_settings")
    import json as _json
    r = await get_redis()
    raw = await r.get(f"rewatch:settings:{user_id}")
    if raw:
        return _json.loads(raw)
    return {"min_rating": 8, "min_months": 12, "seasonal": True}


@router.put("/api/rewatch/{user_id}/settings")
async def update_rewatch_settings(
    user_id: int,
    payload: RewatchSettings,
    _user: User = Depends(get_current_user),
):
    """Save rewatch recommender settings."""
    import json as _json
    require_user_ownership(_user.id, user_id, "rewatch_settings")
    r = await get_redis()
    settings_data = payload.model_dump()
    await r.set(f"rewatch:settings:{user_id}", _json.dumps(settings_data))
    # Also persist to DB for durability
    from app.models.schema import AppSetting
    async with async_session_ctx() as db:
        existing = (await db.execute(
            select(AppSetting).where(AppSetting.key == f"rewatch_settings:{user_id}")
        )).scalar_one_or_none()
        if existing:
            existing.value = _json.dumps(settings_data)
        else:
            db.add(AppSetting(key=f"rewatch_settings:{user_id}", value=_json.dumps(settings_data)))
        await db.commit()
    return {"status": "ok", **settings_data}


# ═══════════════════════════════════════════════════════════════════════════
# Emby Image Proxy — avoids exposing EMBY_API_KEY to the browser
# ═══════════════════════════════════════════════════════════════════════════

_IMAGE_TYPE_RE = re.compile(r"^(Primary|Thumb|Backdrop|Banner|Logo|Art|Disc|Box|BoxRear|Screenshot)$")


@router.get("/api/emby/image/{item_id}/{image_type}")
async def proxy_emby_image(
    item_id: str,
    image_type: str,
    maxWidth: int = 400,
):
    """Proxy Emby item images so the frontend never sees the API key."""
    if not _IMAGE_TYPE_RE.match(image_type):
        raise HTTPException(400, "Invalid image type")
    if not re.match(r"^[A-Za-z0-9]+$", item_id):
        raise HTTPException(400, "Invalid item ID")
    maxWidth = max(50, min(maxWidth, 1920))

    emby_url = os.getenv("EMBY_URL", "")
    emby_key = os.getenv("EMBY_API_KEY", "")
    if not emby_url or not emby_key:
        raise HTTPException(503, "Emby not configured")

    import httpx
    url = f"{emby_url}/Items/{item_id}/Images/{image_type}?maxWidth={maxWidth}&api_key={emby_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
        if resp.status_code == 404:
            raise HTTPException(404, "Image not found")
        if resp.status_code != 200:
            raise HTTPException(502, "Emby returned an error")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(
            content=resp.content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except httpx.TimeoutException:
        raise HTTPException(504, "Emby image request timed out")
    except httpx.RequestError:
        raise HTTPException(502, "Could not reach Emby server")


@router.get("/api/tmdb/image/{path:path}")
async def proxy_tmdb_image(path: str):
    """Proxy TMDB images so they work on networks that block image.tmdb.org."""
    import httpx
    # Validate path looks like a TMDB image path (e.g. w185/abc123.jpg)
    if not re.match(r"^w\d+/[A-Za-z0-9]+\.\w{3,4}$", path):
        raise HTTPException(400, "Invalid TMDB image path")
    url = f"https://image.tmdb.org/t/p/{path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            raise HTTPException(resp.status_code, "TMDB image not found")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(
            content=resp.content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=604800"},
        )
    except httpx.TimeoutException:
        raise HTTPException(504, "TMDB image request timed out")
    except httpx.RequestError:
        raise HTTPException(502, "Could not reach TMDB")


# ═══════════════════════════════════════════════════════════════════════════
# Watch History — persistent local record
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
# Rating Sync — MDBList ↔ Simkl
# ═══════════════════════════════════════════════════════════════════════════

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
# History-Based Recommendations
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/watch-history/{user_id}/recommendations")
async def get_history_recommendations(
    user_id: int,
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Recommend unwatched library items based on the user's watch history
    and MDBList/Simkl ratings.

    Approach:
      1. Fetch user's rated items (MDBList primary, Simkl supplement)
      2. Identify top-rated genres (weighted by rating)
      3. Find unwatched items in Emby library matching those genres
      4. Score by genre overlap × community rating
    """
    require_user_ownership(current_user.id, user_id, "recommendations")
    from app.utils.library_cache import LibraryCache
    from app.utils.mdblist_client import MDBListClient
    from app.utils.secure_redis import secure_get
    from app.utils.emby_client import EmbyClient
    from collections import Counter

    user = (await db.execute(
        select(User).where(User.id == user_id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")

    r = await get_redis()
    cache_key = f"history_recs_v1:{user_id}"
    cached = await r.get(cache_key)
    if cached:
        import json as _json
        return _json.loads(cached)

    # ── 1. Collect rated items from MDBList + Simkl ──
    genre_scores: Counter = Counter()  # genre → sum of ratings
    genre_counts: Counter = Counter()  # genre → count
    rated_imdb: set[str] = set()       # already rated/watched items

    # MDBList ratings (primary — has a rating for every watched item)
    mdb_key = await secure_get("mdblist_api_key")
    if mdb_key:
        mdb = MDBListClient(api_key=mdb_key)
        try:
            mdb_ratings = await mdb.get_ratings()
            for kind in ("movies", "shows"):
                for item in (mdb_ratings.get(kind, [])
                             if isinstance(mdb_ratings, dict) else []):
                    rating = item.get("rating")
                    iid = (item.get("ids") or {}).get("imdb", "")
                    genres = [g.lower() for g in item.get("genres", [])]
                    if rating and iid:
                        rated_imdb.add(iid)
                        for g in genres:
                            genre_scores[g] += float(rating)
                            genre_counts[g] += 1
        except Exception:
            pass
        finally:
            await mdb.close()

    # Simkl ratings (supplement — may have items MDBList doesn't)
    if user.simkl_access_token:
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )
        try:
            sr = await simkl.get_user_ratings("all")
            for entry in sr:
                item_obj = entry.get("movie") or entry.get("show") or entry
                iid = (item_obj.get("ids") or {}).get("imdb", "")
                rating = entry.get("rating")
                genres = [g.lower() for g in item_obj.get("genres", [])]
                if rating and iid and iid not in rated_imdb:
                    rated_imdb.add(iid)
                    for g in genres:
                        genre_scores[g] += float(rating)
                        genre_counts[g] += 1
        except Exception:
            pass
        finally:
            await simkl.close()

    if not genre_scores:
        return {"items": [], "top_genres": [], "rated_count": len(rated_imdb)}

    # ── 2. Compute genre affinity: avg rating per genre ──
    genre_affinity = {
        g: genre_scores[g] / genre_counts[g]
        for g in genre_scores
        if genre_counts[g] >= 3  # need at least 3 ratings to be meaningful
    }
    top_genres = sorted(genre_affinity.items(), key=lambda x: x[1], reverse=True)[:8]
    top_genre_set = {g for g, _ in top_genres}

    # ── 3. Scan Emby library for unwatched items matching top genres ──
    emby = EmbyClient()
    try:
        # Get user's played items from Emby
        played_items = await emby.get_items(
            user_id=user.emby_user_id,
            params={"IsPlayed": "true", "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series",
                    "Fields": "ProviderIds,Genres,CommunityRating",
                    "Limit": "10000"}
        )
        played_imdb = set()
        for pi in (played_items or []):
            pid = (pi.get("ProviderIds") or {}).get("Imdb", "")
            if pid:
                played_imdb.add(pid)

        # Get all unwatched items
        all_items = await emby.get_items(
            user_id=user.emby_user_id,
            params={"IsPlayed": "false", "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series",
                    "Fields": "ProviderIds,Genres,CommunityRating,Overview",
                    "Limit": "5000"}
        )
    finally:
        await emby.close()

    # ── 4. Score unwatched items ──
    scored: list[dict] = []
    for item in (all_items or []):
        item_imdb = (item.get("ProviderIds") or {}).get("Imdb", "")
        if item_imdb in rated_imdb or item_imdb in played_imdb:
            continue  # already watched/rated

        item_genres = {g.lower() for g in item.get("Genres", [])}
        overlap = item_genres & top_genre_set
        if not overlap:
            continue

        community_rating = item.get("CommunityRating") or 0
        # Score = genre overlap count × avg genre affinity × community rating boost
        genre_boost = sum(genre_affinity.get(g, 0) for g in overlap) / len(overlap)
        score = len(overlap) * genre_boost * (1 + community_rating / 10)

        scored.append({
            "emby_id": item.get("Id"),
            "title": item.get("Name", ""),
            "year": item.get("ProductionYear"),
            "item_type": "movie" if item.get("Type") == "Movie" else "show",
            "genres": list(item_genres),
            "matched_genres": list(overlap),
            "community_rating": round(community_rating, 1),
            "score": round(score, 2),
            "imdb_id": item_imdb,
            "overview": (item.get("Overview") or "")[:200],
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    result_items = scored[:limit]

    result = {
        "items": result_items,
        "top_genres": [{"genre": g, "avg_rating": round(s, 1)} for g, s in top_genres],
        "rated_count": len(rated_imdb),
    }

    # Cache for 6 hours
    import json as _json
    try:
        await r.setex(cache_key, 21600, _json.dumps(result))
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════
# User Rating System
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
    from app.models.schema import UserRating

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
        try:
            from app.utils.emby_client import EmbyClient
            emby = EmbyClient()
            search_type = "Series" if item_type == "show" else "Movie"
            results = await emby.search_items(title, item_type=search_type)
            if results:
                for result in (results if isinstance(results, list) else results.get("Items", results)):
                    r_pids = result.get("ProviderIds", {})
                    r_title = result.get("Name", "").strip().lower()
                    if r_title == title.strip().lower():
                        imdb_id = r_pids.get("Imdb") or r_pids.get("imdb")
                        tmdb_id = r_pids.get("Tmdb") or r_pids.get("tmdb")
                        if imdb_id or tmdb_id:
                            break
            await emby.close()
        except Exception as e:
            log_init = structlog.get_logger()
            log_init.debug("rate.emby_search_fallback_failed", error=str(e)[:120])

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

    # ── Push to providers ──────────────────────────────────────────────
    providers = await _get_active_providers(db)

    if "simkl" in providers and user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=user.simkl_access_token,
                token_expires=user.simkl_token_expires,
            )
            simkl_item = {
                "ids": ids_obj,
                "rating": rating,
                "rated_at": now_str,
                "_type": "movies" if item_type == "movie" else "shows",
            }
            if item_type == "episode" and season_number is not None and episode_number is not None:
                # Episode ratings: rating goes on the episode object, show is the container
                simkl_item = {
                    "ids": ids_obj,
                    "_type": "shows",
                    "seasons": [{
                        "number": season_number,
                        "episodes": [{
                            "number": episode_number,
                            "rating": rating,
                            "rated_at": now_str,
                        }],
                    }],
                }
            if title:
                simkl_item["title"] = series_name or title
            resp = await simkl.add_ratings([simkl_item])
            results["simkl"] = {"ok": True, "response": resp}
            log.info("rating.simkl_pushed", user_id=user_id, imdb=imdb_id, rating=rating)
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
                    # MDBList expects episode nested under show wrapper
                    ep_item = dict(mdb_item)
                    if season_number is not None:
                        ep_item["season"] = season_number
                    if episode_number is not None:
                        ep_item["number"] = episode_number
                    resp = await mdb.add_ratings(episodes=[ep_item])
                else:
                    resp = await mdb.add_ratings(shows=[mdb_item])
                results["mdblist"] = {"ok": True, "response": resp}
                log.info("rating.mdblist_pushed", user_id=user_id, imdb=imdb_id, rating=rating)
                await mdb.close()
        except Exception as e:
            results["mdblist"] = {"ok": False, "error": str(e)[:200]}
            log.warning("rating.mdblist_failed", error=str(e)[:200])

    # ── Store locally in UserRating ──────────────────────────────────
    try:
        # Check if a rating already exists (update vs insert)
        existing_q = select(UserRating).where(
            UserRating.user_id == user_id,
            UserRating.item_type == item_type,
        )
        if item_type == "episode" and season_number is not None and episode_number is not None:
            # Episode-level: match by series+season+episode
            existing_q = existing_q.where(
                UserRating.series_name == series_name,
                UserRating.season_number == season_number,
                UserRating.episode_number == episode_number,
            )
        elif imdb_id:
            existing_q = existing_q.where(UserRating.imdb_id == imdb_id)
        elif tmdb_id:
            existing_q = existing_q.where(UserRating.tmdb_id == tmdb_id)

        existing = (await db.execute(existing_q.limit(1))).scalar_one_or_none()
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)

        if existing:
            existing.rating = rating
            existing.rated_at = now_naive
            existing.source = "user"
            if imdb_id:
                existing.imdb_id = imdb_id
            if tmdb_id:
                existing.tmdb_id = tmdb_id
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
        try:
            from app.utils.emby_client import EmbyClient
            emby = EmbyClient()
            searched_titles: set[str] = set()
            for rid, title, itype in cache_misses:
                title_lower = title.strip().lower()
                if title_lower in searched_titles:
                    continue
                searched_titles.add(title_lower)
                try:
                    search_type = "Movie" if itype == "movie" else "Series"
                    results = await emby.search_items(title, item_type=search_type)
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
            await emby.close()
        except Exception:
            pass

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
        try:
            from app.utils.emby_client import EmbyClient
            emby_s = EmbyClient()
            for sname in series_cache_misses:
                try:
                    results = await emby_s.search_items(sname, item_type="Series")
                    for res in results:
                        if res.get("Name", "").strip().lower() == sname.strip().lower():
                            series_emby_cache[sname] = res.get("Id")
                            break
                except Exception:
                    pass
            await emby_s.close()
        except Exception:
            pass

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
# Trakt Data Import
# ═══════════════════════════════════════════════════════════════════════════

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
# Item Detail Page — aggregates Emby + MDBList + TMDB + local DB
# ═══════════════════════════════════════════════════════════════════════

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
# Item Detail — Episodes Breakdown
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
# Item Detail — Watchlist Toggle
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
# WATCHLIST LOCAL SYNC
# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/watchlist/sync/{user_id}")
async def sync_watchlist_local(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Pull watchlist from active providers and store locally for fast lookups."""
    from app.models.schema import WatchlistItem
    _now = datetime.now(timezone.utc).replace(tzinfo=None)
    imported = 0

    # Collect all items keyed by tmdb_id (primary) — Simkl items often lack IMDB
    wl_items: dict[str, dict] = {}  # tmdb_id -> {imdb_id, title, item_type, source}
    providers = await _get_active_providers(db)

    # ── Simkl ──
    if "simkl" in providers and current_user.simkl_access_token:
        try:
            simkl = SimklClient(
                access_token=current_user.simkl_access_token,
                token_expires=current_user.simkl_token_expires,
            )
            try:
                for kind, itype in [("movies", "movie"), ("shows", "show"), ("anime", "show")]:
                    entries = await simkl.get_watchlist(kind=kind)
                    for entry in entries:
                        # Simkl wraps plantowatch items: {"movie": {"title": ..., "ids": {...}}}
                        inner = entry.get("movie") or entry.get("show") or entry
                        ids = inner.get("ids", {})
                        tmdb = str(ids.get("tmdb")) if ids.get("tmdb") else None
                        imdb = ids.get("imdb")
                        # Need at least one ID to store
                        if not tmdb and not imdb:
                            continue
                        key = tmdb or imdb  # prefer tmdb as dedup key
                        if key not in wl_items:
                            wl_items[key] = {
                                "imdb_id": imdb,
                                "tmdb_id": tmdb,
                                "title": inner.get("title"),
                                "item_type": itype,
                                "source": "simkl",
                            }
                log.info("watchlist_local_sync.simkl_collected",
                         count=len(wl_items), user_id=current_user.id)
            finally:
                await simkl.close()
        except Exception as e:
            log.warning("watchlist_local_sync.simkl_failed", error=str(e)[:120])

    # ── MDBList ──
    if "mdblist" in providers:
        _pre = len(wl_items)
        try:
            mdb_key = await _get_mdblist_key(db)
            if mdb_key:
                from app.utils.mdblist_client import MDBListClient
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    wl_data = await mdb.get_watchlist()
                    for mtype, itype in [("movies", "movie"), ("shows", "show")]:
                        for entry in wl_data.get(mtype, []):
                            tmdb = str(entry.get("tmdb_id") or entry.get("tmdb") or "") or None
                            imdb = entry.get("imdb_id") or entry.get("imdb")
                            if not tmdb and not imdb:
                                continue
                            key = tmdb or imdb
                            if key not in wl_items:
                                wl_items[key] = {
                                    "imdb_id": imdb,
                                    "tmdb_id": tmdb,
                                    "title": entry.get("title"),
                                    "item_type": itype,
                                    "source": "mdblist",
                                }
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("watchlist_local_sync.mdblist_failed", error=str(e)[:120])
        log.info("watchlist_local_sync.mdblist_collected",
                 added=len(wl_items) - _pre, user_id=current_user.id)

    # ── Upsert locally, preserve user-submitted entries ──
    # Delete provider-synced rows (not user-submitted)
    await db.execute(
        WatchlistItem.__table__.delete().where(
            WatchlistItem.user_id == current_user.id,
            WatchlistItem.source != "user",
        )
    )

    # Collect existing user-submitted keys for dedup
    _user_rows = (await db.execute(
        select(WatchlistItem.imdb_id, WatchlistItem.tmdb_id).where(
            WatchlistItem.user_id == current_user.id,
            WatchlistItem.source == "user",
        )
    )).all()
    existing_user_keys: set[str] = set()
    for row in _user_rows:
        if row[0]:
            existing_user_keys.add(row[0])
        if row[1]:
            existing_user_keys.add(row[1])

    _seen_imdb: set[str] = set(existing_user_keys)
    _seen_tmdb: set[str] = set(existing_user_keys)
    for key, info in wl_items.items():
        imdb = info["imdb_id"]
        tmdb = info["tmdb_id"]
        # Skip if user already has, or if we'd violate a unique constraint
        if imdb and imdb in _seen_imdb:
            continue
        if tmdb and tmdb in _seen_tmdb:
            continue
        if imdb:
            _seen_imdb.add(imdb)
        if tmdb:
            _seen_tmdb.add(tmdb)
        db.add(WatchlistItem(
            user_id=current_user.id,
            imdb_id=imdb,
            tmdb_id=tmdb,
            title=info["title"],
            item_type=info["item_type"],
            source=info["source"],
            added_at=_now,
            synced_at=_now,
        ))
        imported += 1

    await db.commit()
    log.info("watchlist_local_sync.complete", user_id=current_user.id, imported=imported,
             user_kept=len(_user_rows), provider_total=len(wl_items))

    return {"synced": imported, "user_kept": len(_user_rows), "total": imported + len(_user_rows)}


@router.get("/api/watchlist/check")
async def check_watchlist(
    imdb_id: str | None = None,
    tmdb_id: str | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Check if an item is on the local watchlist."""
    from app.models.schema import WatchlistItem
    from sqlalchemy import or_
    if not imdb_id and not tmdb_id:
        return {"on_watchlist": False}
    conditions = []
    if imdb_id:
        conditions.append(WatchlistItem.imdb_id == imdb_id)
    if tmdb_id:
        conditions.append(WatchlistItem.tmdb_id == str(tmdb_id))
    q = select(WatchlistItem.id).where(
        WatchlistItem.user_id == current_user.id,
        or_(*conditions),
    )
    row = (await db.execute(q.limit(1))).first()
    return {"on_watchlist": row is not None}


# ═══════════════════════════════════════════════════════════════════════════
# FILMOGRAPHY TRACKER
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/filmography")
async def filmography_page():
    """Serve the filmography tracker page."""
    with open("frontend/templates/filmography.html", "r") as f:
        return HTMLResponse(f.read())


@router.get("/api/filmography/popular")
async def get_popular_people(current_user: User = Depends(get_current_user)):
    """Return popular actors/directors from TMDB for suggestions."""
    from app.utils.tmdb_client import get_popular_people as _get_popular
    people = await _get_popular(limit=20)
    return {"people": people}


@router.get("/api/filmography/{person_name}")
async def get_filmography(
    person_name: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch a person's filmography from TMDB, cross-referenced with library, watch history & watchlist."""
    from app.utils.tmdb_client import get_person_details
    from app.models.schema import WatchHistory, WatchlistItem

    person = await get_person_details(person_name)
    if not person:
        raise HTTPException(404, f"Person '{person_name}' not found on TMDB")

    # Merge cast + crew entries, dedup by id+media_type
    all_works = {}
    for item in person.get("cast", []):
        key = f"{item['media_type']}:{item['id']}"
        if key not in all_works:
            all_works[key] = {**item, "roles": []}
        all_works[key]["roles"].append(f"Actor ({item.get('character', '?')})")

    for item in person.get("crew", []):
        key = f"{item['media_type']}:{item['id']}"
        if key not in all_works:
            all_works[key] = {**item, "roles": []}
        all_works[key]["roles"].append(item.get("job", "Crew"))

    # Build watched lookups from watch history
    user_id = current_user.id
    _watched_movie_tmdb = set()
    _watched_movie_imdb = set()
    _watched_series_names: set[str] = set()

    # Movies: collect distinct tmdb_id and imdb_id
    _wm_rows = (await db.execute(
        select(WatchHistory.tmdb_id, WatchHistory.imdb_id)
        .where(WatchHistory.user_id == user_id, WatchHistory.item_type == "movie")
        .distinct()
    )).all()
    for row in _wm_rows:
        if row.tmdb_id:
            _watched_movie_tmdb.add(str(row.tmdb_id))
        if row.imdb_id:
            _watched_movie_imdb.add(row.imdb_id)

    # Shows: collect distinct series_name (case-insensitive)
    _ws_rows = (await db.execute(
        select(WatchHistory.series_name)
        .where(
            WatchHistory.user_id == user_id,
            WatchHistory.item_type == "episode",
            WatchHistory.series_name.isnot(None),
        )
        .distinct()
    )).all()
    for row in _ws_rows:
        if row.series_name:
            _watched_series_names.add(row.series_name.lower().strip())

    # Build watchlist lookup sets (local DB) — both IMDB and TMDB
    _wl_imdb_set: set[str] = set()
    _wl_tmdb_set: set[str] = set()
    _wl_rows = (await db.execute(
        select(WatchlistItem.imdb_id, WatchlistItem.tmdb_id).where(
            WatchlistItem.user_id == user_id,
        )
    )).all()
    for row in _wl_rows:
        if row[0]:
            _wl_imdb_set.add(row[0])
        if row[1]:
            _wl_tmdb_set.add(str(row[1]))

    # Cross-reference with library cache
    works = []
    for work in all_works.values():
        tmdb_id_str = str(work.get("id", ""))
        found = await LibraryCache.find_by_provider_id("Tmdb", tmdb_id_str) if tmdb_id_str else None
        _pids = found.get("provider_ids", {}) if found else {}
        work["in_library"] = found is not None
        work["emby_id"] = found.get("emby_id") if found else None
        work["imdb_id"] = _pids.get("Imdb") if found else None
        work["tvdb_id"] = _pids.get("Tvdb") if found else None

        # Determine watched status
        if work.get("media_type") == "movie":
            work["watched"] = (
                tmdb_id_str in _watched_movie_tmdb
                or (work["imdb_id"] and work["imdb_id"] in _watched_movie_imdb)
            )
        elif work.get("media_type") == "tv":
            _wname = (work.get("name") or work.get("title") or "").lower().strip()
            work["watched"] = _wname in _watched_series_names if _wname else False
        else:
            work["watched"] = False

        # Watchlist status (local DB — match by IMDB or TMDB)
        _on_wl = False
        if work.get("imdb_id") and work["imdb_id"] in _wl_imdb_set:
            _on_wl = True
        elif tmdb_id_str and tmdb_id_str in _wl_tmdb_set:
            _on_wl = True
        work["on_watchlist"] = _on_wl

        works.append(work)

    # Sort by release date descending
    works.sort(key=lambda x: x.get("release_date") or "0000", reverse=True)

    return {
        "person": {
            "name": person["name"],
            "profile_path": person.get("profile_path"),
            "known_for": person.get("known_for_department"),
        },
        "works": works,
        "total": len(works),
        "in_library": sum(1 for w in works if w["in_library"]),
        "watched": sum(1 for w in works if w.get("watched")),
    }


# ═══════════════════════════════════════════════════════════════════════════
# DUPLICATE / CONFLICT DETECTOR
# ═══════════════════════════════════════════════════════════════════════════

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

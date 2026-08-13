"""Encrypt/decrypt secrets stored in Redis using Fernet (AES-128-CBC).

Derives a stable Fernet key from JWT_SECRET_KEY so no extra env var is needed.
Only used for a small set of known secret keys — the rest of Redis stays
plaintext for performance and debuggability.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import structlog
from cryptography.fernet import Fernet, InvalidToken

from app.config import settings
from app.utils.redis_cache import get_redis

log = structlog.get_logger()

# Keys in Redis that contain secrets and must be encrypted.
SECRET_KEYS: frozenset[str] = frozenset({
    "radarr_servers",
    "sonarr_servers",
    "sabnzbd_servers",
    "tmdb_api_key",
    "mdblist_api_key",
    "notifications_config",
})

# Prefix so we can tell encrypted values from legacy plaintext.
_ENC_PREFIX = "enc:1:"


def _derive_fernet_key() -> bytes:
    """Derive a 32-byte URL-safe-base64 Fernet key from JWT_SECRET_KEY."""
    raw = hashlib.sha256(settings.jwt_secret_key.encode()).digest()
    return base64.urlsafe_b64encode(raw)


_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_derive_fernet_key())
    return _fernet


def encrypt(plaintext: str) -> str:
    """Encrypt a string and return prefixed ciphertext."""
    token = _get_fernet().encrypt(plaintext.encode())
    return _ENC_PREFIX + token.decode()


def decrypt(stored: str) -> str:
    """Decrypt a stored value.  If the value lacks the enc prefix it's
    returned as-is (legacy plaintext written before encryption was enabled)."""
    if not stored.startswith(_ENC_PREFIX):
        return stored
    token = stored[len(_ENC_PREFIX):]
    try:
        return _get_fernet().decrypt(token.encode()).decode()
    except InvalidToken:
        log.error("secure_redis.decrypt_failed",
                  hint="JWT_SECRET_KEY may have changed since this value was encrypted")
        raise


# ---------------------------------------------------------------------------
# High-level helpers (read-through / write-through)
# ---------------------------------------------------------------------------

async def secure_set(key: str, value: str, **kwargs) -> None:
    """Write a value to Redis, encrypting it if the key is in SECRET_KEYS."""
    r = await get_redis()
    if key in SECRET_KEYS:
        value = encrypt(value)
    await r.set(key, value, **kwargs)


async def secure_get(key: str) -> str | None:
    """Read a value from Redis, decrypting it if it was encrypted."""
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        return None
    val = raw if isinstance(raw, str) else raw.decode()
    if key in SECRET_KEYS:
        return decrypt(val)
    return val


async def migrate_plaintext_secrets() -> int:
    """Re-encrypt any legacy plaintext secret keys already in Redis.

    Safe to call on every startup — skips values that are already encrypted.
    Returns the number of keys migrated.
    """
    r = await get_redis()
    migrated = 0
    for key in SECRET_KEYS:
        raw = await r.get(key)
        if raw is None:
            continue
        val = raw if isinstance(raw, str) else raw.decode()
        if val.startswith(_ENC_PREFIX):
            continue  # already encrypted
        # Re-write with encryption
        encrypted = encrypt(val)
        await r.set(key, encrypted)
        migrated += 1
        log.info("secure_redis.migrated", key=key)
    return migrated

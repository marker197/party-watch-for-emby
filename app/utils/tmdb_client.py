"""Lightweight TMDB API client — watch providers & release dates.

The API key is stored in Redis (key ``tmdb_api_key``), configured via the
settings page.  If no key is set, all methods return empty results so the
feature degrades silently.
"""

from __future__ import annotations

import httpx
import structlog

from app.utils.redis_cache import get_redis

log = structlog.get_logger()

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w45"

# Map TMDB network/provider names to local icon slugs.
# Keys are lowercase for case-insensitive matching.
_ICON_MAP = {
    "apple tv+": "apple-tv-plus",
    "apple tv plus": "apple-tv-plus",
    "netflix": "netflix",
    "max": "max",
    "hbo": "hbo",
    "hbo max": "max",
    "disney+": "disney-plus",
    "disney plus": "disney-plus",
    "amazon prime video": "amazon-prime-video",
    "prime video": "amazon-prime-video",
    "paramount+": "paramount-plus",
    "paramount plus": "paramount-plus",
    "hulu": "hulu",
    "peacock": "peacock",
    "peacock premium": "peacock",
    "bbc iplayer": "bbc-iplayer",
    "bbc one": "bbc-iplayer",
    "bbc two": "bbc-iplayer",
    "bbc three": "bbc-iplayer",
    "itv": "itv",
    "itv1": "itv",
    "itvx": "itv",
    "channel 4": "channel-4",
    "e4": "channel-4",
    "more4": "channel-4",
    "now tv": "now-tv",
    "now": "now-tv",
    "sky atlantic": "sky-go",
    "sky go": "sky-go",
    "sky one": "sky-go",
    "sky max": "sky-go",
    "britbox": "britbox",
    "crunchyroll": "crunchyroll",
    "starz": "starz",
    "showtime": "showtime",
    "sho": "showtime",
    "mgm+": "mgm-plus",
    "mgm plus": "mgm-plus",
    "amc": "amc-plus",
    "amc+": "amc-plus",
    "amc plus": "amc-plus",
    "bet+": "bet-plus",
    "discovery+": "discovery-plus",
    "discovery plus": "discovery-plus",
    "crave": "crave",
    "stan": "stan",
    "binge": "binge",
}


def _resolve_icon(name: str) -> str:
    """Return the local icon URL for a provider/network name, or empty string."""
    slug = _ICON_MAP.get(name.lower().strip())
    if slug:
        return f"/static/provider-icons/{slug}.svg"
    return ""


async def _get_api_key() -> str | None:
    """Read the TMDB API key from Redis."""
    try:
        r = await get_redis()
        key = await r.get("tmdb_api_key")
        return key if key else None
    except Exception:
        return None


async def get_watch_providers(
    tmdb_id: int,
    media_type: str = "movie",
    country: str = "US",
) -> list[dict]:
    """Return streaming/network info for a movie or TV show.

    For TV shows: fetches the show details to get the ``networks`` field
    (e.g. Apple TV+, HBO, Netflix).  This is where the show *airs*, not
    where it can be rented/bought.

    For movies: fetches the watch/providers endpoint for ``flatrate``
    (subscription streaming) providers in the given country.

    Returns a list of dicts: ``[{name, logo_url}]``
    """
    api_key = await _get_api_key()
    if not api_key:
        return []

    cache_key = f"tmdb_providers:{media_type}:{tmdb_id}:{country}"
    try:
        r = await get_redis()
        cached = await r.get(cache_key)
        if cached:
            import json
            return json.loads(cached)
    except Exception:
        pass

    kind = "tv" if media_type in ("tv", "show") else "movie"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            if kind == "tv":
                # For TV: fetch show details → networks array
                resp = await client.get(
                    f"{TMDB_BASE}/tv/{tmdb_id}",
                    params={"api_key": api_key},
                )
                resp.raise_for_status()
                data = resp.json()

                providers = []
                for net in data.get("networks", []):
                    name = net.get("name", "")
                    logo_path = net.get("logo_path", "")
                    local_icon = _resolve_icon(name)
                    providers.append({
                        "name": name,
                        "logo_url": local_icon or (f"{TMDB_IMAGE_BASE}{logo_path}" if logo_path else ""),
                    })

                log.info("tmdb.networks_fetched", tmdb_id=tmdb_id,
                         networks=[p["name"] for p in providers])

            else:
                # For movies: fetch watch/providers → flatrate
                resp = await client.get(
                    f"{TMDB_BASE}/movie/{tmdb_id}/watch/providers",
                    params={"api_key": api_key},
                )
                resp.raise_for_status()
                data = resp.json()

                results_by_country = data.get("results", {})
                country_data = results_by_country.get(country.upper(), {})

                providers = []
                for p in country_data.get("flatrate", []):
                    name = p.get("provider_name", "")
                    logo_path = p.get("logo_path", "")
                    local_icon = _resolve_icon(name)
                    providers.append({
                        "name": name,
                        "logo_url": local_icon or (f"{TMDB_IMAGE_BASE}{logo_path}" if logo_path else ""),
                    })

                log.info("tmdb.providers_fetched", tmdb_id=tmdb_id,
                         country=country.upper(),
                         available_countries=list(results_by_country.keys())[:10],
                         providers_found=len(providers))

        # Cache: 24h for results with data, 2h for empty
        try:
            import json
            r = await get_redis()
            ttl = 86400 if providers else 7200
            await r.setex(cache_key, ttl, json.dumps(providers))
        except Exception:
            pass

        return providers

    except Exception as e:
        log.debug("tmdb.providers_failed", tmdb_id=tmdb_id,
                  media_type=kind, error=str(e)[:120])
        return []


# TMDB release_date type codes
_RELEASE_TYPE_THEATRICAL = {1, 2, 3}  # Premiere, Theatrical (limited), Theatrical
_RELEASE_TYPE_DIGITAL = {4}
_RELEASE_TYPE_PHYSICAL = {5}


async def get_movie_release_dates(
    tmdb_id: int,
    country: str = "us",
) -> tuple[str | None, str | None, str | None]:
    """Return (theatrical, digital, physical) date strings for a movie.

    Hits ``/movie/{id}/release_dates`` which returns typed releases per
    country.  Tries the requested country first, falls back to US.

    Returns ISO date strings (YYYY-MM-DD) or None for each slot.
    Degrades silently if no TMDB key is configured.
    """
    api_key = await _get_api_key()
    if not api_key or not tmdb_id:
        return None, None, None

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{TMDB_BASE}/movie/{tmdb_id}/release_dates",
                params={"api_key": api_key},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        log.debug("tmdb.release_dates_failed", tmdb_id=tmdb_id,
                  error=str(e)[:120])
        return None, None, None

    # Build lookup: country_code → list of releases
    by_country: dict[str, list[dict]] = {}
    for entry in data.get("results", []):
        cc = (entry.get("iso_3166_1") or "").lower()
        if cc:
            by_country[cc] = entry.get("release_dates", [])

    # Try requested country first, then US fallback
    countries = [country.lower()]
    if country.lower() != "us":
        countries.append("us")

    theatrical = None
    digital = None
    physical = None

    for cc in countries:
        releases = by_country.get(cc, [])
        for rel in releases:
            rtype = rel.get("type")
            rdate = (rel.get("release_date") or "")[:10]  # "2026-07-15T00:00:00.000Z" → "2026-07-15"
            if not rdate or len(rdate) < 10:
                continue

            if rtype in _RELEASE_TYPE_THEATRICAL and not theatrical:
                theatrical = rdate
            elif rtype in _RELEASE_TYPE_DIGITAL and not digital:
                digital = rdate
            elif rtype in _RELEASE_TYPE_PHYSICAL and not physical:
                physical = rdate

        if theatrical or digital or physical:
            break  # got data from this country, stop

    log.debug("tmdb.release_dates_resolved", tmdb_id=tmdb_id,
              country=country, theatrical=theatrical,
              digital=digital, physical=physical)

    return theatrical, digital, physical

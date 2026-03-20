import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import asyncpg
import httpx
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("businessman_feasibility")


class Settings:
    DATABASE_URL:              str   = os.getenv("DATABASE_URL", "")
    DB_POOL_MIN_SIZE:          int   = int(os.getenv("DB_POOL_MIN_SIZE",   "2"))
    DB_POOL_MAX_SIZE:          int   = int(os.getenv("DB_POOL_MAX_SIZE",   "10"))
    DB_COMMAND_TIMEOUT:        int   = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
    REDIS_URL:                 str   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS:     int   = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
    CACHE_TTL:                 int   = int(os.getenv("CACHE_TTL",       "3600"))
    CACHE_EMPTY_TTL:           int   = int(os.getenv("CACHE_EMPTY_TTL", "120"))
    RATE_LIMIT_PER_MIN:        int   = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))
    DEFAULT_PAGE_SIZE:         int   = int(os.getenv("DEFAULT_PAGE_SIZE",  "20"))
    MAX_PAGE_SIZE:             int   = int(os.getenv("MAX_PAGE_SIZE",      "100"))
    HIGH_STREET_THRESHOLD:     float = float(os.getenv("HIGH_STREET_THRESHOLD", "15000"))
    LOW_STREET_THRESHOLD:      float = float(os.getenv("LOW_STREET_THRESHOLD",  "5000"))
    GOOGLE_PLACES_API_KEY:     str   = os.getenv("GOOGLE_PLACES_API_KEY", "")
    GOOGLE_PLACES_CACHE_TTL:   int   = int(os.getenv("GOOGLE_PLACES_CACHE_TTL", str(60 * 60 * 24 * 30)))
    GOOGLE_PLACES_RADIUS:      int   = int(os.getenv("GOOGLE_PLACES_RADIUS", "500"))
    GOOGLE_PLACES_CONCURRENCY: int   = int(os.getenv("GOOGLE_PLACES_CONCURRENCY", "5"))
    CACHE_WARM_CONCURRENCY:    int   = int(os.getenv("CACHE_WARM_CONCURRENCY", "10"))

settings = Settings()

_metrics: Dict[str, int] = {
    "requests_total":              0,
    "requests_ok":                 0,
    "requests_error":              0,
    "rate_limited":                0,
    "property_cache_hits":         0,
    "property_empty_cache_hits":   0,
    "db_errors":                   0,
    "google_places_calls":         0,
    "google_places_cache_hits":    0,
    "google_places_errors":        0,
    "google_places_high_street":   0,
    "google_places_low_street":    0,
    "google_places_middle_street": 0,
    "99acres_rows_fetched":        0,
    "magicbricks_rows_fetched":    0,
    "99acres_price_filtered":      0,
    "magicbricks_price_filtered":  0,
}

def inc(key: str, n: int = 1) -> None:
    _metrics[key] = _metrics.get(key, 0) + n


redis_client:  redis.Redis        = None
_db_pool:      asyncpg.Pool       = None
_http_client:  httpx.AsyncClient  = None
_gp_semaphore: asyncio.Semaphore  = None


async def inc(key: str, n: int = 1) -> None:
    _metrics[key] = _metrics.get(key, 0) + n
    try:
        await redis_client.hincrby("biz:metrics:global", key, n)
    except Exception:
        pass


async def _prewarm_google_places_cache() -> None:
    logger.info("[PREWARM] Starting Google Places cache pre-warm for all MagicBricks coordinates")
    rows = await _db_fetch("""
        SELECT DISTINCT scraped_data->>'location' AS location
        FROM property_raw_data
        WHERE source = 'magicbricks'
          AND scraped_data->>'location' IS NOT NULL
          AND scraped_data->>'location' ~ '^[0-9]'
    """)

    coords: List[Tuple[float, float]] = []
    for row in rows:
        raw = row["location"]
        if not raw:
            continue
        try:
            parts = raw.strip().split(",")
            if len(parts) != 2:
                continue
            lat = float(parts[0].strip())
            lng = float(parts[1].strip())
            if 6.0 <= lat <= 37.0 and 68.0 <= lng <= 97.0:
                coords.append((round(lat, 4), round(lng, 4)))
        except (ValueError, TypeError):
            continue

    unique_coords = list(set(coords))
    logger.info(f"[PREWARM] Found {len(unique_coords)} unique coordinates to check")

    already_cached = 0
    to_fetch:       List[Tuple[float, float]] = []
    for lat, lng in unique_coords:
        cache_key = f"gp:area:{lat}:{lng}"
        cached    = await RedisCache.get(cache_key)
        if cached is not None:
            already_cached += 1
        else:
            to_fetch.append((lat, lng))

    logger.info(
        f"[PREWARM] already_cached={already_cached} "
        f"need_api_call={len(to_fetch)}"
    )

    if not to_fetch:
        logger.info("[PREWARM] All coordinates already cached — nothing to do")
        return

    sem      = asyncio.Semaphore(settings.CACHE_WARM_CONCURRENCY)
    done     = 0
    errors   = 0
    t0       = time.monotonic()

    async def _warm_one(lat: float, lng: float) -> None:
        nonlocal done, errors
        async with sem:
            try:
                await _get_area_type_from_coordinates(lat, lng)
                done += 1
                if done % 50 == 0:
                    elapsed = round(time.monotonic() - t0, 1)
                    logger.info(
                        f"[PREWARM] Progress: {done}/{len(to_fetch)} "
                        f"errors={errors} elapsed={elapsed}s"
                    )
            except Exception as e:
                errors += 1
                logger.warning(f"[PREWARM] Failed lat={lat} lng={lng}: {e}")

    await asyncio.gather(*[_warm_one(lat, lng) for lat, lng in to_fetch])

    elapsed = round(time.monotonic() - t0, 1)
    logger.info(
        f"[PREWARM] Complete: warmed={done} errors={errors} "
        f"already_cached={already_cached} elapsed={elapsed}s"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, _db_pool, _http_client, _gp_semaphore

    logger.info("[STARTUP] Initialising all shared resources")

    if not settings.DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var is not set — cannot start")

    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=3.0, read=8.0, write=3.0, pool=3.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
    logger.info("[STARTUP] HTTP client ready")

    _db_pool = await asyncpg.create_pool(
        settings.DATABASE_URL,
        min_size=settings.DB_POOL_MIN_SIZE,
        max_size=settings.DB_POOL_MAX_SIZE,
        command_timeout=settings.DB_COMMAND_TIMEOUT,
    )
    async with _db_pool.acquire() as conn:
        await conn.execute("SELECT 1")
    logger.info(
        f"[STARTUP] DB pool ready "
        f"min={settings.DB_POOL_MIN_SIZE} max={settings.DB_POOL_MAX_SIZE}"
    )

    redis_client = redis.from_url(
        settings.REDIS_URL,
        max_connections=settings.REDIS_MAX_CONNECTIONS,
        decode_responses=True,
    )
    await redis_client.ping()
    logger.info("[STARTUP] Redis ready")

    _gp_semaphore = asyncio.Semaphore(settings.GOOGLE_PLACES_CONCURRENCY)
    logger.info(
        f"[STARTUP] Google Places semaphore ready "
        f"concurrency={settings.GOOGLE_PLACES_CONCURRENCY}"
    )

    logger.info(
        f"[STARTUP] Cache TTL={settings.CACHE_TTL}s "
        f"empty_TTL={settings.CACHE_EMPTY_TTL}s "
        f"GP_cache_TTL={settings.GOOGLE_PLACES_CACHE_TTL}s"
    )

    if settings.GOOGLE_PLACES_API_KEY:
        logger.info("[STARTUP] Running Google Places cache pre-warm")
        await _prewarm_google_places_cache()
    else:
        logger.warning(
            "[STARTUP] GOOGLE_PLACES_API_KEY not set — "
            "skipping pre-warm, area type falls back to price/sqft only"
        )

    logger.info("[STARTUP] All resources ready — serving traffic")
    yield

    logger.info("[SHUTDOWN] Closing all shared resources")
    await _http_client.aclose()
    await _db_pool.close()
    await redis_client.aclose()
    logger.info("[SHUTDOWN] Clean shutdown complete")


def get_http_client() -> httpx.AsyncClient:
    return _http_client


def get_gp_semaphore() -> asyncio.Semaphore:
    return _gp_semaphore


class RedisCache:
    @staticmethod
    async def get(key: str) -> Optional[Any]:
        try:
            data = await redis_client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.warning(f"Redis GET failed [{key}]: {e}")
            return None

    @staticmethod
    async def set(key: str, value: Any, ttl: int = settings.CACHE_TTL) -> bool:
        try:
            await redis_client.setex(key, ttl, json.dumps(value, default=str))
            return True
        except Exception as e:
            logger.warning(f"Redis SET failed [{key}]: {e}")
            return False

    @staticmethod
    async def delete(*keys: str) -> int:
        try:
            return await redis_client.delete(*keys) if keys else 0
        except Exception as e:
            logger.warning(f"Redis DELETE failed: {e}")
            return 0

    @staticmethod
    async def keys_by_prefix(prefix: str) -> List[str]:
        try:
            keys:   List[str] = []
            cursor: int       = 0
            while True:
                cursor, batch = await redis_client.scan(
                    cursor, match=f"{prefix}*", count=100
                )
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.warning(f"Redis SCAN failed [{prefix}]: {e}")
            return []


class RedisRateLimiter:
    @staticmethod
    async def is_rate_limited(ip: str, prefix: str = "biz") -> bool:
        try:
            key    = f"{prefix}:rl:{ip}"
            now    = time.time()
            member = f"{now:.6f}-{uuid.uuid4().hex[:8]}"
            pipe   = redis_client.pipeline(transaction=True)
            pipe.zremrangebyscore(key, 0, now - 60)
            pipe.zadd(key, {member: now})
            pipe.zcard(key)
            pipe.expire(key, 60)
            results = await pipe.execute()
            count   = results[2]
            if count > settings.RATE_LIMIT_PER_MIN:
                inc("rate_limited")
                logger.warning(
                    f"[RATE_LIMIT] BLOCKED ip={ip} prefix={prefix} "
                    f"count={count}/{settings.RATE_LIMIT_PER_MIN} in last 60s"
                )
                return True
            logger.debug(
                f"[RATE_LIMIT] ALLOWED ip={ip} prefix={prefix} "
                f"count={count}/{settings.RATE_LIMIT_PER_MIN}"
            )
            return False
        except Exception as e:
            logger.warning(f"[RATE_LIMIT] Redis error (fail-open): {e}")
            return False


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


def normalize_city_input(city: str) -> str:
    return city.strip().title()


def extract_city_from_99acres_url(page_url: str) -> Optional[str]:
    if not page_url:
        return None
    try:
        parsed  = urlparse(page_url)
        params  = parse_qs(parsed.query)
        keyword = params.get("keyword", [None])[0]
        if keyword and keyword.strip():
            return keyword.strip().title()
    except Exception:
        pass
    try:
        m = re.search(r"/([a-z][a-z\-]+[a-z])-ffid", page_url, re.IGNORECASE)
        if m:
            return m.group(1).replace("-", " ").strip().title()
    except Exception:
        pass
    return None


_HIGH_STREET_PLACE_TYPES: frozenset = frozenset([
    "shopping_mall", "department_store", "jewelry_store",
    "clothing_store", "shoe_store", "electronics_store",
    "bank", "atm", "insurance_agency", "real_estate_agency",
    "accounting", "lawyer", "finance",
    "hotel", "lodging",
    "restaurant", "food", "cafe", "bar", "night_club",
    "subway_station", "train_station", "transit_station",
    "movie_theater", "amusement_park",
])

_LOW_STREET_PLACE_TYPES: frozenset = frozenset([
    "storage", "moving_company",
    "car_repair", "car_dealer", "car_wash",
    "electrician", "plumber", "locksmith",
    "roofing_contractor", "general_contractor", "painter",
    "hardware_store", "gas_station", "rv_park",
])

_GENERIC_ZONE_NOISE: frozenset = frozenset([
    "business park", "commercial zone", "it park", "tech park",
    "it/ites", "software park", "it hub", "campus",
])


def _parse_coordinates(item: dict) -> Tuple[Optional[float], Optional[float]]:
    raw = item.get("location")
    if not raw or not isinstance(raw, str):
        return None, None
    try:
        parts = raw.strip().split(",")
        if len(parts) != 2:
            return None, None
        lat = float(parts[0].strip())
        lng = float(parts[1].strip())
        if not (6.0 <= lat <= 37.0 and 68.0 <= lng <= 97.0):
            logger.warning(
                f"[COORDS] Out-of-India bounds id={item.get('id')} "
                f"lat={lat} lng={lng} — skipping Google Places"
            )
            return None, None
        return lat, lng
    except (ValueError, TypeError):
        return None, None


async def _call_google_places_api(lat: float, lng: float) -> str:
    if not settings.GOOGLE_PLACES_API_KEY:
        logger.warning(
            "[GOOGLE_PLACES] No API key — all rows use price/sqft detection only"
        )
        return "middle_street"

    call_n = _metrics["google_places_calls"] + 1
    inc("google_places_calls")
    logger.info(
        f"[GOOGLE_PLACES] ── API call #{call_n} ──────────────────────────────"
    )
    logger.info(
        f"[GOOGLE_PLACES] call#{call_n} lat={lat} lng={lng} "
        f"radius={settings.GOOGLE_PLACES_RADIUS}m "
        f"concurrency_limit={settings.GOOGLE_PLACES_CONCURRENCY} "
        f"lifetime_calls_so_far={call_n-1} cache_hits={_metrics['google_places_cache_hits']}"
    )

    async with get_gp_semaphore():
        t0 = time.monotonic()
        try:
            client = get_http_client()
            resp   = await client.get(
                "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                params={
                    "location": f"{lat},{lng}",
                    "radius":   settings.GOOGLE_PLACES_RADIUS,
                    "key":      settings.GOOGLE_PLACES_API_KEY,
                },
            )
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
            resp.raise_for_status()
            data   = resp.json()
            status = data.get("status", "")

            logger.info(
                f"[GOOGLE_PLACES] call#{call_n} response: "
                f"http_status={resp.status_code} api_status={status} elapsed={elapsed_ms}ms"
            )

            if status not in ("OK", "ZERO_RESULTS"):
                logger.error(
                    f"[GOOGLE_PLACES] call#{call_n} API error: "
                    f"status={status} lat={lat} lng={lng} "
                    f"error_message={data.get('error_message', 'none')} "
                    f"→ fallback middle_street"
                )
                inc("google_places_errors")
                return "middle_street"

            results     = data.get("results", [])
            place_count = len(results)

            if not results:
                logger.info(
                    f"[GOOGLE_PLACES] call#{call_n} ZERO_RESULTS "
                    f"lat={lat} lng={lng} → middle_street"
                )
                inc("google_places_middle_street")
                return "middle_street"

            all_types:   List[str] = []
            place_names: List[str] = []
            for place in results:
                all_types.extend(place.get("types", []))
                place_names.append(place.get("name", "unknown"))

            type_set   = set(all_types)
            high_hits  = type_set & _HIGH_STREET_PLACE_TYPES
            low_hits   = type_set & _LOW_STREET_PLACE_TYPES
            high_score = len(high_hits)
            low_score  = len(low_hits)

            if high_score >= 4:
                area_type = "high_street"
                inc("google_places_high_street")
            elif low_score >= 3:
                area_type = "low_street"
                inc("google_places_low_street")
            else:
                area_type = "middle_street"
                inc("google_places_middle_street")

            logger.info(
                f"[GOOGLE_PLACES] call#{call_n} scoring: "
                f"places_found={place_count} "
                f"high_score={high_score}/4 ({sorted(high_hits)}) "
                f"low_score={low_score}/3 ({sorted(low_hits)}) "
                f"→ {area_type.upper()}"
            )
            logger.info(
                f"[GOOGLE_PLACES] call#{call_n} nearby: {place_names[:6]}"
            )
            logger.info(
                f"[GOOGLE_PLACES] running totals: "
                f"calls={_metrics['google_places_calls']} "
                f"cache_hits={_metrics['google_places_cache_hits']} "
                f"high={_metrics['google_places_high_street']} "
                f"middle={_metrics['google_places_middle_street']} "
                f"low={_metrics['google_places_low_street']} "
                f"errors={_metrics['google_places_errors']}"
            )
            return area_type

        except httpx.TimeoutException:
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
            logger.error(
                f"[GOOGLE_PLACES] call#{call_n} TIMEOUT "
                f"lat={lat} lng={lng} elapsed={elapsed_ms}ms → middle_street"
            )
            inc("google_places_errors")
            return "middle_street"
        except httpx.HTTPStatusError as e:
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
            logger.error(
                f"[GOOGLE_PLACES] call#{call_n} HTTP {e.response.status_code} "
                f"lat={lat} lng={lng} elapsed={elapsed_ms}ms → middle_street"
            )
            inc("google_places_errors")
            return "middle_street"
        except Exception as e:
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
            logger.error(
                f"[GOOGLE_PLACES] call#{call_n} unexpected error "
                f"lat={lat} lng={lng} elapsed={elapsed_ms}ms: {e}",
                exc_info=True,
            )
            inc("google_places_errors")
            return "middle_street"


async def _get_area_type_from_coordinates(lat: float, lng: float) -> str:
    lat_r     = round(lat, 4)
    lng_r     = round(lng, 4)
    cache_key = f"gp:area:{lat_r}:{lng_r}"

    cached = await RedisCache.get(cache_key)
    if cached is not None:
        inc("google_places_cache_hits")
        logger.info(
            f"[GOOGLE_PLACES] Cache HIT {cache_key} → {cached} "
            f"(saves an API call — lifetime hits={_metrics['google_places_cache_hits']} "
            f"calls={_metrics['google_places_calls']})"
        )
        return cached

    logger.info(
        f"[GOOGLE_PLACES] Cache MISS {cache_key} — calling API "
        f"(lifetime hits={_metrics['google_places_cache_hits']} "
        f"calls={_metrics['google_places_calls']})"
    )
    area_type = await _call_google_places_api(lat_r, lng_r)

    await redis_client.setex(
        cache_key,
        settings.GOOGLE_PLACES_CACHE_TTL,
        json.dumps(area_type),
    )
    logger.info(
        f"[GOOGLE_PLACES] Stored in cache: {cache_key} = {area_type} "
        f"TTL={settings.GOOGLE_PLACES_CACHE_TTL}s (30 days)"
    )
    return area_type



_UI_CATEGORY_MAP: Dict[str, List[str]] = {
    "office_space": [
        "ready_to_move_office", "bare_shell_office", "office_space",
        "retail_mall_office", "it_park_office", "business_park_office",
        "business_center",
    ],
    "shop_showroom":      ["shop", "showroom"],
    "commercial_land":    ["commercial_land"],
    "coworking":          ["coworking"],
    "warehouse_godown":   ["warehouse_godown"],
    "industrial_building": ["industrial_building"],
    "industrial_shed":    ["industrial_shed"],
}

PROPERTY_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "ready_to_move_office": [
        "ready to move office space", "ready to move office",
        "ready-to-move office", "rtm office",
        "ready to move",
    ],
    "bare_shell_office": [
        "bare shell office space", "bare shell office",
        "bare-shell office", "bareshell",
    ],
    "office_space": [
        "commercial office space", "office space",
        "ready to move office space", "ready to move office",
        "ready-to-move office", "rtm office", "ready to move",
        "bare shell office space", "bare shell office",
        "bare-shell office", "bareshell",
        "space in a retail mall", "retail mall",
        "office in an it park", "it park office",
        "office in a business park", "business park office",
        "business center", "business centre",
        "time share",
    ],
    "shop_showroom": [
        "shop for sale", "shop for rent", "commercial shop", "shop",
        "showroom for sale", "showroom for rent", "commercial showroom", "showroom",
    ],
    "shop": [
        "shop for sale", "shop for rent", "commercial shop", "shop",
    ],
    "showroom": [
        "showroom for sale", "showroom for rent", "commercial showroom", "showroom",
    ],
    "commercial_land": [
        "commercial plot", "commercial land", "commercial plot / land",
        "commercial/inst. land", "commercial institutional land",
        "commercial/industrial land",
        "plot",
    ],
    "farm_land": [
        "agricultural", "farm", "agriculture land", "farm land",
        "farm / agriculture", "agricultural/farm land",
    ],
    "industrial_land": [
        "industrial land", "industrial plot", "industrial land / plot",
        "industrial lands/plots",
    ],
    "warehouse_godown": [
        "warehouse", "godown", "warehouse-godown",
        "warehouse/godown", "warehouse/ godown", "warehouse godown",
    ],
    "cold_storage":        ["cold storage", "cold-storage"],
    "factory":             ["factory"],
    "industrial_building": ["industrial building", "industrial-building"],
    "industrial_shed":     ["industrial shed", "industrial-shed"],
    "hotel_resort": [
        "hotel", "resort", "hotel/resort", "hotel resort", "guest house",
    ],
    "restaurant": [
        "restaurant", "food court", "cafe", "cafeteria", "cloud kitchen",
    ],
    "coworking": [
        "coworking", "co-working", "co working", "coworking space",
    ],
    "manufacturing": [
        "manufacturing", "manufacturing business", "manufacturing unit",
    ],
    "all": [],
}

_MB_OFFICE_TYPES: frozenset = frozenset([
    "office_space", "ready_to_move_office", "bare_shell_office",
])

_MB_TYPE_MAP: Dict[str, List[str]] = {
    "office_space":        ["commercial office space", "office space", "business center", "business centre"],
    "ready_to_move_office": ["commercial office space", "office space"],
    "bare_shell_office":   ["commercial office space", "office space"],
    "shop_showroom":       ["commercial shop", "commercial showroom", "shop", "showroom"],
    "shop":                ["commercial shop", "shop"],
    "showroom":            ["commercial showroom", "showroom"],
    "warehouse_godown":    ["warehouse", "godown"],
    "industrial_building": ["industrial building"],
    "industrial_shed":     ["industrial shed"],
    "commercial_land":     ["commercial land", "land"],
    "farm_land":           ["farm land", "agricultural land", "farm"],
    "industrial_land":     ["industrial land"],
    "coworking":           ["coworking", "co-working"],
    "hotel_resort":        ["hotel", "resort"],
    "factory":             ["factory"],
    "cold_storage":        ["cold storage"],
    "manufacturing":       ["manufacturing"],
    "restaurant":          ["restaurant", "cafe", "food court"],
}


def matches_property_type(item: dict, requested_type: str) -> bool:
    if requested_type == "all":
        return True

    keywords = PROPERTY_TYPE_KEYWORDS.get(requested_type, [])
    if not keywords:
        return True

    source    = item.get("_source", "")
    name_raw  = str(item.get("name",         "") or "").lower().strip()
    pt_raw    = str(item.get("propertyType", "") or "").lower().strip()
    title_raw = str(item.get("title",        "") or "").lower().strip()

    if source == "magicbricks" and name_raw:
        mb_kws = _MB_TYPE_MAP.get(requested_type, keywords)
        if any(kw in name_raw for kw in mb_kws):
            return True
        if requested_type in ("ready_to_move_office", "bare_shell_office"):
            label = "ready" if requested_type == "ready_to_move_office" else "bare"
            return label in name_raw
        return False

    for text in [pt_raw, title_raw, name_raw]:
        if not text:
            continue
        for part in [p.strip() for p in text.split(",")]:
            if any(kw in part for kw in keywords):
                return True

    return False


ZONE_KEYWORDS: Dict[str, List[str]] = {
    "front_shop": [
        "ground floor", "main road", "highway facing",
        "street facing", "high street", "main street",
        "g floor", "gf", "ground level", "roadside",
        "road facing", "ground", "front",
    ],
    "first_floor": [
        "first floor", "1st floor", "1 floor", "floor 1",
        "above ground", "first level", "floor no 1",
    ],
    "back_site": [
        "back", "rear", "interior", "basement",
        "midc", "it park", "business park",
        "commercial zone", "tech park", "it/ites",
        "software park", "it hub", "campus",
    ],
}

_MEANINGFUL_ZONE_KEYWORDS: Dict[str, List[str]] = {
    "front_shop": [
        "ground floor", "main road", "highway facing", "street facing",
        "high street", "main street", "road facing",
    ],
    "first_floor": [
        "first floor", "1st floor", "floor 1", "above ground", "first level",
    ],
    "back_site": [
        "back", "rear", "interior", "basement", "midc",
    ],
}


def _item_combined_text(item: dict) -> str:
    return " ".join([
        str(item.get("propertyType",     "") or ""),
        str(item.get("description",      "") or ""),
        str(item.get("seo_description",  "") or ""),
        str(item.get("title",            "") or ""),
        str(item.get("name",             "") or ""),
        str(item.get("floorSize",        "") or ""),
        str(item.get("possessionStatus", "") or ""),
        str(item.get("areaType",         "") or ""),
        str(item.get("address",          "") or ""),
        str(item.get("landmark",         "") or ""),
        str(item.get("amenities",        "") or ""),
    ]).lower()


def matches_zone(item: dict, zone: str) -> bool:
    combined    = _item_combined_text(item)
    other_zones = [z for z in ZONE_KEYWORDS if z != zone]

    if zone == "first_floor":
        floor_val = item.get("floors")
        if floor_val is not None:
            try:
                if int(floor_val) == 1:
                    return True
            except (ValueError, TypeError):
                pass

    meaningful_requested = _MEANINGFUL_ZONE_KEYWORDS.get(zone, ZONE_KEYWORDS.get(zone, []))
    if any(kw in combined for kw in meaningful_requested):
        return True

    matches_other_meaningful = any(
        any(kw in combined for kw in _MEANINGFUL_ZONE_KEYWORDS.get(z, []))
        for z in other_zones
    )

    if not matches_other_meaningful:
        return True

    return False


_NO_AREA_VALUES: frozenset = frozenset([
    "bare bhk", "bare", "bhk", "rk", "n/a", "see config",
    "see configurations", "studio", "ready bhk", "shop bhk",
    "na", "none", "",
])


def _extract_area_sqft(item: dict) -> Optional[float]:
    source = item.get("_source", "")

    if source == "magicbricks":
        area_fields = ("covered_area", "carpet_area")
    else:
        area_fields = ("carpet_area", "covered_area")

    for field in area_fields:
        val = item.get(field)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    return f
            except (ValueError, TypeError):
                pass

    bedrooms_str = str(item.get("bedrooms", "") or "").strip()
    if bedrooms_str and bedrooms_str.lower() not in _NO_AREA_VALUES:
        m = re.search(r"([\d,]+)\s*sq\.?\s*ft", bedrooms_str, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass

    for field in ("description", "seo_description"):
        desc = str(item.get(field, "") or "")
        if desc:
            m = re.search(r"([\d,]+)\s*sq\.?\s*ft", desc, re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 0:
                        return val
                except (ValueError, TypeError):
                    pass

    return None


def _classify_ppsf(ppsf: float) -> str:
    if ppsf >= settings.HIGH_STREET_THRESHOLD:
        return "high_street"
    if ppsf <= settings.LOW_STREET_THRESHOLD:
        return "low_street"
    return "middle_street"


def _detect_area_type_sync(item: dict) -> Optional[str]:
    source = item.get("_source", "")
    ppsf:   Optional[float] = None
    method: str             = ""

    mb_ppsf = item.get("price_per_sq_ft")
    if mb_ppsf is not None and source == "magicbricks":
        try:
            v = float(mb_ppsf)
            if v > 0:
                ppsf   = v
                method = "mb_price_per_sqft_field"
        except (ValueError, TypeError):
            pass

    if ppsf is None:
        ppsf_str = str(item.get("pricePerSqft", "") or "")
        if "/sqft" in ppsf_str.lower():
            digits = re.sub(r"[^\d.]", "", ppsf_str.replace(",", ""))
            if digits:
                try:
                    v = float(digits)
                    if v > 0:
                        ppsf   = v
                        method = "99acres_pricePerSqft_string"
                except (ValueError, TypeError):
                    pass

    if ppsf is None:
        price = item.get("_price_numeric")
        area  = _extract_area_sqft(item)
        if price and area and area > 0:
            ppsf   = price / area
            method = f"derived(price={price:.0f}/area={area:.0f})"

    if ppsf is not None:
        result = _classify_ppsf(ppsf)
        logger.debug(
            f"[AREA_TYPE_SYNC] id={item.get('id')} source={source} "
            f"method={method} ppsf=₹{ppsf:.0f} "
            f"[low<={settings.LOW_STREET_THRESHOLD} high>={settings.HIGH_STREET_THRESHOLD}] "
            f"→ {result}"
        )
        return result

    if source == "99acres":
        price = item.get("_price_numeric")
        if price:
            if price >= 30_000_000:
                result = "high_street"
            elif price >= 5_000_000:
                result = "middle_street"
            else:
                result = "low_street"
            logger.debug(
                f"[AREA_TYPE_SYNC] id={item.get('id')} source=99acres "
                f"method=price_band price=₹{price:,.0f} → {result}"
            )
            return result

    logger.debug(
        f"[AREA_TYPE_SYNC] id={item.get('id')} source={source} "
        f"no sync signal available — will attempt Google Places"
    )
    return None


async def detect_area_type(item: dict) -> str:
    result = _detect_area_type_sync(item)
    if result is not None:
        return result

    lat, lng = _parse_coordinates(item)
    if lat is not None and lng is not None:
        logger.info(
            f"[AREA_TYPE] id={item.get('id')} source={item.get('_source')} "
            f"no ppsf/price-band → escalating to Google Places lat={lat} lng={lng}"
        )
        return await _get_area_type_from_coordinates(lat, lng)

    logger.debug(
        f"[AREA_TYPE] id={item.get('id')} source={item.get('_source')} "
        f"no ppsf, no coordinates → default middle_street"
    )
    return "middle_street"


def _parse_99acres_price(price_str: str) -> Optional[float]:
    if not price_str or "on request" in price_str.lower():
        return None
    try:
        clean = price_str.replace("₹", "").replace(",", "").strip()
        clean = re.sub(r"/month.*",       "", clean, flags=re.IGNORECASE).strip()
        clean = re.sub(r"\s*onwards?\s*", "", clean, flags=re.IGNORECASE).strip()
        first = re.split(r"[\s\-–]", clean)[0]
        value = float(first)
        upper = price_str.upper()
        if "CR"  in upper:
            return value * 10_000_000
        if "L" in upper or "LAC" in upper or "LAKH" in upper:
            return value * 100_000
        return value
    except Exception as e:
        logger.warning(f"[PRICE_PARSE] 99acres failed for '{price_str}': {e}")
        return None


async def _db_fetch(sql: str, *args) -> List[asyncpg.Record]:
    try:
        async with _db_pool.acquire() as conn:
            return await conn.fetch(sql, *args)
    except Exception as e:
        inc("db_errors")
        logger.error(f"[DB] fetch error: {e}", exc_info=True)
        return []


async def fetch_99acres_commercial(
    city: str, min_price: float, max_price: float, listing_type: str
) -> List[dict]:
    logger.info(
        f"[99ACRES] ── Fetch start ──────────────────────────────────────────"
    )
    logger.info(
        f"[99ACRES] city={city!r} listing_type={listing_type} "
        f"price_range=[₹{min_price:,.0f} – ₹{max_price:,.0f}]"
    )

    sql = """
        SELECT scraped_data
        FROM property_raw_data
        WHERE source = '99acres'
          AND listing_type = $1
          AND scraped_data->>'pageUrl' IS NOT NULL
          AND (
               scraped_data->>'pageUrl' ILIKE $2
            OR scraped_data->>'pageUrl' ILIKE $3
          )
          AND scraped_data->>'priceRange' IS NOT NULL
          AND scraped_data->>'priceRange' NOT ILIKE '%on request%'
    """
    t0   = time.monotonic()
    rows = await _db_fetch(
        sql,
        listing_type,
        f"%keyword={city}%",
        f"%keyword={city.lower()}%",
    )
    db_ms = round((time.monotonic() - t0) * 1000, 1)

    inc("99acres_rows_fetched", len(rows))
    logger.info(
        f"[99ACRES] DB returned {len(rows)} rows in {db_ms}ms "
        f"(matching city={city!r} in pageUrl keyword param)"
    )

    out:                  List[dict] = []
    seen_ids:             set        = set()
    skipped_no_price:     int        = 0
    skipped_price_range:  int        = 0
    skipped_parse_error:  int        = 0
    skipped_duplicate:    int        = 0

    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = (
                json.loads(raw) if isinstance(raw, str)
                else raw         if isinstance(raw, dict)
                else None
            )
            if item is None:
                skipped_parse_error += 1
                continue

            listing_id = item.get("id", "")
            if listing_id and listing_id in seen_ids:
                skipped_duplicate += 1
                logger.debug(
                    f"[99ACRES] Row {i} id={listing_id} "
                    f"skipped: duplicate (same listing scraped from multiple pageUrl prop_type params)"
                )
                continue
            if listing_id:
                seen_ids.add(listing_id)

            price_str = item.get("priceRange", "")
            price     = _parse_99acres_price(price_str)
            if price is None:
                skipped_no_price += 1
                logger.debug(
                    f"[99ACRES] Row {i} id={item.get('id')} "
                    f"skipped: could not parse price='{price_str}'"
                )
                continue

            if not (min_price <= price <= max_price):
                skipped_price_range += 1
                logger.debug(
                    f"[99ACRES] Row {i} id={item.get('id')} "
                    f"skipped: ₹{price:,.0f} outside [₹{min_price:,.0f}–₹{max_price:,.0f}]"
                )
                continue

            item["_source"]        = "99acres"
            item["_listing_type"]  = listing_type
            item["_price_numeric"] = price
            out.append(item)

        except Exception as e:
            skipped_parse_error += 1
            logger.error(f"[99ACRES] Row {i} exception: {e}", exc_info=True)

    inc("99acres_price_filtered", len(out))

    pass_rate = round(len(out) / len(rows) * 100, 1) if rows else 0.0
    logger.info(
        f"[99ACRES] ── Fetch complete ───────────────────────────────────────"
    )
    logger.info(
        f"[99ACRES] db_rows={len(rows)} "
        f"unique_passed={len(out)} ({pass_rate}%) "
        f"skipped_duplicate={skipped_duplicate} "
        f"skipped_unparseable_price={skipped_no_price} "
        f"skipped_out_of_range={skipped_price_range} "
        f"skipped_errors={skipped_parse_error}"
    )
    return out


async def fetch_magicbricks_commercial(
    city: str, min_price: float, max_price: float, listing_type: str
) -> List[dict]:
    logger.info(
        f"[MAGICBRICKS] ── Fetch start ─────────────────────────────────────"
    )
    logger.info(
        f"[MAGICBRICKS] city={city!r} listing_type={listing_type} "
        f"price_range=[₹{min_price:,.0f} – ₹{max_price:,.0f}]"
    )

    sql = r"""
        SELECT scraped_data
        FROM property_raw_data
        WHERE source = 'magicbricks'
          AND listing_type = $1
          AND (
               scraped_data->>'city_name' ILIKE $2
            OR scraped_data->>'address'   ILIKE $2
            OR scraped_data->>'from_url'  ILIKE $5
          )
          AND scraped_data->>'price' IS NOT NULL
          AND scraped_data->>'price' ~ '^[0-9]+(\.[0-9]+)?$'
          AND (scraped_data->>'price')::numeric BETWEEN $3 AND $4
          AND scraped_data->>'from_url' ILIKE '%commercial-real-estate%'
    """
    t0   = time.monotonic()
    rows = await _db_fetch(
        sql,
        listing_type,
        f"%{city}%",
        int(min_price),
        int(max_price),
        f"%cityName={city}%",
    )
    db_ms = round((time.monotonic() - t0) * 1000, 1)

    inc("magicbricks_rows_fetched", len(rows))
    logger.info(
        f"[MAGICBRICKS] DB returned {len(rows)} rows in {db_ms}ms "
        f"(price filter ₹{min_price:,.0f}–₹{max_price:,.0f} applied in SQL)"
    )

    out:               List[dict] = []
    seen_ids:          set        = set()
    seen_fingerprints: set        = set()
    skipped_no_price:  int        = 0
    skipped_bad_price: int        = 0
    skipped_duplicate: int        = 0

    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = (
                json.loads(raw) if isinstance(raw, str)
                else raw         if isinstance(raw, dict)
                else None
            )
            if item is None:
                skipped_bad_price += 1
                continue

            listing_id = str(item.get("id", ""))
            if listing_id and listing_id in seen_ids:
                skipped_duplicate += 1
                logger.debug(
                    f"[MAGICBRICKS] Row {i} id={listing_id} "
                    f"skipped: duplicate id"
                )
                continue
            if listing_id:
                seen_ids.add(listing_id)

            price = item.get("price")
            if price is None:
                skipped_no_price += 1
                logger.debug(
                    f"[MAGICBRICKS] Row {i} id={item.get('id')} "
                    f"skipped: price field null"
                )
                continue
            try:
                price_float = float(price)
            except (ValueError, TypeError):
                skipped_bad_price += 1
                logger.warning(
                    f"[MAGICBRICKS] Row {i} id={item.get('id')} "
                    f"skipped: non-numeric price='{price}'"
                )
                continue

            loc       = str(item.get("location", "") or "")
            cov_area  = str(item.get("covered_area",  "") or "")
            carp_area = str(item.get("carpet_area",   "") or "")
            fingerprint = f"{int(price_float)}:{loc}:{cov_area}:{carp_area}"
            if fingerprint in seen_fingerprints:
                skipped_duplicate += 1
                logger.debug(
                    f"[MAGICBRICKS] Row {i} id={listing_id} "
                    f"skipped: same project+price+area+location listed by different broker"
                )
                continue
            seen_fingerprints.add(fingerprint)

            item["_source"]        = "magicbricks"
            item["_listing_type"]  = listing_type
            item["_price_numeric"] = price_float
            out.append(item)

        except Exception as e:
            skipped_bad_price += 1
            logger.error(f"[MAGICBRICKS] Row {i} exception: {e}", exc_info=True)

    inc("magicbricks_price_filtered", len(out))

    pass_rate      = round(len(out) / len(rows) * 100, 1) if rows else 0.0
    with_coords    = sum(1 for item in out if item.get("location"))
    without_coords = len(out) - with_coords

    logger.info(
        f"[MAGICBRICKS] ── Fetch complete ──────────────────────────────────"
    )
    logger.info(
        f"[MAGICBRICKS] db_rows={len(rows)} "
        f"unique_passed={len(out)} ({pass_rate}%) "
        f"skipped_duplicate={skipped_duplicate} "
        f"skipped_null_price={skipped_no_price} "
        f"skipped_bad_price={skipped_bad_price}"
    )
    logger.info(
        f"[MAGICBRICKS] Coordinate availability for Google Places: "
        f"have_coords={with_coords} no_coords={without_coords}"
    )
    return out


async def fetch_all_commercial(
    city: str, min_price: float, max_price: float, listing_type: str
) -> Dict[str, List[dict]]:
    cache_key = (
        f"biz:{listing_type}:"
        f"{hashlib.md5(f'{city.lower()}:{int(min_price)}:{int(max_price)}'.encode()).hexdigest()}"
    )

    logger.info(
        f"[FETCH] ── Combined fetch ─────────────────────────────────────────"
    )
    logger.info(
        f"[FETCH] city={city!r} listing_type={listing_type} "
        f"cache_key_suffix=...{cache_key[-12:]}"
    )

    cached = await RedisCache.get(cache_key)
    if cached is not None:
        n99 = len(cached.get("99acres",     []))
        nmb = len(cached.get("magicbricks", []))
        inc("property_cache_hits" if (n99 + nmb) > 0 else "property_empty_cache_hits")
        logger.info(
            f"[FETCH] Redis cache HIT — no DB queries needed "
            f"99acres={n99} magicbricks={nmb} total={n99+nmb}"
        )
        return cached

    logger.info(f"[FETCH] Redis cache MISS — fetching both sources in parallel")
    t0       = time.monotonic()
    r99, rmb = await asyncio.gather(
        fetch_99acres_commercial(city, min_price, max_price, listing_type),
        fetch_magicbricks_commercial(city, min_price, max_price, listing_type),
        return_exceptions=True,
    )
    fetch_ms = round((time.monotonic() - t0) * 1000, 1)

    result = {
        "99acres":     r99 if isinstance(r99, list) else [],
        "magicbricks": rmb if isinstance(rmb, list) else [],
    }

    if isinstance(r99, Exception):
        logger.error(f"[FETCH] 99acres raised exception: {r99}")
    if isinstance(rmb, Exception):
        logger.error(f"[FETCH] MagicBricks raised exception: {rmb}")

    all_ids:    set  = set()
    cross_dups: int  = 0
    for source in ("99acres", "magicbricks"):
        clean = []
        for item in result[source]:
            uid = f"{source}:{item.get('id', '')}"
            if uid in all_ids:
                cross_dups += 1
            else:
                all_ids.add(uid)
                clean.append(item)
        result[source] = clean

    total = sum(len(v) for v in result.values())
    ttl   = settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL
    await RedisCache.set(cache_key, result, ttl=ttl)

    logger.info(
        f"[FETCH] ── Fetch summary ──────────────────────────────────────────"
    )
    logger.info(
        f"[FETCH] 99acres={len(result['99acres'])} "
        f"magicbricks={len(result['magicbricks'])} "
        f"combined_total={total} "
        f"cross_source_dups_removed={cross_dups} "
        f"parallel_elapsed={fetch_ms}ms "
        f"cached_TTL={ttl}s"
    )
    return result


async def _apply_filters(
    props_by_src:  Dict[str, List[dict]],
    property_type: str,
    property_zone: str,
    area_type:     str,
    min_area:      float,
    max_area:      float,
) -> Dict[str, List[dict]]:

    total_input = sum(len(v) for v in props_by_src.values())

    logger.info(
        f"[FILTER] ════════════════════════════════════════════════════════"
    )
    logger.info(
        f"[FILTER] Stage 0 — Input "
        f"99acres={len(props_by_src.get('99acres',[]))} "
        f"magicbricks={len(props_by_src.get('magicbricks',[]))} "
        f"total={total_input}"
    )
    logger.info(
        f"[FILTER] Criteria: type={property_type} zone={property_zone} "
        f"area_type={area_type} area=[{min_area}–{max_area}sqft]"
    )

    logger.info(f"[FILTER] ── Stage 1: Property type filter ────────────────")
    pre_type: Dict[str, List[dict]] = {}
    for source, items in props_by_src.items():
        passed   = [i for i in items if matches_property_type(i, property_type)]
        rejected = len(items) - len(passed)
        pre_type[source] = passed
        logger.info(
            f"[FILTER] type={property_type!r} source={source}: "
            f"input={len(items)} passed={len(passed)} rejected={rejected}"
        )
    after_type = sum(len(v) for v in pre_type.values())
    logger.info(
        f"[FILTER] Stage 1 result: {after_type}/{total_input} passed type filter "
        f"(dropped {total_input - after_type})"
    )

    logger.info(f"[FILTER] ── Stage 2: Zone filter ─────────────────────────")
    pre_zone: Dict[str, List[dict]] = {}
    for source, items in pre_type.items():
        passed   = [i for i in items if matches_zone(i, property_zone)]
        rejected = len(items) - len(passed)
        pre_zone[source] = passed
        logger.info(
            f"[FILTER] zone={property_zone!r} source={source}: "
            f"input={len(items)} passed={len(passed)} rejected={rejected}"
        )
    after_zone = sum(len(v) for v in pre_zone.values())
    logger.info(
        f"[FILTER] Stage 2 result: {after_zone}/{after_type} passed zone filter "
        f"(dropped {after_type - after_zone})"
    )

    all_pre = [
        (source, item)
        for source, items in pre_zone.items()
        for item in items
    ]

    if not all_pre:
        logger.warning(
            f"[FILTER] Zero items survived type+zone. "
            f"type={property_type!r} zone={property_zone!r} — "
            f"check data has matching keywords or try broader filters"
        )
        return {src: [] for src in props_by_src}

    logger.info(f"[FILTER] ── Stage 3: Area type detection ─────────────────")
    logger.info(
        f"[FILTER] Running area type detection on {len(all_pre)} items "
        f"targeting='{area_type}'"
    )

    needs_gp_count  = 0
    sync_count      = 0
    no_signal_count = 0
    for _, item in all_pre:
        sync_result = _detect_area_type_sync(item)
        if sync_result is not None:
            sync_count += 1
        else:
            lat, lng = _parse_coordinates(item)
            if lat is not None:
                needs_gp_count += 1
            else:
                no_signal_count += 1

    logger.info(
        f"[FILTER] Area type detection method breakdown for {len(all_pre)} items: "
        f"sync_ppsf/price_band={sync_count} "
        f"needs_google_places={needs_gp_count} "
        f"no_signal_default_middle={no_signal_count}"
    )
    if needs_gp_count > 0:
        logger.info(
            f"[FILTER] Google Places: will call API for up to {needs_gp_count} items "
            f"(concurrency={settings.GOOGLE_PLACES_CONCURRENCY} "
            f"cache may reduce actual calls)"
        )

    t0         = time.monotonic()
    area_types = await asyncio.gather(
        *[detect_area_type(item) for _, item in all_pre],
        return_exceptions=True,
    )
    detect_ms = round((time.monotonic() - t0) * 1000, 1)

    detected_dist: Dict[str, int] = {}
    for det in area_types:
        k = det if isinstance(det, str) else "error"
        detected_dist[k] = detected_dist.get(k, 0) + 1

    logger.info(
        f"[FILTER] Area type detection complete in {detect_ms}ms"
    )
    logger.info(
        f"[FILTER] Detected distribution across {len(all_pre)} items: "
        f"high_street={detected_dist.get('high_street', 0)} "
        f"middle_street={detected_dist.get('middle_street', 0)} "
        f"low_street={detected_dist.get('low_street', 0)} "
        f"errors={detected_dist.get('error', 0)}"
    )
    logger.info(
        f"[FILTER] Items matching requested area_type='{area_type}': "
        f"{detected_dist.get(area_type, 0)} of {len(all_pre)}"
    )

    logger.info(f"[FILTER] ── Stage 4: Area range filter ───────────────────")
    logger.info(
        f"[FILTER] Keeping area_type={area_type!r} "
        f"AND sqft in [{min_area}–{max_area}]"
    )

    filtered:           Dict[str, List[dict]] = {src: [] for src in props_by_src}
    skipped_area_type:  int = 0
    skipped_area_range: int = 0
    soft_pass_no_area:  int = 0
    area_checked:       int = 0

    for (source, item), detected in zip(all_pre, area_types):
        if isinstance(detected, Exception):
            logger.warning(
                f"[FILTER] detect_area_type exception for id={item.get('id')}: "
                f"{detected} — treating as middle_street"
            )
            detected = "middle_street"

        item["_detected_area_type"] = detected

        if detected != area_type:
            skipped_area_type += 1
            continue

        area = _extract_area_sqft(item)
        area_checked += 1

        if area is not None:
            if not (min_area <= area <= max_area):
                skipped_area_range += 1
                logger.debug(
                    f"[FILTER] id={item.get('id')} source={source} "
                    f"rejected: area={area:.0f}sqft not in [{min_area}–{max_area}]"
                )
                continue
        else:
            soft_pass_no_area += 1
            logger.debug(
                f"[FILTER] id={item.get('id')} source={source} "
                f"soft-pass: no numeric area found in any field"
            )

        filtered[source].append(item)

    total_passed = sum(len(v) for v in filtered.values())

    logger.info(
        f"[FILTER] ════════════════════════════════════════════════════════"
    )
    logger.info(f"[FILTER] Pipeline summary")
    logger.info(f"[FILTER]   Stage 0 input:              {total_input}")
    logger.info(f"[FILTER]   Stage 1 after type:         {after_type}  (dropped {total_input - after_type})")
    logger.info(f"[FILTER]   Stage 2 after zone:         {after_zone}  (dropped {after_type - after_zone})")
    logger.info(
        f"[FILTER]   Stage 3 area_type match:    "
        f"{detected_dist.get(area_type, 0)}  "
        f"(dropped {skipped_area_type} wrong area_type)"
    )
    logger.info(
        f"[FILTER]   Stage 4 after area range:   {total_passed}  "
        f"(checked={area_checked} "
        f"rejected_range={skipped_area_range} "
        f"soft_pass_no_area={soft_pass_no_area})"
    )
    logger.info(
        f"[FILTER]   Final: {total_passed}/{total_input} "
        f"({round(total_passed / max(total_input, 1) * 100, 1)}%) "
        f"99acres={len(filtered.get('99acres', []))} "
        f"magicbricks={len(filtered.get('magicbricks', []))}"
    )

    gp_calls = _metrics.get("google_places_calls",      0)
    gp_hits  = _metrics.get("google_places_cache_hits", 0)
    logger.info(
        f"[FILTER] Google Places lifetime totals: "
        f"api_calls={gp_calls} "
        f"cache_hits={gp_hits} "
        f"hit_rate={round(gp_hits / max(gp_calls + gp_hits, 1) * 100, 1)}% "
        f"high={_metrics.get('google_places_high_street', 0)} "
        f"middle={_metrics.get('google_places_middle_street', 0)} "
        f"low={_metrics.get('google_places_low_street', 0)} "
        f"errors={_metrics.get('google_places_errors', 0)}"
    )
    logger.info(
        f"[FILTER] ════════════════════════════════════════════════════════"
    )

    return filtered


def _compute_market_stats(all_items: List[dict]) -> dict:
    prices    = [i["_price_numeric"] for i in all_items if i.get("_price_numeric", 0) > 0]
    areas     = [a for i in all_items for a in [_extract_area_sqft(i)] if a]
    ppsf_vals = [
        i["_price_numeric"] / a
        for i in all_items
        for a in [_extract_area_sqft(i)]
        if a and i.get("_price_numeric", 0) > 0
    ]
    return {
        "avg_price":      round(sum(prices)    / len(prices),    2) if prices    else 0,
        "min_price":      round(min(prices),                     2) if prices    else 0,
        "max_price":      round(max(prices),                     2) if prices    else 0,
        "avg_area":       round(sum(areas)     / len(areas),     2) if areas     else 0,
        "min_area":       round(min(areas),                      2) if areas     else 0,
        "max_area":       round(max(areas),                      2) if areas     else 0,
        "avg_price_sqft": round(sum(ppsf_vals) / len(ppsf_vals), 2) if ppsf_vals else 0,
    }


def _score_feasibility(
    user_min_price: float,
    user_max_price: float,
    user_min_area:  float,
    user_max_area:  float,
    area_type:      str,
    property_zone:  str,
    stats:          dict,
    total_matched:  int,
) -> dict:
    avg_price = stats["avg_price"]
    if avg_price == 0:
        budget_score = 0
    elif user_min_price <= avg_price <= user_max_price:
        budget_score = 25
    else:
        price_range  = max(user_max_price - user_min_price, 1)
        deviation    = min(abs(avg_price - user_min_price), abs(avg_price - user_max_price))
        budget_score = max(5, round(25 - (deviation / price_range) * 20))

    avg_area = stats["avg_area"]
    if avg_area == 0:
        area_score = 0
    elif user_min_area <= avg_area <= user_max_area:
        area_score = 25
    else:
        area_range = max(user_max_area - user_min_area, 1)
        deviation  = min(abs(avg_area - user_min_area), abs(avg_area - user_max_area))
        area_score = max(5, round(25 - (deviation / area_range) * 20))

    if   total_matched >= 10: zone_score = 25
    elif total_matched >= 5:  zone_score = 18
    elif total_matched >= 1:  zone_score = 10
    else:                     zone_score = 0

    if   area_type == "high_street"   and property_zone == "front_shop": location_score = 25
    elif area_type == "high_street":                                      location_score = 20
    elif area_type == "middle_street":                                    location_score = 18
    else:                                                                 location_score = 12

    total = budget_score + area_score + zone_score + location_score

    if   total >= 85: status = "HIGHLY_FEASIBLE"
    elif total >= 65: status = "FEASIBLE"
    elif total >= 40: status = "CAUTION"
    else:             status = "NOT_FEASIBLE"

    verdicts = {
        "HIGHLY_FEASIBLE": "Excellent match — strong market fit within your budget and area",
        "FEASIBLE":        "Good match found within budget and requirements",
        "CAUTION":         "Partial match — consider adjusting budget or area requirements",
        "NOT_FEASIBLE":    "Poor match — market prices or availability don't align",
    }

    logger.info(
        f"[SCORE] budget={budget_score}/25 (avg_market=₹{avg_price:,.0f}) "
        f"area={area_score}/25 (avg_area={avg_area:.0f}sqft) "
        f"zone={zone_score}/25 (matched={total_matched}) "
        f"location={location_score}/25 ({area_type}+{property_zone}) "
        f"total={total}/100 → {status}"
    )

    return {
        "status":  status,
        "score":   total,
        "verdict": verdicts[status],
        "breakdown": {
            "budget_score":   f"{budget_score}/25",
            "area_score":     f"{area_score}/25",
            "zone_score":     f"{zone_score}/25",
            "location_score": f"{location_score}/25",
            "total_score":    f"{total}/100",
        },
    }


def _budget_status(user_min: float, user_max: float, avg: float) -> str:
    if avg == 0:       return "UNKNOWN"
    if avg < user_min: return "UNDER_BUDGET"
    if avg > user_max: return "OVER_BUDGET"
    return "ON_BUDGET"


def _area_status(user_min: float, user_max: float, avg: float) -> str:
    if avg == 0:                    return "UNKNOWN"
    if user_min <= avg <= user_max: return "GOOD_MATCH"
    pct = min(abs(avg-user_min), abs(avg-user_max)) / max(user_max-user_min, 1) * 100
    return "PARTIAL_MATCH" if pct <= 30 else "NO_MATCH"


def _clean_item_for_response(item: dict) -> dict:
    price_numeric = item.get("_price_numeric")
    area_numeric  = _extract_area_sqft(item)
    source        = item.get("_source", "")
    detected      = item.get("_detected_area_type", "middle_street")

    bedrooms_raw  = str(item.get("bedrooms", "") or "").strip().lower()
    is_project    = bedrooms_raw in ("bare bhk", "ready bhk", "shop bhk", "see configurations", "bare", "n/a")

    clean = {k: v for k, v in item.items() if not k.startswith("_")}
    clean["_source"]                  = source
    clean["_computed_price_numeric"]  = price_numeric
    clean["_computed_area_sqft"]      = area_numeric
    clean["_detected_area_type"]      = detected
    clean["_computed_price_per_sqft"] = (
        round(price_numeric / area_numeric, 0)
        if price_numeric and area_numeric and area_numeric > 0
        else None
    )
    clean["_listing_kind"] = (
        "project_level" if (source == "99acres" and is_project)
        else "individual_unit"
    )
    if clean["_listing_kind"] == "project_level":
        clean["_area_note"] = (
            "Project-level listing — individual unit sizes vary. "
            "Check builder page for exact configurations."
        )
    return clean


VALID_PROPERTY_ZONES = {"front_shop", "first_floor", "back_site"}
VALID_AREA_TYPES     = {"high_street", "middle_street", "low_street"}
VALID_PROPERTY_TYPES = set(PROPERTY_TYPE_KEYWORDS.keys())


class RangeField(BaseModel):
    min_range: float = Field(..., ge=0)
    max_range: float = Field(..., ge=0)

    @model_validator(mode="after")
    def max_gte_min(self) -> "RangeField":
        if self.max_range < self.min_range:
            raise ValueError("max_range must be >= min_range")
        return self


class BusinessRentInput(BaseModel):
    city:          str        = Field(..., min_length=2, max_length=60)
    property_zone: str        = Field(...)
    area_type:     str        = Field(...)
    property_area: RangeField
    monthly_rent:  RangeField
    property_type: str        = Field("all")

    @field_validator("city")
    @classmethod
    def validate_city(cls, v: str) -> str:
        v = v.strip()
        if not re.match(r"^[a-zA-Z\s\-]+$", v):
            raise ValueError("City must contain only letters, spaces, or hyphens")
        return normalize_city_input(v)

    @field_validator("property_zone")
    @classmethod
    def validate_zone(cls, v: str) -> str:
        if v not in VALID_PROPERTY_ZONES:
            raise ValueError(f"Must be one of {sorted(VALID_PROPERTY_ZONES)}")
        return v

    @field_validator("area_type")
    @classmethod
    def validate_area_type(cls, v: str) -> str:
        if v not in VALID_AREA_TYPES:
            raise ValueError(f"Must be one of {sorted(VALID_AREA_TYPES)}")
        return v

    @field_validator("property_type")
    @classmethod
    def validate_property_type(cls, v: str) -> str:
        n = v.strip().lower().replace(" ", "_").replace("-", "_")
        if n not in VALID_PROPERTY_TYPES:
            raise ValueError(f"Invalid property_type '{v}'. Valid: {sorted(VALID_PROPERTY_TYPES)}")
        return n


class BusinessBuyInput(BaseModel):
    city:              str        = Field(..., min_length=2, max_length=60)
    property_zone:     str        = Field(...)
    area_type:         str        = Field(...)
    property_area:     RangeField
    investment_budget: RangeField
    property_type:     str        = Field("all")

    @field_validator("city")
    @classmethod
    def validate_city(cls, v: str) -> str:
        v = v.strip()
        if not re.match(r"^[a-zA-Z\s\-]+$", v):
            raise ValueError("City must contain only letters, spaces, or hyphens")
        return normalize_city_input(v)

    @field_validator("property_zone")
    @classmethod
    def validate_zone(cls, v: str) -> str:
        if v not in VALID_PROPERTY_ZONES:
            raise ValueError(f"Must be one of {sorted(VALID_PROPERTY_ZONES)}")
        return v

    @field_validator("area_type")
    @classmethod
    def validate_area_type(cls, v: str) -> str:
        if v not in VALID_AREA_TYPES:
            raise ValueError(f"Must be one of {sorted(VALID_AREA_TYPES)}")
        return v

    @field_validator("property_type")
    @classmethod
    def validate_property_type(cls, v: str) -> str:
        n = v.strip().lower().replace(" ", "_").replace("-", "_")
        if n not in VALID_PROPERTY_TYPES:
            raise ValueError(f"Invalid property_type '{v}'. Valid: {sorted(VALID_PROPERTY_TYPES)}")
        return n


async def build_feasibility_report(
    city:          str,
    property_zone: str,
    area_type:     str,
    min_area:      float,
    max_area:      float,
    min_price:     float,
    max_price:     float,
    property_type: str,
    listing_type:  str,
    page:          int,
    page_size:     int,
) -> dict:
    t_start = time.monotonic()

    logger.info(
        f"[REPORT] ╔══════════════════════════════════════════════════════╗"
    )
    logger.info(
        f"[REPORT] ║  {listing_type} feasibility report starting{' '*(19-len(listing_type))}║"
    )
    logger.info(
        f"[REPORT] ╚══════════════════════════════════════════════════════╝"
    )
    logger.info(
        f"[REPORT] city={city!r} type={property_type} zone={property_zone} "
        f"area_type={area_type} "
        f"price=[₹{min_price:,.0f}–₹{max_price:,.0f}] "
        f"area=[{min_area}–{max_area}sqft] "
        f"page={page} page_size={page_size}"
    )

    raw_props      = await fetch_all_commercial(city, min_price, max_price, listing_type)
    filtered_props = await _apply_filters(
        raw_props, property_type, property_zone, area_type, min_area, max_area
    )

    all_filtered  = (
        filtered_props.get("99acres",     []) +
        filtered_props.get("magicbricks", [])
    )
    total_matched = len(all_filtered)

    logger.info(
        f"[REPORT] Filter complete: total_matched={total_matched} "
        f"(99acres={len(filtered_props.get('99acres',[]))} "
        f"magicbricks={len(filtered_props.get('magicbricks',[]))})"
    )

    if total_matched == 0:
        logger.warning(
            f"[REPORT] NO_DATA — zero properties passed all filters for city={city!r}. "
            f"Suggestions: broaden price range ₹{min_price:,.0f}–₹{max_price:,.0f}, "
            f"area {min_area}–{max_area}sqft, or try zone='back_site' / area_type='middle_street'"
        )
        return {
            "listing_type": listing_type,
            "feasibility_summary": {
                "status":  "NO_DATA",
                "score":   0,
                "verdict": "No matching properties found — try broadening your filters",
            },
            "budget_analysis":        {},
            "area_analysis":          {},
            "location_analysis":      {"city": city, "area_type": area_type, "property_zone": property_zone},
            "property_type_analysis": {"requested_type": property_type, "total_available": 0},
            "score_breakdown":        {},
            "properties":             {
                "total": 0, "page": page, "page_size": page_size,
                "total_pages": 0, "items": {"99acres": [], "magicbricks": []},
            },
        }

    stats       = _compute_market_stats(all_filtered)
    feasibility = _score_feasibility(
        min_price, max_price, min_area, max_area,
        area_type, property_zone, stats, total_matched,
    )
    b_status = _budget_status(min_price, max_price, stats["avg_price"])
    a_status = _area_status(min_area, max_area, stats["avg_area"])

    logger.info(
        f"[REPORT] Market stats: "
        f"avg_price=₹{stats['avg_price']:,.0f} [{b_status}] "
        f"min=₹{stats['min_price']:,.0f} max=₹{stats['max_price']:,.0f} "
        f"avg_area={stats['avg_area']:.0f}sqft [{a_status}] "
        f"avg_ppsf=₹{stats['avg_price_sqft']:,.0f}"
    )

    budget_analysis = (
        {
            "min_rent_given":      min_price,
            "max_rent_given":      max_price,
            "avg_market_rent":     stats["avg_price"],
            "cheapest_available":  stats["min_price"],
            "costliest_available": stats["max_price"],
            "budget_status":       b_status,
            "properties_in_range": total_matched,
        }
        if listing_type == "commercial_rent"
        else {
            "min_budget_given":    min_price,
            "max_budget_given":    max_price,
            "avg_market_price":    stats["avg_price"],
            "cheapest_available":  stats["min_price"],
            "costliest_available": stats["max_price"],
            "budget_status":       b_status,
            "properties_in_range": total_matched,
        }
    )

    area_analysis = {
        "min_area_given":      min_area,
        "max_area_given":      max_area,
        "avg_area_available":  stats["avg_area"],
        "smallest_available":  stats["min_area"],
        "largest_available":   stats["max_area"],
        "area_status":         a_status,
        "properties_in_range": total_matched,
    }

    _insight_map: Dict[tuple, str] = {
        ("high_street",   "front_shop"):  f"High street {city} front shops — premium footfall, highest cost",
        ("high_street",   "first_floor"): f"First floor on high street {city} — good visibility at lower cost",
        ("high_street",   "back_site"):   f"Back site in high street {city} — offices and service businesses",
        ("middle_street", "front_shop"):  f"Middle street {city} front shops — balanced cost and footfall",
        ("middle_street", "first_floor"): f"Middle street first floor {city} — cost-effective for professionals",
        ("middle_street", "back_site"):   f"Middle street back site {city} — offices and light industrial",
        ("low_street",    "front_shop"):  f"Low street {city} — budget-friendly, limited footfall",
        ("low_street",    "first_floor"): f"Low street first floor {city} — storage or back-office use",
        ("low_street",    "back_site"):   f"Low street back site {city} — ideal for warehouse, factory, MIDC",
    }

    location_analysis = {
        "city":               city,
        "area_type":          area_type,
        "property_zone":      property_zone,
        "market_insight":     _insight_map.get(
            (area_type, property_zone),
            f"{area_type.replace('_', ' ').title()} in {city}",
        ),
        "price_per_sqft_avg": stats["avg_price_sqft"],
    }

    property_type_analysis = {
        "requested_type":  property_type,
        "total_available": total_matched,
        "by_source": {
            "99acres":     len(filtered_props.get("99acres",     [])),
            "magicbricks": len(filtered_props.get("magicbricks", [])),
        },
    }

    all_sorted  = sorted(all_filtered, key=lambda x: x.get("_price_numeric", float("inf")))
    total_pages = math.ceil(total_matched / page_size)

    if page > total_pages:
        logger.warning(
            f"[REPORT] page={page} exceeds total_pages={total_pages} "
            f"for total_matched={total_matched} — 404"
        )
        raise HTTPException(
            status_code=404,
            detail=f"Page {page} exceeds total pages {total_pages}",
        )

    start      = (page - 1) * page_size
    page_items = all_sorted[start: start + page_size]

    logger.info(
        f"[REPORT] Pagination: total={total_matched} pages={total_pages} "
        f"current_page={page} page_size={page_size} "
        f"items_on_page={len(page_items)} "
        f"[{start+1}–{start+len(page_items)}] sorted_by=price_asc"
    )

    items_99: List[dict] = []
    items_mb: List[dict] = []
    for item in page_items:
        src = item.get("_source", "")
        c   = _clean_item_for_response(item)
        (items_99 if src == "99acres" else items_mb).append(c)

    gp_calls = _metrics.get("google_places_calls",      0)
    gp_hits  = _metrics.get("google_places_cache_hits", 0)
    gp_diag  = {
        "api_key_configured":        bool(settings.GOOGLE_PLACES_API_KEY),
        "concurrency_limit":         settings.GOOGLE_PLACES_CONCURRENCY,
        "total_api_calls_lifetime":  gp_calls,
        "total_cache_hits_lifetime": gp_hits,
        "cache_hit_rate_pct":        round(gp_hits / max(gp_calls + gp_hits, 1) * 100, 1),
        "total_errors":              _metrics.get("google_places_errors", 0),
        "area_type_results": {
            "high_street":   _metrics.get("google_places_high_street",   0),
            "middle_street": _metrics.get("google_places_middle_street", 0),
            "low_street":    _metrics.get("google_places_low_street",    0),
        },
    }

    total_ms = round((time.monotonic() - t_start) * 1000, 1)
    logger.info(
        f"[REPORT] ╔══════════════════════════════════════════════════════╗"
    )
    logger.info(
        f"[REPORT] ║  Complete in {total_ms}ms{' '*(40-len(str(total_ms)))}║"
    )
    logger.info(
        f"[REPORT] ╚══════════════════════════════════════════════════════╝"
    )
    logger.info(
        f"[REPORT] status={feasibility['status']} score={feasibility['score']}/100 "
        f"matched={total_matched} page_items={len(page_items)} "
        f"gp_api_calls={gp_calls} gp_cache_hits={gp_hits} "
        f"gp_hit_rate={gp_diag['cache_hit_rate_pct']}%"
    )

    return {
        "listing_type":              listing_type,
        "feasibility_summary":       {
            "status":  feasibility["status"],
            "score":   feasibility["score"],
            "verdict": feasibility["verdict"],
        },
        "budget_analysis":           budget_analysis,
        "area_analysis":             area_analysis,
        "location_analysis":         location_analysis,
        "property_type_analysis":    property_type_analysis,
        "score_breakdown":           feasibility["breakdown"],
        "google_places_diagnostics": gp_diag,
        "properties": {
            "total":       total_matched,
            "page":        page,
            "page_size":   page_size,
            "total_pages": total_pages,
            "items": {
                "99acres":     items_99,
                "magicbricks": items_mb,
            },
        },
    }


rent_router = APIRouter(
    prefix="/businessman/rent",
    tags=["Businessman Feasibility — Rent"],
)
buy_router = APIRouter(
    prefix="/businessman/buy",
    tags=["Businessman Feasibility — Buy"],
)


@rent_router.post("/feasibility")
async def businessman_rent_feasibility(
    request:   Request,
    data:      BusinessRentInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    inc("requests_total")
    ip = _extract_client_ip(request)

    if await RedisRateLimiter.is_rate_limited(ip, prefix="biz_rent"):
        return JSONResponse(
            status_code=429,
            content={"error": "Rate limit exceeded. Try again in a minute."},
        )

    logger.info(
        f"[BIZ_RENT] ► Request #{_metrics['requests_total']} "
        f"ip={ip} city={data.city} zone={data.property_zone} "
        f"area_type={data.area_type} type={data.property_type} "
        f"rent=[₹{data.monthly_rent.min_range:,.0f}–₹{data.monthly_rent.max_range:,.0f}] "
        f"area=[{data.property_area.min_range}–{data.property_area.max_range}sqft]"
    )
    try:
        result = await build_feasibility_report(
            city          = data.city,
            property_zone = data.property_zone,
            area_type     = data.area_type,
            min_area      = data.property_area.min_range,
            max_area      = data.property_area.max_range,
            min_price     = data.monthly_rent.min_range,
            max_price     = data.monthly_rent.max_range,
            property_type = data.property_type,
            listing_type  = "commercial_rent",
            page          = page,
            page_size     = page_size,
        )
        inc("requests_ok")
        logger.info(
            f"[BIZ_RENT] ✓ Request #{_metrics['requests_total']} done "
            f"ok={_metrics['requests_ok']} errors={_metrics['requests_error']}"
        )
        return result
    except Exception as e:
        inc("requests_error")
        logger.error(
            f"[BIZ_RENT] ✗ Request #{_metrics['requests_total']} unhandled: {e}",
            exc_info=True,
        )
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@buy_router.post("/feasibility")
async def businessman_buy_feasibility(
    request:   Request,
    data:      BusinessBuyInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    inc("requests_total")
    ip = _extract_client_ip(request)

    if await RedisRateLimiter.is_rate_limited(ip, prefix="biz_buy"):
        return JSONResponse(
            status_code=429,
            content={"error": "Rate limit exceeded. Try again in a minute."},
        )

    logger.info(
        f"[BIZ_BUY] ► Request #{_metrics['requests_total']} "
        f"ip={ip} city={data.city} zone={data.property_zone} "
        f"area_type={data.area_type} type={data.property_type} "
        f"budget=[₹{data.investment_budget.min_range:,.0f}–₹{data.investment_budget.max_range:,.0f}] "
        f"area=[{data.property_area.min_range}–{data.property_area.max_range}sqft]"
    )
    try:
        result = await build_feasibility_report(
            city          = data.city,
            property_zone = data.property_zone,
            area_type     = data.area_type,
            min_area      = data.property_area.min_range,
            max_area      = data.property_area.max_range,
            min_price     = data.investment_budget.min_range,
            max_price     = data.investment_budget.max_range,
            property_type = data.property_type,
            listing_type  = "commercial_buy",
            page          = page,
            page_size     = page_size,
        )
        inc("requests_ok")
        logger.info(
            f"[BIZ_BUY] ✓ Request #{_metrics['requests_total']} done "
            f"ok={_metrics['requests_ok']} errors={_metrics['requests_error']}"
        )
        return result
    except Exception as e:
        inc("requests_error")
        logger.error(
            f"[BIZ_BUY] ✗ Request #{_metrics['requests_total']} unhandled: {e}",
            exc_info=True,
        )
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


async def _health_check() -> JSONResponse:
    redis_ok = db_ok = False
    try:
        await redis_client.ping()
        redis_ok = True
    except Exception as e:
        logger.warning(f"[HEALTH] Redis ping failed: {e}")
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        db_ok = True
    except Exception as e:
        logger.warning(f"[HEALTH] DB check failed: {e}")

    overall = "healthy" if (redis_ok and db_ok) else "degraded"
    logger.info(
        f"[HEALTH] status={overall} redis={'ok' if redis_ok else 'FAIL'} "
        f"db={'ok' if db_ok else 'FAIL'} "
        f"gp_calls={_metrics.get('google_places_calls', 0)} "
        f"gp_cache_hits={_metrics.get('google_places_cache_hits', 0)}"
    )
    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={
            "status":   overall,
            "redis":    "connected"   if redis_ok else "disconnected",
            "database": "connected"   if db_ok    else "disconnected",
            "google_places": {
                "configured":          bool(settings.GOOGLE_PLACES_API_KEY),
                "concurrency_limit":   settings.GOOGLE_PLACES_CONCURRENCY,
                "api_calls":           _metrics.get("google_places_calls",        0),
                "cache_hits":          _metrics.get("google_places_cache_hits",   0),
                "errors":              _metrics.get("google_places_errors",       0),
                "high_street":         _metrics.get("google_places_high_street",  0),
                "middle_street":       _metrics.get("google_places_middle_street",0),
                "low_street":          _metrics.get("google_places_low_street",   0),
            },
        },
    )


@rent_router.get("/health/detailed")
async def rent_health():
    return await _health_check()

@buy_router.get("/health/detailed")
async def buy_health():
    return await _health_check()


async def _metrics_payload() -> dict:
    try:
        global_raw = await redis_client.hgetall("biz:metrics:global")
        global_metrics = {k: int(v) for k, v in global_raw.items()} if global_raw else {}
    except Exception:
        global_metrics = {}

    merged = {k: global_metrics.get(k, v) for k, v in _metrics.items()}

    calls = merged.get("google_places_calls",      0)
    hits  = merged.get("google_places_cache_hits", 0)
    errs  = merged.get("google_places_errors",     0)
    return {
        "metrics":        merged,
        "metrics_source": "redis_global" if global_metrics else "in_process_fallback",
        "timestamp":      time.time(),
        "google_places_summary": {
            "api_key_configured":  bool(settings.GOOGLE_PLACES_API_KEY),
            "concurrency_limit":   settings.GOOGLE_PLACES_CONCURRENCY,
            "total_calls":         calls,
            "total_cache_hits":    hits,
            "cache_hit_rate_pct":  round(hits / max(calls + hits, 1) * 100, 1),
            "error_rate_pct":      round(errs / max(calls,         1) * 100, 1),
            "area_type_results": {
                "high_street":   merged.get("google_places_high_street",   0),
                "middle_street": merged.get("google_places_middle_street", 0),
                "low_street":    merged.get("google_places_low_street",    0),
            },
        },
        "db_summary": {
            "99acres_fetched":          merged.get("99acres_rows_fetched",        0),
            "magicbricks_fetched":      merged.get("magicbricks_rows_fetched",    0),
            "99acres_price_passed":     merged.get("99acres_price_filtered",      0),
            "magicbricks_price_passed": merged.get("magicbricks_price_filtered",  0),
        },
    }

@rent_router.get("/metrics")
async def rent_metrics():
    return await _metrics_payload()

@buy_router.get("/metrics")
async def buy_metrics():
    return await _metrics_payload()


@rent_router.get("/clear-cache")
async def rent_clear_cache():
    deleted = 0
    for prefix in ("biz:commercial_rent:", "biz_rent:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys:
            deleted += await RedisCache.delete(*keys)
    logger.info(f"[CACHE] Rent cache cleared: {deleted} keys")
    return {"status": "cache cleared", "keys_deleted": deleted}

@buy_router.get("/clear-cache")
async def buy_clear_cache():
    deleted = 0
    for prefix in ("biz:commercial_buy:", "biz_buy:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys:
            deleted += await RedisCache.delete(*keys)
    logger.info(f"[CACHE] Buy cache cleared: {deleted} keys")
    return {"status": "cache cleared", "keys_deleted": deleted}

@rent_router.get("/clear-google-cache")
@buy_router.get("/clear-google-cache")
async def clear_google_cache():
    keys    = await RedisCache.keys_by_prefix("gp:area:")
    deleted = await RedisCache.delete(*keys) if keys else 0
    logger.info(f"[CACHE] Google Places cache cleared: {deleted} coordinate entries")
    return {"status": "google places cache cleared", "keys_deleted": deleted}


@buy_router.get("/db-diagnostic")
@rent_router.get("/db-diagnostic")
async def db_diagnostic():
    results: Dict[str, Any] = {}

    rows = await _db_fetch("""
        SELECT source, listing_type, COUNT(*) as cnt
        FROM property_raw_data
        GROUP BY source, listing_type
        ORDER BY source, listing_type
    """)
    results["listing_types_by_source"] = [
        {"source": r["source"], "listing_type": r["listing_type"], "count": r["cnt"]}
        for r in rows
    ]

    rows = await _db_fetch("""
        SELECT scraped_data->>'city_name' as city, COUNT(*) as cnt
        FROM property_raw_data
        WHERE source = 'magicbricks'
        GROUP BY city
        ORDER BY cnt DESC
        LIMIT 30
    """)
    results["magicbricks_cities"] = [
        {"city": r["city"], "count": r["cnt"]} for r in rows
    ]

    rows = await _db_fetch("""
        SELECT
            regexp_replace(
                scraped_data->>'pageUrl',
                '.*keyword=([^&]+).*', '\\1'
            ) as city,
            COUNT(*) as cnt
        FROM property_raw_data
        WHERE source = '99acres'
          AND scraped_data->>'pageUrl' ILIKE '%keyword=%'
        GROUP BY city
        ORDER BY cnt DESC
        LIMIT 30
    """)
    results["99acres_cities"] = [
        {"city": r["city"], "count": r["cnt"]} for r in rows
    ]

    rows = await _db_fetch("""
        SELECT source, COUNT(*) as total
        FROM property_raw_data
        GROUP BY source
        ORDER BY total DESC
    """)
    results["total_rows_per_source"] = [
        {"source": r["source"], "total": r["total"]} for r in rows
    ]

    logger.info(f"[DIAGNOSTIC] {json.dumps(results, default=str)[:600]}")
    return results
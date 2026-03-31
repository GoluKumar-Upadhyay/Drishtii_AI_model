import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Optional

import httpx
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

load_dotenv()



logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("astrology_api")




class Settings:
    VEDIC_API_KEY:          str   = os.getenv("VEDIC_API_KEY", "")
    VEDIC_BASE_URL:         str   = os.getenv("VEDIC_BASE_URL", "https://api.vedicastroapi.com/v3-json")
   
    REDIS_URL:              str   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS:  int   = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
    CACHE_TTL:              int   = int(os.getenv("ASTRO_CACHE_TTL",       "3600"))
    CACHE_EMPTY_TTL:        int   = int(os.getenv("ASTRO_CACHE_EMPTY_TTL", "60"))
    RATE_LIMIT_PER_MIN:     int   = int(os.getenv("ASTRO_RATE_LIMIT",      "30"))
    HTTP_TIMEOUT:           int   = int(os.getenv("ASTRO_HTTP_TIMEOUT",    "15"))
    HTTP_MAX_RETRIES:       int   = int(os.getenv("ASTRO_HTTP_RETRIES",    "3"))

settings = Settings()






_metrics: dict[str, int] = {
    "requests_total":   0,
    "requests_ok":      0,
    "requests_error":   0,
    "cache_hits":       0,
    "cache_misses":     0,
    "rate_limited":     0,
    "vedic_api_calls":  0,
    "vedic_api_errors": 0,
}

def inc(key: str, n: int = 1):
    _metrics[key] = _metrics.get(key, 0) + n






_redis_client: redis.Redis = None

def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=True,
        )
    return _redis_client


class RedisCache:
    @staticmethod
    async def get(key: str) -> Optional[Any]:
        try:
            data = await get_redis().get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.warning(f"Redis GET failed [{key}]: {e}")
            return None

    @staticmethod
    async def set(key: str, value: Any, ttl: int = settings.CACHE_TTL) -> bool:
        try:
            await get_redis().setex(key, ttl, json.dumps(value, default=str))
            return True
        except Exception as e:
            logger.warning(f"Redis SET failed [{key}]: {e}")
            return False

    @staticmethod
    async def delete(*keys: str) -> int:
        try:
            return await get_redis().delete(*keys) if keys else 0
        except Exception as e:
            logger.warning(f"Redis DELETE failed: {e}")
            return 0

    @staticmethod
    async def keys_by_prefix(prefix: str) -> list[str]:
        try:
            return await get_redis().keys(f"{prefix}*")
        except Exception as e:
            logger.warning(f"Redis KEYS failed [{prefix}]: {e}")
            return []






class RedisRateLimiter:
    @staticmethod
    async def is_rate_limited(ip: str) -> bool:
        try:
            key  = f"astro:rl:{ip}"
            now  = time.time()
            pipe = get_redis().pipeline(transaction=True)
            pipe.zremrangebyscore(key, 0, now - 60)
            pipe.zadd(key, {f"{now:.6f}": now})
            pipe.zcard(key)
            pipe.expire(key, 60)
            results = await pipe.execute()
            if results[2] > settings.RATE_LIMIT_PER_MIN:
                inc("rate_limited")
                return True
            return False
        except Exception as e:
            logger.warning(f"Rate limiter error (fail-open): {e}")
            return False




def build_cache_key(endpoint: str, params: dict) -> str:
    safe = {k: v for k, v in params.items() if k != "api_key"}
    raw  = f"{endpoint}:{json.dumps(safe, sort_keys=True)}"
    return f"astro:v1:{hashlib.md5(raw.encode()).hexdigest()}"






async def call_vedic_api(endpoint: str, params: dict) -> dict:
    
    if not settings.VEDIC_API_KEY:
        logger.error("VEDIC_API_KEY not set in .env!")
        raise HTTPException(status_code=500, detail="API key not configured on server")

   
    params["api_key"] = settings.VEDIC_API_KEY

    
    cache_key = build_cache_key(endpoint, params)
    cached    = await RedisCache.get(cache_key)
    if cached is not None:
        inc("cache_hits")
        logger.info(f"Cache HIT [{endpoint}] key=[{cache_key[-8:]}]")
        return cached

    inc("cache_misses")
    url       = f"{settings.VEDIC_BASE_URL}/{endpoint}"
    last_err  = None

    for attempt in range(settings.HTTP_MAX_RETRIES):
        try:
            inc("vedic_api_calls")
            async with httpx.AsyncClient(timeout=settings.HTTP_TIMEOUT) as client:
                response = await client.get(url, params=params)

            if response.status_code == 429:
                wait = (2 ** attempt) + 0.5
                logger.warning(f"VedicAPI rate limited [{endpoint}], retry in {wait}s")
                await asyncio.sleep(wait)
                continue

            if response.status_code != 200:
                inc("vedic_api_errors")
                logger.error(f"VedicAPI HTTP error [{endpoint}]: {response.status_code}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"VedicAstroAPI returned HTTP {response.status_code}"
                )

            data = response.json()
            if data.get("status") != 200:
                inc("vedic_api_errors")
                logger.error(f"VedicAPI error [{endpoint}]: status={data.get('status')} msg={data.get('response')}")
                raise HTTPException(
                    status_code=data.get("status", 400),
                    detail=data.get("response", "Bad request to VedicAstroAPI")
                )
            await RedisCache.set(cache_key, data, ttl=settings.CACHE_TTL)
            logger.info(f"VedicAPI success [{endpoint}]")
            return data

        except HTTPException:
            raise
        except Exception as e:
            last_err = e
            if attempt < settings.HTTP_MAX_RETRIES - 1:
                wait = (2 ** attempt) + 0.3
                logger.warning(f"VedicAPI attempt {attempt+1} failed [{endpoint}], retry {wait}s: {e}")
                await asyncio.sleep(wait)

    inc("vedic_api_errors")
    logger.error(f"VedicAPI exhausted retries [{endpoint}]: {last_err}")
    raise HTTPException(status_code=503, detail="VedicAstroAPI unavailable, try again later")






class KundliParams(BaseModel):
    """Group 1 — Standard Kundli (used by ~40 APIs)"""
    dob:  str   = Field(..., example="15/8/1990")
    tob:  str   = Field(..., example="10:30")
    lat:  float = Field(..., example=26.85)
    lon:  float = Field(..., example=75.79)
    tz:   float = Field(..., example=5.5)
    lang: str   = Field("en", example="en")

    @validator("dob")
    def validate_dob(cls, v):
        import re
        if not re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", v):
            raise ValueError("dob must be DD/MM/YYYY e.g. 15/8/1990")
        return v

    @validator("tob")
    def validate_tob(cls, v):
        import re
        if not re.match(r"^\d{1,2}:\d{2}$", v):
            raise ValueError("tob must be HH:MM e.g. 10:30")
        return v


class KundliWithDateParams(KundliParams):
    """Group 2 — Kundli + Date (daily/weekly/monthly predictions)"""
    date: str = Field(..., example="17/3/2026")


class KundliWithZodiacParams(KundliWithDateParams):
    """Group 3 — Kundli + Date + Zodiac (daily sun/moon)"""
    zodiac: int = Field(..., ge=1, le=12, example=1,
                        description="1=Aries 2=Taurus ... 12=Pisces")


class PanchangParams(BaseModel):
    """Group 4 — Panchang (no dob/tob needed)"""
    date: str   = Field(..., example="17/3/2026")
    lat:  float = Field(..., example=26.85)
    lon:  float = Field(..., example=75.79)
    tz:   float = Field(..., example=5.5)
    lang: str   = Field("en", example="en")


class PanchangMonthlyParams(BaseModel):
    """Group 5 — Panchang Monthly/Yearly"""
    month: int   = Field(..., ge=1, le=12, example=3)
    year:  int   = Field(..., ge=2000,     example=2026)
    lat:   float = Field(...,              example=26.85)
    lon:   float = Field(...,              example=75.79)
    tz:    float = Field(...,              example=5.5)
    lang:  str   = Field("en",             example="en")


class MatchingParams(BaseModel):
    """Group 6 — Matching (Two people)"""
    boy_dob: str   = Field(..., example="15/8/1990")
    boy_tob: str   = Field(..., example="10:30")
    boy_lat: float = Field(..., example=26.85)
    boy_lon: float = Field(..., example=75.79)
    boy_tz:  float = Field(..., example=5.5)
    girl_dob: str  = Field(..., example="20/5/1992")
    girl_tob: str  = Field(..., example="08:00")
    girl_lat: float = Field(..., example=28.61)
    girl_lon: float = Field(..., example=77.20)
    girl_tz:  float = Field(..., example=5.5)
    lang:     str   = Field("en", example="en")


class MatchingAstroParams(MatchingParams):
    """Group 7 — Matching + astro details flag"""
    astro_details: bool = Field(True)


class BulkMatchingParams(BaseModel):
    """Group 8 — Bulk Matching"""
    lang:     str        = Field("en", example="en")
    profiles: list[dict] = Field(...,  description="List of {dob,tob,lat,lon,tz} objects")


class DivisionalChartParams(KundliParams):
    """Group 9 — Divisional Charts"""
    div: str = Field(..., example="D9",
                     description="D1 D2 D3 D4 D7 D9 D10 D12 D16 D20 D24 D27 D30 D40 D45 D60")


class ChartImageParams(KundliParams):
    """Group 10 — Chart Image"""
    div:   int = Field(1,       example=1)
    style: str = Field("north", example="north",
                        description="north / south / east")
    color: str = Field("",      example="#FF0000")


class VarshapalParams(KundliParams):
    """Group 11 — Varshapal"""
    year: int = Field(..., ge=2000, example=2026)


class SubDashaParams(KundliParams):
    """Group 12 — Specific Sub Dasha"""
    planet_name: str = Field(..., example="Jupiter")


class YearlyParams(KundliParams):
    """Group 14 — Yearly Prediction"""
    year: int = Field(..., ge=2000, example=2026)






prediction_router = APIRouter(prefix="/api/prediction",    tags=["Prediction"])
horoscope_router  = APIRouter(prefix="/api/horoscope",     tags=["Horoscope"])
matching_router   = APIRouter(prefix="/api/matching",      tags=["Matching"])
panchang_router   = APIRouter(prefix="/api/panchang",      tags=["Panchang"])
dosha_router      = APIRouter(prefix="/api/dosha",         tags=["Dosha"])
dashas_router     = APIRouter(prefix="/api/dashas",        tags=["Dashas"])
extended_router   = APIRouter(prefix="/api/extended",      tags=["Extended Horoscope"])
utilities_router  = APIRouter(prefix="/api/utilities",     tags=["Utilities"])
health_router     = APIRouter(prefix="/api/astro",         tags=["Health"])






@prediction_router.post("/biorhythm")
async def biorhythm(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/biorhythm", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/daily-moon")
async def daily_moon(data: KundliWithZodiacParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/daily-moon", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/daily-nakshatra")
async def daily_nakshatra(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/daily-nakshatra", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/daily-sun")
async def daily_sun(data: KundliWithZodiacParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/daily-sun", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/day-number")
async def day_number(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/day-number", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/monthly")
async def monthly_prediction(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/monthly", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/numerology")
async def numerology(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/numerology", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/weekly-moon")
async def weekly_moon(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/weekly-moon", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/weekly-sun")
async def weekly_sun(data: KundliWithDateParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/weekly-sun", data.dict())
    inc("requests_ok"); return result

@prediction_router.post("/yearly")
async def yearly_prediction(data: YearlyParams):
    inc("requests_total")
    result = await call_vedic_api("prediction/yearly", data.dict())
    inc("requests_ok"); return result



#  HOROSCOPE  (13 endpoints)


@horoscope_router.post("/planet-details")
async def planet_details(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/planet-details", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/planet-report")
async def planet_report(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/planet-report", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/aspects")
async def aspects(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/aspects", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/planets-by-houses")
async def planets_by_houses(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/planets-by-houses", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/divisional-charts")
async def divisional_charts(data: DivisionalChartParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/divisional-charts", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/personal-characteristics")
async def personal_characteristics(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/personal-characteristics", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/12-month-prediction")
async def twelve_month_prediction(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/12-month-prediction", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/ascendant-report")
async def ascendant_report(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/ascendant-report", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/ashtakvarga")
async def ashtakvarga(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/ashtakvarga", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/binnashtakvarga")
async def binnashtakvarga(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/binnashtakvarga", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/chart-image")
async def chart_image(data: ChartImageParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/chart-image", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/ashtakvarga-chart-image")
async def ashtakvarga_chart_image(data: ChartImageParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/ashtakvarga-chart-image", data.dict())
    inc("requests_ok"); return result

@horoscope_router.post("/western-planets")
async def western_planets(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("horoscope/western-planets", data.dict())
    inc("requests_ok"); return result



#  MATCHING  (16 endpoints)


@matching_router.post("/aggregate-match")
async def aggregate_match(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/aggregate-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/ashtakoot")
async def ashtakoot(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/ashtakoot", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/ashtakoot-astro-details")
async def ashtakoot_astro_details(data: MatchingAstroParams):
    inc("requests_total")
    result = await call_vedic_api("matching/ashtakoot-astro-details", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/bulk-nakshatra-match")
async def bulk_nakshatra_match(data: BulkMatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/bulk-nakshatra-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/bulk-ashtakoot")
async def bulk_ashtakoot(data: BulkMatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/bulk-ashtakoot", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/bulk-dashakoot")
async def bulk_dashakoot(data: BulkMatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/bulk-dashakoot", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/bulk-western-match")
async def bulk_western_match(data: BulkMatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/bulk-western-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/dashakoot")
async def dashakoot(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/dashakoot", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/dashakoot-astro-details")
async def dashakoot_astro_details(data: MatchingAstroParams):
    inc("requests_total")
    result = await call_vedic_api("matching/dashakoot-astro-details", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/south-match")
async def south_match(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/south-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/nakshatra-match")
async def nakshatra_match(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/nakshatra-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/papasamaya")
async def papasamaya(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/papasamaya", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/papasamaya-match")
async def papasamaya_match(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/papasamaya-match", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/quick-matcher")
async def quick_matcher(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/quick-matcher", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/rajju-vedha")
async def rajju_vedha(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/rajju-vedha-details", data.dict())
    inc("requests_ok"); return result

@matching_router.post("/western-match")
async def western_match(data: MatchingParams):
    inc("requests_total")
    result = await call_vedic_api("matching/western-match", data.dict())
    inc("requests_ok"); return result



#  PANCHANG  (14 endpoints)


@panchang_router.post("/panchang")
async def panchang(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/panchang", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/choghadiya-muhurta")
async def choghadiya_muhurta(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/choghadiya-muhurta", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/hora-muhurta")
async def hora_muhurta(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/hora-muhurta", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/monthly-panchang")
async def monthly_panchang(data: PanchangMonthlyParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/monthly-panchang", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/moon-calendar")
async def moon_calendar(data: PanchangMonthlyParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/moon-calendar", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/moon-phase")
async def moon_phase(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/moon-phase", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/moonrise")
async def moonrise(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/moonrise", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/moonset")
async def moonset(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/moonset", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/retrogrades")
async def retrogrades(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/retrogrades", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/solarnoon")
async def solarnoon(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/solarnoon", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/sunrise")
async def sunrise(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/sunrise", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/sunset")
async def sunset(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/sunset", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/transit")
async def transit(data: PanchangParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/transit", data.dict())
    inc("requests_ok"); return result

@panchang_router.post("/festivals")
async def festivals(data: PanchangMonthlyParams):
    inc("requests_total")
    result = await call_vedic_api("panchang/festivals", data.dict())
    inc("requests_ok"); return result



#  DOSHA  (4 endpoints)


@dosha_router.post("/kaalsarp-dosh")
async def kaalsarp_dosh(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dosha/kaalsarp-dosh", data.dict())
    inc("requests_ok"); return result

@dosha_router.post("/mangal-dosh")
async def mangal_dosh(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dosha/mangal-dosh", data.dict())
    inc("requests_ok"); return result

@dosha_router.post("/manglik-dosh")
async def manglik_dosh(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dosha/manglik-dosh", data.dict())
    inc("requests_ok"); return result

@dosha_router.post("/pitra-dosh")
async def pitra_dosh(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dosha/pitra-dosh", data.dict())
    inc("requests_ok"); return result



#  DASHAS  (12 endpoints)


@dashas_router.post("/antar-dasha")
async def antar_dasha(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/antar-dasha", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/char-dasha-current")
async def char_dasha_current(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/char-dasha-current", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/char-dasha-main")
async def char_dasha_main(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/char-dasha-main", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/char-dasha-sub")
async def char_dasha_sub(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/char-dasha-sub", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/current-mahadasha")
async def current_mahadasha(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/current-mahadasha", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/current-mahadasha-full")
async def current_mahadasha_full(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/current-mahadasha-full", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/maha-dasha")
async def maha_dasha(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/maha-dasha", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/maha-dasha-predictions")
async def maha_dasha_predictions(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/maha-dasha-predictions", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/paryantar-dasha")
async def paryantar_dasha(data: SubDashaParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/paryantar-dasha", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/specific-sub-dasha")
async def specific_sub_dasha(data: SubDashaParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/specific-sub-dasha", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/yogini-dasha-main")
async def yogini_dasha_main(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/yogini-dasha-main", data.dict())
    inc("requests_ok"); return result

@dashas_router.post("/yogini-dasha-sub")
async def yogini_dasha_sub(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("dashas/yogini-dasha-sub", data.dict())
    inc("requests_ok"); return result



#  EXTENDED HOROSCOPE  (19 endpoints)


@extended_router.post("/arutha-lagnas")
async def arutha_lagnas(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/arutha-lagnas", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/current-sade-sati")
async def current_sade_sati(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/current-sade-sati", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/kundli-details")
async def kundli_details(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/extended-kundli-details", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/find-ascendant")
async def find_ascendant(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/find-ascendant", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/find-moon-sign")
async def find_moon_sign(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/find-moon-sign", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/find-sun-sign")
async def find_sun_sign(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/find-sun-sign", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/friendship")
async def friendship(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/friendship", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/gem-suggestion")
async def gem_suggestion(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/gem-suggestion", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/jaimini-karakas")
async def jaimini_karakas(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/jaimini-karakas", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/kp-houses")
async def kp_houses(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/kp-houses", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/kp-planets")
async def kp_planets(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/kp-planets", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/numero-table")
async def numero_table(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/numero-table", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/rudraksh-suggestion")
async def rudraksh_suggestion(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/rudraksh-suggestion", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/sade-sati-table")
async def sade_sati_table(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/sade-sati-table", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/shad-bala")
async def shad_bala(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/shad-bala", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/varshapal-details")
async def varshapal_details(data: VarshapalParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/varshapal-details", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/varshapal-month-chart")
async def varshapal_month_chart(data: VarshapalParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/varshapal-month-chart", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/varshapal-year-chart")
async def varshapal_year_chart(data: VarshapalParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/varshapal-year-chart", data.dict())
    inc("requests_ok"); return result

@extended_router.post("/yoga-calculator")
async def yoga_calculator(data: KundliParams):
    inc("requests_total")
    result = await call_vedic_api("extended-horoscope/yoga-calculator", data.dict())
    inc("requests_ok"); return result




@utilities_router.get("/geo-search")
async def geo_search(place: str = Query(..., examples="Jodhpur")):
    inc("requests_total")
    result = await call_vedic_api("utilities/geo-search", {"place": place})
    inc("requests_ok"); return result

@utilities_router.get("/geo-search-advanced")
async def geo_search_advanced(
    place: str = Query(..., examples="Jodhpur"),
    state: str = Query("",  examples="Rajasthan"),
):
    inc("requests_total")
    result = await call_vedic_api("utilities/geo-search-advanced", {"place": place, "state": state})
    inc("requests_ok"); return result

@utilities_router.get("/gem-details")
async def gem_details(stone: str = Query(..., examples="Ruby")):
    inc("requests_total")
    result = await call_vedic_api("utilities/gem-details", {"stone": stone})
    inc("requests_ok"); return result

@utilities_router.get("/nakshatra-vastu")
async def nakshatra_vastu(nakshatra: str = Query(..., examples="Svati")):
    inc("requests_total")
    result = await call_vedic_api("utilities/nakshatra-vastu-details", {"nakshatra": nakshatra})
    inc("requests_ok"); return result

@utilities_router.get("/radical-number")
async def radical_number(dob: str = Query(..., examples="15/8/1990")):
    inc("requests_total")
    result = await call_vedic_api("utilities/radical-number-details", {"dob": dob})
    inc("requests_ok"); return result




@health_router.get("/health")
async def health_check():
    redis_ok = False
    try:
        await get_redis().ping()
        redis_ok = True
    except Exception:
        pass

    api_key_ok = bool(settings.VEDIC_API_KEY)
    overall    = "healthy" if (redis_ok and api_key_ok) else "degraded"

    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={
            "status":      overall,
            "redis":       "connected"   if redis_ok   else "disconnected",
            "api_key_set": api_key_ok,
            "base_url":    settings.VEDIC_BASE_URL,
        },
    )


@health_router.get("/metrics")
async def astro_metrics():
    return {"metrics": _metrics, "timestamp": time.time()}


@health_router.get("/clear-cache")
async def clear_astro_cache():
    keys    = await RedisCache.keys_by_prefix("astro:v1:")
    deleted = await RedisCache.delete(*keys) if keys else 0
    logger.info(f"[ASTRO] Cache cleared: {deleted} keys")
    return {"status": "cache cleared", "keys_deleted": deleted}
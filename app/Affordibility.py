import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
import uuid
from typing import Any, Optional, List

import asyncpg
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from google import genai
from pydantic import BaseModel, Field, validator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("property_api")


class Settings:
    DATABASE_URL: str          = os.getenv("DATABASE_URL", "")
    DB_POOL_MIN_SIZE: int      = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
    DB_POOL_MAX_SIZE: int      = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
    DB_COMMAND_TIMEOUT: int    = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
    DOWN_PAYMENT_RATIO: float  = float(os.getenv("DOWN_PAYMENT_RATIO", "0.20"))
    LOAN_ANNUAL_RATE:  float   = float(os.getenv("LOAN_ANNUAL_RATE",  "0.085"))
    LOAN_TENURE_YEARS: int     = int(os.getenv("LOAN_TENURE_YEARS",   "20"))
    REDIS_URL: str             = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
    CACHE_TTL: int             = int(os.getenv("CACHE_TTL",          "300"))
    CACHE_EMPTY_TTL: int       = int(os.getenv("CACHE_EMPTY_TTL",    "60"))
    TIPS_CACHE_TTL: int        = int(os.getenv("TIPS_CACHE_TTL",     "3600"))
    TIPS_FALLBACK_TTL: int     = int(os.getenv("TIPS_FALLBACK_TTL",  "120"))
    GEMINI_INFLIGHT_TTL: int   = int(os.getenv("GEMINI_INFLIGHT_TTL", "30"))
    RATE_LIMIT_PER_MIN: int    = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))
    GCP_PROJECT: str           = os.getenv("GCP_PROJECT", "")
    GCP_LOCATION: str          = os.getenv("GCP_LOCATION", "us-central1")
    GEMINI_MODEL: str          = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    GEMINI_TIMEOUT: int        = int(os.getenv("GEMINI_TIMEOUT", "20"))
    GEMINI_MAX_CONCURRENT: int = int(os.getenv("GEMINI_MAX_CONCURRENT", "5"))
    GEMINI_MAX_RETRIES: int    = int(os.getenv("GEMINI_MAX_RETRIES", "3"))
    MAX_SALARY: float          = float(os.getenv("MAX_SALARY", "1e7"))
    MAX_SAVINGS: float         = float(os.getenv("MAX_SAVINGS", "5e7"))
    MAX_EXISTING_EMI: float    = float(os.getenv("MAX_EXISTING_EMI", "5e6"))
    DEFAULT_PAGE_SIZE: int     = int(os.getenv("DEFAULT_PAGE_SIZE", "20"))
    MAX_PAGE_SIZE: int         = int(os.getenv("MAX_PAGE_SIZE", "100"))
    ALLOWED_ORIGINS: List[str] = os.getenv("ALLOWED_ORIGINS", "*").split(",")

settings = Settings()


_metrics: dict[str, int] = {
    "requests_total": 0, "requests_ok": 0, "requests_error": 0,
    "rate_limited": 0,
    "gemini_calls": 0, "gemini_cache_hits": 0,
    "gemini_inflight_hits": 0,
    "gemini_fallbacks": 0,
    "property_cache_hits": 0,
    "property_empty_cache_hits": 0,
    "db_errors": 0,
}
def inc(key: str, n: int = 1):
    _metrics[key] = _metrics.get(key, 0) + n


FALLBACK_TIPS: dict[str, List[str]] = {
    "SAFE": [
        "Abhi market conditions achhi hain — home loan pre-approval le lo.",
        "Down payment ke saath 6-month emergency fund bhi maintain karo.",
        "Fixed-rate home loan consider karo, interest rates badh sakti hain.",
        "Metro/infra projects wale areas mein property value tezi se badhti hai.",
        "Loan tenure chhota rakho agar EMI afford ho; interest bachega.",
    ],
    "CAUTION": [
        "Existing EMI pehle reduce karo — loan eligibility improve hogi.",
        "Savings ke liye dedicated RD ya SIP shuru karo aur 6-12 mahine wait karo.",
        "Co-applicant add karo — loan eligibility aur tax benefit milega.",
        "Peripheral location se start karo; equity build hone par upgrade karna aasaan hoga.",
        "Bada EMI lene se pehle fee-only financial advisor se zaroor milo.",
    ],
    "AVOID": [
        "Abhi property lena financially risky hai — pehle debts clear karo.",
        "EMI burden bahut zyada hai; income ya savings improve ka wait karo.",
        "Renting zyada samajhdari hai — difference ko SIP mein invest karo.",
        "Unnecessary kharch band karo aur 12 mahine mein reassess karo.",
        "Certified financial planner se debt-reduction plan banwao.",
    ],
}


redis_client: redis.Redis = None
_db_pool: asyncpg.Pool = None
_gemini_semaphore: asyncio.Semaphore = None


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
            if not keys:
                return 0
            return await redis_client.delete(*keys)
        except Exception as e:
            logger.warning(f"Redis DELETE failed: {e}")
            return 0

    @staticmethod
    async def keys_by_prefix(prefix: str) -> List[str]:
        try:
            return await redis_client.keys(f"{prefix}*")
        except Exception as e:
            logger.warning(f"Redis KEYS failed [{prefix}]: {e}")
            return []


class RedisRateLimiter:
    @staticmethod
    async def is_rate_limited(ip: str, prefix: str = "afford") -> bool:
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
                return True
            return False
        except Exception as e:
            logger.warning(f"Rate limiter error (fail-open): {e}")
            return False


gemini_client = genai.Client(
    vertexai=True, project=settings.GCP_PROJECT, location=settings.GCP_LOCATION,
)

_inflight_events: dict[str, asyncio.Event] = {}


def _tips_cache_key(report: dict) -> str:
    raw = ":".join([
        report["status"],
        report["risk_profile"],
        str(report["score"]),
        str(report["savings_adequate"]),
        report["emi_stress"],
    ])
    return f"tips:v1:{hashlib.md5(raw.encode()).hexdigest()}"


def _inflight_key(cache_key: str) -> str:
    return f"inflight:{cache_key}"


async def _call_gemini_once(prompt: str) -> List[str]:
    async with _gemini_semaphore:
        inc("gemini_calls")
        async def _req():
            return gemini_client.models.generate_content(
                model=settings.GEMINI_MODEL, contents=prompt
            )
        response = await asyncio.wait_for(_req(), timeout=settings.GEMINI_TIMEOUT)
        tips = [l.lstrip("•-* ").strip() for l in response.text.strip().splitlines() if l.strip()]
        tips = [t for t in tips if len(t) > 5]
        if not tips:
            raise ValueError("Gemini returned empty response")
        return tips[:5]


async def generate_tips(report: dict) -> List[str]:
    cache_key    = _tips_cache_key(report)
    inflight_key = _inflight_key(cache_key)

    cached = await RedisCache.get(cache_key)
    if cached:
        inc("gemini_cache_hits")
        logger.info(f"Gemini L1 cache hit [{cache_key[-8:]}]")
        return cached

    lock_acquired = await redis_client.set(
        inflight_key, "1", nx=True, ex=settings.GEMINI_INFLIGHT_TTL,
    )

    if not lock_acquired:
        event = _inflight_events.get(cache_key)
        if event is None:
            event = asyncio.Event()
            _inflight_events[cache_key] = event
        try:
            await asyncio.wait_for(
                asyncio.shield(event.wait()),
                timeout=settings.GEMINI_INFLIGHT_TTL,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timed out waiting for in-flight Gemini [{cache_key[-8:]}]")
        cached = await RedisCache.get(cache_key)
        if cached:
            inc("gemini_inflight_hits")
            return cached

    local_event = asyncio.Event()
    _inflight_events[cache_key] = local_event

    prompt = f"""
You are a financial advisor. Generate EXACTLY 5 short Hinglish tips based on this report.
Rules: No calculations. No numbers. Only 5 bullet points. Each tip 1 sentence.
Status:{report['status']} Risk:{report['risk_profile']} Stress:{report['emi_stress']}
Score:{report['score']} SavingsOK:{report['savings_adequate']}
Format: • Tip1 • Tip2 • Tip3 • Tip4 • Tip5
"""
    last_err = None
    tips = None

    for attempt in range(settings.GEMINI_MAX_RETRIES):
        try:
            tips = await _call_gemini_once(prompt)
            await RedisCache.set(cache_key, tips, ttl=settings.TIPS_CACHE_TTL)
            break
        except Exception as e:
            last_err = e
            err = str(e).lower()
            if not any(kw in err for kw in ("429", "quota", "rate", "503", "500", "timeout")):
                break
            if attempt < settings.GEMINI_MAX_RETRIES - 1:
                wait = (2 ** attempt) + random.uniform(0.1, 0.5)
                logger.warning(f"Gemini attempt {attempt+1} failed, retrying in {wait:.1f}s: {e}")
                await asyncio.sleep(wait)

    if tips is None:
        inc("gemini_fallbacks")
        logger.error(f"Gemini exhausted retries [{cache_key[-8:]}]: {last_err}")
        tips = FALLBACK_TIPS.get(report["status"], FALLBACK_TIPS["CAUTION"])
        await RedisCache.set(cache_key, tips, ttl=settings.TIPS_FALLBACK_TTL)

    await RedisCache.delete(inflight_key)
    local_event.set()
    _inflight_events.pop(cache_key, None)
    return tips


# ─────────────────────────────────────────────
#  Pydantic Models
# ─────────────────────────────────────────────

class FinanceInput(BaseModel):
    salary:       float = Field(..., gt=0,  le=settings.MAX_SALARY)
    existing_emi: float = Field(..., ge=0,  le=settings.MAX_EXISTING_EMI)
    savings:      float = Field(..., ge=0,  le=settings.MAX_SAVINGS)
    city:         str   = Field(..., min_length=2, max_length=50)
    risk:         float = Field(0.40, ge=0.30, le=0.50)

    @validator("city")
    def sanitize_city(cls, v):
        v = v.strip()
        if not re.match(r"^[a-zA-Z\s\-]+$", v):
            raise ValueError("City must contain only letters, spaces, or hyphens")
        return v.title()

    @validator("risk")
    def validate_risk(cls, v):
        if not any(abs(v - r) < 0.001 for r in {0.30, 0.40, 0.50}):
            raise ValueError("Risk must be 0.30, 0.40, or 0.50")
        return v


class RentInput(BaseModel):
    monthly_income: float = Field(..., gt=0,  le=settings.MAX_SALARY)
    existing_emi:   float = Field(..., ge=0,  le=settings.MAX_EXISTING_EMI)
    city:           str   = Field(..., min_length=2, max_length=50)
    rent_ratio:     float = Field(0.30, ge=0.20, le=0.40)

    @validator("city")
    def sanitize_city(cls, v):
        v = v.strip()
        if not re.match(r"^[a-zA-Z\s\-]+$", v):
            raise ValueError("City must contain only letters, spaces, or hyphens")
        return v.title()

    @validator("rent_ratio")
    def validate_rent_ratio(cls, v):
        if not any(abs(v - r) < 0.001 for r in {0.20, 0.30, 0.40}):
            raise ValueError("rent_ratio must be 0.20, 0.30, or 0.40")
        return v


# ─────────────────────────────────────────────
#  Financial Logic
# ─────────────────────────────────────────────

def salary_projection(salary: float) -> dict:
    return {
        "current":  round(salary,        2),
        "3_years":  round(salary * 1.25, 2),
        "5_years":  round(salary * 1.45, 2),
        "10_years": round(salary * 2.10, 2),
    }


def _compute_loan_multiplier() -> float:
    r = settings.LOAN_ANNUAL_RATE / 12
    n = settings.LOAN_TENURE_YEARS * 12
    if r == 0:
        return float(n)
    return (1 - (1 + r) ** -n) / r


_LOAN_MULTIPLIER: float = _compute_loan_multiplier()


def financial_analysis(data: FinanceInput) -> dict:
    allowed_emi    = data.salary * data.risk
    property_emi   = max(allowed_emi - data.existing_emi, 0)
    max_loan       = property_emi * _LOAN_MULTIPLIER
    property_price = max_loan / (1 - settings.DOWN_PAYMENT_RATIO)
    down_payment   = property_price * settings.DOWN_PAYMENT_RATIO

    total_emi    = data.existing_emi + property_emi
    stress_ratio = (total_emi / data.salary) * 100
    if stress_ratio < 30:    stress, status = "Low",    "SAFE"
    elif stress_ratio < 40: stress, status = "Medium", "CAUTION"
    else:                    stress, status = "High",   "AVOID"

    risk_label       = ("Conservative" if data.risk == 0.30
                        else "Balanced" if data.risk == 0.40 else "Aggressive")
    savings_gap      = max(down_payment - data.savings, 0)
    savings_adequate = data.savings >= down_payment

    score = 100
    if stress == "Medium":                      score -= 15
    elif stress == "High":                      score -= 35
    if not savings_adequate:                    score -= 10
    if data.existing_emi / data.salary > 0.20: score -= 10

    return {
        "status": status, "risk_profile": risk_label,
        "max_property_price":   round(property_price, 2),
        "max_monthly_emi":      round(property_emi, 2),
        "emi_stress":           stress,
        "stress_ratio_percent": round(stress_ratio, 2),
        "score":                max(score, 0),
        "down_payment_needed":  round(down_payment, 2),
        "savings_adequate":     savings_adequate,
        "savings_gap":          round(savings_gap, 2),
        "salary_growth":        salary_projection(data.salary),
        "range_min":            round(property_price * 0.80, 2),
        "range_max":            round(property_price * 1.20, 2),
        "loan_assumptions": {
            "annual_interest_rate_pct": round(settings.LOAN_ANNUAL_RATE * 100, 2),
            "tenure_years":             settings.LOAN_TENURE_YEARS,
            "loan_multiplier":          round(_LOAN_MULTIPLIER, 4),
            "max_loan_amount":          round(max_loan, 2),
        },
    }


def rent_analysis(data: RentInput) -> dict:
    max_rent     = data.monthly_income * data.rent_ratio
    available    = max(max_rent - data.existing_emi, 0)

    stress_ratio = ((data.existing_emi + available) / data.monthly_income) * 100
    if stress_ratio < 30:    stress, status = "Low",    "SAFE"
    elif stress_ratio <= 40: stress, status = "Medium", "CAUTION"
    else:                    stress, status = "High",   "AVOID"

    ratio_label = ("Conservative" if data.rent_ratio == 0.20
                   else "Balanced" if data.rent_ratio == 0.30 else "Aggressive")

    score = 100
    if stress == "Medium":                            score -= 15
    elif stress == "High":                            score -= 35
    if data.existing_emi / data.monthly_income > 0.20: score -= 10

    return {
        "status":               status,
        "risk_profile":         ratio_label,
        "max_monthly_rent":     round(available, 2),
        "emi_stress":           stress,
        "stress_ratio_percent": round(stress_ratio, 2),
        "score":                max(score, 0),
        "savings_adequate":     True,   # rent has no down payment
        "range_min":            round(available * 0.80, 2),
        "range_max":            round(available * 1.20, 2),
        "income_projection":    salary_projection(data.monthly_income),
    }


# ─────────────────────────────────────────────
#  Price Parsers
# ─────────────────────────────────────────────

def _parse_99acres_price(price_str: str) -> Optional[float]:
    """
    Handles all 99acres price formats:
      Buy  → "₹1.4 Cr" | "₹99 L" | "₹2.75 - 2.82 Cr"
      Rent → "₹64000.80 /month" | "₹1.2 L /month" | "₹1.4 Cr /month"
    Returns numeric rupee value (monthly for rent, total for buy).
    """
    if not price_str or "on Request" in price_str:
        return None
    try:
        # Step 1: Remove ₹ symbol and commas
        clean = price_str.replace("₹", "").replace(",", "")
        # Step 2: Remove /month suffix and anything after it
        clean = re.sub(r"/month.*", "", clean, flags=re.IGNORECASE).strip()
        # Step 3: Take first number before range separator or space
        first = re.split(r"[\s\-–]", clean)[0]
        value = float(first)
        # Step 4: Apply multiplier based on original string
        upper = price_str.upper()
        if "CR" in upper:                              return value * 10_000_000
        if "L" in upper or "LAC" in upper                         or "LAKH" in upper:            return value * 100_000
        # Plain number like ₹64000.80 /month — return as-is
        return value
    except Exception as e:
        logger.warning(f"_parse_99acres_price failed for '{price_str}': {e}")
        return None


# ─────────────────────────────────────────────
#  DB Fetchers — BUY (listing_type = 'buy')
# ─────────────────────────────────────────────

async def fetch_99acres_buy(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit = int(min_p)
    max_lit = int(max_p)
    sql = r"""
        SELECT scraped_data FROM property_raw_data
        WHERE source = '99acres'
          AND listing_type = 'buy'
          AND (
               scraped_data->>'description' ILIKE $1
            OR scraped_data->>'title'       ILIKE $1
            OR scraped_data->>'pageUrl'     ILIKE $1
          )
          AND scraped_data->>'priceRange' IS NOT NULL
          AND scraped_data->>'priceRange' NOT ILIKE '%on Request%'
          AND CASE
                WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
                  THEN (REGEXP_REPLACE(SPLIT_PART(
                         REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                         '-',1),'[^0-9.]','','g'))::numeric * 10000000
                WHEN scraped_data->>'priceRange' ~* 'L'
                  THEN (REGEXP_REPLACE(SPLIT_PART(
                         REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                         '-',1),'[^0-9.]','','g'))::numeric * 100000
                ELSE NULL
              END BETWEEN $2 AND $3
        LIMIT 50
    """
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(sql, f"%{city}%", min_lit, max_lit)
    except Exception as e:
        inc("db_errors")
        logger.error(f"[99acres][buy] DB error: {e}", exc_info=True)
        return []

    out = []
    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, dict) else None
            if item is None: continue
            price = _parse_99acres_price(item.get("priceRange", ""))
            if price is None: continue
            item["_source"] = "99acres"
            item["_listing_type"] = "buy"
            item["_price_numeric"] = price
            out.append(item)
        except Exception as e:
            logger.error(f"[99acres][buy] row {i} error: {e}", exc_info=True)
    return out


async def fetch_magicbricks_buy(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit = int(min_p)
    max_lit = int(max_p)
    sql = """
        SELECT scraped_data FROM property_raw_data
        WHERE source = 'magicbricks'
          AND listing_type = 'buy'
          AND (
               scraped_data->>'city_name' ILIKE $1
            OR scraped_data->>'address'   ILIKE $1
          )
          AND scraped_data->>'price' IS NOT NULL
          AND (scraped_data->>'price')::numeric BETWEEN $2 AND $3
        LIMIT 50
    """
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(sql, f"%{city}%", min_lit, max_lit)
    except Exception as e:
        inc("db_errors")
        logger.error(f"[magicbricks][buy] DB error: {e}", exc_info=True)
        return []

    out = []
    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, dict) else None
            if item is None: continue
            price_val = item.get("price")
            if price_val is None: continue
            item["_source"] = "magicbricks"
            item["_listing_type"] = "buy"
            item["_price_numeric"] = float(price_val)
            out.append(item)
        except Exception as e:
            logger.error(f"[magicbricks][buy] row {i} error: {e}", exc_info=True)
    return out


# ─────────────────────────────────────────────
#  DB Fetchers — RENT (listing_type = 'rent')
# ─────────────────────────────────────────────

async def fetch_99acres_rent(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit = int(min_p)
    max_lit = int(max_p)
    sql = r"""
        SELECT scraped_data FROM property_raw_data
        WHERE source = '99acres'
          AND listing_type = 'rent'
          AND (
               scraped_data->>'description' ILIKE $1
            OR scraped_data->>'title'       ILIKE $1
            OR scraped_data->>'pageUrl'     ILIKE $1
          )
          AND scraped_data->>'priceRange' IS NOT NULL
          AND scraped_data->>'priceRange' NOT ILIKE '%on Request%'
          AND CASE
                WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
                  THEN (REGEXP_REPLACE(SPLIT_PART(
                         REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                         '-',1),'[^0-9.]','','g'))::numeric * 10000000
                WHEN scraped_data->>'priceRange' ILIKE '%L%'
                  THEN (REGEXP_REPLACE(SPLIT_PART(
                         REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                         '-',1),'[^0-9.]','','g'))::numeric * 100000
                ELSE
                  -- Handle plain rent like ₹64000.80 /month
                  (REGEXP_REPLACE(
                     REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                     '[^0-9.]','','g'
                  ))::numeric
              END BETWEEN $2 AND $3
        LIMIT 50
    """
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(sql, f"%{city}%", min_lit, max_lit)
    except Exception as e:
        inc("db_errors")
        logger.error(f"[99acres][rent] DB error: {e}", exc_info=True)
        return []

    out = []
    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, dict) else None
            if item is None: continue
            price = _parse_99acres_price(item.get("priceRange", ""))
            if price is None: continue
            item["_source"] = "99acres"
            item["_listing_type"] = "rent"
            item["_price_numeric"] = price
            out.append(item)
        except Exception as e:
            logger.error(f"[99acres][rent] row {i} error: {e}", exc_info=True)
    return out


async def fetch_magicbricks_rent(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit = int(min_p)
    max_lit = int(max_p)
    sql = """
        SELECT scraped_data FROM property_raw_data
        WHERE source = 'magicbricks'
          AND listing_type = 'rent'
          AND (
               scraped_data->>'city_name' ILIKE $1
            OR scraped_data->>'address'   ILIKE $1
          )
          AND scraped_data->>'price' IS NOT NULL
          AND (scraped_data->>'price')::numeric BETWEEN $2 AND $3
        LIMIT 50
    """
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(sql, f"%{city}%", min_lit, max_lit)
    except Exception as e:
        inc("db_errors")
        logger.error(f"[magicbricks][rent] DB error: {e}", exc_info=True)
        return []

    out = []
    for i, row in enumerate(rows):
        try:
            raw  = row["scraped_data"]
            item = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, dict) else None
            if item is None: continue
            price_val = item.get("price")
            if price_val is None: continue
            item["_source"] = "magicbricks"
            item["_listing_type"] = "rent"
            item["_price_numeric"] = float(price_val)
            out.append(item)
        except Exception as e:
            logger.error(f"[magicbricks][rent] row {i} error: {e}", exc_info=True)
    return out


# ─────────────────────────────────────────────
#  Aggregate Fetchers with Cache
# ─────────────────────────────────────────────

async def fetch_all_buy_properties(city: str, min_p: float, max_p: float) -> dict:
    ck = f"afford_buy:props:{hashlib.md5(f'{city}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"

    cached = await RedisCache.get(ck)
    if cached is not None:
        total_cached = sum(len(v) for v in cached.values() if isinstance(v, list))
        if total_cached > 0:
            inc("property_cache_hits")
            return cached
        inc("property_empty_cache_hits")
        return cached

    r99, rmb = await asyncio.gather(
        fetch_99acres_buy(city, min_p, max_p),
        fetch_magicbricks_buy(city, min_p, max_p),
        return_exceptions=True,
    )

    out = {
        "99acres":     r99 if isinstance(r99, list) else [],
        "magicbricks": rmb if isinstance(rmb, list) else [],
    }
    total = sum(len(v) for v in out.values())
    ttl   = settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL
    await RedisCache.set(ck, out, ttl=ttl)
    return out


async def fetch_all_rent_properties(city: str, min_p: float, max_p: float) -> dict:
    ck = f"afford_rent:props:{hashlib.md5(f'{city}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"

    cached = await RedisCache.get(ck)
    if cached is not None:
        total_cached = sum(len(v) for v in cached.values() if isinstance(v, list))
        if total_cached > 0:
            inc("property_cache_hits")
            return cached
        inc("property_empty_cache_hits")
        return cached

    r99, rmb = await asyncio.gather(
        fetch_99acres_rent(city, min_p, max_p),
        fetch_magicbricks_rent(city, min_p, max_p),
        return_exceptions=True,
    )

    out = {
        "99acres":     r99 if isinstance(r99, list) else [],
        "magicbricks": rmb if isinstance(rmb, list) else [],
    }
    total = sum(len(v) for v in out.values())
    ttl   = settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL
    await RedisCache.set(ck, out, ttl=ttl)
    return out


# ─────────────────────────────────────────────
#  Routers
# ─────────────────────────────────────────────

buy_router  = APIRouter(prefix="/affordability/buy",  tags=["Affordability Buy"])
rent_router = APIRouter(prefix="/affordability/rent", tags=["Affordability Rent"])


# ── BUY endpoints ──────────────────────────────

@buy_router.post("/report")
async def generate_buy_report(
    data:      FinanceInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    logger.info(f"[BUY] Report: salary={data.salary} city={data.city} risk={data.risk}")
    finance = financial_analysis(data)

    props_by_src, tips = await asyncio.gather(
        fetch_all_buy_properties(data.city, finance["range_min"], finance["range_max"]),
        generate_tips(finance),
    )

    props_99acres     = props_by_src.get("99acres",     [])
    props_magicbricks = props_by_src.get("magicbricks", [])

    return {
        "financial_report": finance,
        "city":             data.city,
        "listing_type":     "buy",
        "ai_tips":          tips,
        "properties_summary": {
            "total": len(props_99acres) + len(props_magicbricks),
            "by_source": {
                "99acres":     len(props_99acres),
                "magicbricks": len(props_magicbricks),
            },
        },
        "properties_by_source": {
            "99acres":     props_99acres,
            "magicbricks": props_magicbricks,
        },
    }


@buy_router.get("/health/detailed")
async def buy_detailed_health():
    redis_ok = db_ok = False
    try:
        await redis_client.ping(); redis_ok = True
    except Exception: pass
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        db_ok = True
    except Exception: pass
    overall = "healthy" if (redis_ok and db_ok) else "degraded"
    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={
            "status":   overall,
            "redis":    "connected"    if redis_ok else "disconnected",
            "database": "connected"    if db_ok    else "disconnected",
        },
    )


@buy_router.get("/metrics")
async def buy_metrics():
    return {"metrics": _metrics, "timestamp": time.time()}


@buy_router.get("/clear-cache")
async def buy_clear_cache():
    deleted = 0
    for prefix in ("afford_buy:props:", "tips:", "inflight:", "afford_buy:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys:
            deleted += await RedisCache.delete(*keys)
    logger.info(f"[BUY] Cache cleared: {deleted} keys deleted")
    return {"status": "cache cleared", "keys_deleted": deleted}


# ── RENT endpoints ─────────────────────────────

@rent_router.post("/report")
async def generate_rent_report(
    data:      RentInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    logger.info(f"[RENT] Report: income={data.monthly_income} city={data.city} ratio={data.rent_ratio}")
    finance = rent_analysis(data)

    props_by_src, tips = await asyncio.gather(
        fetch_all_rent_properties(data.city, finance["range_min"], finance["range_max"]),
        generate_tips(finance),
    )

    props_99acres     = props_by_src.get("99acres",     [])
    props_magicbricks = props_by_src.get("magicbricks", [])

    return {
        "financial_report": finance,
        "city":             data.city,
        "listing_type":     "rent",
        "ai_tips":          tips,
        "properties_summary": {
            "total": len(props_99acres) + len(props_magicbricks),
            "by_source": {
                "99acres":     len(props_99acres),
                "magicbricks": len(props_magicbricks),
            },
        },
        "properties_by_source": {
            "99acres":     props_99acres,
            "magicbricks": props_magicbricks,
        },
    }


@rent_router.get("/health/detailed")
async def rent_detailed_health():
    redis_ok = db_ok = False
    try:
        await redis_client.ping(); redis_ok = True
    except Exception: pass
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        db_ok = True
    except Exception: pass
    overall = "healthy" if (redis_ok and db_ok) else "degraded"
    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={
            "status":   overall,
            "redis":    "connected"    if redis_ok else "disconnected",
            "database": "connected"    if db_ok    else "disconnected",
        },
    )


@rent_router.get("/metrics")
async def rent_metrics():
    return {"metrics": _metrics, "timestamp": time.time()}


@rent_router.get("/clear-cache")
async def rent_clear_cache():
    deleted = 0
    for prefix in ("afford_rent:props:", "tips:", "inflight:", "afford_rent:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys:
            deleted += await RedisCache.delete(*keys)
    logger.info(f"[RENT] Cache cleared: {deleted} keys deleted")
    return {"status": "cache cleared", "keys_deleted": deleted}
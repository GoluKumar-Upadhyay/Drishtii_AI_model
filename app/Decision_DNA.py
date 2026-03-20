# import asyncio
# import hashlib
# import json
# import logging
# import math
# import os
# import re
# import time
# import uuid
# from typing import Any, Dict, List, Optional

# import asyncpg
# import httpx
# import redis.asyncio as redis
# from dotenv import load_dotenv
# from fastapi import APIRouter, Query, Request
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel, Field, validator

# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
# )
# logger = logging.getLogger("decision_dna")


# class Settings:
#     DATABASE_URL: str          = os.getenv("DATABASE_URL", "")
#     DB_POOL_MIN_SIZE: int      = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
#     DB_POOL_MAX_SIZE: int      = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
#     DB_COMMAND_TIMEOUT: int    = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
#     DOWN_PAYMENT_RATIO: float  = float(os.getenv("DOWN_PAYMENT_RATIO", "0.20"))
#     LOAN_ANNUAL_RATE: float    = float(os.getenv("LOAN_ANNUAL_RATE",   "0.085"))
#     LOAN_TENURE_YEARS: int     = int(os.getenv("LOAN_TENURE_YEARS",   "20"))
#     FOIR: float                = float(os.getenv("FOIR",               "0.40"))
#     REDIS_URL: str             = os.getenv("REDIS_URL", "redis://localhost:6379/0")
#     REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
#     CACHE_TTL: int             = int(os.getenv("CACHE_TTL",           "300"))
#     CACHE_EMPTY_TTL: int       = int(os.getenv("CACHE_EMPTY_TTL",     "60"))
#     PLACES_CACHE_TTL: int      = int(os.getenv("PLACES_CACHE_TTL",    "86400"))
#     GEOCODE_CACHE_TTL: int     = int(os.getenv("GEOCODE_CACHE_TTL",   "2592000"))
#     GOOGLE_PLACES_API_KEY: str    = os.getenv("GOOGLE_PLACES_API_KEY",    "")
#     GOOGLE_GEOCODING_API_KEY: str = os.getenv("GOOGLE_GEOCODING_API_KEY", "")
#     PLACES_RADIUS_M: float        = float(os.getenv("PLACES_RADIUS_M",    "5000"))
#     PLACES_MAX_RESULTS: int       = int(os.getenv("PLACES_MAX_RESULTS",   "10"))
#     PLACES_HTTP_TIMEOUT: float    = float(os.getenv("PLACES_HTTP_TIMEOUT","8.0"))
#     PLACES_MAX_CONCURRENT: int    = int(os.getenv("PLACES_MAX_CONCURRENT","10"))
#     PLACES_SATURATION: int        = int(os.getenv("PLACES_SATURATION",    "10"))
#     BINARY_REJECT_THRESHOLD: int  = int(os.getenv("BINARY_REJECT_THRESHOLD", "7"))
#     RATE_LIMIT_PER_MIN: int    = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))
#     DEFAULT_PAGE_SIZE: int     = int(os.getenv("DEFAULT_PAGE_SIZE",  "20"))
#     MAX_PAGE_SIZE: int         = int(os.getenv("MAX_PAGE_SIZE",      "100"))
#     ALLOWED_ORIGINS: List[str] = os.getenv("ALLOWED_ORIGINS", "*").split(",")
#     MAX_SALARY: float          = float(os.getenv("MAX_SALARY",      "9e7"))
#     MAX_SAVINGS: float         = float(os.getenv("MAX_SAVINGS",     "9e7"))
#     MAX_EXISTING_EMI: float    = float(os.getenv("MAX_EXISTING_EMI","9e6"))

# settings = Settings()

# _metrics: Dict[str, int] = {
#     "requests_total": 0, "requests_ok": 0, "requests_error": 0,
#     "rate_limited": 0,
#     "property_cache_hits": 0, "property_empty_cache_hits": 0,
#     "db_errors": 0,
#     "places_api_calls": 0, "places_cache_hits": 0, "places_errors": 0,
#     "geocode_api_calls": 0, "geocode_cache_hits": 0, "geocode_errors": 0,
#     "binary_rejected": 0,
# }

# def inc(key: str, n: int = 1) -> None:
#     _metrics[key] = _metrics.get(key, 0) + n


# LIFESTYLE_TYPES: Dict[str, List[str]] = {
#     "commute":     ["transit_station","subway_station","bus_station",
#                     "train_station","light_rail_station"],
#     "safety":      ["police","fire_station","hospital"],
#     "education":   ["school","primary_school","secondary_school","university"],
#     "greenery":    ["park","national_park","botanical_garden","playground"],
#     "social_life": ["restaurant","bar","shopping_mall","movie_theater","cafe"],
# }

# PLACES_URL            = "https://places.googleapis.com/v1/places:searchNearby"
# PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
# FAMILY_MIN_BHK: Dict[str, int] = {"1+": 1, "2+": 2, "3+": 3, "4+": 4}

# # Injected by drishtii.py lifespan
# redis_client: redis.Redis      = None
# _db_pool: asyncpg.Pool         = None
# _places_sem: asyncio.Semaphore = None


# # ─────────────────────────────────────────────
# #  Redis helpers
# # ─────────────────────────────────────────────

# class RedisCache:
#     @staticmethod
#     async def get(key: str) -> Optional[Any]:
#         try:
#             data = await redis_client.get(key)
#             return json.loads(data) if data else None
#         except Exception as e:
#             logger.warning(f"Redis GET failed [{key}]: {e}")
#             return None

#     @staticmethod
#     async def set(key: str, value: Any, ttl: int = settings.CACHE_TTL) -> bool:
#         try:
#             await redis_client.setex(key, ttl, json.dumps(value, default=str))
#             return True
#         except Exception as e:
#             logger.warning(f"Redis SET failed [{key}]: {e}")
#             return False

#     @staticmethod
#     async def delete(*keys: str) -> int:
#         try:
#             if not keys: return 0
#             return await redis_client.delete(*keys)
#         except Exception as e:
#             logger.warning(f"Redis DELETE failed: {e}")
#             return 0

#     @staticmethod
#     async def keys_by_prefix(prefix: str) -> List[str]:
#         try:
#             return await redis_client.keys(f"{prefix}*")
#         except Exception as e:
#             logger.warning(f"Redis KEYS failed [{prefix}]: {e}")
#             return []


# class RedisRateLimiter:
#     @staticmethod
#     async def is_rate_limited(ip: str) -> bool:
#         try:
#             key    = f"decision_dna:rl:{ip}"
#             now    = time.time()
#             member = f"{now:.6f}-{uuid.uuid4().hex[:8]}"
#             pipe   = redis_client.pipeline(transaction=True)
#             pipe.zremrangebyscore(key, 0, now - 60)
#             pipe.zadd(key, {member: now})
#             pipe.zcard(key)
#             pipe.expire(key, 60)
#             results = await pipe.execute()
#             count   = results[2]
#             if count > settings.RATE_LIMIT_PER_MIN:
#                 inc("rate_limited")
#                 return True
#             return False
#         except Exception as e:
#             logger.warning(f"Rate limiter error (fail-open): {e}")
#             return False


# # ─────────────────────────────────────────────
# #  Financial helpers
# # ─────────────────────────────────────────────

# def _compute_loan_multiplier() -> float:
#     r = settings.LOAN_ANNUAL_RATE / 12
#     n = settings.LOAN_TENURE_YEARS * 12
#     return (1 - (1 + r) ** -n) / r if r else float(n)

# _LOAN_MULTIPLIER: float = _compute_loan_multiplier()


# def _salary_projection(salary: float) -> dict:
#     return {
#         "now":  round(salary,        2),
#         "3yr":  round(salary * 1.25, 2),
#         "5yr":  round(salary * 1.45, 2),
#         "10yr": round(salary * 2.10, 2),
#     }


# def financial_analysis(
#     salary: float, existing_emi: float, savings: float,
#     property_price: Optional[float], expected_monthly_rent: Optional[float],
# ) -> dict:
#     allowed_emi    = salary * settings.FOIR
#     property_emi   = max(allowed_emi - existing_emi, 0.0)
#     emi_overloaded = existing_emi >= allowed_emi

#     if property_price and property_price > 0:
#         max_property_price = property_price
#         max_loan           = property_price * (1 - settings.DOWN_PAYMENT_RATIO)
#         price_source       = "user_provided"
#     elif emi_overloaded:
#         max_loan           = 0.0
#         max_property_price = 0.0
#         price_source       = "ineligible_existing_emi_exceeds_foir"
#     else:
#         max_loan           = property_emi * _LOAN_MULTIPLIER
#         max_property_price = max_loan / (1 - settings.DOWN_PAYMENT_RATIO)
#         price_source       = "calculated_from_salary"

#     down_payment     = max_property_price * settings.DOWN_PAYMENT_RATIO
#     raw_stress       = (existing_emi / salary) * 100
#     stress_ratio     = min(round(raw_stress, 2), 999.99)

#     if emi_overloaded:       stress, status = "High",   "AVOID"
#     elif stress_ratio < 30:  stress, status = "Low",    "SAFE"
#     elif stress_ratio <= 40: stress, status = "Medium", "CAUTION"
#     else:                    stress, status = "High",   "AVOID"

#     savings_gap      = max(down_payment - savings, 0.0)
#     savings_adequate = savings >= down_payment

#     score = 100
#     if emi_overloaded:               score -= 55
#     elif stress == "Medium":         score -= 15
#     elif stress == "High":           score -= 35
#     if not savings_adequate:         score -= 10
#     if existing_emi / salary > 0.20: score -= 10

#     range_min = round(max_property_price * 0.80, 2)
#     range_max = round(max_property_price * 1.20, 2)

#     if expected_monthly_rent and expected_monthly_rent > 0:
#         extra = round(property_emi - expected_monthly_rent, 2)
#         buy_vs_rent = {
#             "applicable":        True,
#             "current_rent":      expected_monthly_rent,
#             "property_emi":      round(property_emi, 2),
#             "extra_cost_to_own": extra,
#             "verdict": (
#                 f"Buying costs \u20b9{abs(extra):,.0f}/month "
#                 + ("more" if extra > 0 else "less")
#                 + " than renting currently"
#             ),
#         }
#     else:
#         buy_vs_rent = {"applicable": False, "reason": "expected_monthly_rent not provided"}

#     return {
#         "price_source":        price_source,
#         "status":              status,
#         "affordability_score": max(score, 0),
#         "emi_stress":          stress,
#         "stress_ratio_pct":    round(stress_ratio, 2),
#         "max_property_price":  round(max_property_price, 2),
#         "max_monthly_emi":     round(property_emi, 2),
#         "down_payment_needed": round(down_payment, 2),
#         "savings_adequate":    savings_adequate,
#         "savings_gap":         round(savings_gap, 2),
#         "price_range":         {"min": range_min, "max": range_max},
#         "loan": {
#             "max_amount":   round(max_loan, 2),
#             "rate_pct":     round(settings.LOAN_ANNUAL_RATE * 100, 2),
#             "tenure_years": settings.LOAN_TENURE_YEARS,
#             "multiplier":   round(_LOAN_MULTIPLIER, 4),
#         },
#         "salary_growth": _salary_projection(salary),
#         "buy_vs_rent":   buy_vs_rent,
#     }


# def rent_financial_analysis(
#     monthly_income: float, existing_emi: float, rent_ratio: float,
#     expected_monthly_rent: Optional[float],
# ) -> dict:
#     """Financial analysis for rent decision DNA."""
#     max_rent_budget   = monthly_income * rent_ratio
#     available_rent    = max(max_rent_budget - existing_emi, 0.0)
#     total_obligations = existing_emi + available_rent
#     foir_actual       = (total_obligations / monthly_income) * 100

#     if foir_actual < 30:    stress, status = "Low",    "SAFE"
#     elif foir_actual <= 40: stress, status = "Medium", "CAUTION"
#     else:                   stress, status = "High",   "AVOID"

#     score = 100
#     if stress == "Medium":                                 score -= 15
#     elif stress == "High":                                 score -= 35
#     if existing_emi / monthly_income > 0.20:               score -= 10
#     if existing_emi / monthly_income > 0.35:               score -= 10

#     leftover             = monthly_income - total_obligations
#     recommended_savings  = monthly_income * 0.20
#     can_save_20pct       = leftover >= recommended_savings

#     range_min = round(available_rent * 0.80, 2)
#     range_max = round(available_rent * 1.20, 2)

#     rent_vs_buy = {"applicable": False, "reason": "expected_monthly_rent not provided"}
#     if expected_monthly_rent and expected_monthly_rent > 0:
#         diff = round(available_rent - expected_monthly_rent, 2)
#         rent_vs_buy = {
#             "applicable":       True,
#             "max_affordable":   round(available_rent, 2),
#             "expected_rent":    expected_monthly_rent,
#             "budget_surplus":   diff,
#             "verdict": (
#                 f"Budget has \u20b9{abs(diff):,.0f}/month "
#                 + ("surplus" if diff >= 0 else "shortfall")
#             ),
#         }

#     return {
#         "status":              status,
#         "affordability_score": max(score, 0),
#         "emi_stress":          stress,
#         "foir_actual_pct":     round(foir_actual, 2),
#         "max_monthly_rent":    round(available_rent, 2),
#         "leftover_after_rent": round(leftover, 2),
#         "recommended_savings": round(recommended_savings, 2),
#         "can_save_20pct":      can_save_20pct,
#         "price_range":         {"min": range_min, "max": range_max},
#         "income_projection":   _salary_projection(monthly_income),
#         "rent_vs_buy":         rent_vs_buy,
#     }


# # ─────────────────────────────────────────────
# #  DB helpers
# # ─────────────────────────────────────────────

# async def _db_fetch(sql: str, *args) -> List[asyncpg.Record]:
#     try:
#         async with _db_pool.acquire() as conn:
#             return await conn.fetch(sql, *args)
#     except Exception as e:
#         inc("db_errors")
#         logger.error(f"DB error: {e}", exc_info=True)
#         return []


# def _row_to_dict(row: asyncpg.Record) -> Optional[dict]:
#     try:
#         raw = row["scraped_data"]
#         if isinstance(raw, str):  return json.loads(raw)
#         if isinstance(raw, dict): return raw
#     except Exception:
#         pass
#     return None


# # ─────────────────────────────────────────────
# #  Price parsers
# # ─────────────────────────────────────────────

# def _parse_99acres_price(price_str: str) -> Optional[float]:
#     """
#     Handles all 99acres price formats:
#       Buy  -> "\u20b91.4 Cr" | "\u20b999 L" | "\u20b92.75 - 2.82 Cr"
#       Rent -> "\u20b964000.80 /month" | "\u20b91.2 L /month"
#     """
#     if not price_str or "on Request" in price_str:
#         return None
#     try:
#         clean = price_str.replace("\u20b9", "").replace(",", "")
#         clean = re.sub(r"/month.*", "", clean, flags=re.IGNORECASE).strip()
#         first = re.split(r"[\s\-\u2013]", clean)[0]
#         value = float(first)
#         upper = price_str.upper()
#         if "CR"  in upper:                   return value * 10_000_000
#         if "L"   in upper or "LAC" in upper                           or "LAKH" in upper: return value * 100_000
#         return value
#     except Exception as e:
#         logger.warning(f"_parse_99acres_price failed for '{price_str}': {e}")
#         return None


# # ─────────────────────────────────────────────
# #  DB Fetchers — BUY (listing_type = 'buy')
# # ─────────────────────────────────────────────

# async def fetch_99acres_buy(city: str, min_p: float, max_p: float) -> List[dict]:
#     min_lit, max_lit = int(min_p), int(max_p)
#     logger.info(f"[DB][99acres][buy] city={city!r} range=[{min_lit}-{max_lit}]")
#     sql = r"""
#         SELECT scraped_data FROM property_raw_data
#         WHERE source = '99acres'
#           AND listing_type = 'buy'
#           AND (
#                scraped_data->>'description' ILIKE $1
#             OR scraped_data->>'title'       ILIKE $1
#             OR scraped_data->>'pageUrl'     ILIKE $1
#           )
#           AND scraped_data->>'priceRange' IS NOT NULL
#           AND scraped_data->>'priceRange' NOT ILIKE '%on Request%'
#           AND CASE
#                 WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
#                   THEN (REGEXP_REPLACE(SPLIT_PART(
#                          REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
#                          '-',1),'[^0-9.]','','g'))::numeric * 10000000
#                 WHEN scraped_data->>'priceRange' ILIKE '%L%'
#                   THEN (REGEXP_REPLACE(SPLIT_PART(
#                          REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
#                          '-',1),'[^0-9.]','','g'))::numeric * 100000
#                 ELSE NULL
#               END BETWEEN $2 AND $3
        
#     """
#     rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
#     out = []
#     for i, row in enumerate(rows):
#         try:
#             item = _row_to_dict(row)
#             if not item: continue
#             price = _parse_99acres_price(item.get("priceRange", ""))
#             if price is None: continue
#             item["_source"]        = "99acres"
#             item["_listing_type"]  = "buy"
#             item["_price_numeric"] = price
#             out.append(item)
#         except Exception as e:
#             logger.error(f"[99acres][buy] row {i}: {e}", exc_info=True)
#     logger.info(f"[DB][99acres][buy] valid={len(out)}")
#     return out


# async def fetch_magicbricks_buy(city: str, min_p: float, max_p: float) -> List[dict]:
#     min_lit, max_lit = int(min_p), int(max_p)
#     logger.info(f"[DB][magicbricks][buy] city={city!r} range=[{min_lit}-{max_lit}]")
#     sql = """
#         SELECT scraped_data FROM property_raw_data
#         WHERE source = 'magicbricks'
#           AND listing_type = 'buy'
#           AND (
#                scraped_data->>'city_name' ILIKE $1
#             OR scraped_data->>'address'   ILIKE $1
#           )
#           AND scraped_data->>'price' IS NOT NULL
#           AND (scraped_data->>'price')::numeric BETWEEN $2 AND $3
        
#     """
#     rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
#     out = []
#     for i, row in enumerate(rows):
#         try:
#             item = _row_to_dict(row)
#             if not item: continue
#             price = item.get("price")
#             if price is None: continue
#             item["_source"]        = "magicbricks"
#             item["_listing_type"]  = "buy"
#             item["_price_numeric"] = float(price)
#             out.append(item)
#         except Exception as e:
#             logger.error(f"[magicbricks][buy] row {i}: {e}", exc_info=True)
#     logger.info(f"[DB][magicbricks][buy] valid={len(out)}")
#     return out


# # ─────────────────────────────────────────────
# #  DB Fetchers — RENT (listing_type = 'rent')
# # ─────────────────────────────────────────────

# async def fetch_99acres_rent(city: str, min_p: float, max_p: float) -> List[dict]:
#     min_lit, max_lit = int(min_p), int(max_p)
#     logger.info(f"[DB][99acres][rent] city={city!r} range=[{min_lit}-{max_lit}]")
#     sql = r"""
#         SELECT scraped_data FROM property_raw_data
#         WHERE source = '99acres'
#           AND listing_type = 'rent'
#           AND (
#                scraped_data->>'description' ILIKE $1
#             OR scraped_data->>'title'       ILIKE $1
#             OR scraped_data->>'pageUrl'     ILIKE $1
#           )
#           AND scraped_data->>'priceRange' IS NOT NULL
#           AND scraped_data->>'priceRange' NOT ILIKE '%on Request%'
#           AND CASE
#                 WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
#                   THEN (REGEXP_REPLACE(SPLIT_PART(
#                          REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
#                          '-',1),'[^0-9.]','','g'))::numeric * 10000000
#                 WHEN scraped_data->>'priceRange' ILIKE '%L%'
#                   THEN (REGEXP_REPLACE(SPLIT_PART(
#                          REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
#                          '-',1),'[^0-9.]','','g'))::numeric * 100000
#                 ELSE
#                   (REGEXP_REPLACE(
#                      REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
#                      '[^0-9.]','','g'
#                   ))::numeric
#               END BETWEEN $2 AND $3
        
#     """
#     rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
#     out = []
#     for i, row in enumerate(rows):
#         try:
#             item = _row_to_dict(row)
#             if not item: continue
#             price = _parse_99acres_price(item.get("priceRange", ""))
#             if price is None: continue
#             item["_source"]        = "99acres"
#             item["_listing_type"]  = "rent"
#             item["_price_numeric"] = price
#             out.append(item)
#         except Exception as e:
#             logger.error(f"[99acres][rent] row {i}: {e}", exc_info=True)
#     logger.info(f"[DB][99acres][rent] valid={len(out)}")
#     return out


# async def fetch_magicbricks_rent(city: str, min_p: float, max_p: float) -> List[dict]:
#     min_lit, max_lit = int(min_p), int(max_p)
#     logger.info(f"[DB][magicbricks][rent] city={city!r} range=[{min_lit}-{max_lit}]")
#     sql = """
#         SELECT scraped_data FROM property_raw_data
#         WHERE source = 'magicbricks'
#           AND listing_type = 'rent'
#           AND (
#                scraped_data->>'city_name' ILIKE $1
#             OR scraped_data->>'address'   ILIKE $1
#           )
#           AND scraped_data->>'price' IS NOT NULL
#           AND (scraped_data->>'price')::numeric BETWEEN $2 AND $3
        
#     """
#     rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
#     out = []
#     for i, row in enumerate(rows):
#         try:
#             item = _row_to_dict(row)
#             if not item: continue
#             price = item.get("price")
#             if price is None: continue
#             item["_source"]        = "magicbricks"
#             item["_listing_type"]  = "rent"
#             item["_price_numeric"] = float(price)
#             out.append(item)
#         except Exception as e:
#             logger.error(f"[magicbricks][rent] row {i}: {e}", exc_info=True)
#     logger.info(f"[DB][magicbricks][rent] valid={len(out)}")
#     return out


# # ─────────────────────────────────────────────
# #  Aggregate fetchers with cache
# # ─────────────────────────────────────────────

# async def fetch_all_buy_properties(city: str, min_p: float, max_p: float) -> dict:
#     ck = f"decision_dna:buy:props:{hashlib.md5(f'{city.lower()}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"
#     cached = await RedisCache.get(ck)
#     if cached is not None:
#         total = sum(len(v) for v in cached.values() if isinstance(v, list))
#         inc("property_cache_hits" if total > 0 else "property_empty_cache_hits")
#         return cached
#     r99, rmb = await asyncio.gather(
#         fetch_99acres_buy(city, min_p, max_p),
#         fetch_magicbricks_buy(city, min_p, max_p),
#         return_exceptions=True,
#     )
#     out = {
#         "99acres":     r99 if isinstance(r99, list) else [],
#         "magicbricks": rmb if isinstance(rmb, list) else [],
#     }
#     total = sum(len(v) for v in out.values())
#     await RedisCache.set(ck, out, settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL)
#     logger.info(f"[DB][buy] 99acres={len(out['99acres'])} magicbricks={len(out['magicbricks'])} total={total}")
#     return out


# async def fetch_all_rent_properties(city: str, min_p: float, max_p: float) -> dict:
#     ck = f"decision_dna:rent:props:{hashlib.md5(f'{city.lower()}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"
#     cached = await RedisCache.get(ck)
#     if cached is not None:
#         total = sum(len(v) for v in cached.values() if isinstance(v, list))
#         inc("property_cache_hits" if total > 0 else "property_empty_cache_hits")
#         return cached
#     r99, rmb = await asyncio.gather(
#         fetch_99acres_rent(city, min_p, max_p),
#         fetch_magicbricks_rent(city, min_p, max_p),
#         return_exceptions=True,
#     )
#     out = {
#         "99acres":     r99 if isinstance(r99, list) else [],
#         "magicbricks": rmb if isinstance(rmb, list) else [],
#     }
#     total = sum(len(v) for v in out.values())
#     await RedisCache.set(ck, out, settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL)
#     logger.info(f"[DB][rent] 99acres={len(out['99acres'])} magicbricks={len(out['magicbricks'])} total={total}")
#     return out


# # ─────────────────────────────────────────────
# #  Bedroom + family filter
# # ─────────────────────────────────────────────

# def _bedrooms(item: dict) -> Optional[int]:
#     src = item.get("_source", "")
#     try:
#         if src == "magicbricks":
#             v = item.get("bedrooms")
#             if v is not None: return int(v)
#             for field in ("name", "description"):
#                 txt = item.get(field, "") or ""
#                 m = re.search(r"(\d+)\s*BHK", txt, re.IGNORECASE)
#                 if m: return int(m.group(1))
#             return None
#         if src == "99acres":
#             for field in ("floorSize", "propertyType", "title"):
#                 txt = item.get(field, "") or ""
#                 m = re.search(r"(\d+)\s*BHK", txt, re.IGNORECASE)
#                 if m: return int(m.group(1))
#             return None
#     except Exception:
#         pass
#     return None


# def _passes_family(item: dict, fs: str) -> bool:
#     if not fs or fs not in FAMILY_MIN_BHK: return True
#     beds = _bedrooms(item)
#     if beds is None: return True
#     return beds >= FAMILY_MIN_BHK[fs]


# # ─────────────────────────────────────────────
# #  Coordinate extraction
# # ─────────────────────────────────────────────

# def _coords_direct(item: dict) -> Optional[tuple]:
#     src = item.get("_source", "")
#     try:
#         if src == "magicbricks":
#             loc = item.get("location", "") or ""
#             if loc.strip():
#                 parts = loc.split(",")
#                 if len(parts) >= 2:
#                     lat, lng = float(parts[0].strip()), float(parts[1].strip())
#                     if -90 <= lat <= 90 and -180 <= lng <= 180:
#                         return lat, lng
#         elif src == "99acres":
#             lat = item.get("latitude") or item.get("lat")
#             lng = item.get("longitude") or item.get("lng") or item.get("lon")
#             if lat and lng:
#                 return float(lat), float(lng)
#     except Exception:
#         pass
#     return None


# def _extract_city_from_item(item: dict) -> Optional[str]:
#     src = item.get("_source", "")
#     if src == "magicbricks":
#         return item.get("city_name") or None
#     if src == "99acres":
#         url = item.get("pageUrl", "") or item.get("url", "") or ""
#         m = re.search(r"/(?:buy|rent)/([a-z\-]+?)(?:-ffid|/|\?|$)", url)
#         if m: return m.group(1).replace("-", " ").title()
#         title = item.get("title", "") or ""
#         parts = title.split(",")
#         if len(parts) >= 2: return parts[-1].strip()
#     return None


# def _extract_locality_from_item(item: dict) -> Optional[str]:
#     src = item.get("_source", "")
#     if src == "99acres":
#         title = item.get("title", "") or ""
#         m = re.search(r" in (.+)$", title, re.IGNORECASE)
#         if m: return m.group(1).strip()
#     if src == "magicbricks":
#         name = item.get("name", "") or ""
#         m = re.search(r"at\s+(.+)$", name, re.IGNORECASE)
#         if m: return m.group(1).strip()
#         url = item.get("url", "") or ""
#         m2 = re.search(r"FOR-(?:Sale|Rent)-([A-Za-z\-]+)-in-", url, re.IGNORECASE)
#         if m2: return m2.group(1).replace("-", " ").title()
#         lm = item.get("landmark", "") or ""
#         if lm: return lm
#     return None


# # ─────────────────────────────────────────────
# #  Places / geocoding
# # ─────────────────────────────────────────────

# async def _resolve_coords_via_places(
#     client: httpx.AsyncClient, item: dict
# ) -> Optional[tuple]:
#     locality = _extract_locality_from_item(item)
#     city     = _extract_city_from_item(item)
#     if not locality and not city:
#         return None
#     query = ", ".join(p for p in [locality, city] if p)
#     ck    = f"decision_dna:geocode:{hashlib.md5(query.lower().encode()).hexdigest()}"
#     cached = await RedisCache.get(ck)
#     if cached:
#         inc("geocode_cache_hits")
#         return tuple(cached)
#     try:
#         inc("geocode_api_calls")
#         headers = {"Content-Type": "application/json", "X-Goog-FieldMask": "places.location,places.displayName"}
#         resp = await client.post(
#             f"{PLACES_TEXT_SEARCH_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
#             json={"textQuery": query, "maxResultCount": 1},
#             headers=headers, timeout=6.0,
#         )
#         resp.raise_for_status()
#         places = resp.json().get("places", [])
#         if places:
#             loc = places[0].get("location", {})
#             lat, lng = loc.get("latitude"), loc.get("longitude")
#             if lat and lng:
#                 await RedisCache.set(ck, [lat, lng], settings.GEOCODE_CACHE_TTL)
#                 return float(lat), float(lng)
#         # city-only fallback
#         if locality and city:
#             resp2 = await client.post(
#                 f"{PLACES_TEXT_SEARCH_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
#                 json={"textQuery": city, "maxResultCount": 1},
#                 headers=headers, timeout=6.0,
#             )
#             resp2.raise_for_status()
#             places2 = resp2.json().get("places", [])
#             if places2:
#                 loc2 = places2[0].get("location", {})
#                 lat2, lng2 = loc2.get("latitude"), loc2.get("longitude")
#                 if lat2 and lng2:
#                     await RedisCache.set(ck, [lat2, lng2], settings.GEOCODE_CACHE_TTL)
#                     return float(lat2), float(lng2)
#     except Exception as e:
#         inc("geocode_errors")
#         logger.warning(f"[PlacesSearch] FAILED for {query!r}: {e}")
#     return None


# def _places_ck(lat: float, lng: float, cat: str) -> str:
#     return f"decision_dna:places:v1:{hashlib.md5(f'{lat:.5f}:{lng:.5f}:{cat}'.encode()).hexdigest()}"


# async def _places_count(client: httpx.AsyncClient, lat: float, lng: float, cat: str) -> int:
#     ck = _places_ck(lat, lng, cat)
#     cached = await RedisCache.get(ck)
#     if cached is not None:
#         inc("places_cache_hits"); return int(cached)
#     types = LIFESTYLE_TYPES.get(cat, [])
#     if not types: return 0
#     payload = {
#         "includedTypes": types,
#         "maxResultCount": settings.PLACES_MAX_RESULTS,
#         "locationRestriction": {
#             "circle": {
#                 "center": {"latitude": lat, "longitude": lng},
#                 "radius": settings.PLACES_RADIUS_M,
#             }
#         },
#     }
#     headers = {"Content-Type": "application/json", "X-Goog-FieldMask": "places.id"}
#     try:
#         async with _places_sem:
#             inc("places_api_calls")
#             resp = await client.post(
#                 f"{PLACES_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
#                 json=payload, headers=headers, timeout=settings.PLACES_HTTP_TIMEOUT,
#             )
#             resp.raise_for_status()
#             body  = resp.json()
#             count = len(body.get("places", []))
#             if "error" in body:
#                 logger.error(f"[Places][ERROR] cat={cat}: {body['error']}")
#     except Exception as e:
#         inc("places_errors")
#         logger.warning(f"[Places][FAILED] cat={cat} ({lat:.4f},{lng:.4f}): {e}")
#         return 0
#     await RedisCache.set(ck, count, settings.PLACES_CACHE_TTL)
#     return count


# async def _dna_score(
#     client: httpx.AsyncClient, lat: float, lng: float, weights: Dict[str, float],
# ) -> Dict[str, Any]:
#     active      = [c for c, w in weights.items() if w > 0]
#     counts_list = await asyncio.gather(
#         *[_places_count(client, lat, lng, c) for c in active],
#         return_exceptions=True,
#     )
#     count_map = {c: (v if isinstance(v, int) else 0) for c, v in zip(active, counts_list)}
#     total = 0.0; rejected = False; breakdown: Dict[str, Any] = {}
#     for cat, weight in weights.items():
#         count       = count_map.get(cat, 0)
#         normalised  = min(count / settings.PLACES_SATURATION, 1.0)
#         ws          = round(normalised * weight, 3)
#         total      += ws
#         binary_fail = (weight >= settings.BINARY_REJECT_THRESHOLD and count == 0)
#         if binary_fail:
#             rejected = True; inc("binary_rejected")
#         breakdown[cat] = {
#             "places_found_in_5km": count,
#             "binary_check":        "FAIL" if binary_fail else "PASS",
#             "normalised":          round(normalised, 3),
#             "weight":              weight,
#             "score_contribution":  ws,
#         }
#     max_possible = sum(weights.values())
#     match_pct    = round((total / max_possible) * 100, 2) if max_possible > 0 else 0
#     return {
#         "dna_score":    round(total, 2),
#         "max_possible": max_possible,
#         "match_pct":    match_pct,
#         "rejected":     rejected,
#         "breakdown":    breakdown,
#     }


# # ─────────────────────────────────────────────
# #  Score & rank (shared by buy and rent)
# # ─────────────────────────────────────────────

# async def score_and_rank(
#     props_by_src: Dict[str, List[dict]],
#     weights: Dict[str, float],
#     family_size: Optional[str],
#     page: int, page_size: int,
# ) -> dict:
#     all_props: List[dict] = []
#     for items in props_by_src.values():
#         all_props.extend(items)

#     if family_size:
#         all_props = [p for p in all_props if _passes_family(p, family_size)]

#     with_coords: List[tuple] = []
#     needs_geocode: List[dict] = []

#     for prop in all_props:
#         c = _coords_direct(prop)
#         if c:
#             with_coords.append((prop, c[0], c[1]))
#         else:
#             needs_geocode.append(prop)

#     if needs_geocode:
#         async with httpx.AsyncClient() as http_client:
#             gc_results = await asyncio.gather(
#                 *[_resolve_coords_via_places(http_client, p) for p in needs_geocode],
#                 return_exceptions=True,
#             )
#         no_coords = []
#         for prop, gc in zip(needs_geocode, gc_results):
#             if isinstance(gc, tuple) and len(gc) == 2:
#                 with_coords.append((prop, gc[0], gc[1]))
#             else:
#                 no_coords.append(prop)
#     else:
#         no_coords = []

#     scored: List[dict] = []
#     if with_coords:
#         async with httpx.AsyncClient() as http_client:
#             dna_results = await asyncio.gather(
#                 *[_dna_score(http_client, lat, lng, weights) for (_, lat, lng) in with_coords],
#                 return_exceptions=True,
#             )
#         for (prop, lat, lng), dna in zip(with_coords, dna_results):
#             if isinstance(dna, Exception):
#                 dna = {"dna_score": 0.0, "rejected": False, "breakdown": {}}
#             if not dna.get("rejected", False):
#                 scored.append({**prop, "dna": dna})

#     for prop in no_coords:
#         scored.append({
#             **prop,
#             "dna": {
#                 "dna_score":    0.0,
#                 "max_possible": sum(weights.values()),
#                 "match_pct":    0.0,
#                 "rejected":     False,
#                 "breakdown":    {},
#                 "note":         "coordinates_unavailable",
#             },
#         })

#     scored.sort(key=lambda p: (-p["dna"]["dna_score"], p.get("_price_numeric", float("inf"))))
#     total      = len(scored)
#     start      = (page - 1) * page_size
#     page_items = scored[start: start + page_size]
#     return {
#         "total":       total,
#         "page":        page,
#         "page_size":   page_size,
#         "total_pages": math.ceil(total / page_size) if total > 0 else 0,
#         "items":       page_items,
#     }


# # ─────────────────────────────────────────────
# #  Pydantic models
# # ─────────────────────────────────────────────

# class LifestyleInput(BaseModel):
#     commute:     float = Field(5.0, ge=0, le=10)
#     safety:      float = Field(5.0, ge=0, le=10)
#     education:   float = Field(5.0, ge=0, le=10)
#     greenery:    float = Field(5.0, ge=0, le=10)
#     social_life: float = Field(5.0, ge=0, le=10)


# class BuyReportInput(BaseModel):
#     salary:                float          = Field(..., gt=0, le=settings.MAX_SALARY)
#     existing_emi:          float          = Field(..., ge=0, le=settings.MAX_EXISTING_EMI)
#     savings:               float          = Field(..., ge=0, le=settings.MAX_SAVINGS)
#     city:                  str            = Field(..., min_length=2, max_length=60)
#     family_size:           Optional[str]  = Field(None)
#     property_price:        Optional[float]= Field(None, ge=0)
#     expected_monthly_rent: Optional[float]= Field(None, ge=0)
#     lifestyle:             LifestyleInput = Field(default_factory=LifestyleInput)

#     @validator("city")
#     def sanitize_city(cls, v):
#         v = v.strip()
#         if not re.match(r"^[a-zA-Z\s\-]+$", v):
#             raise ValueError("City must contain only letters, spaces, or hyphens")
#         return v.title()

#     @validator("family_size")
#     def validate_family_size(cls, v):
#         if v is not None and v not in FAMILY_MIN_BHK:
#             raise ValueError(f"family_size must be one of {list(FAMILY_MIN_BHK)}")
#         return v


# class RentReportInput(BaseModel):
#     monthly_income:        float          = Field(..., gt=0, le=settings.MAX_SALARY,       description="Monthly net income in \u20b9")
#     existing_emi:          float          = Field(..., ge=0, le=settings.MAX_EXISTING_EMI, description="Existing monthly EMIs in \u20b9")
#     city:                  str            = Field(..., min_length=2, max_length=60)
#     rent_ratio:            float          = Field(0.30, ge=0.20, le=0.40,                  description="Max % of income for rent: 0.20/0.30/0.40")
#     family_size:           Optional[str]  = Field(None)
#     expected_monthly_rent: Optional[float]= Field(None, ge=0)
#     lifestyle:             LifestyleInput = Field(default_factory=LifestyleInput)

#     @validator("city")
#     def sanitize_city(cls, v):
#         v = v.strip()
#         if not re.match(r"^[a-zA-Z\s\-]+$", v):
#             raise ValueError("City must contain only letters, spaces, or hyphens")
#         return v.title()

#     @validator("rent_ratio")
#     def validate_rent_ratio(cls, v):
#         if not any(abs(v - r) < 0.001 for r in {0.20, 0.30, 0.40}):
#             raise ValueError("rent_ratio must be 0.20, 0.30, or 0.40")
#         return v

#     @validator("family_size")
#     def validate_family_size(cls, v):
#         if v is not None and v not in FAMILY_MIN_BHK:
#             raise ValueError(f"family_size must be one of {list(FAMILY_MIN_BHK)}")
#         return v


# # ─────────────────────────────────────────────
# #  Routers
# # ─────────────────────────────────────────────

# buy_router  = APIRouter(prefix="/decision/buy",  tags=["Decision DNA Buy"])
# rent_router = APIRouter(prefix="/decision/rent", tags=["Decision DNA Rent"])

# # keep old router alias so drishtii.py import still works
# router = buy_router


# # ── BUY endpoints ────────────────────────────

# @buy_router.post("/Decision_report")
# async def generate_buy_report(
#     data:      BuyReportInput,
#     page:      int = Query(1,                          ge=1),
#     page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
# ):
#     finance      = financial_analysis(
#         data.salary, data.existing_emi, data.savings,
#         data.property_price, data.expected_monthly_rent,
#     )
#     props_by_src = await fetch_all_buy_properties(
#         data.city, finance["price_range"]["min"], finance["price_range"]["max"],
#     )
#     weights = data.lifestyle.dict()
#     ranked  = await score_and_rank(props_by_src, weights, data.family_size, page, page_size)
#     return {
#         "listing_type":       "buy",
#         "financial_summary":  finance,
#         "search_summary": {
#             "city":              data.city,
#             "family_size":       data.family_size,
#             "price_range":       finance["price_range"],
#             "source_raw_counts": {src: len(items) for src, items in props_by_src.items()},
#         },
#         "lifestyle_weights": weights,
#         "properties":        ranked,
#     }


# @buy_router.get("/health/detailed")
# async def buy_health():
#     redis_ok = db_ok = False
#     try: await redis_client.ping(); redis_ok = True
#     except Exception: pass
#     try:
#         async with _db_pool.acquire() as c: await c.execute("SELECT 1"); db_ok = True
#     except Exception: pass
#     overall = "healthy" if (redis_ok and db_ok) else "degraded"
#     return JSONResponse(
#         status_code=200 if overall == "healthy" else 503,
#         content={"status": overall, "redis": "connected" if redis_ok else "disconnected",
#                  "database": "connected" if db_ok else "disconnected"},
#     )


# @buy_router.get("/metrics")
# async def buy_metrics():
#     return {"metrics": _metrics, "timestamp": time.time()}


# @buy_router.get("/clear-cache")
# async def buy_clear_cache():
#     deleted = 0
#     for prefix in ("decision_dna:buy:props:", "decision_dna:places:", "decision_dna:geocode:", "decision_dna:rl:"):
#         keys = await RedisCache.keys_by_prefix(prefix)
#         if keys: deleted += await RedisCache.delete(*keys)
#     return {"status": "cache cleared", "keys_deleted": deleted}


# # ── RENT endpoints ───────────────────────────

# @rent_router.post("/Decision_report")
# async def generate_rent_report(
#     data:      RentReportInput,
#     page:      int = Query(1,                          ge=1),
#     page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
# ):
#     finance      = rent_financial_analysis(
#         data.monthly_income, data.existing_emi,
#         data.rent_ratio, data.expected_monthly_rent,
#     )
#     props_by_src = await fetch_all_rent_properties(
#         data.city, finance["price_range"]["min"], finance["price_range"]["max"],
#     )
#     weights = data.lifestyle.dict()
#     ranked  = await score_and_rank(props_by_src, weights, data.family_size, page, page_size)
#     return {
#         "listing_type":       "rent",
#         "financial_summary":  finance,
#         "search_summary": {
#             "city":              data.city,
#             "family_size":       data.family_size,
#             "price_range":       finance["price_range"],
#             "source_raw_counts": {src: len(items) for src, items in props_by_src.items()},
#         },
#         "lifestyle_weights": weights,
#         "properties":        ranked,
#     }


# @rent_router.get("/health/detailed")
# async def rent_health():
#     redis_ok = db_ok = False
#     try: await redis_client.ping(); redis_ok = True
#     except Exception: pass
#     try:
#         async with _db_pool.acquire() as c: await c.execute("SELECT 1"); db_ok = True
#     except Exception: pass
#     overall = "healthy" if (redis_ok and db_ok) else "degraded"
#     return JSONResponse(
#         status_code=200 if overall == "healthy" else 503,
#         content={"status": overall, "redis": "connected" if redis_ok else "disconnected",
#                  "database": "connected" if db_ok else "disconnected"},
#     )


# @rent_router.get("/metrics")
# async def rent_metrics():
#     return {"metrics": _metrics, "timestamp": time.time()}


# @rent_router.get("/clear-cache")
# async def rent_clear_cache():
#     deleted = 0
#     for prefix in ("decision_dna:rent:props:", "decision_dna:places:", "decision_dna:geocode:", "decision_dna:rl:"):
#         keys = await RedisCache.keys_by_prefix(prefix)
#         if keys: deleted += await RedisCache.delete(*keys)
#     return {"status": "cache cleared", "keys_deleted": deleted}

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional

import asyncpg
import httpx
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("decision_dna")


class Settings:
    DATABASE_URL: str          = os.getenv("DATABASE_URL", "")
    DB_POOL_MIN_SIZE: int      = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
    DB_POOL_MAX_SIZE: int      = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
    DB_COMMAND_TIMEOUT: int    = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
    DOWN_PAYMENT_RATIO: float  = float(os.getenv("DOWN_PAYMENT_RATIO", "0.20"))
    LOAN_ANNUAL_RATE: float    = float(os.getenv("LOAN_ANNUAL_RATE",   "0.085"))
    LOAN_TENURE_YEARS: int     = int(os.getenv("LOAN_TENURE_YEARS",   "20"))
    FOIR: float                = float(os.getenv("FOIR",               "0.40"))
    REDIS_URL: str             = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
    CACHE_TTL: int             = int(os.getenv("CACHE_TTL",           "300"))
    CACHE_EMPTY_TTL: int       = int(os.getenv("CACHE_EMPTY_TTL",     "60"))
    PLACES_CACHE_TTL: int      = int(os.getenv("PLACES_CACHE_TTL",    "86400"))
    GEOCODE_CACHE_TTL: int     = int(os.getenv("GEOCODE_CACHE_TTL",   "2592000"))
    GOOGLE_PLACES_API_KEY: str    = os.getenv("GOOGLE_PLACES_API_KEY",    "")
    GOOGLE_GEOCODING_API_KEY: str = os.getenv("GOOGLE_GEOCODING_API_KEY", "")
    PLACES_RADIUS_M: float        = float(os.getenv("PLACES_RADIUS_M",    "5000"))
    PLACES_MAX_RESULTS: int       = int(os.getenv("PLACES_MAX_RESULTS",   "10"))
    PLACES_HTTP_TIMEOUT: float    = float(os.getenv("PLACES_HTTP_TIMEOUT","8.0"))
    PLACES_MAX_CONCURRENT: int    = int(os.getenv("PLACES_MAX_CONCURRENT","10"))
    PLACES_SATURATION: int        = int(os.getenv("PLACES_SATURATION",    "10"))
    BINARY_REJECT_THRESHOLD: int  = int(os.getenv("BINARY_REJECT_THRESHOLD", "7"))
    RATE_LIMIT_PER_MIN: int    = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))
    DEFAULT_PAGE_SIZE: int     = int(os.getenv("DEFAULT_PAGE_SIZE",  "20"))
    MAX_PAGE_SIZE: int         = int(os.getenv("MAX_PAGE_SIZE",      "100"))
    ALLOWED_ORIGINS: List[str] = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    MAX_SALARY: float          = float(os.getenv("MAX_SALARY",      "9e7"))
    MAX_SAVINGS: float         = float(os.getenv("MAX_SAVINGS",     "9e7"))
    MAX_EXISTING_EMI: float    = float(os.getenv("MAX_EXISTING_EMI","9e6"))

settings = Settings()

_metrics: Dict[str, int] = {
    "requests_total": 0, "requests_ok": 0, "requests_error": 0,
    "rate_limited": 0,
    "property_cache_hits": 0, "property_empty_cache_hits": 0,
    "db_errors": 0,
    "places_api_calls": 0, "places_cache_hits": 0, "places_errors": 0,
    "geocode_api_calls": 0, "geocode_cache_hits": 0, "geocode_errors": 0,
    "binary_rejected": 0,
}

def inc(key: str, n: int = 1) -> None:
    _metrics[key] = _metrics.get(key, 0) + n


LIFESTYLE_TYPES: Dict[str, List[str]] = {
    "commute":     ["transit_station","subway_station","bus_station",
                    "train_station","light_rail_station"],
    "safety":      ["police","fire_station","hospital"],
    "education":   ["school","primary_school","secondary_school","university"],
    "greenery":    ["park","national_park","botanical_garden","playground"],
    "social_life": ["restaurant","bar","shopping_mall","movie_theater","cafe"],
}

PLACES_URL             = "https://places.googleapis.com/v1/places:searchNearby"
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

FAMILY_MIN_BHK: Dict[str, int] = {
    "1+": 1,
    "2+": 2,
    "3+": 3,
    "4+": 4,
}

# Injected by drishtii.py lifespan
redis_client: redis.Redis      = None
_db_pool: asyncpg.Pool         = None
_places_sem: asyncio.Semaphore = None


# ─────────────────────────────────────────────
#  Redis helpers
# ─────────────────────────────────────────────

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
            if not keys: return 0
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
    async def is_rate_limited(ip: str) -> bool:
        try:
            key    = f"decision_dna:rl:{ip}"
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


# ─────────────────────────────────────────────
#  Financial helpers
# ─────────────────────────────────────────────

def _compute_loan_multiplier() -> float:
    r = settings.LOAN_ANNUAL_RATE / 12
    n = settings.LOAN_TENURE_YEARS * 12
    return (1 - (1 + r) ** -n) / r if r else float(n)

_LOAN_MULTIPLIER: float = _compute_loan_multiplier()


def _salary_projection(salary: float) -> dict:
    return {
        "now":  round(salary,        2),
        "3yr":  round(salary * 1.25, 2),
        "5yr":  round(salary * 1.45, 2),
        "10yr": round(salary * 2.10, 2),
    }


def financial_analysis(
    salary: float, existing_emi: float, savings: float,
    property_price: Optional[float], expected_monthly_rent: Optional[float],
) -> dict:
    allowed_emi    = salary * settings.FOIR
    property_emi   = max(allowed_emi - existing_emi, 0.0)
    emi_overloaded = existing_emi >= allowed_emi

    if property_price and property_price > 0:
        max_property_price = property_price
        max_loan           = property_price * (1 - settings.DOWN_PAYMENT_RATIO)
        price_source       = "user_provided"
    elif emi_overloaded:
        max_loan           = 0.0
        max_property_price = 0.0
        price_source       = "ineligible_existing_emi_exceeds_foir"
    else:
        max_loan           = property_emi * _LOAN_MULTIPLIER
        max_property_price = max_loan / (1 - settings.DOWN_PAYMENT_RATIO)
        price_source       = "calculated_from_salary"

    down_payment     = max_property_price * settings.DOWN_PAYMENT_RATIO
    raw_stress       = (existing_emi / salary) * 100
    stress_ratio     = min(round(raw_stress, 2), 999.99)

    if emi_overloaded:       stress, status = "High",   "AVOID"
    elif stress_ratio < 30:  stress, status = "Low",    "SAFE"
    elif stress_ratio <= 40: stress, status = "Medium", "CAUTION"
    else:                    stress, status = "High",   "AVOID"

    savings_gap      = max(down_payment - savings, 0.0)
    savings_adequate = savings >= down_payment

    score = 100
    if emi_overloaded:               score -= 55
    elif stress == "Medium":         score -= 15
    elif stress == "High":           score -= 35
    if not savings_adequate:         score -= 10
    if existing_emi / salary > 0.20: score -= 10

    range_min = round(max_property_price * 0.80, 2)
    range_max = round(max_property_price * 1.20, 2)

    if expected_monthly_rent and expected_monthly_rent > 0:
        extra = round(property_emi - expected_monthly_rent, 2)
        buy_vs_rent = {
            "applicable":        True,
            "current_rent":      expected_monthly_rent,
            "property_emi":      round(property_emi, 2),
            "extra_cost_to_own": extra,
            "verdict": (
                f"Buying costs ₹{abs(extra):,.0f}/month "
                + ("more" if extra > 0 else "less")
                + " than renting currently"
            ),
        }
    else:
        buy_vs_rent = {"applicable": False, "reason": "expected_monthly_rent not provided"}

    return {
        "price_source":        price_source,
        "status":              status,
        "affordability_score": max(score, 0),
        "emi_stress":          stress,
        "stress_ratio_pct":    round(stress_ratio, 2),
        "max_property_price":  round(max_property_price, 2),
        "max_monthly_emi":     round(property_emi, 2),
        "down_payment_needed": round(down_payment, 2),
        "savings_adequate":    savings_adequate,
        "savings_gap":         round(savings_gap, 2),
        "price_range":         {"min": range_min, "max": range_max},
        "loan": {
            "max_amount":   round(max_loan, 2),
            "rate_pct":     round(settings.LOAN_ANNUAL_RATE * 100, 2),
            "tenure_years": settings.LOAN_TENURE_YEARS,
            "multiplier":   round(_LOAN_MULTIPLIER, 4),
        },
        "salary_growth": _salary_projection(salary),
        "buy_vs_rent":   buy_vs_rent,
    }


def rent_financial_analysis(
    monthly_income: float, existing_emi: float, rent_ratio: float,
    expected_monthly_rent: Optional[float],
) -> dict:
    max_rent_budget   = monthly_income * rent_ratio
    available_rent    = max(max_rent_budget - existing_emi, 0.0)
    total_obligations = existing_emi + available_rent
    foir_actual       = (total_obligations / monthly_income) * 100

    if foir_actual < 30:    stress, status = "Low",    "SAFE"
    elif foir_actual <= 40: stress, status = "Medium", "CAUTION"
    else:                   stress, status = "High",   "AVOID"

    score = 100
    if stress == "Medium":                                 score -= 15
    elif stress == "High":                                 score -= 35
    if existing_emi / monthly_income > 0.20:               score -= 10
    if existing_emi / monthly_income > 0.35:               score -= 10

    leftover             = monthly_income - total_obligations
    recommended_savings  = monthly_income * 0.20
    can_save_20pct       = leftover >= recommended_savings

    range_min = round(available_rent * 0.80, 2)
    range_max = round(available_rent * 1.20, 2)

    rent_vs_buy = {"applicable": False, "reason": "expected_monthly_rent not provided"}
    if expected_monthly_rent and expected_monthly_rent > 0:
        diff = round(available_rent - expected_monthly_rent, 2)
        rent_vs_buy = {
            "applicable":       True,
            "max_affordable":   round(available_rent, 2),
            "expected_rent":    expected_monthly_rent,
            "budget_surplus":   diff,
            "verdict": (
                f"Budget has ₹{abs(diff):,.0f}/month "
                + ("surplus" if diff >= 0 else "shortfall")
            ),
        }

    return {
        "status":              status,
        "affordability_score": max(score, 0),
        "emi_stress":          stress,
        "foir_actual_pct":     round(foir_actual, 2),
        "max_monthly_rent":    round(available_rent, 2),
        "leftover_after_rent": round(leftover, 2),
        "recommended_savings": round(recommended_savings, 2),
        "can_save_20pct":      can_save_20pct,
        "price_range":         {"min": range_min, "max": range_max},
        "income_projection":   _salary_projection(monthly_income),
        "rent_vs_buy":         rent_vs_buy,
    }


# ─────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────

async def _db_fetch(sql: str, *args) -> List[asyncpg.Record]:
    try:
        async with _db_pool.acquire() as conn:
            return await conn.fetch(sql, *args)
    except Exception as e:
        inc("db_errors")
        logger.error(f"DB error: {e}", exc_info=True)
        return []


def _row_to_dict(row: asyncpg.Record) -> Optional[dict]:
    try:
        raw = row["scraped_data"]
        if isinstance(raw, str):  return json.loads(raw)
        if isinstance(raw, dict): return raw
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────
#  Price parsers
# ─────────────────────────────────────────────

def _parse_99acres_price(price_str: str) -> Optional[float]:
    if not price_str or "on Request" in price_str:
        return None
    try:
        clean = price_str.replace("₹", "").replace(",", "")
        clean = re.sub(r"/month.*", "", clean, flags=re.IGNORECASE).strip()
        first = re.split(r"[\s\-–]", clean)[0]
        value = float(first)
        upper = price_str.upper()
        if "CR"  in upper:                                       return value * 10_000_000
        if "L"   in upper or "LAC" in upper or "LAKH" in upper: return value * 100_000
        return value
    except Exception as e:
        logger.warning(f"_parse_99acres_price failed for '{price_str}': {e}")
        return None


# ─────────────────────────────────────────────
#  DB Fetchers — BUY  (no LIMIT — fetch all matching rows)
# ─────────────────────────────────────────────

async def fetch_99acres_buy(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit, max_lit = int(min_p), int(max_p)
    logger.info(f"[DB][99acres][buy] city={city!r} range=[{min_lit}-{max_lit}]")
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
                WHEN scraped_data->>'priceRange' ILIKE '%L%'
                  THEN (REGEXP_REPLACE(SPLIT_PART(
                         REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                         '-',1),'[^0-9.]','','g'))::numeric * 100000
                ELSE NULL
              END BETWEEN $2 AND $3
    """
    rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
    out = []
    for i, row in enumerate(rows):
        try:
            item = _row_to_dict(row)
            if not item: continue
            price = _parse_99acres_price(item.get("priceRange", ""))
            if price is None: continue
            item["_source"]        = "99acres"
            item["_listing_type"]  = "buy"
            item["_price_numeric"] = price
            out.append(item)
        except Exception as e:
            logger.error(f"[99acres][buy] row {i}: {e}", exc_info=True)
    logger.info(f"[DB][99acres][buy] valid={len(out)}")
    return out


async def fetch_magicbricks_buy(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit, max_lit = int(min_p), int(max_p)
    logger.info(f"[DB][magicbricks][buy] city={city!r} range=[{min_lit}-{max_lit}]")
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
    """
    rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
    out = []
    for i, row in enumerate(rows):
        try:
            item = _row_to_dict(row)
            if not item: continue
            price = item.get("price")
            if price is None: continue
            item["_source"]        = "magicbricks"
            item["_listing_type"]  = "buy"
            item["_price_numeric"] = float(price)
            out.append(item)
        except Exception as e:
            logger.error(f"[magicbricks][buy] row {i}: {e}", exc_info=True)
    logger.info(f"[DB][magicbricks][buy] valid={len(out)}")
    return out


# ─────────────────────────────────────────────
#  DB Fetchers — RENT  (no LIMIT — fetch all matching rows)
# ─────────────────────────────────────────────

async def fetch_99acres_rent(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit, max_lit = int(min_p), int(max_p)
    logger.info(f"[DB][99acres][rent] city={city!r} range=[{min_lit}-{max_lit}]")
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
                  (REGEXP_REPLACE(
                     REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                     '[^0-9.]','','g'
                  ))::numeric
              END BETWEEN $2 AND $3
    """
    rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
    out = []
    for i, row in enumerate(rows):
        try:
            item = _row_to_dict(row)
            if not item: continue
            price = _parse_99acres_price(item.get("priceRange", ""))
            if price is None: continue
            item["_source"]        = "99acres"
            item["_listing_type"]  = "rent"
            item["_price_numeric"] = price
            out.append(item)
        except Exception as e:
            logger.error(f"[99acres][rent] row {i}: {e}", exc_info=True)
    logger.info(f"[DB][99acres][rent] valid={len(out)}")
    return out


async def fetch_magicbricks_rent(city: str, min_p: float, max_p: float) -> List[dict]:
    min_lit, max_lit = int(min_p), int(max_p)
    logger.info(f"[DB][magicbricks][rent] city={city!r} range=[{min_lit}-{max_lit}]")
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
    """
    rows = await _db_fetch(sql, f"%{city}%", min_lit, max_lit)
    out = []
    for i, row in enumerate(rows):
        try:
            item = _row_to_dict(row)
            if not item: continue
            price = item.get("price")
            if price is None: continue
            item["_source"]        = "magicbricks"
            item["_listing_type"]  = "rent"
            item["_price_numeric"] = float(price)
            out.append(item)
        except Exception as e:
            logger.error(f"[magicbricks][rent] row {i}: {e}", exc_info=True)
    logger.info(f"[DB][magicbricks][rent] valid={len(out)}")
    return out


# ─────────────────────────────────────────────
#  Aggregate fetchers with cache
# ─────────────────────────────────────────────

async def fetch_all_buy_properties(city: str, min_p: float, max_p: float) -> dict:
    ck = f"decision_dna:buy:props:{hashlib.md5(f'{city.lower()}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"
    cached = await RedisCache.get(ck)
    if cached is not None:
        total = sum(len(v) for v in cached.values() if isinstance(v, list))
        inc("property_cache_hits" if total > 0 else "property_empty_cache_hits")
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
    await RedisCache.set(ck, out, settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL)
    logger.info(f"[DB][buy] 99acres={len(out['99acres'])} magicbricks={len(out['magicbricks'])} total={total}")
    return out


async def fetch_all_rent_properties(city: str, min_p: float, max_p: float) -> dict:
    ck = f"decision_dna:rent:props:{hashlib.md5(f'{city.lower()}:{int(min_p)}:{int(max_p)}'.encode()).hexdigest()}"
    cached = await RedisCache.get(ck)
    if cached is not None:
        total = sum(len(v) for v in cached.values() if isinstance(v, list))
        inc("property_cache_hits" if total > 0 else "property_empty_cache_hits")
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
    await RedisCache.set(ck, out, settings.CACHE_TTL if total > 0 else settings.CACHE_EMPTY_TTL)
    logger.info(f"[DB][rent] 99acres={len(out['99acres'])} magicbricks={len(out['magicbricks'])} total={total}")
    return out


# ─────────────────────────────────────────────
#  Bedroom + family filter
# ─────────────────────────────────────────────

def _bedrooms(item: dict) -> Optional[int]:
    """
    Extract BHK count from a listing.

    Returns:
        int   — BHK count if confidently detected
        None  — BHK genuinely unknown (no BHK mention found anywhere)
        -1    — listing is NOT a BHK property at all (plot/land/studio etc.)
                callers treat -1 as a hard EXCLUDE when family_size filter is active
    """
    src = item.get("_source", "")

    def _find_bhk(txt: str) -> Optional[int]:
        if not txt:
            return None
        m = re.search(r"(\d+)\s*BHK", str(txt), re.IGNORECASE)
        return int(m.group(1)) if m else None

    def _is_non_bhk(txt: str) -> bool:
        if not txt:
            return False
        txt_lower = str(txt).lower()
        if re.search(r"\d[\d,]*\s*(sqft|sq\.ft|sqm|sq\.m|sq\s*yard)", txt_lower):
            return True
        return any(kw in txt_lower for kw in ("plot", "land", "studio", "office", "commercial", "shop"))

    try:
        # ── Step 1: always try the bedrooms field first for BOTH sources ─────
        raw = item.get("bedrooms")
        if raw is not None:
            raw_str = str(raw)
            # integer value e.g. 3  (magicbricks stores it as int)
            try:
                return int(raw)
            except (ValueError, TypeError):
                pass
            # string like "2 BHK", "3 BHK"  (99acres stores it as string)
            bhk = _find_bhk(raw_str)
            if bhk is not None:
                return bhk
            # non-BHK garbage like "30,000 sqft", "Plot/Land"
            if _is_non_bhk(raw_str):
                return -1

        # ── Step 2: fall back to other text fields ───────────────────────────
        if src == "magicbricks":
            fallback_fields = ("name", "description")
        else:  # 99acres
            fallback_fields = ("floorSize", "propertyType", "title", "description")

        for field in fallback_fields:
            txt = item.get(field, "") or ""
            if field == "floorSize" and _is_non_bhk(txt):
                return -1
            bhk = _find_bhk(txt)
            if bhk is not None:
                return bhk

        return None  # genuinely unknown

    except Exception:
        pass
    return None


def _passes_family(item: dict, fs: str) -> bool:
    """
    Filter properties by minimum BHK requirement.

    Logic:
      1+  → beds >= 1
      2+  → beds >= 2
      3+  → beds >= 3
      4+  → beds >= 4

    beds == -1  → non-BHK property (plot/land/studio) → always EXCLUDE
    beds is None → BHK unknown → let through (don't penalise unknown listings)
    """
    if not fs or fs not in FAMILY_MIN_BHK:
        return True

    beds = _bedrooms(item)

    # Non-BHK property (plot, land, etc.) — always exclude when family filter is set
    if beds == -1:
        logger.debug(
            f"[family_filter] EXCLUDED (non-BHK) — source={item.get('_source')} "
            f"price={item.get('_price_numeric')}"
        )
        return False

    # BHK genuinely unknown — let through
    if beds is None:
        return True

    min_required = FAMILY_MIN_BHK[fs]
    passes = beds >= min_required

    if not passes:
        logger.debug(
            f"[family_filter] EXCLUDED — required={min_required}+ BHK "
            f"actual={beds} BHK source={item.get('_source')} "
            f"price={item.get('_price_numeric')}"
        )

    return passes


# ─────────────────────────────────────────────
#  Coordinate extraction
# ─────────────────────────────────────────────

def _coords_direct(item: dict) -> Optional[tuple]:
    src = item.get("_source", "")
    try:
        if src == "magicbricks":
            loc = item.get("location", "") or ""
            if loc.strip():
                parts = loc.split(",")
                if len(parts) >= 2:
                    lat, lng = float(parts[0].strip()), float(parts[1].strip())
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        return lat, lng
        elif src == "99acres":
            lat = item.get("latitude") or item.get("lat")
            lng = item.get("longitude") or item.get("lng") or item.get("lon")
            if lat and lng:
                return float(lat), float(lng)
    except Exception:
        pass
    return None


def _extract_city_from_item(item: dict) -> Optional[str]:
    src = item.get("_source", "")
    if src == "magicbricks":
        return item.get("city_name") or None
    if src == "99acres":
        url = item.get("pageUrl", "") or item.get("url", "") or ""
        m = re.search(r"/(?:buy|rent)/([a-z\-]+?)(?:-ffid|/|\?|$)", url)
        if m: return m.group(1).replace("-", " ").title()
        title = item.get("title", "") or ""
        parts = title.split(",")
        if len(parts) >= 2: return parts[-1].strip()
    return None


def _extract_locality_from_item(item: dict) -> Optional[str]:
    src = item.get("_source", "")
    if src == "99acres":
        title = item.get("title", "") or ""
        m = re.search(r" in (.+)$", title, re.IGNORECASE)
        if m: return m.group(1).strip()
    if src == "magicbricks":
        name = item.get("name", "") or ""
        m = re.search(r"at\s+(.+)$", name, re.IGNORECASE)
        if m: return m.group(1).strip()
        url = item.get("url", "") or ""
        m2 = re.search(r"FOR-(?:Sale|Rent)-([A-Za-z\-]+)-in-", url, re.IGNORECASE)
        if m2: return m2.group(1).replace("-", " ").title()
        lm = item.get("landmark", "") or ""
        if lm: return lm
    return None


# ─────────────────────────────────────────────
#  Places / geocoding
# ─────────────────────────────────────────────

async def _resolve_coords_via_places(
    client: httpx.AsyncClient, item: dict
) -> Optional[tuple]:
    locality = _extract_locality_from_item(item)
    city     = _extract_city_from_item(item)
    if not locality and not city:
        return None
    query = ", ".join(p for p in [locality, city] if p)
    ck    = f"decision_dna:geocode:{hashlib.md5(query.lower().encode()).hexdigest()}"
    cached = await RedisCache.get(ck)
    if cached:
        inc("geocode_cache_hits")
        return tuple(cached)
    try:
        inc("geocode_api_calls")
        headers = {"Content-Type": "application/json", "X-Goog-FieldMask": "places.location,places.displayName"}
        resp = await client.post(
            f"{PLACES_TEXT_SEARCH_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
            json={"textQuery": query, "maxResultCount": 1},
            headers=headers, timeout=6.0,
        )
        resp.raise_for_status()
        places = resp.json().get("places", [])
        if places:
            loc = places[0].get("location", {})
            lat, lng = loc.get("latitude"), loc.get("longitude")
            if lat and lng:
                await RedisCache.set(ck, [lat, lng], settings.GEOCODE_CACHE_TTL)
                return float(lat), float(lng)
        if locality and city:
            resp2 = await client.post(
                f"{PLACES_TEXT_SEARCH_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
                json={"textQuery": city, "maxResultCount": 1},
                headers=headers, timeout=6.0,
            )
            resp2.raise_for_status()
            places2 = resp2.json().get("places", [])
            if places2:
                loc2 = places2[0].get("location", {})
                lat2, lng2 = loc2.get("latitude"), loc2.get("longitude")
                if lat2 and lng2:
                    await RedisCache.set(ck, [lat2, lng2], settings.GEOCODE_CACHE_TTL)
                    return float(lat2), float(lng2)
    except Exception as e:
        inc("geocode_errors")
        logger.warning(f"[PlacesSearch] FAILED for {query!r}: {e}")
    return None


def _places_ck(lat: float, lng: float, cat: str) -> str:
    return f"decision_dna:places:v1:{hashlib.md5(f'{lat:.5f}:{lng:.5f}:{cat}'.encode()).hexdigest()}"


async def _places_count(client: httpx.AsyncClient, lat: float, lng: float, cat: str) -> int:
    ck = _places_ck(lat, lng, cat)
    cached = await RedisCache.get(ck)
    if cached is not None:
        inc("places_cache_hits"); return int(cached)
    types = LIFESTYLE_TYPES.get(cat, [])
    if not types: return 0
    payload = {
        "includedTypes": types,
        "maxResultCount": settings.PLACES_MAX_RESULTS,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": settings.PLACES_RADIUS_M,
            }
        },
    }
    headers = {"Content-Type": "application/json", "X-Goog-FieldMask": "places.id"}
    try:
        async with _places_sem:
            inc("places_api_calls")
            resp = await client.post(
                f"{PLACES_URL}?key={settings.GOOGLE_PLACES_API_KEY}",
                json=payload, headers=headers, timeout=settings.PLACES_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            body  = resp.json()
            count = len(body.get("places", []))
            if "error" in body:
                logger.error(f"[Places][ERROR] cat={cat}: {body['error']}")
    except Exception as e:
        inc("places_errors")
        logger.warning(f"[Places][FAILED] cat={cat} ({lat:.4f},{lng:.4f}): {e}")
        return 0
    await RedisCache.set(ck, count, settings.PLACES_CACHE_TTL)
    return count


async def _dna_score(
    client: httpx.AsyncClient, lat: float, lng: float, weights: Dict[str, float],
) -> Dict[str, Any]:
    active      = [c for c, w in weights.items() if w > 0]
    counts_list = await asyncio.gather(
        *[_places_count(client, lat, lng, c) for c in active],
        return_exceptions=True,
    )
    count_map = {c: (v if isinstance(v, int) else 0) for c, v in zip(active, counts_list)}
    total = 0.0; rejected = False; breakdown: Dict[str, Any] = {}
    for cat, weight in weights.items():
        count       = count_map.get(cat, 0)
        normalised  = min(count / settings.PLACES_SATURATION, 1.0)
        ws          = round(normalised * weight, 3)
        total      += ws
        binary_fail = (weight >= settings.BINARY_REJECT_THRESHOLD and count == 0)
        if binary_fail:
            rejected = True; inc("binary_rejected")
        breakdown[cat] = {
            "places_found_in_5km": count,
            "binary_check":        "FAIL" if binary_fail else "PASS",
            "normalised":          round(normalised, 3),
            "weight":              weight,
            "score_contribution":  ws,
        }
    max_possible = sum(weights.values())
    match_pct    = round((total / max_possible) * 100, 2) if max_possible > 0 else 0
    return {
        "dna_score":    round(total, 2),
        "max_possible": max_possible,
        "match_pct":    match_pct,
        "rejected":     rejected,
        "breakdown":    breakdown,
    }


# ─────────────────────────────────────────────
#  Score & rank (shared by buy and rent)
# ─────────────────────────────────────────────

async def score_and_rank(
    props_by_src: Dict[str, List[dict]],
    weights: Dict[str, float],
    family_size: Optional[str],
    page: int, page_size: int,
) -> dict:
    all_props: List[dict] = []
    for items in props_by_src.values():
        all_props.extend(items)

    # Apply family/BHK filter
    if family_size:
        before = len(all_props)
        all_props = [p for p in all_props if _passes_family(p, family_size)]
        after  = len(all_props)
        logger.info(
            f"[family_filter] family_size={family_size} "
            f"min_bhk={FAMILY_MIN_BHK.get(family_size, '?')}+ "
            f"before={before} after={after} excluded={before-after}"
        )

    with_coords: List[tuple] = []
    needs_geocode: List[dict] = []

    for prop in all_props:
        c = _coords_direct(prop)
        if c:
            with_coords.append((prop, c[0], c[1]))
        else:
            needs_geocode.append(prop)

    if needs_geocode:
        async with httpx.AsyncClient() as http_client:
            gc_results = await asyncio.gather(
                *[_resolve_coords_via_places(http_client, p) for p in needs_geocode],
                return_exceptions=True,
            )
        no_coords = []
        for prop, gc in zip(needs_geocode, gc_results):
            if isinstance(gc, tuple) and len(gc) == 2:
                with_coords.append((prop, gc[0], gc[1]))
            else:
                no_coords.append(prop)
    else:
        no_coords = []

    scored: List[dict] = []
    if with_coords:
        async with httpx.AsyncClient() as http_client:
            dna_results = await asyncio.gather(
                *[_dna_score(http_client, lat, lng, weights) for (_, lat, lng) in with_coords],
                return_exceptions=True,
            )
        for (prop, lat, lng), dna in zip(with_coords, dna_results):
            if isinstance(dna, Exception):
                dna = {"dna_score": 0.0, "rejected": False, "breakdown": {}}
            if not dna.get("rejected", False):
                scored.append({**prop, "dna": dna})

    for prop in no_coords:
        scored.append({
            **prop,
            "dna": {
                "dna_score":    0.0,
                "max_possible": sum(weights.values()),
                "match_pct":    0.0,
                "rejected":     False,
                "breakdown":    {},
                "note":         "coordinates_unavailable",
            },
        })

    scored.sort(key=lambda p: (-p["dna"]["dna_score"], p.get("_price_numeric", float("inf"))))
    total      = len(scored)
    start      = (page - 1) * page_size
    page_items = scored[start: start + page_size]
    return {
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": math.ceil(total / page_size) if total > 0 else 0,
        "items":       page_items,
    }


# ─────────────────────────────────────────────
#  Pydantic models
# ─────────────────────────────────────────────

class LifestyleInput(BaseModel):
    commute:     float = Field(5.0, ge=0, le=10)
    safety:      float = Field(5.0, ge=0, le=10)
    education:   float = Field(5.0, ge=0, le=10)
    greenery:    float = Field(5.0, ge=0, le=10)
    social_life: float = Field(5.0, ge=0, le=10)


class BuyReportInput(BaseModel):
    salary:                float          = Field(..., gt=0, le=settings.MAX_SALARY)
    existing_emi:          float          = Field(..., ge=0, le=settings.MAX_EXISTING_EMI)
    savings:               float          = Field(..., ge=0, le=settings.MAX_SAVINGS)
    city:                  str            = Field(..., min_length=2, max_length=60)
    family_size:           Optional[str]  = Field(None, description="1+, 2+, 3+, or 4+")
    property_price:        Optional[float]= Field(None, ge=0)
    expected_monthly_rent: Optional[float]= Field(None, ge=0)
    lifestyle:             LifestyleInput = Field(default_factory=LifestyleInput)

    @validator("city")
    def sanitize_city(cls, v):
        v = v.strip()
        if not re.match(r"^[a-zA-Z\s\-]+$", v):
            raise ValueError("City must contain only letters, spaces, or hyphens")
        return v.title()

    @validator("family_size")
    def validate_family_size(cls, v):
        if v is not None and v not in FAMILY_MIN_BHK:
            raise ValueError(f"family_size must be one of {list(FAMILY_MIN_BHK)}")
        return v


class RentReportInput(BaseModel):
    monthly_income:        float          = Field(..., gt=0, le=settings.MAX_SALARY,       description="Monthly net income in ₹")
    existing_emi:          float          = Field(..., ge=0, le=settings.MAX_EXISTING_EMI, description="Existing monthly EMIs in ₹")
    city:                  str            = Field(..., min_length=2, max_length=60)
    rent_ratio:            float          = Field(0.30, ge=0.20, le=0.40,                  description="Max % of income for rent: 0.20/0.30/0.40")
    family_size:           Optional[str]  = Field(None, description="1+, 2+, 3+, or 4+")
    expected_monthly_rent: Optional[float]= Field(None, ge=0)
    lifestyle:             LifestyleInput = Field(default_factory=LifestyleInput)

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

    @validator("family_size")
    def validate_family_size(cls, v):
        if v is not None and v not in FAMILY_MIN_BHK:
            raise ValueError(f"family_size must be one of {list(FAMILY_MIN_BHK)}")
        return v


# ─────────────────────────────────────────────
#  Routers
# ─────────────────────────────────────────────

buy_router  = APIRouter(prefix="/decision/buy",  tags=["Decision DNA Buy"])
rent_router = APIRouter(prefix="/decision/rent", tags=["Decision DNA Rent"])
router      = buy_router   # backward compat alias


# ── BUY endpoints ────────────────────────────

@buy_router.post("/Decision_report")
async def generate_buy_report(
    data:      BuyReportInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    finance      = financial_analysis(
        data.salary, data.existing_emi, data.savings,
        data.property_price, data.expected_monthly_rent,
    )
    props_by_src = await fetch_all_buy_properties(
        data.city, finance["price_range"]["min"], finance["price_range"]["max"],
    )
    weights = data.lifestyle.dict()
    ranked  = await score_and_rank(props_by_src, weights, data.family_size, page, page_size)
    return {
        "listing_type":       "buy",
        "financial_summary":  finance,
        "search_summary": {
            "city":              data.city,
            "family_size":       data.family_size,
            "min_bhk_required":  FAMILY_MIN_BHK.get(data.family_size) if data.family_size else None,
            "price_range":       finance["price_range"],
            "source_raw_counts": {src: len(items) for src, items in props_by_src.items()},
        },
        "lifestyle_weights": weights,
        "properties":        ranked,
    }


@buy_router.get("/health/detailed")
async def buy_health():
    redis_ok = db_ok = False
    try: await redis_client.ping(); redis_ok = True
    except Exception: pass
    try:
        async with _db_pool.acquire() as c: await c.execute("SELECT 1"); db_ok = True
    except Exception: pass
    overall = "healthy" if (redis_ok and db_ok) else "degraded"
    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={"status": overall, "redis": "connected" if redis_ok else "disconnected",
                 "database": "connected" if db_ok else "disconnected"},
    )


@buy_router.get("/metrics")
async def buy_metrics():
    return {"metrics": _metrics, "timestamp": time.time()}


@buy_router.get("/clear-cache")
async def buy_clear_cache():
    deleted = 0
    for prefix in ("decision_dna:buy:props:", "decision_dna:places:", "decision_dna:geocode:", "decision_dna:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys: deleted += await RedisCache.delete(*keys)
    return {"status": "cache cleared", "keys_deleted": deleted}


# ── RENT endpoints ───────────────────────────

@rent_router.post("/Decision_report")
async def generate_rent_report(
    data:      RentReportInput,
    page:      int = Query(1,                          ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
):
    finance      = rent_financial_analysis(
        data.monthly_income, data.existing_emi,
        data.rent_ratio, data.expected_monthly_rent,
    )
    props_by_src = await fetch_all_rent_properties(
        data.city, finance["price_range"]["min"], finance["price_range"]["max"],
    )
    weights = data.lifestyle.dict()
    ranked  = await score_and_rank(props_by_src, weights, data.family_size, page, page_size)
    return {
        "listing_type":       "rent",
        "financial_summary":  finance,
        "search_summary": {
            "city":              data.city,
            "family_size":       data.family_size,
            "min_bhk_required":  FAMILY_MIN_BHK.get(data.family_size) if data.family_size else None,
            "price_range":       finance["price_range"],
            "source_raw_counts": {src: len(items) for src, items in props_by_src.items()},
        },
        "lifestyle_weights": weights,
        "properties":        ranked,
    }


@rent_router.get("/health/detailed")
async def rent_health():
    redis_ok = db_ok = False
    try: await redis_client.ping(); redis_ok = True
    except Exception: pass
    try:
        async with _db_pool.acquire() as c: await c.execute("SELECT 1"); db_ok = True
    except Exception: pass
    overall = "healthy" if (redis_ok and db_ok) else "degraded"
    return JSONResponse(
        status_code=200 if overall == "healthy" else 503,
        content={"status": overall, "redis": "connected" if redis_ok else "disconnected",
                 "database": "connected" if db_ok else "disconnected"},
    )


@rent_router.get("/metrics")
async def rent_metrics():
    return {"metrics": _metrics, "timestamp": time.time()}


@rent_router.get("/clear-cache")
async def rent_clear_cache():
    deleted = 0
    for prefix in ("decision_dna:rent:props:", "decision_dna:places:", "decision_dna:geocode:", "decision_dna:rl:"):
        keys = await RedisCache.keys_by_prefix(prefix)
        if keys: deleted += await RedisCache.delete(*keys)
    return {"status": "cache cleared", "keys_deleted": deleted}
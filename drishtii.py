
# import asyncio
# import logging
# import os
# from contextlib import asynccontextmanager

# import asyncpg
# import redis.asyncio as redis
# import uvicorn
# from dotenv import load_dotenv
# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.gzip import GZipMiddleware
# from fastapi.responses import JSONResponse


# import app.Affordibility          as afford_mod
# import app.Decision_DNA           as decision_mod
# import app.Vedic_Astro              as astrology_mod
# import app.Business_feasibility as biz_mod

# from app.Affordibility           import buy_router  as afford_buy_router
# from app.Affordibility           import rent_router as afford_rent_router
# from app.Decision_DNA            import buy_router  as decision_buy_router
# from app.Decision_DNA            import rent_router as decision_rent_router
# from app.Business_feasibility import rent_router as biz_rent_router
# from app.Business_feasibility import buy_router  as biz_buy_router

# from app.Vedic_Astro import (
#     prediction_router,
#     horoscope_router,
#     matching_router,
#     panchang_router,
#     dosha_router,
#     dashas_router,
#     extended_router,
#     utilities_router,
#     health_router as astro_health_router,
# )

# load_dotenv()

# logger = logging.getLogger("drishtii")

# REDIS_URL             = os.getenv("REDIS_URL",              "redis://localhost:6379/0")
# REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
# DATABASE_URL          = os.getenv("DATABASE_URL",           "")
# DB_POOL_MIN_SIZE      = int(os.getenv("DB_POOL_MIN_SIZE",   "2"))
# DB_POOL_MAX_SIZE      = int(os.getenv("DB_POOL_MAX_SIZE",   "10"))
# DB_COMMAND_TIMEOUT    = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
# ALLOWED_ORIGINS       = os.getenv("ALLOWED_ORIGINS",        "*").split(",")
# PLACES_MAX_CONCURRENT = int(os.getenv("PLACES_MAX_CONCURRENT", "10"))
# GEMINI_MAX_CONCURRENT = int(os.getenv("GEMINI_MAX_CONCURRENT", "5"))



# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logger.info("Drishtii starting up — initialising shared resources ...")

   
#     shared_redis = redis.from_url(
#         REDIS_URL,
#         decode_responses=True,
#         max_connections=REDIS_MAX_CONNECTIONS,
#     )
#     try:
#         await shared_redis.ping()
#         logger.info("Redis connected and healthy")
#     except Exception as e:
#         logger.critical(f"Redis unreachable at startup: {e}")
#         raise

    
#     db_pool = await asyncpg.create_pool(
#         dsn=DATABASE_URL,
#         min_size=DB_POOL_MIN_SIZE,
#         max_size=DB_POOL_MAX_SIZE,
#         command_timeout=DB_COMMAND_TIMEOUT,
#     )
#     logger.info("Postgres pool ready")

    
#     afford_mod.redis_client       = shared_redis
#     afford_mod._db_pool           = db_pool
#     afford_mod._gemini_semaphore  = asyncio.Semaphore(GEMINI_MAX_CONCURRENT)

  
#     decision_mod.redis_client     = shared_redis
#     decision_mod._db_pool         = db_pool
#     decision_mod._places_sem      = asyncio.Semaphore(PLACES_MAX_CONCURRENT)

    
#     astrology_mod._redis_client   = shared_redis
#     if not astrology_mod.settings.VEDIC_API_KEY:
#         logger.warning("VEDIC_API_KEY is not set — astrology endpoints will fail!")
#     else:
#         logger.info("VedicAstroAPI key loaded successfully")

    
#     biz_mod.redis_client          = shared_redis
#     biz_mod._db_pool              = db_pool

#     logger.info("All modules initialised successfully")

#     yield  

   
#     logger.info("Drishtii shutting down ...")
#     await shared_redis.aclose()
#     await db_pool.close()
#     logger.info("All connections closed cleanly")



# app = FastAPI(
#     title       = "Drishtii API",
#     version     = "1.0.0",
#     lifespan    = lifespan,
#     docs_url    = "/docs",
#     redoc_url   = "/redoc",
# )

# app.add_middleware(GZipMiddleware, minimum_size=1000)
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins     = ALLOWED_ORIGINS,
#     allow_credentials = True,
#     allow_methods     = ["*"],
#     allow_headers     = ["*"],
# )


# app.include_router(afford_buy_router)
# app.include_router(afford_rent_router)


# app.include_router(decision_buy_router)
# app.include_router(decision_rent_router)


# app.include_router(biz_rent_router)
# app.include_router(biz_buy_router)


# app.include_router(prediction_router)
# app.include_router(horoscope_router)
# app.include_router(matching_router)
# app.include_router(panchang_router)
# app.include_router(dosha_router)
# app.include_router(dashas_router)
# app.include_router(extended_router)
# app.include_router(utilities_router)
# app.include_router(astro_health_router)



# @app.get("/", tags=["Health"])
# async def root():
#     return {
#         "status":  "running",
#         "version": "1.0.0",
#         "service": "Drishtii API",
#         "routes": {
            
#             "affordability_buy":       "/affordability/buy/report",
#             "affordability_rent":      "/affordability/rent/report",
#             "afford_buy_health":       "/affordability/buy/health/detailed",
#             "afford_rent_health":      "/affordability/rent/health/detailed",
#             "afford_buy_metrics":      "/affordability/buy/metrics",
#             "afford_rent_metrics":     "/affordability/rent/metrics",

            
#             "decision_dna_buy":        "/decision/buy/Decision_report",
#             "decision_dna_rent":       "/decision/rent/Decision_report",
#             "decision_buy_health":     "/decision/buy/health/detailed",
#             "decision_rent_health":    "/decision/rent/health/detailed",

            
#             "business_rent":           "/businessman/rent/feasibility",
#             "business_buy":            "/businessman/buy/feasibility",
#             "business_rent_health":    "/businessman/rent/health/detailed",
#             "business_buy_health":     "/businessman/buy/health/detailed",
#             "business_rent_metrics":   "/businessman/rent/metrics",
#             "business_buy_metrics":    "/businessman/buy/metrics",

            
#             "astro_prediction":        "/api/prediction/{endpoint}",
#             "astro_horoscope":         "/api/horoscope/{endpoint}",
#             "astro_matching":          "/api/matching/{endpoint}",
#             "astro_panchang":          "/api/panchang/{endpoint}",
#             "astro_dosha":             "/api/dosha/{endpoint}",
#             "astro_dashas":            "/api/dashas/{endpoint}",
#             "astro_extended":          "/api/extended/{endpoint}",
#             "astro_utilities":         "/api/utilities/{endpoint}",
#             "astro_health":            "/api/astro/health",
#             "astro_metrics":           "/api/astro/metrics",
#             "astro_clear_cache":       "/api/astro/clear-cache",

            
#             "docs":   "/docs",
#             "redoc":  "/redoc",
#         },
#     }


# if __name__ == "__main__":
#     uvicorn.run("drishtii:app", host="0.0.0.0", port=8000, reload=True)



# drishtii.py - UPDATED

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as redis
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

import app.Affordibility as afford_mod
import app.Decision_DNA as decision_mod
import app.Vedic_Astro as astrology_mod
import app.Business_feasibility as biz_mod

from app.Affordibility import buy_router as afford_buy_router
from app.Affordibility import rent_router as afford_rent_router
from app.Decision_DNA import buy_router as decision_buy_router
from app.Decision_DNA import rent_router as decision_rent_router
from app.Business_feasibility import rent_router as biz_rent_router
from app.Business_feasibility import buy_router as biz_buy_router

from app.Vedic_Astro import (
    prediction_router,
    horoscope_router,
    matching_router,
    panchang_router,
    dosha_router,
    dashas_router,
    extended_router,
    utilities_router,
    health_router as astro_health_router,
)

# ✅ ADD THESE IMPORTS
from app.Astrology import drishtii_router, set_engine
from app.Astrology.Engine import DrishtiiEngine

load_dotenv()

logger = logging.getLogger("drishtii")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
DB_COMMAND_TIMEOUT = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
PLACES_MAX_CONCURRENT = int(os.getenv("PLACES_MAX_CONCURRENT", "10"))
GEMINI_MAX_CONCURRENT = int(os.getenv("GEMINI_MAX_CONCURRENT", "5"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Drishtii starting up — initialising shared resources ...")

    shared_redis = redis.from_url(
        REDIS_URL,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    try:
        await shared_redis.ping()
        logger.info("✅ Redis connected and healthy")
    except Exception as e:
        logger.critical(f"❌ Redis unreachable at startup: {e}")
        raise

    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        command_timeout=DB_COMMAND_TIMEOUT,
    )
    logger.info("✅ Postgres pool ready")

    afford_mod.redis_client = shared_redis
    afford_mod._db_pool = db_pool
    afford_mod._gemini_semaphore = asyncio.Semaphore(GEMINI_MAX_CONCURRENT)

    decision_mod.redis_client = shared_redis
    decision_mod._db_pool = db_pool
    decision_mod._places_sem = asyncio.Semaphore(PLACES_MAX_CONCURRENT)

    astrology_mod._redis_client = shared_redis
    if not astrology_mod.settings.VEDIC_API_KEY:
        logger.warning("⚠️ VEDIC_API_KEY is not set — astrology endpoints will fail!")
    else:
        logger.info("✅ VedicAstroAPI key loaded")

    biz_mod.redis_client = shared_redis
    biz_mod._db_pool = db_pool

    # ✅ ADD: Initialize DRISHTII Engine
    drishtii_engine = DrishtiiEngine(redis_client=shared_redis)
    set_engine(drishtii_engine)
    logger.info("✅ DRISHTII Engine initialized")

    logger.info("✅ All modules initialised successfully")

    yield

    logger.info("Drishtii shutting down ...")
    await shared_redis.aclose()
    await db_pool.close()
    logger.info("✅ All connections closed cleanly")


app = FastAPI(
    title="Drishtii API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Affordability routers
app.include_router(afford_buy_router)
app.include_router(afford_rent_router)

# Decision DNA routers
app.include_router(decision_buy_router)
app.include_router(decision_rent_router)

# Business routers
app.include_router(biz_rent_router)
app.include_router(biz_buy_router)

# Vedic Astro routers
app.include_router(prediction_router)
app.include_router(horoscope_router)
app.include_router(matching_router)
app.include_router(panchang_router)
app.include_router(dosha_router)
app.include_router(dashas_router)
app.include_router(extended_router)
app.include_router(utilities_router)
app.include_router(astro_health_router)

# ✅ ADD: DRISHTII router
app.include_router(drishtii_router)


@app.get("/", tags=["Health"])
async def root():
    return {
        "status": "running",
        "version": "1.0.0",
        "service": "Drishtii API",
        "routes": {
            "affordability_buy": "/affordability/buy/report",
            "affordability_rent": "/affordability/rent/report",
            "decision_dna_buy": "/decision/buy/Decision_report",
            "decision_dna_rent": "/decision/rent/Decision_report",
            "business_rent": "/businessman/rent/feasibility",
            "business_buy": "/businessman/buy/feasibility",
            "drishtii_analyze": "/api/drishtii/analyze",  
            "astro_prediction": "/api/prediction/{endpoint}",
            "astro_horoscope": "/api/horoscope/{endpoint}",
            "astro_matching": "/api/matching/{endpoint}",
            "astro_panchang": "/api/panchang/{endpoint}",
            "astro_dosha": "/api/dosha/{endpoint}",
            "astro_dashas": "/api/dashas/{endpoint}",
            "astro_extended": "/api/extended/{endpoint}",
            "astro_utilities": "/api/utilities/{endpoint}",
            "astro_health": "/api/astro/health",            
            "docs": "/docs",
            "redoc": "/redoc",
        },
    }


if __name__ == "__main__":
    uvicorn.run("drishtii:app", host="0.0.0.0", port=8000, reload=True)
# app/Astrology/Router.py
# ════════════════════════════════════════════════════════════════════
#  FastAPI router — POST /api/drishtii/analyze  (PRODUCTION v2)
#
#  ENHANCEMENTS vs v1:
#   1. Structured JSON error envelope for CATEGORY_MISMATCH (HTTP 422)
#   2. Required-params validation before engine call
#   3. Chart-hash logged for cache debugging
#   4. /categories returns corrected normalisation note
#   5. All error paths return machine-parseable JSON bodies
# ════════════════════════════════════════════════════════════════════

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from .model import (
    DrishtiiRequest,
    CATEGORY_LABELS,
    CATEGORY_PARAM_SHAPES,
    CATEGORY_API_MAP,
)
from .Engine import DrishtiiEngine

logger = logging.getLogger("drishtii.router")

drishtii_router = APIRouter(prefix="/api/drishtii", tags=["DRISHTII Analysis"])

_engine: DrishtiiEngine | None = None


def set_engine(engine: DrishtiiEngine) -> None:
    global _engine
    _engine = engine
    logger.info("[ROUTER] ✅ DrishtiiEngine injected and ready")


# ─────────────────────────────────────────────────────────────────
# POST /api/drishtii/analyze
# ─────────────────────────────────────────────────────────────────

@drishtii_router.post("/analyze")
async def analyze(request: DrishtiiRequest):
    """
    Drishtii analysis endpoint — simplified 7-category interface.

    ⚠️  Send ONLY: category, objective, language, target_date, params{}
    DO NOT send group or api — these are auto-resolved from category.

    ────────────────────────────────────────────────────────────────
    EXAMPLE — Financial:
    {
      "category": "financial",
      "objective": "Should I invest in stocks today?",
      "language": "hi",
      "target_date": "26/3/2026",
      "params": {
        "dob": "15/8/1990", "tob": "10:30",
        "lat": 26.85, "lon": 75.79, "tz": 5.5
      }
    }

    EXAMPLE — Marriage:
    {
      "category": "marriage",
      "objective": "Is this match suitable for us?",
      "language": "hi",
      "target_date": "26/3/2026",
      "params": {
        "m_dob": "15/8/1990", "m_tob": "10:30",
        "m_lat": 26.85, "m_lon": 75.79, "m_tz": 5.5,
        "f_dob": "20/5/1992", "f_tob": "08:00",
        "f_lat": 28.61, "f_lon": 77.20, "f_tz": 5.5
      }
    }
    ────────────────────────────────────────────────────────────────
    """

    # ── 1. Required params check ─────────────────────────────────
    param_error = request.validate_required_params()
    if param_error:
        logger.warning(
            f"[ROUTER] ⛔ Missing params | category={request.category} | {param_error}"
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error_code":  "MISSING_PARAMS",
                "category":    request.category,
                "message":     param_error,
                "required":    CATEGORY_PARAM_SHAPES.get(request.category, []),
            },
        )

    # ── 2. Category ↔ objective mismatch guard ───────────────────
    mismatch = request.check_objective_relevance()
    if mismatch:
        logger.warning(
            f"[ROUTER] ⛔ Category-objective mismatch | "
            f"category={request.category} | "
            f"detected_topic={mismatch['detected_topic']} | "
            f"correct_category={mismatch['correct_category']}"
        )
        # HTTP 422 Unprocessable Entity — data is syntactically valid
        # but semantically wrong.  Machine-parseable envelope lets the
        # frontend show a friendly redirect message.
        return JSONResponse(
            status_code=422,
            content={
                "status":            422,
                "error_code":        mismatch["error_code"],
                "detected_topic":    mismatch["detected_topic"],
                "correct_category":  mismatch["correct_category"],
                "correct_label":     mismatch["correct_label"],
                "selected_category": mismatch["selected_category"],
                "selected_label":    mismatch["selected_label"],
                "message":           mismatch["message"],
                "action_required":   (
                    f"Please change your category from "
                    f"'{mismatch['selected_category']}' to "
                    f"'{mismatch['correct_category']}' and resubmit."
                ),
            },
        )

    # ── 3. Log enriched request context ─────────────────────────
    chart_hash = request.get_chart_hash()
    logger.info(
        f"\n{'─'*60}\n"
        f"[ROUTER] ▶ POST /api/drishtii/analyze\n"
        f"  category    : {request.category} ({CATEGORY_LABELS.get(request.category, '')})\n"
        f"  group/api   : {request.group}/{request.api}  [auto-resolved]\n"
        f"  language    : {request.language}\n"
        f"  target_date : {request.target_date or 'today'}\n"
        f"  chart_hash  : {chart_hash}  [deterministic]\n"
        f"  norm_params : {request.get_normalized_params()}\n"
        f"{'─'*60}"
    )

    # ── 4. Engine guard ──────────────────────────────────────────
    if _engine is None:
        logger.error("[ROUTER] ❌ Engine not initialized")
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "ENGINE_NOT_READY",
                "message":    "Drishtii engine not initialized. Try again in a moment.",
            },
        )

    # ── 5. Run analysis ──────────────────────────────────────────
    try:
        result = await _engine.analyze(request)
        logger.info(
            f"[ROUTER] ✅ Response ready — "
            f"verdict={result.get('verdict')} "
            f"score={result.get('total_score')}/40 "
            f"time={result.get('execution_time')}s"
        )
        return result

    except ValueError as e:
        logger.warning(f"[ROUTER] ⚠️ Validation error: {e}")
        raise HTTPException(
            status_code=400,
            detail={"error_code": "VALIDATION_ERROR", "message": str(e)},
        )

    except Exception as e:
        err_msg = str(e)
        logger.error(f"[ROUTER] ❌ Analysis failed: {err_msg}", exc_info=True)

        if "503" in err_msg or "unavailable" in err_msg.lower():
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "VEDIC_API_UNAVAILABLE",
                    "message": (
                        "VedicAstroAPI is temporarily unavailable. "
                        "Please try again in 10–30 seconds."
                    ),
                    "technical": err_msg,
                },
            )

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ANALYSIS_FAILED",
                "message":    f"Analysis failed: {err_msg}",
            },
        )


# ─────────────────────────────────────────────────────────────────
# GET /api/drishtii/categories
# ─────────────────────────────────────────────────────────────────

@drishtii_router.get("/categories")
async def list_categories():
    """Returns all 7 supported categories with param shapes and example requests."""
    examples = {
        "financial": {
            "label":            CATEGORY_LABELS["financial"],
            "description":      "Investment, stocks, loans, wealth, savings decisions",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["financial"],
            "forbidden_topics": "marriage, surgery, hospital, court, legal",
            "example_request":  {
                "category":    "financial",
                "objective":   "Should I invest in mutual funds this month?",
                "language":    "hi",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "15/8/1990", "tob": "10:30",
                    "lat": 26.85, "lon": 75.79, "tz": 5.5,
                },
            },
        },
        "business": {
            "label":            CATEGORY_LABELS["business"],
            "description":      "Business decisions, tender filing, contracts, partnerships",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["business"],
            "forbidden_topics": "marriage, surgery, hospital, court, legal",
            "example_request":  {
                "category":    "business",
                "objective":   "Should I file this government tender today?",
                "language":    "en",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "10/5/1985", "tob": "07:15",
                    "lat": 28.61, "lon": 77.20, "tz": 5.5,
                },
            },
        },
        "career": {
            "label":            CATEGORY_LABELS["career"],
            "description":      "Job change, promotion, interview, resignation",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["career"],
            "forbidden_topics": "marriage, surgery, hospital",
            "example_request":  {
                "category":    "career",
                "objective":   "Is this a good time to switch jobs?",
                "language":    "en",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "3/11/1992", "tob": "14:45",
                    "lat": 19.07, "lon": 72.87, "tz": 5.5,
                },
            },
        },
        "marriage": {
            "label":            CATEGORY_LABELS["marriage"],
            "description":      "Marriage compatibility (kundali matching), engagement",
            "apis_used":        "matching/ashtakoot",
            "params_required":  CATEGORY_PARAM_SHAPES["marriage"],
            "note":             "Provide BOTH male (m_*) and female (f_*) birth details",
            "forbidden_topics": (
                "financial, business, career, legal, travel, health — "
                "marriage category handles ONLY compatibility questions"
            ),
            "example_request":  {
                "category":    "marriage",
                "objective":   "Is this kundali match suitable for marriage?",
                "language":    "hi",
                "target_date": "26/3/2026",
                "params": {
                    "m_dob": "15/8/1990", "m_tob": "10:30",
                    "m_lat": 26.85, "m_lon": 75.79, "m_tz": 5.5,
                    "f_dob": "20/5/1992", "f_tob": "08:00",
                    "f_lat": 28.61, "f_lon": 77.20, "f_tz": 5.5,
                },
            },
        },
        "legal": {
            "label":            CATEGORY_LABELS["legal"],
            "description":      "Court hearings, legal disputes, case filing, police matters",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["legal"],
            "forbidden_topics": "marriage compatibility, investment, stocks",
            "example_request":  {
                "category":    "legal",
                "objective":   "Should I file the court case this week?",
                "language":    "en",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "7/2/1978", "tob": "09:00",
                    "lat": 23.02, "lon": 72.57, "tz": 5.5,
                },
            },
        },
        "health": {
            "label":            CATEGORY_LABELS["health"],
            "description":      "Surgery timing, treatment decisions, medical procedures",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["health"],
            "forbidden_topics": "investment, business, marriage, court, legal",
            "example_request":  {
                "category":    "health",
                "objective":   "Is this a good time for my knee surgery?",
                "language":    "en",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "22/9/1965", "tob": "06:30",
                    "lat": 13.08, "lon": 80.27, "tz": 5.5,
                },
            },
        },
        "travel": {
            "label":            CATEGORY_LABELS["travel"],
            "description":      "Visa applications, NRI decisions, foreign travel, migration",
            "apis_used":        "horoscope/planet-details + dashas/current-mahadasha-full (auto)",
            "params_required":  CATEGORY_PARAM_SHAPES["travel"],
            "forbidden_topics": "marriage, surgery, hospital",
            "example_request":  {
                "category":    "travel",
                "objective":   "Should I apply for a US visa this month?",
                "language":    "en",
                "target_date": "26/3/2026",
                "params": {
                    "dob": "14/6/1995", "tob": "11:20",
                    "lat": 17.38, "lon": 78.48, "tz": 5.5,
                },
            },
        },
    }

    return {
        "total_categories": 7,
        "note": (
            "Send only: category, objective, language, target_date, params{}. "
            "group and api are auto-selected by the engine. "
            "Input is normalised (DOB/TOB/coords) for deterministic caching — "
            "same birth details always produce the same chart."
        ),
        "categories": examples,
    }


# ─────────────────────────────────────────────────────────────────
# GET /api/drishtii/health
# ─────────────────────────────────────────────────────────────────

@drishtii_router.get("/health")
async def health():
    return {
        "status":       "ok",
        "engine_ready": _engine is not None,
        "version":      "production-v2",
    }
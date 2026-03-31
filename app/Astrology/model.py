from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, validator

logger = logging.getLogger("drishtii.model")

# ── Category → (group, api) ──────────────────────────────────────
CATEGORY_API_MAP: Dict[str, tuple[str, str]] = {
    "financial": ("horoscope", "planet-details"),
    "business":  ("horoscope", "planet-details"),
    "career":    ("horoscope", "planet-details"),
    "marriage":  ("matching",  "ashtakoot"),
    "legal":     ("horoscope", "planet-details"),
    "health":    ("horoscope", "planet-details"),
    "travel":    ("horoscope", "planet-details"),
}

CATEGORY_LABELS: Dict[str, str] = {
    "financial": "💰 Financial",
    "business":  "🏢 Business/Tender",
    "career":    "💼 Career",
    "marriage":  "❤️ Family/Marriage",
    "legal":     "⚖️ Legal",
    "health":    "🏥 Health",
    "travel":    "✈️ NRI/Travel",
}

CATEGORY_PARAM_SHAPES: Dict[str, list[str]] = {
    "financial": ["dob", "tob", "lat", "lon", "tz"],
    "business":  ["dob", "tob", "lat", "lon", "tz"],
    "career":    ["dob", "tob", "lat", "lon", "tz"],
    "marriage":  ["m_dob", "m_tob", "m_lat", "m_lon", "m_tz",
                  "f_dob", "f_tob", "f_lat", "f_lon", "f_tz"],
    "legal":     ["dob", "tob", "lat", "lon", "tz"],
    "health":    ["dob", "tob", "lat", "lon", "tz"],
    "travel":    ["dob", "tob", "lat", "lon", "tz"],
}

# ── Marriage also accepts boy_/girl_ style from VedicAPI convention ──
# Users may send boy_dob/girl_dob instead of m_dob/f_dob.
# normalize_params() handles both via the alias map below.
MARRIAGE_PARAM_ALIASES: Dict[str, str] = {
    "boy_dob": "m_dob", "boy_tob": "m_tob",
    "boy_lat": "m_lat", "boy_lon": "m_lon", "boy_tz": "m_tz",
    "girl_dob": "f_dob", "girl_tob": "f_tob",
    "girl_lat": "f_lat", "girl_lon": "f_lon", "girl_tz": "f_tz",
}

# ── Strict cross-category keyword blocks ────────────────────────
# Keys = categories.  Values = forbidden topic keywords that
# definitively prove the objective belongs to a DIFFERENT category.
CROSS_CATEGORY_BLOCKS: Dict[str, list[str]] = {
    # Marriage category must NOT handle financial / business / career / legal queries
    "marriage": [
        "invest", "stock", "mutual fund", "trading", "loan", "debt", "finance",
        "business", "tender", "contract", "company", "startup", "deal",
        "job", "career", "promotion", "interview", "resign", "salary",
        "court", "case", "lawyer", "fir", "police", "lawsuit", "legal",
        "visa", "travel", "abroad", "migrate", "nri", "foreign",
        "surgery", "hospital", "disease", "medicine", "treatment",
    ],
    # Financial must NOT handle marriage / health / legal queries
    "financial": [
        "marriage", "marry", "wedding", "kundali", "compatibility",
        "surgery", "hospital", "diagnos", "court", "case", "lawsuit",
    ],
    # Business must NOT handle marriage / health / legal queries
    "business": [
        "marriage", "marry", "wedding", "kundali", "compatibility",
        "surgery", "hospital", "diagnos", "court", "case", "lawsuit",
    ],
    # Health must NOT handle financial / business / marriage queries
    "health": [
        "invest", "stock", "mutual fund", "business", "tender", "contract",
        "marriage", "marry", "wedding", "court", "case", "lawsuit",
    ],
    # Legal must NOT handle marriage compatibility / investment queries
    "legal": [
        "kundali match", "marriage match", "compatibility",
        "invest", "stock",
    ],
    # Career must NOT handle marriage / health queries
    "career": [
        "marriage", "marry", "wedding", "kundali",
        "surgery", "hospital", "diagnos",
    ],
    # Travel must NOT handle marriage / health queries
    "travel": [
        "marriage", "marry", "wedding", "kundali",
        "surgery", "hospital", "diagnos",
    ],
}

# Which category owns these topics (used in the warning message)
TOPIC_OWNER: Dict[str, str] = {
    "invest": "financial", "stock": "financial", "mutual fund": "financial",
    "loan": "financial", "trading": "financial",
    "business": "business", "tender": "business", "contract": "business",
    "job": "career", "career": "career", "promotion": "career", "salary": "career",
    "court": "legal", "case": "legal", "lawyer": "legal", "lawsuit": "legal",
    "surgery": "health", "hospital": "health", "diagnos": "health", "treatment": "health",
    "visa": "travel", "abroad": "travel", "nri": "travel", "migrate": "travel",
    "marriage": "marriage", "marry": "marriage", "wedding": "marriage",
    "kundali": "marriage", "compatibility": "marriage",
}


# ── DOB / TOB normalisation helpers ─────────────────────────────

def _normalise_dob(raw: str) -> str:
    """
    Accept common date formats and return strict DD/MM/YYYY.
    Raises ValueError with a clear message on unknown format.
    """
    raw = raw.strip()
    # Already DD/MM/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}"
    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    # DD-MM-YYYY
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", raw)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{d}/{mo}/{y}"
    raise ValueError(
        f"Invalid date format '{raw}'. Use DD/MM/YYYY (e.g. 15/08/1990)."
    )


def _normalise_tob(raw: str) -> str:
    """Accept HH:MM or H:MM and return zero-padded HH:MM."""
    raw = raw.strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    raise ValueError(
        f"Invalid time format '{raw}'. Use HH:MM (e.g. 10:30)."
    )


def _normalise_coord(val: Any, name: str) -> float:
    """Round coordinate to 4 decimal places for determinism."""
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid coordinate for '{name}': {val!r}")


def _normalise_tz(val: Any) -> float:
    """Normalise timezone offset to 1 decimal place."""
    try:
        return round(float(val), 1)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid timezone offset: {val!r}")


# ── Pydantic model ───────────────────────────────────────────────

class DrishtiiRequest(BaseModel):
    """
    Simplified Drishtii request — user picks a category, engine picks the API.

    ════════════════════════════════════════════════════════
    CATEGORIES (pick one):
    ════════════════════════════════════════════════════════
      financial  → 💰 Financial decisions (stocks, investment, loans)
      business   → 🏢 Business/Tender decisions
      career     → 💼 Career/Job decisions
      marriage   → ❤️ Family/Marriage compatibility
      legal      → ⚖️ Legal/Court matters
      health     → 🏥 Health/Medical decisions
      travel     → ✈️ NRI/Travel/Migration decisions

    ════════════════════════════════════════════════════════
    PARAMS (flat dict, no nesting):
    ════════════════════════════════════════════════════════
    Most categories (financial/business/career/legal/health/travel):
      { "dob": "15/8/1990", "tob": "10:30",
        "lat": 26.85, "lon": 75.79, "tz": 5.5 }

    marriage only:
      { "m_dob": "15/8/1990", "m_tob": "10:30",
        "m_lat": 26.85, "m_lon": 75.79, "m_tz": 5.5,
        "f_dob": "20/5/1992", "f_tob": "08:00",
        "f_lat": 28.61, "f_lon": 77.20, "f_tz": 5.5 }
    ════════════════════════════════════════════════════════
    """

    category: str = Field(
        ...,
        example="financial",
        description="financial | business | career | marriage | legal | health | travel",
    )
    objective: str = Field(
        ...,
        example="Should I invest in stocks today?",
        description="User's plain-text decision question (min 5 chars)",
    )
    target_date: Optional[str] = Field(
        None,
        example="26/3/2026",
        description="DD/MM/YYYY — the date being evaluated (defaults to today)",
    )
    language: str = Field(
        "en",
        example="hi",
        description="en | hi | es | fr | de | ta | te | mr | gu | bn | kn | ml | pa | ur",
    )
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Flat birth details — see category descriptions above. DO NOT nest.",
    )

    # ── Validators ─────────────────────────────────────────────

    @validator("category")
    def validate_category(cls, v: str) -> str:
        v = v.strip().lower()
        valid = set(CATEGORY_API_MAP.keys())
        if v not in valid:
            raise ValueError(
                f"category must be one of: {', '.join(sorted(valid))}. "
                "Choose: financial | business | career | marriage | legal | health | travel"
            )
        return v

    @validator("objective")
    def validate_objective(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 5:
            raise ValueError("objective must be at least 5 characters.")
        if len(v) > 500:
            raise ValueError("objective must be 500 characters or fewer.")
        return v

    @validator("target_date")
    def validate_target_date(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        try:
            return _normalise_dob(v)
        except ValueError as e:
            raise ValueError(f"target_date: {e}")

    @validator("language")
    def validate_language(cls, v: str) -> str:
        valid = {
            "en", "hi", "es", "fr", "de", "ta", "te",
            "mr", "gu", "bn", "kn", "ml", "pa", "ur",
        }
        v = v.strip().lower()
        if v not in valid:
            logger.warning(f"[MODEL] Unknown language '{v}' — defaulting to 'en'")
            return "en"
        return v

    @validator("params")
    def validate_params_flat(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """
        Guard against accidentally nested params like
        { "additionalProp1": { "dob": ... } }
        Flatten one level if exactly one key wraps the real dict.
        Also normalises boy_/girl_ marriage params → m_/f_ internal keys.
        """
        STANDARD_KEYS = {
            "dob", "tob", "lat", "lon", "tz", "lang", "date",
            "zodiac", "year", "month", "div", "style", "color",
            "planet_name", "place", "state", "stone", "nakshatra",
            "m_dob", "m_tob", "m_lat", "m_lon", "m_tz",
            "f_dob", "f_tob", "f_lat", "f_lon", "f_tz",
            "boy_dob", "boy_tob", "boy_lat", "boy_lon", "boy_tz",
            "girl_dob", "girl_tob", "girl_lat", "girl_lon", "girl_tz",
            "astro_details", "profiles",
        }
        if len(v) == 1:
            only_key   = next(iter(v))
            only_value = v[only_key]
            if only_key not in STANDARD_KEYS and isinstance(only_value, dict):
                logger.warning(
                    f"[MODEL] params auto-flattened: removed wrapper key '{only_key}'"
                )
                return only_value

        # ── Normalise boy_/girl_ → m_/f_ so the rest of the pipeline
        #    only ever sees m_/f_ internally.
        if any(k in v for k in MARRIAGE_PARAM_ALIASES):
            normalised = {}
            for k, val in v.items():
                normalised[MARRIAGE_PARAM_ALIASES.get(k, k)] = val
            logger.info("[MODEL] Marriage params normalised: boy_/girl_ → m_/f_")
            return normalised

        return v

    # ── Resolved properties ──────────────────────────────────────

    @property
    def group(self) -> str:
        return CATEGORY_API_MAP[self.category][0]

    @property
    def api(self) -> str:
        return CATEGORY_API_MAP[self.category][1]

    # ── Deterministic normalised params ──────────────────────────

    def get_normalized_params(self) -> Dict[str, Any]:
        """
        Returns params normalised for determinism:
          • DOB  → DD/MM/YYYY  (zero-padded)
          • TOB  → HH:MM       (zero-padded)
          • lat/lon → 4 d.p.
          • tz   → 1 d.p.
          • lang is always injected as 'en' (VedicAPI needs English data)

        Same human input → always identical dict → same SHA-256 hash →
        same Redis cache key → same chart from VedicAPI every time.
        """
        p = dict(self.params)

        # Standard single-person birth params
        for key in ("dob",):
            if key in p:
                try:
                    p[key] = _normalise_dob(str(p[key]))
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {key}: {e}")

        for key in ("tob",):
            if key in p:
                try:
                    p[key] = _normalise_tob(str(p[key]))
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {key}: {e}")

        for key in ("lat", "lon"):
            if key in p:
                try:
                    p[key] = _normalise_coord(p[key], key)
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {key}: {e}")

        if "tz" in p:
            try:
                p["tz"] = _normalise_tz(p["tz"])
            except ValueError as e:
                logger.warning(f"[MODEL] normalise tz: {e}")

        # Marriage dual-person params
        for prefix in ("m_", "f_"):
            dob_key = f"{prefix}dob"
            tob_key = f"{prefix}tob"
            lat_key = f"{prefix}lat"
            lon_key = f"{prefix}lon"
            tz_key  = f"{prefix}tz"

            if dob_key in p:
                try:
                    p[dob_key] = _normalise_dob(str(p[dob_key]))
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {dob_key}: {e}")
            if tob_key in p:
                try:
                    p[tob_key] = _normalise_tob(str(p[tob_key]))
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {tob_key}: {e}")
            for coord_key in (lat_key, lon_key):
                if coord_key in p:
                    try:
                        p[coord_key] = _normalise_coord(p[coord_key], coord_key)
                    except ValueError as e:
                        logger.warning(f"[MODEL] normalise {coord_key}: {e}")
            if tz_key in p:
                try:
                    p[tz_key] = _normalise_tz(p[tz_key])
                except ValueError as e:
                    logger.warning(f"[MODEL] normalise {tz_key}: {e}")

        # Always pass 'en' to VedicAPI — Gemini handles language translation
        p["lang"] = "en"

        # ── Marriage: translate m_/f_ prefixes → boy_/girl_ for VedicAPI ──
        # Internal model uses m_dob/f_dob but VedicAPI matching endpoints
        # require boy_dob/girl_dob — translate here before any HTTP call.
        if self.category == "marriage":
            prefix_map = {
                "m_dob": "boy_dob", "m_tob": "boy_tob",
                "m_lat": "boy_lat", "m_lon": "boy_lon", "m_tz": "boy_tz",
                "f_dob": "girl_dob", "f_tob": "girl_tob",
                "f_lat": "girl_lat", "f_lon": "girl_lon", "f_tz": "girl_tz",
            }
            p = {prefix_map.get(k, k): v for k, v in p.items()}
            logger.info(
                "[MODEL] Marriage params translated m_/f_ → boy_/girl_ for VedicAPI"
            )

        return p

    def get_flat_params(self) -> Dict[str, Any]:
        """Alias kept for backward compatibility — returns normalised params."""
        return self.get_normalized_params()

    def get_chart_hash(self) -> str:
        """
        SHA-256 of the normalised birth params.
        Identical input → identical hash → cache reuse → stable chart.
        """
        key_str = self.category + ":" + str(sorted(self.get_normalized_params().items()))
        return hashlib.sha256(key_str.encode()).hexdigest()[:16]

    # ── Objective ↔ Category guard ───────────────────────────────

    def check_objective_relevance(self) -> Optional[Dict[str, str]]:
        """
        Returns a structured warning dict if the objective clearly belongs
        to a DIFFERENT category than the one selected.

        Returns None if everything looks fine.

        The warning dict has:
          {
            "error_code":        "CATEGORY_MISMATCH",
            "detected_topic":    "<matched forbidden keyword>",
            "correct_category":  "<suggested category>",
            "selected_category": "<what user sent>",
            "message":           "<human-readable explanation>",
          }
        """
        objective_lower = self.objective.lower()
        forbidden_list  = CROSS_CATEGORY_BLOCKS.get(self.category, [])

        for bad_topic in forbidden_list:
            if bad_topic in objective_lower:
                correct_cat = TOPIC_OWNER.get(bad_topic, "the appropriate category")
                selected_lbl = CATEGORY_LABELS.get(self.category, self.category)
                correct_lbl  = CATEGORY_LABELS.get(correct_cat, correct_cat)

                return {
                    "error_code":        "CATEGORY_MISMATCH",
                    "detected_topic":    bad_topic,
                    "correct_category":  correct_cat,
                    "correct_label":     correct_lbl,
                    "selected_category": self.category,
                    "selected_label":    selected_lbl,
                    "message": (
                        f"Your question contains the topic '{bad_topic}', which belongs to "
                        f"the '{correct_lbl}' category — not '{selected_lbl}'. "
                        f"Please change your category to '{correct_cat}' and resubmit. "
                        f"Available categories: "
                        "financial | business | career | marriage | legal | health | travel"
                    ),
                }
        return None

    def validate_required_params(self) -> Optional[str]:
        """
        Checks that the required params for this category are present.
        Returns a human-readable error string, or None if all is fine.
        """
        required = CATEGORY_PARAM_SHAPES.get(self.category, [])
        p = self.params
        missing = [k for k in required if k not in p or p[k] is None or str(p[k]).strip() == ""]
        if missing:
            example = {k: CATEGORY_PARAM_SHAPES[self.category] for k in [self.category]}
            return (
                f"Missing required params for category '{self.category}': "
                f"{', '.join(missing)}. "
                f"Required: {required}"
            )
        return None
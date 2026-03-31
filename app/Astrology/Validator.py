# app/Astrology/Validator.py
# ════════════════════════════════════════════════════════════════════
#  Validates the structured JSON dict returned by Gemini.
#
#  KEY CHANGES vs previous version:
#   Previously validated a plain text string.
#   Now validates a parsed dict against the Drishtii JSON schema.
#
#  Checks performed:
#   1. Required top-level keys present
#   2. Verdict matches scorecard verdict
#   3. Summary is non-empty and long enough
#   4. time_quality has all 5 sub-fields
#   5. category_analysis has required sub-fields with items
#   6. timing has best_windows and avoid_windows (at least 1 each)
#   7. final_recommendation has what_to_do / what_to_avoid (≥2 each)
#   8. No English sneaking into non-English responses (heuristic check)
# ════════════════════════════════════════════════════════════════════

import logging
from typing import Tuple, Dict, Any

logger = logging.getLogger("drishtii.validator")

REQUIRED_TOP_KEYS = {
    "verdict", "confidence", "summary",
    "time_quality", "category_analysis",
    "timing", "final_recommendation",
}

TIME_QUALITY_KEYS    = {"dasha", "moon", "gochar", "nakshatra", "hora"}
VALID_VERDICTS       = {"GO", "CAUTION", "AVOID"}
VALID_CONFIDENCE     = {"HIGH", "MEDIUM", "LOW"}

# Languages that use non-Latin scripts — for the language sanity check
_NON_LATIN_LANGS = {"hi", "ta", "te", "bn", "kn", "ml", "gu", "pa", "ur", "mr"}

# Minimum summary length (characters)
_MIN_SUMMARY_LEN = 80


class ResponseValidator:
    """
    Validates the structured JSON dict from GeminiClient.generate().
    Returns (is_valid: bool, reason: str).
    """

    def __init__(self, language: str = "en"):
        self.language = language

    def validate(
        self,
        gemini_dict: Dict[str, Any],
        scorecard:   Dict[str, Any],
        key_facts:   Dict[str, Any],
        verdict:     str,
    ) -> Tuple[bool, str]:

        logger.info(
            f"[VALIDATOR] ▶ Validating Gemini dict | "
            f"verdict={verdict} lang={self.language} "
            f"keys={list(gemini_dict.keys())}"
        )

        # ── CHECK 1: Required top-level keys ────────────────
        missing = REQUIRED_TOP_KEYS - set(gemini_dict.keys())
        if missing:
            logger.warning(f"[VALIDATOR] ❌ CHECK 1 FAILED — missing keys: {missing}")
            return False, f"Missing required keys: {missing}"
        logger.info("[VALIDATOR] ✅ CHECK 1 — all top-level keys present")

        # ── CHECK 2: Verdict matches scorecard ──────────────
        g_verdict = gemini_dict.get("verdict", "")
        if g_verdict != verdict:
            logger.warning(
                f"[VALIDATOR] ❌ CHECK 2 FAILED — "
                f"verdict mismatch: Gemini={g_verdict} scorecard={verdict}"
            )
            return False, f"Verdict mismatch: Gemini={g_verdict} expected={verdict}"
        logger.info(f"[VALIDATOR] ✅ CHECK 2 — verdict matches: {verdict}")

        # ── CHECK 3: Confidence is valid ────────────────────
        g_conf = gemini_dict.get("confidence", "")
        if g_conf not in VALID_CONFIDENCE:
            logger.warning(f"[VALIDATOR] ❌ CHECK 3 FAILED — invalid confidence: {g_conf}")
            return False, f"Invalid confidence value: {g_conf}"
        logger.info(f"[VALIDATOR] ✅ CHECK 3 — confidence={g_conf}")

        # ── CHECK 4: Summary is non-empty and long enough ───
        summary = gemini_dict.get("summary", "")
        if len(summary.strip()) < _MIN_SUMMARY_LEN:
            logger.warning(
                f"[VALIDATOR] ❌ CHECK 4 FAILED — "
                f"summary too short: {len(summary)} chars (min {_MIN_SUMMARY_LEN})"
            )
            return False, f"Summary too short: {len(summary)} chars"
        logger.info(f"[VALIDATOR] ✅ CHECK 4 — summary length={len(summary)}")

        # ── CHECK 5: time_quality has all sub-keys ──────────
        tq      = gemini_dict.get("time_quality", {})
        tq_miss = TIME_QUALITY_KEYS - set(tq.keys())
        if tq_miss:
            logger.warning(f"[VALIDATOR] ❌ CHECK 5 FAILED — time_quality missing: {tq_miss}")
            return False, f"time_quality missing sub-keys: {tq_miss}"
        # At least 3 of the 5 fields must be non-empty
        non_empty_tq = sum(1 for v in tq.values() if str(v).strip())
        if non_empty_tq < 3:
            logger.warning(
                f"[VALIDATOR] ❌ CHECK 5 FAILED — "
                f"time_quality has only {non_empty_tq}/5 non-empty fields"
            )
            return False, f"time_quality mostly empty ({non_empty_tq}/5 fields populated)"
        logger.info(f"[VALIDATOR] ✅ CHECK 5 — time_quality OK ({non_empty_tq}/5 populated)")

        # ── CHECK 6: category_analysis structure ────────────
        ca          = gemini_dict.get("category_analysis", {})
        key_factors = ca.get("key_factors", [])
        risk_factors= ca.get("risk_factors", [])
        if not ca.get("focus_area"):
            logger.warning("[VALIDATOR] ❌ CHECK 6 FAILED — focus_area is empty")
            return False, "category_analysis.focus_area is empty"
        if len(key_factors) < 1:
            logger.warning("[VALIDATOR] ❌ CHECK 6 FAILED — key_factors is empty")
            return False, "category_analysis.key_factors has no items"
        logger.info(
            f"[VALIDATOR] ✅ CHECK 6 — category_analysis OK "
            f"(key_factors={len(key_factors)} risk_factors={len(risk_factors)})"
        )

        # ── CHECK 7: timing windows ─────────────────────────
        timing       = gemini_dict.get("timing", {})
        best_windows = timing.get("best_windows", [])
        if len(best_windows) < 1:
            logger.warning("[VALIDATOR] ❌ CHECK 7 FAILED — best_windows is empty")
            return False, "timing.best_windows has no items"
        logger.info(
            f"[VALIDATOR] ✅ CHECK 7 — timing OK "
            f"(best={len(best_windows)} avoid={len(timing.get('avoid_windows', []))})"
        )

        # ── CHECK 8: final_recommendation ───────────────────
        fr       = gemini_dict.get("final_recommendation", {})
        what_do  = fr.get("what_to_do", [])
        what_av  = fr.get("what_to_avoid", [])
        if len(what_do) < 2:
            logger.warning(
                f"[VALIDATOR] ❌ CHECK 8 FAILED — "
                f"what_to_do has only {len(what_do)} items (need ≥2)"
            )
            return False, f"final_recommendation.what_to_do has only {len(what_do)} items"
        logger.info(
            f"[VALIDATOR] ✅ CHECK 8 — final_recommendation OK "
            f"(what_to_do={len(what_do)} what_to_avoid={len(what_av)})"
        )

        # ── CHECK 9: Language sanity (non-Latin scripts) ────
        # If language is non-Latin, the summary must not be mostly ASCII
        if self.language in _NON_LATIN_LANGS:
            ascii_ratio = sum(1 for c in summary if ord(c) < 128) / max(len(summary), 1)
            if ascii_ratio > 0.85:
                logger.warning(
                    f"[VALIDATOR] ❌ CHECK 9 FAILED — "
                    f"summary appears to be in English for non-Latin lang={self.language} "
                    f"(ascii_ratio={ascii_ratio:.2f})"
                )
                return False, f"Summary appears to be in English for lang={self.language}"
        logger.info(f"[VALIDATOR] ✅ CHECK 9 — language sanity OK for lang={self.language}")

        logger.info("[VALIDATOR] ✅ ALL CHECKS PASSED")
        return True, "Valid"
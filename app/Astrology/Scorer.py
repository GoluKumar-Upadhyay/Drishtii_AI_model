
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("drishtii.scorer")

# ── Dasha planet base quality (1-9, not 1-10 to allow natal modifier) ──
DASHA_BASE: dict[str, int] = {
    "Jupiter": 8, "Venus": 7, "Mercury": 6, "Moon": 5,
    "Sun":     5, "Mars":  3, "Saturn":  3, "Rahu": 2, "Ketu": 2,
}

# ── Antardasha quality per category (Ketu is bad for financial) ──────
ANTAR_CATEGORY_SCORE: dict[str, dict[str, int]] = {
    "financial": {
        "Jupiter": 8, "Venus": 8, "Mercury": 6, "Moon": 5,
        "Sun": 4, "Saturn": 3, "Mars": 3, "Rahu": 2, "Ketu": 2,
    },
    "career": {
        "Sun": 8, "Mercury": 7, "Jupiter": 7, "Saturn": 6,
        "Mars": 6, "Venus": 5, "Moon": 4, "Rahu": 3, "Ketu": 2,
    },
    "health": {
        "Jupiter": 8, "Venus": 7, "Moon": 6, "Mercury": 5,
        "Sun": 5, "Saturn": 3, "Mars": 3, "Rahu": 2, "Ketu": 2,
    },
    "marriage": {
        "Venus": 9, "Moon": 8, "Jupiter": 7, "Mercury": 5,
        "Sun": 4, "Mars": 3, "Saturn": 3, "Rahu": 2, "Ketu": 2,
    },
    "business": {
        "Mercury": 8, "Jupiter": 8, "Venus": 7, "Sun": 6,
        "Moon": 5, "Saturn": 4, "Mars": 3, "Rahu": 3, "Ketu": 2,
    },
    "legal": {
        "Jupiter": 9, "Sun": 7, "Mars": 6, "Saturn": 5,
        "Mercury": 5, "Moon": 4, "Venus": 4, "Rahu": 2, "Ketu": 2,
    },
    "travel": {
        "Rahu": 7, "Moon": 7, "Jupiter": 6, "Mercury": 6,
        "Venus": 5, "Sun": 4, "Saturn": 3, "Mars": 3, "Ketu": 3,
    },
}

# ── Hora planet quality (0-7 max, recalibrated) ──────────────────────
HORA_SCORE: dict[str, int] = {
    "Jupiter": 7, "Venus": 6, "Mercury": 5, "Moon": 4,
    "Sun": 3, "Mars": 2, "Saturn": 1,
}

# ── Nakshatra groupings (lower-cased) ────────────────────────────────
RAHU_NAKSHATRAS  = {"ardra", "swati", "shatabhisha"}
KETU_NAKSHATRAS  = {"ashwini", "magha", "moola"}
GURU_NAKSHATRAS  = {"punarvasu", "vishakha", "purva bhadra", "purvabhadra", "purvabhadrapada"}
VENUS_NAKSHATRAS = {"bharani", "purva phalguni", "purva ashadha",
                    "purvaphalguni", "purvaashadha", "purvashada"}
MOON_NAKSHATRAS  = {"rohini", "hasta", "shravana"}
MARS_NAKSHATRAS  = {"mrigashira", "chitra", "dhanishtha", "dhanistha"}
SUN_NAKSHATRAS   = {"kritika", "uttara phalguni", "uttara ashadha",
                    "uttaraphalguni", "uttarashadha"}

# ── Verdict thresholds ────────────────────────────────────────────────
VERDICT_GO      = 28   # Lowered from 30 — recalibrated realistic scale
VERDICT_CAUTION = 18   # Lowered from 20

# ── Functional benefic/malefic table for each ascendant ──────────────
# Values: "benefic", "malefic", "maraka", "yogakaraka", "neutral"
# Source: Classical Parashari rules
FUNCTIONAL_STATUS: dict[str, dict[str, str]] = {
    "Aries":       {"Sun":"benefic","Moon":"neutral","Mars":"yogakaraka","Mercury":"neutral","Jupiter":"benefic","Venus":"maraka","Saturn":"malefic","Rahu":"malefic","Ketu":"malefic"},
    "Taurus":      {"Sun":"neutral","Moon":"benefic","Mars":"malefic","Mercury":"yogakaraka","Jupiter":"maraka","Venus":"yogakaraka","Saturn":"benefic","Rahu":"malefic","Ketu":"malefic"},
    "Gemini":      {"Sun":"neutral","Moon":"neutral","Mars":"malefic","Mercury":"benefic","Jupiter":"maraka","Venus":"benefic","Saturn":"yogakaraka","Rahu":"neutral","Ketu":"neutral"},
    "Cancer":      {"Sun":"benefic","Moon":"benefic","Mars":"yogakaraka","Mercury":"malefic","Jupiter":"benefic","Venus":"malefic","Saturn":"malefic","Rahu":"malefic","Ketu":"malefic"},
    "Leo":         {"Sun":"benefic","Moon":"neutral","Mars":"yogakaraka","Mercury":"malefic","Jupiter":"benefic","Venus":"malefic","Saturn":"malefic","Rahu":"neutral","Ketu":"neutral"},
    "Virgo":       {"Sun":"neutral","Moon":"malefic","Mars":"malefic","Mercury":"benefic","Jupiter":"maraka","Venus":"yogakaraka","Saturn":"yogakaraka","Rahu":"neutral","Ketu":"neutral"},
    "Libra":       {"Sun":"malefic","Moon":"neutral","Mars":"malefic","Mercury":"benefic","Jupiter":"malefic","Venus":"yogakaraka","Saturn":"yogakaraka","Rahu":"neutral","Ketu":"neutral"},
    "Scorpio":     {"Sun":"benefic","Moon":"neutral","Mars":"yogakaraka","Mercury":"maraka","Jupiter":"benefic","Venus":"maraka","Saturn":"neutral","Rahu":"malefic","Ketu":"malefic"},
    "Sagittarius": {"Sun":"benefic","Moon":"neutral","Mars":"yogakaraka","Mercury":"malefic","Jupiter":"benefic","Venus":"maraka","Saturn":"malefic","Rahu":"neutral","Ketu":"neutral"},
    "Capricorn":   {"Sun":"malefic","Moon":"neutral","Mars":"yogakaraka","Mercury":"benefic","Jupiter":"malefic","Venus":"yogakaraka","Saturn":"benefic","Rahu":"neutral","Ketu":"neutral"},
    "Aquarius":    {"Sun":"malefic","Moon":"neutral","Mars":"neutral","Mercury":"neutral","Jupiter":"malefic","Venus":"yogakaraka","Saturn":"benefic","Rahu":"neutral","Ketu":"neutral"},
    "Pisces":      {"Sun":"neutral","Moon":"benefic","Mars":"benefic","Mercury":"maraka","Jupiter":"benefic","Venus":"malefic","Saturn":"malefic","Rahu":"neutral","Ketu":"neutral"},
}

# ── Good transit houses per planet per category ───────────────────────
# Transit planets in these natal houses = favorable
TRANSIT_GOOD_HOUSES: dict[str, dict[str, list[int]]] = {
    "financial": {
        "Jupiter": [2, 5, 7, 9, 11],
        "Venus":   [1, 2, 4, 5, 11],
        "Mercury": [1, 2, 6, 10, 11],
        "Moon":    [1, 3, 6, 10, 11],
        "Sun":     [3, 6, 10, 11],
        "Saturn":  [3, 6, 11],
        "Mars":    [3, 6, 11],
    },
    "career": {
        "Jupiter": [1, 5, 9, 10, 11],
        "Sun":     [1, 9, 10, 11],
        "Saturn":  [1, 3, 6, 11],
        "Mercury": [1, 3, 6, 10],
        "Mars":    [3, 6, 10, 11],
    },
    "health": {
        "Jupiter": [1, 5, 7, 9, 11],
        "Moon":    [1, 3, 6, 10, 11],
        "Mars":    [3, 6, 11],
        "Saturn":  [3, 6, 11],
    },
    "marriage": {
        "Jupiter": [1, 5, 7, 9, 11],
        "Venus":   [1, 2, 5, 7, 11],
        "Moon":    [1, 5, 7, 11],
    },
    "legal": {
        "Jupiter": [1, 6, 9, 10, 11],
        "Sun":     [1, 9, 10, 11],
        "Mars":    [3, 6, 10, 11],
    },
    "travel": {
        "Jupiter": [9, 12],
        "Rahu":    [9, 12],
        "Moon":    [9, 11],
        "Mercury": [3, 9, 12],
    },
    "business": {
        "Mercury": [1, 2, 7, 10, 11],
        "Jupiter": [2, 5, 7, 9, 11],
        "Venus":   [2, 7, 11],
    },
}

# Transit planets in these natal houses = bad
TRANSIT_BAD_HOUSES: dict[str, list[int]] = {
    "Jupiter": [4, 8, 12],
    "Saturn":  [1, 4, 8, 12],
    "Mars":    [1, 4, 8, 12],
    "Rahu":    [1, 5, 7, 8],
    "Ketu":    [1, 5, 7, 8],
    "Moon":    [4, 8, 12],
}


class Scorer:
    """
    Applies 7 Sutras + yoga bonuses to extracted facts.
    v3: Uses transit planets, live panchang, ascendant-aware scoring.
    Pure Python — no AI, no external calls.
    """

    def score(
        self,
        key_facts:   Dict[str, Any],
        category:    str,
        group:       str,
        api:         str,
        target_date: str | None = None,
    ) -> Dict[str, Any]:

        context_type = key_facts.get("context_type", f"{group}/{api}")
        warnings:  List[str] = []
        positives: List[str] = []
        scores:    Dict[str, int] = {}

        logger.info(
            f"[SCORER] ▶ Scoring start\n"
            f"  context_type      = {context_type}\n"
            f"  category          = {category}\n"
            f"  has_planets       = {bool(key_facts.get('planets'))}\n"
            f"  has_transit       = {bool(key_facts.get('transit_planets'))}\n"
            f"  has_today_panch   = {bool(key_facts.get('today_panchang'))}"
        )

        if key_facts.get("planets"):
            logger.info("[SCORER] → PATH 1: Planet data — full 7-Sutra scoring")
            scores, warnings, positives = self._score_with_planets(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("dashas/") or group == "dashas":
            logger.info("[SCORER] → PATH 2: Dasha-only scoring")
            scores, warnings, positives = self._score_dashas(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("panchang/") or group == "panchang":
            logger.info("[SCORER] → PATH 3: Panchang scoring")
            scores, warnings, positives = self._score_panchang(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("dosha/") or group == "dosha":
            logger.info("[SCORER] → PATH 4: Dosha scoring")
            scores, warnings, positives = self._score_dosha(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("matching/") or group == "matching":
            logger.info("[SCORER] → PATH 5: Matching scoring")
            scores, warnings, positives = self._score_matching(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("prediction/") or group == "prediction":
            logger.info("[SCORER] → PATH 6: Prediction text scoring")
            scores, warnings, positives = self._score_prediction(
                key_facts, category, warnings, positives
            )
        elif context_type.startswith("utilities/") or group == "utilities":
            logger.info("[SCORER] → PATH 7: Utilities neutral")
            scores, warnings, positives = self._score_utilities(
                key_facts, category, warnings, positives
            )
        else:
            logger.warning(f"[SCORER] → PATH UNKNOWN: defaulting to neutral for {context_type}")
            scores = {"base": 4, "category": 4, "context": 4, "timing": 4}

        # ── Calculate total ─────────────────────────────────────
        raw_total = sum(scores.values())
        total     = max(0, min(34, raw_total))   # FIX 8: realistic cap is 34, not 40

        logger.info(
            f"[SCORER] Score breakdown:\n"
            + "\n".join(f"  {k:30} = {v}" for k, v in scores.items())
            + f"\n  {'RAW TOTAL':30} = {raw_total}\n"
            f"  {'CAPPED TOTAL (0-34)':30} = {total}"
        )

        # ── Triple-malefic penalty ──────────────────────────────
        warn_text = " ".join(warnings).lower()
        if all(p in warn_text for p in ("saturn", "rahu", "ketu")):
            penalty = 10
            total   = max(0, total - penalty)
            warnings.append(
                f"⚠️ TRIPLE MALEFIC — Saturn+Rahu+Ketu all active → -{penalty} penalty"
            )
            logger.warning(f"[SCORER] Triple malefic penalty applied. New total = {total}")

        # ── Verdict ─────────────────────────────────────────────
        if total >= VERDICT_GO:
            verdict = "GO"
        elif total >= VERDICT_CAUTION:
            verdict = "CAUTION"
        else:
            verdict = "AVOID"

        logger.info(
            f"[SCORER] ✅ VERDICT: {verdict}\n"
            f"  Total Score : {total}/34\n"
            f"  Warnings    : {warnings}\n"
            f"  Positives   : {positives}"
        )

        return {
            "scores":       scores,
            "total_score":  total,
            "max_score":    34,
            "verdict":      verdict,
            "warnings":     warnings[:6],
            "positives":    positives[:6],
            "category":     category,
            "context_type": context_type,
        }

    # ═══════════════════════════════════════════════════════════
    # PATH 1 — PLANET DATA (horoscope, extended, divisional)
    # ═══════════════════════════════════════════════════════════

    def _score_with_planets(
        self,
        facts:     Dict[str, Any],
        category:  str,
        warnings:  List[str],
        positives: List[str],
    ) -> Tuple[Dict[str, int], List[str], List[str]]:

        planets          = facts.get("planets",         {})
        panchang         = facts.get("panchang",        {})   # NATAL panchang (birth)
        dasha            = facts.get("dasha",           {})
        transit_planets  = facts.get("transit_planets", {})   # FIX 3: TODAY's planets
        ashtakavarga     = facts.get("ashtakavarga",    {})   # Ashtakavarga bindus per planet/house
        today_panchang   = facts.get("today_panchang",  {})   # FIX 1,2: TODAY's panchang
        # today_nakshatra may arrive as a dict (e.g. {'name': 'Ardra', 'pada': 3, ...})
        # or as a plain string — normalise to string here so .lower() never crashes.
        _raw_nakshatra   = facts.get("today_nakshatra", "")
        today_nakshatra  = (
            _raw_nakshatra.get("name", "") if isinstance(_raw_nakshatra, dict)
            else str(_raw_nakshatra)
        )
        # Same safety for today_hora
        _raw_hora        = facts.get("today_hora", "")
        today_hora       = (
            _raw_hora.get("name", "") if isinstance(_raw_hora, dict)
            else str(_raw_hora)
        )

        # Get ascendant zodiac for functional status table (FIX 4)
        ascendant_zodiac = planets.get("ascendant", {}).get("zodiac", "")
        functional_map   = FUNCTIONAL_STATUS.get(ascendant_zodiac, {})

        logger.info(
            f"[SCORER] Ascendant: {ascendant_zodiac} | "
            f"Functional map loaded: {bool(functional_map)}"
        )

        scores: Dict[str, int] = {}

        # ── Sutra 1: Dasha Quality (FIX 5, 6, 7) ────────────────
        current_dasa = dasha.get("current_dasa", "")
        dasha_score  = 4  # neutral default (lowered from 5)
        if current_dasa and "undefined" not in str(current_dasa).lower():
            parts = str(current_dasa).split(">")
            maha  = parts[0].strip() if parts else ""
            antar = parts[1].strip() if len(parts) > 1 else ""
            logger.info(f"[SCORER] Sutra1 Dasha: maha={maha} antar={antar}")
            dasha_score = self._calc_dasha_score(
                maha, antar, category, planets, functional_map, warnings, positives
            )
        else:
            logger.info("[SCORER] Sutra1 Dasha: no current_dasa — using neutral 4")
        scores["dasha_quality"] = dasha_score

        # ── Sutra 2: Moon State (natal — correct, this is birth Moon) ─
        moon = planets.get("moon", {})
        logger.info(f"[SCORER] Sutra2 Moon: house={moon.get('house')} nakshatra={moon.get('nakshatra')}")
        scores["moon_state"] = self._calc_moon_score(moon, functional_map, warnings, positives)

        # ── Sutra 3: Nakshatra — FIX 1: use TODAY's nakshatra ────────
        # Priority: today_nakshatra (from panchang API) > transit panchang > birth nakshatra
        if today_nakshatra:
            day_nakshatra = today_nakshatra.lower().strip()
            logger.info(f"[SCORER] Sutra3 Nakshatra (TODAY): {day_nakshatra}")
        elif today_panchang.get("nakshatra"):
            _panch_nak    = today_panchang["nakshatra"]
            day_nakshatra = (
                _panch_nak.get("name", "") if isinstance(_panch_nak, dict)
                else str(_panch_nak)
            ).lower().strip()
            logger.info(f"[SCORER] Sutra3 Nakshatra (today_panchang): {day_nakshatra}")
        else:
            # Fallback: birth nakshatra (old behaviour, clearly flagged)
            day_nakshatra = facts.get("nakshatra", moon.get("nakshatra", "")).lower().strip()
            logger.warning(
                f"[SCORER] Sutra3 Nakshatra FALLBACK to birth nakshatra: {day_nakshatra} "
                "— today_nakshatra not available"
            )
        scores["nakshatra"] = self._calc_nakshatra_score(day_nakshatra, warnings, positives)

        # ── Sutra 4: Hora — FIX 2: use TODAY's hora ──────────────────
        # Priority: today_hora (panchang API) > today_panchang hora > birth panchang hora
        if today_hora:
            live_hora = today_hora
            logger.info(f"[SCORER] Sutra4 Hora (TODAY): {live_hora}")
        elif today_panchang.get("hora_lord"):
            live_hora = today_panchang["hora_lord"]
            logger.info(f"[SCORER] Sutra4 Hora (today_panchang): {live_hora}")
        else:
            live_hora = panchang.get("hora_lord", "")
            logger.warning(
                f"[SCORER] Sutra4 Hora FALLBACK to birth panchang hora: {live_hora} "
                "— today_hora not available"
            )
        hora_detail = facts.get("today_hora_detail", {})
        scores["hora"] = self._calc_hora_score(live_hora, category, warnings, positives, hora_detail)

        # ── Sutra 5: Malefic Afflictions ──────────────────────────────
        logger.info(
            f"[SCORER] Sutra5 Afflictions: "
            f"rahu_house={planets.get('rahu',{}).get('house')} "
            f"ketu_house={planets.get('ketu',{}).get('house')}"
        )
        scores["afflictions"] = self._calc_afflictions(planets, warnings, positives)

        # ── Sutra 6: Functional Benefics (FIX 4, 6) ──────────────────
        scores["benefics"] = self._calc_benefics(planets, functional_map, positives, warnings)

        # ── Sutra 7: Category-Specific ────────────────────────────────
        logger.info(f"[SCORER] Sutra7 Category-specific: {category}")
        scores["category_specific"] = self._score_category(
            planets, panchang, category, functional_map, warnings, positives
        )

        # ── Sutra 8 (NEW): Transit / Gochar Score (+ Ashtakavarga) ──────
        if transit_planets:
            logger.info(
                f"[SCORER] Sutra8 Transit: {len(transit_planets)} transit planets | "
                f"ashtakavarga_planets={len(ashtakavarga)}"
            )
            scores["transit_gochar"] = self._calc_transit_score(
                transit_planets, planets, category, warnings, positives,
                ashtakavarga=ashtakavarga,
            )
        else:
            logger.info("[SCORER] Sutra8 Transit: no transit data — skipping")

        # ── Yoga/Combination Bonuses ──────────────────────────────────
        yoga_adj = self._check_yogas(planets, category, positives, warnings)
        if yoga_adj != 0:
            scores["yoga_bonus"] = yoga_adj
            logger.info(f"[SCORER] Yoga adjustment: {yoga_adj}")

        return scores, warnings, positives

    # ── Sub-calculators ──────────────────────────────────────────────

    def _calc_dasha_score(
        self,
        maha:         str,
        antar:        str,
        category:     str,
        planets:      Dict,
        functional_map: Dict,
        warnings:     List[str],
        positives:    List[str],
    ) -> int:
        """
        FIX 5: Dasha lord quality modified by its natal placement.
        FIX 6: Uses functional_map (ascendant-specific) not generic benefic list.
        FIX 7: Antardasha scores are category-specific (Ketu = 2 for financial).
        """
        m_base = DASHA_BASE.get(maha, 4)

        # FIX 5: Modify maha score based on natal placement of dasha lord
        maha_planet = planets.get(maha.lower(), {})
        maha_house  = maha_planet.get("house", 0)
        natal_mod   = 0
        if maha_house in (1, 5, 9, 10, 11):     # trikona + kendra + 11th = strong
            natal_mod = +1
        elif maha_house in (6, 8, 12):           # dusthana = weakened
            natal_mod = -2
        elif maha_house in (2, 7):               # maraka houses = caution
            natal_mod = -1

        # FIX 6: Use functional status if available
        func_status = functional_map.get(maha, "neutral")
        func_mod = 0
        if func_status == "yogakaraka":
            func_mod = +2
        elif func_status == "benefic":
            func_mod = +1
        elif func_status == "maraka":
            func_mod = -2
        elif func_status == "malefic":
            func_mod = -1

        m_score = max(1, min(8, m_base + natal_mod + func_mod))

        # FIX 7: Antardasha score is category-specific
        cat_antar = ANTAR_CATEGORY_SCORE.get(category, ANTAR_CATEGORY_SCORE["financial"])
        a_score   = cat_antar.get(antar, 3)

        combined = round(m_score * 0.6 + a_score * 0.4)

        # Logging
        if m_score >= 6:
            positives.append(f"{maha} Mahadasha (natal house {maha_house}) — favorable dasha period")
        elif m_score <= 2:
            warnings.append(f"{maha} Mahadasha — difficult period (natal placement + functional status weak)")

        if a_score <= 2:
            warnings.append(f"{antar} Antardasha — challenging sub-period for {category}")
        elif a_score >= 7:
            positives.append(f"{antar} Antardasha — highly supportive sub-period for {category}")

        result = max(1, min(7, combined))  # FIX 8: max 7 for this sutra (recalibrated)
        logger.info(
            f"[SCORER]   dasha_score: maha_base={m_base} natal_mod={natal_mod} "
            f"func_mod={func_mod} m_score={m_score} antar={a_score} combined={result}"
        )
        return result

    def _calc_moon_score(
        self, moon: Dict, functional_map: Dict, warnings: List[str], positives: List[str]
    ) -> int:
        house          = moon.get("house", 0)
        nakshatra_lord = moon.get("nakshatra_lord", "")

        score = 4  # neutral default (recalibrated from 5)
        if house == 8:
            score = 2
            warnings.append("Moon in 8th house — emotional turbulence, sudden changes")
        elif house in (6, 12):
            score = 3
            warnings.append(f"Moon in {house}th house — challenging placement")
        elif house in (1, 4, 7, 10):
            score = 6
            positives.append(f"Moon in {house}th house (angular) — strong, stable energy")
        elif house in (2, 5):
            score = 5
            positives.append(f"Moon in {house}th house — financial and creative energy")
        elif house in (3, 9, 11):
            score = 5

        if nakshatra_lord in ("Rahu", "Ketu"):
            score = max(1, score - 2)
            warnings.append(f"Moon in {nakshatra_lord} Nakshatra — disturbed clarity")
        elif nakshatra_lord == "Jupiter":
            score = min(7, score + 1)
            positives.append("Moon in Jupiter Nakshatra — wisdom and good judgment")
        elif nakshatra_lord == "Venus":
            score = min(7, score + 1)
            positives.append("Moon in Venus Nakshatra — harmony and positive emotions")

        # FIX 6: Check Moon's functional status
        moon_func = functional_map.get("Moon", "neutral")
        if moon_func == "malefic":
            score = max(1, score - 1)
            warnings.append("Moon is functional malefic for this ascendant — emotional decisions risky")
        elif moon_func in ("benefic", "yogakaraka"):
            score = min(7, score + 1)

        if moon.get("is_combust"):
            score = max(1, score - 2)
            warnings.append("Moon combust — mental clarity reduced, near Amavasya")

        result = max(1, min(7, score))
        logger.info(
            f"[SCORER]   moon_score: house={house} nk_lord={nakshatra_lord} "
            f"func={moon_func} score={result}"
        )
        return result

    def _calc_nakshatra_score(
        self, nakshatra: str, warnings: List[str], positives: List[str]
    ) -> int:
        """FIX 1: Scores TODAY's nakshatra (passed in from panchang, not birth)."""
        # Recalibrated: max 6 (not 7) to reduce score inflation
        if nakshatra in RAHU_NAKSHATRAS:
            warnings.append(f"Today's Nakshatra ({nakshatra}) — Rahu zone, unexpected changes likely")
            return 2
        if nakshatra in KETU_NAKSHATRAS:
            warnings.append(f"Today's Nakshatra ({nakshatra}) — Ketu zone, endings/detachment energy")
            return 2
        if nakshatra in GURU_NAKSHATRAS:
            positives.append(f"Today's Nakshatra ({nakshatra}) — Jupiter zone, expansion and growth")
            return 6
        if nakshatra in VENUS_NAKSHATRAS:
            positives.append(f"Today's Nakshatra ({nakshatra}) — Venus zone, harmony and gains")
            return 5
        if nakshatra in MOON_NAKSHATRAS:
            positives.append(f"Today's Nakshatra ({nakshatra}) — Moon zone, emotional intelligence")
            return 5
        if nakshatra in MARS_NAKSHATRAS:
            positives.append(f"Today's Nakshatra ({nakshatra}) — Mars zone, energy and initiative")
            return 4
        if nakshatra in SUN_NAKSHATRAS:
            positives.append(f"Today's Nakshatra ({nakshatra}) — Sun zone, authority and clarity")
            return 4
        return 3  # unknown/neutral

    def _calc_hora_score(
        self,
        hora_lord:   str,
        category:    str,
        warnings:    List[str],
        positives:   List[str],
        hora_detail: Dict | None = None,
    ) -> int:
        """FIX 2: Uses TODAY's hora with time-window details."""
        base = HORA_SCORE.get(hora_lord, 3)  # max 7

        # Build window suffix for messages when detail available
        window       = (hora_detail or {}).get("hora_window", "")
        next_lord    = (hora_detail or {}).get("next_lord", "")
        next_window  = (hora_detail or {}).get("next_hora_window", "")
        win_str      = f" ({window})" if window else ""
        next_str     = f" — next: {next_lord} Hora {next_window}" if next_lord else ""

        cat_mod = 0
        if category == "financial":
            if hora_lord in ("Jupiter", "Venus"):
                cat_mod = +1
                positives.append(f"{hora_lord} Hora{win_str} — best time for financial decisions{next_str}")
            elif hora_lord == "Mars":
                cat_mod = -2
                warnings.append(f"Mars Hora{win_str} — aggressive energy, avoid financial decisions now{next_str}")
            elif hora_lord == "Saturn":
                cat_mod = -1
                warnings.append(f"Saturn Hora{win_str} — delays and friction for financial transactions{next_str}")
        elif category == "career":
            if hora_lord in ("Sun", "Mercury"):
                cat_mod = +1
                positives.append(f"{hora_lord} Hora{win_str} — good for career moves and interviews{next_str}")
        elif category == "health":
            if hora_lord == "Moon":
                cat_mod = +1
            elif hora_lord == "Mars":
                cat_mod = +1
                positives.append(f"Mars Hora{win_str} — acceptable for medical procedures and surgery{next_str}")
        elif category == "marriage":
            if hora_lord in ("Venus", "Moon"):
                cat_mod = +1
                positives.append(f"{hora_lord} Hora{win_str} — auspicious for relationship decisions{next_str}")
            elif hora_lord == "Mars":
                cat_mod = -1
                warnings.append(f"Mars Hora{win_str} — aggressive energy for marriage discussion{next_str}")
        elif category == "legal":
            if hora_lord == "Jupiter":
                cat_mod = +1
                positives.append(f"Jupiter Hora{win_str} — best for court matters and legal filings{next_str}")
        elif category == "travel":
            if hora_lord == "Mercury":
                cat_mod = +1
                positives.append(f"Mercury Hora{win_str} — good for travel bookings and visa applications{next_str}")

        if hora_lord == "Jupiter" and cat_mod == 0:
            positives.append(f"Jupiter Hora{win_str} — favorable for important decisions{next_str}")
        elif hora_lord == "Saturn" and cat_mod == 0:
            warnings.append(f"Saturn Hora{win_str} — delays and friction, routine tasks only{next_str}")
        elif hora_lord == "Mars" and cat_mod == 0:
            warnings.append(f"Mars Hora{win_str} — aggressive energy, proceed with caution{next_str}")

        result = max(1, min(7, base + cat_mod))
        logger.info(
            f"[SCORER]   hora_score: hora_lord={hora_lord} "
            f"window={window} base={base} cat_mod={cat_mod} score={result}"
        )
        return result

    def _calc_transit_score(
        self,
        transit_planets:  Dict,
        natal_planets:    Dict,
        category:         str,
        warnings:         List[str],
        positives:        List[str],
        ashtakavarga:     Dict | None = None,
    ) -> int:
        """
        FIX 3 + Ashtakavarga: Score transiting planets over natal houses.

        When ashtakavarga (binnashtakavarga) data is available we use the actual
        bindu count (0-8) of the transiting planet in that natal house instead of
        the generic good/bad house lists.  This is the classical Vedic method:
          ≥5 bindus = favorable transit in that house
          ≤3 bindus = unfavorable transit in that house
          4 bindus  = neutral

        Falls back to the good/bad house list when ashtakavarga is absent.
        """
        score = 4  # neutral base

        good_houses  = TRANSIT_GOOD_HOUSES.get(category, TRANSIT_GOOD_HOUSES["financial"])
        planet_names = ["jupiter", "venus", "saturn", "mars", "mercury", "moon", "rahu", "ketu"]
        astaka       = ashtakavarga or {}

        for planet_name in planet_names:
            t_planet  = transit_planets.get(planet_name, {})
            t_house   = t_planet.get("house", 0)
            if not t_house:
                continue

            planet_cap = planet_name.capitalize()

            # ── Ashtakavarga path (precise) ────────────────────────────
            planet_bindus = astaka.get(planet_name, {})
            if planet_bindus:
                bindus = planet_bindus.get(t_house, 4)  # default 4 (neutral)
                if bindus >= 5:
                    score += 1
                    positives.append(
                        f"Transit {planet_cap} in {t_house}th house ({bindus}/8 bindus) — "
                        f"strong gochar for {category}"
                    )
                elif bindus <= 3:
                    score -= 1
                    warnings.append(
                        f"Transit {planet_cap} in {t_house}th house ({bindus}/8 bindus) — "
                        f"weak gochar, limited support"
                    )
                # 4 bindus = neutral, no message
                continue  # skip generic path for this planet

            # ── Fallback: generic good/bad house lists ─────────────────
            good = good_houses.get(planet_cap, [])
            if t_house in good:
                score += 1
                positives.append(
                    f"Transit {planet_cap} in {t_house}th house — favorable gochar for {category}"
                )

            bad = TRANSIT_BAD_HOUSES.get(planet_cap, [])
            if t_house in bad:
                score -= 1
                warnings.append(
                    f"Transit {planet_cap} in {t_house}th house — challenging gochar placement"
                )

        result = max(1, min(6, score))  # max 6 for transit sutra
        logger.info(
            f"[SCORER]   transit_score: {result} | "
            f"ashtakavarga_used={bool(astaka)}"
        )
        return result

    def _calc_afflictions(
        self, planets: Dict, warnings: List[str], positives: List[str]
    ) -> int:
        """
        FIX 8 (recalibration): Malefic impact is now STRONGER than benefic boost.
        Saturn in 8th + Ardra + Mars Hora combo should suppress score to ~26-28.
        Base lowered from 5 → 4; individual penalties increased.
        """
        score  = 4  # base lowered from 5
        rahu   = planets.get("rahu",   {})
        ketu   = planets.get("ketu",   {})
        saturn = planets.get("saturn", {})
        mars   = planets.get("mars",   {})
        sun    = planets.get("sun",    {})

        if rahu.get("house") == 1:
            score -= 3   # was -2: Rahu in lagna = very dangerous for all decisions
            warnings.append("Rahu in 1st house — clouded judgment, unclear decisions")
        if rahu.get("house") in (2, 11):
            score -= 2   # was -1
            warnings.append(f"Rahu in {rahu.get('house')}th house — speculative financial risk")
        if rahu.get("house") == 7:
            score -= 2   # was -1
            warnings.append("Rahu in 7th house — partnership instability, deception risk")
        if ketu.get("house") == 7:
            score -= 2   # was -1
            warnings.append("Ketu in 7th house — partner detachment or separation energy")
        if ketu.get("house") == 2:
            score -= 1
            warnings.append("Ketu in 2nd house — unexpected money outflow possible")
        if mars.get("house") == 8:
            score -= 3   # was -2: Mars in 8th = accidents, surgery, sudden loss
            warnings.append("Mars in 8th house — sudden loss risk, surgery caution")
        if str(mars.get("lord_status", "")).lower() == "maraka":
            score -= 1
            warnings.append("Mars Maraka — disruption and conflict risk")
        if saturn.get("house") == 8:
            score -= 3   # was -2: Saturn in 8th = chronic delay and obstruction
            warnings.append("Saturn in 8th house — chronic obstacles, serious delays")
        elif str(saturn.get("lord_status", "")).lower() == "yogakaraka":
            score += 1
            positives.append("Saturn Yogakaraka — disciplined long-term gains")
        elif saturn.get("house") in (6, 10):
            score += 1
            positives.append(f"Saturn in {saturn.get('house')}th — diligent, structured energy")
        if sun.get("house") == 10:
            score += 1
            positives.append("Sun in 10th house — authority and recognition energy")

        result = max(1, min(7, score))
        logger.info(f"[SCORER]   afflictions_score: {result}")
        return result

    def _calc_benefics(
        self,
        planets:       Dict,
        functional_map: Dict,
        positives:     List[str],
        warnings:      List[str],
    ) -> int:
        """
        FIX 4 + 6: Uses ascendant-based functional_map.
        Jupiter for Virgo ascendant = Maraka → does NOT get +2 benefic bonus.
        """
        score   = 4  # base (recalibrated from 5)
        mercury = planets.get("mercury", {})
        venus   = planets.get("venus",   {})
        jupiter = planets.get("jupiter", {})
        sun     = planets.get("sun",     {})
        moon    = planets.get("moon",    {})

        planet_map = {
            "Mercury": mercury, "Venus": venus,
            "Jupiter": jupiter, "Sun": sun, "Moon": moon,
        }

        for planet_name, planet in planet_map.items():
            func = functional_map.get(planet_name, "neutral")
            house = planet.get("house", 0)

            if func == "yogakaraka":
                score += 2
                positives.append(f"{planet_name} Yogakaraka — very strong benefic for this ascendant")
            elif func == "benefic":
                score += 1
                positives.append(f"{planet_name} Benefic — supportive planetary energy")
            elif func == "maraka":
                score -= 1
                warnings.append(
                    f"{planet_name} is Maraka for this ascendant — treat as caution despite natural benefic nature"
                )
            elif func == "malefic":
                score -= 1

            # House bonus (functional-status-aware)
            if func not in ("maraka", "malefic") and house in (1, 5, 9, 10, 11):
                score += 1
                positives.append(f"{planet_name} in {house}th house — strong placement")

            if planet.get("is_combust"):
                score -= 1
                warnings.append(f"{planet_name} combust — weakened, near Sun")

        result = max(1, min(8, score))
        logger.info(f"[SCORER]   benefics_score: {result}")
        return result

    def _score_category(
        self,
        planets:       Dict,
        panchang:      Dict,
        category:      str,
        functional_map: Dict,
        warnings:      List[str],
        positives:     List[str],
    ) -> int:
        jupiter = planets.get("jupiter", {})
        mercury = planets.get("mercury", {})
        venus   = planets.get("venus",   {})
        mars    = planets.get("mars",    {})
        sun     = planets.get("sun",     {})
        moon    = planets.get("moon",    {})
        saturn  = planets.get("saturn",  {})
        rahu    = planets.get("rahu",    {})

        score = 4  # recalibrated base (was 5)

        if category == "financial":
            # FIX 6: Jupiter functional status check
            jup_func = functional_map.get("Jupiter", "neutral")
            if jupiter.get("house") in (2, 11):
                if jup_func not in ("maraka", "malefic"):
                    score += 2
                    positives.append(f"Jupiter in {jupiter.get('house')}th — wealth house activation")
                else:
                    score += 0  # no bonus — Jupiter is maraka here
                    warnings.append(
                        f"Jupiter in {jupiter.get('house')}th BUT is {jup_func} for this ascendant — muted benefit"
                    )
            if jupiter.get("house") in (6, 8, 12):
                score -= 2
            ven_func = functional_map.get("Venus", "neutral")
            if venus.get("house") in (2, 11) and ven_func in ("benefic", "yogakaraka"):
                score += 2
                positives.append(f"Venus in {venus.get('house')}th — strong financial energy")
            if rahu.get("house") in (2, 11):
                warnings.append(f"Rahu in {rahu.get('house')}th — speculative risk, avoid gambling")

        elif category == "career":
            if sun.get("house") == 10:
                score += 2
                positives.append("Sun in 10th — career power and authority peak")
            if mars.get("house") == 10:
                score -= 2
                warnings.append("Mars in 10th — career conflict or sudden disruption")
            if saturn.get("house") in (6, 10):
                score += 1
                positives.append("Saturn in 6th/10th — disciplined career growth")
            if mercury.get("house") in (2, 6, 10):
                score += 1
                positives.append("Mercury in career house — communication advantage")

        elif category == "health":
            if mars.get("house") == 6:
                score += 1
                positives.append("Mars in 6th — strong disease-fighting capacity")
            if moon.get("house") in (6, 8, 12):
                score -= 2
                warnings.append("Moon in 6/8/12 — watch emotional and physical health closely")
            if saturn.get("house") in (1, 6, 8):
                score -= 1
                warnings.append("Saturn in health house — chronic conditions possible")
            if jupiter.get("house") in (1, 5, 9):
                jup_func = functional_map.get("Jupiter", "neutral")
                if jup_func not in ("maraka", "malefic"):
                    score += 1
                    positives.append("Jupiter in wellness house — healing and recovery supported")

        elif category in ("relationships", "marriage"):
            ven_func = functional_map.get("Venus", "neutral")
            if ven_func in ("benefic", "yogakaraka"):
                score += 2
                positives.append("Venus benefic for this ascendant — strong relationship harmony")
            if mars.get("house") == 7:
                score -= 2
                warnings.append("Mars in 7th — aggression in partnerships, conflict risk")
            if venus.get("house") == 7 and ven_func not in ("malefic", "maraka"):
                score += 1
                positives.append("Venus in 7th house — harmonious relationships")
            if rahu.get("house") == 7:
                score -= 1
                warnings.append("Rahu in 7th — partner surprises, need clarity in expectations")
            if saturn.get("house") == 7:
                score -= 1
                warnings.append("Saturn in 7th — delays in marriage, partner may be serious")

        elif category == "business":
            mer_func = functional_map.get("Mercury", "neutral")
            if mer_func in ("benefic", "yogakaraka"):
                score += 2
                positives.append("Mercury benefic for this ascendant — business acumen high")
            jup_func = functional_map.get("Jupiter", "neutral")
            if jup_func in ("benefic", "yogakaraka"):
                score += 1
                positives.append("Jupiter benefic — business expansion energy")
            if mars.get("lord_status", "").lower() == "maraka":
                score -= 2
                warnings.append("Mars Maraka — deal cancellation or partner conflict risk")
            if mercury.get("house") in (1, 7, 10):
                score += 1
                positives.append(f"Mercury in {mercury.get('house')}th — strong business communication")

        elif category == "legal":
            if sun.get("house") in (6, 9, 10):
                score += 2
                positives.append("Sun in legal house — authority and legal strength")
            jup_func = functional_map.get("Jupiter", "neutral")
            if jupiter.get("house") == 9 and jup_func not in ("maraka", "malefic"):
                score += 2
                positives.append("Jupiter in 9th — dharma supports, favorable legal outcome")
            if mars.get("house") in (6, 12):
                score -= 1
                warnings.append("Mars in 6th/12th — legal battles may intensify and cost more")
            if saturn.get("house") == 9:
                score -= 2
                warnings.append("Saturn in 9th — delayed justice, unfavorable court timing")

        elif category == "travel":
            if moon.get("house") == 9:
                score += 2
                positives.append("Moon in 9th house — favorable long-distance travel")
            if saturn.get("house") == 9:
                score -= 2
                warnings.append("Saturn in 9th house — travel delays, visa/permit issues possible")
            if rahu.get("house") in (9, 12):
                score += 1
                positives.append(f"Rahu in {rahu.get('house')}th — strong foreign/travel connection")
            jup_func = functional_map.get("Jupiter", "neutral")
            if jupiter.get("house") in (9, 12) and jup_func not in ("maraka", "malefic"):
                score += 1
                positives.append("Jupiter in travel house — material and spiritual foreign gains")

        result = max(1, min(7, score))
        logger.info(f"[SCORER]   category_specific ({category}) score: {result}")
        return result

    # ── Yoga / Combination Bonuses ───────────────────────────────────

    def _check_yogas(
        self,
        planets:   Dict,
        category:  str,
        positives: List[str],
        warnings:  List[str],
    ) -> int:
        jupiter = planets.get("jupiter", {})
        venus   = planets.get("venus",   {})
        mercury = planets.get("mercury", {})
        sun     = planets.get("sun",     {})
        mars    = planets.get("mars",    {})
        saturn  = planets.get("saturn",  {})
        moon    = planets.get("moon",    {})
        rahu    = planets.get("rahu",    {})

        adj = 0

        # Dhana yoga: Jupiter in 2nd or 11th + Venus benefic/yogakaraka
        if (jupiter.get("house") in (2, 11)
                and "benefic" in str(venus.get("lord_status", "")).lower()
                and "maraka" not in str(jupiter.get("lord_status", "")).lower()):
            adj += 1  # was +2, reduced to +1
            positives.append("Dhana Yoga (Jupiter wealth house + Venus benefic) — financial gains potential")

        # Raj yoga: Sun in 10th + Jupiter in 1/5/9/11
        if (sun.get("house") == 10
                and jupiter.get("house") in (1, 5, 9, 11)
                and "maraka" not in str(jupiter.get("lord_status", "")).lower()):
            adj += 1
            positives.append("Raj Yoga (Sun 10th + Jupiter auspicious) — authority peak")

        # Budh-Aditya yoga: Sun and Mercury in same house
        if sun.get("house") and sun.get("house") == mercury.get("house"):
            adj += 1
            positives.append(f"Budh-Aditya Yoga (Sun+Mercury in {sun.get('house')}th) — intelligence clarity")

        # Gajakesari yoga: Moon angular + Jupiter angular (only if Jupiter not maraka)
        if (moon.get("house") in (1, 4, 7, 10)
                and jupiter.get("house") in (1, 4, 7, 10)
                and "maraka" not in str(jupiter.get("lord_status", "")).lower()):
            adj += 1
            positives.append("Gajakesari Yoga (Moon+Jupiter angular) — wisdom and prosperity")

        # Risk combinations
        if mars.get("house") == 8 and rahu.get("house") == 2 and category == "financial":
            adj -= 2
            warnings.append("Danger Yoga (Mars 8th + Rahu 2nd) — serious sudden financial loss risk")

        if saturn.get("house") == 8 and mars.get("house") == 8:
            adj -= 2
            warnings.append("Saturn+Mars in 8th — high accident/surgery/chronic illness risk")

        if rahu.get("house") == 1 and planets.get("ketu", {}).get("house") == 7:
            adj -= 1
            warnings.append("Rahu 1st + Ketu 7th — identity confusion, partnership instability")

        result = max(-4, min(4, adj))  # FIX 8: capped at ±4 (was ±5)
        logger.info(f"[SCORER]   yoga_check adj: {result}")
        return result

    # ═══════════════════════════════════════════════════════════
    # PATH 2 — DASHAS ONLY
    # ═══════════════════════════════════════════════════════════

    def _score_dashas(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        maha   = facts.get("mahadasha",  "")
        antar  = facts.get("antardasha", "")
        scores: Dict[str, int] = {}

        logger.info(f"[SCORER]   dasha-only: maha={maha} antar={antar}")

        # FIX 7: use category-specific antar score
        m_base = DASHA_BASE.get(maha, 4)
        cat_antar = ANTAR_CATEGORY_SCORE.get(category, ANTAR_CATEGORY_SCORE["financial"])
        a_score   = cat_antar.get(antar, 3)
        combined  = max(1, min(7, round(m_base * 0.6 + a_score * 0.4)))

        scores["dasha_quality"] = combined

        cat_score = 4
        if category in ("financial", "business") and maha in ("Jupiter", "Venus", "Mercury"):
            cat_score = 6
            positives.append(f"{maha} Mahadasha — good period for {category} decisions")
        elif category == "career" and maha in ("Sun", "Saturn", "Mars"):
            cat_score = 5
            positives.append(f"{maha} Mahadasha — career-focused dasha period")
        elif category == "health" and maha in ("Rahu", "Ketu", "Saturn"):
            cat_score = 2
            warnings.append(f"{maha} Mahadasha — take care of health proactively")
        elif category in ("relationships", "marriage") and maha in ("Venus", "Moon"):
            cat_score = 6
            positives.append(f"{maha} Mahadasha — relationship-focused, auspicious period")

        scores["category_specific"] = cat_score
        scores["context_bonus"]     = 4
        scores["timing"]            = 4

        return scores, warnings, positives

    # ═══════════════════════════════════════════════════════════
    # PATH 3 — PANCHANG
    # ═══════════════════════════════════════════════════════════

    def _score_panchang(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        scores: Dict[str, int] = {}

        current_hora = facts.get("current_hora", facts.get("hora_lord", ""))
        hora_score   = self._calc_hora_score(current_hora, category, warnings, positives)
        scores["hora"] = hora_score

        tithi  = str(facts.get("tithi",  "")).lower()
        paksha = str(facts.get("paksha", "")).lower()

        if "amavasya" in tithi:
            scores["tithi"] = 1
            warnings.append("Amavasya — mental clarity low, avoid major decisions")
        elif "purnima" in tithi:
            scores["tithi"] = 7
            positives.append("Purnima — peak energy, full moon clarity")
        elif "shukla" in paksha or "waxing" in paksha:
            scores["tithi"] = 5
            positives.append("Shukla Paksha — waxing moon, favorable for new beginnings")
        elif "krishna" in paksha or "waning" in paksha:
            scores["tithi"] = 3
        elif "ekadasi" in tithi:
            scores["tithi"] = 5
            positives.append("Ekadashi — auspicious for important decisions")
        else:
            scores["tithi"] = 4

        nakshatra = str(facts.get("nakshatra", "")).lower().strip()
        scores["nakshatra"]         = self._calc_nakshatra_score(nakshatra, warnings, positives)
        scores["category_specific"] = 4

        return scores, warnings, positives

    # ═══════════════════════════════════════════════════════════
    # PATH 4 — DOSHA
    # ═══════════════════════════════════════════════════════════

    def _score_dosha(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        scores:    Dict[str, int] = {}
        has_dosha  = facts.get("has_dosha", False)
        severity   = str(facts.get("severity", "")).lower()
        dosha_type = str(facts.get("context_type", "")).lower()

        if has_dosha:
            if "high" in severity or "severe" in severity or "strong" in severity:
                base = 1
                warnings.append("Severe dosha present — strong negative planetary influence")
            elif "medium" in severity or "moderate" in severity:
                base = 3
                warnings.append("Moderate dosha present — proceed with caution")
            elif "low" in severity or "mild" in severity:
                base = 4
                warnings.append("Mild dosha present — remedies recommended")
            else:
                base = 3
                warnings.append("Dosha present — impact needs further analysis")
        else:
            base = 6
            positives.append("No significant dosha — clear planetary field")

        scores["dosha_quality"]     = base
        scores["category_specific"] = 4
        scores["timing"]            = 4
        scores["context_bonus"]     = 4

        if "mangal" in dosha_type and category in ("marriage", "relationships"):
            if has_dosha:
                scores["category_specific"] = 1
                warnings.append("Mangal Dosha active — marriage timing requires careful matching")
        elif "kaalsarp" in dosha_type and category in ("career", "financial"):
            if has_dosha:
                scores["category_specific"] = 2
                warnings.append("Kaalsarp Dosha — structured obstacles in career/financial growth")
        elif "pitra" in dosha_type:
            if has_dosha:
                scores["category_specific"] = 2
                warnings.append("Pitra Dosha — ancestral karma influencing decisions, do remedies")

        return scores, warnings, positives

    # ═══════════════════════════════════════════════════════════
    # PATH 5 — MATCHING
    # ═══════════════════════════════════════════════════════════

    def _score_matching(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        scores:    Dict[str, int] = {}
        total_pts  = facts.get("total_points",  0)
        max_pts    = facts.get("max_points",    36)
        match_pct  = facts.get("match_percent", 0)

        match_score = round((match_pct / 100) * 8)  # max 8 (recalibrated from 10)

        if match_pct >= 72:
            positives.append(f"Ashtakoot {total_pts}/{max_pts} ({match_pct}%) — excellent kundali compatibility")
        elif match_pct >= 55:
            positives.append(f"Ashtakoot {total_pts}/{max_pts} ({match_pct}%) — average-to-good compatibility")
        else:
            warnings.append(f"Ashtakoot {total_pts}/{max_pts} ({match_pct}%) — below-average, challenges likely")

        if facts.get("bulk_results"):
            best = facts.get("best_score", 0)
            avg  = facts.get("avg_score",  0)
            positives.append(f"Bulk match best={best} avg={avg} across {facts.get('total_profiles', 0)} profiles")
            if best >= max_pts * 0.7:
                match_score = 7

        scores["match_quality"] = match_score

        # ── When male natal chart was fetched, run full 7-Sutra scoring ──
        # Engine._merge_marriage_natal() stores male planets as facts["planets"].
        # We redirect to planet-based scoring for all sutras except match_quality.
        if facts.get("planets"):
            logger.info("[SCORER] PATH 5+: Marriage has natal planets — running 7-Sutra supplement")

            male_planets = facts.get("planets", {})
            female_planets = facts.get("female_planets", {})
            panchang      = facts.get("panchang", {})
            dasha         = facts.get("dasha",   {})
            transit_planets = facts.get("transit_planets", {})
            today_panchang  = facts.get("today_panchang", {})
            _raw_nakshatra  = facts.get("today_nakshatra", "")
            today_nakshatra = (
                _raw_nakshatra.get("name", "") if isinstance(_raw_nakshatra, dict)
                else str(_raw_nakshatra)
            )
            _raw_hora   = facts.get("today_hora", "")
            today_hora  = (
                _raw_hora.get("name", "") if isinstance(_raw_hora, dict) else str(_raw_hora)
            )
            hora_detail = facts.get("today_hora_detail", {})

            # Ascendant-based functional map from male chart
            ascendant_zodiac = male_planets.get("ascendant", {}).get("zodiac", "")
            functional_map   = FUNCTIONAL_STATUS.get(ascendant_zodiac, {})
            logger.info(f"[SCORER] Marriage ascendant (male): {ascendant_zodiac}")

            # Female ascendant for additional 7th-lord check
            female_ascendant = female_planets.get("ascendant", {}).get("zodiac", "") if female_planets else ""
            if female_ascendant and female_ascendant != ascendant_zodiac:
                f_func_map = FUNCTIONAL_STATUS.get(female_ascendant, {})
                # Check female Venus functional role
                f_venus_func = f_func_map.get("Venus", "neutral")
                if f_venus_func in ("benefic", "yogakaraka"):
                    scores["female_venus_bonus"] = 1
                    positives.append(f"Female chart: Venus {f_venus_func} for {female_ascendant} ascendant — harmony")
                elif f_venus_func in ("malefic", "maraka"):
                    scores["female_venus_penalty"] = -1
                    warnings.append(f"Female chart: Venus {f_venus_func} for {female_ascendant} ascendant — friction")

            # Dasha quality (male)
            current_dasa = dasha.get("current_dasa", "")
            if current_dasa and "undefined" not in str(current_dasa).lower():
                parts = str(current_dasa).split(">")
                maha  = parts[0].strip() if parts else ""
                antar = parts[1].strip() if len(parts) > 1 else ""
                scores["dasha_quality"] = self._calc_dasha_score(
                    maha, antar, "marriage", male_planets, functional_map, warnings, positives
                )
            else:
                scores["dasha_quality"] = 4

            # Moon state (male natal)
            moon = male_planets.get("moon", {})
            scores["moon_state"] = self._calc_moon_score(moon, functional_map, warnings, positives)

            # Today's nakshatra
            if today_nakshatra:
                day_nak = today_nakshatra.lower().strip()
            elif today_panchang.get("nakshatra"):
                _pn = today_panchang["nakshatra"]
                day_nak = (_pn.get("name","") if isinstance(_pn,dict) else str(_pn)).lower().strip()
            else:
                day_nak = facts.get("nakshatra", moon.get("nakshatra","")).lower().strip()
            scores["nakshatra"] = self._calc_nakshatra_score(day_nak, warnings, positives)

            # Hora
            if today_hora:
                live_hora = today_hora
            elif today_panchang.get("hora_lord"):
                live_hora = today_panchang["hora_lord"]
            else:
                live_hora = panchang.get("hora_lord", "")
            scores["hora"] = self._calc_hora_score(live_hora, "marriage", warnings, positives, hora_detail)

            # Malefic afflictions (male chart)
            scores["afflictions"] = self._calc_afflictions(male_planets, warnings, positives)

            # Functional benefics (male chart)
            scores["benefics"] = self._calc_benefics(male_planets, functional_map, positives, warnings)

            # Category-specific: 7th house analysis using male chart
            venus_m = male_planets.get("venus", {})
            mars_m  = male_planets.get("mars",  {})
            saturn_m = male_planets.get("saturn",{})
            rahu_m  = male_planets.get("rahu",  {})
            ven_func = functional_map.get("Venus", "neutral")
            cat_score = 4
            if ven_func in ("benefic", "yogakaraka"):
                cat_score += 2
                positives.append(f"Venus {ven_func} for {ascendant_zodiac} ascendant — strong marriage harmony")
            elif ven_func in ("malefic", "maraka"):
                cat_score -= 1
                warnings.append(f"Venus {ven_func} for {ascendant_zodiac} ascendant — marriage needs extra care")
            if mars_m.get("house") == 7:
                cat_score -= 2
                warnings.append("Mars in 7th (male chart) — Mangal Dosha check essential")
            if venus_m.get("house") == 7 and ven_func not in ("malefic","maraka"):
                cat_score += 1
                positives.append("Venus in 7th house (male) — strong love and marital happiness")
            if rahu_m.get("house") == 7:
                cat_score -= 1
                warnings.append("Rahu in 7th (male chart) — partner may have unconventional background")
            if saturn_m.get("house") == 7:
                cat_score -= 1
                warnings.append("Saturn in 7th (male chart) — delayed or serious marriage")
            scores["category_specific"] = max(1, min(7, cat_score))

            # Transit
            if transit_planets:
                scores["transit_gochar"] = self._calc_transit_score(
                    transit_planets, male_planets, "marriage", warnings, positives,
                    ashtakavarga=facts.get("ashtakavarga", {}),
                )

            # Yoga check
            yoga_adj = self._check_yogas(male_planets, "marriage", positives, warnings)
            if yoga_adj != 0:
                scores["yoga_bonus"] = yoga_adj

        else:
            # No natal data — pure match score path (old behaviour)
            scores["category_specific"] = match_score
            scores["context_bonus"]     = 4
            scores["timing"]            = 4

        return scores, warnings, positives

    # ═══════════════════════════════════════════════════════════
    # PATH 6 — PREDICTION TEXT
    # ═══════════════════════════════════════════════════════════

    def _score_prediction(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        scores: Dict[str, int] = {}
        text = str(facts.get("prediction_text", "")).lower()

        positive_words = {
            "favorable", "good", "excellent", "gain", "success", "profit",
            "benefit", "auspicious", "positive", "growth", "fortunate",
            "blessing", "achievement", "progress", "opportunity",
            "शुभ", "लाभ", "सफलता", "अनुकूल", "उन्नति",
        }
        negative_words = {
            "avoid", "caution", "loss", "delay", "conflict", "difficult",
            "problem", "obstacle", "bad", "warning", "struggle", "challenge",
            "setback", "unfavorable", "risk", "danger",
            "सावधान", "हानि", "बाधा", "कठिन", "अशुभ",
        }

        pos_count = sum(1 for w in positive_words if w in text)
        neg_count = sum(1 for w in negative_words if w in text)

        if pos_count > neg_count:
            sentiment_score = min(6, 4 + pos_count)
            positives.append(f"Prediction indicates favorable conditions for {category}")
        elif neg_count > pos_count:
            sentiment_score = max(1, 4 - neg_count)
            warnings.append(f"Prediction indicates caution needed for {category}")
        else:
            sentiment_score = 4

        scores["prediction_sentiment"] = sentiment_score
        scores["category_specific"]    = sentiment_score
        scores["timing"]               = 4
        scores["context_bonus"]        = 4

        return scores, warnings, positives

    # ═══════════════════════════════════════════════════════════
    # PATH 7 — UTILITIES
    # ═══════════════════════════════════════════════════════════

    def _score_utilities(
        self, facts: Dict, category: str, warnings: List[str], positives: List[str]
    ) -> Tuple[Dict[str, int], List[str], List[str]]:
        scores: Dict[str, int] = {
            "context_bonus":     4,
            "category_specific": 4,
            "timing":            4,
            "base":              4,
        }
        positives.append("Utility lookup complete — use results for informed decision-making")
        return scores, warnings, positives
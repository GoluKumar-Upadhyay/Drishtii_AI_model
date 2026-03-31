
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import httpx

from .model import DrishtiiRequest, CATEGORY_LABELS
from .Data_extractor import DataExtractor, _compute_hora_lord, get_hora_detail
from .Scorer import Scorer
from .Prompt_builder import PromptBuilder
from .Gemini_client import GeminiClient
from .Validator import ResponseValidator

logger = logging.getLogger("drishtii.engine")

# ── VedicAPI group → URL path ───────────────────────────────────
GROUP_TO_ENDPOINT: dict[str, str] = {
    "horoscope":  "horoscope",
    "prediction": "prediction",
    "panchang":   "panchang",
    "dashas":     "dashas",
    "dosha":      "dosha",
    "extended":   "extended-horoscope",
    "matching":   "matching",
    "utilities":  "utilities",
}

GEMINI_MAX_RETRIES = 2

NEEDS_DASHA_SUPPLEMENT = {
    "planet-details",
    "planets-by-houses",
    "aspects",
    "personal-characteristics",
}

# APIs that also need transit + panchang for the target date
NEEDS_TRANSIT_SUPPLEMENT = {
    "planet-details",
    "planets-by-houses",
    "aspects",
    "personal-characteristics",
}

# APIs that benefit from Ashtakavarga (binnashtakavarga) supplement.
# Binnashtakavarga gives per-planet transit strength (0-8 pts per house).
# Critical for accurate transit scoring — without it gochar is guesswork.
NEEDS_ASHTAKAVARGA_SUPPLEMENT = {
    "planet-details",
    "planets-by-houses",
    "personal-characteristics",
}

# ── Tone map: score range → Gemini tone instruction ─────────────
TONE_MAP: list[tuple[int, str, str]] = [
    (35, "STRONG_GO",    "strongly positive and encouraging"),
    (28, "MODERATE_GO",  "cautiously positive with measured optimism"),
    (20, "CAUTION",      "neutral and cautiously advisory"),
    (0,  "NO_GO",        "honest and discouraging — this is not the right time"),
]

# ── Risk planets whose presence in warnings triggers a post-guard ─
CRITICAL_WARNING_TRIGGERS = {
    "Mars in 8th",
    "Saturn in 8th",
    "Rahu in 1st",
    "Moon combust",
    "Triple malefic",
}


def _get_tone(total_score: int) -> tuple[str, str]:
    """Returns (tone_label, tone_instruction) for given score."""
    for min_s, label, instruction in TONE_MAP:
        if total_score >= min_s:
            return label, instruction
    return "NO_GO", "discouraging — this is not the right time"


def _parse_target_date(target_date: str | None) -> str:
    """
    Converts target_date (DD/MM/YYYY or DD-MM-YYYY) to DD/MM/YYYY.
    Falls back to today if None or unparseable.
    """
    if not target_date:
        return datetime.now().strftime("%d/%m/%Y")
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(target_date, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return datetime.now().strftime("%d/%m/%Y")


class DrishtiiEngine:

    def __init__(self, redis_client=None):
        self.redis          = redis_client
        self.vedic_base_url = os.getenv("VEDIC_BASE_URL", "https://json.vedicastroapi.com/v3-json")
        self.vedic_api_key  = os.getenv("VEDIC_API_KEY", "")
        self.http_timeout   = int(os.getenv("DRISHTII_HTTP_TIMEOUT", "20"))

    # ─────────────────────────────────────────────────────────────
    # MAIN ENTRY
    # ─────────────────────────────────────────────────────────────

    async def analyze(self, request: DrishtiiRequest) -> dict:
        overall_start = time.time()
        rid           = self._request_id(request)
        chart_hash    = request.get_chart_hash()
        norm_params   = request.get_normalized_params()

        logger.info(
            f"\n{'='*70}\n"
            f"[D-{rid}] ▶ REQUEST\n"
            f"  Category   : {request.category}\n"
            f"  Group/API  : {request.group}/{request.api}  [auto-resolved]\n"
            f"  Language   : {request.language}\n"
            f"  Target     : {request.target_date or 'not specified'}\n"
            f"  ChartHash  : {chart_hash}  [deterministic]\n"
            f"  NormParams : {json.dumps(norm_params, indent=4)}\n"
            f"{'='*70}"
        )

        phase_times: dict[str, float] = {}

        # ── PHASE 1: Primary VedicAPI Call ──────────────────────
        t = time.time()
        logger.info(f"[D-{rid}] ── PHASE 1: VedicAPI (primary) ──")
        api_response = await self._fetch_vedic_by_group_api(
            request.group, request.api, norm_params, rid
        )
        phase_times["vedic_primary"] = round(time.time() - t, 2)

        # ── PHASE 1b: Supplemental Dasha Call ─────────────────
        dasha_response       = None
        male_horoscope_resp  = None   # marriage: male natal chart for ascendant/planets
        female_horoscope_resp= None   # marriage: female natal chart
        apis_used            = [f"{request.group}/{request.api}"]

        if request.group == "horoscope" and request.api in NEEDS_DASHA_SUPPLEMENT:
            logger.info(f"[D-{rid}] ── PHASE 1b: Auto dasha call ──")
            t = time.time()
            try:
                dasha_response = await self._fetch_vedic_by_group_api(
                    "dashas", "current-mahadasha-full", norm_params, rid
                )
                apis_used.append("dashas/current-mahadasha-full")
                logger.info(f"[D-{rid}] ✅ Dasha supplement received")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Dasha supplement failed (non-fatal): {e}")
            phase_times["vedic_dasha"] = round(time.time() - t, 2)

        # ── PHASE 1b-MARRIAGE: Fetch male & female natal charts + dasha ──
        # The matching/ashtakoot API returns compatibility data only (no planets).
        # To get ascendant-based scoring, functional roles, and dasha we must
        # separately fetch planet-details and mahadasha using each person's params.
        if request.category == "marriage":
            logger.info(f"[D-{rid}] ── PHASE 1b-MARRIAGE: Male/Female natal + dasha calls ──")
            t = time.time()

            # Build single-person params from boy_ / girl_ keys in norm_params
            male_params = {
                "dob":  norm_params.get("boy_dob", ""),
                "tob":  norm_params.get("boy_tob", ""),
                "lat":  norm_params.get("boy_lat", ""),
                "lon":  norm_params.get("boy_lon", ""),
                "tz":   norm_params.get("boy_tz",  ""),
                "lang": "en",
            }
            female_params = {
                "dob":  norm_params.get("girl_dob", ""),
                "tob":  norm_params.get("girl_tob", ""),
                "lat":  norm_params.get("girl_lat", ""),
                "lon":  norm_params.get("girl_lon", ""),
                "tz":   norm_params.get("girl_tz",  ""),
                "lang": "en",
            }

            # Fetch male planet details (for ascendant, house positions, functional roles)
            try:
                male_horoscope_resp = await self._fetch_vedic_by_group_api(
                    "horoscope", "planet-details", male_params, rid,
                    cache_suffix="_male_natal"
                )
                apis_used.append("horoscope/planet-details[male]")
                logger.info(f"[D-{rid}] ✅ Male natal chart received")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Male natal chart failed (non-fatal): {e}")

            # Fetch female planet details
            try:
                female_horoscope_resp = await self._fetch_vedic_by_group_api(
                    "horoscope", "planet-details", female_params, rid,
                    cache_suffix="_female_natal"
                )
                apis_used.append("horoscope/planet-details[female]")
                logger.info(f"[D-{rid}] ✅ Female natal chart received")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Female natal chart failed (non-fatal): {e}")

            # Fetch male Mahadasha (dasha-based timing check for marriage)
            try:
                dasha_response = await self._fetch_vedic_by_group_api(
                    "dashas", "current-mahadasha-full", male_params, rid,
                    cache_suffix="_male_dasha"
                )
                apis_used.append("dashas/current-mahadasha-full[male]")
                logger.info(f"[D-{rid}] ✅ Male dasha received")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Male dasha failed (non-fatal): {e}")

            phase_times["vedic_marriage_supplements"] = round(time.time() - t, 2)

        # ── PHASE 1c: Transit (Gochar) Call for target_date ────
        # This is the MOST IMPORTANT fix — planets TODAY vs natal houses
        transit_response  = None
        panchang_response = None

        # For horoscope categories use norm_params directly;
        # for marriage use male_params (birth coords of the male person)
        _transit_base = (
            {
                "dob": norm_params.get("boy_dob", ""),
                "tob": norm_params.get("boy_tob", ""),
                "lat": norm_params.get("boy_lat", ""),
                "lon": norm_params.get("boy_lon", ""),
                "tz":  norm_params.get("boy_tz",  ""),
            }
            if request.category == "marriage"
            else {
                "dob": norm_params.get("dob", ""),
                "tob": norm_params.get("tob", ""),
                "lat": norm_params.get("lat", ""),
                "lon": norm_params.get("lon", ""),
                "tz":  norm_params.get("tz",  ""),
            }
        )

        needs_transit = (
            (request.group == "horoscope" and request.api in NEEDS_TRANSIT_SUPPLEMENT)
            or request.category == "marriage"
        )

        if needs_transit:
            target_dmy = _parse_target_date(request.target_date)
            logger.info(f"[D-{rid}] ── PHASE 1c: Transit (Gochar) call for {target_dmy} ──")
            t = time.time()

            transit_params = {**_transit_base, "lang": "en", "date": target_dmy}

            try:
                transit_response = await self._fetch_vedic_by_group_api(
                    "horoscope", "planet-details", transit_params, rid,
                    cache_suffix="_transit_" + target_dmy.replace("/", "")
                )
                apis_used.append(f"horoscope/planet-details[transit:{target_dmy}]")
                logger.info(f"[D-{rid}] ✅ Transit data received for {target_dmy}")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Transit call failed (non-fatal): {e}")

            # ── PHASE 1d: Panchang for target_date ───────────
            logger.info(f"[D-{rid}] ── PHASE 1d: Panchang call for {target_dmy} ──")

            panchang_params = {
                "date": target_dmy,
                "lat":  _transit_base.get("lat", ""),
                "lon":  _transit_base.get("lon", ""),
                "tz":   _transit_base.get("tz",  ""),
                "lang": "en",
            }

            try:
                panchang_response = await self._fetch_vedic_by_group_api(
                    "panchang", "panchang", panchang_params, rid,
                    cache_suffix="_panchang_" + target_dmy.replace("/", "")
                )
                apis_used.append(f"panchang/panchang[{target_dmy}]")
                logger.info(f"[D-{rid}] ✅ Panchang received for {target_dmy}")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Panchang call failed (non-fatal): {e}")

            phase_times["vedic_transit"] = round(time.time() - t, 2)

        # ── PHASE 1e: Ashtakavarga (Binnashtakavarga) Supplement ──────
        # Gives per-planet house strength (0-8 bindus per house).
        # Used by Scorer to weight transit quality rather than using
        # generic good/bad house lists alone.
        # Fetched for horoscope APIs and marriage (using male params).
        ashtakavarga_response = None
        _astaka_params = (
            {
                "dob": norm_params.get("boy_dob", ""),
                "tob": norm_params.get("boy_tob", ""),
                "lat": norm_params.get("boy_lat", ""),
                "lon": norm_params.get("boy_lon", ""),
                "tz":  norm_params.get("boy_tz",  ""),
                "lang": "en",
            }
            if request.category == "marriage"
            else norm_params
        )

        needs_ashtakavarga = (
            (request.group == "horoscope" and request.api in NEEDS_ASHTAKAVARGA_SUPPLEMENT)
            or request.category == "marriage"
        )

        if needs_ashtakavarga and _astaka_params.get("dob"):
            logger.info(f"[D-{rid}] ── PHASE 1e: Ashtakavarga supplement ──")
            t_astaka = time.time()
            try:
                ashtakavarga_response = await self._fetch_vedic_by_group_api(
                    "horoscope", "binnashtakvarga", _astaka_params, rid,
                    cache_suffix="_binnashtakavarga"
                )
                apis_used.append("horoscope/binnashtakvarga")
                logger.info(f"[D-{rid}] ✅ Ashtakavarga received")
            except Exception as e:
                logger.warning(f"[D-{rid}] ⚠️ Ashtakavarga call failed (non-fatal): {e}")
            phase_times["vedic_ashtakavarga"] = round(time.time() - t_astaka, 2)

        # ── PHASE 2: Data Extraction ────────────────────────────
        t = time.time()
        logger.info(f"[D-{rid}] ── PHASE 2: Extraction ──")
        extractor = DataExtractor()
        key_facts = await extractor.extract(
            api_response   = api_response,
            group          = request.group,
            api            = request.api,
            request_params = norm_params,
        )
        if dasha_response:
            key_facts = self._merge_dasha_data(key_facts, dasha_response, rid)

        # ── Marriage: merge male/female natal charts ─────────
        # Injects "planets" (male ascendant + house data) so the full
        # 7-Sutra planet-based scoring path runs alongside match scoring.
        if request.category == "marriage":
            key_facts = self._merge_marriage_natal(
                key_facts,
                male_horoscope_resp,
                female_horoscope_resp,
                rid,
            )

        # Store target_date so downstream merges (hora computation) can use it
        key_facts["target_date"] = _parse_target_date(request.target_date)

        # ── Merge Transit data into key_facts ──────────────────
        if transit_response:
            key_facts = self._merge_transit_data(key_facts, transit_response, rid)

        # ── Merge Panchang data into key_facts ─────────────────
        if panchang_response:
            key_facts = self._merge_panchang_data(key_facts, panchang_response, rid)

        # ── Merge Ashtakavarga into key_facts ──────────────────
        if ashtakavarga_response:
            key_facts = self._merge_ashtakavarga_data(key_facts, ashtakavarga_response, rid)

        # ── DATA QUALITY VALIDATION ─────────────────────────────
        data_quality = self._validate_data_quality(key_facts, rid)
        phase_times["extraction"] = round(time.time() - t, 2)

        # ── PHASE 3: Scoring ─────────────────────────────────────
        t = time.time()
        logger.info(f"[D-{rid}] ── PHASE 3: Scoring ──")
        scorer    = Scorer()
        scorecard = scorer.score(
            key_facts   = key_facts,
            category    = request.category,
            group       = request.group,
            api         = request.api,
            target_date = request.target_date,
        )
        scorecard["category"] = request.category

        # ── Apply data-quality caps ─────────────────────────────
        if data_quality["dasha_missing"]:
            scorecard["total_score"] = min(scorecard["total_score"], 30)
            if scorecard.get("verdict") == "GO" and scorecard["total_score"] < 30:
                scorecard["verdict"] = "CAUTION"
            scorecard["warnings"].append(
                "⚠️ Dasha data unavailable — confidence limited to MEDIUM/LOW"
            )

        if data_quality["incomplete_chart"]:
            scorecard["total_score"] = min(scorecard["total_score"], 25)
            scorecard["warnings"].append(
                "⚠️ Incomplete birth chart (< 9 planets) — analysis may be partial"
            )

        # Re-derive verdict after caps
        total = scorecard["total_score"]
        if total >= 30:
            scorecard["verdict"] = "GO"
        elif total >= 20:
            scorecard["verdict"] = "CAUTION"
        else:
            scorecard["verdict"] = "AVOID"

        # ── Tone controller ─────────────────────────────────────
        tone_label, tone_instruction = _get_tone(scorecard["total_score"])
        scorecard["tone_label"]       = tone_label
        scorecard["tone_instruction"] = tone_instruction

        logger.info(
            f"[D-{rid}] ✅ Scorecard | "
            f"verdict={scorecard['verdict']} score={scorecard['total_score']}/40 "
            f"tone={tone_label} "
            f"warnings={len(scorecard.get('warnings', []))} "
            f"positives={len(scorecard.get('positives', []))}"
        )
        phase_times["scoring"] = round(time.time() - t, 2)

        # ── PHASE 4: Prompt Build ────────────────────────────────
        t = time.time()
        logger.info(f"[D-{rid}] ── PHASE 4: Prompt ──")
        prompt = PromptBuilder().build(
            scorecard   = scorecard,
            key_facts   = key_facts,
            objective   = request.objective,
            category    = request.category,
            language    = request.language,
            group       = request.group,
            api         = request.api,
            target_date = request.target_date,
        )
        phase_times["prompt_build"] = round(time.time() - t, 2)

        # ── PHASE 5: Gemini (with retry) ────────────────────────
        t = time.time()
        logger.info(f"[D-{rid}] ── PHASE 5: Gemini ──")
        gemini_dict, is_valid, validation_reason = await self._generate_with_retry(
            request   = request,
            scorecard = scorecard,
            key_facts = key_facts,
            prompt    = prompt,
            rid       = rid,
        )
        phase_times["gemini"] = round(time.time() - t, 2)

        # ── POST-GUARD ───────────────────────────────────────────
        gemini_dict = self._post_guard(gemini_dict, scorecard, rid)

        # ── FINAL RESPONSE ───────────────────────────────────────
        total_time = round(time.time() - overall_start, 2)
        logger.info(
            f"[D-{rid}] ✅ COMPLETE in {total_time}s | phases={phase_times} | "
            f"verdict={scorecard['verdict']} score={scorecard['total_score']}/40 "
            f"valid={is_valid}"
        )

        return self._build_final_response(
            request           = request,
            scorecard         = scorecard,
            gemini_dict       = gemini_dict,
            is_valid          = is_valid,
            validation_reason = validation_reason,
            total_time        = total_time,
            apis_used         = apis_used,
            phase_times       = phase_times,
            data_quality      = data_quality,
        )

    # ─────────────────────────────────────────────────────────────
    # MARRIAGE NATAL MERGE  ← NEW in v4
    # ─────────────────────────────────────────────────────────────

    def _merge_marriage_natal(
        self,
        key_facts:             dict,
        male_horoscope_resp:   dict | None,
        female_horoscope_resp: dict | None,
        rid:                   str,
    ) -> dict:
        """
        For the marriage category the primary API (matching/ashtakoot) returns
        only compatibility scores — no planet positions, no ascendant.

        This merge:
          1. Extracts male natal planets → stored as key_facts["planets"]
             (drives the full 7-Sutra ascendant-aware scoring path)
          2. Extracts female natal planets → stored as key_facts["female_planets"]
          3. Marks context so Scorer knows it has BOTH matching data AND natal data.
        """
        extractor = DataExtractor()

        if male_horoscope_resp:
            try:
                raw_m = male_horoscope_resp.get("response", male_horoscope_resp)
                male_facts = extractor._extract_planets_block(raw_m, "planet-details")
                male_planets = male_facts.get("planets", {})
                if male_planets:
                    key_facts["planets"] = male_planets
                    key_facts["panchang"] = male_facts.get("panchang", {})
                    # Preserve match data alongside natal
                    key_facts["has_marriage_natal"] = True
                    ascendant = male_planets.get("ascendant", {}).get("zodiac", "?")
                    logger.info(
                        f"[D-{rid}] ✅ Male natal merged | "
                        f"planets={len(male_planets)} ascendant={ascendant}"
                    )
                else:
                    logger.warning(f"[D-{rid}] ⚠️ Male natal: no planets extracted")
            except Exception as e:
                logger.error(f"[D-{rid}] ❌ Male natal merge failed: {e}", exc_info=True)

        if female_horoscope_resp:
            try:
                raw_f = female_horoscope_resp.get("response", female_horoscope_resp)
                female_facts = extractor._extract_planets_block(raw_f, "planet-details")
                female_planets = female_facts.get("planets", {})
                if female_planets:
                    key_facts["female_planets"] = female_planets
                    ascendant_f = female_planets.get("ascendant", {}).get("zodiac", "?")
                    logger.info(
                        f"[D-{rid}] ✅ Female natal merged | "
                        f"planets={len(female_planets)} ascendant={ascendant_f}"
                    )
                else:
                    logger.warning(f"[D-{rid}] ⚠️ Female natal: no planets extracted")
            except Exception as e:
                logger.error(f"[D-{rid}] ❌ Female natal merge failed: {e}", exc_info=True)

        return key_facts

    # ─────────────────────────────────────────────────────────────
    # ASHTAKAVARGA MERGE  ← NEW in v4
    # ─────────────────────────────────────────────────────────────

    def _merge_ashtakavarga_data(self, key_facts: dict, astaka_response: dict, rid: str) -> dict:
        """
        Extracts binnashtakavarga data (per-planet house bindus) and stores in
        key_facts["ashtakavarga"].

        Structure stored:
          {
            "jupiter": {1: 4, 2: 3, 3: 5, ...},   # bindus in each natal house
            "venus":   {1: 2, 2: 6, ...},
            ...
          }

        The Scorer uses this to replace generic good/bad transit house lists with
        actual bindu-strength values (0-8 per house).  ≥5 = strong, ≤3 = weak.
        """
        try:
            raw = astaka_response.get("response", astaka_response)
            # Binnashtakavarga response is a list of planet objects or a dict keyed by planet
            # VedicAPI returns: [{"name": "Jupiter", "houses": {"1": 4, "2": 3, ...}}, ...]
            astaka_map: dict = {}

            if isinstance(raw, list):
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name", item.get("planet", ""))).lower()
                    houses_raw = item.get("houses", item.get("house_scores", item.get("bindus", {})))
                    if name and isinstance(houses_raw, dict):
                        astaka_map[name] = {int(k): int(v) for k, v in houses_raw.items() if str(k).isdigit()}
            elif isinstance(raw, dict):
                # Some responses: {"Jupiter": {"1": 4, ...}, ...}
                for planet_name, houses_raw in raw.items():
                    if isinstance(houses_raw, dict):
                        name = planet_name.lower()
                        astaka_map[name] = {int(k): int(v) for k, v in houses_raw.items() if str(k).isdigit()}

            if astaka_map:
                key_facts["ashtakavarga"] = astaka_map
                planet_count = len(astaka_map)
                sample = {p: astaka_map[p].get(1, "?") for p in list(astaka_map.keys())[:3]}
                logger.info(
                    f"[D-{rid}] ✅ Ashtakavarga merged | "
                    f"planets={planet_count} sample_h1_bindus={sample}"
                )
            else:
                logger.warning(f"[D-{rid}] ⚠️ Ashtakavarga merge: could not parse response | raw_type={type(raw).__name__}")

        except Exception as e:
            logger.error(f"[D-{rid}] ❌ Ashtakavarga merge failed: {e}", exc_info=True)

        return key_facts

    # ─────────────────────────────────────────────────────────────
    # TRANSIT MERGE  ← NEW in v3
    # ─────────────────────────────────────────────────────────────

    def _merge_transit_data(self, key_facts: dict, transit_response: dict, rid: str) -> dict:
        """
        Extracts today's planetary positions from the transit API response
        and stores them in key_facts["transit_planets"].

        These are the planets' positions TODAY (target_date), NOT birth positions.
        The Scorer uses these to calculate gochar (transit) effects over natal houses.
        """
        try:
            extractor = DataExtractor()
            raw       = transit_response.get("response", transit_response)

            # Reuse the planet extractor — same structure as natal
            transit_facts = extractor._extract_planets_block(raw, "planet-details")
            transit_planets = transit_facts.get("planets", {})
            transit_panchang = transit_facts.get("panchang", {})

            if transit_planets:
                key_facts["transit_planets"] = transit_planets
                logger.info(
                    f"[D-{rid}] ✅ Transit merged | "
                    f"planets={len(transit_planets)} "
                    f"transit_hora={transit_panchang.get('hora_lord','?')} "
                    f"transit_nakshatra={transit_facts.get('nakshatra','?')}"
                )
            else:
                logger.warning(f"[D-{rid}] ⚠️ Transit merge: no planets extracted")

            # Also store today's panchang from transit response (hora, nakshatra, tithi)
            if transit_panchang and not key_facts.get("today_panchang"):
                key_facts["today_panchang"] = transit_panchang
                key_facts["today_nakshatra"] = transit_facts.get("nakshatra", "")

        except Exception as e:
            logger.error(f"[D-{rid}] ❌ Transit merge failed: {e}", exc_info=True)

        return key_facts

    # ─────────────────────────────────────────────────────────────
    # PANCHANG MERGE  ← NEW in v3
    # ─────────────────────────────────────────────────────────────

    def _merge_panchang_data(self, key_facts: dict, panchang_response: dict, rid: str) -> dict:
        """
        Extracts today's panchang (nakshatra, hora, tithi, paksha, yoga)
        from the panchang API response and merges into key_facts["today_panchang"].

        This gives us the LIVE values for Sutra 3 (Nakshatra) and Sutra 4 (Hora).
        """
        try:
            extractor = DataExtractor()
            raw       = panchang_response.get("response", panchang_response)

            panchang_facts = extractor._extract_panchang(raw, "panchang")

            if panchang_facts:
                # today_panchang is authoritative — overwrites transit-derived panchang
                key_facts["today_panchang"]  = panchang_facts
                key_facts["today_nakshatra"] = panchang_facts.get("nakshatra", "")
                key_facts["today_tithi"]     = panchang_facts.get("tithi", "")
                key_facts["today_paksha"]    = panchang_facts.get("paksha", "")
                key_facts["today_yoga"]      = panchang_facts.get("yoga", "")

                hora = panchang_facts.get("hora_lord", "")
                # If extractor computed hora without knowing target_date, recompute
                # now that we have the correct date + sunrise from the panchang response.
                if not hora or hora == _compute_hora_lord(None, None):
                    target_dmy = key_facts.get("target_date", "")
                    sunrise    = panchang_facts.get("sunrise", "")
                    hora       = _compute_hora_lord(target_dmy, sunrise)
                    logger.info(
                        f"[D-{rid}] ℹ️ Hora recomputed with target_date={target_dmy} "
                        f"sunrise={sunrise} → {hora}"
                    )

                key_facts["today_hora"]        = hora
                key_facts["today_hora_detail"] = get_hora_detail()

                logger.info(
                    f"[D-{rid}] ✅ Panchang merged | "
                    f"nakshatra={key_facts['today_nakshatra']} "
                    f"hora={key_facts['today_hora']} "
                    f"tithi={key_facts['today_tithi']} "
                    f"paksha={key_facts['today_paksha']}"
                )
            else:
                logger.warning(f"[D-{rid}] ⚠️ Panchang merge: empty facts")

        except Exception as e:
            logger.error(f"[D-{rid}] ❌ Panchang merge failed: {e}", exc_info=True)

        return key_facts

    # ─────────────────────────────────────────────────────────────
    # DATA QUALITY VALIDATOR (pre-scoring)
    # ─────────────────────────────────────────────────────────────

    def _validate_data_quality(self, key_facts: dict, rid: str) -> dict[str, Any]:
        planets = key_facts.get("planets", {})
        dasha   = key_facts.get("dasha",   {})

        planet_count = len(planets)
        maha_lord    = dasha.get("mahadasha", "")
        current_dasa = dasha.get("current_dasa", "")

        dasha_missing = (
            not maha_lord
            or "undefined" in str(maha_lord).lower()
            or str(maha_lord).strip() == ""
        ) and (
            not current_dasa
            or "undefined" in str(current_dasa).lower()
        )

        incomplete_chart = 0 < planet_count < 9

        quality = {
            "planet_count":       planet_count,
            "dasha_missing":      dasha_missing,
            "incomplete_chart":   incomplete_chart,
            "transit_available":  bool(key_facts.get("transit_planets")),
            "panchang_available": bool(key_facts.get("today_panchang")),
            "flags":              [],
        }

        if dasha_missing:
            quality["flags"].append("DASHA_MISSING")
        if incomplete_chart:
            quality["flags"].append(f"INCOMPLETE_CHART ({planet_count}/10 planets)")
        if not quality["transit_available"]:
            quality["flags"].append("NO_TRANSIT_DATA")
        if not quality["panchang_available"]:
            quality["flags"].append("NO_PANCHANG_DATA")

        logger.info(
            f"[D-{rid}] 🔍 Data quality | "
            f"planets={planet_count} "
            f"dasha_missing={dasha_missing} "
            f"incomplete={incomplete_chart} "
            f"transit={quality['transit_available']} "
            f"panchang={quality['panchang_available']} "
            f"flags={quality['flags']}"
        )
        return quality

    # ─────────────────────────────────────────────────────────────
    # POST-GUARD (after Gemini)
    # ─────────────────────────────────────────────────────────────

    def _post_guard(self, gemini_dict: dict, scorecard: dict, rid: str) -> dict:
        total    = scorecard.get("total_score", 20)
        warnings = scorecard.get("warnings", [])
        positives = scorecard.get("positives", [])
        verdict  = scorecard.get("verdict", "CAUTION")

        # Rule 1: high score but barely any positives → downgrade confidence
        if total >= 35 and len(positives) < 2:
            logger.warning(
                f"[D-{rid}] POST-GUARD: score={total} but positives={len(positives)} "
                "— downgrading confidence to MEDIUM"
            )
            gemini_dict["confidence"] = "MEDIUM"

        # Rule 2: critical warnings present → ensure risk_factors are non-empty
        critical_active = [
            w for w in warnings
            if any(trigger.lower() in w.lower() for trigger in CRITICAL_WARNING_TRIGGERS)
        ]
        if critical_active:
            risk_factors = gemini_dict.get("category_analysis", {}).get("risk_factors", [])
            if not risk_factors:
                gemini_dict.setdefault("category_analysis", {})["risk_factors"] = [
                    f"Warning: {w}" for w in critical_active[:2]
                ]
            logger.info(
                f"[D-{rid}] POST-GUARD: critical warnings injected into risk_factors"
            )

        # Rule 3: AVOID verdict but confidence is HIGH → force down to MEDIUM
        if verdict == "AVOID" and gemini_dict.get("confidence") == "HIGH":
            logger.warning(
                f"[D-{rid}] POST-GUARD: AVOID verdict cannot have HIGH confidence — "
                "forcing to MEDIUM"
            )
            gemini_dict["confidence"] = "MEDIUM"

        return gemini_dict

    # ─────────────────────────────────────────────────────────────
    # DASHA MERGE
    # ─────────────────────────────────────────────────────────────

    def _merge_dasha_data(self, key_facts: dict, dasha_response: dict, rid: str) -> dict:
        try:
            extractor = DataExtractor()
            raw       = dasha_response.get("response", dasha_response)
            logger.info(
                f"[D-{rid}] 🔍 Dasha raw keys: "
                f"{list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__}"
            )

            dasha_facts = extractor._extract_dashas(raw, "current-mahadasha-full")

            maha_lord  = dasha_facts.get("mahadasha",  "")
            antar_lord = dasha_facts.get("antardasha", "")
            end_date   = dasha_facts.get("maha_end_date", "")
            prat_lord  = dasha_facts.get("pratyantar_dasha", "")

            if maha_lord and str(maha_lord).strip():
                parts            = [p for p in [maha_lord, antar_lord, prat_lord] if p and str(p).strip()]
                current_dasa_str = ">".join(parts)

                if "dasha" not in key_facts:
                    key_facts["dasha"] = {}

                key_facts["dasha"]["current_dasa"]    = current_dasa_str
                key_facts["dasha"]["mahadasha"]        = maha_lord
                key_facts["dasha"]["antardasha"]       = antar_lord
                key_facts["dasha"]["pratyantar_dasha"] = prat_lord
                key_facts["dasha"]["maha_end_date"]    = end_date
                key_facts["dasha"]["dasha_source"]     = "current-mahadasha-full"

                logger.info(
                    f"[D-{rid}] ✅ Dasha merged | "
                    f"Maha={maha_lord} | Antar={antar_lord} | "
                    f"Pratyantar={prat_lord} | Ends={end_date}"
                )
            else:
                logger.warning(
                    f"[D-{rid}] ⚠️ Dasha merge: extractor returned empty mahadasha. "
                    f"dasha_facts={dasha_facts} | "
                    f"raw_preview={str(raw)[:300]}"
                )

        except Exception as e:
            logger.error(f"[D-{rid}] ❌ Dasha merge failed: {e}", exc_info=True)

        return key_facts

    # ─────────────────────────────────────────────────────────────
    # RESPONSE BUILDER
    # ─────────────────────────────────────────────────────────────

    def _build_final_response(
        self,
        request:           DrishtiiRequest,
        scorecard:         dict,
        gemini_dict:       dict,
        is_valid:          bool,
        validation_reason: str,
        total_time:        float,
        apis_used:         list       = None,
        phase_times:       dict       = None,
        data_quality:      dict       = None,
    ) -> dict:
        g = gemini_dict

        verdict    = scorecard["verdict"]
        confidence = g.get("confidence", self._infer_confidence(scorecard["total_score"]))
        summary    = g.get("summary", "")
        tq         = g.get("time_quality", {})
        ca         = g.get("category_analysis", {})
        timing     = g.get("timing", {})
        fr         = g.get("final_recommendation", {})

        return {
            "status":           200,
            "verdict":          verdict,
            "confidence":       confidence,
            "total_score":      scorecard["total_score"],
            "max_score":        40,
            "tone":             scorecard.get("tone_label", ""),
            "scores":           scorecard["scores"],
            "warnings":         scorecard.get("warnings",  []),
            "positive_signals": scorecard.get("positives", []),
            "summary":          summary,
            "time_quality": {
                "dasha":     tq.get("dasha",     ""),
                "moon":      tq.get("moon",       ""),
                "gochar":    tq.get("gochar",     ""),
                "nakshatra": tq.get("nakshatra",  ""),
                "hora":      tq.get("hora",       ""),
            },
            "category_analysis": {
                "focus_area":   ca.get("focus_area",   ""),
                "key_factors":  ca.get("key_factors",  []),
                "risk_factors": ca.get("risk_factors", []),
            },
            "timing": {
                "best_windows":  timing.get("best_windows",  []),
                "avoid_windows": timing.get("avoid_windows", []),
            },
            "final_recommendation": {
                "what_to_do":    fr.get("what_to_do",    []),
                "what_to_avoid": fr.get("what_to_avoid", []),
            },
            "category":          request.category,
            "category_label":    CATEGORY_LABELS.get(request.category, request.category),
            "objective":         request.objective,
            "language":          request.language,
            "target_date":       request.target_date,
            "apis_used":         apis_used or [f"{request.group}/{request.api}"],
            "validation_passed": is_valid,
            "execution_time":    total_time,
            "phase_times":       phase_times or {},
            "timestamp":         int(time.time()),
            "data_quality": {
                "planet_count":       (data_quality or {}).get("planet_count", 0),
                "dasha_missing":      (data_quality or {}).get("dasha_missing", False),
                "incomplete_chart":   (data_quality or {}).get("incomplete_chart", False),
                "transit_available":  (data_quality or {}).get("transit_available", False),
                "panchang_available": (data_quality or {}).get("panchang_available", False),
                "flags":              (data_quality or {}).get("flags", []),
            },
        }

    @staticmethod
    def _infer_confidence(total_score: int) -> str:
        if total_score >= 32:
            return "HIGH"
        if total_score >= 22:
            return "MEDIUM"
        return "LOW"

    # ─────────────────────────────────────────────────────────────
    # GEMINI GENERATION WITH RETRY
    # ─────────────────────────────────────────────────────────────

    async def _generate_with_retry(
        self,
        request:   DrishtiiRequest,
        scorecard: dict,
        key_facts: dict,
        prompt:    str,
        rid:       str,
    ):
        gemini    = GeminiClient(language=request.language)
        validator = ResponseValidator(language=request.language)

        for attempt in range(1, GEMINI_MAX_RETRIES + 1):
            try:
                logger.info(f"[D-{rid}] Gemini attempt {attempt}/{GEMINI_MAX_RETRIES}")
                gemini_dict = await gemini.generate(prompt=prompt, scorecard=scorecard)

                is_valid, reason = validator.validate(
                    gemini_dict = gemini_dict,
                    scorecard   = scorecard,
                    key_facts   = key_facts,
                    verdict     = scorecard["verdict"],
                )

                if is_valid:
                    logger.info(f"[D-{rid}] ✅ Gemini OK on attempt {attempt}")
                    return gemini_dict, True, reason

                logger.warning(
                    f"[D-{rid}] ⚠️ Gemini attempt {attempt} invalid: {reason} — retrying"
                )

            except Exception as e:
                logger.error(f"[D-{rid}] ❌ Gemini attempt {attempt} exception: {e}")

        logger.warning(f"[D-{rid}] ⚠️ All Gemini retries failed — using structured fallback")
        fallback_dict = self._fallback(
            verdict   = scorecard["verdict"],
            language  = request.language,
            category  = request.category,
            scorecard = scorecard,
        )
        return fallback_dict, False, "fallback_used"

    # ─────────────────────────────────────────────────────────────
    # VedicAPI FETCHER  (with normalised params + optional suffix)
    # ─────────────────────────────────────────────────────────────

    async def _fetch_vedic_by_group_api(
        self,
        group:        str,
        api:          str,
        params:       dict,
        rid:          str,
        cache_suffix: str = "",
    ) -> dict:
        group_path = GROUP_TO_ENDPOINT.get(group, group)
        endpoint   = f"{group_path}/{api}"
        url        = f"{self.vedic_base_url}/{endpoint}"

        call_params = dict(params)
        call_params["api_key"] = self.vedic_api_key

        safe_params = {k: v for k, v in call_params.items() if k != "api_key"}

        logger.info(
            f"[D-{rid}] 🌐 VedicAPI | URL={url} | params={json.dumps(safe_params)}"
        )

        # ── Redis cache check ──────────────────────────────────
        if self.redis:
            cache_key = self._cache_key(url, safe_params) + cache_suffix
            try:
                cached = await self.redis.get(cache_key)
                if cached:
                    logger.info(f"[D-{rid}] ⚡ Cache HIT for {endpoint}{cache_suffix}")
                    return json.loads(cached)
                logger.info(f"[D-{rid}] 💾 Cache MISS for {endpoint}{cache_suffix}")
            except Exception as e:
                logger.warning(f"[D-{rid}] Redis error (skipping cache): {e}")

        # ── HTTP call with retry ──────────────────────────────
        last_err = None
        for attempt in range(1, 4):
            try:
                call_start = time.time()
                async with httpx.AsyncClient(timeout=self.http_timeout) as client:
                    resp = await client.get(url, params=call_params)

                elapsed = round(time.time() - call_start, 2)

                if resp.status_code == 503:
                    logger.warning(
                        f"[D-{rid}] VedicAPI 503 on attempt {attempt} [{endpoint}]"
                    )
                    if attempt < 3:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise httpx.HTTPStatusError(
                        "VedicAPI temporarily unavailable (503).",
                        request=resp.request,
                        response=resp,
                    )

                resp.raise_for_status()
                data = resp.json()
                logger.info(
                    f"[D-{rid}] ✅ VedicAPI [{endpoint}] responded in {elapsed}s"
                )

                # Write to cache
                if self.redis:
                    try:
                        cache_ttl = int(os.getenv("CACHE_TTL", "3600"))
                        await self.redis.setex(
                            self._cache_key(url, safe_params) + cache_suffix,
                            cache_ttl,
                            json.dumps(data),
                        )
                    except Exception as e:
                        logger.warning(f"[D-{rid}] Redis write failed: {e}")

                return data

            except httpx.HTTPStatusError:
                raise
            except Exception as e:
                last_err = e
                logger.warning(
                    f"[D-{rid}] VedicAPI attempt {attempt} failed [{endpoint}]: {e}"
                )
                if attempt < 3:
                    await asyncio.sleep(1)

        raise Exception(
            f"VedicAPI unreachable after 3 attempts [{endpoint}]: {last_err}"
        )

    # ─────────────────────────────────────────────────────────────
    # STRUCTURED FALLBACK
    # ─────────────────────────────────────────────────────────────

    def _fallback(
        self,
        verdict:   str,
        language:  str,
        category:  str,
        scorecard: dict,
    ) -> dict:
        category_hints = {
            "financial": "आपके वित्तीय निर्णय" if language == "hi" else "your financial decision",
            "career":    "आपके करियर निर्णय"   if language == "hi" else "your career decision",
            "health":    "आपके स्वास्थ्य निर्णय" if language == "hi" else "your health decision",
            "marriage":  "आपके विवाह निर्णय"   if language == "hi" else "your marriage decision",
            "business":  "आपके व्यापार निर्णय"  if language == "hi" else "your business decision",
            "legal":     "आपके कानूनी मामले"    if language == "hi" else "your legal matter",
            "travel":    "आपकी यात्रा योजना"    if language == "hi" else "your travel plan",
        }
        hint = category_hints.get(category, "your decision")

        summaries = {
            "hi": {
                "GO":      f"ग्रह स्थिति {hint} के लिए अनुकूल है। आत्मविश्वास के साथ आगे बढ़ें।",
                "CAUTION": f"मिश्रित ग्रह ऊर्जा है {hint} के संदर्भ में। सावधानीपूर्वक आगे बढ़ें।",
                "AVOID":   f"इस समय ग्रह स्थिति {hint} के अनुकूल नहीं है। बेहतर समय का इंतजार करें।",
            },
            "en": {
                "GO":      f"Planetary conditions are favorable for {hint}. Proceed with confidence.",
                "CAUTION": f"Mixed planetary energy exists for {hint}. Proceed carefully.",
                "AVOID":   f"Planetary conditions are not favorable for {hint}. Wait for a better window.",
            },
        }

        lang_sum = summaries.get(language, summaries["en"])
        summary  = lang_sum.get(verdict, lang_sum["CAUTION"])
        warnings  = scorecard.get("warnings",  [])
        positives = scorecard.get("positives", [])

        return {
            "verdict":    verdict,
            "confidence": self._infer_confidence(scorecard.get("total_score", 20)),
            "summary":    summary,
            "time_quality": {
                "dasha": "", "moon": "", "gochar": "", "nakshatra": "", "hora": "",
            },
            "category_analysis": {
                "focus_area":   hint,
                "key_factors":  positives[:2],
                "risk_factors": warnings[:2],
            },
            "timing": {
                "best_windows":  [],
                "avoid_windows": [],
            },
            "final_recommendation": {
                "what_to_do":    [],
                "what_to_avoid": [],
            },
        }

    # ─────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────

    def _request_id(self, request: DrishtiiRequest) -> str:
        raw = f"{request.category}:{request.group}:{request.api}:{time.time()}"
        return hashlib.md5(raw.encode()).hexdigest()[:8]

    def _cache_key(self, url: str, params: dict) -> str:
        key_str = url + json.dumps(params, sort_keys=True)
        return "drishtii:v3:" + hashlib.md5(key_str.encode()).hexdigest()
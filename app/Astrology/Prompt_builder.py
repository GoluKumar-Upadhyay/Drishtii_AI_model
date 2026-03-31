# app/Astrology/Prompt_builder.py
# ════════════════════════════════════════════════════════════════════
#  Builds the Gemini prompt from all 5 data blocks  —  PRODUCTION v2
#
#  KEY ENHANCEMENTS vs v1:
#   1. Tone block injected into prompt (from scorecard.tone_label)
#   2. Hard "ONLY USE ASTRO DATA" block — last thing Gemini sees
#   3. Data summary is capped to 2 000 chars to prevent prompt bloat
#      and Gemini wandering off into irrelevant data
#   4. Dasha notice is cleaner — missing vs present path unchanged
#   5. Score → Confidence label is locked in the prompt (no ambiguity)
# ════════════════════════════════════════════════════════════════════

from __future__ import annotations

import logging

logger = logging.getLogger("drishtii.prompt_builder")

# ── Category Sutras (unchanged from v1) ─────────────────────────
CATEGORY_SUTRAS: dict[str, str] = {
    "financial": """
FINANCIAL DECISION SUTRAS:
1. 2nd House = personal wealth. 11th House = gains. 8th House = sudden gains/losses.
2. Jupiter strong = wealth flows freely. Jupiter malefic = gains are blocked.
3. Rahu in 2nd/11th = speculative risk, fraud possible. Ketu in 2nd = sudden money cut.
4. Shukla Paksha (waxing Moon) = growth period. Krishna Paksha = reduction period.
5. Jupiter/Venus Hora = best for investment. Saturn Hora = delay in payments.
6. Saturn Mahadasha = delays but structured success eventually.
7. Moon afflicted = wrong financial judgment. Strong Moon = correct financial decisions.""",

    "career": """
CAREER DECISION SUTRAS:
1. 10th House lord strong = career power. 10th House afflicted = obstacles.
2. Sun/Jupiter in 10th = leadership, recognition, and authority.
3. Saturn Dasha = career tests, slow but steady. Jupiter Dasha = career leap.
4. D10 chart = actual career karma. Must be strong for sustained career success.
5. Rahu in 10th = shortcuts that backfire. Verify all career opportunities.
6. Mercury/Sun Hora = best for job applications, interviews, and promotions.
7. Sade Sati = 7.5-year Saturn test — career change possible but with struggle.""",

    "health": """
HEALTH DECISION SUTRAS:
1. 6th House = disease. 8th House = chronic issues. 12th House = hospitalization.
2. Mars in 6th = surgery needed but healing power strong.
3. Saturn in 6th/8th = chronic, slow illness — long treatment needed.
4. Moon afflicted = mental health issues cascading to physical health.
5. Mars Hora = acceptable for surgery. Jupiter Hora = healing and recovery.
6. Rahu in 1st = mysterious illness, wrong diagnosis risk.
7. Shukla Paksha = better healing. Amavasya = avoid surgery completely.""",

    "marriage": """
MARRIAGE DECISION SUTRAS:
1. D9 (Navamsha) = marriage fate — more important than D1 for marriage timing.
2. 7th lord strength = partner quality and relationship stability.
3. Venus strong = attraction and harmony. Venus weak/afflicted = conflicts.
4. Mars in 7th = aggression — Mangal Dosha check required.
5. Ashtakoot score > 18/36 = acceptable. > 24/36 = good. < 18 = serious challenges.
6. Moon compatibility = emotional bonding — critical for long-term health.
7. Jupiter/Venus Hora + Shukla Paksha = best for engagement/marriage decisions.""",

    "business": """
BUSINESS DECISION SUTRAS:
1. D10 = career path. D14 = business karma (hidden success/failure factor).
2. Mercury benefic = business acumen, negotiation skills, good contract energy.
3. 7th House = business partnerships. 7th lord afflicted = partner betrayal risk.
4. Rahu active = unexpected twists — verify all partner information thoroughly.
5. Saturn in business = delays in payment/approval but eventual long-term stability.
6. D10 + D14 both strong = entrepreneurship succeeds even after initial struggles.
7. Mercury + Jupiter Hora = best times for contract signing and major business deals.""",

    "legal": """
LEGAL DECISION SUTRAS:
1. 6th House = disputes and legal battles. 9th House = justice, law, dharma.
2. Jupiter strong = favorable court outcomes, justice served in your favor.
3. Saturn afflicting 9th = delayed justice, unfavorable judge or magistrate.
4. Rahu in 6th = false accusations, sudden unexpected legal complications.
5. Mars-Saturn clash = case gets expensive, complicated, and emotionally draining.
6. Jupiter Hora (Thursday) = best for court hearings and legal consultations.
7. Mercury + Jupiter Hora = best for legal document filing and submissions.""",

    "travel": """
TRAVEL / NRI DECISION SUTRAS:
1. 9th House = long-distance travel. 12th House = foreign lands and settlement.
2. Rahu in 12th = strong foreign connection, overseas opportunity or compulsion.
3. Saturn in 9th = visa delays, travel blocked or severely delayed.
4. Ketu in 12th = forced return from abroad, or loss in foreign lands.
5. Moon strong + 9th lord favorable = safe, successful, and enjoyable travel.
6. Mercury Hora = best for booking travel, visa applications, and travel planning.
7. Jupiter in 9th or 12th = spiritual and material gains in foreign lands.""",
}

VERDICT_TONE: dict[str, tuple[str, str, str]] = {
    "GO":      ("✅ GO",      "strongly favorable",        "execute confidently and without delay"),
    "CAUTION": ("⚠️ CAUTION", "mixed — proceed carefully", "verify and plan thoroughly before acting"),
    "AVOID":   ("🚫 AVOID",   "unfavorable",               "wait for a better planetary window"),
}

CATEGORY_ACTION_GUIDE: dict[str, str] = {
    "financial": "Best Hora for financial execution: Jupiter or Venus Hora. New investment: Shukla Paksha only. Loan decisions: Saturn window.",
    "career":    "Best Hora for career actions: Mercury or Sun Hora. Job change timing: Jupiter transit 6th or 10th house.",
    "health":    "Surgery: Mars Hora + Moon waxing. Start treatment: Mercury or Jupiter Hora. Avoid decisions near Amavasya.",
    "marriage":  "Marriage date: Venus + Jupiter alignment day. Reconciliation: Venus Hora + Moon exalted in Taurus.",
    "business":  "Tender filing: Mercury + Jupiter Hora. Payment receipt: Venus Hora. Partner meeting: Moon strong + 7th house clean.",
    "legal":     "Court hearing: Jupiter strong day (Thursday). File case: Mercury + Jupiter Hora. Settlement: Venus Hora.",
    "travel":    "VISA application: Mercury Hora + Jupiter blessing. Move abroad: Rahu Dasha + 12th lord strong period.",
}

# Tone → prompt instruction
TONE_PROMPT_TEXT: dict[str, str] = {
    "STRONG_GO":   "✅ TONE = STRONG_GO: Write with strong confidence and encouragement. This is an excellent time.",
    "MODERATE_GO": "🟡 TONE = MODERATE_GO: Write with measured optimism. Acknowledge positives, note the need for care.",
    "CAUTION":     "⚠️ TONE = CAUTION: Write neutrally. Balance positives and risks clearly. Do not over-encourage.",
    "NO_GO":       "🚫 TONE = NO_GO: Write honestly that this is not the right time. Be direct but compassionate. NO false positivity.",
}


class PromptBuilder:

    def build(
        self,
        scorecard:   dict,
        key_facts:   dict,
        objective:   str,
        category:    str,
        language:    str,
        group:       str,
        api:         str,
        target_date: str | None = None,
    ) -> str:

        lang_name      = self._lang_name(language)
        sutras         = CATEGORY_SUTRAS.get(category, CATEGORY_SUTRAS.get("travel", ""))
        scores_text    = self._format_scores(scorecard["scores"])
        warnings_text  = "\n".join(f"  • {w}" for w in scorecard.get("warnings",  [])) or "  None"
        positives_text = "\n".join(f"  • {p}" for p in scorecard.get("positives", [])) or "  None"
        data_summary   = self._build_data_summary(key_facts, group, api)
        verdict        = scorecard["verdict"]
        total          = scorecard["total_score"]
        action_guide   = CATEGORY_ACTION_GUIDE.get(category, "")
        tone_label     = scorecard.get("tone_label", "CAUTION")
        tone_text      = TONE_PROMPT_TEXT.get(tone_label, TONE_PROMPT_TEXT["CAUTION"])

        verdict_label, verdict_quality, verdict_action = VERDICT_TONE.get(
            verdict, VERDICT_TONE["CAUTION"]
        )

        # Confidence label locked here so Gemini cannot deviate
        if total >= 32:
            confidence_label = "HIGH"
        elif total >= 22:
            confidence_label = "MEDIUM"
        else:
            confidence_label = "LOW"

        # ── Dasha notice / injection ─────────────────────────────
        dasha_info   = key_facts.get("dasha", {})
        current_dasa = dasha_info.get("current_dasa", "")
        maha_lord    = dasha_info.get("mahadasha",    "")
        antar_lord   = dasha_info.get("antardasha",   "")
        dasha_source = dasha_info.get("dasha_source", "")
        dasha_missing = (
            not current_dasa
            or "undefined" in str(current_dasa).lower()
            or not maha_lord
        )

        if dasha_missing:
            dasha_notice = (
                "\n⚠️  DASHA DATA UNAVAILABLE: Current Mahadasha/Antardasha could not be "
                "retrieved. For the 'time_quality.dasha' field, write a note in "
                f"{lang_name} that dasha data was unavailable for this analysis. "
                "DO NOT invent or guess any Mahadasha or Antardasha lord names.\n"
            )
        else:
            end_date = dasha_info.get("maha_end_date", "")
            end_note = f" (Mahadasha ends: {end_date})" if end_date else ""
            dasha_notice = (
                f"\n✅ CURRENT DASHA DATA (use exactly these values in time_quality.dasha):\n"
                f"   Mahadasha  : {maha_lord}{end_note}\n"
                f"   Antardasha : {antar_lord or 'unknown'}\n"
                f"   Source     : {dasha_source}\n"
                f"   Interpret the quality of {maha_lord} Mahadasha for the user's "
                f"'{category}' decision in {lang_name}.\n"
            )

        logger.info(
            f"[PROMPT] Building prompt | "
            f"lang={language} category={category} "
            f"verdict={verdict} total={total}/40 tone={tone_label} "
            f"dasha_missing={dasha_missing} mahadasha={maha_lord or 'N/A'}"
        )

        prompt = f"""════════════════════════════════════════════════════════
DRISHTII ONE — VEDIC DECISION ENGINE
════════════════════════════════════════════════════════

TASK: Analyze the astrology data below and produce a structured JSON decision report.
OUTPUT LANGUAGE: Write ALL text values in {lang_name}. Every word must be in {lang_name}.
OUTPUT FORMAT: Return ONLY a valid JSON object. No text before or after. No markdown fences.

════════════════════════════════════════════════════════
USER REQUEST
════════════════════════════════════════════════════════
Category    : {category.upper()}
Objective   : "{objective}"
Target Date : {target_date or "today"}

════════════════════════════════════════════════════════
{tone_text}
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
PRE-CALCULATED SCORECARD (ACCEPT AS FINAL TRUTH — DO NOT CHANGE ANY NUMBER)
════════════════════════════════════════════════════════
Verdict     : {verdict_label}
Confidence  : {confidence_label}   ← you MUST use exactly this value
Total Score : {total}/40
Quality     : Planetary conditions are {verdict_quality}.
User must   : {verdict_action}.

Score Breakdown:
{scores_text}

Warning Signals (reference at least 2 of these in your analysis):
{warnings_text}

Positive Signals (reference at least 1 of these in your analysis):
{positives_text}

════════════════════════════════════════════════════════
ASTRO DATA ← USE ONLY WHAT IS LISTED HERE. INVENT NOTHING.
════════════════════════════════════════════════════════
{data_summary}
{dasha_notice}

════════════════════════════════════════════════════════
{category.upper()} DOMAIN FRAMEWORK
════════════════════════════════════════════════════════
{sutras}

TIMING GUIDANCE FOR {category.upper()}:
{action_guide}

════════════════════════════════════════════════════════
STRICT RULES FOR YOUR JSON OUTPUT
════════════════════════════════════════════════════════
1. Every string value must be written in {lang_name} — no English unless language IS English.
2. "verdict" must match exactly: {verdict}
3. "confidence" must match exactly: {confidence_label}
4. "summary" must be 3–5 complete sentences — cover why verdict is {verdict}, which planet/house is key, and what to do now.
5. "time_quality" fields must address that specific timing layer using ONLY data from ASTRO DATA above.
6. "category_analysis.key_factors" must name specific planets or houses from ASTRO DATA.
7. "category_analysis.risk_factors" must come from the Warning Signals list above.
8. "timing.best_windows" must name specific Hora lords, days, or dasha periods — not vague phrases.
9. "final_recommendation.what_to_do" = 3 concrete steps the user can take TODAY or this week.
10. "final_recommendation.what_to_avoid" = 3 specific actions/timings to avoid.
11. DO NOT mention any planet, house number, or dasha lord that is NOT in ASTRO DATA above.

Now return your JSON analysis:"""

        logger.info(f"[PROMPT] ✅ Prompt built — {len(prompt)} chars")
        return prompt

    # ── Data summary builder (capped at 2 000 chars) ────────────

    def _build_data_summary(self, facts: dict, group: str, api: str) -> str:
        raw = self._build_data_summary_uncapped(facts, group, api)
        # Raised from 2000 → 3500 to accommodate 3-layer data (natal + dasha + transit)
        if len(raw) > 3500:
            logger.info(
                f"[PROMPT] Data summary capped from {len(raw)} → 3500 chars"
            )
            return raw[:3500] + "\n[... additional data truncated for focus ...]"
        return raw

    def _build_data_summary_uncapped(self, facts: dict, group: str, api: str) -> str:
        context = facts.get("context_type", f"{group}/{api}")

        if facts.get("planets"):
            return self._format_planets(facts)

        if context.startswith("dashas/") or group == "dashas":
            maha  = facts.get("mahadasha",       "unknown")
            antar = facts.get("antardasha",       "unknown")
            prat  = facts.get("pratyantar_dasha", "")
            end   = facts.get("maha_end_date",    "unknown")
            return (
                f"Current Mahadasha  : {maha}\n"
                f"Current Antardasha : {antar}\n"
                f"Pratyantar Dasha   : {prat}\n"
                f"Mahadasha Ends     : {end}"
            )

        if context == "panchang/hora-muhurta":
            current = facts.get("current_hora", "unknown")
            fav     = ", ".join(h.get("planet", "") for h in facts.get("favorable_horas",   []))
            unfav   = ", ".join(h.get("planet", "") for h in facts.get("unfavorable_horas", []))
            return (
                f"Current Hora        : {current}\n"
                f"Favorable Horas     : {fav}\n"
                f"Unfavorable Horas   : {unfav}"
            )

        if context.startswith("panchang/") or group == "panchang":
            return (
                f"Tithi     : {facts.get('tithi',    'unknown')}\n"
                f"Paksha    : {facts.get('paksha',   'unknown')}\n"
                f"Nakshatra : {facts.get('nakshatra','unknown')}\n"
                f"Day Lord  : {facts.get('day_lord', 'unknown')}\n"
                f"Hora Lord : {facts.get('hora_lord','unknown')}\n"
                f"Yoga      : {facts.get('yoga',     'unknown')}\n"
                f"Moon Sign : {facts.get('moon_sign','unknown')}"
            )

        if context.startswith("dosha/") or group == "dosha":
            return (
                f"Dosha Present : {facts.get('has_dosha', False)}\n"
                f"Severity      : {facts.get('severity',  'unknown')}\n"
                f"Description   : {str(facts.get('description', ''))[:250]}"
            )

        if context.startswith("matching/") or group == "matching":
            lines = []
            lines.append(
                f"Ashtakoot Score  : {facts.get('total_points', 0)}/{facts.get('max_points', 36)}\n"
                f"Match Percent    : {facts.get('match_percent', 0)}%\n"
                f"Compatibility    : {facts.get('compatibility', 'unknown')}\n"
                f"Conclusion       : {str(facts.get('conclusion', ''))[:200]}"
            )
            # Koota breakdown if available
            koota = facts.get("koota_details", [])
            if koota:
                lines.append("\nKoota Breakdown:")
                for k in (koota if isinstance(koota, list) else [])[:8]:
                    if isinstance(k, dict):
                        name  = k.get("name", k.get("koota", ""))
                        score = k.get("score", k.get("points", "?"))
                        max_s = k.get("max_score", k.get("max", "?"))
                        lines.append(f"  {name}: {score}/{max_s}")
            # Male natal if injected
            male_planets = facts.get("planets", {})
            if male_planets:
                m_asc = male_planets.get("ascendant", {}).get("zodiac", "?")
                m_moon_h = male_planets.get("moon", {}).get("house", "?")
                m_ven_h  = male_planets.get("venus", {}).get("house", "?")
                m_mars_h = male_planets.get("mars", {}).get("house", "?")
                m_jup_h  = male_planets.get("jupiter", {}).get("house", "?")
                m_sat_h  = male_planets.get("saturn", {}).get("house", "?")
                lines.append(
                    f"\nMale Natal Chart (ascendant = {m_asc}):\n"
                    f"  Moon  : house {m_moon_h} | Venus : house {m_ven_h} | "
                    f"Mars : house {m_mars_h}\n"
                    f"  Jupiter : house {m_jup_h} | Saturn : house {m_sat_h}"
                )
            # Female natal if available
            female_planets = facts.get("female_planets", {})
            if female_planets:
                f_asc = female_planets.get("ascendant", {}).get("zodiac", "?")
                f_moon_h = female_planets.get("moon", {}).get("house", "?")
                f_ven_h  = female_planets.get("venus", {}).get("house", "?")
                f_mars_h = female_planets.get("mars", {}).get("house", "?")
                lines.append(
                    f"\nFemale Natal Chart (ascendant = {f_asc}):\n"
                    f"  Moon  : house {f_moon_h} | Venus : house {f_ven_h} | "
                    f"Mars : house {f_mars_h}"
                )
            # Dasha if available
            dasha = facts.get("dasha", {})
            if dasha.get("mahadasha"):
                lines.append(
                    f"\nMale Dasha:\n"
                    f"  Mahadasha  : {dasha.get('mahadasha')} (ends {dasha.get('maha_end_date','')})\n"
                    f"  Antardasha : {dasha.get('antardasha','?')}"
                )
            return "\n".join(lines)

        if context.startswith("prediction/") or group == "prediction":
            return (
                f"Prediction : {str(facts.get('prediction_text', ''))[:400]}\n"
                f"Moon Sign  : {facts.get('moon_sign', '')}\n"
                f"Sun Sign   : {facts.get('sun_sign',  '')}"
            )

        if "sade" in context:
            return (
                f"In Sade Sati : {facts.get('is_active', facts.get('is_in_sade_sati', False))}\n"
                f"Phase        : {facts.get('phase', facts.get('current_phase', 'unknown'))}\n"
                f"Saturn Sign  : {facts.get('saturn_sign', 'unknown')}\n"
                f"Moon Sign    : {facts.get('moon_sign', 'unknown')}\n"
                f"Description  : {str(facts.get('description', ''))[:200]}"
            )

        if context.startswith("utilities/") or group == "utilities":
            lines = []
            for k, v in facts.items():
                if k not in ("context_type", "all_results") and isinstance(v, (str, int, float, bool)):
                    lines.append(f"{k:20}: {v}")
            return "\n".join(lines)

        # Generic fallback
        lines = []
        for k, v in facts.items():
            if k not in ("context_type", "raw_summary") and isinstance(v, (str, int, float, bool)):
                lines.append(f"{k}: {v}")
        return "\n".join(lines) or "No structured data available"

    def _format_planets(self, facts: dict) -> str:
        planets          = facts.get("planets",          {})
        panchang         = facts.get("panchang",         {})   # birth-time panchang
        dasha            = facts.get("dasha",            {})
        transit_planets  = facts.get("transit_planets",  {})   # TODAY's transits
        today_panchang   = facts.get("today_panchang",   {})   # TODAY's panchang
        today_nakshatra  = facts.get("today_nakshatra",  "")
        today_hora       = facts.get("today_hora",       "")
        today_tithi      = facts.get("today_tithi",      "")
        today_paksha     = facts.get("today_paksha",     "")
        today_yoga       = facts.get("today_yoga",       "")

        lines = []
        planet_order = [
            "ascendant", "sun", "moon", "mars", "mercury",
            "jupiter", "venus", "saturn", "rahu", "ketu",
        ]

        # ── NATAL CHART (birth positions) ──────────────────────────
        lines.append("── NATAL CHART (birth positions — personality & baseline karma) ──")
        for name in planet_order:
            p = planets.get(name)
            if p:
                flags = ""
                if p.get("is_combust"):
                    flags += " [COMBUST]"
                if p.get("retro"):
                    flags += " [RETRO]"
                lines.append(
                    f"{name.capitalize():12} | "
                    f"House {p.get('house', '?'):>2} | "
                    f"{p.get('zodiac', '?'):12} | "
                    f"{p.get('lord_status', '-'):20} | "
                    f"Nakshatra: {p.get('nakshatra', '?')}"
                    f"{flags}"
                )

        if panchang.get("tithi"):
            lines.append(
                f"\nBirth Panchang — Tithi: {panchang.get('tithi','')} | "
                f"Hora: {panchang.get('hora_lord','')} | "
                f"Day: {panchang.get('day_lord','')}"
            )

        # ── DASHA (time period) ─────────────────────────────────────
        maha_lord    = dasha.get("mahadasha",    "")
        antar_lord   = dasha.get("antardasha",   "")
        current_dasa = dasha.get("current_dasa", "")
        birth_dasa   = dasha.get("birth_dasa",   "")

        if maha_lord and "undefined" not in str(maha_lord).lower():
            end_date = dasha.get("maha_end_date", "")
            end_note = f" (ends {end_date})" if end_date else ""
            lines.append(
                f"\n── DASHA (time period) ──\n"
                f"Current Dasha: Mahadasha={maha_lord}{end_note} | "
                f"Antardasha={antar_lord or 'N/A'}"
            )
        elif current_dasa and "undefined" not in str(current_dasa).lower():
            lines.append(f"\n── DASHA ──\nCurrent Dasha: {current_dasa}")

        if birth_dasa:
            lines.append(f"Birth Dasha  : {birth_dasa}")

        # ── TODAY's PANCHANG (live timing for target date) ──────────
        # This is what tells us TODAY's nakshatra, hora, tithi — NOT birth
        lines.append("\n── TODAY'S PANCHANG (target date — for timing decisions) ──")
        if today_nakshatra or today_panchang:
            nk   = today_nakshatra or today_panchang.get("nakshatra", "unknown")
            hora = today_hora       or today_panchang.get("hora_lord", "unknown")
            tithi = today_tithi    or today_panchang.get("tithi", "unknown")
            paksha = today_paksha  or today_panchang.get("paksha", "unknown")
            yoga   = today_yoga    or today_panchang.get("yoga", "unknown")
            lines.append(
                f"Today Nakshatra : {nk}\n"
                f"Today Hora      : {hora}\n"
                f"Today Tithi     : {tithi}\n"
                f"Today Paksha    : {paksha}\n"
                f"Today Yoga      : {yoga}"
            )
        else:
            lines.append("Today panchang data unavailable — using birth panchang as fallback")

        # ── TRANSIT / GOCHAR (planets TODAY in sky) ─────────────────
        # These are where planets are RIGHT NOW, not at birth
        if transit_planets:
            lines.append("\n── TRANSIT / GOCHAR (planets TODAY — real-time decision signal) ──")
            for name in planet_order:
                tp = transit_planets.get(name)
                if tp:
                    flags = ""
                    if tp.get("retro"):
                        flags += " [RETRO]"
                    lines.append(
                        f"Transit {name.capitalize():12} | "
                        f"House {tp.get('house', '?'):>2} | "
                        f"{tp.get('zodiac', '?'):12}"
                        f"{flags}"
                    )
        else:
            lines.append("\n── TRANSIT / GOCHAR ──\nTransit data unavailable — using natal chart only")

        return "\n".join(lines)

    def _format_scores(self, scores: dict) -> str:
        return "\n".join(
            f"  {k.replace('_', ' ').title():30} : {v}/10"
            for k, v in scores.items()
        )

    def _lang_name(self, code: str) -> str:
        names = {
            "en": "English",  "hi": "Hindi",    "es": "Spanish",
            "fr": "French",   "de": "German",   "ta": "Tamil",
            "te": "Telugu",   "mr": "Marathi",  "gu": "Gujarati",
            "bn": "Bengali",  "kn": "Kannada",  "ml": "Malayalam",
            "pa": "Punjabi",  "ur": "Urdu",
        }
        return names.get(code, "English")
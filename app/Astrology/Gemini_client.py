# app/Astrology/Gemini_client.py
# ════════════════════════════════════════════════════════════════════
#  Calls Gemini 2.5 Pro via Vertex AI SDK
#
#  KEY CHANGES vs previous version:
#   1. max_output_tokens raised to 2048 — full JSON report needs space
#   2. System instruction enforces STRICT JSON-only output
#   3. Temperature lowered to 0.15 for consistent structured output
#   4. Response parser strips ```json fences automatically
#   5. generate() returns a parsed dict, not a raw string
# ════════════════════════════════════════════════════════════════════

import asyncio
import functools
import json
import logging
import time

from google import genai
from google.genai import types

logger = logging.getLogger("drishtii.gemini")

GEMINI_MODEL    = "gemini-2.5-pro"
VERTEX_PROJECT  = "bustling-joy-488514-u2"
VERTEX_LOCATION = "us-central1"
GEMINI_TIMEOUT  = 120

# Hindi/non-Latin languages use 2-3x more tokens per character than English.
# 2048 was causing truncated JSON mid-string for Hindi responses.
# 4096 gives enough headroom for all supported languages.
MAX_OUTPUT_TOKENS = 4096

LANG_NAMES = {
    "en": "English",   "hi": "Hindi",     "es": "Spanish",
    "fr": "French",    "de": "German",    "ta": "Tamil",
    "te": "Telugu",    "mr": "Marathi",   "gu": "Gujarati",
    "bn": "Bengali",   "kn": "Kannada",   "ml": "Malayalam",
    "pa": "Punjabi",   "ur": "Urdu",
}

CATEGORY_DOMAIN = {
    "financial":     "financial investment, wealth, and money decisions. Reference 2nd, 8th, 11th houses, Jupiter, Venus, Moon.",
    "career":        "career, job, and professional decisions. Reference 10th house, Sun, Saturn, D10 chart.",
    "health":        "health, medical, and wellness decisions. Reference 6th, 8th, 12th houses, Mars, Moon.",
    "relationships": "relationship and partnership decisions. Reference 7th house, Venus, Moon Nakshatra.",
    "marriage":      "marriage and long-term commitment. Reference D9 Navamsha, 7th lord, Venus, Ashtakoot score.",
    "business":      "business, tender, and entrepreneurship. Reference D10, D14, 7th house, Mercury.",
    "legal":         "legal, court, and dispute matters. Reference 6th, 9th houses, Jupiter, Saturn, Mars.",
    "education":     "education, learning, and academic decisions. Reference 5th, 9th houses, Mercury, Jupiter.",
    "travel":        "travel, migration, and foreign-land decisions. Reference 9th, 12th houses, Rahu, Moon.",
    "personal":      "personal life and self-development. Reference ascendant, Moon, Sun, current Dasha.",
}


class GeminiClient:

    def __init__(self, language: str = "en"):
        self.language  = language
        self.lang_name = LANG_NAMES.get(language, "English")

        self.client = genai.Client(
            vertexai=True,
            project=VERTEX_PROJECT,
            location=VERTEX_LOCATION,
        )

        logger.info(
            f"[GEMINI] ✅ Configured via Vertex AI | "
            f"Model={GEMINI_MODEL} | Lang={language} ({self.lang_name})"
        )

    async def generate(self, prompt: str, scorecard: dict) -> dict:
        """Returns a parsed dict conforming to the Drishtii JSON schema."""
        category = scorecard.get("category", "personal")

        logger.info(
            f"[GEMINI] ▶ Request | verdict={scorecard.get('verdict')} "
            f"score={scorecard.get('total_score')}/40 "
            f"lang={self.language} category={category} "
            f"prompt_len={len(prompt)}"
        )

        try:
            return await asyncio.wait_for(
                self._call_vertex(prompt, category),
                timeout=GEMINI_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error(f"[GEMINI] ❌ Timed out after {GEMINI_TIMEOUT}s")
            raise TimeoutError(f"Gemini did not respond within {GEMINI_TIMEOUT}s")

    async def _call_vertex(self, prompt: str, category: str) -> dict:
        gen_config = types.GenerateContentConfig(
            system_instruction=self._build_system_instruction(category),
            temperature=0.2,
            top_p=0.8,
            max_output_tokens=MAX_OUTPUT_TOKENS,
        )

        t0   = time.time()
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None,
            functools.partial(
                self.client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=gen_config,
            )
        )
        elapsed  = round(time.time() - t0, 2)
        raw_text = resp.text.strip() if resp.text else ""

        if not raw_text:
            raise ValueError("Gemini returned empty response")

        logger.info(
            f"[GEMINI] ✅ Raw response | elapsed={elapsed}s "
            f"len={len(raw_text)} preview={raw_text[:120]!r}"
        )

        parsed = self._parse_json(raw_text)
        logger.info(f"[GEMINI] ✅ JSON parsed | keys={list(parsed.keys())}")
        return parsed

    def _parse_json(self, raw: str) -> dict:
        """Strips markdown fences and parses JSON."""
        text = raw.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            end   = -1 if lines[-1].strip() == "```" else len(lines)
            text  = "\n".join(lines[1:end]).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"[GEMINI] ❌ JSON parse failed: {e} | raw[:500]={raw[:500]}")
            raise ValueError(f"Gemini response is not valid JSON: {e}")

    def _build_system_instruction(self, category: str) -> str:
        domain    = CATEGORY_DOMAIN.get(category, CATEGORY_DOMAIN["personal"])
        lang_name = self.lang_name
        return f"""You are Drishtii One — a Vedic astrology decision engine specialising in {domain}

ABSOLUTE RULES:
1. Return ONLY a single valid JSON object. No text before or after. No markdown fences.
2. Write ALL string values inside the JSON in {lang_name} ONLY. Every word must be {lang_name}.
3. Never contradict the pre-calculated verdict given in the prompt.
4. Never invent planetary positions not listed in ASTRO DATA.
5. Never recalculate any score — accept all scores as final truth.
6. Be specific — name actual planets, house numbers, and dasha lords from the data.
7. Each text field must be a complete, meaningful sentence, not a fragment.

FIELD REQUIREMENTS:
- "summary": 3–5 sentences covering verdict reason, key planetary factor, and what user should do now.
- "time_quality.*": each field = 1–2 sentences about that exact timing component.
- "category_analysis.key_factors": 2–3 specific favorable factors from the data.
- "category_analysis.risk_factors": 2–3 specific risks from the scorecard warnings.
- "timing.best_windows": 2–3 specific windows (name day, hora, or dasha period).
- "timing.avoid_windows": 2–3 specific periods to avoid.
- "final_recommendation.what_to_do": 3 concrete, actionable steps the user can act on TODAY.
- "final_recommendation.what_to_avoid": 3 specific things to avoid.

OUTPUT JSON SCHEMA (follow EXACTLY — no extra keys, no missing keys):
{{
  "verdict": "GO | CAUTION | AVOID",
  "confidence": "HIGH | MEDIUM | LOW",
  "summary": "<3-5 sentences in {lang_name}>",
  "time_quality": {{
    "dasha": "<1-2 sentences about Mahadasha/Antardasha quality in {lang_name}>",
    "moon": "<1-2 sentences about Moon condition, Nakshatra, phase in {lang_name}>",
    "gochar": "<1-2 sentences about key transit influences in {lang_name}>",
    "nakshatra": "<1-2 sentences about today Nakshatra impact in {lang_name}>",
    "hora": "<1-2 sentences about current Hora and best windows in {lang_name}>"
  }},
  "category_analysis": {{
    "focus_area": "<one sentence about what this analysis focuses on in {lang_name}>",
    "key_factors": [
      "<specific favorable planetary factor in {lang_name}>",
      "<another specific favorable factor in {lang_name}>"
    ],
    "risk_factors": [
      "<specific risk from warnings in {lang_name}>",
      "<another specific risk in {lang_name}>"
    ]
  }},
  "timing": {{
    "best_windows": [
      "<specific best time window in {lang_name}>",
      "<another best window in {lang_name}>"
    ],
    "avoid_windows": [
      "<specific time to avoid in {lang_name}>",
      "<another time to avoid in {lang_name}>"
    ]
  }},
  "final_recommendation": {{
    "what_to_do": [
      "<concrete actionable step 1 in {lang_name}>",
      "<concrete actionable step 2 in {lang_name}>",
      "<concrete actionable step 3 in {lang_name}>"
    ],
    "what_to_avoid": [
      "<specific thing to avoid 1 in {lang_name}>",
      "<specific thing to avoid 2 in {lang_name}>",
      "<specific thing to avoid 3 in {lang_name}>"
    ]
  }}
}}"""
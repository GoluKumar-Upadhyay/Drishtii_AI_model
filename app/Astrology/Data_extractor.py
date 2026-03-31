import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger("drishtii.extractor")

# ── Hora calculation (Vedic) ─────────────────────────────────────────
# Each weekday has a ruling planet; hora cycles through 7 planets from day lord
_HORA_SEQUENCE  = ["Sun", "Venus", "Mercury", "Moon", "Saturn", "Jupiter", "Mars"]
_DAY_LORD_INDEX = {0: "Moon", 1: "Mars", 2: "Mercury", 3: "Jupiter",
                   4: "Venus", 5: "Saturn", 6: "Sun"}   # Python weekday: 0=Mon … 6=Sun


def _compute_hora_lord(target_date_str: str | None, sunrise_str: str | None) -> str:
    """
    Computes the Vedic hora lord for the current moment on target_date.
    Returns the planet name as a string (for backward-compat with all callers).
    Stores extended info (window, next hora) in module-level _last_hora_detail.
    Falls back gracefully — never raises.
    """
    global _last_hora_detail

    # ── Resolve target date ──────────────────────────────────────
    target_dt = datetime.now()
    if target_date_str:
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                target_dt = datetime.strptime(target_date_str, fmt)
                break
            except ValueError:
                continue

    # ── Parse sunrise ────────────────────────────────────────────
    sunrise_hour, sunrise_min = 6, 0
    if sunrise_str:
        for fmt in ("%I:%M:%S %p", "%I:%M %p", "%H:%M:%S", "%H:%M",
                    "%I:%M:%S%p", "%I:%M%p"):
            try:
                t = datetime.strptime(str(sunrise_str).strip(), fmt)
                sunrise_hour, sunrise_min = t.hour, t.minute
                break
            except ValueError:
                continue

    # ── Current moment anchored to target date ───────────────────
    now_dt     = datetime.now().replace(
        year=target_dt.year, month=target_dt.month, day=target_dt.day
    )
    sunrise_dt = target_dt.replace(
        hour=sunrise_hour, minute=sunrise_min, second=0, microsecond=0
    )

    # ── Day lord → start index in sequence ──────────────────────
    weekday       = target_dt.weekday()
    day_lord_name = _DAY_LORD_INDEX[weekday]
    start_index   = _HORA_SEQUENCE.index(day_lord_name)

    # ── Hora number since sunrise (0-based, wraps every 24h) ────
    mins_since_sunrise = max(0, (now_dt - sunrise_dt).total_seconds() / 60)
    hora_number        = int(mins_since_sunrise // 60) % 24
    hora_planet        = _HORA_SEQUENCE[(start_index + hora_number) % 7]

    # ── Build time window strings ────────────────────────────────
    hora_start_dt = sunrise_dt + __import__("datetime").timedelta(hours=hora_number)
    hora_end_dt   = hora_start_dt + __import__("datetime").timedelta(hours=1)
    hora_window   = f"{hora_start_dt.strftime('%H:%M')}–{hora_end_dt.strftime('%H:%M')}"

    next_hora_number = (hora_number + 1) % 24
    next_hora_planet = _HORA_SEQUENCE[(start_index + next_hora_number) % 7]
    next_hora_start  = hora_end_dt
    next_hora_end    = next_hora_start + __import__("datetime").timedelta(hours=1)
    next_hora_window = f"{next_hora_start.strftime('%H:%M')}–{next_hora_end.strftime('%H:%M')}"

    # Store extended detail for callers that want it
    _last_hora_detail = {
        "lord":            hora_planet,
        "hora_window":     hora_window,
        "next_lord":       next_hora_planet,
        "next_hora_window": next_hora_window,
    }

    logger.info(
        f"[EXTRACT] 🕐 Hora computed | date={target_date_str} "
        f"day_lord={day_lord_name} sunrise={sunrise_hour:02d}:{sunrise_min:02d} "
        f"mins_since_sunrise={int(mins_since_sunrise)} hora_no={hora_number} "
        f"→ hora_lord={hora_planet} window={hora_window} "
        f"next={next_hora_planet} {next_hora_window}"
    )
    return hora_planet


# Module-level storage for the last computed hora detail (avoids API change)
_last_hora_detail: dict = {}


def get_hora_detail() -> dict:
    """Returns the extended hora detail dict from the last _compute_hora_lord call."""
    return dict(_last_hora_detail)

# Standard planet position index used by VedicAPI (numeric keys 0-9)
PLANET_INDEX = {
    "0": "ascendant",
    "1": "sun",
    "2": "moon",
    "3": "mars",
    "4": "mercury",
    "5": "jupiter",
    "6": "venus",
    "7": "saturn",
    "8": "rahu",
    "9": "ketu",
}

# Planets that are benefic by nature
NATURAL_BENEFICS  = {"jupiter", "venus", "mercury", "moon"}
NATURAL_MALEFICS  = {"saturn", "mars", "rahu", "ketu", "sun"}


class DataExtractor:
    """
    Routes every group+api to its extractor.
    Always returns a normalised dict.
    Logs every extracted field with values.
    """

    async def extract(
        self,
        api_response:   Dict[str, Any],
        group:          str,
        api:            str,
        request_params: Dict[str, Any]
    ) -> Dict[str, Any]:

        logger.info(
            f"[EXTRACT] Starting extraction for group={group} api={api}\n"
            f"  Raw response keys: {list(api_response.keys()) if isinstance(api_response, dict) else type(api_response).__name__}"
        )

        # VedicAPI wraps data in "response" key — unwrap it
        raw = api_response.get("response", api_response)
        if "response" in api_response:
            logger.info(f"[EXTRACT] Unwrapped 'response' key — inner type: {type(raw).__name__}")

        try:
            if group == "horoscope":
                result = self._extract_horoscope(raw, api)
            elif group == "prediction":
                result = self._extract_prediction(raw, api)
            elif group == "panchang":
                result = self._extract_panchang(raw, api)
            elif group == "dashas":
                result = self._extract_dashas(raw, api)
            elif group == "dosha":
                result = self._extract_dosha(raw, api)
            elif group == "extended":
                result = self._extract_extended(raw, api)
            elif group == "matching":
                result = self._extract_matching(raw, api)
            elif group == "utilities":
                result = self._extract_utilities(raw, api, request_params)
            else:
                logger.warning(f"[EXTRACT] Unknown group '{group}' — using generic extractor")
                result = self._generic_extract(raw, group, api)

            logger.info(
                f"[EXTRACT] ✅ Extraction complete for {group}/{api}\n"
                f"  context_type : {result.get('context_type', 'unknown')}\n"
                f"  Keys found   : {[k for k in result if k != 'context_type']}\n"
                f"  Full result  :\n{json.dumps(result, indent=4, default=str)}"
            )
            return result

        except Exception as e:
            logger.error(
                f"[EXTRACT] ❌ Extraction FAILED for {group}/{api}: {e}",
                exc_info=True
            )
            fallback = {
                "context_type": f"{group}/{api}",
                "raw_summary":  str(raw)[:500],
                "error":        str(e),
            }
            logger.info(f"[EXTRACT] Using fallback result: {fallback}")
            return fallback

    # ═══════════════════════════════════════════════════════════
    # GROUP 1 — HOROSCOPE
    # APIs: planet-details, planet-report, aspects,
    #       planets-by-houses, personal-characteristics,
    #       12-month-prediction, ascendant-report,
    #       ashtakvarga, binnashtakvarga, chart-image,
    #       western-planets, divisional-charts
    # ═══════════════════════════════════════════════════════════

    def _extract_horoscope(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] horoscope/{api} — extracting planet block")

        if api == "chart-image":
            return {
                "context_type": "horoscope/chart-image",
                "image_url":    raw.get("chart_url", raw.get("url", "")),
                "chart_style":  raw.get("style", ""),
            }

        if api == "divisional-charts":
            chart_data = raw.get("chart", raw)
            result = self._extract_planets_block(chart_data, api)
            result["chart_type"]   = raw.get("div", raw.get("chart_type", "unknown"))
            result["context_type"] = "horoscope/divisional-charts"
            return result

        # All other horoscope APIs share the planet block format
        return self._extract_planets_block(raw, api)

    def _extract_planets_block(self, raw: Dict, api: str) -> Dict:
        """
        Extracts all planet positions from numeric keys 0-9.
        Also extracts embedded panchang and dasha data.
        """
        planets = {}

        for idx, name in PLANET_INDEX.items():
            p = raw.get(idx) or raw.get(name)
            if p and isinstance(p, dict):
                planets[name] = {
                    "zodiac":         p.get("zodiac",        p.get("sign", "")),
                    "house":          self._safe_int(p.get("house", 0)),
                    "lord_status":    p.get("lord_status",   p.get("planet_lordship", "-")),
                    "nakshatra":      p.get("nakshatra",     p.get("nakshatraName", "")),
                    "nakshatra_lord": p.get("nakshatra_lord",p.get("nakshatraLord", "")),
                    "is_combust":     bool(p.get("is_combust", p.get("isCombust", False))),
                    "retro":          bool(p.get("retro",      p.get("isRetrograde", False))),
                    "basic_avastha":  p.get("basic_avastha",  p.get("avastha", "")),
                    "zodiac_lord":    p.get("zodiac_lord",    p.get("signLord", "")),
                    "degree":         p.get("degree",         p.get("full_degree", 0)),
                    "planet_type":    "malefic" if name in NATURAL_MALEFICS else "benefic",
                }
                logger.info(
                    f"[EXTRACT]   {name.upper():10} → "
                    f"house={planets[name]['house']} zodiac={planets[name]['zodiac']} "
                    f"status={planets[name]['lord_status']} "
                    f"{'[COMBUST]' if planets[name]['is_combust'] else ''}"
                    f"{'[RETRO]' if planets[name]['retro'] else ''}"
                )

        # Embedded panchang block
        pc = raw.get("panchang", {})
        if not pc:
            # Some APIs flatten panchang into the root
            pc = {
                "tithi":     raw.get("tithi",     raw.get("Tithi", "")),
                "yoga":      raw.get("yoga",      raw.get("Yoga", "")),
                "karana":    raw.get("karana",    raw.get("Karana", "")),
                "day_lord":  raw.get("day_lord",  raw.get("dayLord", "")),
                "hora_lord": raw.get("hora_lord", raw.get("horaLord", "")),
            }

        panchang = {
            "tithi":     pc.get("tithi",     ""),
            "yoga":      pc.get("yoga",      ""),
            "karana":    pc.get("karana",    ""),
            "day_lord":  pc.get("day_lord",  pc.get("dayLord", "")),
            "hora_lord": pc.get("hora_lord", pc.get("horaLord", "")),
            "paksha":    pc.get("paksha",    raw.get("paksha", "")),
        }

        # Embedded dasha block
        dasha = {
            "birth_dasa":   raw.get("birth_dasa",   raw.get("birthDasa", "")),
            "current_dasa": raw.get("current_dasa", raw.get("currentDasa", "")),
        }

        result = {
            "context_type":  f"horoscope/{api}",
            "planets":       planets,
            "panchang":      panchang,
            "dasha":         dasha,
            "lucky_gem":     raw.get("lucky_gem",  raw.get("luckyGem", [])),
            "rasi":          raw.get("rasi",       raw.get("moonSign", "")),
            "nakshatra":     raw.get("nakshatra",  ""),
            "ascendant":     raw.get("ascendant",  raw.get("lagna", "")),
            "planet_count":  len(planets),
        }

        logger.info(
            f"[EXTRACT] Planet block extracted: {len(planets)} planets found | "
            f"panchang tithi={panchang['tithi']} hora={panchang['hora_lord']} | "
            f"dasha={dasha['current_dasa']}"
        )
        return result

    # ═══════════════════════════════════════════════════════════
    # GROUP 2 — PREDICTION
    # APIs: biorhythm, daily-moon, daily-nakshatra, daily-sun,
    #       day-number, monthly, numerology, weekly-moon,
    #       weekly-sun, yearly
    # ═══════════════════════════════════════════════════════════

    def _extract_prediction(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] prediction/{api} — extracting prediction text")

        base = {
            "context_type":    f"prediction/{api}",
            "prediction_text": "",
            "moon_sign":       "",
            "sun_sign":        "",
        }

        text_keys = ("prediction", "bot_response", "description", "summary",
                     "daily_prediction", "weekly_prediction", "monthly_prediction",
                     "yearly_prediction")

        def _grab_text(d: Dict) -> str:
            for k in text_keys:
                if k in d and d[k]:
                    return str(d[k])
            return ""

        if api in ("daily-moon", "daily-sun"):
            base["prediction_text"] = _grab_text(raw)
            base["moon_sign"]       = raw.get("moonSign",  raw.get("moon_sign", ""))
            base["sun_sign"]        = raw.get("sunSign",   raw.get("sun_sign", ""))
            base["zodiac"]          = raw.get("zodiac",    "")

        elif api == "daily-nakshatra":
            base["prediction_text"] = _grab_text(raw)
            base["nakshatra"]       = raw.get("nakshatra", "")
            base["nakshatra_lord"]  = raw.get("nakshatra_lord", "")

        elif api == "monthly":
            base["prediction_text"] = _grab_text(raw)
            base["month"]           = raw.get("month", "")
            base["year"]            = raw.get("year",  "")

        elif api in ("weekly-moon", "weekly-sun"):
            base["prediction_text"] = _grab_text(raw)
            base["week_start"]      = raw.get("week_start", raw.get("weekStart", ""))
            base["week_end"]        = raw.get("week_end",   raw.get("weekEnd", ""))

        elif api == "yearly":
            base["prediction_text"] = _grab_text(raw)
            base["year"]            = raw.get("year", "")

        elif api == "numerology":
            base["prediction_text"] = _grab_text(raw)
            base["life_path"]       = raw.get("life_path_number", raw.get("numerology_number", ""))
            base["lucky_numbers"]   = raw.get("lucky_numbers", [])
            base["ruling_planet"]   = raw.get("ruling_planet", "")

        elif api == "biorhythm":
            base["physical"]        = raw.get("physical",     0)
            base["emotional"]       = raw.get("emotional",    0)
            base["intellectual"]    = raw.get("intellectual", 0)
            base["prediction_text"] = _grab_text(raw)

        elif api == "day-number":
            base["day_number"]      = raw.get("day_number", raw.get("dayNumber", ""))
            base["prediction_text"] = _grab_text(raw)

        else:
            base["prediction_text"] = _grab_text(raw)

        logger.info(
            f"[EXTRACT]   prediction_text = {base['prediction_text'][:120]}...\n"
            f"[EXTRACT]   moon_sign={base.get('moon_sign','')} "
            f"sun_sign={base.get('sun_sign','')}"
        )
        return base

    # ═══════════════════════════════════════════════════════════
    # GROUP 3 — PANCHANG
    # APIs: panchang, choghadiya-muhurta, hora-muhurta,
    #       monthly-panchang, moon-calendar, moon-phase,
    #       moonrise, moonset, retrogrades, solarnoon,
    #       sunrise, sunset, transit, festivals
    # ═══════════════════════════════════════════════════════════

    def _extract_panchang(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] panchang/{api} — extracting panchang data")

        if api == "panchang":
            # panchang/panchang API may nest data under a sub-key like "day" or "panchang"
            # Try to find the right nested dict that contains tithi/nakshatra fields
            data = raw
            for sub_key in ("day", "panchang", "data", "panchang_data"):
                if isinstance(raw.get(sub_key), dict) and raw[sub_key].get("tithi"):
                    data = raw[sub_key]
                    logger.info(f"[EXTRACT] panchang: unwrapped sub-key '{sub_key}'")
                    break

            # ── hora_lord: try all known field names, compute if missing ──
            sunrise_raw = data.get("sunrise", raw.get("sunrise", ""))
            hora_lord   = (
                data.get("horaLord")
                or data.get("hora_lord")
                or data.get("hora")
                or raw.get("horaLord")
                or raw.get("hora_lord")
                or raw.get("hora")
                or ""
            )
            if not hora_lord:
                hora_lord = _compute_hora_lord(None, sunrise_raw)
                logger.info(
                    f"[EXTRACT] ℹ️ hora_lord absent in API response — "
                    f"computed from sunrise '{sunrise_raw}': {hora_lord}"
                )

            result = {
                "context_type": "panchang/panchang",
                "tithi":        data.get("tithi",     data.get("Tithi",     raw.get("tithi", ""))),
                "paksha":       data.get("paksha",    data.get("Paksha",    raw.get("paksha", ""))),
                "nakshatra":    data.get("nakshatra", data.get("moonNakshatra", data.get("Nakshatra", raw.get("nakshatra", "")))),
                "yoga":         data.get("yoga",      data.get("Yoga",      raw.get("yoga", ""))),
                "karana":       data.get("karana",    data.get("Karana",    raw.get("karana", ""))),
                "day_lord":     data.get("dayLord",   data.get("day_lord",  raw.get("day_lord", ""))),
                "hora_lord":    hora_lord,
                "sunrise":      sunrise_raw,
                "sunset":       data.get("sunset",    raw.get("sunset", "")),
                "moon_sign":    data.get("moonSign",  data.get("moon_sign", raw.get("moon_sign", ""))),
                "rahu_kaal":    raw.get("rahukaal",   raw.get("rahu_kaal",  raw.get("rahuKaal", ""))),
                "moon_phase":   data.get("moon_phase", ""),
                "auspicious_periods":   data.get("auspicious", raw.get("auspicious", [])),
                "inauspicious_periods": data.get("inauspicious", raw.get("inauspicious", [])),
            }
            logger.info(
                f"[EXTRACT]   tithi={result['tithi']} paksha={result['paksha']} "
                f"nakshatra={result['nakshatra']} hora={result['hora_lord']}"
            )
            return result

        elif api == "hora-muhurta":
            horas = self._extract_hora_list(raw)

            current_hora   = None
            favorable      = []
            unfavorable    = []
            upcoming_good  = []

            for h in horas:
                planet = h.get("planet", h.get("hora_planet", h.get("name", "")))
                is_now = h.get("is_current", h.get("current", False))
                time_  = h.get("time", h.get("start_time", h.get("startTime", "")))

                if is_now:
                    current_hora = planet

                if planet in ("Jupiter", "Venus", "Mercury", "Moon"):
                    favorable.append({"planet": planet, "time": time_, "is_current": is_now})
                    if not is_now:
                        upcoming_good.append({"planet": planet, "time": time_})
                elif planet in ("Saturn", "Mars"):
                    unfavorable.append({"planet": planet, "time": time_, "is_current": is_now})

            result = {
                "context_type":      "panchang/hora-muhurta",
                "current_hora":      current_hora,
                "favorable_horas":   favorable[:4],
                "unfavorable_horas": unfavorable[:4],
                "upcoming_good":     upcoming_good[:3],
                "total_horas":       len(horas),
                "all_horas":         horas[:12],
            }
            logger.info(
                f"[EXTRACT]   current_hora={current_hora} | "
                f"favorable={[h['planet'] for h in favorable]} | "
                f"unfavorable={[h['planet'] for h in unfavorable]}"
            )
            return result

        elif api == "choghadiya-muhurta":
            chog_list = (
                raw if isinstance(raw, list) else
                raw.get("choghadiya", raw.get("day", raw.get("choghadiya_day", [])))
            )
            good_types = {"amrit", "shubh", "labh", "char"}
            good = []
            bad  = []
            for period in (chog_list if isinstance(chog_list, list) else []):
                ptype = period.get("type", period.get("muhurta_type", "")).lower()
                if any(g in ptype for g in good_types):
                    good.append(period)
                else:
                    bad.append(period)

            result = {
                "context_type":         "panchang/choghadiya-muhurta",
                "favorable_periods":    good[:5],
                "unfavorable_periods":  bad[:3],
                "total_periods":        len(chog_list) if isinstance(chog_list, list) else 0,
            }
            logger.info(f"[EXTRACT]   choghadiya favorable={len(good)} unfavorable={len(bad)}")
            return result

        elif api in ("monthly-panchang", "moon-calendar"):
            days = raw if isinstance(raw, list) else raw.get("days", raw.get("months", []))
            result = {
                "context_type": f"panchang/{api}",
                "days_count":   len(days),
                "days_summary": days[:5],
            }
            logger.info(f"[EXTRACT]   monthly days_count={len(days)}")
            return result

        elif api == "festivals":
            events = raw if isinstance(raw, list) else raw.get("festivals", raw.get("events", []))
            result = {
                "context_type": "panchang/festivals",
                "festivals":    events[:10],
                "total":        len(events),
            }
            logger.info(f"[EXTRACT]   festivals count={len(events)}")
            return result

        elif api == "moon-phase":
            result = {
                "context_type": "panchang/moon-phase",
                "phase":        raw.get("phase",        raw.get("moon_phase", "")),
                "illumination": raw.get("illumination", raw.get("illumation", 0)),
                "paksha":       raw.get("paksha",       ""),
                "tithi":        raw.get("tithi",        ""),
            }
            logger.info(f"[EXTRACT]   moon_phase={result['phase']} illumination={result['illumination']}")
            return result

        elif api in ("sunrise", "sunset", "moonrise", "moonset", "solarnoon"):
            result = {
                "context_type": f"panchang/{api}",
                "time":         raw.get("time", raw.get(api, raw.get(api.replace("solar", "solar_"), ""))),
            }
            logger.info(f"[EXTRACT]   {api} time={result['time']}")
            return result

        elif api == "transit":
            transits = raw.get("transits", raw.get("planet_transits", raw if isinstance(raw, list) else []))
            result = {
                "context_type":   "panchang/transit",
                "transits":       transits,
                "transit_count":  len(transits) if isinstance(transits, list) else 0,
            }
            logger.info(f"[EXTRACT]   transit count={result['transit_count']}")
            return result

        elif api == "retrogrades":
            retros = raw.get("retrogrades", raw.get("retrograde_planets", raw if isinstance(raw, list) else []))
            result = {
                "context_type":       "panchang/retrogrades",
                "retrograde_planets": retros,
                "total_retrogrades":  len(retros) if isinstance(retros, list) else 0,
            }
            logger.info(f"[EXTRACT]   retrograde planets={retros}")
            return result

        else:
            logger.warning(f"[EXTRACT] panchang/{api} — no specific extractor, using generic")
            return self._generic_extract(raw, "panchang", api)

    def _extract_hora_list(self, raw: Any) -> List[Dict]:
        """Normalise hora list from any response shape."""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("horas", "hora", "hora_list", "muhurta"):
                if key in raw and isinstance(raw[key], list):
                    return raw[key]
            # Sometimes horas are dict values with numeric keys
            horas = []
            for k, v in raw.items():
                if isinstance(v, dict) and ("planet" in v or "hora_planet" in v):
                    horas.append(v)
            return horas
        return []

    # ═══════════════════════════════════════════════════════════
    # GROUP 4 — DASHAS
    # APIs: antar-dasha, char-dasha-current, char-dasha-main,
    #       char-dasha-sub, current-mahadasha,
    #       current-mahadasha-full, maha-dasha,
    #       maha-dasha-predictions, paryantar-dasha,
    #       specific-sub-dasha, yogini-dasha-main,
    #       yogini-dasha-sub
    # ═══════════════════════════════════════════════════════════

    def _extract_dashas(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] dashas/{api} — extracting dasha data")

        def _planet_name(obj) -> str:
            if isinstance(obj, dict):
                return obj.get("planet", obj.get("name", obj.get("dasa_planet", "")))
            return str(obj)

        if api in ("current-mahadasha", "current-mahadasha-full"):
            maha    = raw.get("maha_dasha",       raw.get("mahadasha",    raw.get("currentMahadasha", {})))
            antar   = raw.get("antar_dasha",      raw.get("antardasha",   raw.get("currentAntardasha", {})))
            pratyan = raw.get("pratyantar_dasha",  raw.get("pratyantardasha", raw.get("sookshma", {})))

            maha_planet  = _planet_name(maha)
            antar_planet = _planet_name(antar)
            prat_planet  = _planet_name(pratyan)

            maha_end  = (maha.get("end_date",  maha.get("endDate",  "")) if isinstance(maha,  dict) else "")
            antar_end = (antar.get("end_date", antar.get("endDate", "")) if isinstance(antar, dict) else "")

            result = {
                "context_type":     "dashas/current-mahadasha",
                "mahadasha":        maha_planet,
                "antardasha":       antar_planet,
                "pratyantar_dasha": prat_planet,
                "maha_end_date":    maha_end,
                "antar_end_date":   antar_end,
                "raw_maha":         maha  if isinstance(maha,  dict) else {},
                "raw_antar":        antar if isinstance(antar, dict) else {},
            }
            logger.info(
                f"[EXTRACT]   maha={maha_planet} | antar={antar_planet} | "
                f"pratyantar={prat_planet} | maha_ends={maha_end}"
            )
            return result

        elif api == "maha-dasha":
            periods = (
                raw.get("dasha_periods", raw.get("mahadasha_list",
                raw.get("dasha",         raw if isinstance(raw, list) else [])))
            )
            current = next(
                (p for p in (periods if isinstance(periods, list) else [])
                 if p.get("is_current", p.get("current", False))),
                {}
            )
            result = {
                "context_type":    "dashas/maha-dasha",
                "all_periods":     periods[:10] if isinstance(periods, list) else [],
                "current_period":  current,
                "mahadasha":       _planet_name(current) if current else "",
            }
            logger.info(f"[EXTRACT]   maha-dasha current={result['mahadasha']} total periods={len(result['all_periods'])}")
            return result

        elif api == "antar-dasha":
            antar_periods = raw.get("antar_periods", raw.get("antarDasha", raw.get("antar_dasha", [])))
            current = next(
                (p for p in (antar_periods if isinstance(antar_periods, list) else [])
                 if p.get("is_current", p.get("current", False))),
                {}
            )
            result = {
                "context_type":  "dashas/antar-dasha",
                "antar_periods": antar_periods if isinstance(antar_periods, list) else [],
                "antardasha":    _planet_name(current) if current else "",
            }
            logger.info(f"[EXTRACT]   antar-dasha current={result['antardasha']}")
            return result

        elif api == "maha-dasha-predictions":
            result = {
                "context_type":    "dashas/maha-dasha-predictions",
                "prediction_text": raw.get("prediction",
                                    raw.get("maha_dasha_prediction",
                                    raw.get("bot_response", ""))),
                "mahadasha":       raw.get("mahadasha",  raw.get("dasa_planet", "")),
            }
            logger.info(f"[EXTRACT]   maha-dasha-predictions mahadasha={result['mahadasha']} text_len={len(result['prediction_text'])}")
            return result

        elif api in ("yogini-dasha-main", "yogini-dasha-sub"):
            yogini = raw.get("yogini_dasha", raw.get("current_yogini", raw.get("yogini", {})))
            result = {
                "context_type": f"dashas/{api}",
                "yogini_dasha": yogini,
                "yogini_name":  yogini.get("name", yogini.get("yogini", "")) if isinstance(yogini, dict) else str(yogini),
            }
            logger.info(f"[EXTRACT]   yogini_dasha={result['yogini_name']}")
            return result

        elif api in ("char-dasha-main", "char-dasha-sub", "char-dasha-current"):
            char = raw.get("char_dasha", raw.get("charDasha", raw.get("char", {})))
            result = {
                "context_type": f"dashas/{api}",
                "char_dasha":   char,
                "current_rasi": char.get("rasi", char.get("sign", "")) if isinstance(char, dict) else "",
            }
            logger.info(f"[EXTRACT]   char_dasha rasi={result['current_rasi']}")
            return result

        elif api in ("specific-sub-dasha", "paryantar-dasha"):
            planet = raw.get("planet", raw.get("planet_name", ""))
            result = {
                "context_type": f"dashas/{api}",
                "sub_dasha":    raw,
                "planet":       planet,
            }
            logger.info(f"[EXTRACT]   specific sub-dasha planet={planet}")
            return result

        else:
            logger.warning(f"[EXTRACT] dashas/{api} — no specific extractor, using generic")
            return self._generic_extract(raw, "dashas", api)

    # ═══════════════════════════════════════════════════════════
    # GROUP 5 — DOSHA
    # APIs: kaalsarp-dosh, mangal-dosh, manglik-dosh, pitra-dosh
    # ═══════════════════════════════════════════════════════════

    def _extract_dosha(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] dosha/{api} — extracting dosha data")

        has_dosha   = raw.get("has_dosha",   raw.get("mangal_dosha",  raw.get("is_present", raw.get("present", False))))
        severity    = raw.get("severity",    raw.get("level",         raw.get("dosha_level", "")))
        description = raw.get("description", raw.get("dosha_details", raw.get("bot_response", raw.get("summary", ""))))
        remedies    = raw.get("remedies",    raw.get("remedy",        []))

        if api == "kaalsarp-dosh":
            has_dosha  = raw.get("present",    raw.get("is_kaal_sarp_dosha", raw.get("kaal_sarp_dosha", has_dosha)))
            dosha_type = raw.get("type",       raw.get("kaal_sarp_type",     raw.get("ks_type", "")))
            result = {
                "context_type": "dosha/kaalsarp-dosh",
                "has_dosha":    bool(has_dosha),
                "dosha_type":   dosha_type,
                "severity":     str(severity),
                "description":  str(description)[:400],
                "remedies":     remedies,
            }

        elif api in ("mangal-dosh", "manglik-dosh"):
            result = {
                "context_type":    f"dosha/{api}",
                "has_dosha":       bool(has_dosha),
                "mangal_position": raw.get("mangal_position", raw.get("mars_position", raw.get("position", ""))),
                "affected_houses": raw.get("affected_houses", raw.get("houses_affected", [])),
                "severity":        str(severity),
                "description":     str(description)[:400],
                "remedies":        remedies,
                "is_cancellation": raw.get("is_cancellation", raw.get("mangal_cancel", False)),
            }

        elif api == "pitra-dosh":
            result = {
                "context_type": "dosha/pitra-dosh",
                "has_dosha":    bool(has_dosha),
                "severity":     str(severity),
                "description":  str(description)[:400],
                "remedies":     remedies,
                "sun_position": raw.get("sun_position", ""),
            }

        else:
            result = {
                "context_type": f"dosha/{api}",
                "has_dosha":    bool(has_dosha),
                "severity":     str(severity),
                "description":  str(description)[:400],
                "remedies":     remedies,
            }

        logger.info(
            f"[EXTRACT]   has_dosha={result['has_dosha']} "
            f"severity={result['severity']} "
            f"description_len={len(result['description'])}"
        )
        return result

    # ═══════════════════════════════════════════════════════════
    # GROUP 6 — EXTENDED HOROSCOPE
    # APIs: arutha-lagnas, current-sade-sati, kundli-details,
    #       find-ascendant, find-moon-sign, find-sun-sign,
    #       friendship, gem-suggestion, jaimini-karakas,
    #       kp-houses, kp-planets, numero-table,
    #       rudraksh-suggestion, sade-sati-table, shad-bala,
    #       varshapal-details, varshapal-month-chart,
    #       varshapal-year-chart, yoga-calculator
    # ═══════════════════════════════════════════════════════════

    def _extract_extended(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] extended/{api} — extracting extended data")

        # APIs that return planet-block format
        PLANET_BLOCK_APIS = {
            "kundli-details", "find-ascendant", "find-moon-sign",
            "find-sun-sign", "friendship", "jaimini-karakas",
            "kp-houses", "kp-planets", "shad-bala",
            "yoga-calculator", "arutha-lagnas", "numero-table",
        }

        if api in PLANET_BLOCK_APIS:
            result = self._extract_planets_block(raw, api)
            result["context_type"]    = f"extended/{api}"
            result["interpretation"]  = raw.get("interpretation", raw.get("description", raw.get("bot_response", "")))
            result["special_yogas"]   = raw.get("yogas", raw.get("yoga_list", []))
            if api == "find-ascendant":
                result["ascendant"] = raw.get("ascendant", raw.get("lagna", raw.get("rising_sign", "")))
                logger.info(f"[EXTRACT]   ascendant={result['ascendant']}")
            elif api == "find-moon-sign":
                result["moon_sign"] = raw.get("moon_sign", raw.get("moonSign", raw.get("rasi", "")))
                logger.info(f"[EXTRACT]   moon_sign={result['moon_sign']}")
            elif api == "find-sun-sign":
                result["sun_sign"] = raw.get("sun_sign", raw.get("sunSign", ""))
                logger.info(f"[EXTRACT]   sun_sign={result['sun_sign']}")
            elif api == "yoga-calculator":
                yogas = raw.get("yogas", raw.get("yoga_list", []))
                result["yogas"] = yogas
                logger.info(f"[EXTRACT]   yogas count={len(yogas) if isinstance(yogas, list) else 0}")
            return result

        elif api == "sade-sati-table":
            result = {
                "context_type":     "extended/sade-sati-table",
                "is_in_sade_sati":  raw.get("is_in_sade_sati", raw.get("current_phase_active", raw.get("is_active", False))),
                "current_phase":    raw.get("current_phase",   raw.get("phase", "")),
                "phase_details":    raw.get("phase_details",   {}),
                "periods":          raw.get("periods",         raw.get("sade_sati_periods", [])),
                "moon_sign":        raw.get("moon_sign",       ""),
            }
            logger.info(f"[EXTRACT]   sade-sati is_active={result['is_in_sade_sati']} phase={result['current_phase']}")
            return result

        elif api == "current-sade-sati":
            result = {
                "context_type": "extended/current-sade-sati",
                "is_active":    raw.get("is_active",    raw.get("active",        raw.get("is_in_sade_sati", False))),
                "phase":        raw.get("phase",        raw.get("current_phase", "")),
                "saturn_sign":  raw.get("saturn_sign",  raw.get("saturnSign",    "")),
                "moon_sign":    raw.get("moon_sign",    raw.get("moonSign",      "")),
                "description":  str(raw.get("description", raw.get("bot_response", "")))[:400],
            }
            logger.info(f"[EXTRACT]   sade-sati is_active={result['is_active']} phase={result['phase']}")
            return result

        elif api == "gem-suggestion":
            gems = raw.get("suggestions", raw.get("gems", raw.get("gem_list", [])))
            result = {
                "context_type": "extended/gem-suggestion",
                "suggestions":  gems,
                "primary_gem":  gems[0] if isinstance(gems, list) and gems else {},
                "description":  str(raw.get("description", raw.get("bot_response", "")))[:300],
            }
            logger.info(f"[EXTRACT]   gem suggestions count={len(gems) if isinstance(gems, list) else 0}")
            return result

        elif api == "rudraksh-suggestion":
            rudraksha = raw.get("rudraksha", raw.get("suggestions", raw.get("rudraksha_list", [])))
            result = {
                "context_type": "extended/rudraksh-suggestion",
                "suggestions":  rudraksha,
                "primary":      rudraksha[0] if isinstance(rudraksha, list) and rudraksha else {},
                "description":  str(raw.get("description", ""))[:300],
            }
            logger.info(f"[EXTRACT]   rudraksha suggestions count={len(rudraksha) if isinstance(rudraksha, list) else 0}")
            return result

        elif api in ("varshapal-details", "varshapal-month-chart", "varshapal-year-chart"):
            result = self._extract_planets_block(raw, api)
            result["context_type"] = f"extended/{api}"
            result["year"]         = raw.get("year", raw.get("varshapal_year", ""))
            logger.info(f"[EXTRACT]   varshapal year={result['year']}")
            return result

        else:
            logger.warning(f"[EXTRACT] extended/{api} — no specific extractor, using planet-block fallback")
            result = self._extract_planets_block(raw, api)
            result["context_type"] = f"extended/{api}"
            return result

    # ═══════════════════════════════════════════════════════════
    # GROUP 7 — MATCHING
    # APIs: aggregate-match, ashtakoot, ashtakoot-astro-details,
    #       bulk-ashtakoot, bulk-dashakoot, bulk-nakshatra-match,
    #       bulk-western-match, dashakoot,
    #       dashakoot-astro-details, nakshatra-match,
    #       papasamaya, papasamaya-match, quick-matcher,
    #       rajju-vedha, south-match, western-match
    # ═══════════════════════════════════════════════════════════

    def _extract_matching(self, raw: Dict, api: str) -> Dict:
        logger.info(f"[EXTRACT] matching/{api} — extracting compatibility data")

        BULK_APIS = {"bulk-nakshatra-match", "bulk-ashtakoot", "bulk-dashakoot", "bulk-western-match"}

        if api in BULK_APIS:
            results = (
                raw if isinstance(raw, list)
                else raw.get("results", raw.get("matches", raw.get("profiles", [])))
            )
            scores = [
                r.get("total_points", r.get("score", r.get("match_score", r.get("points", 0))))
                for r in (results if isinstance(results, list) else [])
                if isinstance(r, dict)
            ]
            result = {
                "context_type":   f"matching/{api}",
                "bulk_results":   results[:10],
                "total_profiles": len(results) if isinstance(results, list) else 0,
                "avg_score":      round(sum(scores) / len(scores), 1) if scores else 0,
                "best_score":     max(scores) if scores else 0,
                "worst_score":    min(scores) if scores else 0,
            }
            logger.info(
                f"[EXTRACT]   bulk profiles={result['total_profiles']} "
                f"best={result['best_score']} avg={result['avg_score']}"
            )
            return result

        # Single-match APIs
        total_points  = raw.get("total_points",  raw.get("totalPoints",   raw.get("score",       0)))
        max_points    = raw.get("total_maximum", raw.get("maximumPoints", raw.get("max_score",   36)))
        compatibility = raw.get("compatibility", raw.get("match_result",  raw.get("result",      "")))
        conclusion    = raw.get("conclusion",    raw.get("bot_response",  raw.get("description", "")))
        koota_details = raw.get("koota_details", raw.get("koot_points",   raw.get("kuta_details",[])))

        # API-specific max points
        if api in ("dashakoot", "dashakoot-astro-details"):
            max_points = max_points or 55
        elif api in ("south-match",):
            max_points = max_points or 10
        else:
            max_points = max_points or 36

        match_percent = round((total_points / max_points) * 100, 1) if max_points else 0

        # Extract individual birth charts if embedded
        male_planets   = {}
        female_planets = {}
        for key in ("male_details", "male_birth_details", "boy_details"):
            if key in raw:
                male_planets = self._extract_planets_block(raw[key], api).get("planets", {})
                break
        for key in ("female_details", "female_birth_details", "girl_details"):
            if key in raw:
                female_planets = self._extract_planets_block(raw[key], api).get("planets", {})
                break

        result = {
            "context_type":   f"matching/{api}",
            "total_points":   total_points,
            "max_points":     max_points,
            "match_percent":  match_percent,
            "compatibility":  compatibility,
            "conclusion":     str(conclusion)[:400],
            "koota_details":  koota_details,
            "male_planets":   male_planets,
            "female_planets": female_planets,
        }
        logger.info(
            f"[EXTRACT]   match {total_points}/{max_points} ({match_percent}%) "
            f"compatibility={compatibility}"
        )
        return result

    # ═══════════════════════════════════════════════════════════
    # GROUP 8 — UTILITIES
    # APIs: geo-search, geo-search-advanced, gem-details,
    #       nakshatra-vastu, radical-number
    # ═══════════════════════════════════════════════════════════

    def _extract_utilities(self, raw: Dict, api: str, params: Dict) -> Dict:
        logger.info(f"[EXTRACT] utilities/{api} — extracting utility data")

        if api == "geo-search":
            places = (
                raw if isinstance(raw, list)
                else raw.get("places", raw.get("results", raw.get("locations", [raw])))
            )
            best = places[0] if isinstance(places, list) and places else {}
            result = {
                "context_type": "utilities/geo-search",
                "query":        params.get("place", ""),
                "lat":          best.get("lat",       best.get("latitude",  0)),
                "lon":          best.get("lon",       best.get("longitude", 0)),
                "place_name":   best.get("name",      best.get("place",     "")),
                "all_results":  places[:3],
            }
            logger.info(f"[EXTRACT]   geo-search query={result['query']} lat={result['lat']} lon={result['lon']}")
            return result

        elif api == "geo-search-advanced":
            places = raw if isinstance(raw, list) else raw.get("places", [raw])
            best   = places[0] if isinstance(places, list) and places else {}
            result = {
                "context_type": "utilities/geo-search-advanced",
                "place":        params.get("place", ""),
                "state":        params.get("state", ""),
                "lat":          best.get("lat", best.get("latitude",  0)),
                "lon":          best.get("lon", best.get("longitude", 0)),
                "all_results":  places[:5],
            }
            logger.info(f"[EXTRACT]   geo-search-advanced {result['place']},{result['state']} lat={result['lat']}")
            return result

        elif api == "gem-details":
            result = {
                "context_type": "utilities/gem-details",
                "stone":        params.get("stone", raw.get("gem_name", raw.get("stone", ""))),
                "planet":       raw.get("ruling_planet", raw.get("planet", "")),
                "benefits":     raw.get("benefits",      raw.get("gem_benefits", "")),
                "wearing_day":  raw.get("wearing_day",   raw.get("best_day", "")),
                "description":  str(raw.get("description", ""))[:400],
            }
            logger.info(f"[EXTRACT]   gem stone={result['stone']} planet={result['planet']}")
            return result

        elif api == "nakshatra-vastu":
            result = {
                "context_type": "utilities/nakshatra-vastu",
                "nakshatra":    params.get("nakshatra", raw.get("nakshatra", "")),
                "direction":    raw.get("direction",    raw.get("vastu_direction", raw.get("ideal_direction", ""))),
                "description":  str(raw.get("description", raw.get("vastu_info", "")))[:400],
            }
            logger.info(f"[EXTRACT]   nakshatra={result['nakshatra']} direction={result['direction']}")
            return result

        elif api == "radical-number":
            result = {
                "context_type":   "utilities/radical-number",
                "radical_number": raw.get("radical_number", raw.get("number",          raw.get("numerology_number", ""))),
                "planet":         raw.get("ruling_planet",  raw.get("planet",           "")),
                "description":    str(raw.get("description", raw.get("number_info",     "")))[:400],
                "lucky_numbers":  raw.get("lucky_numbers",  raw.get("lucky",            [])),
            }
            logger.info(f"[EXTRACT]   radical_number={result['radical_number']} planet={result['planet']}")
            return result

        else:
            logger.warning(f"[EXTRACT] utilities/{api} — no specific extractor, using generic")
            return self._generic_extract(raw, "utilities", api)

    # ═══════════════════════════════════════════════════════════
    # FALLBACK — catches anything not handled above
    # ═══════════════════════════════════════════════════════════

    def _generic_extract(self, raw: Any, group: str, api: str) -> Dict:
        logger.info(f"[EXTRACT] generic extractor for {group}/{api}")
        extracted = {"context_type": f"{group}/{api}"}
        if isinstance(raw, dict):
            for k, v in raw.items():
                if isinstance(v, (str, int, float, bool)):
                    extracted[k] = v
                    logger.info(f"[EXTRACT]   {k} = {str(v)[:80]}")
        elif isinstance(raw, list):
            extracted["items"] = raw[:5]
            logger.info(f"[EXTRACT]   list with {len(raw)} items, taking first 5")
        return extracted

    # ───────────────────────────────────────────────────────────
    # UTILITY
    # ───────────────────────────────────────────────────────────

    @staticmethod
    def _safe_int(val) -> int:
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0
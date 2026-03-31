# Drishtii API — Input Guide for All 7 Categories

## Endpoint

```
POST /api/drishtii/analyze
```

## How It Works

You send only **5 fields**:

| Field | Required | Description |
|-------|----------|-------------|
| `category` | ✅ | One of the 7 categories below |
| `objective` | ✅ | Your question in plain text (min 5 chars) |
| `params` | ✅ | Birth details (flat dict — see each category) |
| `language` | optional | `en` (default), `hi`, `ta`, `te`, `mr`, `gu`, `bn`, `kn`, `ml`, `pa`, `ur` |
| `target_date` | optional | `DD/MM/YYYY` — defaults to today |

> ⚠️ **Do NOT send `group` or `api`** — these are auto-selected by the engine based on `category`.

> ⚠️ **Do NOT nest params** — send as a flat dict, not wrapped in another key.

---

## 7 Categories

---

### 💰 1. `financial` — Investment, Stocks, Loans, Savings

**Use for:** Stock market decisions, investment timing, loan approvals, wealth management, savings plans, mutual funds.

**APIs auto-called internally:**
- `horoscope/planet-details` (birth chart)
- `dashas/current-mahadasha-full` (current dasha — auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "financial",
  "objective": "Should I invest in stocks today?",
  "language": "hi",
  "target_date": "26/03/2026",
  "params": {
    "dob": "15/08/1990",
    "tob": "10:30",
    "lat": 26.85,
    "lon": 75.79,
    "tz": 5.5
  }
}
```

---

### 🏢 2. `business` — Business, Tenders, Contracts, Partnerships

**Use for:** Starting a business, filing government tenders, signing contracts, evaluating business partnerships, entrepreneurship timing.

**APIs auto-called internally:**
- `horoscope/planet-details`
- `dashas/current-mahadasha-full` (auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "business",
  "objective": "Is this a good time to file the government tender?",
  "language": "en",
  "target_date": "26/03/2026",
  "params": {
    "dob": "10/05/1985",
    "tob": "07:15",
    "lat": 28.61,
    "lon": 77.20,
    "tz": 5.5
  }
}
```

---

### 💼 3. `career` — Job Change, Promotion, Interview, Resignation

**Use for:** Switching jobs, accepting a promotion, attending an interview, starting a new profession, evaluating a job offer.

**APIs auto-called internally:**
- `horoscope/planet-details`
- `dashas/current-mahadasha-full` (auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "career",
  "objective": "Is this a good time to switch jobs?",
  "language": "en",
  "target_date": "26/03/2026",
  "params": {
    "dob": "3/11/1992",
    "tob": "14:45",
    "lat": 19.07,
    "lon": 72.87,
    "tz": 5.5
  }
}
```

---

### ❤️ 4. `marriage` — Kundali Matching, Compatibility, Engagement

**Use for:** Checking kundali compatibility before marriage, evaluating a marriage proposal, engagement timing.

> ⚠️ **Requires BOTH male and female birth details** (prefixed `m_` and `f_`).
> ⚠️ **This category cannot be used for financial, business, NRI, or legal queries.**

**API auto-called internally:**
- `matching/ashtakoot` (kundali compatibility scoring)

**Required params:** `m_dob`, `m_tob`, `m_lat`, `m_lon`, `m_tz`, `f_dob`, `f_tob`, `f_lat`, `f_lon`, `f_tz`

```json


{
  "category": "marriage",
  "objective": "hamari sadi ke liye",
  "language": "hi",
  "target_date": "26/3/2026",
  "params": {
    
  "boy_dob": "15/8/1990",
  "boy_tob": "10:30",
  "boy_lat": 26.85,
  "boy_lon": 75.79,
  "boy_tz": 5.5,
  "girl_dob": "20/5/1992",
  "girl_tob": "08:00",
  "girl_lat": 28.61,
  "girl_lon": 77.2,
  "girl_tz": 5.5,
  "lang": "en"

  }
}
```

---

### ⚖️ 5. `legal` — Court Cases, Disputes, Legal Filing

**Use for:** Filing a court case, attending a hearing, legal document signing, property disputes, police/FIR matters.

**APIs auto-called internally:**
- `horoscope/planet-details`
- `dashas/current-mahadasha-full` (auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "legal",
  "objective": "Should I file the property dispute case this week?",
  "language": "en",
  "target_date": "26/3/2026",
  "params": {
    "dob": "7/2/1978",
    "tob": "09:00",
    "lat": 23.02,
    "lon": 72.57,
    "tz": 5.5
  }
}
```

---

### 🏥 6. `health` — Surgery, Medical Procedures, Treatment Timing

**Use for:** Timing major surgeries, starting treatment plans, recovery decisions, mental health interventions, medical procedures.

**APIs auto-called internally:**
- `horoscope/planet-details`
- `dashas/current-mahadasha-full` (auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "health",
  "objective": "Is this a good time for my knee surgery?",
  "language": "en",
  "target_date": "26/3/2026",
  "params": {
    "dob": "22/9/1965",
    "tob": "06:30",
    "lat": 13.08,
    "lon": 80.27,
    "tz": 5.5
  }
}
```

---

### ✈️ 7. `travel` — NRI, Visa, Migration, Foreign Opportunities

**Use for:** Visa applications, moving abroad, settling in a foreign country, NRI investment decisions, international travel timing.

**APIs auto-called internally:**
- `horoscope/planet-details`
- `dashas/current-mahadasha-full` (auto supplemented)

**Required params:** `dob`, `tob`, `lat`, `lon`, `tz`

```json
{
  "category": "travel",
  "objective": "Should I apply for a US visa this month?",
  "language": "en",
  "target_date": "26/3/2026",
  "params": {
    "dob": "14/6/1995",
    "tob": "11:20",
    "lat": 17.38,
    "lon": 78.48,
    "tz": 5.5
  }
}
```

---

## Parameter Reference

| Param | Type | Example | Notes |
|-------|------|---------|-------|
| `dob` | string | `"15/8/1990"` | Date of birth — DD/MM/YYYY |
| `tob` | string | `"10:30"` | Time of birth — HH:MM (24h) |
| `lat` | float | `26.85` | Latitude of birth place |
| `lon` | float | `75.79` | Longitude of birth place |
| `tz` | float | `5.5` | Timezone offset (IST = 5.5) |
| `m_dob` | string | `"15/8/1990"` | Male DOB — marriage only |
| `m_tob` | string | `"10:30"` | Male TOB — marriage only |
| `m_lat` | float | `26.85` | Male birth lat — marriage only |
| `m_lon` | float | `75.79` | Male birth lon — marriage only |
| `m_tz` | float | `5.5` | Male TZ — marriage only |
| `f_dob` | string | `"20/5/1992"` | Female DOB — marriage only |
| `f_tob` | string | `"08:00"` | Female TOB — marriage only |
| `f_lat` | float | `28.61` | Female birth lat — marriage only |
| `f_lon` | float | `77.20` | Female birth lon — marriage only |
| `f_tz` | float | `5.5` | Female TZ — marriage only |

---

## Category-Query Guard

The API enforces topic relevance. If you choose `marriage` but ask a financial or travel question, you will get a `400` error with a helpful message:

```json
{
  "detail": "⚠️ Your question appears to be about 'invest', which is outside the scope of the '❤️ Family/Marriage' category. Please choose the appropriate category for your question."
}
```

---

## Response Format

All 7 categories return the same output structure:

```json
{
  "status": 200,
  "verdict": "GO | CAUTION | AVOID",
  "confidence": "HIGH | MEDIUM | LOW",
  "total_score": 31,
  "max_score": 40,
  "scores": { ... },
  "warnings": [ ... ],
  "positive_signals": [ ... ],
  "summary": "...",
  "time_quality": {
    "dasha": "...",
    "moon": "...",
    "gochar": "...",
    "nakshatra": "...",
    "hora": "..."
  },
  "category_analysis": {
    "focus_area": "...",
    "key_factors": [ ... ],
    "risk_factors": [ ... ]
  },
  "timing": {
    "best_windows": [ ... ],
    "avoid_windows": [ ... ]
  },
  "final_recommendation": {
    "what_to_do": [ ... ],
    "what_to_avoid": [ ... ]
  },
  "category": "financial",
  "category_label": "💰 Financial",
  "objective": "Should I invest in stocks today?",
  "language": "hi",
  "target_date": "26/3/2026",
  "apis_used": [
    "horoscope/planet-details",
    "dashas/current-mahadasha-full"
  ],
  "validation_passed": true,
  "execution_time": 29.5,
  "timestamp": 1774630433
}
```

---

## Quick Reference: Find Your Birth Coordinates

Use `GET /api/drishtii/categories` to see all examples in one call.

Use the geo-search utility for coordinates:
```
GET /api/astro/utilities/geo-search?place=Jodhpur
```

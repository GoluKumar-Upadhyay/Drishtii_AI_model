# # import asyncpg
# # import os
# # from dotenv import load_dotenv
# # from fastapi import FastAPI

# # load_dotenv()

# # DATABASE_URL = os.getenv("DATABASE_URL", "")

# # app = FastAPI()


# # @app.get("/test-db")
# # async def test_db():
# #     sql = r"""
# #         SELECT 'magicbricks' as source, COUNT(*) as count
# #         FROM property_raw_data
# #         WHERE source = 'magicbricks'
# #           AND scraped_data->>'city_name' ILIKE '%Mumbai%'
# #           AND (scraped_data->>'price')::numeric BETWEEN 9216000 AND 13824000

# #         UNION ALL

# #         SELECT 'housing' as source, COUNT(*) as count
# #         FROM property_raw_data
# #         WHERE source = 'housing'
# #           AND scraped_data->'polygons_hash'->'city'->>'name' ILIKE '%Mumbai%'
# #           AND (scraped_data->>'min_price')::numeric BETWEEN 9216000 AND 13824000

# #         UNION ALL

# #         SELECT '99acres' as source, COUNT(*) as count
# #         FROM property_raw_data
# #         WHERE source = '99acres'
# #           AND scraped_data->>'description' ILIKE '%Mumbai%'
# #           AND CASE
# #                 WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
# #                   THEN (REGEXP_REPLACE(
# #                           SPLIT_PART(
# #                             REGEXP_REPLACE(scraped_data->>'priceRange', '[₹\s,]', '', 'g'),
# #                           '-', 1),
# #                         '[^0-9.]', '', 'g')
# #                        )::numeric * 10000000
# #                 WHEN scraped_data->>'priceRange' ILIKE '%L%'
# #                   THEN (REGEXP_REPLACE(
# #                           SPLIT_PART(
# #                             REGEXP_REPLACE(scraped_data->>'priceRange', '[₹\s,]', '', 'g'),
# #                           '-', 1),
# #                         '[^0-9.]', '', 'g')
# #                        )::numeric * 100000
# #                 ELSE NULL
# #               END
# #               BETWEEN 9216000 AND 13824000
# #     """

# #     try:
# #         conn = await asyncpg.connect(dsn=DATABASE_URL)
# #         rows = await conn.fetch(sql)
# #         await conn.close()

# #         results = [{"source": row["source"], "count": row["count"]} for row in rows]
# #         total   = sum(row["count"] for row in rows)

# #         return {
# #             "status":  "SUCCESS" if total > 0 else "No data found",
# #             "total":   total,
# #             "results": results,
# #         }

# #     except Exception as e:
# #         return {
# #             "status": "QUERY FAILED",
# #             "error":  str(e),
# #         }


# from google import genai

# client = genai.Client(
#     vertexai=True,
#     project="bustling-joy-488514-u2",
#     location="us-central1"
# )

# response = client.models.generate_content(
#     model="gemini-2.5-pro",
#     contents="Explain astrology in simple terms. What is astrology and how do astrologers use planets and zodiac signs to make predictions?"
# )

# print(response.text)


"""
Drishtii — Full Connection & Query Test
========================================
Run this independently to verify:
  1. DB connection + all 3 source queries
  2. Google Places Text Search (coord resolution)
  3. Google Places Nearby Search (DNA scoring)
  4. Full pipeline simulation (mini end-to-end)

Usage:
    python test_drishtii.py

No FastAPI server needed. Just make sure your .env is present.
"""

import asyncio
import json
import os
import re
import sys
import httpx
import asyncpg
from dotenv import load_dotenv

load_dotenv()

# ── Config from env ──────────────────────────────────────────────────────────
DATABASE_URL          = os.getenv("DATABASE_URL", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
GOOGLE_GEOCODING_KEY  = os.getenv("GOOGLE_GEOCODING_API_KEY", "")

PLACES_NEARBY_URL     = "https://places.googleapis.com/v1/places:searchNearby"
PLACES_TEXT_URL       = "https://places.googleapis.com/v1/places:searchText"
GEOCODE_URL           = "https://maps.googleapis.com/maps/api/geocode/json"

# Test parameters — change these to match what you want to verify
TEST_CITY      = "Mumbai"
TEST_MIN_PRICE = 9_000_000
TEST_MAX_PRICE = 14_000_000
TEST_LAT       = 19.0760
TEST_LNG       = 72.8777

# ── Colours for terminal output ──────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}!{RESET} {msg}")
def info(msg):  print(f"  {BLUE}→{RESET} {msg}")
def section(title): print(f"\n{BOLD}{'─'*60}{RESET}\n{BOLD}{title}{RESET}\n{'─'*60}")

passed = 0
failed = 0

def record(success: bool, msg: str):
    global passed, failed
    if success:
        passed += 1
        ok(msg)
    else:
        failed += 1
        fail(msg)

# ── TEST 1: Environment variables ────────────────────────────────────────────
section("TEST 1 — Environment variables")

record(bool(DATABASE_URL),          f"DATABASE_URL is set")
record(bool(GOOGLE_PLACES_API_KEY), f"GOOGLE_PLACES_API_KEY is set")
if not GOOGLE_GEOCODING_KEY:
    warn("GOOGLE_GEOCODING_API_KEY not set (optional — Places Text Search used instead)")
else:
    ok(f"GOOGLE_GEOCODING_API_KEY is set")


# ── TEST 2: PostgreSQL connection ────────────────────────────────────────────
async def test_db():
    section("TEST 2 — PostgreSQL connection")
    try:
        conn = await asyncpg.connect(dsn=DATABASE_URL)
        version = await conn.fetchval("SELECT version()")
        ok(f"Connected: {version[:60]}...")

        # Check table exists
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='property_raw_data')"
        )
        record(exists, "Table property_raw_data exists")

        if exists:
            # Count rows per source
            rows = await conn.fetch(
                "SELECT source, COUNT(*) as cnt FROM property_raw_data GROUP BY source ORDER BY cnt DESC"
            )
            info(f"Rows per source:")
            for r in rows:
                print(f"    {r['source']:15} → {r['cnt']:,} rows")

            # Check price field types
            info("Checking price field storage per source:")

            mb_sample = await conn.fetchrow(
                "SELECT scraped_data->>'price' as price_text, "
                "pg_typeof(scraped_data->'price') as price_type "
                "FROM property_raw_data WHERE source='magicbricks' LIMIT 1"
            )
            if mb_sample:
                info(f"  magicbricks price text='{mb_sample['price_text']}' type={mb_sample['price_type']}")
                # Test the correct cast
                cast_ok = await conn.fetchval(
                    "SELECT (scraped_data->>'price')::numeric FROM property_raw_data "
                    "WHERE source='magicbricks' AND scraped_data->>'price' IS NOT NULL LIMIT 1"
                )
                record(cast_ok is not None, f"magicbricks price cast (->>'price')::numeric works → {cast_ok}")
            else:
                warn("No magicbricks rows found to test price cast")

        await conn.close()
    except Exception as e:
        fail(f"DB connection failed: {e}")


# ── TEST 3: DB queries per source ────────────────────────────────────────────
async def test_db_queries():
    section(f"TEST 3 — DB queries  city={TEST_CITY!r}  price=[{TEST_MIN_PRICE:,} – {TEST_MAX_PRICE:,}]")
    try:
        conn = await asyncpg.connect(dsn=DATABASE_URL)

        # 99acres
        sql_99 = rf"""
            SELECT COUNT(*) FROM property_raw_data
            WHERE source = '99acres'
              AND (scraped_data->>'description' ILIKE $1 OR scraped_data->>'title' ILIKE $1)
              AND scraped_data->>'priceRange' IS NOT NULL
              AND CASE
                    WHEN scraped_data->>'priceRange' ILIKE '%Cr%'
                      THEN (REGEXP_REPLACE(SPLIT_PART(
                             REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                             '-',1),'[^0-9.]','','g'))::numeric * 10000000
                    WHEN scraped_data->>'priceRange' ILIKE '%L%'
                      THEN (REGEXP_REPLACE(SPLIT_PART(
                             REGEXP_REPLACE(scraped_data->>'priceRange','[₹\s,]','','g'),
                             '-',1),'[^0-9.]','','g'))::numeric * 100000
                    ELSE NULL
                  END BETWEEN {TEST_MIN_PRICE} AND {TEST_MAX_PRICE}
        """
        cnt_99 = await conn.fetchval(sql_99, f"%{TEST_CITY}%")
        record(cnt_99 is not None, f"99acres query ran OK → {cnt_99} rows")

        # housing
        sql_ho = f"""
            SELECT COUNT(*) FROM property_raw_data
            WHERE source = 'housing'
              AND scraped_data->'polygons_hash'->'city'->>'name' ILIKE $1
              AND scraped_data->>'min_price' IS NOT NULL
              AND (scraped_data->>'min_price')::numeric BETWEEN {TEST_MIN_PRICE} AND {TEST_MAX_PRICE}
        """
        cnt_ho = await conn.fetchval(sql_ho, f"%{TEST_CITY}%")
        record(cnt_ho is not None, f"housing query ran OK → {cnt_ho} rows")

        # magicbricks — verify price cast works and show actual price range in DB
        # Step 1: Check city match works
        sql_mb_city = """
            SELECT COUNT(*) FROM property_raw_data
            WHERE source = 'magicbricks'
              AND scraped_data->>'city_name' ILIKE $1
        """
        cnt_mb_city = await conn.fetchval(sql_mb_city, f"%{TEST_CITY}%")
        record(cnt_mb_city is not None and cnt_mb_city > 0,
               f"magicbricks city match for {TEST_CITY!r} → {cnt_mb_city} total rows")

        # Step 2: Find actual min/max price in DB so we know what range to query
        price_range = await conn.fetchrow(
            "SELECT MIN((scraped_data->>'price')::numeric) as min_p, "
            "       MAX((scraped_data->>'price')::numeric) as max_p "
            "FROM property_raw_data WHERE source='magicbricks' "
            "AND scraped_data->>'city_name' ILIKE $1",
            f"%{TEST_CITY}%"
        )
        if price_range and price_range['min_p']:
            actual_min = int(price_range['min_p'])
            actual_max = int(price_range['max_p'])
            info(f"magicbricks actual price range in DB: ₹{actual_min:,} – ₹{actual_max:,}")

            # Step 3: Query using actual price range — should return rows
            sql_mb_correct = f"""
                SELECT COUNT(*) FROM property_raw_data
                WHERE source = 'magicbricks'
                  AND scraped_data->>'city_name' ILIKE $1
                  AND (scraped_data->>'price')::numeric BETWEEN {actual_min} AND {actual_max}
            """
            cnt_mb = await conn.fetchval(sql_mb_correct, f"%{TEST_CITY}%")
            record(cnt_mb > 0, f"magicbricks price query with correct range → {cnt_mb} rows ✓")

            # Step 4: Check if TEST price range overlaps
            if TEST_MAX_PRICE < actual_min:
                warn(f"Your TEST_MIN_PRICE/MAX_PRICE ({TEST_MIN_PRICE:,}–{TEST_MAX_PRICE:,}) "
                     f"is BELOW magicbricks prices ({actual_min:,}–{actual_max:,})")
                warn(f"Change TEST_MIN_PRICE = {actual_min} in test to see magicbricks results")
            else:
                sql_mb_range = f"""
                    SELECT COUNT(*) FROM property_raw_data
                    WHERE source = 'magicbricks'
                      AND scraped_data->>'city_name' ILIKE $1
                      AND (scraped_data->>'price')::numeric BETWEEN {TEST_MIN_PRICE} AND {TEST_MAX_PRICE}
                """
                cnt_mb_range = await conn.fetchval(sql_mb_range, f"%{TEST_CITY}%")
                record(cnt_mb_range >= 0,
                       f"magicbricks in test price range [{TEST_MIN_PRICE:,}–{TEST_MAX_PRICE:,}] → {cnt_mb_range} rows")
        else:
            warn("No magicbricks rows found for city — check city_name spelling in DB")

        # Show sample prices for reference
        sample = await conn.fetch(
            "SELECT scraped_data->>'city_name' as city, scraped_data->>'price' as price "
            "FROM property_raw_data WHERE source='magicbricks' LIMIT 5"
        )
        info("Sample magicbricks prices in DB:")
        for s in sample:
            print(f"    city={s['city']!r}  price=₹{int(float(s['price'])):,}")

        await conn.close()
    except Exception as e:
        fail(f"DB query test failed: {e}")


# ── TEST 4: Google Places Text Search (coord resolution) ─────────────────────
async def test_places_text_search():
    section("TEST 4 — Google Places Text Search  (locality → coordinates)")

    test_queries = [
        "Chirle, Navi Mumbai, Mumbai",
        "Jankalyan Nagar, Malad West, Mumbai",
        "Andheri West, Mumbai",
        "Mahalakshmi, Mumbai",
    ]

    async with httpx.AsyncClient() as client:
        for query in test_queries:
            try:
                resp = await client.post(
                    f"{PLACES_TEXT_URL}?key={GOOGLE_PLACES_API_KEY}",
                    json={"textQuery": query, "maxResultCount": 1},
                    headers={"Content-Type": "application/json", "X-Goog-FieldMask": "places.location,places.displayName"},
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    places = resp.json().get("places", [])
                    if places:
                        loc  = places[0].get("location", {})
                        name = places[0].get("displayName", {}).get("text", "?")
                        lat  = loc.get("latitude")
                        lng  = loc.get("longitude")
                        record(bool(lat and lng), f"Text Search {query!r} → {name!r} lat={lat:.4f} lng={lng:.4f}")
                    else:
                        fail(f"Text Search {query!r} → no results (check query or API key)")
                else:
                    body = resp.json()
                    fail(f"Text Search {query!r} → HTTP {resp.status_code}: {body.get('error', {}).get('message', '')}")
            except Exception as e:
                fail(f"Text Search {query!r} → exception: {e}")


# ── TEST 5: Google Places Nearby Search (DNA scoring) ────────────────────────
async def test_places_nearby():
    section(f"TEST 5 — Google Places Nearby  lat={TEST_LAT} lng={TEST_LNG}  radius=5km")

    categories = {
        "commute":     ["transit_station","subway_station","bus_station","train_station","light_rail_station"],
        "safety":      ["police","fire_station","hospital"],
        "education":   ["school","primary_school","secondary_school","university"],
        "greenery":    ["park","national_park","botanical_garden","playground"],
        "social_life": ["restaurant","bar","shopping_mall","movie_theater","cafe"],
    }

    async with httpx.AsyncClient() as client:
        for cat, types in categories.items():
            try:
                payload = {
                    "includedTypes": types,
                    "maxResultCount": 10,
                    "locationRestriction": {
                        "circle": {
                            "center": {"latitude": TEST_LAT, "longitude": TEST_LNG},
                            "radius": 5000.0,
                        }
                    },
                }
                resp = await client.post(
                    f"{PLACES_NEARBY_URL}?key={GOOGLE_PLACES_API_KEY}",
                    json=payload,
                    headers={"Content-Type": "application/json", "X-Goog-FieldMask": "places.id"},
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    count = len(resp.json().get("places", []))
                    record(True, f"Nearby [{cat:12}] → found {count} places")
                else:
                    body = resp.json()
                    err  = body.get("error", {})
                    fail(f"Nearby [{cat:12}] → HTTP {resp.status_code}: {err.get('message','')}")
            except Exception as e:
                fail(f"Nearby [{cat:12}] → exception: {e}")


# ── TEST 6: Full mini pipeline simulation ────────────────────────────────────
async def test_full_pipeline():
    section("TEST 6 — Full mini pipeline simulation")

    info("Simulating: salary=200000, city=Mumbai, family=2+")

    # Financial calc
    salary       = 200_000
    existing_emi = 0
    foir         = 0.40
    rate         = 0.085 / 12
    n            = 20 * 12
    multiplier   = (1 - (1 + rate) ** -n) / rate
    property_emi = salary * foir - existing_emi
    max_loan     = property_emi * multiplier
    max_price    = max_loan / 0.80
    range_min    = round(max_price * 0.80)
    range_max    = round(max_price * 1.20)

    ok(f"Financial calc → EMI=₹{property_emi:,.0f}  max_price=₹{max_price:,.0f}")
    ok(f"Search range   → ₹{range_min:,} – ₹{range_max:,}")

    try:
        conn = await asyncpg.connect(dsn=DATABASE_URL)

        # Fetch housing
        sql = f"""
            SELECT scraped_data->>'name' as name,
                   scraped_data->>'min_price' as price,
                   scraped_data->'coords' as coords,
                   scraped_data->'property_information'->>'bedrooms' as beds,
                   scraped_data->>'subtitle' as subtitle
            FROM property_raw_data
            WHERE source = 'housing'
              AND scraped_data->'polygons_hash'->'city'->>'name' ILIKE $1
              AND scraped_data->>'min_price' IS NOT NULL
              AND (scraped_data->>'min_price')::numeric BETWEEN {range_min} AND {range_max}
            LIMIT 5
        """
        rows = await conn.fetch(sql, f"%Mumbai%")
        record(len(rows) > 0, f"Housing DB query returned {len(rows)} properties")

        props_with_coords = 0
        for r in rows:
            coords = r['coords']
            if coords:
                props_with_coords += 1
            beds    = r['beds']
            subtitle = r['subtitle'] or ""
            bhk_matches = re.findall(r'(\d+)\s*BHK', subtitle, re.IGNORECASE)
            max_bhk = max(int(x) for x in bhk_matches) if bhk_matches else (int(beds) if beds else None)
            info(f"  {r['name']!r:40} price={int(float(r['price'])):,}  max_bhk={max_bhk}  coords={'✓' if coords else '✗'}")

        record(props_with_coords > 0, f"{props_with_coords}/{len(rows)} properties have stored coords")

        # Test Places Nearby on first property with coords
        for r in rows:
            if r['coords']:
                coords = json.loads(r['coords']) if isinstance(r['coords'], str) else r['coords']
                lat, lng = float(coords[0]), float(coords[1])
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        f"{PLACES_NEARBY_URL}?key={GOOGLE_PLACES_API_KEY}",
                        json={
                            "includedTypes": ["transit_station","subway_station","bus_station","train_station"],
                            "maxResultCount": 10,
                            "locationRestriction": {"circle": {"center": {"latitude": lat, "longitude": lng}, "radius": 5000.0}},
                        },
                        headers={"Content-Type": "application/json", "X-Goog-FieldMask": "places.id"},
                        timeout=8.0,
                    )
                    count = len(resp.json().get("places", [])) if resp.status_code == 200 else -1
                    record(count >= 0, f"DNA scoring on '{r['name']}' → commute places={count}")
                break

        await conn.close()
    except Exception as e:
        fail(f"Pipeline simulation failed: {e}")


# ── SUMMARY ──────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}{'='*60}")
    print("  DRISHTII — Full Connection & Query Test")
    print(f"{'='*60}{RESET}")

    if not DATABASE_URL:
        fail("DATABASE_URL not set in .env — cannot run DB tests")
        sys.exit(1)
    if not GOOGLE_PLACES_API_KEY:
        fail("GOOGLE_PLACES_API_KEY not set in .env — cannot run Places tests")
        sys.exit(1)

    await test_db()
    await test_db_queries()
    await test_places_text_search()
    await test_places_nearby()
    await test_full_pipeline()

    # Summary
    total = passed + failed
    print(f"\n{BOLD}{'='*60}")
    print(f"  RESULTS: {GREEN}{passed} passed{RESET}  {RED}{failed} failed{RESET}  (total {total})")
    print(f"{'='*60}{RESET}\n")

    if failed == 0:
        print(f"{GREEN}{BOLD}  All tests passed! Your pipeline is 100% connected.{RESET}\n")
    else:
        print(f"{YELLOW}{BOLD}  Fix the {failed} failing test(s) above before going to production.{RESET}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
import json
import psycopg2

CONN_STRING = "postgresql://neondb_owner:npg_6myMW8kbrpEC@ep-crimson-haze-ad1fbkf7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"

def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def push_data():
    conn = psycopg2.connect(CONN_STRING)
    cur = conn.cursor()

    files = [
        # ("99acres_buy.json",                  "99acres",     "buy"),
        # ("99acres_rent.json",                 "99acres",     "rent"),
        # ("magicbricks_buy.json",              "magicbricks", "buy"),
        # ("magicbricks_rent.json",             "magicbricks", "rent"),
        # ── new commercial files ──────────────────────────────────
        ("99acres_commercial_buy.json",       "99acres",     "commercial_buy"),
        ("99acres_commercial_rent.json",      "99acres",     "commercial_rent"),
        ("magicbricks_commercial_buy.json",   "magicbricks", "commercial_buy"),
        ("magicbricks_commercial_rent.json",  "magicbricks", "commercial_rent"),
    ]

    total = 0
    for filename, source, listing_type in files:
        data = load_json(filename)
        for item in data:
            cur.execute(
                "INSERT INTO property_raw_data (source, listing_type, scraped_data) VALUES (%s, %s, %s)",
                (source, listing_type, json.dumps(item))
            )
            total += 1
        print(f"✅ {filename} → {len(data)} rows")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n🎉 Total {total} rows pushed!")

push_data()
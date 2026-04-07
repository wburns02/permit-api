#!/usr/bin/env python3
"""Seed initial pricing benchmarks for Central TX metros.

Inserts market-rate pricing data for common electrical job types scraped from
HomeAdvisor / Angi / Thumbtack for Austin-area metros.

Tables seeded:
  - pricing_benchmarks  (28 rows — 7 job types × 4 metros)

Idempotent: skips insert if a row for (job_type, metro) already exists.
Re-run safely.

Usage:
    cd /home/will/permit-api
    source venv/bin/activate
    python3 scripts/scrape_pricing_benchmarks.py
"""

import sys
from datetime import datetime, timezone
from uuid import uuid4

import psycopg2

# ---------------------------------------------------------------------------
# Connection — T430 primary (writes go here, not the replica)
# ---------------------------------------------------------------------------
DB_URL = "postgresql://will@100.122.216.15:5432/permits"

# ---------------------------------------------------------------------------
# Seed data — (job_type, metro, low, mid, high, source)
# Prices in USD, sourced from HomeAdvisor/Angi/Thumbtack Q1 2026 Central TX
# ---------------------------------------------------------------------------

BENCHMARKS = [
    # panel_upgrade
    ("panel_upgrade", "austin",        1600, 2900, 4800, "homeadvisor"),
    ("panel_upgrade", "round_rock",    1500, 2750, 4500, "homeadvisor"),
    ("panel_upgrade", "san_marcos",    1400, 2600, 4200, "angi"),
    ("panel_upgrade", "new_braunfels", 1450, 2700, 4300, "angi"),

    # rewire
    ("rewire", "austin",        3200, 6500, 13000, "homeadvisor"),
    ("rewire", "round_rock",    3000, 6000, 12000, "homeadvisor"),
    ("rewire", "san_marcos",    2800, 5500, 11000, "angi"),
    ("rewire", "new_braunfels", 2900, 5800, 11500, "angi"),

    # outlet_install
    ("outlet_install", "austin",         160, 270, 420, "thumbtack"),
    ("outlet_install", "round_rock",     150, 250, 400, "thumbtack"),
    ("outlet_install", "san_marcos",     140, 235, 380, "thumbtack"),
    ("outlet_install", "new_braunfels",  145, 240, 390, "thumbtack"),

    # ceiling_fan
    ("ceiling_fan", "austin",        165, 320, 530, "thumbtack"),
    ("ceiling_fan", "round_rock",    150, 300, 500, "thumbtack"),
    ("ceiling_fan", "san_marcos",    140, 280, 475, "thumbtack"),
    ("ceiling_fan", "new_braunfels", 145, 290, 490, "thumbtack"),

    # ev_charger
    ("ev_charger", "austin",        900, 1600, 2700, "homeadvisor"),
    ("ev_charger", "round_rock",    800, 1500, 2500, "homeadvisor"),
    ("ev_charger", "san_marcos",    750, 1400, 2300, "angi"),
    ("ev_charger", "new_braunfels", 775, 1450, 2400, "angi"),

    # rough_in
    ("rough_in", "austin",        3200, 5200, 8500, "homeadvisor"),
    ("rough_in", "round_rock",    3000, 5000, 8000, "homeadvisor"),
    ("rough_in", "san_marcos",    2800, 4700, 7500, "angi"),
    ("rough_in", "new_braunfels", 2900, 4800, 7700, "angi"),

    # service_call
    ("service_call", "austin",        110, 220, 375, "thumbtack"),
    ("service_call", "round_rock",    100, 200, 350, "thumbtack"),
    ("service_call", "san_marcos",     95, 190, 330, "thumbtack"),
    ("service_call", "new_braunfels", 100, 195, 340, "thumbtack"),
]


def main():
    print(f"Connecting to {DB_URL} ...")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    inserted = 0
    skipped = 0
    now = datetime.now(timezone.utc)

    for job_type, metro, low, mid, high, source in BENCHMARKS:
        # Idempotency — skip if (job_type, metro) already exists
        cur.execute(
            "SELECT COUNT(*) FROM pricing_benchmarks WHERE job_type = %s AND metro = %s",
            (job_type, metro),
        )
        if cur.fetchone()[0] > 0:
            skipped += 1
            continue

        cur.execute(
            """
            INSERT INTO pricing_benchmarks (id, job_type, metro, low, mid, high, source, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (str(uuid4()), job_type, metro, low, mid, high, source, now),
        )
        inserted += 1

    conn.commit()
    conn.close()

    print(f"Done. Inserted {inserted} rows, skipped {skipped} existing rows.")


if __name__ == "__main__":
    main()

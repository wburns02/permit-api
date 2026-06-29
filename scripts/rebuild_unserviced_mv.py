#!/usr/bin/env python3
"""
Rebuild the unserviced_hail_leads MV on the T430 using the EXACT SQL from
app/main.py (single source of truth), then REFRESH it with data.

Extracts the CREATE MATERIALIZED VIEW ... block from main.py so the DB object
matches the deployed-code definition (which now includes the Bexar + Travis +
Harris arms). The running prod app self-heals on its next deploy via the staleness
sentinel, but this performs the load-time rebuild + refresh directly against
the T430.

Gentle on a recovering box: statement_timeout=0 only for the build/refresh
(the no-timeout MAINTENANCE path used for Bexar/Pearland), lock_timeout=30s so
a blocked write surfaces rather than hangs. NEVER pg_terminate / pg_cancel.
"""
import os
import psycopg2

# Default to THIS worktree's main.py so the rebuild matches the branch under test.
MAIN_PY = os.environ.get(
    "MAIN_PY",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "main.py"),
)
DSN = os.environ.get("UHL_DSN", "postgresql://will@100.122.216.15:5432/permits")


def extract_create_sql() -> str:
    src = open(MAIN_PY).read()
    start = src.index("CREATE MATERIALIZED VIEW IF NOT EXISTS unserviced_hail_leads AS")
    rest = src[start:]
    end = rest.index("WITH NO DATA") + len("WITH NO DATA")
    block = rest[:end]
    # main.py uses a Python raw string r"""...""" where regex backslashes are
    # written as \\s. Postgres wants a single backslash, so collapse \\ -> \.
    block = block.replace("\\\\", "\\")
    return block


def main():
    create_sql = extract_create_sql()
    print(f"extracted MV SQL: {len(create_sql)} chars", flush=True)
    assert "bexar_parcel_geometries" in create_sql, "Bexar arm missing!"
    assert "comal_parcel_geometries" in create_sql, "Comal arm missing!"
    assert "travis_parcel_geometries" in create_sql, "Travis arm missing!"
    assert "hcad_parcel_geometries" in create_sql, "Harris arm missing!"
    assert "nueces_permits" in create_sql, "Nueces serviced-exclusion missing!"

    conn = psycopg2.connect(DSN, connect_timeout=30)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")
    cur.execute("SET lock_timeout = '30s'")

    print("dropping old MV (CASCADE)...", flush=True)
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS unserviced_hail_leads CASCADE")

    print("creating MV (WITH NO DATA)...", flush=True)
    cur.execute(create_sql)

    print("creating indexes...", flush=True)
    for ddl in (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_unserviced_hail_leads_parcel "
        "ON unserviced_hail_leads (parcel_id, county_source)",
        "CREATE INDEX IF NOT EXISTS ix_uhl_county ON unserviced_hail_leads (county)",
        "CREATE INDEX IF NOT EXISTS ix_uhl_storm_date "
        "ON unserviced_hail_leads (matched_storm_date DESC)",
        "CREATE INDEX IF NOT EXISTS ix_uhl_score "
        "ON unserviced_hail_leads (lead_score DESC NULLS LAST)",
    ):
        cur.execute(ddl)

    print("REFRESH MATERIALIZED VIEW (populating)... this can take minutes", flush=True)
    cur.execute("REFRESH MATERIALIZED VIEW unserviced_hail_leads")

    cur.execute("SELECT county_source, count(*) FROM unserviced_hail_leads "
                "GROUP BY county_source ORDER BY county_source")
    print("=== MV row counts by county_source ===", flush=True)
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}", flush=True)
    cur.execute("SELECT count(*) FROM unserviced_hail_leads")
    print(f"  TOTAL: {cur.fetchone()[0]:,}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Load RRC Wellbore EWA report (OG_WELLBORE_EWA_Report.csv) into
canonical.wells.

59-column headerless CSV (layout: og_wellbore_ewadefinitionmanual2013-10-30,
extracted 2026-06-09). One row per completion; wellbores with multiple
completions repeat the 8-digit API (up to 25x), so we dedup to wellbore level
keeping the most recent completion. Upsert key: api14 = '42' + api8 + '0000'
(default-wellbore convention; RRC tracks at the 8-digit level).

EWA has no coordinates. Run update_wells_geom afterwards to borrow surface
coords from canonical.well_permits via api10.

Known dirt (verified): 21 lines that don't parse to 59 fields (SQL*Plus
trailer banner + 5 rows with unescaped quotes) -> skipped. '0' is a null
sentinel in several date/flag columns.
"""
import csv
import io
import time
from datetime import date

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
SRC = "/mnt/win11/Fedora/raw-public-data/rrc/wellbore/OG_WELLBORE_EWA_Report.csv"
SOURCE = "rrc_wellbore_ewa"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def d8(v):
    v = (v or "").strip()
    if not v or v == "0" or len(v) != 8 or not v.isdigit():
        return None
    try:
        dt = date(int(v[:4]), int(v[4:6]), int(v[6:8]))
    except ValueError:  # invalid calendar dates exist in historical data
        return None
    if dt.year < 1900 or dt.year > 2100:
        return None
    return dt.isoformat()


def nz(v):
    v = (v or "").strip()
    return v if v and v != "0" else None


WELL_TYPE = {"O": "oil", "G": "gas"}


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TEMP TABLE wb_stage (
            api8 TEXT, district TEXT, county TEXT, well_type TEXT,
            lease_name TEXT, lease_number TEXT, field_number TEXT,
            field_name TEXT, well_number TEXT, operator_name TEXT,
            operator_number TEXT, total_depth TEXT, status TEXT,
            completion_date TEXT, plug_date TEXT, ewa_id TEXT,
            wb_shut_in TEXT, multi_comp TEXT
        )
    """)

    buf, n, skipped = [], 0, 0
    t0 = time.time()

    def esc(v):
        return v.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ") if v else "\\N"

    def flush():
        if buf:
            cur.copy_expert("COPY wb_stage FROM STDIN", io.StringIO("".join(buf)))
            buf.clear()

    with open(SRC, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if len(row) != 59:
                skipped += 1
                continue
            api8 = row[2].strip()
            if not api8 or not api8.isdigit():
                skipped += 1
                continue
            vals = [
                api8, nz(row[0]), nz(row[3]), WELL_TYPE.get(row[4].strip()),
                nz(row[5]), nz(row[8]), nz(row[6]), nz(row[7]),
                nz(row[36]), nz(row[11]), nz(row[12]),
                nz(row[15]), nz(row[18]),
                d8(row[30]), d8(row[20]), nz(row[27]),
                nz(row[16]), nz(row[14]),
            ]
            buf.append("\t".join(esc(v) for v in vals) + "\n")
            n += 1
            if n % 200_000 == 0:
                flush()
                log(f"staged {n:,} ({n / (time.time() - t0):,.0f}/s)")
    flush()
    log(f"staged {n:,} completions ({skipped} skipped); deduping + upserting")

    cur.execute("""
        INSERT INTO canonical.wells
            (state, api14, api10, well_name, well_number, operator_name_raw,
             lease_name, lease_number, district, county, field_name,
             field_number, well_type, status, completion_date, plug_date,
             total_depth, source, lineage, freshness_at)
        SELECT DISTINCT ON (api8)
               'TX', '42' || api8 || '0000', '42' || api8,
               lease_name, well_number, operator_name,
               lease_name, lease_number, district, county, field_name,
               field_number, well_type, status,
               completion_date::date, plug_date::date, total_depth::numeric,
               %s,
               jsonb_strip_nulls(jsonb_build_object(
                   'ewa_id', ewa_id, 'wb_shut_in', wb_shut_in,
                   'multi_comp', multi_comp, 'operator_number', operator_number)),
               now()
        FROM wb_stage
        ORDER BY api8, completion_date DESC NULLS LAST
        ON CONFLICT (state, api14) WHERE api14 IS NOT NULL DO UPDATE SET
            well_number = EXCLUDED.well_number,
            operator_name_raw = EXCLUDED.operator_name_raw,
            lease_name = EXCLUDED.lease_name,
            lease_number = EXCLUDED.lease_number,
            district = EXCLUDED.district,
            county = EXCLUDED.county,
            field_name = EXCLUDED.field_name,
            field_number = EXCLUDED.field_number,
            well_type = COALESCE(EXCLUDED.well_type, canonical.wells.well_type),
            status = EXCLUDED.status,
            completion_date = COALESCE(EXCLUDED.completion_date, canonical.wells.completion_date),
            plug_date = COALESCE(EXCLUDED.plug_date, canonical.wells.plug_date),
            total_depth = COALESCE(EXCLUDED.total_depth, canonical.wells.total_depth),
            lineage = canonical.wells.lineage || EXCLUDED.lineage,
            freshness_at = now(),
            updated_at = now()
    """, (SOURCE,))
    log(f"upserted {cur.rowcount:,} wellbores into canonical.wells")
    conn.commit()

    cur.execute("""
        UPDATE canonical.wells w
        SET operator_id = o.id
        FROM canonical.operators o
        WHERE w.operator_id IS NULL
          AND w.lineage->>'operator_number' IS NOT NULL
          AND o.state = w.state
          AND o.operator_number = w.lineage->>'operator_number'
    """)
    log(f"operator_id resolved on {cur.rowcount:,} wells")
    conn.commit()

    # borrow surface coords from the most recent W-1 permit on the same API
    cur.execute("""
        UPDATE canonical.wells w
        SET lat = wp.lat, lng = wp.lng, geom = wp.geom
        FROM (
            SELECT DISTINCT ON (api10) api10, lat, lng, geom
            FROM canonical.well_permits
            WHERE api10 IS NOT NULL AND geom IS NOT NULL
            ORDER BY api10, approved_date DESC NULLS LAST
        ) wp
        WHERE w.geom IS NULL AND w.api10 = wp.api10
    """)
    log(f"geom borrowed from W-1 permits on {cur.rowcount:,} wells")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    main()

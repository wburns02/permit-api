#!/usr/bin/env python3
"""Load RRC UIC master file (uif700a.txt.gz) into the warehouse.

Layout: UIA010 manual (extracted 2026-06-09). 622-byte fixed-width records,
latin-1, record type cols 1-2, hierarchical: child records belong to the most
recent type-01 (permit root).

  01 -> canonical.disposal_wells (one per UIC permit; injection/disposal/
        storage discriminated by UIC-TYPE-INJ)
  04 -> rrc_raw.uic_h10_monthly (H-10 reported monthly injection volumes,
        keyed by parent uic_cntl_no)

Operator NAME is not in this file; operator_id resolves via P-5 number.
"""
import gzip
import io
import time
from datetime import date

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
SRC = "/mnt/win11/Fedora/raw-public-data/rrc/uic/uif700a.txt.gz"
SOURCE = "rrc_uic"

DISTRICT_ADP = {
    "01": "01", "02": "02", "03": "03", "04": "04", "05": "05", "06": "06",
    "07": "6E", "08": "7B", "09": "7C", "10": "08", "11": "8A", "12": "8B",
    "13": "09", "14": "10",
}

INJ_TYPE = {
    "1": "disposal (nonproductive zone)",
    "2": "disposal (productive zone)",
    "3": "secondary/tertiary recovery",
    "4": "miscellaneous",
    "5": "liquid storage (salt)",
    "6": "gas storage (reservoir)",
    "7": "gas storage (salt)",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fw(line, start, length):
    v = line[start - 1:start - 1 + length].strip()
    return v or None


def d8(v):
    if not v or v == "00000000" or len(v) != 8 or not v.isdigit():
        return None
    try:
        dt = date(int(v[:4]), int(v[4:6]), int(v[6:8]))
    except ValueError:
        return None
    if dt.year < 1900 or dt.year > 2100:
        return None
    return dt.isoformat()


def num(v):
    if v is None:
        return None
    try:
        return str(int(v))
    except ValueError:
        return None  # comment redefines in numeric fields


def esc(v):
    return v.replace("\\", "\\\\").replace("\t", " ") if v else "\\N"


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS rrc_raw")
    cur.execute("DROP TABLE IF EXISTS rrc_raw.uic_h10_monthly")
    cur.execute("""
        CREATE TABLE rrc_raw.uic_h10_monthly (
            uic_cntl_no TEXT,
            report_month DATE,
            avg_inj_psig INTEGER,
            max_inj_psig INTEGER,
            vol_bbl BIGINT,
            vol_mcf BIGINT
        )
    """)
    cur.execute(
        "DELETE FROM canonical.disposal_wells WHERE source = %s", (SOURCE,)
    )
    conn.commit()

    wells, h10, cur_ctl = [], [], None
    n01 = n04 = skipped = 0

    def flush_h10():
        if h10:
            cur.copy_expert("COPY rrc_raw.uic_h10_monthly FROM STDIN",
                            io.StringIO("".join(h10)))
            conn.commit()
            h10.clear()

    with gzip.open(SRC, "rt", encoding="latin-1") as f:
        for line in f:
            line = line.replace("\x00", "").rstrip("\r\n")
            rid = line[:2]
            if rid == "01":
                line = line.ljust(622)
                ctl = fw(line, 3, 9)
                if not ctl or set(ctl) == {"0"}:
                    cur_ctl = None
                    continue
                cur_ctl = ctl
                cnty = fw(line, 33, 3)
                api5 = fw(line, 36, 5)
                api10 = (f"42{cnty}{api5}"
                         if cnty and api5 and api5 != "00000" else None)
                cancel = d8(fw(line, 91, 8))
                w3 = d8(fw(line, 107, 8))
                active = fw(line, 90, 1)
                status = ("cancelled" if cancel else
                          "plugged" if w3 else
                          "active" if active == "Y" else "inactive")
                top, bot = num(fw(line, 194, 5)), num(fw(line, 199, 5))
                commercial = fw(line, 253, 1)
                wells.append("\t".join(esc(v) for v in [
                    cur_ctl, api10,
                    num(fw(line, 214, 5)) or num(fw(line, 209, 5)),  # W-14 else H-1
                    fw(line, 27, 6),
                    DISTRICT_ADP.get(fw(line, 19, 2) or ""),
                    cnty,
                    INJ_TYPE.get(fw(line, 115, 1) or ""),
                    status,
                    f"{top}-{bot}" if top and bot else None,
                    num(fw(line, 204, 5)),
                    num(fw(line, 176, 9)),
                    d8(fw(line, 50, 8)),
                    "true" if commercial == "Y" else "false",
                ]) + "\n")
                n01 += 1
            elif rid == "04" and cur_ctl:
                line = line.ljust(60)
                y, m = fw(line, 3, 4), fw(line, 7, 2)
                if not (y and m and y.isdigit() and m.isdigit()
                        and 1 <= int(m) <= 12 and 1900 < int(y) < 2100):
                    skipped += 1
                    continue
                h10.append("\t".join(esc(v) for v in [
                    cur_ctl, f"{y}-{int(m):02d}-01",
                    num(fw(line, 9, 4)), num(fw(line, 13, 4)),
                    num(fw(line, 17, 8)), num(fw(line, 25, 8)),
                ]) + "\n")
                n04 += 1
                if n04 % 500_000 == 0:
                    flush_h10()
                    log(f"h10 monthly: {n04:,} loaded")

    flush_h10()
    log(f"parsed {n01:,} permits, {n04:,} H-10 months ({skipped} skipped)")
    cur.execute("""
        CREATE TEMP TABLE uic_stage (
            uic_number TEXT, api10 TEXT, permit_number TEXT,
            operator_number TEXT, district TEXT, county_code TEXT,
            well_kind TEXT, status TEXT, depth_interval TEXT,
            max_psig TEXT, max_bpd TEXT, approved_date TEXT, commercial TEXT
        )
    """)
    cur.copy_expert("COPY uic_stage FROM STDIN", io.StringIO("".join(wells)))
    log("uic_stage copied; upserting disposal_wells")

    cur.execute("""
        INSERT INTO canonical.disposal_wells
            (state, uic_number, permit_number, api10, operator_id, district,
             county, well_kind, status, depth_interval,
             max_injection_pressure, max_injection_bpd, source, lineage,
             freshness_at)
        SELECT DISTINCT ON (s.uic_number)
               'TX', s.uic_number, s.permit_number, s.api10, o.id, s.district,
               g.county_name, s.well_kind, s.status, s.depth_interval,
               s.max_psig::numeric, s.max_bpd::numeric, %s,
               jsonb_strip_nulls(jsonb_build_object(
                   'operator_number', s.operator_number,
                   'approved_date', s.approved_date,
                   'commercial', s.commercial)),
               now()
        FROM uic_stage s
        LEFT JOIN rrc_raw.gp_county g ON g.county_no = s.county_code
        LEFT JOIN canonical.operators o
               ON o.state = 'TX' AND o.operator_number = s.operator_number
        ORDER BY s.uic_number
        ON CONFLICT (state, uic_number) WHERE uic_number IS NOT NULL
        DO UPDATE SET
            status = EXCLUDED.status,
            operator_id = COALESCE(EXCLUDED.operator_id, canonical.disposal_wells.operator_id),
            max_injection_pressure = EXCLUDED.max_injection_pressure,
            max_injection_bpd = EXCLUDED.max_injection_bpd,
            lineage = canonical.disposal_wells.lineage || EXCLUDED.lineage,
            freshness_at = now()
    """, (SOURCE,))
    log(f"upserted {cur.rowcount:,} disposal/injection wells")
    conn.commit()

    cur.execute("CREATE INDEX IF NOT EXISTS ix_uic_h10_ctl ON rrc_raw.uic_h10_monthly (uic_cntl_no, report_month)")
    # borrow coords from canonical.wells via api10
    cur.execute("""
        UPDATE canonical.disposal_wells d
        SET lat = w.lat, lng = w.lng, geom = w.geom
        FROM canonical.wells w
        WHERE d.geom IS NULL AND d.api10 IS NOT NULL
          AND w.state = 'TX' AND w.api10 = d.api10 AND w.geom IS NOT NULL
    """)
    log(f"geom borrowed on {cur.rowcount:,} disposal wells")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    main()

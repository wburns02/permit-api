#!/usr/bin/env python3
"""Load RRC drilling permit master/trailer files (daf802 full master,
daf420 daily) into canonical.well_permits.

Layout: OGA049 manual, field positions extracted 2026-06-09 (see the spec in
the conversation/blueprint notes). Record types used:
  01 DAROOT   application status (operator-reported, has app-status flag)
  02 DAPERMIT permit master (RRC-approved values; authoritative)
  14/15       lat-long surface/bottomhole (daily file only, not in manual)
All other types (fields, restrictions, BHL detail) skipped for v1.

Identity: canonical.well_permits (state='TX', permit_number) where
permit_number = DA-STATUS-NUMBER (== RRC permit number for status >= 320000,
i.e. post-Sept-1986; for older permits the assigned permit number is kept in
lineage.rrc_permit_number).

Upsert via temp table + ON CONFLICT, so daf420 daily refresh is the same code
path as the daf802 backfill:
  load_rrc_w1.py /path/daf802.txt --source rrc_daf802
  load_rrc_w1.py /path/daf420.dat --source rrc_daf420
"""
import argparse
import io
import time
from datetime import date

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"

VALID_IDS = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
             "11", "12", "13", "14", "15"}

DISTRICT_ADP = {
    "01": "01", "02": "02", "03": "03", "04": "04", "05": "05", "06": "06",
    "07": "6E", "08": "7B", "09": "7C", "10": "08", "11": "8A", "12": "8B",
    "13": "09", "14": "10",
}

FILING_PURPOSE = {
    "01": "new drill", "02": "deepen below casing", "03": "deepen within casing",
    "04": "plug back", "05": "other", "06": "amended drill", "07": "re-enter",
    "08": "sidetrack", "09": "field transfer", "10": "drill (pre-1977)",
    "11": "directional/sidetrack drill", "12": "horizontal drill",
    "13": "horizontal sidetrack", "14": "recompletion", "15": "reclass",
}

APP_STATUS = {
    "P": "pending", "A": "approved", "W": "withdrawn", "D": "dismissed",
    "E": "denied", "C": "closed", "O": "other", "X": "deleted",
    "Z": "cancelled",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fw(line, start, length):
    v = line[start - 1:start - 1 + length].strip()
    return v or None


def d8(v):
    if not v or len(v) != 8 or not v.isdigit() or v == "00000000":
        return None
    try:
        dt = date(int(v[:4]), int(v[4:6]), int(v[6:8]))
    except ValueError:  # e.g. 19780229 exists in the historical master
        return None
    if dt.year < 1900 or dt.year > 2100:
        return None
    return dt.isoformat()


def num(v, div=1):
    if not v:
        return None
    try:
        return str(int(v) / div) if div != 1 else str(int(v))
    except ValueError:
        return None


def latlng(line):
    """'14: -102.2123389  32.4347917' -> (lng, lat) or None."""
    parts = line[3:].split()
    if len(parts) != 2:
        return None
    try:
        lng, lat = float(parts[0]), float(parts[1])
    except ValueError:
        return None
    if not (-107 < lng < -93 and 25 < lat < 37):  # TX bounds sanity
        return None
    return lng, lat


class Permit:
    __slots__ = ("status_no", "rec01", "rec02", "surface")

    def __init__(self, status_no):
        self.status_no = status_no
        self.rec01 = None
        self.rec02 = None
        self.surface = None


def permits_from_file(path):
    """Yield Permit groups. Records for one permit are contiguous: 01 opens a
    group; 02/14 attach to the open group."""
    cur = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.replace("\x00", "").rstrip("\r\n")
            rid = line[:2]
            if rid not in VALID_IDS:
                continue  # NEL-corrupt fragments
            line = line.ljust(510)
            if rid == "01":
                if cur:
                    yield cur
                cur = Permit(fw(line, 3, 7))
                cur.rec01 = line
            elif cur is None:
                continue
            elif rid == "02":
                cur.rec02 = line
            elif rid == "14" and cur.surface is None:
                cur.surface = latlng(line)
        if cur:
            yield cur


def to_row(p, counties, source, source_file):
    r1, r2 = p.rec01, p.rec02
    if not p.status_no:
        return None
    seq = fw(r1, 10, 2)
    status_flag = fw(r1, 101, 1)
    row = {
        "seq": seq,
        "permit_number": p.status_no,
        "operator_name_raw": fw(r1, 67, 32),
        "submitted_date": d8(fw(r1, 59, 8)),
        "current_status": APP_STATUS.get(status_flag or "", status_flag),
        "status_date": None,
        "amended": "true" if seq and seq != "99" else "false",
        "api10": None, "operator_number": None, "lease_name": fw(r1, 15, 46 - 15 + 1),
        "well_number": fw(r1, 157, 6),
        "district": DISTRICT_ADP.get(fw(r1, 47, 2) or ""),
        "county": counties.get(fw(r1, 12, 3) or ""),
        "filing_purpose": None, "wellbore_profile": None,
        "total_depth": None, "approved_date": None, "spud_date": None,
        "lat": None, "lng": None,
        "rrc_permit_number": fw(r1, 113, 7),
    }
    if r2:
        api8 = fw(r2, 503, 8)
        horizontal = fw(r2, 494, 1) == "Y"
        directional = fw(r2, 482, 1) == "Y"
        sidetrack = fw(r2, 483, 1) == "Y"
        profile = ("horizontal" if horizontal
                   else "directional" if directional
                   else "sidetrack" if sidetrack else "vertical")
        row.update({
            "rrc_permit_number": fw(r2, 3, 7) or row["rrc_permit_number"],
            "county": counties.get(fw(r2, 12, 3) or "") or row["county"],
            "lease_name": fw(r2, 15, 32) or row["lease_name"],
            "district": DISTRICT_ADP.get(fw(r2, 47, 2) or "") or row["district"],
            "well_number": fw(r2, 49, 6) or row["well_number"],
            "total_depth": num(fw(r2, 55, 5)),
            "operator_number": fw(r2, 60, 6),
            "filing_purpose": FILING_PURPOSE.get(fw(r2, 66, 2) or ""),
            "wellbore_profile": profile,
            "submitted_date": d8(fw(r2, 122, 8)) or row["submitted_date"],
            "approved_date": d8(fw(r2, 130, 8)),
            "spud_date": d8(fw(r2, 154, 8)),
            "status_date": d8(fw(r2, 171, 8)),
            "api10": f"42{api8}" if api8 and api8.isdigit() and int(api8) > 0 else None,
        })
    if p.surface:
        row["lng"], row["lat"] = str(p.surface[0]), str(p.surface[1])
    row["source"] = source
    row["source_file"] = source_file
    return row


COLS = ["seq", "permit_number", "api10", "operator_number", "operator_name_raw",
        "lease_name", "well_number", "district", "county", "wellbore_profile",
        "filing_purpose", "amended", "total_depth", "current_status",
        "status_date", "submitted_date", "approved_date", "spud_date",
        "lat", "lng", "source", "source_file", "rrc_permit_number"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--source", required=True)
    args = ap.parse_args()
    source_file = args.path.rsplit("/", 1)[-1]

    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SELECT county_no, county_name FROM rrc_raw.gp_county")
    counties = dict(cur.fetchall())

    cur.execute(f"""
        CREATE TEMP TABLE w1_stage (
            {', '.join(c + ' TEXT' for c in COLS)}
        ) ON COMMIT PRESERVE ROWS
    """)

    buf, n, skipped = [], 0, 0
    t0 = time.time()

    def esc(v):
        return v.replace("\\", "\\\\").replace("\t", " ") if v else "\\N"

    def flush():
        if buf:
            cur.copy_expert(
                f"COPY w1_stage ({', '.join(COLS)}) FROM STDIN",
                io.StringIO("".join(buf)),
            )
            buf.clear()

    for p in permits_from_file(args.path):
        row = to_row(p, counties, args.source, source_file)
        if row is None:
            skipped += 1
            continue
        buf.append("\t".join(esc(row.get(c)) for c in COLS) + "\n")
        n += 1
        if n % 100_000 == 0:
            flush()
            log(f"staged {n:,} ({n / (time.time() - t0):,.0f}/s)")
    flush()
    log(f"staged total {n:,} permits ({skipped} skipped); upserting")

    cur.execute("""
        INSERT INTO canonical.well_permits
            (state, permit_number, api10, operator_number, operator_name_raw,
             lease_name, well_number, district, county, wellbore_profile,
             filing_purpose, amended, total_depth, current_status, status_date,
             submitted_date, approved_date, spud_date, lat, lng, geom,
             source, source_file, lineage, freshness_at)
        SELECT DISTINCT ON (permit_number)
               'TX', permit_number, api10, operator_number, operator_name_raw,
               lease_name, well_number, district, county, wellbore_profile,
               filing_purpose, amended::boolean, total_depth::numeric,
               current_status, status_date::date, submitted_date::date,
               approved_date::date, spud_date::date,
               lat::double precision, lng::double precision,
               CASE WHEN lat IS NOT NULL THEN
                   ST_SetSRID(ST_MakePoint(lng::float, lat::float), 4326)
               END,
               source, source_file,
               jsonb_strip_nulls(jsonb_build_object(
                   'rrc_permit_number', rrc_permit_number)),
               now()
        FROM w1_stage
        -- amendments share the status number; sequence 99=original, 98=1st
        -- amendment, 97=2nd... so ascending seq puts the newest first
        ORDER BY permit_number, seq ASC NULLS LAST, ctid DESC
        ON CONFLICT (state, permit_number) DO UPDATE SET
            api10 = COALESCE(EXCLUDED.api10, canonical.well_permits.api10),
            operator_number = COALESCE(EXCLUDED.operator_number, canonical.well_permits.operator_number),
            operator_name_raw = COALESCE(EXCLUDED.operator_name_raw, canonical.well_permits.operator_name_raw),
            lease_name = COALESCE(EXCLUDED.lease_name, canonical.well_permits.lease_name),
            well_number = COALESCE(EXCLUDED.well_number, canonical.well_permits.well_number),
            district = COALESCE(EXCLUDED.district, canonical.well_permits.district),
            county = COALESCE(EXCLUDED.county, canonical.well_permits.county),
            wellbore_profile = COALESCE(EXCLUDED.wellbore_profile, canonical.well_permits.wellbore_profile),
            filing_purpose = COALESCE(EXCLUDED.filing_purpose, canonical.well_permits.filing_purpose),
            amended = EXCLUDED.amended,
            total_depth = COALESCE(EXCLUDED.total_depth, canonical.well_permits.total_depth),
            current_status = COALESCE(EXCLUDED.current_status, canonical.well_permits.current_status),
            status_date = COALESCE(EXCLUDED.status_date, canonical.well_permits.status_date),
            submitted_date = COALESCE(EXCLUDED.submitted_date, canonical.well_permits.submitted_date),
            approved_date = COALESCE(EXCLUDED.approved_date, canonical.well_permits.approved_date),
            spud_date = COALESCE(EXCLUDED.spud_date, canonical.well_permits.spud_date),
            lat = COALESCE(EXCLUDED.lat, canonical.well_permits.lat),
            lng = COALESCE(EXCLUDED.lng, canonical.well_permits.lng),
            geom = COALESCE(EXCLUDED.geom, canonical.well_permits.geom),
            source = EXCLUDED.source,
            source_file = EXCLUDED.source_file,
            lineage = canonical.well_permits.lineage || EXCLUDED.lineage,
            freshness_at = now(),
            updated_at = now()
    """)
    log(f"upserted {cur.rowcount:,} into canonical.well_permits")
    conn.commit()

    cur.execute("""
        UPDATE canonical.well_permits wp
        SET operator_id = o.id
        FROM canonical.operators o
        WHERE wp.operator_id IS NULL
          AND wp.operator_number IS NOT NULL
          AND o.state = wp.state AND o.operator_number = wp.operator_number
    """)
    log(f"operator_id resolved on {cur.rowcount:,} rows")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    main()

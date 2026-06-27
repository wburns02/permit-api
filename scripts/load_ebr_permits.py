#!/usr/bin/env python3
"""Load East Baton Rouge Parish, LA building permits (Socrata).

Source (free Socrata):
    https://data.brla.gov/resource/7fq7-8j7r.json  (~141,635 permits)
    Fields: permittype, designation, projectdescription, issueddate,
    streetaddress/zip, contractorname, projectvalue.

The roof signal lives in `permittype`: `Re-Roof (R)` (1,210) and
`Re-Roof (C)` (110). These flag a property as SERVICED — a roofer already
re-roofed it — and drive the un-serviced exclusion in the EBR storm-lead arm
of `unserviced_hail_leads`.

Target: a DEDICATED, INDEXED `ebr_permits` table — NOT the 99.6M-row shared
`permits_la` (which has no index on `source` and already carries this dataset
under source='socrata_7fq7-8j7r' but cannot be filtered without a full scan).
A small county-scoped table keeps the serviced-exclusion join in the MV fast
and box-gentle. We index normalized address so the MV's address match is keyed.

Box-gentle: chunked upserts, lock_timeout, idempotent ON CONFLICT. Raw pages
staged to /mnt/win11/Fedora/free_data/ebr/.

Usage:
    python3 scripts/load_ebr_permits.py [--limit-pages N]

DSN resolution: --dsn, $PERMITS_DSN, ~/.config/permitlookup/permits_dsn.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch

SOCRATA_URL = "https://data.brla.gov/resource/7fq7-8j7r.json"
PAGE_SIZE = 5000
STAGING_DIR = Path("/mnt/win11/Fedora/free_data/ebr")

# Re-Roof permittypes — the serviced signal.
REROOF_TYPES = {"Re-Roof (R)", "Re-Roof (C)"}

DDL = """
CREATE TABLE IF NOT EXISTS ebr_permits (
    permit_id        TEXT PRIMARY KEY,
    permit_number    TEXT,
    permit_type      TEXT,
    designation      TEXT,
    project_desc     TEXT,
    street_address   TEXT,
    address_norm     TEXT,
    zip              TEXT,
    city             TEXT,
    parish           TEXT,
    project_value    NUMERIC,
    contractor_name  TEXT,
    applicant_name   TEXT,
    subdivision      TEXT,
    creation_date    TIMESTAMPTZ,
    issued_date      TIMESTAMPTZ,
    is_reroof        BOOLEAN NOT NULL DEFAULT FALSE,
    raw              JSONB,
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

INDEXES = [
    # Normalized-address lookup, restricted to the rows the MV cares about
    # (re-roof, the serviced signal) so the partial index stays tiny.
    "CREATE INDEX IF NOT EXISTS ix_ebr_permits_reroof_addrnorm "
    "ON ebr_permits (address_norm) WHERE is_reroof",
    "CREATE INDEX IF NOT EXISTS ix_ebr_permits_issued "
    "ON ebr_permits (issued_date DESC)",
    "CREATE INDEX IF NOT EXISTS ix_ebr_permits_type "
    "ON ebr_permits (permit_type)",
]

UPSERT = """
INSERT INTO ebr_permits
    (permit_id, permit_number, permit_type, designation, project_desc,
     street_address, address_norm, zip, city, parish, project_value,
     contractor_name, applicant_name, subdivision, creation_date,
     issued_date, is_reroof, raw, loaded_at)
VALUES
    (%(permit_id)s, %(permit_number)s, %(permit_type)s, %(designation)s,
     %(project_desc)s, %(street_address)s,
     -- normalize: UPPER, strip unit designators, strip punctuation, collapse
     -- whitespace. Mirrors the MV / search_service normalize_address().
     NULLIF(TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(COALESCE(%(street_address)s, '')),
            '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
          '[.,#]', '', 'g'),
        '\\s+', ' ')), ''),
     %(zip)s, %(city)s, %(parish)s, %(project_value)s, %(contractor_name)s,
     %(applicant_name)s, %(subdivision)s, %(creation_date)s, %(issued_date)s,
     %(is_reroof)s, %(raw)s, NOW())
ON CONFLICT (permit_id) DO UPDATE SET
    permit_type     = EXCLUDED.permit_type,
    designation     = EXCLUDED.designation,
    project_desc    = EXCLUDED.project_desc,
    street_address  = EXCLUDED.street_address,
    address_norm    = EXCLUDED.address_norm,
    zip             = EXCLUDED.zip,
    city            = EXCLUDED.city,
    parish          = EXCLUDED.parish,
    project_value   = EXCLUDED.project_value,
    contractor_name = EXCLUDED.contractor_name,
    applicant_name  = EXCLUDED.applicant_name,
    subdivision     = EXCLUDED.subdivision,
    creation_date   = EXCLUDED.creation_date,
    issued_date     = EXCLUDED.issued_date,
    is_reroof       = EXCLUDED.is_reroof,
    raw             = EXCLUDED.raw,
    loaded_at       = NOW();
"""


def resolve_dsn(cli_dsn: str | None) -> str:
    if cli_dsn:
        return cli_dsn
    env = os.environ.get("PERMITS_DSN")
    if env:
        return env
    path = Path.home() / ".config" / "permitlookup" / "permits_dsn"
    if path.exists():
        return path.read_text().strip()
    sys.exit("No DSN: pass --dsn or set $PERMITS_DSN.")


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return None if f != f else f
    except (ValueError, TypeError):
        return None


def _ts(v):
    """Socrata floating-timestamps come ISO 8601; pass through, psycopg casts."""
    s = _clean(v)
    return s


def fetch_page(client: httpx.Client, offset: int) -> list[dict]:
    params = {"$limit": str(PAGE_SIZE), "$offset": str(offset), "$order": "permitid"}
    r = client.get(SOCRATA_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn")
    ap.add_argument("--limit-pages", type=int, default=None)
    ap.add_argument("--no-stage", action="store_true")
    args = ap.parse_args()

    dsn = resolve_dsn(args.dsn)
    stage = not args.no_stage
    if stage:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

    started = time.time()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    total_rows = reroof_rows = 0

    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '15s'")
        cur.execute("SET statement_timeout = '60s'")
        cur.execute(DDL)
        for ddl in INDEXES:
            cur.execute(ddl)
    conn.commit()

    with httpx.Client(follow_redirects=True) as client:
        offset = 0
        page_no = 0
        while True:
            if args.limit_pages is not None and page_no >= args.limit_pages:
                break
            try:
                recs = fetch_page(client, offset)
            except Exception as exc:  # noqa: BLE001
                print(f"page offset={offset} fetch failed: {exc}", flush=True)
                time.sleep(3)
                continue
            if not recs:
                break

            if stage:
                (STAGING_DIR / f"permits_{offset:07d}.json").write_text(
                    json.dumps(recs)
                )

            rows = []
            for rec in recs:
                pid = _clean(rec.get("permitid")) or _clean(rec.get("permitnumber"))
                if not pid:
                    continue
                ptype = _clean(rec.get("permittype"))
                is_reroof = ptype in REROOF_TYPES
                if is_reroof:
                    reroof_rows += 1
                rows.append({
                    "permit_id": pid,
                    "permit_number": _clean(rec.get("permitnumber")),
                    "permit_type": ptype,
                    "designation": _clean(rec.get("designation")),
                    "project_desc": _clean(rec.get("projectdescription")),
                    "street_address": _clean(rec.get("streetaddress"))
                                      or _clean(rec.get("address")),
                    "zip": _clean(rec.get("zip")),
                    "city": _clean(rec.get("city1")),
                    "parish": _clean(rec.get("parishname")),
                    "project_value": _num(rec.get("projectvalue")),
                    "contractor_name": _clean(rec.get("contractorname")),
                    "applicant_name": _clean(rec.get("applicantname")),
                    "subdivision": _clean(rec.get("subdivision")),
                    "creation_date": _ts(rec.get("creationdate")),
                    "issued_date": _ts(rec.get("issueddate")),
                    "is_reroof": is_reroof,
                    "raw": json.dumps(rec),
                })

            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '15s'")
                cur.execute("SET statement_timeout = '60s'")
                execute_batch(cur, UPSERT, rows, page_size=500)
            conn.commit()

            total_rows += len(rows)
            page_no += 1
            offset += len(recs)
            if page_no % 5 == 0 or len(recs) < PAGE_SIZE:
                print(f"  page {page_no}: offset={offset} "
                      f"total={total_rows} reroof={reroof_rows}", flush=True)
            if len(recs) < PAGE_SIZE:
                break

    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '15s'")
        try:
            cur.execute("ANALYZE ebr_permits")
        except Exception as exc:  # noqa: BLE001
            print(f"ANALYZE skipped: {exc}", flush=True)
    conn.commit()
    conn.close()

    dur = time.time() - started
    print(f"DONE: total={total_rows} reroof={reroof_rows} in {dur:.0f}s",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

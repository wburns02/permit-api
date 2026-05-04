#!/usr/bin/env python3
"""
Multi-jurisdiction MGO (MyGovernmentOnline) Connect loader for the
Baton Rouge metro 14 jurisdictions. Loads into the canonical `hot_leads`
table with state='LA' (NOT the TX default).

Design:
  - Primary path: read from the daily MGOConnect scraper SQLite snapshot at
    /mnt/win11/fedora-moved/Data/crm_permits.db (same source as
    load_mgo_septic_permits.py for TX). Filter by jurisdiction_id.
  - Optional live path (--live): hit the MGO HTTP search API with a
    session cookie supplied via the MGO_PHPSESSID env var. Anonymous
    calls return []; without a cookie this path is a no-op and exits
    with a clear message.

Output table: public.hot_leads, source='mgo_<slug>', state='LA',
jurisdiction='<value>'. Dedup is enforced by the partial unique index
idx_hot_leads_dedup (permit_number, address, state)
  WHERE permit_number IS NOT NULL AND address IS NOT NULL.

Banner note: MyGovernmentOnline.org has announced the customer portal is
being retired soon. Endpoints currently still work but plan to migrate
once a replacement is published.

Do NOT modify load_mgo_septic_permits.py: this file is a separate loader
that targets hot_leads (LA), not mgo_septic_permits (TX).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #

PG_HOST = "100.122.216.15"
PG_PORT = 5432
PG_DB = "permits"
PG_USER = "will"

MGO_SQLITE = "/mnt/win11/fedora-moved/Data/crm_permits.db"

# 14 BR-metro jurisdiction IDs to scrape.
# (juris_id, source_slug, jurisdiction_value, name)
BR_METRO = [
    (26,  "mgo_ascension",          "Ascension",            "Ascension Parish"),
    (35,  "mgo_west_baton_rouge",   "West Baton Rouge",     "West Baton Rouge Parish"),
    (460, "mgo_iberville",          "Iberville",            "Iberville Parish"),
    (189, "mgo_livingston_parish",  "Livingston Parish",    "Livingston Parish"),
    (121, "mgo_denham_springs",     "Denham Springs",       "Denham Springs"),
    (392, "mgo_town_of_livingston", "Town of Livingston",   "Town of Livingston"),
    (456, "mgo_walker",             "Walker",               "Walker"),
    (143, "mgo_gonzales",           "Gonzales",             "Gonzales"),
    (528, "mgo_donaldsonville",     "Donaldsonville",       "Donaldsonville"),
    (547, "mgo_sorrento",           "Sorrento",             "Sorrento"),
    (214, "mgo_baker",              "Baker",                "Baker"),
    (532, "mgo_city_of_plaquemine", "Plaquemine",           "City of Plaquemine"),
    (160, "mgo_st_gabriel",         "St. Gabriel",          "St. Gabriel"),
    # East Baton Rouge: included for parity. Deprecate after Socrata loader is live.
    (27,  "mgo_east_baton_rouge",   "East Baton Rouge",     "East Baton Rouge"),
]
BR_JURIS_BY_ID = {row[0]: row for row in BR_METRO}
ALL_JURIS_IDS = [row[0] for row in BR_METRO]

# Defensive deny-list for any test/internal jurisdiction names.
DENY_JURIS_NAMES = {"Test", "Demo", "Sandbox", "Internal"}

INSERT_SQL = """
INSERT INTO hot_leads (
    permit_number, permit_type, work_class, description,
    address, city, state, zip, county,
    lat, lng, issue_date, applied_date, status,
    contractor_company, applicant_name, applicant_org,
    owner_name, jurisdiction, source
) VALUES %s
ON CONFLICT (permit_number, address, state)
WHERE permit_number IS NOT NULL AND address IS NOT NULL
DO UPDATE SET
    permit_type        = COALESCE(EXCLUDED.permit_type,        hot_leads.permit_type),
    work_class         = COALESCE(EXCLUDED.work_class,         hot_leads.work_class),
    description        = COALESCE(EXCLUDED.description,        hot_leads.description),
    city               = COALESCE(EXCLUDED.city,               hot_leads.city),
    zip                = COALESCE(EXCLUDED.zip,                hot_leads.zip),
    county             = COALESCE(EXCLUDED.county,             hot_leads.county),
    lat                = COALESCE(EXCLUDED.lat,                hot_leads.lat),
    lng                = COALESCE(EXCLUDED.lng,                hot_leads.lng),
    issue_date         = COALESCE(EXCLUDED.issue_date,         hot_leads.issue_date),
    applied_date       = COALESCE(EXCLUDED.applied_date,       hot_leads.applied_date),
    status             = COALESCE(EXCLUDED.status,             hot_leads.status),
    contractor_company = COALESCE(EXCLUDED.contractor_company, hot_leads.contractor_company),
    applicant_name     = COALESCE(EXCLUDED.applicant_name,     hot_leads.applicant_name),
    applicant_org      = COALESCE(EXCLUDED.applicant_org,      hot_leads.applicant_org),
    owner_name         = COALESCE(EXCLUDED.owner_name,         hot_leads.owner_name),
    jurisdiction       = COALESCE(EXCLUDED.jurisdiction,       hot_leads.jurisdiction)
RETURNING (xmax = 0) AS inserted
"""

INSERT_COLS = [
    "permit_number", "permit_type", "work_class", "description",
    "address", "city", "state", "zip", "county",
    "lat", "lng", "issue_date", "applied_date", "status",
    "contractor_company", "applicant_name", "applicant_org",
    "owner_name", "jurisdiction", "source",
]


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #

def parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def trim(s, maxlen=None):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if maxlen and len(s) > maxlen:
        s = s[:maxlen]
    return s


def parish_county_for(jurisdiction_value: str) -> str | None:
    """Map a jurisdiction value to its containing parish (LA equivalent of county)."""
    if not jurisdiction_value:
        return None
    v = jurisdiction_value.strip()
    # Direct parish-level entries
    parish_map = {
        "Ascension":           "Ascension Parish",
        "West Baton Rouge":    "West Baton Rouge Parish",
        "Iberville":           "Iberville Parish",
        "Livingston Parish":   "Livingston Parish",
        "East Baton Rouge":    "East Baton Rouge Parish",
        # Cities -> parish
        "Denham Springs":      "Livingston Parish",
        "Town of Livingston":  "Livingston Parish",
        "Walker":              "Livingston Parish",
        "Gonzales":            "Ascension Parish",
        "Donaldsonville":      "Ascension Parish",
        "Sorrento":            "Ascension Parish",
        "Baker":               "East Baton Rouge Parish",
        "Plaquemine":          "Iberville Parish",
        "St. Gabriel":         "Iberville Parish",
    }
    return parish_map.get(v)


# --------------------------------------------------------------------------- #
# Source: SQLite snapshot from the MGOConnect daily scraper                    #
# --------------------------------------------------------------------------- #

def fetch_from_sqlite(juris_id: int, since: date, limit: int) -> list[dict]:
    """Pull rows for one juris_id from the MGOConnect SQLite snapshot."""
    if not os.path.exists(MGO_SQLITE):
        raise SystemExit(
            f"MGO snapshot not found at {MGO_SQLITE}. "
            "Run the MGOConnect scraper or update MGO_SQLITE path."
        )
    src = sqlite3.connect(f"file:{MGO_SQLITE}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    q = """
        SELECT * FROM permits
        WHERE state='LA'
          AND jurisdiction_id = ?
          AND (created_date IS NULL OR substr(created_date, 1, 10) >= ?)
        ORDER BY created_date DESC
        LIMIT ?
    """
    cur = src.execute(q, (juris_id, since.isoformat(), limit))
    rows = [dict(r) for r in cur]
    src.close()
    return rows


# --------------------------------------------------------------------------- #
# Source: live MGO HTTP search (requires session cookie)                       #
# --------------------------------------------------------------------------- #

def fetch_from_live(juris_id: int, since: date, limit: int) -> list[dict]:
    """Hit the live MGO Connect search API. Requires MGO_PHPSESSID env var.

    Anonymous calls return []. The session cookie can be grabbed from a logged-in
    browser session at https://www.mygovernmentonline.org/ (DevTools -> Application
    -> Cookies -> PHPSESSID). Set:  export MGO_PHPSESSID=<cookie value>
    """
    import urllib.request

    sess = os.environ.get("MGO_PHPSESSID")
    if not sess:
        print("  [live] MGO_PHPSESSID not set; skipping live fetch.")
        return []
    payload = {
        "AutoSearch": "",
        "CountryID": 0, "StateID": 0, "JurisdictionID": juris_id,
        "ProjectTypeID": 0,
        "ProjectNumber": "", "StreetNumber": "", "StreetName": "",
        "LocalRegistrationNumber": "",
        "CreateDateFrom": since.isoformat(),
        "CreateDateTo": "",
        "City": "", "Apartment": "",
        "TypeID": 0, "SpecificUseID": 0,
        "ContractorFirstName": "", "ContractorLastName": "", "ContractorPhone": "",
        "OtherFirstName": "", "OtherLastName": "", "OtherBusinessName": "", "OtherPhone": "",
        "ParcelNumber": "",
    }
    url = "https://www.mygovernmentonline.org/api/helper/searchprojects/-"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": f"PHPSESSID={sess}",
            "User-Agent": "Mozilla/5.0 (permitlookup MGO loader)",
            "Referer": "https://www.mygovernmentonline.org/",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode() or "[]")
    if not isinstance(data, list):
        print(f"  [live] juris {juris_id}: unexpected response: {str(data)[:200]}")
        return []
    return data[:limit]


def map_live_row(raw: dict, juris_id: int) -> dict:
    """Best-effort mapping of MGO live API row -> SQLite-shape dict.

    The live response field names are PascalCase (ProjectID, ProjectNumber, etc.).
    Unknown fields fall back to None; the SQLite path is the primary loader.
    """
    return {
        "id":                None,
        "permit_number":     raw.get("ProjectNumber") or raw.get("LocalRegistrationNumber"),
        "jurisdiction_id":   juris_id,
        "jurisdiction_name": raw.get("JurisdictionName"),
        "state":             "LA",
        "project_type":      raw.get("ProjectType"),
        "work_type":         raw.get("WorkType"),
        "trade":             raw.get("Trade"),
        "status":            raw.get("ProjectStatus") or raw.get("Status"),
        "created_date":      raw.get("CreateDate"),
        "issued_date":       raw.get("IssueDate"),
        "completed_date":    raw.get("CompletedDate"),
        "address":           raw.get("Address") or raw.get("StreetAddress"),
        "city":              raw.get("City"),
        "zip":               raw.get("ZipCode") or raw.get("Zip"),
        "lat":               raw.get("Latitude"),
        "lng":               raw.get("Longitude"),
        "owner_name":        raw.get("OwnerName"),
        "applicant_name":    raw.get("ApplicantName"),
        "applicant_company": raw.get("ApplicantCompany"),
        "description":       raw.get("Description"),
    }


# --------------------------------------------------------------------------- #
# Map raw row -> hot_leads tuple                                               #
# --------------------------------------------------------------------------- #

def to_hot_leads(raw: dict, source: str, jurisdiction_value: str) -> tuple | None:
    permit_number = trim(raw.get("permit_number"))
    address       = trim(raw.get("address"))
    if not permit_number and not address:
        return None  # no dedup key possible; skip
    juris_name = trim(raw.get("jurisdiction_name"))
    if juris_name and juris_name in DENY_JURIS_NAMES:
        return None
    return (
        permit_number,
        trim(raw.get("project_type")),                  # permit_type
        trim(raw.get("work_type")),                     # work_class
        trim(raw.get("description"), 2000),             # description
        address,
        trim(raw.get("city")),
        "LA",                                            # state -- override default 'TX'
        trim(raw.get("zip"))[:10] if raw.get("zip") else None,
        parish_county_for(jurisdiction_value),          # county <- parish
        raw.get("lat"),
        raw.get("lng"),
        parse_date(raw.get("issued_date")),             # issue_date
        parse_date(raw.get("created_date")),            # applied_date
        trim(raw.get("status")),
        trim(raw.get("applicant_company")),             # contractor_company (best-effort)
        trim(raw.get("applicant_name")),                # applicant_name
        trim(raw.get("applicant_company")),             # applicant_org
        trim(raw.get("owner_name")),
        jurisdiction_value,                             # jurisdiction
        source,
    )


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

def parse_args():
    p = argparse.ArgumentParser(description="MGO BR-metro multi-juris loader (LA)")
    p.add_argument("--juris", default="",
                   help="Comma-separated juris IDs. Default: all 14 BR-metro.")
    p.add_argument("--since",
                   default=(date.today() - timedelta(days=30)).isoformat(),
                   help="CreateDateFrom (YYYY-MM-DD). Default: last 30 days.")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap rows per juris. Default: 10 in dry-run, 500 in --commit.")
    p.add_argument("--dry-run", action="store_true", default=True,
                   help="Default. Fetch + parse + print, no DB writes.")
    p.add_argument("--commit", action="store_true",
                   help="Actually write to DB (overrides --dry-run).")
    p.add_argument("--live", action="store_true",
                   help="Use live MGO HTTP API instead of SQLite snapshot. "
                        "Requires MGO_PHPSESSID env var.")
    return p.parse_args()


def main():
    args = parse_args()
    if args.commit:
        args.dry_run = False
    if args.juris:
        juris_ids = [int(x) for x in args.juris.split(",") if x.strip()]
    else:
        juris_ids = ALL_JURIS_IDS
    if args.limit is None:
        args.limit = 10 if args.dry_run else 500

    try:
        since = datetime.strptime(args.since, "%Y-%m-%d").date()
    except ValueError:
        sys.exit(f"--since must be YYYY-MM-DD, got {args.since!r}")

    print(f"=== MGO BR-metro loader ===")
    print(f"mode      : {'LIVE-HTTP' if args.live else 'SQLITE-SNAPSHOT'}")
    print(f"target    : {'COMMIT' if args.commit else 'DRY-RUN (no DB writes)'}")
    print(f"juris_ids : {juris_ids}")
    print(f"since     : {since}")
    print(f"limit/jur : {args.limit}")
    print()

    pg = None
    if args.commit:
        pg = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                              user=PG_USER, connect_timeout=15)
        with pg.cursor() as c:
            c.execute("SET statement_timeout = 60000")
        pg.commit()

    grand_fetched = grand_inserted = grand_updated = grand_skipped = 0

    for i, juris_id in enumerate(juris_ids):
        if juris_id not in BR_JURIS_BY_ID:
            print(f"!! Unknown juris_id {juris_id}, skipping")
            continue
        _, source, jurisdiction_value, display = BR_JURIS_BY_ID[juris_id]

        if args.live:
            raw_rows = fetch_from_live(juris_id, since, args.limit)
            raw_rows = [map_live_row(r, juris_id) for r in raw_rows]
        else:
            raw_rows = fetch_from_sqlite(juris_id, since, args.limit)

        tuples = []
        skipped = 0
        for r in raw_rows:
            t = to_hot_leads(r, source, jurisdiction_value)
            if t is None:
                skipped += 1
            else:
                tuples.append(t)

        fetched = len(raw_rows)
        inserted = updated = 0

        if args.dry_run:
            print(f"-- {display} (juris={juris_id}, source={source})")
            print(f"   fetched={fetched}, mappable={len(tuples)}, skipped={skipped}")
            for t in tuples[:2]:
                rec = dict(zip(INSERT_COLS, t))
                print(f"   sample: permit_number={rec['permit_number']!r}")
                print(f"           address      ={rec['address']!r}")
                print(f"           city/zip     ={rec['city']!r} / {rec['zip']!r}")
                print(f"           state/juris  ={rec['state']!r} / {rec['jurisdiction']!r}")
                print(f"           county       ={rec['county']!r}")
                print(f"           applied_date ={rec['applied_date']!r}")
                print(f"           issue_date   ={rec['issue_date']!r}")
                print(f"           status       ={rec['status']!r}")
                print(f"           description  ={(rec['description'] or '')[:80]!r}")
                print(f"           owner_name   ={rec['owner_name']!r}")
                print(f"           applicant    ={rec['applicant_name']!r}")
                print(f"           source       ={rec['source']!r}")
                print()
        else:
            if tuples:
                with pg.cursor() as cur:
                    rows = execute_values(cur, INSERT_SQL, tuples,
                                          page_size=500, fetch=True)
                pg.commit()
                for (was_inserted,) in rows:
                    if was_inserted:
                        inserted += 1
                    else:
                        updated += 1

        line = (f"{display} ({juris_id}): fetched {fetched}, "
                f"inserted {inserted}, updated {updated}, skipped {skipped}")
        print(line)
        grand_fetched += fetched
        grand_inserted += inserted
        grand_updated += updated
        grand_skipped += skipped

        # Be polite between juris IDs
        if i < len(juris_ids) - 1:
            time.sleep(1.5)

    print()
    print("=== TOTALS ===")
    print(f"fetched : {grand_fetched}")
    print(f"inserted: {grand_inserted}")
    print(f"updated : {grand_updated}")
    print(f"skipped : {grand_skipped}")
    if pg:
        pg.close()


if __name__ == "__main__":
    main()

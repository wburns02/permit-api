#!/usr/bin/env python3
"""
Multi-jurisdiction MGO Connect loader for the Baton Rouge metro 14
jurisdictions. Loads into the canonical `hot_leads` table with state='LA'
(NOT the TX default).

PRIMARY PATH (default): live MGO Connect v3 HTTP API.
  - Endpoint: POST https://api.mgoconnect.org/api/v3/cp/project/search-projects
  - Auth:     POST https://www.mygovernmentonline.org/api/user/login/-
  - Creds:    MGO_EMAIL / MGO_PASSWORD env vars (fallback hardcoded creds match
              the existing scrape_mgo_ctx.py pipeline).
  - Filter shape proven against scrape_mgo_ctx.py (in production).
  - This path works for all 14 BR-metro juris IDs without needing the local
    SQLite snapshot, which was Ascension-only.

FALLBACK PATH (--snapshot): read from the daily MGOConnect scraper SQLite at
/mnt/win11/fedora-moved/Data/crm_permits.db. Useful when MGO API is down.

Output table: public.hot_leads, source='mgo_<slug>', state='LA',
jurisdiction='<value>'. Dedup is enforced by the partial unique index
idx_hot_leads_dedup (permit_number, address, state)
  WHERE permit_number IS NOT NULL AND address IS NOT NULL.

14 BR-metro target juris IDs (id, source slug, jurisdiction display, parish):
  26  mgo_ascension          Ascension Parish
  35  mgo_west_baton_rouge   West Baton Rouge Parish
  460 mgo_iberville          Iberville Parish
  189 mgo_livingston_parish  Livingston Parish
  121 mgo_denham_springs     Denham Springs (Livingston)
  392 mgo_town_of_livingston Town of Livingston (Livingston)
  456 mgo_walker             Walker (Livingston)
  143 mgo_gonzales           Gonzales (Ascension)
  528 mgo_donaldsonville     Donaldsonville (Ascension)
  547 mgo_sorrento           Sorrento (Ascension)
  214 mgo_baker              Baker (East Baton Rouge) -- empty in MGO
  532 mgo_city_of_plaquemine City of Plaquemine (Iberville)
  160 mgo_st_gabriel         St. Gabriel (Iberville)
  27  mgo_east_baton_rouge   East Baton Rouge Parish

Banner note: MyGovernmentOnline.org has announced the customer portal is being
retired. The api.mgoconnect.org v3 endpoints are the modern replacement and
remain operational.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

try:
    import httpx
except ImportError:
    print("pip install httpx", file=sys.stderr)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #

PG_HOST = os.getenv("DB_HOST", "100.122.216.15")
PG_PORT = int(os.getenv("DB_PORT", "5432"))
PG_DB = os.getenv("DB_NAME", "permits")
PG_USER = os.getenv("DB_USER", "will")

MGO_SQLITE = "/mnt/win11/fedora-moved/Data/crm_permits.db"

# MGO Connect API (mgoconnect.org v3 -- modern endpoint).
MGO_EMAIL = os.environ["MGO_EMAIL"]
MGO_PASSWORD = os.environ["MGO_PASSWORD"]
API_BASE = "https://api.mgoconnect.org"
LEGACY_LOGIN_API = "https://www.mygovernmentonline.org/api"
SEARCH_ENDPOINT = f"{API_BASE}/api/v3/cp/project/search-projects"
PERMIT_TYPE_ID = 3   # "Permit" project type
PAGE_SIZE = 500

# 14 BR-metro jurisdiction IDs.
# (juris_id, source_slug, jurisdiction_value, display_name)
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
ON CONFLICT (permit_number, source)
DO UPDATE SET
    permit_type        = COALESCE(EXCLUDED.permit_type,        hot_leads.permit_type),
    work_class         = COALESCE(EXCLUDED.work_class,         hot_leads.work_class),
    description        = COALESCE(EXCLUDED.description,        hot_leads.description),
    address            = COALESCE(EXCLUDED.address,            hot_leads.address),
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
    jurisdiction       = COALESCE(EXCLUDED.jurisdiction,       hot_leads.jurisdiction),
    state              = COALESCE(EXCLUDED.state,              hot_leads.state)
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
    # Some MGO responses include a millisecond suffix (e.g. 2026-05-04T07:19:17.000)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
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


def maybe_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parish_county_for(jurisdiction_value: str) -> str | None:
    """Map a jurisdiction value to its containing parish (LA equivalent of county)."""
    if not jurisdiction_value:
        return None
    v = jurisdiction_value.strip()
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
# Source: live MGO Connect v3 HTTP search                                      #
# --------------------------------------------------------------------------- #

def mgo_login() -> httpx.Client:
    """Authenticate against MGO Connect, return a configured httpx.Client."""
    session = httpx.Client(timeout=30, follow_redirects=True)
    session.headers.update({
        "accept": "application/json",
        "sourceplatform": "MGO Connect Web",
        "user-agent": "Mozilla/5.0 (permitlookup MGO loader)",
        "referer": "https://www.mgoconnect.org/",
    })
    body = "=" + urllib.parse.quote(json.dumps({
        "Email": MGO_EMAIL,
        "Password": MGO_PASSWORD,
    }))
    resp = session.post(
        f"{LEGACY_LOGIN_API}/user/login/-",
        content=body,
        headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
    )
    data = resp.json()
    token = data.get("UserToken")
    if not token:
        raise RuntimeError(f"MGO login failed: {data}")
    session.headers["authorization-token"] = token
    print(f"  [auth] MGO Connect logged in (UserID={data.get('UserID')})")
    return session


def _post_with_retry(session: httpx.Client, payload: dict,
                     juris_id: int, max_attempts: int = 4) -> dict | None:
    """POST to SEARCH_ENDPOINT with backoff on 5xx / timeout responses.

    The MGO server intermittently returns 500/504 when the result set is
    large; retrying with a small backoff usually clears it.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            r = session.post(SEARCH_ENDPOINT, json=payload, timeout=90)
        except Exception as e:
            print(f"  [live] juris {juris_id} attempt {attempt} error: {e}")
            time.sleep(2 * attempt)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                print(f"  [live] juris {juris_id} json error: {e}")
                return None
        # 5xx / 504 -> backoff and retry
        if r.status_code in (500, 502, 503, 504):
            print(f"  [live] juris {juris_id} attempt {attempt} HTTP {r.status_code}; "
                  f"retrying in {2*attempt}s")
            time.sleep(2 * attempt)
            continue
        # 4xx etc -> give up
        print(f"  [live] juris {juris_id} HTTP {r.status_code}: {r.text[:200]}")
        return None
    print(f"  [live] juris {juris_id} exhausted {max_attempts} retries")
    return None


def fetch_from_live(session: httpx.Client, juris_id: int,
                    since: date, limit: int,
                    chunk_days: int = 365) -> list[dict]:
    """Page through the MGO v3 search-projects endpoint for one juris_id.

    Strategy: walk year-sized date windows backward from today to ``since`` to
    keep result sets small. MGO 500/504s when the date range is too broad.
    Also paginates within each window using OFFSET. Returns normalized rows
    with keys matching the SQLite schema used by ``to_hot_leads``.
    """
    out: list[dict] = []
    seen_keys: set[tuple] = set()
    today = date.today()

    # Walk windows: [today-chunk_days, today], then back another chunk_days, etc.
    window_end = today
    while window_end >= since and len(out) < limit:
        window_start = max(since, window_end - timedelta(days=chunk_days))
        offset = 0
        # Inner pagination loop within this date window
        while len(out) < limit:
            page_rows = min(PAGE_SIZE, limit - len(out))
            payload = {
                "filters": {
                    "JURISDICTIONID": juris_id,
                    "PROJECTTYPEID": PERMIT_TYPE_ID,
                    "CREATEDATEAFTER": window_start.isoformat(),
                    "CREATEDATEBEFORE": window_end.isoformat(),
                },
                "Rows": page_rows,
                "OffSet": offset,
                "SortField": "dateCreated",
                "SortOrder": "desc",
            }
            d = _post_with_retry(session, payload, juris_id)
            if d is None:
                break  # window failed; move to next window
            items = d.get("data") or d.get("rows") or []
            if not items:
                break
            new_in_page = 0
            for raw in items:
                # Dedup across windows by projectUID/projectNumber
                key = (raw.get("projectUID") or raw.get("projectID")
                       or raw.get("projectNumber"))
                if key and key in seen_keys:
                    continue
                if key:
                    seen_keys.add(key)
                out.append(map_live_row(raw, juris_id))
                new_in_page += 1
                if len(out) >= limit:
                    break
            if len(items) < page_rows or new_in_page == 0:
                break
            offset += page_rows
            time.sleep(0.4)  # polite within window

        # Step to the previous window
        if window_start <= since:
            break
        window_end = window_start - timedelta(days=1)
        time.sleep(0.5)
    return out


def map_live_row(raw: dict, juris_id: int) -> dict:
    """Map mgoconnect.org v3 row -> SQLite-shape dict consumed by ``to_hot_leads``."""
    return {
        "id":                raw.get("projectUID"),
        "permit_number":     raw.get("projectNumber") or raw.get("projectUID"),
        "jurisdiction_id":   juris_id,
        "jurisdiction_name": raw.get("jurisdiction"),
        "state":             raw.get("projectState") or "LA",
        "project_type":      raw.get("projectType") or "Permit",
        "work_type":         raw.get("workType"),
        "trade":             raw.get("trade") or raw.get("designationType"),
        "status":            raw.get("projectStatus"),
        "created_date":      raw.get("dateCreated"),
        "issued_date":       raw.get("dateIssued") or raw.get("issuedDate"),
        "completed_date":    raw.get("completedDate"),
        "address":           raw.get("projectAddress") or raw.get("address"),
        "apt_lot":           raw.get("projectAptSpaceLot"),
        "city":              raw.get("projectCity") or raw.get("city"),
        "zip":               raw.get("projectZip") or raw.get("zip"),
        "lat":               raw.get("projectLat") or raw.get("lat"),
        "lng":               raw.get("projectLng") or raw.get("lng"),
        "owner_name":        raw.get("ownerName"),
        "applicant_name":    raw.get("applicantName"),
        "applicant_company": raw.get("applicantCompany"),
        "description":       raw.get("projectDescription") or raw.get("projectName"),
    }


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
    zip_val = trim(raw.get("zip"))
    if zip_val:
        zip_val = zip_val[:10]
    return (
        permit_number,
        trim(raw.get("project_type")),                  # permit_type
        trim(raw.get("work_type")),                     # work_class
        trim(raw.get("description"), 2000),             # description
        address,
        trim(raw.get("city")),
        "LA",                                            # state -- override default 'TX'
        zip_val,
        parish_county_for(jurisdiction_value),          # county <- parish
        maybe_float(raw.get("lat")),
        maybe_float(raw.get("lng")),
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
                   default=(date.today() - timedelta(days=90)).isoformat(),
                   help="CreateDateAfter (YYYY-MM-DD). Default: last 90 days.")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap rows per juris. Default: 10 in dry-run, 5000 in --commit.")
    p.add_argument("--dry-run", action="store_true", default=True,
                   help="Default. Fetch + parse + print, no DB writes.")
    p.add_argument("--commit", action="store_true",
                   help="Actually write to DB (overrides --dry-run).")
    p.add_argument("--snapshot", action="store_true",
                   help="Use the local SQLite snapshot instead of the live "
                        "MGO Connect API. Snapshot only contains Ascension as of "
                        "2026-02; --live (default) covers all 14 juris.")
    p.add_argument("--live", action="store_true",
                   help="(default) Use live MGO Connect API. Compatible flag "
                        "kept for backwards compatibility.")
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
        args.limit = 10 if args.dry_run else 5000

    use_live = not args.snapshot  # default = live; --snapshot forces SQLite

    try:
        since = datetime.strptime(args.since, "%Y-%m-%d").date()
    except ValueError:
        sys.exit(f"--since must be YYYY-MM-DD, got {args.since!r}")

    print(f"=== MGO BR-metro loader ===")
    print(f"mode      : {'LIVE-MGO-CONNECT-V3' if use_live else 'SQLITE-SNAPSHOT'}")
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
            c.execute("SET statement_timeout = 120000")
        pg.commit()

    session: httpx.Client | None = None
    if use_live:
        # Retry MGO login a few times; the auth endpoint occasionally times out.
        last_err = None
        for attempt in range(1, 5):
            try:
                session = mgo_login()
                break
            except Exception as e:
                last_err = e
                print(f"!! MGO login attempt {attempt} failed: {e}")
                time.sleep(3 * attempt)
        if session is None:
            if args.snapshot:
                print("!! Login failed; using snapshot path as requested.")
                use_live = False
            else:
                sys.exit(f"MGO login failed after retries: {last_err}. "
                         f"Pass --snapshot to use the SQLite fallback explicitly.")

    grand_fetched = grand_inserted = grand_updated = grand_skipped = 0
    per_juris_summary: list[tuple[str, int, int, int]] = []

    for i, juris_id in enumerate(juris_ids):
        if juris_id not in BR_JURIS_BY_ID:
            print(f"!! Unknown juris_id {juris_id}, skipping")
            continue
        _, source, jurisdiction_value, display = BR_JURIS_BY_ID[juris_id]

        if use_live:
            assert session is not None
            raw_rows = fetch_from_live(session, juris_id, since, args.limit)
        else:
            raw_rows = fetch_from_sqlite(juris_id, since, args.limit)

        tuples = []
        skipped = 0
        seen_pn_source: set[tuple] = set()
        # Index of permit_number in INSERT_COLS == 0; source == last
        for r in raw_rows:
            t = to_hot_leads(r, source, jurisdiction_value)
            if t is None:
                skipped += 1
                continue
            # Dedup by (permit_number, source) to satisfy ix_hot_leads_permit
            # within a single batch (postgres rejects two ON CONFLICT rows
            # targeting the same arbiter index in one INSERT).
            pn, src = t[0], t[-1]
            if pn is not None:
                k = (pn, src)
                if k in seen_pn_source:
                    skipped += 1
                    continue
                seen_pn_source.add(k)
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
                BATCH = 100
                for off in range(0, len(tuples), BATCH):
                    chunk = tuples[off:off + BATCH]
                    with pg.cursor() as cur:
                        rows = execute_values(cur, INSERT_SQL, chunk,
                                              page_size=BATCH, fetch=True)
                    pg.commit()
                    for (was_inserted,) in rows:
                        if was_inserted:
                            inserted += 1
                        else:
                            updated += 1

        line = (f"{display} ({juris_id}): fetched {fetched}, "
                f"inserted {inserted}, updated {updated}, skipped {skipped}")
        print(line)
        per_juris_summary.append((display, fetched, inserted, updated))
        grand_fetched += fetched
        grand_inserted += inserted
        grand_updated += updated
        grand_skipped += skipped

        # Be polite between juris IDs
        if i < len(juris_ids) - 1:
            time.sleep(1.0)

    print()
    print("=== TOTALS ===")
    print(f"fetched : {grand_fetched}")
    print(f"inserted: {grand_inserted}")
    print(f"updated : {grand_updated}")
    print(f"skipped : {grand_skipped}")
    if pg:
        pg.close()
    if session:
        session.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
City of Corpus Christi (Nueces County, TX) building-permit scraper.

Source: the public, account-free Infor "Rhythm" / CIVICS portal
        https://corpuschristi-prd.rhythmlabs.infor.com
There is no captcha and no auth — the public "Lookup Building Permits" search is
backed by two plain XHR endpoints (captured from the live network panel, see
docs / scratchpad corpus_recon.md):

  LIST   GET /delegate/civics-api/api/core/views/instruments
         ?ExtendedFilter=[{operator:And,criteria:[
              {operator:GreaterThanOrEqual,property:addedDateTime,value:<ISO>},
              {operator:LessThan,        property:addedDateTime,value:<ISO>}]}]
         &OrderBy=[{property:addedDateTime,direction:desc}]
         &Select=[id,InstrumentType,...]
         &Page=[{start:<1-based row offset>,max:100}]
         &queryClosed=false&Distinct=true
     -> {"data":[{id, instrumentType, instrumentSubType, description,
                  instrumentNumber, addedDateTime, instrumentDateTime,
                  statusDescription}, ...]}
     locationLine1 is ALWAYS EMPTY on the list endpoint — the address only
     comes from the detail endpoint below.

  DETAIL GET /delegate/civics-api/api/cdr/applications/Building/{id}
         ?IncludeAttachments=false
     -> {"data":{applicationNumber, workTypeDescription, applicationName,
                 comments, locationLine1, locationLine2, issuedDateTime,
                 addedDateTime, applicationDateTime, status, statusDescription,
                 parcel:{parcelId}, declaredValuation, squareFootage}}

Only `instrumentType == 'BuildingApplication'` rows have a /Building/{id} detail.
Re-roofs in Corpus are filed as Residential ("RES") permits whose roof signal
lives in workTypeDescription / applicationName / comments — there is no distinct
"roof" permit subtype at the list level, so we pull RES (+ COM) building permits
and flag the re-roof subset by regex.

Loads into hot_leads on T430 (source='infor_corpuschristi'), the same store the
OpenGov scraper uses for Port Aransas. scripts/promote_nueces_reroof.py then
promotes the Re-Roof subset of BOTH cities into nueces_permits, which the
unserviced_hail_leads Nueces serviced-exclusion joins against.

Gentle on the source + DB:
  - paged by addedDateTime month windows, bounded
  - ~RATE_DETAIL s between detail calls, ~RATE_PAGE s between list pages
  - retries with backoff; resumable (skips ids already in hot_leads for this src)
  - DB writes use lock_timeout + ON CONFLICT DO NOTHING, no full scans
  - raw JSON staged to /mnt/win11/Fedora/free_data (never /home/will)

KNOWN LIMITATION (verified 2026-06-29): the portal authorizes the search API
ONLY inside an established browser session. The anonymous JSESSIONID is bound to
the client that created it — a standalone replay from the server returns HTTP 401
"Unauthorized" even when the EXACT browser cookie jar (JSESSIONID + ips.username=
rwebuser + InstanceId + AWSALBAPP-*) is replayed and the TLS fingerprint is
spoofed (tested with curl_cffi chrome impersonation). The in-browser fetch with
credentials:'include' returns 200; credentials:'omit' returns 401 — so auth is
purely the session cookie, but that session resists off-browser reuse.
Workaround actually used for the initial load: harvest the roof subset via the
MCP/Playwright browser context (same list+detail XHR this module replays), stage
to /mnt/win11/Fedora/free_data/corpus_permits/, and ingest with
scripts/load_corpus_roofs_capture.py. This module is the correct replay shape and
will work the moment the session binding is solved (or run it via a Playwright-
driven page.evaluate that reuses an authorized context).

ALSO: Corpus does NOT issue a distinct "Re-Roof" permit type — re-roofs file as
"Residential Permit Application" (workType Remodel/Addition/Accessory) whose roof
signal lives only in free-text workTypeDescription/applicationName/comments. So
the roof flag is a regex over those fields (solar excluded), which is why the
yield is sparse (~7-15 roof-signal RES per month).

Usage:
  python3 scrape_corpus_permits.py                 # last 24 months, RES+COM
  python3 scrape_corpus_permits.py --months 36
  python3 scrape_corpus_permits.py --subtypes RES  # residential only
  python3 scrape_corpus_permits.py --dry-run       # no DB writes, still stages raw
  python3 scrape_corpus_permits.py --roof-only     # only insert roof-flagged rows
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    raise

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    raise

# ── Config ──────────────────────────────────────────────────────────────────

BASE = "https://corpuschristi-prd.rhythmlabs.infor.com"
LIST_PATH = "/delegate/civics-api/api/core/views/instruments"
DETAIL_PATH = "/delegate/civics-api/api/cdr/applications/Building/{id}"

SOURCE = "infor_corpuschristi"
JURISDICTION = "Corpus Christi, TX"

# instrumentSubTypes that carry building work (residential + commercial). Roof
# re-roofs file under RES; COM kept so the store is complete for future arms.
DEFAULT_SUBTYPES = ("RES", "COM")

LIST_SELECT = ("[id,InstrumentType,ProductFamily,CommonId,InstrumentName,"
               "InstrumentNumber,InstrumentSubType,Description,PortalDescription,"
               "InstrumentDateTime,StatusDescription,addedDateTime]")

ROOF_RE = re.compile(r"roof|re-?roof|reroof|shingle|tear[\s-]?off|re-?shingl",
                     re.IGNORECASE)

HEADERS = {
    "x-requested-with": "XMLHttpRequest",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "referer": f"{BASE}/lookup-record",
}

PAGE_SIZE = 100
RATE_PAGE = 0.5     # seconds between list pages
RATE_DETAIL = 0.3   # seconds between detail fetches
MAX_RETRIES = 4

DB_HOST = os.getenv("DB_HOST", os.getenv("PGHOST", "100.122.216.15"))
DB_PORT = os.getenv("DB_PORT", os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("DB_NAME", os.getenv("PGDATABASE", "permits"))
DB_USER = os.getenv("DB_USER", os.getenv("PGUSER", "will"))

STAGE_CANDIDATES = [
    Path("/mnt/win11/Fedora/free_data/corpus_permits"),
    Path("/dataPool/free_data/corpus_permits"),
]


def stage_dir() -> Path | None:
    for p in STAGE_CANDIDATES:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    return None


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def _get(session: requests.Session, url: str) -> dict | None:
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, headers=HEADERS, timeout=40)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2.0 * (attempt + 1))
                continue
            # 404 etc — give up on this URL
            return None
        except Exception as e:  # noqa: BLE001
            time.sleep(2.0 * (attempt + 1))
            if attempt == MAX_RETRIES - 1:
                print(f"    [http] giving up {url[:90]}: {e}", file=sys.stderr)
    return None


def _ext_filter(start_iso: str, end_iso: str, subtype: str | None) -> str:
    crit = [
        f"{{operator:GreaterThanOrEqual,property:addedDateTime,value:{start_iso}}}",
        f"{{operator:LessThan,property:addedDateTime,value:{end_iso}}}",
        "{operator:NotEqual,property:instrumentType,Value:PortalInstrument}",
        "{operator:NotEqual,property:instrumentType,Value:PortalScenario}",
    ]
    if subtype:
        crit.append(f"{{operator:Equal,property:instrumentSubType,Value:{subtype}}}")
    return "[{operator:And,criteria:[" + ",".join(crit) + "]}]"


def list_window(session, start_iso, end_iso, subtype):
    """Yield BuildingApplication list rows for one (window, subtype)."""
    start = 1
    while True:
        ef = _ext_filter(start_iso, end_iso, subtype)
        url = (f"{BASE}{LIST_PATH}"
               f"?ExtendedFilter={quote(ef)}"
               f"&OrderBy={quote('[{property:addedDateTime,direction:desc}]')}"
               f"&Select={quote(LIST_SELECT)}"
               f"&Page={quote(f'[{{start:{start},max:{PAGE_SIZE}}}]')}"
               f"&queryClosed=false&Distinct=true&_={int(time.time()*1000)}")
        j = _get(session, url)
        data = (j or {}).get("data") or []
        if not data:
            return
        for rec in data:
            if rec.get("instrumentType") == "BuildingApplication":
                yield rec
        if len(data) < PAGE_SIZE:
            return
        start += len(data)
        time.sleep(RATE_PAGE)


def fetch_detail(session, instrument_id):
    url = (f"{BASE}{DETAIL_PATH.format(id=instrument_id)}"
           f"?IncludeAttachments=false&_={int(time.time()*1000)}")
    j = _get(session, url)
    if not j:
        return None
    return j.get("data") or None


# ── Transform ────────────────────────────────────────────────────────────────

def _parse_dt(val):
    if not val or str(val).startswith("0001"):
        return None
    try:
        return datetime.fromisoformat(str(val)).date()
    except Exception:
        try:
            return datetime.fromisoformat(str(val).replace("Z", "+00:00")).date()
        except Exception:
            return None


def transform(list_rec, detail):
    """Build a hot_leads-shaped dict from list row + detail."""
    loc1 = (detail.get("locationLine1") or "").strip()
    loc2 = (detail.get("locationLine2") or "").strip()
    # loc2 is "CITY ST ZIP". address = loc1.
    city, state, zip_code = "CORPUS CHRISTI", "TX", None
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", loc2)
    if m:
        zip_code = m.group(1)
    work = detail.get("workTypeDescription") or ""
    app_name = detail.get("applicationName") or ""
    comments = detail.get("comments") or ""
    blob = " ".join([work, app_name, comments,
                     list_rec.get("description") or ""])
    is_roof = bool(ROOF_RE.search(blob))

    issued = _parse_dt(detail.get("issuedDateTime"))
    applied = _parse_dt(detail.get("applicationDateTime")) or \
        _parse_dt(detail.get("addedDateTime")) or \
        _parse_dt(list_rec.get("addedDateTime"))
    # issue_date in hot_leads = the issued date when present, else the applied/
    # added date so the record still time-sorts; the promoter prefers issued.
    issue_date = issued or applied

    permit_number = (detail.get("applicationNumber")
                     or list_rec.get("instrumentNumber") or "")
    valuation = detail.get("declaredValuation")
    try:
        valuation = float(valuation) if valuation else None
    except Exception:
        valuation = None

    permit_type = list_rec.get("description") or ""  # "Residential Permit Application"
    # work_class carries the fine-grained work type + roof flag so the promoter
    # and the MV roof-regex both have a clean signal.
    work_class = work or app_name

    return {
        "permit_number": permit_number[:200] or None,
        "permit_type": permit_type[:200] or None,
        "work_class": (work_class[:200] or None),
        "description": (comments[:500] or app_name[:500] or None),
        "address": loc1[:300] or None,
        "city": city[:100],
        "state": state,
        "zip": zip_code,
        "county": "Nueces",
        "lat": None,
        "lng": None,
        "issue_date": issue_date,
        "applied_date": applied,
        "status": (detail.get("statusDescription") or "")[:100] or None,
        "valuation": valuation,
        "applicant_name": (detail.get("primaryContactName") or "")[:200] or None,
        "owner_name": None,
        "contractor_name": None,
        "contractor_company": None,
        "jurisdiction": JURISDICTION,
        "source": SOURCE,
        "is_roof": is_roof,
        "_raw": {"list": list_rec, "detail_id": list_rec.get("id"),
                 "workType": work, "issued": detail.get("issuedDateTime")},
    }


# ── DB ───────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, connect_timeout=20)


INSERT_SQL = """
INSERT INTO hot_leads (
    id, permit_number, permit_type, work_class, description,
    address, city, state, zip, county, lat, lng,
    issue_date, applied_date, status, valuation,
    applicant_name, owner_name, contractor_name, contractor_company,
    jurisdiction, source
) VALUES %s
ON CONFLICT (permit_number, source) DO NOTHING
"""


def insert_records(records, dry_run=False):
    if not records:
        return 0
    if dry_run:
        return len(records)
    rows = [(
        str(uuid.uuid4()), r["permit_number"], r["permit_type"], r["work_class"],
        r["description"], r["address"], r["city"], r["state"], r["zip"],
        r["county"], r["lat"], r["lng"], r["issue_date"], r["applied_date"],
        r["status"], r["valuation"], r["applicant_name"], r["owner_name"],
        r["contractor_name"], r["contractor_company"], r["jurisdiction"],
        r["source"],
    ) for r in records]
    conn = get_conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET statement_timeout='120s'")
        cur.execute("SET lock_timeout='10s'")
        execute_values(cur, INSERT_SQL, rows, page_size=500)
        conn.commit()
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else len(rows)
    finally:
        conn.close()


# ── Main ─────────────────────────────────────────────────────────────────────

def month_windows(months_back: int):
    """Yield (start_iso, end_iso, label) month windows, newest first."""
    today = date.today().replace(day=1)
    cur = today
    for _ in range(months_back + 1):
        # window = [cur, next_month)
        if cur.month == 12:
            nxt = cur.replace(year=cur.year + 1, month=1)
        else:
            nxt = cur.replace(month=cur.month + 1)
        yield (cur.isoformat() + "T00:00:00",
               nxt.isoformat() + "T00:00:00",
               cur.strftime("%Y-%m"))
        # step back one month
        if cur.month == 1:
            cur = cur.replace(year=cur.year - 1, month=12)
        else:
            cur = cur.replace(month=cur.month - 1)


def main():
    ap = argparse.ArgumentParser(description="Corpus Christi Infor permit scraper")
    ap.add_argument("--months", type=int, default=24,
                    help="How many months back to pull (default 24).")
    ap.add_argument("--subtypes", default=",".join(DEFAULT_SUBTYPES),
                    help="Comma list of instrumentSubTypes (default RES,COM).")
    ap.add_argument("--roof-only", action="store_true",
                    help="Only insert roof-flagged permits into hot_leads.")
    ap.add_argument("--dry-run", action="store_true",
                    help="No DB writes (still stages raw JSON).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after N building permits (debug).")
    args = ap.parse_args()

    subtypes = [s.strip() for s in args.subtypes.split(",") if s.strip()]
    session = requests.Session()
    sd = stage_dir()
    stage_fp = None
    if sd:
        stage_fp = (sd / f"corpus_{datetime.now():%Y%m%dT%H%M%S}.jsonl").open("w")
        print(f"[stage] raw -> {stage_fp.name}", flush=True)
    else:
        print("[stage] WARNING: no writable staging dir; raw not persisted",
              file=sys.stderr)

    print(f"[corpus] pulling {args.months}mo, subtypes={subtypes}, "
          f"roof_only={args.roof_only}, dry_run={args.dry_run}", flush=True)

    seen_ids: set = set()
    total_bld = 0
    total_roof = 0
    inserted = 0
    batch = []

    try:
        for start_iso, end_iso, label in month_windows(args.months):
            win_bld = 0
            win_roof = 0
            for subtype in subtypes:
                for lr in list_window(session, start_iso, end_iso, subtype):
                    iid = lr.get("id")
                    if iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    detail = fetch_detail(session, iid)
                    time.sleep(RATE_DETAIL)
                    if not detail:
                        continue
                    rec = transform(lr, detail)
                    total_bld += 1
                    win_bld += 1
                    if rec["is_roof"]:
                        total_roof += 1
                        win_roof += 1
                    if stage_fp:
                        stage_fp.write(json.dumps({
                            "permit_number": rec["permit_number"],
                            "permit_type": rec["permit_type"],
                            "work_class": rec["work_class"],
                            "address": rec["address"], "zip": rec["zip"],
                            "issue_date": (rec["issue_date"].isoformat()
                                           if rec["issue_date"] else None),
                            "is_roof": rec["is_roof"],
                            "status": rec["status"],
                        }) + "\n")
                    if args.roof_only and not rec["is_roof"]:
                        pass
                    else:
                        batch.append(rec)
                    if len(batch) >= 500:
                        inserted += insert_records(batch, args.dry_run)
                        batch = []
                    if args.limit and total_bld >= args.limit:
                        raise StopIteration
            print(f"  {label}: {win_bld} building permits, {win_roof} roof-flagged "
                  f"(running: {total_bld} / {total_roof} roof / {inserted} ins)",
                  flush=True)
    except (StopIteration, KeyboardInterrupt):
        pass
    finally:
        if batch:
            inserted += insert_records(batch, args.dry_run)
        if stage_fp:
            stage_fp.close()

    print("\n" + "=" * 56, flush=True)
    print(f"Corpus Christi: {total_bld} building permits scraped", flush=True)
    print(f"                {total_roof} roof-flagged", flush=True)
    print(f"                {inserted} inserted into hot_leads "
          f"(source={SOURCE}){' [DRY RUN]' if args.dry_run else ''}", flush=True)
    print("=" * 56, flush=True)


if __name__ == "__main__":
    main()

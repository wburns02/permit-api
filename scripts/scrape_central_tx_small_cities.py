#!/usr/bin/env python3
"""
Central Texas Small Cities — Combined Permit Scraper

Pulls recent permits for smaller Central TX jurisdictions and loads
them into hot_leads (+ permits_tx) on the permits DB.

Targeted cities & data sources:
    Georgetown       MGO Connect (API)      jurisdiction ID 48
    Dripping Springs MGO Connect (API)      jurisdiction ID 180
    Bee Cave         MGO Connect (API)      jurisdiction ID 130
    Buda             MGO Connect (API)      jurisdiction ID 129
    Hutto            ArcGIS FeatureServer   City_of_Hutto_Building_Permits_2025_2026
    Leander          ArcGIS FeatureServer   BLDPermit_2025
    Kyle             SKIP — Tyler EnerGov CSS (OIDC-locked, needs browser)
    Lakeway          SKIP — Tyler EnerGov CSS (OIDC-locked, needs browser)

Usage:
    python3 scrape_central_tx_small_cities.py --city georgetown
    python3 scrape_central_tx_small_cities.py --all --days 30
    python3 scrape_central_tx_small_cities.py --all --days 30 --dry-run
    python3 scrape_central_tx_small_cities.py --db-host 100.122.216.15 --city hutto

Database target (defaults):
    host=100.122.216.15 dbname=permits user=will
    hot_leads: UNIQUE (permit_number, source) — uses ON CONFLICT DO UPDATE
    permits_tx: creates UNIQUE index on (source, permit_number) if missing,
                then ON CONFLICT DO UPDATE
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ImportError:
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# ───────────────────────── config ────────────────────────────────────────
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
DEFAULT_DB_HOST = "100.122.216.15"

MGO_EMAIL = "willwalterburns@gmail.com"
MGO_PASSWORD = "<redacted-rotate-me>"
MGO_API_BASE = "https://api.mgoconnect.org"
MGO_LEGACY_API = "https://www.mygovernmentonline.org/api"
MGO_SEARCH = f"{MGO_API_BASE}/api/v3/cp/project/search-projects"
MGO_PERMIT_TYPE_ID = 3
MGO_PAGE_SIZE = 500

ARCGIS_UA = "Mozilla/5.0 (permit-scraper)"

# City → config
CITIES = {
    "georgetown": {
        "platform": "mgo",
        "mgo_id": 48,
        "mgo_name": "Georgetown",
        "city_upper": "GEORGETOWN",
        "county": "Williamson",
        "source": "georgetown_direct",
    },
    "dripping_springs": {
        "platform": "mgo",
        "mgo_id": 180,
        "mgo_name": "Dripping Springs",
        "city_upper": "DRIPPING SPRINGS",
        "county": "Hays",
        "source": "dripping_springs_direct",
    },
    "bee_cave": {
        "platform": "mgo",
        "mgo_id": 130,
        "mgo_name": "Bee Cave",
        "city_upper": "BEE CAVE",
        "county": "Travis",
        "source": "bee_cave_direct",
    },
    "buda": {
        "platform": "mgo",
        "mgo_id": 129,
        "mgo_name": "Buda",
        "city_upper": "BUDA",
        "county": "Hays",
        "source": "buda_direct",
    },
    "hutto": {
        "platform": "arcgis",
        "url": "https://services.arcgis.com/YZhxlqU7ABWQBGTG/arcgis/rest/services/City_of_Hutto_Building_Permits_2025_2026/FeatureServer/0",
        "field_map": {
            "permit_number": "USER_Number",
            "permit_type": "USER_Workflow_Type",
            "address": "USER_Address",
            "issue_date": "USER_Issued_Date",
        },
        "date_field": "USER_Issued_Date",
        "city_upper": "HUTTO",
        "county": "Williamson",
        "source": "hutto_direct",
    },
    "leander": {
        "platform": "arcgis",
        "url": "https://services1.arcgis.com/L0MLvN0Ay0iEjnCT/arcgis/rest/services/BLDPermit_2025/FeatureServer/0",
        "field_map": {
            "permit_number": "Permit_Number",
            "permit_type": "Type",
            "work_class": "Workclass",
            "status": "Status",
            "address": "Main_Address",
            "applied_date": "App_Date",
            "project": "Project",
            "parcel": "Parcel",
        },
        "date_field": "App_Date",
        "city_upper": "LEANDER",
        "county": "Williamson",
        "source": "leander_direct",
    },
    # SKIP (noted in --help and startup banner)
    # "kyle"    — Tyler EnerGov CSS, OIDC-locked
    # "lakeway" — Tyler EnerGov CSS, OIDC-locked
}

SKIPPED = {
    "kyle": "Tyler EnerGov Citizen Self Service (kyletx-energovpub.tylerhost.net) — OIDC-locked JSON API; no public FeatureServer found. Needs browser automation (Playwright) with logged-in session.",
    "lakeway": "Tyler EnerGov Civic Access (lakewaytx-energovweb.tylerhost.net) — OIDC-locked JSON API; public Energov FeatureServer contains only geo reference layers (address/parcels/zoning). Needs browser automation.",
}


# ───────────────────────── helpers ───────────────────────────────────────
def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    return psycopg2.connect(host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def ensure_permits_tx_unique(conn):
    """Ensure we have a unique (source, permit_number) constraint on permits_tx
    so we can do ON CONFLICT upserts. Uses a partial unique index so existing
    rows with NULL values are not affected.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_permits_tx_source_permit "
            "ON permits_tx (source, permit_number) "
            "WHERE permit_number IS NOT NULL AND source IS NOT NULL"
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (permits_tx unique index: {e})")
    finally:
        cur.close()


def ensure_hot_leads(conn):
    """Create hot_leads table if missing (matches structure used by other scrapers)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hot_leads (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                permit_number TEXT,
                permit_type TEXT,
                work_class TEXT,
                description TEXT,
                address TEXT,
                city TEXT,
                state VARCHAR(2) NOT NULL DEFAULT 'TX',
                zip TEXT,
                county TEXT,
                lat FLOAT,
                lng FLOAT,
                issue_date DATE,
                applied_date DATE,
                status TEXT,
                valuation FLOAT,
                sqft FLOAT,
                housing_units INTEGER,
                contractor_company TEXT,
                contractor_name TEXT,
                contractor_phone TEXT,
                contractor_address TEXT,
                contractor_city TEXT,
                contractor_zip TEXT,
                contractor_trade TEXT,
                applicant_name TEXT,
                applicant_org TEXT,
                applicant_phone TEXT,
                owner_name TEXT,
                jurisdiction TEXT,
                source TEXT NOT NULL,
                scraped_at DATE DEFAULT CURRENT_DATE
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_hot_leads_permit "
            "ON hot_leads (permit_number, source)"
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (hot_leads ensure: {e})")
    finally:
        cur.close()


def parse_date(v) -> Optional[date]:
    if v in (None, "", "NA"):
        return None
    # ArcGIS epoch ms
    if isinstance(v, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(v) / 1000.0).date()
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        try:
            return datetime.strptime(s, "%m/%d/%Y").date()
        except Exception:
            return None


def safe_float(v) -> Optional[float]:
    if v in (None, "", "NA"):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def extract_zip(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", address)
    return m.group(1) if m else None


# ───────────────────────── MGO fetch ─────────────────────────────────────
def mgo_authenticate():
    session = httpx.Client(timeout=30, follow_redirects=True)
    session.headers.update(
        {
            "accept": "application/json",
            "sourceplatform": "MGO Connect Web",
            "user-agent": "Mozilla/5.0",
            "referer": "https://www.mgoconnect.org/",
        }
    )
    body = "=" + urllib.parse.quote(
        json.dumps({"Email": MGO_EMAIL, "Password": MGO_PASSWORD})
    )
    resp = session.post(
        f"{MGO_LEGACY_API}/user/login/-",
        content=body,
        headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
    )
    data = resp.json()
    token = data.get("UserToken")
    if not token:
        raise RuntimeError(f"MGO login failed: {data}")
    log(f"  MGO auth OK (UserID {data.get('UserID')})")
    session.headers["authorization-token"] = token
    return session


def mgo_fetch(session, jurisdiction_id, jurisdiction_name, days=30):
    filters = {
        "JURISDICTIONID": jurisdiction_id,
        "PROJECTTYPEID": MGO_PERMIT_TYPE_ID,
    }
    if days:
        since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        filters["CREATEDATEAFTER"] = since

    all_items = []
    offset = 0
    while True:
        payload = {
            "filters": filters,
            "Rows": MGO_PAGE_SIZE,
            "OffSet": offset,
            "SortField": "dateCreated",
            "SortOrder": "desc",
        }
        for attempt in range(4):
            try:
                r = session.post(MGO_SEARCH, json=payload, timeout=30)
                if r.status_code == 200:
                    break
                if r.status_code in (429, 503):
                    wait = 5 * (2**attempt)
                    log(f"    {r.status_code} from MGO, retry in {wait}s")
                    time.sleep(wait)
                    continue
                log(f"    MGO API error {r.status_code} for {jurisdiction_name}")
                return all_items
            except Exception as e:
                log(f"    MGO fetch error: {e}")
                time.sleep(3)
        else:
            return all_items

        data = r.json()
        items = data.get("data", data.get("rows", []))
        if not items:
            break
        all_items.extend(items)
        if len(items) < MGO_PAGE_SIZE:
            break
        offset += MGO_PAGE_SIZE
        if offset >= 20000:
            log(f"    Hit 20K cap for {jurisdiction_name}")
            break
    return all_items


def mgo_normalize(raw, cfg):
    address = (
        raw.get("address") or raw.get("projectAddress") or raw.get("siteAddress") or ""
    ).strip()
    permit_number = (
        raw.get("projectUID") or raw.get("permitNumber") or raw.get("projectNumber") or ""
    ).strip()
    if not (address or permit_number):
        return None

    issue_date = parse_date(
        raw.get("dateCreated") or raw.get("createdDate") or raw.get("issueDate")
    )
    description = (
        raw.get("description")
        or raw.get("projectDescription")
        or raw.get("projectName")
        or ""
    ).strip()
    permit_type = (
        raw.get("projectTypeName") or raw.get("permitType") or raw.get("category")
    )
    work_class = raw.get("workClass") or raw.get("subType") or raw.get("subcategory")
    valuation = safe_float(
        raw.get("valuation") or raw.get("estimatedCost") or raw.get("projectValue")
    )
    contractor = raw.get("contractorName") or raw.get("applicantName")
    zip_code = raw.get("zip") or raw.get("zipCode") or extract_zip(address)

    return {
        "permit_number": (permit_number[:100]) or None,
        "permit_type": (str(permit_type)[:80]) if permit_type else None,
        "work_class": (str(work_class)[:120]) if work_class else None,
        "description": (description[:500]) if description else None,
        "address": (address[:200]) if address else None,
        "city": cfg["city_upper"],
        "state": "TX",
        "zip": str(zip_code)[:5] if zip_code else None,
        "county": cfg["county"],
        "issue_date": issue_date,
        "applied_date": issue_date,
        "status": raw.get("status") or raw.get("projectStatus"),
        "valuation": valuation,
        "contractor_name": (str(contractor)[:200]) if contractor else None,
        "applicant_name": (str(raw.get("applicantName") or "")[:200]) or None,
        "owner_name": (str(raw.get("ownerName") or "")[:200]) or None,
        "jurisdiction": f"{cfg['mgo_name']}, TX",
        "source": cfg["source"],
        "raw": raw,
    }


# ───────────────────────── ArcGIS fetch ──────────────────────────────────
def arcgis_fetch(url, date_field, days=30, session=None):
    """Query an ArcGIS FeatureServer layer for rows newer than `days`.

    Tries multiple WHERE syntaxes (TIMESTAMP, DATE, epoch-ms) since different
    layers use DateOnly vs Date vs esriFieldTypeDate.
    """
    session = session or httpx.Client(
        timeout=45, headers={"User-Agent": ARCGIS_UA}, follow_redirects=True
    )
    since_dt = datetime.utcnow() - timedelta(days=days)
    since_iso_date = since_dt.strftime("%Y-%m-%d")
    since_iso_ts = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    since_ms = int(since_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    # Try TIMESTAMP first (works for Date), then DATE (works for DateOnly), then epoch ms (numeric compare fallback)
    where_candidates = [
        f"{date_field} >= TIMESTAMP '{since_iso_ts}'",
        f"{date_field} >= DATE '{since_iso_date}'",
        f"{date_field} >= {since_ms}",
    ]
    working_where = None
    for cand in where_candidates:
        try:
            r = session.get(
                f"{url}/query",
                params={
                    "where": cand,
                    "returnCountOnly": "true",
                    "f": "json",
                },
                timeout=30,
            )
            j = r.json()
            if "error" not in j and "count" in j:
                working_where = cand
                break
        except Exception:
            continue

    if working_where is None:
        log(f"    ArcGIS: no compatible WHERE syntax worked for {date_field}")
        return []

    all_features = []
    offset = 0
    page_size = 2000
    while True:
        params = {
            "where": working_where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "orderByFields": f"{date_field} DESC",
            "returnGeometry": "false",
        }
        try:
            r = session.get(f"{url}/query", params=params, timeout=45)
            data = r.json()
        except Exception as e:
            log(f"    ArcGIS error: {e}")
            break
        if "error" in data:
            log(f"    ArcGIS returned error: {data.get('error')}")
            break
        feats = data.get("features", [])
        if not feats:
            break
        all_features.extend(feats)
        if len(feats) < page_size:
            break
        offset += page_size
        if offset >= 50000:
            break
    return all_features


def arcgis_normalize(feature, cfg):
    attrs = feature.get("attributes") or feature
    fm = cfg["field_map"]

    permit_number = attrs.get(fm.get("permit_number", "permit_number"))
    if not permit_number:
        return None
    permit_number = str(permit_number).strip()

    address = attrs.get(fm.get("address", "address"))
    address = str(address).strip() if address else None

    permit_type = attrs.get(fm.get("permit_type", "permit_type"))
    work_class = attrs.get(fm.get("work_class", "work_class")) if "work_class" in fm else None
    status = attrs.get(fm.get("status", "status")) if "status" in fm else None
    applied_date = parse_date(attrs.get(fm.get("applied_date", "applied_date"))) if "applied_date" in fm else None
    issue_date = parse_date(attrs.get(fm.get("issue_date", "issue_date"))) if "issue_date" in fm else None

    # Use whichever is present as primary date
    primary_date = issue_date or applied_date

    zip_code = extract_zip(address)

    return {
        "permit_number": permit_number[:100],
        "permit_type": (str(permit_type)[:80]) if permit_type else None,
        "work_class": (str(work_class)[:120]) if work_class else None,
        "description": None,
        "address": (address[:200]) if address else None,
        "city": cfg["city_upper"],
        "state": "TX",
        "zip": zip_code,
        "county": cfg["county"],
        "issue_date": issue_date,
        "applied_date": applied_date,
        "status": (str(status)[:80]) if status else None,
        "valuation": None,
        "contractor_name": None,
        "applicant_name": None,
        "owner_name": None,
        "jurisdiction": f"{cfg['city_upper'].title()}, TX",
        "source": cfg["source"],
        "raw": attrs,
    }


# ───────────────────────── DB upserts ────────────────────────────────────
HOT_LEADS_COLS = [
    "id", "permit_number", "permit_type", "work_class", "description",
    "address", "city", "state", "zip", "county",
    "issue_date", "applied_date", "status", "valuation",
    "contractor_name", "applicant_name", "owner_name",
    "jurisdiction", "source",
]


def upsert_hot_leads(conn, permits):
    if not permits:
        return 0
    rows = []
    for p in permits:
        if not p.get("permit_number"):
            continue
        rows.append(
            (
                str(uuid.uuid4()),
                p["permit_number"], p["permit_type"], p["work_class"], p["description"],
                p["address"], p["city"], p["state"], p["zip"], p["county"],
                p["issue_date"], p["applied_date"], p["status"], p["valuation"],
                p["contractor_name"], p["applicant_name"], p["owner_name"],
                p["jurisdiction"], p["source"],
            )
        )
    sql = f"""
        INSERT INTO hot_leads ({', '.join(HOT_LEADS_COLS)})
        VALUES %s
        ON CONFLICT (permit_number, source) DO UPDATE SET
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            work_class = COALESCE(EXCLUDED.work_class, hot_leads.work_class),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            city = COALESCE(EXCLUDED.city, hot_leads.city),
            zip = COALESCE(EXCLUDED.zip, hot_leads.zip),
            county = COALESCE(EXCLUDED.county, hot_leads.county),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            valuation = COALESCE(EXCLUDED.valuation, hot_leads.valuation),
            contractor_name = COALESCE(EXCLUDED.contractor_name, hot_leads.contractor_name),
            applicant_name = COALESCE(EXCLUDED.applicant_name, hot_leads.applicant_name),
            jurisdiction = COALESCE(EXCLUDED.jurisdiction, hot_leads.jurisdiction),
            scraped_at = CURRENT_DATE
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, page_size=500)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log(f"  hot_leads upsert error: {e}")
        return 0
    finally:
        cur.close()


PERMITS_TX_COLS = [
    "permit_number", "address", "city", "state_code", "zip_code", "county",
    "project_type", "work_type", "category", "description",
    "status", "date_created", "applicant_name", "owner_name",
    "source", "raw_data",
]


def upsert_permits_tx(conn, permits):
    if not permits:
        return 0
    rows = []
    for p in permits:
        if not p.get("permit_number") or not p.get("source"):
            continue
        date_created = None
        d = p["issue_date"] or p["applied_date"]
        if d:
            date_created = datetime.combine(d, datetime.min.time())
        rows.append(
            (
                p["permit_number"],
                p["address"],
                p["city"],
                "TX",
                p["zip"],
                p["county"],
                p["permit_type"],
                p["work_class"],
                p["permit_type"],
                p["description"],
                p["status"],
                date_created,
                p["applicant_name"],
                p["owner_name"],
                p["source"],
                Json(p.get("raw") or {}),
            )
        )
    sql = f"""
        INSERT INTO permits_tx ({', '.join(PERMITS_TX_COLS)})
        VALUES %s
        ON CONFLICT (source, permit_number)
        WHERE permit_number IS NOT NULL AND source IS NOT NULL
        DO UPDATE SET
            address = COALESCE(EXCLUDED.address, permits_tx.address),
            city = COALESCE(EXCLUDED.city, permits_tx.city),
            zip_code = COALESCE(EXCLUDED.zip_code, permits_tx.zip_code),
            county = COALESCE(EXCLUDED.county, permits_tx.county),
            project_type = COALESCE(EXCLUDED.project_type, permits_tx.project_type),
            work_type = COALESCE(EXCLUDED.work_type, permits_tx.work_type),
            category = COALESCE(EXCLUDED.category, permits_tx.category),
            description = COALESCE(EXCLUDED.description, permits_tx.description),
            status = COALESCE(EXCLUDED.status, permits_tx.status),
            date_created = COALESCE(EXCLUDED.date_created, permits_tx.date_created),
            applicant_name = COALESCE(EXCLUDED.applicant_name, permits_tx.applicant_name),
            owner_name = COALESCE(EXCLUDED.owner_name, permits_tx.owner_name),
            raw_data = EXCLUDED.raw_data,
            loaded_at = now()
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, page_size=500, template=None)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log(f"  permits_tx upsert error: {e}")
        return 0
    finally:
        cur.close()


# ───────────────────────── orchestration ─────────────────────────────────
def scrape_city(city_key, cfg, mgo_session, days):
    log(f"[{city_key}] platform={cfg['platform']} source={cfg['source']}")

    if cfg["platform"] == "mgo":
        raw_items = mgo_fetch(mgo_session, cfg["mgo_id"], cfg["mgo_name"], days=days)
        normalized = [mgo_normalize(r, cfg) for r in raw_items]
    elif cfg["platform"] == "arcgis":
        feats = arcgis_fetch(cfg["url"], cfg["date_field"], days=days)
        normalized = [arcgis_normalize(f, cfg) for f in feats]
    else:
        log(f"  unknown platform {cfg['platform']}")
        return []

    normalized = [p for p in normalized if p and p.get("permit_number")]
    log(f"  fetched {len(normalized)} permits")
    return normalized


def main():
    parser = argparse.ArgumentParser(
        description="Central Texas small cities permit scraper"
    )
    parser.add_argument(
        "--city",
        choices=list(CITIES.keys()) + list(SKIPPED.keys()),
        help="Single city to scrape",
    )
    parser.add_argument("--all", action="store_true", help="Scrape all supported cities")
    parser.add_argument("--days", type=int, default=30, help="Days of history (default 30)")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST)
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument(
        "--list", action="store_true", help="List supported and skipped cities"
    )
    args = parser.parse_args()

    if args.list:
        print("Supported cities:")
        for k, v in CITIES.items():
            print(f"  {k:20s} platform={v['platform']:6s} source={v['source']}")
        print("\nSkipped (needs browser automation):")
        for k, reason in SKIPPED.items():
            print(f"  {k:20s} — {reason}")
        return

    if not args.city and not args.all:
        parser.error("must provide --city or --all (or --list)")

    if args.city in SKIPPED:
        log(f"SKIPPED: {args.city} — {SKIPPED[args.city]}")
        sys.exit(2)

    targets = [args.city] if args.city else list(CITIES.keys())

    log("=" * 60)
    log(f"CTX SMALL CITIES SCRAPER — {len(targets)} city/cities, days={args.days}")
    log(f"Targets: {', '.join(targets)}")
    if SKIPPED:
        log(f"Skipped (browser needed): {', '.join(SKIPPED.keys())}")
    log("=" * 60)

    needs_mgo = any(CITIES[c]["platform"] == "mgo" for c in targets)
    mgo_session = None
    if needs_mgo:
        mgo_session = mgo_authenticate()

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)
        ensure_hot_leads(conn)
        ensure_permits_tx_unique(conn)
        log(f"DB connected: {args.db_host}/{DB_NAME}")

    summary = []
    for city_key in targets:
        cfg = CITIES[city_key]
        try:
            permits = scrape_city(city_key, cfg, mgo_session, days=args.days)
        except Exception as e:
            log(f"  ERROR scraping {city_key}: {e}")
            summary.append((city_key, 0, 0, 0))
            continue

        hl = pt = 0
        if not args.dry_run and permits:
            hl = upsert_hot_leads(conn, permits)
            pt = upsert_permits_tx(conn, permits)

        summary.append((city_key, len(permits), hl, pt))
        log(f"  → {city_key}: fetched={len(permits)} hot_leads={hl} permits_tx={pt}")
        time.sleep(2)  # polite pacing

    log("=" * 60)
    log(f"{'CITY':<20} {'FETCHED':>10} {'HOT_LEADS':>12} {'PERMITS_TX':>12}")
    for city_key, f, h, p in summary:
        log(f"{city_key:<20} {f:>10} {h:>12} {p:>12}")
    log("=" * 60)

    total_fetched = sum(s[1] for s in summary)
    total_hl = sum(s[2] for s in summary)
    log(f"TOTAL fetched={total_fetched} hot_leads_upserted={total_hl}")

    if conn:
        conn.close()
    if mgo_session:
        mgo_session.close()
    log("Done.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""EBR (East Baton Rouge Parish) per-property year_built / sqft scraper.

Fills the EAST BATON ROUGE year_built / building_sqft gap in tx_cad_parcels
(cad_source='EBRPA'). The bulk BRLA cadastral feed (maps.brla.gov Cadastral/
Tax_Parcel) carries owner / situs / value but NO improvement detail. That detail
lives only in the parish ASSESSOR's CAMA system (SmartCAMA):

    https://eastbatonrouge.smartcama.com/Assessments/Search   (form)

--------------------------------------------------------------------------------
HOW THE SITE ACTUALLY WORKS  (verified live 2026-06-28)
--------------------------------------------------------------------------------
1. reCAPTCHA **Enterprise v2 CHECKBOX** gates searching. site key
   6Le9Gb8eAAAAADRJkJ3Lt5aryjSL8dj8wQKVc7gm, page /Assessments/Search.
   On solve the page calls  POST /Captcha/VerifyCaptcha?token=<g-recaptcha-response>
   (antiforgery in body) which marks the SESSION captcha-valid server-side; a
   GET /Captcha/IsValid returns "true" thereafter. ONE solved token validates the
   session for many subsequent searches (we re-solve only when IsValid flips to
   false / a search 302-bounces). This makes 2Captcha cost ~$0.0018/solve and a
   tiny fraction of a cent per parcel.

2. Our DB parcel_id (e.g. "000-0004-3") is the GIS/cadastral parcel number, NOT
   the assessor AssessmentNumber (which is a short int like "43" / "2700514*").
   So we search by **situs address** (PhysicalStreetNumber + PhysicalStreetName)
   via  POST /Assessments/SearchAjax  (a DataTables payload) and address-match
   the returned PhysicalAddress back to our parcel to get its assessor row Id.

3. year_built / sqft are NOT in the search list. They live in the per-assessment
   detail JSON:  POST /Assessments/FetchAssessment?Id=<id>  ->
   TaxItems[].WorkItems[].ConstructionDate / DepreciationYear / EffectiveSqft.

SOURCE DATA CAVEAT (measured): EBR CAMA populates ConstructionDate sparsely.
Parish-wide ~28% of parcels carry year_built (~48% carry sqft); the older inner-
Baton-Rouge storm-lead set (ZIP 70802-70806) is worse (~0% year_built, ~15% sqft)
because the assessor never recorded construction year for those. We fill what the
source has and record the rest as 'no_year' so we don't re-hit them.

--------------------------------------------------------------------------------
CAPTCHA TOKEN PROVIDER (required)
--------------------------------------------------------------------------------
  --token-cmd "<cmd that prints a fresh g-recaptcha-response>"
      e.g.  --token-cmd "python3 scripts/ebr_captcha_token.py"   (2Captcha solver).
      Called the FIRST time and whenever the session captcha goes invalid.
  --token-file <path>     a human-pasted token, re-read on invalidation.
Env equivalents: EBR_TOKEN_CMD / EBR_TOKEN_FILE. With NO provider the scraper does
a reachability probe and exits 4 with instructions (never silently no-ops).

--------------------------------------------------------------------------------
DB write  (same safety contract as scrape_bcad_improvements.py)
--------------------------------------------------------------------------------
Idempotent keyed UPDATE of the existing EBRPA row (cad_source, parcel_id,
tax_year). lock_timeout per write; NEVER pg_terminate / pg_cancel; no full scans
(targets come from an indexed predicate, each UPDATE hits one keyed row).
Resumable via checkpoint table ebr_improvement_progress. Raw JSON optionally
staged to --raw-dir (MUST NOT be /home/will, per Storage Policy).

Priority: lead-set parcels (unserviced_hail_leads EBR arm + --seed-csv) FIRST,
then the rest of the parish, resumable.

Usage (on R730-2, which can reach smartcama + the T430 DB):
  python3 scrape_ebr_improvements.py --token-cmd "python3 scripts/ebr_captcha_token.py"
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    raise

BASE = "https://eastbatonrouge.smartcama.com"
SEARCH_PAGE = BASE + "/Assessments/Search"
SEARCH_AJAX = BASE + "/Assessments/SearchAjax"
VERIFY_CAPTCHA = BASE + "/Captcha/VerifyCaptcha"
IS_VALID = BASE + "/Captcha/IsValid"
FETCH_ASSESSMENT = BASE + "/Assessments/FetchAssessment"
CAD = "EBRPA"
DEFAULT_YEAR = 2026

DB_HOST_DEFAULT = os.environ.get("PGHOST", "100.122.216.15")
DB_NAME_DEFAULT = os.environ.get("PGDATABASE", "permits")
DB_USER_DEFAULT = os.environ.get("PGUSER", "will")
DB_PORT_DEFAULT = int(os.environ.get("PGPORT", "5432"))

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 (ecbtx ebr improvement enrich)")

# DataTables column scaffold the SearchAjax endpoint requires. The server filters
# the physical-address column by its server-side NAME, so columns 3 and 13 carry
# the exact .NET property names captured from the live request.
_COLS = [
    "Select", "Action", "MappingNumber", "AssessmentNumber", "MobileHomeParkName",
    "MappingNumber", "Lot", "Block", "LastOrBusiness", "First", "HomesteadStatus",
    "LegalDescription", "MailingAddress", "PhysicalAddress", "City", "State",
    "ZipCode", "Country", "Roll", "Location", "PersonalPropertyNoteNaicsCode",
    "IsExactNameMatch", "OwnershipPercent",
]
_COL_NAMES = {3: "Assessment.AssessmentNumber", 13: "Assessment.FullPhysicalAddress"}


def _datatable_fields() -> dict:
    f = {}
    for i, c in enumerate(_COLS):
        f[f"DataTableRequest[columns][{i}][data]"] = c
        f[f"DataTableRequest[columns][{i}][name]"] = _COL_NAMES.get(i, c)
        f[f"DataTableRequest[columns][{i}][searchable]"] = "true"
        f[f"DataTableRequest[columns][{i}][orderable]"] = "true"
        f[f"DataTableRequest[columns][{i}][search][value]"] = ""
        f[f"DataTableRequest[columns][{i}][search][regex]"] = "false"
    f["DataTableRequest[order][0][column]"] = "3"
    f["DataTableRequest[order][0][dir]"] = "asc"
    f["DataTableRequest[start]"] = "0"
    f["DataTableRequest[length]"] = "25"
    f["DataTableRequest[search][value]"] = ""
    f["DataTableRequest[search][regex]"] = "false"
    f["DataTableRequest[draw]"] = "1"
    return f


# ---- address parse / match -----------------------------------------------

def split_situs(situs: str):
    """'1958 WISTERIA ST' -> ('1958', 'WISTERIA ST'). Returns (num, rest)."""
    if not situs:
        return None, None
    m = re.match(r"\s*(\d+[A-Za-z]?)\s+(.*?)\s*$", situs)
    if not m:
        return None, situs.strip()
    return m.group(1), m.group(2).strip()


def _norm(a: str) -> str:
    return re.sub(r"[^A-Z0-9 ]", "", (a or "").upper()).strip()


def match_row(situs: str, rows: list) -> dict | None:
    """Pick the assessor row whose PhysicalAddress matches our situs."""
    num, rest = split_situs(situs)
    want = _norm(f"{num or ''} {rest or ''}")
    want_house = _norm(num or "")
    for r in rows:
        pa = _norm(r.get("PhysicalAddress") or "")
        if pa.startswith(want):
            return r
    # looser: same house number and street first word present
    sw = (_norm(rest or "").split() or [""])[0]
    for r in rows:
        pa = _norm(r.get("PhysicalAddress") or "")
        if want_house and pa.startswith(want_house + " ") and sw and sw in pa:
            return r
    return None


# ---- detail JSON extraction ----------------------------------------------

def extract_detail(d: dict) -> dict:
    """Min ConstructionDate-year (fallback DepreciationYear) + SUM EffectiveSqft
    across improvement WorkItems."""
    years, areas = [], []

    def walk(o):
        if isinstance(o, dict):
            cd = o.get("ConstructionDate")
            if isinstance(cd, str):
                m = re.search(r"(\d{4})", cd)
                if m:
                    years.append(int(m.group(1)))
            dy = o.get("DepreciationYear")
            if isinstance(dy, int) and 1700 < dy < 2100:
                years.append(dy)
            es = o.get("EffectiveSqft")
            if isinstance(es, (int, float)) and es > 100:
                areas.append(float(es))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(d)
    years = [y for y in years if 1700 < y < 2100]
    return {
        "year_built": min(years) if years else None,
        "building_sqft": round(sum(areas), 1) if areas else None,
    }


# ---- captcha session ------------------------------------------------------

class TokenProvider:
    def __init__(self, token_cmd: str | None, token_file: str | None):
        self.token_cmd = token_cmd
        self.token_file = token_file

    @property
    def configured(self) -> bool:
        return bool(self.token_cmd or self.token_file)

    def fresh(self) -> str | None:
        if self.token_cmd:
            try:
                return subprocess.check_output(
                    self.token_cmd, shell=True, text=True, timeout=300).strip() or None
            except Exception as e:  # noqa: BLE001
                print(f"[ebr] token-cmd failed: {e}", file=sys.stderr)
                return None
        if self.token_file:
            try:
                with open(self.token_file) as fh:
                    return fh.read().strip() or None
            except Exception as e:  # noqa: BLE001
                print(f"[ebr] token-file read failed: {e}", file=sys.stderr)
        return None


def _antiforgery(sess) -> str | None:
    try:
        r = sess.get(SEARCH_PAGE, timeout=30)
        m = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
        return m.group(1) if m else None
    except Exception:  # noqa: BLE001
        return None


class Session:
    """SmartCAMA session that keeps itself captcha-valid via VerifyCaptcha."""

    def __init__(self, provider: TokenProvider):
        self.provider = provider
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA, "Referer": SEARCH_PAGE})
        self.af: str | None = None
        self.solves = 0
        self._prime()

    def _prime(self):
        self.sess.cookies.clear()
        r = self.sess.get(BASE + "/", timeout=30)
        m = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
        self.af = m.group(1) if m else None

    def is_valid(self) -> bool:
        try:
            r = self.sess.get(IS_VALID, timeout=20,
                              headers={"X-Requested-With": "XMLHttpRequest"})
            return r.status_code == 200 and "true" in r.text.lower()
        except Exception:  # noqa: BLE001
            return False

    def ensure_valid(self) -> bool:
        """Solve + VerifyCaptcha if the session isn't captcha-valid. Returns
        True if valid afterwards."""
        if self.is_valid():
            return True
        token = self.provider.fresh()
        if not token:
            return False
        self.solves += 1
        try:
            if not self.af:
                self.af = _antiforgery(self.sess)
            vr = self.sess.post(
                VERIFY_CAPTCHA, params={"token": token},
                data={"__RequestVerificationToken": self.af or ""},
                headers={"X-Requested-With": "XMLHttpRequest"}, timeout=40)
        except Exception as e:  # noqa: BLE001
            print(f"[ebr] VerifyCaptcha error: {e}", file=sys.stderr)
            return False
        ok = vr.status_code == 200 and "true" in vr.text.lower()
        if not ok:
            print(f"[ebr] VerifyCaptcha rejected ({vr.status_code} "
                  f"{vr.text[:60]!r})", file=sys.stderr)
        return ok and self.is_valid()

    def search(self, situs: str) -> list:
        num, rest = split_situs(situs)
        if not rest:
            return []
        self.af = _antiforgery(self.sess) or self.af
        fields = {
            "__RequestVerificationToken": self.af or "",
            "AssessmentNumber": "",
            "PhysicalStreetNumber": num or "",
            "PhysicalStreetName": rest,
            "PerformSearch": "true",
            "InitialSearch": "true",
        }
        fields.update(_datatable_fields())
        r = self.sess.post(
            SEARCH_AJAX, data=fields, timeout=40, allow_redirects=False,
            headers={"X-Requested-With": "XMLHttpRequest"})
        if r.status_code in (301, 302, 303):
            raise _CaptchaExpired()
        if r.status_code != 200:
            raise _HttpErr(r.status_code)
        try:
            return r.json().get("Data", {}).get("data", []) or []
        except Exception:  # noqa: BLE001
            return []

    def fetch_detail(self, row_id) -> dict:
        r = self.sess.post(
            FETCH_ASSESSMENT, params={"Id": row_id, "PriorYear": ""},
            data={"__RequestVerificationToken": self.af or ""},
            headers={"X-Requested-With": "XMLHttpRequest"}, timeout=40,
            allow_redirects=False)
        if r.status_code in (301, 302, 303):
            raise _CaptchaExpired()
        if r.status_code != 200:
            raise _HttpErr(r.status_code)
        try:
            return r.json()
        except Exception:  # noqa: BLE001
            return {}


class _CaptchaExpired(Exception):
    pass


class _HttpErr(Exception):
    def __init__(self, code):
        self.code = code


# ---- db ------------------------------------------------------------------

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS public.ebr_improvement_progress (
    parcel_id    text PRIMARY KEY,
    tax_year     integer NOT NULL,
    outcome      text NOT NULL,
    year_built   integer,
    building_sqft numeric,
    scraped_at   timestamptz NOT NULL DEFAULT now()
);
"""

UPDATE_SQL = """
UPDATE public.tx_cad_parcels
   SET year_built    = COALESCE(%(year_built)s, year_built),
       building_sqft = COALESCE(%(building_sqft)s, building_sqft),
       raw = COALESCE(raw, '{}'::jsonb)
             || jsonb_build_object('ebr_improvements', %(detail)s::jsonb)
 WHERE cad_source = %(cad)s
   AND parcel_id  = %(parcel_id)s
   AND tax_year   = %(tax_year)s
"""

PROGRESS_UPSERT = """
INSERT INTO public.ebr_improvement_progress
    (parcel_id, tax_year, outcome, year_built, building_sqft)
VALUES %s
ON CONFLICT (parcel_id) DO UPDATE
   SET tax_year=EXCLUDED.tax_year, outcome=EXCLUDED.outcome,
       year_built=EXCLUDED.year_built, building_sqft=EXCLUDED.building_sqft,
       scraped_at=now()
"""

# Priority = EBR arm of unserviced_hail_leads still lacking year_built, WITH situs.
PRIORITY_SQL = """
SELECT DISTINCT u.parcel_id, p.situs_address
  FROM public.unserviced_hail_leads u
  JOIN public.tx_cad_parcels p
    ON p.parcel_id = u.parcel_id
   AND p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
 WHERE u.county_source = 'EBR'
   AND p.year_built IS NULL
   AND p.situs_address ~ '^[0-9]'
   AND NOT EXISTS (
        SELECT 1 FROM public.ebr_improvement_progress g
         WHERE g.parcel_id = p.parcel_id)
"""

ALL_SQL = """
SELECT p.parcel_id, p.situs_address
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
   AND p.year_built IS NULL
   AND p.situs_address ~ '^[0-9]'
   AND NOT EXISTS (
        SELECT 1 FROM public.ebr_improvement_progress g
         WHERE g.parcel_id = p.parcel_id)
 ORDER BY p.parcel_id
"""

SEED_SITUS_SQL = """
SELECT parcel_id, situs_address
  FROM public.tx_cad_parcels
 WHERE cad_source = %(cad)s AND tax_year = %(tax_year)s
   AND parcel_id = ANY(%(ids)s)
"""


_KEEPALIVE = dict(keepalives=1, keepalives_idle=20, keepalives_interval=10,
                  keepalives_count=5)


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn, connect_timeout=20, **_KEEPALIVE)
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user,
        connect_timeout=20, **_KEEPALIVE)


def flush_writes(args, updates, prog_rows):
    """Open a SHORT-LIVED connection, apply keyed UPDATEs + progress upserts, then
    close. Opening per-flush avoids the idle-session drop that some network paths
    (CF tunnel) impose during the long captcha solves between flushes. Retries
    once on a dropped connection. Returns (n_update_ok, n_lock_skip).

    Safety contract preserved: per-statement lock_timeout, single keyed-row
    UPDATEs, NEVER pg_terminate / pg_cancel, no full scans."""
    n_ok = n_lock = 0
    for attempt in (1, 2):
        try:
            conn = get_conn(args)
            conn.autocommit = False
            cur = conn.cursor()
            for up in updates:
                cur.execute(f"SET LOCAL lock_timeout = '{args.lock_timeout}'")
                try:
                    cur.execute(UPDATE_SQL, up)
                    n_ok += 1
                except psycopg2.errors.LockNotAvailable:
                    conn.rollback()
                    n_lock += 1
                    # downgrade this row's progress outcome to lock_skip
                    for i, r in enumerate(prog_rows):
                        if r[0] == up["parcel_id"]:
                            prog_rows[i] = (r[0], r[1], "lock_skip", r[3], r[4])
            if prog_rows:
                execute_values(cur, PROGRESS_UPSERT, prog_rows)
            conn.commit()
            cur.close()
            conn.close()
            return n_ok, n_lock
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"[ebr] DB flush connection lost ({e}); retry {attempt}",
                  flush=True)
            n_ok = n_lock = 0
            time.sleep(1.0)
        except Exception as e:  # noqa: BLE001
            print(f"[ebr] db flush err: {e}", flush=True)
            return n_ok, n_lock
    print("[ebr] DB flush failed after retry — progress for this batch lost",
          flush=True)
    return n_ok, n_lock


def load_seed_csv(path: str) -> list[str]:
    """Optional seed list of parcel_id (one per line, or a CSV with a parcel_id /
    assessment column). Addresses are resolved from the DB."""
    if not path or not os.path.exists(path):
        return []
    out, import_csv = [], __import__("csv")
    with open(path, newline="") as fh:
        sniff = fh.read(2048)
        fh.seek(0)
        if "," in sniff and ("parcel" in sniff.lower() or "assessment" in sniff.lower()):
            rd = import_csv.DictReader(fh)
            key = next((k for k in (rd.fieldnames or [])
                        if k and ("parcel" in k.lower() or "assessment" in k.lower())), None)
            if key:
                for r in rd:
                    v = (r.get(key) or "").strip()
                    if v:
                        out.append(v)
        else:
            for line in fh:
                v = line.strip()
                if v:
                    out.append(v)
    return out


def load_targets(conn, args) -> list[tuple[str, str]]:
    """Return ordered list of (parcel_id, situs_address)."""
    cur = conn.cursor()
    cur.execute(PROGRESS_DDL)
    conn.commit()
    params = {"cad": CAD, "tax_year": args.tax_year}
    targets: list[tuple[str, str]] = []
    seen: set[str] = set()

    seed_ids = [s for s in load_seed_csv(args.seed_csv) if s]
    if seed_ids:
        cur.execute(SEED_SITUS_SQL, {**params, "ids": seed_ids})
        rows = cur.fetchall()
        cur.execute("SELECT parcel_id FROM public.ebr_improvement_progress "
                    "WHERE parcel_id = ANY(%s)", ([r[0] for r in rows],))
        done = {r[0] for r in cur.fetchall()}
        for pid, situs in rows:
            if pid not in seen and pid not in done and situs:
                seen.add(pid)
                targets.append((pid, situs))
        print(f"[seed] {len(targets)} seed parcels pending", flush=True)

    if not args.full_only:
        try:
            cur.execute(PRIORITY_SQL, params)
            for pid, situs in cur.fetchall():
                if pid not in seen and situs:
                    seen.add(pid)
                    targets.append((pid, situs))
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            print(f"[priority] skipped ({e})", flush=True)
        print(f"[priority] {len(targets)} lead+seed parcels pending", flush=True)

    if not args.priority_only:
        cur.execute(ALL_SQL, params)
        for pid, situs in cur.fetchall():
            if pid not in seen and situs:
                seen.add(pid)
                targets.append((pid, situs))
    cur.close()
    return targets


# ---- scrape loop ---------------------------------------------------------

_STOP = False


def _sig(_s, _f):
    global _STOP
    _STOP = True
    print("[signal] stopping after current parcel...", flush=True)


def process_one(session: Session, situs: str) -> tuple[str, dict, dict]:
    """Return (outcome, parsed, raw_detail). outcome in
    enriched|no_year|no_match|no_improvement|captcha|http_err."""
    if not session.ensure_valid():
        return "captcha", {"year_built": None, "building_sqft": None}, {}
    try:
        rows = session.search(situs)
    except _CaptchaExpired:
        session.ensure_valid()
        return "captcha", {"year_built": None, "building_sqft": None}, {}
    except _HttpErr as e:
        return f"http_{e.code}", {"year_built": None, "building_sqft": None}, {}
    if not rows:
        return "no_match", {"year_built": None, "building_sqft": None}, {}
    row = match_row(situs, rows)
    if not row:
        return "no_match", {"year_built": None, "building_sqft": None}, {}
    try:
        detail = session.fetch_detail(row["Id"])
    except _CaptchaExpired:
        session.ensure_valid()
        return "captcha", {"year_built": None, "building_sqft": None}, {}
    except _HttpErr as e:
        return f"http_{e.code}", {"year_built": None, "building_sqft": None}, {}
    parsed = extract_detail(detail)
    if parsed["year_built"]:
        return "enriched", parsed, detail
    if parsed["building_sqft"]:
        return "no_year", parsed, detail   # has sqft, source lacks year
    return "no_improvement", parsed, detail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("EBRPA_DSN",
                    os.environ.get("PERMITS_DSN", "")))
    ap.add_argument("--host", default=DB_HOST_DEFAULT)
    ap.add_argument("--db", default=DB_NAME_DEFAULT)
    ap.add_argument("--user", default=DB_USER_DEFAULT)
    ap.add_argument("--port", type=int, default=DB_PORT_DEFAULT)
    ap.add_argument("--tax-year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--rate", type=float, default=1.5,
                    help="max requests/sec (default 1.5, gentle)")
    ap.add_argument("--priority-only", action="store_true")
    ap.add_argument("--full-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--lock-timeout", default="5s")
    ap.add_argument("--seed-csv", default="",
                    help="file of parcel_id to do first (situs resolved from DB)")
    ap.add_argument("--token-cmd", default=os.environ.get("EBR_TOKEN_CMD", ""),
                    help="shell cmd that prints a fresh g-recaptcha-response")
    ap.add_argument("--token-file", default=os.environ.get("EBR_TOKEN_FILE", ""),
                    help="file containing a fresh g-recaptcha-response token")
    ap.add_argument("--raw-dir", default="",
                    help="optional dir to stage raw detail JSON (NOT /home/will)")
    ap.add_argument("--probe-only", action="store_true",
                    help="check reachability + captcha gate, then exit")
    args = ap.parse_args()

    if args.raw_dir and args.raw_dir.startswith("/home/will"):
        print("REFUSING: --raw-dir on home drive violates Storage Policy",
              file=sys.stderr)
        sys.exit(3)
    if args.raw_dir:
        os.makedirs(args.raw_dir, exist_ok=True)

    provider = TokenProvider(args.token_cmd or None, args.token_file or None)

    try:
        probe = requests.get(SEARCH_PAGE, headers={"User-Agent": UA}, timeout=30,
                             allow_redirects=True)
        reachable = probe.status_code == 200
    except Exception as e:  # noqa: BLE001
        reachable = False
        print(f"[ebr] UNREACHABLE: {e}", file=sys.stderr)
    print(f"[ebr] search page reachable={reachable} "
          f"token_provider={'yes' if provider.configured else 'NONE'}", flush=True)

    if args.probe_only:
        sys.exit(0 if reachable else 2)

    if not provider.configured:
        print(
            "\n[ebr] NO CAPTCHA TOKEN PROVIDER CONFIGURED — cannot scrape.\n"
            "  SmartCAMA searching is gated by reCAPTCHA Enterprise v2 (checkbox).\n"
            "  Provide one of:\n"
            "    --token-cmd 'python3 scripts/ebr_captcha_token.py'\n"
            "    --token-file <path with a freshly-solved token>\n"
            "  (env EBR_TOKEN_CMD / EBR_TOKEN_FILE also work.)\n", file=sys.stderr)
        sys.exit(4)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    conn = get_conn(args)
    conn.autocommit = False
    targets = load_targets(conn, args)
    conn.close()  # don't hold an idle connection across the scrape loop
    if args.limit:
        targets = targets[: args.limit]
    print(f"[ebr] {len(targets):,} parcels to scrape (rate<= {args.rate}/s)",
          flush=True)

    session = Session(provider)
    if not session.ensure_valid():
        print("[ebr] could not obtain a valid captcha session — token provider "
              "may be down. Exiting.", file=sys.stderr)
        sys.exit(4)

    min_interval = 1.0 / args.rate if args.rate > 0 else 0
    t0 = time.time()
    enriched = sqft_only = no_match = no_imp = errs = captcha_skips = 0
    prog_batch: list[tuple] = []     # progress upsert rows
    update_batch: list[dict] = []    # UPDATE params for enriched/no_year
    last_req = 0.0
    i = 0

    def flush():
        nonlocal prog_batch, update_batch
        if not prog_batch and not update_batch:
            return
        flush_writes(args, update_batch, prog_batch)
        prog_batch = []
        update_batch = []

    for i, (pid, situs) in enumerate(targets, 1):
        if _STOP:
            break
        dt = time.time() - last_req
        if dt < min_interval:
            time.sleep(min_interval - dt)
        last_req = time.time()

        outcome, parsed, detail = process_one(session, situs)

        if outcome == "captcha":
            captcha_skips += 1
            if not session.ensure_valid():
                print("[ebr] token provider exhausted — stopping", flush=True)
                prog_batch.append((pid, args.tax_year, "captcha", None, None))
                break
            # retry this parcel once now that the session is valid
            outcome, parsed, detail = process_one(session, situs)

        if outcome in ("enriched", "no_year"):
            if args.raw_dir and detail:
                try:
                    with open(os.path.join(args.raw_dir, f"{pid}.json"), "w") as fh:
                        json.dump(detail, fh)
                except OSError:
                    pass
            detail_json = json.dumps({
                "year_built": parsed["year_built"],
                "building_sqft": parsed["building_sqft"],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            update_batch.append({
                "year_built": parsed["year_built"],
                "building_sqft": parsed["building_sqft"],
                "detail": detail_json, "cad": CAD,
                "parcel_id": pid, "tax_year": args.tax_year})
            if outcome == "enriched":
                enriched += 1
            else:
                sqft_only += 1
            prog_batch.append((pid, args.tax_year, outcome,
                               parsed["year_built"], parsed["building_sqft"]))
        else:
            if outcome == "no_match":
                no_match += 1
            elif outcome == "no_improvement":
                no_imp += 1
            elif outcome == "captcha":
                captcha_skips += 1
            else:
                errs += 1
            prog_batch.append((pid, args.tax_year, outcome,
                               parsed["year_built"], parsed["building_sqft"]))

        if len(prog_batch) >= 25:
            flush()

        if i % 100 == 0 or i <= 5:
            el = time.time() - t0
            rate = i / el if el else 0
            eta = (len(targets) - i) / rate / 60 if rate else 0
            print(f"[ebr] {i:,}/{len(targets):,} yb={enriched:,} sqft_only={sqft_only:,} "
                  f"no_match={no_match:,} no_imp={no_imp:,} captcha={captcha_skips} "
                  f"err={errs} solves={session.solves} {rate:.2f}/s eta={eta:.0f}m",
                  flush=True)

    flush()
    el = (time.time() - t0) / 60
    print(f"[ebr] DONE processed={i:,} yb={enriched:,} sqft_only={sqft_only:,} "
          f"no_match={no_match:,} no_imp={no_imp:,} captcha={captcha_skips} "
          f"err={errs} solves={session.solves} in {el:.1f}m", flush=True)


if __name__ == "__main__":
    main()

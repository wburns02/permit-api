#!/usr/bin/env python3
"""EBR (East Baton Rouge Parish) per-property year_built / sqft scraper.

Fills the EAST BATON ROUGE year_built / building_sqft gap in tx_cad_parcels
(cad_source='EBRPA'). The bulk BRLA cadastral feed
(maps.brla.gov/.../Cadastral/Tax_Parcel) carries owner / situs / SUM_* value but
NO improvement detail (no year built, no living area). That detail lives only in
the parish ASSESSOR's CAMA system:

    https://eastbatonrouge.smartcama.com/Assessments/Search   (form)
    POST https://eastbatonrouge.smartcama.com/Assessments/SearchAjax   (data)

Search is keyed by "Assessment No." which is EXACTLY our parcel_id
(tx_cad_parcels.parcel_id for cad_source='EBRPA' == ASSESSMENT_NUM, format
NNN-NNNN-N). So no extra ID mapping is needed: parcel_id IS the assessor key.

--------------------------------------------------------------------------------
CAPTCHA  (this is why this scraper does NOT auto-run as a systemd service)
--------------------------------------------------------------------------------
SmartCAMA guards /Assessments/SearchAjax with reCAPTCHA ENTERPRISE. Unlike the
Brazoria BCAD esearch viewer (whose reCAPTCHA only guarded the SEARCH FORM, not
the direct GetImprovements AJAX route), here the DATA endpoint itself 302-bounces
to an image-grid challenge ("select all images with bicycles") on every search
when no fresh g-recaptcha-response token is present. Verified 2026-06-28: the
checkbox does NOT pass frictionlessly for an automated browser; an image
challenge is presented each time. There is NO open backdoor: every public EBRGIS
/ geoportalmaps ArcGIS layer either lacks year_built, has ~0.9% fill
(Cadastral/Building_Footprint), or is an 18K-row demo subset on the wrong key
(TaxParcels_CAMA, ArcGIS Online org ue9rwulIoeLEI9bj).

Therefore this scraper requires a CAPTCHA TOKEN PROVIDER. Two are supported:

  --token-cmd "<shell command>"   prints a fresh g-recaptcha-response token to
                                  stdout (e.g. a 2captcha/anti-captcha solver, or
                                  a human-in-the-loop helper). Called per search
                                  batch; the token is reused until SearchAjax 302s
                                  again, then a new token is requested.

  --token-file <path>            reads a token from a file (a human pastes a
                                  freshly-solved token; the scraper re-reads on
                                  302). Lowest-tech manual mode.

With NO provider the scraper does a single reachability probe and EXITS non-zero
with the exact instructions — it never silently no-ops.

--------------------------------------------------------------------------------
DB write  (identical safety contract to scrape_bcad_improvements.py)
--------------------------------------------------------------------------------
Idempotent UPDATE of the existing EBRPA row keyed by (cad_source, parcel_id,
tax_year). lock_timeout set per-statement; NEVER calls pg_terminate / pg_cancel;
no full-table scans (each UPDATE hits a single keyed row). Resumable via a
checkpoint table ebr_improvement_progress. Raw JSON optionally staged to
--raw-dir (MUST NOT be /home/will — Storage Policy).

Priority ordering (matches Brazoria): the EBR lead-set parcels are scraped FIRST
(the unserviced_hail_leads EBR arm + the postcard-poc Baton Rouge skiptraced
leads), then the rest of the parish, resumable.

Usage (on R730-2, which can reach smartcama + the T430 DB):
  # priority lead parcels first, manual captcha paste:
  python3 scrape_ebr_improvements.py --priority-only --token-file /tmp/ebr.tok
  # whole parish via an automated solver:
  python3 scrape_ebr_improvements.py --token-cmd "python3 solve_recaptcha.py"
"""
from __future__ import annotations

import argparse
import html as _html
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
CAD = "EBRPA"
DEFAULT_YEAR = 2026

DB_HOST_DEFAULT = os.environ.get("PGHOST", "100.122.216.15")
DB_NAME_DEFAULT = os.environ.get("PGDATABASE", "permits")
DB_USER_DEFAULT = os.environ.get("PGUSER", "will")
DB_PORT_DEFAULT = int(os.environ.get("PGPORT", "5432"))

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 (ecbtx ebr improvement enrich)")

# ---- parsing -------------------------------------------------------------
# SearchAjax returns the SmartCAMA assessment edit view (HTML / JSON-wrapped
# HTML). year_built lives in the residential "Year Built*" input; living area in
# the improvement panels. We mirror the BCAD parser shape: tolerant regex over
# whatever markup comes back, taking MIN plausible year + SUM finished area.
YEAR_INPUT = re.compile(
    r'name=["\']YearBuilt["\'][^>]*value=["\'](\d{3,4})["\']', re.I)
YEAR_LABEL = re.compile(
    r'Year Built[^0-9]{0,40}?(\d{4})', re.I)
LIVING_AREA = re.compile(
    r'(?:Living Area|Finished Area|Total Living|Heated Area)'
    r'[^0-9]{0,40}?([\d,]+)\s*(?:sq|sf)?', re.I)
BLDG_SQFT = re.compile(
    r'(?:Building Square Feet|Total Area|Gross Area)[^0-9]{0,40}?([\d,]+)', re.I)


def _num(x):
    if x is None:
        return None
    x = str(x).replace(",", "").strip()
    if not x:
        return None
    try:
        return float(x)
    except ValueError:
        return None


def parse_assessment(body: str) -> dict:
    out = {"year_built": None, "building_sqft": None}
    if not body:
        return out
    years = []
    for m in YEAR_INPUT.finditer(body):
        years.append(int(m.group(1)))
    for m in YEAR_LABEL.finditer(body):
        years.append(int(m.group(1)))
    years = [y for y in years if 1700 < y < 2100]
    if years:
        out["year_built"] = min(years)

    areas = [_num(m.group(1)) for m in LIVING_AREA.finditer(body)]
    areas = [a for a in areas if a and a > 100]
    if areas:
        out["building_sqft"] = round(sum(areas), 1)
    else:
        bs = [_num(m.group(1)) for m in BLDG_SQFT.finditer(body)]
        bs = [a for a in bs if a and a > 100]
        if bs:
            out["building_sqft"] = round(sum(bs), 1)
    return out


# ---- captcha token provider ----------------------------------------------

class TokenProvider:
    """Supplies a fresh g-recaptcha-response token on demand.

    refresh() is called the FIRST time and whenever SearchAjax 302s (token
    expired / consumed). Returns None if no provider configured.
    """

    def __init__(self, token_cmd: str | None, token_file: str | None):
        self.token_cmd = token_cmd
        self.token_file = token_file
        self._token: str | None = None

    @property
    def configured(self) -> bool:
        return bool(self.token_cmd or self.token_file)

    def get(self) -> str | None:
        if self._token is None:
            self.refresh()
        return self._token

    def refresh(self) -> str | None:
        tok = None
        if self.token_cmd:
            try:
                tok = subprocess.check_output(
                    self.token_cmd, shell=True, text=True, timeout=180).strip()
            except Exception as e:  # noqa: BLE001
                print(f"[ebr] token-cmd failed: {e}", file=sys.stderr)
        elif self.token_file:
            try:
                with open(self.token_file) as fh:
                    tok = fh.read().strip()
            except Exception as e:  # noqa: BLE001
                print(f"[ebr] token-file read failed: {e}", file=sys.stderr)
        self._token = tok or None
        return self._token


# ---- db ------------------------------------------------------------------

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS public.ebr_improvement_progress (
    parcel_id    text PRIMARY KEY,
    tax_year     integer NOT NULL,
    outcome      text NOT NULL,   -- enriched | no_improvement | http_err | captcha | parse_empty
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

# Priority = parcels in the EBR arm of unserviced_hail_leads that still lack
# year_built. These are exactly the storm-hit, un-serviced Baton Rouge leads.
PRIORITY_SQL = """
SELECT DISTINCT u.parcel_id
  FROM public.unserviced_hail_leads u
  JOIN public.tx_cad_parcels p
    ON p.parcel_id = u.parcel_id
   AND p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
 WHERE u.county_source = 'EBR'
   AND p.year_built IS NULL
   AND NOT EXISTS (
        SELECT 1 FROM public.ebr_improvement_progress g
         WHERE g.parcel_id = p.parcel_id)
"""

ALL_SQL = """
SELECT p.parcel_id
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
   AND p.year_built IS NULL
   AND NOT EXISTS (
        SELECT 1 FROM public.ebr_improvement_progress g
         WHERE g.parcel_id = p.parcel_id)
 ORDER BY p.parcel_id
"""


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn, connect_timeout=20)
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user,
        connect_timeout=20)


def load_seed_csv(path: str) -> list[str]:
    """Optional explicit seed list of ASSESSMENT_NUM/parcel_id (one per line, or
    a CSV with a parcel_id / assessment_num column)."""
    if not path or not os.path.exists(path):
        return []
    out = []
    import csv
    with open(path, newline="") as fh:
        sniff = fh.read(2048)
        fh.seek(0)
        if "," in sniff and ("parcel" in sniff.lower() or "assessment" in sniff.lower()):
            rd = csv.DictReader(fh)
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


def load_targets(conn, args):
    cur = conn.cursor()
    cur.execute(PROGRESS_DDL)
    conn.commit()
    params = {"cad": CAD, "tax_year": args.tax_year}
    targets: list[str] = []
    seen: set[str] = set()

    for pid in load_seed_csv(args.seed_csv):
        if pid not in seen:
            seen.add(pid)
            targets.append(pid)
    if targets:
        cur.execute(
            "SELECT parcel_id FROM public.ebr_improvement_progress "
            "WHERE parcel_id = ANY(%s)", (targets,))
        done = {r[0] for r in cur.fetchall()}
        targets = [p for p in targets if p not in done]
        print(f"[seed] {len(targets)} seed parcels pending", flush=True)

    if not args.full_only:
        try:
            cur.execute(PRIORITY_SQL, params)
            for (pid,) in cur.fetchall():
                if pid not in seen:
                    seen.add(pid)
                    targets.append(pid)
        except Exception as e:  # noqa: BLE001 — MV may not exist yet
            conn.rollback()
            print(f"[priority] skipped ({e})", flush=True)
        print(f"[priority] {len(targets)} lead+seed parcels pending", flush=True)

    if not args.priority_only:
        cur.execute(ALL_SQL, params)
        for (pid,) in cur.fetchall():
            if pid not in seen:
                seen.add(pid)
                targets.append(pid)
    cur.close()
    return targets


# ---- scrape loop ---------------------------------------------------------

_STOP = False


def _sig(_s, _f):
    global _STOP
    _STOP = True
    print("[signal] stopping after current parcel...", flush=True)


def new_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Referer": SEARCH_PAGE})
    # Prime cookies + antiforgery token by loading the search page.
    sess.get(SEARCH_PAGE, timeout=30)
    return sess


def antiforgery(sess: requests.Session) -> str | None:
    try:
        r = sess.get(SEARCH_PAGE, timeout=30)
        m = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
        return m.group(1) if m else None
    except Exception:  # noqa: BLE001
        return None


def fetch_assessment(sess, pid, tok_provider, tax_year):
    """Return (outcome, parsed, raw_body). outcome in
    enriched|no_improvement|parse_empty|captcha|http_err."""
    grc = tok_provider.get()
    if not grc:
        return "captcha", {"year_built": None, "building_sqft": None}, ""
    af = antiforgery(sess)
    data = {
        "__RequestVerificationToken": af or "",
        "AssessmentNumber": pid,
        "g-recaptcha-response": grc,
        "DataTableRequest[draw]": "1",
        "DataTableRequest[start]": "0",
        "DataTableRequest[length]": "25",
        "DataTableRequest[search][value]": "",
        "DataTableRequest[search][regex]": "false",
    }
    try:
        r = sess.post(SEARCH_AJAX, data=data, timeout=40,
                      allow_redirects=False,
                      headers={"X-Requested-With": "XMLHttpRequest"})
    except Exception:  # noqa: BLE001
        return "http_err", {"year_built": None, "building_sqft": None}, ""
    if r.status_code in (301, 302, 303):
        # token consumed / expired → refresh once and signal captcha to caller
        tok_provider.refresh()
        return "captcha", {"year_built": None, "building_sqft": None}, ""
    if r.status_code != 200:
        return f"http_{r.status_code}", {"year_built": None, "building_sqft": None}, r.text[:2000]
    body = r.text
    parsed = parse_assessment(body)
    if parsed["year_built"] or parsed["building_sqft"]:
        return "enriched", parsed, body
    if "YearBuilt" in body or "Year Built" in body:
        return "parse_empty", parsed, body
    return "no_improvement", parsed, body


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
                    help="optional file of parcel_id/ASSESSMENT_NUM to do first")
    ap.add_argument("--token-cmd", default=os.environ.get("EBR_TOKEN_CMD", ""),
                    help="shell cmd that prints a fresh g-recaptcha-response")
    ap.add_argument("--token-file", default=os.environ.get("EBR_TOKEN_FILE", ""),
                    help="file containing a fresh g-recaptcha-response token")
    ap.add_argument("--raw-dir", default="",
                    help="optional dir to stage raw HTML (MUST NOT be /home/will)")
    ap.add_argument("--probe-only", action="store_true",
                    help="check reachability + captcha gate, then exit")
    args = ap.parse_args()

    if args.raw_dir and args.raw_dir.startswith("/home/will"):
        print("REFUSING: --raw-dir on home drive violates Storage Policy",
              file=sys.stderr)
        sys.exit(3)
    if args.raw_dir:
        os.makedirs(args.raw_dir, exist_ok=True)

    tok_provider = TokenProvider(args.token_cmd or None, args.token_file or None)

    # Reachability + captcha probe.
    try:
        probe = requests.get(SEARCH_PAGE, headers={"User-Agent": UA}, timeout=30)
        reachable = probe.status_code == 200
    except Exception as e:  # noqa: BLE001
        reachable = False
        print(f"[ebr] UNREACHABLE: {e}", file=sys.stderr)
    print(f"[ebr] search page reachable={reachable} "
          f"token_provider={'yes' if tok_provider.configured else 'NONE'}",
          flush=True)

    if args.probe_only:
        sys.exit(0 if reachable else 2)

    if not tok_provider.configured:
        print(
            "\n[ebr] NO CAPTCHA TOKEN PROVIDER CONFIGURED — cannot scrape.\n"
            "  SmartCAMA /Assessments/SearchAjax is guarded by reCAPTCHA\n"
            "  Enterprise (image challenge per search). Provide one of:\n"
            "    --token-cmd '<solver that prints g-recaptcha-response>'\n"
            "    --token-file <path with a freshly-solved token>\n"
            "  (env EBR_TOKEN_CMD / EBR_TOKEN_FILE also work.)\n",
            file=sys.stderr)
        sys.exit(4)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    conn = get_conn(args)
    conn.autocommit = False
    targets = load_targets(conn, args)
    if args.limit:
        targets = targets[: args.limit]
    print(f"[ebr] {len(targets):,} parcels to scrape (rate<= {args.rate}/s)",
          flush=True)

    sess = new_session()
    min_interval = 1.0 / args.rate if args.rate > 0 else 0
    cur = conn.cursor()
    t0 = time.time()
    enriched = no_improv = errs = captcha_skips = 0
    prog_batch: list[tuple] = []
    last_req = 0.0
    i = 0

    for i, pid in enumerate(targets, 1):
        if _STOP:
            break
        dt = time.time() - last_req
        if dt < min_interval:
            time.sleep(min_interval - dt)
        last_req = time.time()

        outcome, parsed, body = fetch_assessment(sess, pid, tok_provider, args.tax_year)

        if outcome == "captcha":
            captcha_skips += 1
            # Re-prime a fresh session occasionally; if the provider can't supply
            # a token, bail out cleanly rather than spinning.
            if not tok_provider.get():
                print("[ebr] token provider exhausted — stopping", flush=True)
                prog_batch.append((pid, args.tax_year, "captcha", None, None))
                break
            sess = new_session()
            prog_batch.append((pid, args.tax_year, "captcha", None, None))
        elif outcome == "enriched":
            if args.raw_dir and body:
                with open(os.path.join(args.raw_dir, f"{pid}.html"), "w") as fh:
                    fh.write(body)
            detail = json.dumps({
                "year_built": parsed["year_built"],
                "building_sqft": parsed["building_sqft"],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            try:
                cur.execute(f"SET LOCAL lock_timeout = '{args.lock_timeout}'")
                cur.execute(UPDATE_SQL, {
                    "year_built": parsed["year_built"],
                    "building_sqft": parsed["building_sqft"],
                    "detail": detail, "cad": CAD,
                    "parcel_id": pid, "tax_year": args.tax_year})
                enriched += 1
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                outcome = "lock_skip"
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                outcome = "db_err"
                print(f"[ebr] db err pid={pid}: {e}", flush=True)
            prog_batch.append((pid, args.tax_year, outcome,
                               parsed["year_built"], parsed["building_sqft"]))
        else:
            if outcome in ("no_improvement", "parse_empty"):
                no_improv += 1
            else:
                errs += 1
            prog_batch.append((pid, args.tax_year, outcome,
                               parsed["year_built"], parsed["building_sqft"]))

        if len(prog_batch) >= 25:
            execute_values(cur, PROGRESS_UPSERT, prog_batch)
            conn.commit()
            prog_batch.clear()

        if i % 200 == 0 or i <= 5:
            el = time.time() - t0
            rate = i / el if el else 0
            eta = (len(targets) - i) / rate / 60 if rate else 0
            print(f"[ebr] {i:,}/{len(targets):,} enriched={enriched:,} "
                  f"no_imp={no_improv:,} captcha={captcha_skips} err={errs} "
                  f"{rate:.1f}/s eta={eta:.0f}m", flush=True)

    if prog_batch:
        execute_values(cur, PROGRESS_UPSERT, prog_batch)
        conn.commit()
    cur.close()
    conn.close()
    el = (time.time() - t0) / 60
    print(f"[ebr] DONE processed={i:,} enriched={enriched:,} "
          f"no_imp={no_improv:,} captcha={captcha_skips} err={errs} "
          f"in {el:.1f}m", flush=True)


if __name__ == "__main__":
    main()

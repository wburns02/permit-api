#!/usr/bin/env python3
"""Comal Appraisal District (CCAD) per-property year_built / building_sqft scraper.

Fills the COMAL year_built gap in tx_cad_parcels (cad_source='CCAD'). Our bulk
Comal parcel layer (comal_parcel_geometries -> tx_cad_parcels CCAD rows) carries
owner / situs / value but leaves year_built NULL — the county GIS layer has no
construction year. That detail lives only in the Comal AD property viewer:

    https://esearch.comalad.org/Property/View/{propId}?year={year}   (BIS Consultants)

--------------------------------------------------------------------------------
HOW THE SITE ACTUALLY WORKS  (verified live 2026-07-01)
--------------------------------------------------------------------------------
1. Property detail pages are keyed by an internal sequential integer propId
   (e.g. 379). The join key we need — the Geographic ID (e.g. 10000037200) —
   is rendered on that page and equals tx_cad_parcels.parcel_id for CCAD. So we
   DON'T need the (token-gated, /Search/Expired) search: we crawl propId 1..N,
   read the GeoId off each page, and join on it.

2. year_built + building sqft live in the "Property Improvement - Building"
   table, which is injected by an AJAX call `propertyView.getImprovements(...)`.
   That fragment returns EMPTY to plain requests (anti-bot / session-gated), so
   we render the page in headless Chromium (Playwright) and read the table from
   the DOM. Everything else on the page is static, but rendering is the reliable
   path for the one field we care about.

3. An "invalid" propId still returns HTTP 200 but with no Geographic ID — that's
   how we detect the end of the range (STOP after --max-empty consecutive).

SOURCE CAVEAT: commercial parcels put year on the improvement (e.g. concrete
paving 2021); residential parcels put it on the dwelling. We take the MIN
plausible Year Built (>1700, !=0) across building improvements as the roof-age
proxy, and record parcels whose source has no year as outcome 'no_year' so we
never re-hit them.

--------------------------------------------------------------------------------
DB write  (same safety contract as scrape_ebr_improvements.py)
--------------------------------------------------------------------------------
Idempotent keyed UPDATE of the existing CCAD row (cad_source, parcel_id,
tax_year); COALESCE so we never clobber an existing value. Per-write lock_timeout,
single keyed-row UPDATEs, NEVER pg_terminate/pg_cancel, no full scans. Resumable
via checkpoint table comalad_improvement_progress (keyed on propId). Short-lived
connections per flush so the long between-render gaps don't trip idle drops.

PAUSABLE: SIGINT/SIGTERM stop after the current parcel and flush — safe to halt
before DB maintenance (progress is durable; re-run resumes from the next propId).

Usage (run where Chromium + the permits DB are reachable):
  playwright install chromium         # once
  python3 scrape_comalad_improvements.py --dry-run --limit 20     # no DB, prove parse
  python3 scrape_comalad_improvements.py --start 1 --end 250000   # full backfill
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    raise

BASE = "https://esearch.comalad.org"
VIEW = BASE + "/Property/View/{pid}?year={year}"
CAD = "CCAD"
DEFAULT_YEAR = 2026

DB_HOST_DEFAULT = os.environ.get("PGHOST", "100.122.216.15")
DB_NAME_DEFAULT = os.environ.get("PGDATABASE", "permits")
DB_USER_DEFAULT = os.environ.get("PGUSER", "will")
DB_PORT_DEFAULT = int(os.environ.get("PGPORT", "5432"))


# ---- parse (pure; unit-tested against captured page text) -----------------

def parse_property(page_text: str) -> dict:
    """Extract join key + improvement detail from the RENDERED property page text.

    Returns {geo_id, situs, year_built, building_sqft}. year_built is the MIN
    plausible Year Built across building improvements (roof-age proxy); None if
    the source carries none. geo_id None means 'not a real property page'.
    """
    txt = re.sub(r"[ \t]+", " ", page_text)
    geo = re.search(r"Geographic ID:\s*([0-9]{6,})", txt)
    situs = re.search(r"Situs Address:\s*(.+?)\s*(?:Map ID:|Legal Description:|$)", txt)

    year_built = None
    building_sqft = None
    # Isolate the improvement-building section (between its header and the next
    # section) so we don't pick up years from Roll Value / Deed History.
    m = re.search(r"Property Improvement - Building(.*?)(?:Property Land|Property Roll Value|$)",
                  txt, re.DOTALL)
    if m:
        block = m.group(1)
        years, sqfts = [], []
        # Each improvement row ends "... <ClassCD> <YearBuilt> <SQFT>" on the
        # rendered table; pull trailing "<year> <sqft>" integer pairs.
        for yr, sq in re.findall(r"(\d{4})\s+([\d,]+)(?=\s|$)", block):
            y = int(yr)
            if 1700 < y < 2035:
                years.append(y)
                s = int(sq.replace(",", ""))
                if s > 100:
                    sqfts.append(s)
        years = [y for y in years if 1700 < y < 2035]
        if years:
            year_built = min(years)
        if sqfts:
            building_sqft = sum(sqfts)
    return {
        "geo_id": geo.group(1) if geo else None,
        "situs": situs.group(1).strip()[:200] if situs else None,
        "year_built": year_built,
        "building_sqft": building_sqft,
    }


# ---- db --------------------------------------------------------------------

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS public.comalad_improvement_progress (
    prop_id       integer PRIMARY KEY,
    geo_id        text,
    tax_year      integer NOT NULL,
    outcome       text NOT NULL,
    year_built    integer,
    building_sqft numeric,
    scraped_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS comalad_improvement_progress_geo
    ON public.comalad_improvement_progress (geo_id);
"""

UPDATE_SQL = """
UPDATE public.tx_cad_parcels
   SET year_built    = COALESCE(%(year_built)s, year_built),
       building_sqft = COALESCE(%(building_sqft)s, building_sqft),
       raw = COALESCE(raw, '{}'::jsonb)
             || jsonb_build_object('comalad_improvements', %(detail)s::jsonb)
 WHERE cad_source = %(cad)s
   AND parcel_id  = %(geo_id)s
   AND tax_year   = %(tax_year)s
"""

PROGRESS_UPSERT = """
INSERT INTO public.comalad_improvement_progress
    (prop_id, geo_id, tax_year, outcome, year_built, building_sqft)
VALUES %s
ON CONFLICT (prop_id) DO UPDATE
   SET geo_id=EXCLUDED.geo_id, tax_year=EXCLUDED.tax_year,
       outcome=EXCLUDED.outcome, year_built=EXCLUDED.year_built,
       building_sqft=EXCLUDED.building_sqft, scraped_at=now()
"""

DONE_SQL = "SELECT prop_id FROM public.comalad_improvement_progress WHERE prop_id BETWEEN %s AND %s"

_KEEPALIVE = dict(keepalives=1, keepalives_idle=20, keepalives_interval=10,
                  keepalives_count=5)


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn, connect_timeout=20, **_KEEPALIVE)
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user,
        connect_timeout=20, **_KEEPALIVE)


def flush_writes(args, updates, prog_rows):
    """Short-lived connection: keyed UPDATEs (per-row lock_timeout) + progress
    upserts, then close. Retries once on a dropped connection. Returns
    (n_update_ok, n_lock_skip). Never pg_terminate/pg_cancel; single keyed rows."""
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
                    n_ok += cur.rowcount
                except psycopg2.errors.LockNotAvailable:
                    conn.rollback()
                    n_lock += 1
            if prog_rows:
                execute_values(cur, PROGRESS_UPSERT, prog_rows)
            conn.commit()
            cur.close()
            conn.close()
            return n_ok, n_lock
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"[comalad] DB flush connection lost ({e}); retry {attempt}", flush=True)
            n_ok = n_lock = 0
            time.sleep(1.0)
        except Exception as e:  # noqa: BLE001
            print(f"[comalad] db flush err: {e}", flush=True)
            return n_ok, n_lock
    print("[comalad] DB flush failed after retry — progress for this batch lost", flush=True)
    return n_ok, n_lock


def load_done(args) -> set[int]:
    """propIds already scraped in [start,end], for resume."""
    conn = get_conn(args)
    cur = conn.cursor()
    cur.execute(PROGRESS_DDL)
    conn.commit()
    cur.execute(DONE_SQL, (args.start, args.end))
    done = {r[0] for r in cur.fetchall()}
    cur.close()
    conn.close()
    return done


# ---- scrape loop -----------------------------------------------------------

_STOP = False


def _sig(_s, _f):
    global _STOP
    _STOP = True
    print("[signal] stopping after current parcel + flush...", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("CCAD_DSN",
                    os.environ.get("PERMITS_DSN", "")))
    ap.add_argument("--host", default=DB_HOST_DEFAULT)
    ap.add_argument("--db", default=DB_NAME_DEFAULT)
    ap.add_argument("--user", default=DB_USER_DEFAULT)
    ap.add_argument("--port", type=int, default=DB_PORT_DEFAULT)
    ap.add_argument("--tax-year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--start", type=int, default=1, help="first propId")
    ap.add_argument("--end", type=int, default=250000, help="last propId (inclusive)")
    ap.add_argument("--max-empty", type=int, default=500,
                    help="stop after this many consecutive no-GeoId pages (end of range)")
    ap.add_argument("--rate", type=float, default=1.0, help="max pages/sec (gentle)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--lock-timeout", default="5s")
    ap.add_argument("--raw-dir", default="", help="stage raw page text (NOT /home/will)")
    ap.add_argument("--dry-run", action="store_true",
                    help="scrape + parse + print, NO DB reads or writes")
    ap.add_argument("--probe-only", action="store_true", help="render one page, print, exit")
    args = ap.parse_args()

    if args.raw_dir and args.raw_dir.startswith("/home/will"):
        print("REFUSING: --raw-dir on home drive violates Storage Policy", file=sys.stderr)
        sys.exit(3)
    if args.raw_dir:
        os.makedirs(args.raw_dir, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: pip install playwright && playwright install chromium", file=sys.stderr)
        sys.exit(4)

    done: set[int] = set()
    if not args.dry_run:
        try:
            done = load_done(args)
            print(f"[comalad] {len(done):,} propIds already done in range (resume)", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[comalad] could not read progress (DB down?): {e}", file=sys.stderr)
            print("[comalad] use --dry-run to scrape without DB, or bring the DB up.",
                  file=sys.stderr)
            sys.exit(4)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    min_interval = 1.0 / args.rate if args.rate > 0 else 0
    enriched = no_year = no_geo = errs = matched = 0
    empty_streak = 0
    prog_batch: list[tuple] = []
    update_batch: list[dict] = []
    t0 = time.time()
    last = 0.0
    processed = 0

    def flush():
        nonlocal prog_batch, update_batch, matched
        if args.dry_run or (not prog_batch and not update_batch):
            prog_batch, update_batch = [], []
            return
        n_ok, _ = flush_writes(args, update_batch, prog_batch)
        matched += n_ok
        prog_batch, update_batch = [], []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "Chrome/124.0 Safari/537.36 (ecbtx ccad enrich)")
        page = ctx.new_page()

        for pid in range(args.start, args.end + 1):
            if _STOP:
                break
            if pid in done:
                continue
            if args.limit and processed >= args.limit:
                break
            dt = time.time() - last
            if dt < min_interval:
                time.sleep(min_interval - dt)
            last = time.time()
            processed += 1

            try:
                page.goto(VIEW.format(pid=pid, year=args.tax_year),
                          wait_until="networkidle", timeout=30000)
                text = page.inner_text("body")
            except Exception as e:  # noqa: BLE001
                errs += 1
                prog_batch.append((pid, None, args.tax_year, "error", None, None))
                if processed <= 3:
                    print(f"[comalad] render error pid={pid}: {e}", flush=True)
                if len(prog_batch) >= 25:
                    flush()
                continue

            rec = parse_property(text)
            geo = rec["geo_id"]

            if not geo:
                no_geo += 1
                empty_streak += 1
                prog_batch.append((pid, None, args.tax_year, "no_property", None, None))
                if empty_streak >= args.max_empty:
                    print(f"[comalad] {empty_streak} consecutive empty pages at "
                          f"pid={pid} — assuming end of range, stopping.", flush=True)
                    break
                if len(prog_batch) >= 25:
                    flush()
                continue
            empty_streak = 0

            if args.probe_only or args.dry_run:
                print(f"pid={pid} geo={geo} yb={rec['year_built']} "
                      f"sqft={rec['building_sqft']} situs={rec['situs']!r}", flush=True)
                if args.probe_only:
                    break

            if args.raw_dir:
                try:
                    with open(os.path.join(args.raw_dir, f"{pid}.txt"), "w") as fh:
                        fh.write(text)
                except OSError:
                    pass

            if rec["year_built"]:
                enriched += 1
                outcome = "enriched"
            else:
                no_year += 1
                outcome = "no_year"

            if not args.dry_run and rec["year_built"]:
                detail_json = json.dumps({
                    "year_built": rec["year_built"],
                    "building_sqft": rec["building_sqft"],
                    "prop_id": pid,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
                update_batch.append({
                    "year_built": rec["year_built"],
                    "building_sqft": rec["building_sqft"],
                    "detail": detail_json, "cad": CAD,
                    "geo_id": geo, "tax_year": args.tax_year})
            prog_batch.append((pid, geo, args.tax_year, outcome,
                               rec["year_built"], rec["building_sqft"]))

            if len(prog_batch) >= 25:
                flush()

            if processed % 100 == 0 or processed <= 5:
                el = time.time() - t0
                rate = processed / el if el else 0
                print(f"[comalad] pid={pid} done={processed:,} yb={enriched:,} "
                      f"no_year={no_year:,} no_prop={no_geo:,} err={errs} "
                      f"matched={matched:,} {rate:.2f}/s", flush=True)

        flush()
        browser.close()

    el = (time.time() - t0) / 60
    print(f"[comalad] DONE processed={processed:,} yb={enriched:,} no_year={no_year:,} "
          f"no_prop={no_geo:,} err={errs} db_matched={matched:,} in {el:.1f}m", flush=True)


if __name__ == "__main__":
    main()

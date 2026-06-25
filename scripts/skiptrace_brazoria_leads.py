#!/usr/bin/env python3
"""Skip-trace Brazoria permit leads -> brazoria_lead_contacts (PAID, GATED).

Phase 4 of the Brazoria TX permit-lead feed. Takes leads that already have an
owner_name + mailable address (attributed FREE from Brazoria CAD in
load_brazoriacad.py) and resolves a best phone + email via BatchData
skip-trace, caching the result in `brazoria_lead_contacts` keyed on the lead's
`address_norm`. The `brazoria_permit_leads` MV LEFT JOINs that table to surface
phone/email on the /v1/permit-leads endpoint.

SAFETY (this is the only PAID step in the whole feed):
  * --dry-run is the DEFAULT. Nothing is charged unless --execute is passed.
  * --limit hard-caps how many FRESH lookups happen this run (default 25).
  * Already-cached addresses (a row in brazoria_lead_contacts) are skipped, so
    re-runs never double-charge.
  * Priority ordering: lead_class IN ('new_construction','addition') first,
    newest event_date first — so a small cap spends on the best leads.
  * NEVER fabricates a phone. A miss writes a hit=false row (so we don't
    re-charge for a known-empty address) with NULL phone.

Cost: BatchData residential skip-trace ~ COST_CENTS_PER_LOOKUP (Will's rate).

Usage:
  # dry run — show what WOULD be skip-traced, spend nothing:
  python3 scripts/skiptrace_brazoria_leads.py --limit 25

  # real run, capped at 25, priority leads only:
  BATCHDATA_API_KEY=... python3 scripts/skiptrace_brazoria_leads.py \
      --execute --limit 25 --priority-only

DB defaults to T430 (host 100.122.216.15 dbname=permits) or --dsn / $PERMITS_DSN.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time

import httpx

# NOTE: psycopg2 is imported lazily inside the DB functions (fetch_candidates /
# main), NOT at module scope, so the pure-logic helpers (best_phone /
# best_email / split_addr / normalize_persons) are importable + unit-testable
# in environments that ship asyncpg but not psycopg2 (e.g. CI).

BATCHDATA_URL = "https://api.batchdata.com/api/v1/property/skip-trace"
COST_CENTS_PER_LOOKUP = 25  # BatchData residential skip-trace — Will's rate
PRIORITY_CLASSES = ("new_construction", "addition")


def log(*a):
    print(*a, flush=True)


def fetch_candidates(conn, limit, priority_only):
    """Leads with an owner + address but no contact row yet.

    Priority leads (new_construction / addition) ordered first so a tight cap
    spends on the best leads. A lead needs an owner_name AND an address; the
    CAD join (Phase 4 Part A) is what fills owner_name for most of them.
    """
    where = [
        "bpl.owner_name IS NOT NULL",
        "length(trim(bpl.owner_name)) > 1",
        "bpl.address IS NOT NULL",
        "c.address_norm IS NULL",  # not yet skip-traced
    ]
    if priority_only:
        where.append("bpl.lead_class IN ('new_construction','addition')")
    from psycopg2.extras import RealDictCursor
    sql = f"""
        SELECT bpl.address_norm, bpl.address, bpl.city, bpl.zip,
               bpl.owner_name, bpl.lead_class, bpl.event_date,
               COALESCE(bpl.mailable_address, bpl.address) AS mailable_address
          FROM brazoria_permit_leads bpl
          LEFT JOIN brazoria_lead_contacts c USING (address_norm)
         WHERE {' AND '.join(where)}
         ORDER BY (bpl.lead_class IN ('new_construction','addition')) DESC,
                  bpl.event_date DESC NULLS LAST,
                  bpl.address_norm ASC
         LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


def split_addr(row):
    """Best street/city/zip for the BatchData request.

    Prefer the structured lead columns (city/zip), fall back to the mailable
    address's tail. The street is the lead `address` line.
    """
    street = (row.get("address") or "").strip()
    city = (row.get("city") or "").strip()
    zp = (row.get("zip") or "").strip()
    if (not city or not zp) and row.get("mailable_address"):
        parts = [p.strip() for p in row["mailable_address"].split(",")]
        if len(parts) >= 2 and not city:
            city = parts[1]
        if not zp:
            tail = parts[-1].split()
            if tail and tail[-1].isdigit():
                zp = tail[-1][:5]
    return street, city, "TX", zp


def call_batchdata(client, api_key, street, city, state, zp):
    payload = {"requests": [{"propertyAddress": {
        "street": street, "city": city, "state": state, "zip": zp}}]}
    resp = client.post(
        BATCHDATA_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=60.0,
    )
    if resp.status_code == 401:
        raise RuntimeError("BatchData 401 — credential not configured or revoked")
    resp.raise_for_status()
    body = resp.json()
    return (body.get("results") or {}).get("persons") or []


def best_phone(persons):
    """Highest-score phone across all returned persons; Mobile preferred."""
    cands = []
    for p in persons or []:
        for ph in p.get("phoneNumbers") or []:
            cands.append(ph)
    if not cands:
        return None
    cands.sort(key=lambda ph: (
        -int(ph.get("score") or 0),
        0 if ph.get("type") == "Mobile" else 1,
    ))
    top = cands[0]
    return {
        "number": top.get("number"),
        "type": top.get("type"),
        "dnc": bool(top.get("dnc")) if top.get("dnc") is not None else None,
    }


def best_email(persons):
    for p in persons or []:
        for e in p.get("emails") or []:
            val = e.get("email") if isinstance(e, dict) else e
            if val:
                return val
    return None


def normalize_persons(persons):
    """Compact, auditable persons array stored in brazoria_lead_contacts.persons."""
    out = []
    for p in persons or []:
        if not isinstance(p, dict):
            continue
        name = p.get("name") or {}
        phones = [
            {"number": ph.get("number"), "type": ph.get("type"),
             "score": ph.get("score"),
             "dnc": bool(ph.get("dnc")) if ph.get("dnc") is not None else None}
            for ph in (p.get("phoneNumbers") or [])
        ]
        emails = [
            (e.get("email") if isinstance(e, dict) else e)
            for e in (p.get("emails") or []) if e
        ]
        out.append({
            "name": {"first": name.get("first"), "last": name.get("last"),
                     "full": name.get("full")},
            "phones": phones[:5],
            "emails": emails[:5],
        })
    return out


UPSERT_SQL = """
    INSERT INTO brazoria_lead_contacts (
        address_norm, address, owner_name,
        best_phone, best_phone_type, best_phone_dnc, best_email,
        persons, hit, skiptraced, cost_cents, provider, fetched_at
    ) VALUES (
        %(address_norm)s, %(address)s, %(owner_name)s,
        %(best_phone)s, %(best_phone_type)s, %(best_phone_dnc)s, %(best_email)s,
        %(persons)s, %(hit)s, true, %(cost_cents)s, 'batchdata', now()
    )
    ON CONFLICT (address_norm) DO UPDATE SET
        best_phone = EXCLUDED.best_phone,
        best_phone_type = EXCLUDED.best_phone_type,
        best_phone_dnc = EXCLUDED.best_phone_dnc,
        best_email = EXCLUDED.best_email,
        persons = EXCLUDED.persons,
        hit = EXCLUDED.hit,
        cost_cents = EXCLUDED.cost_cents,
        fetched_at = now()
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("PERMITS_DSN", ""))
    ap.add_argument("--host", default=os.environ.get("PGHOST", "100.122.216.15"))
    ap.add_argument("--db", default="permits")
    ap.add_argument("--user", default=os.environ.get("PGUSER", "will"))
    ap.add_argument("--api-key", default=os.environ.get("BATCHDATA_API_KEY", ""))
    ap.add_argument("--limit", type=int, default=25,
                    help="HARD cap on FRESH skip-traces this run (default 25).")
    ap.add_argument("--priority-only", action="store_true",
                    help="only new_construction / addition leads.")
    ap.add_argument("--execute", action="store_true",
                    help="actually spend money. Without it this is a DRY RUN.")
    ap.add_argument("--sleep", type=float, default=0.3)
    args = ap.parse_args()

    if args.limit > 200:
        log(f"refusing --limit {args.limit}: hard ceiling is 200 for safety.")
        return 2

    import psycopg2  # lazy: only the DB path needs it (see module note)
    if args.dsn:
        conn = psycopg2.connect(args.dsn, connect_timeout=20)
    else:
        conn = psycopg2.connect(dbname=args.db, host=args.host,
                                user=args.user, connect_timeout=20)
    conn.autocommit = True

    try:
        cands = fetch_candidates(conn, args.limit, args.priority_only)
    except psycopg2.errors.ObjectNotInPrerequisiteState:
        log("brazoria_permit_leads MV is not populated — refresh it first.")
        conn.close()
        return 1
    except Exception as e:  # noqa: BLE001
        log(f"FATAL: candidate query failed: {e}")
        conn.close()
        return 1

    log(f"{len(cands)} candidate lead(s) (cap {args.limit}, "
        f"priority_only={args.priority_only}).")
    proj_total_cents = len(cands) * COST_CENTS_PER_LOOKUP
    log(f"max spend this run: {len(cands)} x {COST_CENTS_PER_LOOKUP}c = "
        f"${proj_total_cents/100:.2f}")

    if not args.execute:
        log("DRY RUN (no --execute) — would skip-trace:")
        for c in cands[:30]:
            log(f"  [{c['lead_class']}] {c['address']} / {c.get('city')} "
                f"{c.get('zip') or ''} — owner={c['owner_name']}")
        conn.close()
        return 0

    api_key = (args.api_key or "").strip()
    if not api_key:
        log("NO BATCHDATA_API_KEY — integration is built but cannot run paid. "
            "Set BATCHDATA_API_KEY (Railway has it) and re-run with --execute.")
        conn.close()
        return 3

    hits = misses = spent_cents = 0
    with httpx.Client() as client:
        for i, c in enumerate(cands, 1):
            street, city, state, zp = split_addr(c)
            try:
                persons = call_batchdata(client, api_key, street, city, state, zp)
            except Exception as e:  # noqa: BLE001
                log(f"  [{i}/{len(cands)}] ERROR {c['address']}: {e}")
                time.sleep(2)
                continue
            spent_cents += COST_CENTS_PER_LOOKUP
            ph = best_phone(persons)
            em = best_email(persons)
            hit = bool(ph or em)
            if hit:
                hits += 1
            else:
                misses += 1
            conn.cursor().execute(UPSERT_SQL, {
                "address_norm": c["address_norm"],
                "address": c["address"],
                "owner_name": c["owner_name"],
                "best_phone": ph["number"] if ph else None,
                "best_phone_type": ph["type"] if ph else None,
                "best_phone_dnc": ph["dnc"] if ph else None,
                "best_email": em,
                "persons": json.dumps(normalize_persons(persons)),
                "hit": hit,
                "cost_cents": COST_CENTS_PER_LOOKUP,
            })
            tag = (ph["number"] if ph else "no-phone")
            log(f"  [{i}/{len(cands)}] {c['address']} -> {tag} "
                f"({'hit' if hit else 'miss'})")
            time.sleep(args.sleep)

    log(f"DONE: {hits} hit / {misses} miss / spent ${spent_cents/100:.2f} "
        f"(${(spent_cents/max(hits,1))/100:.2f}/hit).")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

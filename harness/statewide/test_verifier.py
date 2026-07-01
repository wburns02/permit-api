#!/usr/bin/env python3
"""Self-test proving the VERIFIER is real backpressure: it PASSES a real load and
FAILS a deliberately garbage one. Run before trusting the loop.

POSITIVE: pull a real sample from Austin's Socrata building-permit dataset, load
it under a `__selftest_pos` source_tag, verify -> must PASS.

NEGATIVE: load obviously-fabricated rows (all "123 TEST ST", bogus date) under a
`__selftest_neg` source_tag, verify -> must FAIL.

Cleans up both source_tags afterward (DELETE WHERE source = tag — bounded by the
indexed source column, no full scan).
"""
from __future__ import annotations

import json
import sys
import urllib.request

import db
import verify

POS_TAG = "statewide_loop:__selftest_pos"
NEG_TAG = "statewide_loop:__selftest_neg"
AUSTIN = (
    "https://data.austintexas.gov/resource/3syk-w9eu.json"
    "?$limit=25&$where=issue_date IS NOT NULL&$order=issue_date DESC"
)

COLS = [
    "id", "permit_number", "permit_type", "description", "address",
    "city", "state", "zip", "issue_date", "jurisdiction", "source",
]


def _uuid() -> str:
    import uuid
    return str(uuid.uuid4())


def _cleanup(tag: str) -> None:
    db._run_psql(["-c", f"DELETE FROM hot_leads WHERE source = '{tag}'"])


def load_positive() -> int:
    url = AUSTIN.replace(" ", "%20")
    with urllib.request.urlopen(url, timeout=40) as r:
        data = json.load(r)
    recs = []
    for d in data:
        addr = d.get("original_address1")
        if not addr:
            continue
        recs.append({
            "id": _uuid(),
            "permit_number": d.get("permit_number"),
            "permit_type": (d.get("permit_type_desc") or d.get("permittype")),
            "description": (d.get("description") or "")[:300] or None,
            "address": addr,
            "city": d.get("original_city") or "AUSTIN",
            "state": "TX",
            "zip": d.get("original_zip"),
            "issue_date": (d.get("issue_date") or "")[:10] or None,
            "jurisdiction": "Austin",
            "source": POS_TAG,
        })
    _cleanup(POS_TAG)
    return db.load_hot_leads(recs, COLS)


def load_negative() -> int:
    # Deliberately garbage: identical placeholder address, bogus date, no type.
    recs = []
    for i in range(8):
        recs.append({
            "id": _uuid(),
            "permit_number": f"FAKE-{i}",
            "permit_type": None,
            "description": None,
            "address": "123 TEST ST",          # placeholder + zero variety
            "city": "NOWHERE",
            "state": "TX",
            "zip": None,
            "issue_date": "1700-01-01",         # implausible
            "jurisdiction": "FakeTown",
            "source": NEG_TAG,
        })
    _cleanup(NEG_TAG)
    return db.load_hot_leads(recs, COLS)


def main() -> int:
    if not db.ping():
        print("DB unreachable; cannot run self-test", file=sys.stderr)
        return 2

    print("== loading positive sample (real Austin permits) ==")
    npos = load_positive()
    print(f"   loaded {npos} real rows under {POS_TAG}")
    pos = verify.verify(POS_TAG, AUSTIN.replace(" ", "%20"))
    print(f"   verify -> {'PASS' if pos.passed else 'FAIL'} :: {pos.reason}")
    print(f"   stats: {json.dumps(pos.stats)}")

    print("== loading negative sample (fabricated garbage) ==")
    nneg = load_negative()
    print(f"   loaded {nneg} garbage rows under {NEG_TAG}")
    neg = verify.verify(NEG_TAG, None)
    print(f"   verify -> {'PASS' if neg.passed else 'FAIL'} :: {neg.reason}")
    print(f"   stats: {json.dumps(neg.stats)}")

    print("== cleanup ==")
    _cleanup(POS_TAG)
    _cleanup(NEG_TAG)
    print("   removed self-test rows")

    ok = pos.passed and (not neg.passed)
    print(f"\nSELF-TEST {'PASSED' if ok else 'FAILED'}: "
          f"verifier {'correctly' if ok else 'did NOT'} pass-real / fail-garbage")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

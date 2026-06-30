"""The deterministic VERIFIER — the backpressure of the loop.

Given a `source_tag`, independently confirm that REAL permit data landed in
`hot_leads`. The agent's self-reported "built" means nothing here. A
built-but-garbage jurisdiction MUST fail this gate.

Checks (all must pass):
  1. ROW COUNT  >= MIN_ROWS for that source_tag (bounded query, indexed on
     `source`, capped with LIMIT — never a full-table count).
  2. REQUIRED FIELDS non-null on a sample: `address` present, AND at least one
     of (`permit_type`, `issue_date`). Enough of the sample must satisfy this.
  3. PLAUSIBILITY: values aren't obvious placeholder/garbage — addresses aren't
     all identical, aren't 'test'/'n/a'/'xxx', dates (if present) are real and
     not absurd (1990..now+1d), and there's real variety in the rows.
  4. (best-effort) ANTI-FABRICATION live re-fetch: if a source_url is given,
     HEAD/GET it and confirm it's a live 2xx feed. A pass here strengthens the
     verdict; a network failure does NOT by itself fail an otherwise-good load
     (the data already landed), but a source_url that 404s on a tiny row count
     is treated as suspicious.

Returns a Result(passed: bool, reason: str, stats: dict).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime

import db

# source_tag must match this pattern before any SQL use.
# Eliminates injection: the regex rejects anything that isn't a controlled
# statewide-loop tag, so `_q()` is redundant but kept as defense-in-depth.
_VALID_TAG_RE = re.compile(
    r"^statewide_loop:(tx_(city|county)_[a-z0-9_]+|__selftest_(pos|neg))$"
)

MIN_ROWS = 5                 # a "sample" must be at least this many real rows
SAMPLE = 50                  # how many rows we pull to inspect
MIN_GOOD_FRACTION = 0.6      # >=60% of sampled rows must have required fields
MIN_DISTINCT_ADDR = 3        # guard against "same row N times" fabrication

PLACEHOLDER_TOKENS = {
    "", "test", "n/a", "na", "none", "null", "xxx", "tbd", "unknown",
    "placeholder", "sample", "123 main st", "123 test st",
}


@dataclass
class Result:
    passed: bool
    reason: str
    stats: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {"passed": self.passed, "reason": self.reason, "stats": self.stats}
        )


def _q(val: str) -> str:
    """Single-quote-escape a literal for inline SQL (source_tag only — it is a
    controlled, namespaced string, but escape anyway)."""
    return val.replace("'", "''")


def _bounded_count(source_tag: str) -> int:
    # Count via a LIMIT-bounded subquery so we never scan the whole table.
    # Cap at SAMPLE*4 — we only need to know "enough", not the true total.
    cap = SAMPLE * 4
    sql = (
        f"SELECT count(*) FROM (SELECT 1 FROM hot_leads "
        f"WHERE source = '{_q(source_tag)}' LIMIT {cap}) z"
    )
    return int(db.scalar(sql) or 0)


def _sample_rows(source_tag: str) -> list[dict]:
    cols = ["permit_number", "address", "permit_type", "issue_date", "city", "state"]
    sql = (
        f"SELECT {', '.join(cols)} FROM hot_leads "
        f"WHERE source = '{_q(source_tag)}' LIMIT {SAMPLE}"
    )
    rows = db.query(sql)
    out = []
    for r in rows:
        out.append({c: (v if v != "" else None) for c, v in zip(cols, r)})
    return out


def _looks_placeholder(addr: str | None) -> bool:
    if addr is None:
        return True
    return addr.strip().lower() in PLACEHOLDER_TOKENS


def _plausible_date(s: str | None) -> bool:
    if not s:
        return True  # absence is fine; only present dates are sanity-checked
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            d = datetime.strptime(s[: len(fmt) + 2], fmt).date()
        except ValueError:
            continue
        return date(1990, 1, 1) <= d <= date.today().replace(year=date.today().year + 1)
    return False  # present but unparseable -> not plausible


def _live_feed(url: str | None) -> bool | None:
    if not url or not url.startswith("http"):
        return None
    try:
        req = urllib.request.Request(url, method="GET", headers={"User-Agent": "permit-loop-verify/1"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def verify(source_tag: str, source_url: str | None = None) -> Result:
    if not _VALID_TAG_RE.match(source_tag):
        return Result(False, f"invalid source_tag format: {source_tag!r}", {})

    if not db.ping():
        return Result(False, "db unreachable — cannot verify", {})

    n = _bounded_count(source_tag)
    stats = {"source_tag": source_tag, "row_count_bounded": n}
    if n < MIN_ROWS:
        return Result(False, f"too few rows: {n} < MIN_ROWS={MIN_ROWS}", stats)

    rows = _sample_rows(source_tag)
    stats["sampled"] = len(rows)

    # Required-field check.
    good = [
        r for r in rows
        if r["address"] and not _looks_placeholder(r["address"])
        and (r["permit_type"] or r["issue_date"])
    ]
    frac = len(good) / len(rows) if rows else 0.0
    stats["good_fraction"] = round(frac, 2)
    if frac < MIN_GOOD_FRACTION:
        return Result(
            False,
            f"required fields missing: only {frac:.0%} of sampled rows have "
            f"address + (permit_type|issue_date)",
            stats,
        )

    # Variety / anti-fabrication.
    distinct_addr = {r["address"].strip().lower() for r in good}
    stats["distinct_addresses"] = len(distinct_addr)
    if len(distinct_addr) < MIN_DISTINCT_ADDR:
        return Result(
            False,
            f"suspiciously low address variety: {len(distinct_addr)} distinct "
            f"(looks fabricated/duplicated)",
            stats,
        )

    # Date plausibility on the rows that carry a date.
    dated = [r for r in good if r["issue_date"]]
    bad_dates = [r for r in dated if not _plausible_date(r["issue_date"])]
    stats["dated_rows"] = len(dated)
    stats["implausible_dates"] = len(bad_dates)
    if dated and len(bad_dates) / len(dated) > 0.5:
        return Result(False, "majority of dates implausible/garbage", stats)

    # State sanity — pilot is TX only.
    non_tx = [r for r in good if r["state"] and r["state"].upper() != "TX"]
    if non_tx and len(non_tx) / len(good) > 0.5:
        stats["non_tx_rows"] = len(non_tx)
        return Result(False, "majority of rows not TX — wrong jurisdiction", stats)

    # Best-effort live re-fetch (anti-fabrication). Non-fatal unless data is
    # already thin AND the feed is dead.
    live = _live_feed(source_url)
    stats["source_live"] = live
    if live is False and n < MIN_ROWS * 2:
        return Result(
            False,
            "thin data AND source_url not live (2xx) — cannot confirm real feed",
            stats,
        )

    return Result(True, f"verified: {n}+ real rows, {len(distinct_addr)} distinct addrs", stats)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source_tag")
    ap.add_argument("--source-url", default=None)
    args = ap.parse_args()
    res = verify(args.source_tag, args.source_url)
    print(res.to_json())
    return 0 if res.passed else 1


if __name__ == "__main__":
    sys.exit(main())

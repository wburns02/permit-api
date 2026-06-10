"""Sample 500 TX permits for the classifier eval set.

Stratified across:
  - permit_type frequency bucket: top-20 types / rank 21-200 / long tail (incl. null)
  - decade of issued_date (<1990, 1990s, 2000s, 2010s, 2020s)
  - source diversity: per-stratum round-robin across source_id so the top 10
    sources are all represented without letting austin_socrata dominate.

TX universe = jurisdiction.state='TX' AND source_id IN canonical.enrichment_tx_sources
(see scripts/permit_enrichment_schema.sql for the pollution rationale).

Writes eval_candidates.jsonl next to this script.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
HERE = Path(__file__).resolve().parent
OUT = HERE / "eval_candidates.jsonl"

TOTAL = 500
PER_CELL_FETCH = 80  # oversample per stratum cell, trim later

SQL = """
WITH tx AS (
  SELECT p.permit_id, p.source_id, p.source_record_id, p.permit_type,
         p.description_raw, p.address_raw, p.declared_value, p.issued_date
  FROM canonical.permits p
  WHERE p.jurisdiction_id IN (SELECT id FROM canonical.jurisdictions WHERE state='TX')
    AND p.source_id IN (SELECT source_id FROM canonical.enrichment_tx_sources)
),
type_counts AS (
  SELECT permit_type, count(*) AS n, row_number() OVER (ORDER BY count(*) DESC) AS rnk
  FROM tx GROUP BY permit_type
),
bucketed AS (
  SELECT t.*,
    CASE WHEN tc.rnk <= 20 THEN 'top20'
         WHEN tc.rnk <= 200 THEN 'mid'
         ELSE 'tail' END AS freq_bucket,
    CASE WHEN t.issued_date < '1990-01-01' THEN 'pre1990'
         WHEN t.issued_date < '2000-01-01' THEN '1990s'
         WHEN t.issued_date < '2010-01-01' THEN '2000s'
         WHEN t.issued_date < '2020-01-01' THEN '2010s'
         ELSE '2020s' END AS decade
  FROM tx t LEFT JOIN type_counts tc ON tc.permit_type IS NOT DISTINCT FROM t.permit_type
),
ranked AS (
  SELECT *,
    row_number() OVER (
      PARTITION BY freq_bucket, decade, source_id ORDER BY random()
    ) AS src_rank
  FROM bucketed
),
interleaved AS (
  SELECT *,
    row_number() OVER (PARTITION BY freq_bucket, decade ORDER BY src_rank, random()) AS cell_rank
  FROM ranked
)
SELECT permit_id, source_id, source_record_id, permit_type, description_raw,
       address_raw, declared_value, issued_date, freq_bucket, decade
FROM interleaved
WHERE cell_rank <= %(per_cell)s
"""


def main() -> None:
    random.seed(42)
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        conn.execute("SET statement_timeout = '1800s'")
        rows = conn.execute(SQL, {"per_cell": PER_CELL_FETCH}).fetchall()
    print(f"fetched {len(rows)} candidates across strata")

    # Trim to TOTAL with equal-ish allocation per (freq_bucket, decade) cell.
    cells: dict[tuple, list[dict]] = {}
    for r in rows:
        cells.setdefault((r["freq_bucket"], r["decade"]), []).append(r)
    n_cells = len(cells)
    base = TOTAL // n_cells
    picked: list[dict] = []
    leftovers: list[dict] = []
    for key, items in sorted(cells.items()):
        random.shuffle(items)
        picked.extend(items[:base])
        leftovers.extend(items[base:])
    random.shuffle(leftovers)
    picked.extend(leftovers[: TOTAL - len(picked)])
    print(f"cells={n_cells}, picked={len(picked)}")

    srcs: dict[str, int] = {}
    for r in picked:
        srcs[r["source_id"]] = srcs.get(r["source_id"], 0) + 1
    print("source distribution:", json.dumps(dict(sorted(srcs.items(), key=lambda kv: -kv[1])), indent=1))

    with open(OUT, "w") as f:
        for r in picked:
            rec = {
                "id": str(r["permit_id"]),
                "source_id": r["source_id"],
                "source_record_id": r["source_record_id"],
                "permit_type": r["permit_type"],
                "description_raw": r["description_raw"],
                "address_raw": r["address_raw"],
                "declared_value": float(r["declared_value"]) if r["declared_value"] is not None else None,
                "issued_date": str(r["issued_date"]) if r["issued_date"] else None,
                "freq_bucket": r["freq_bucket"],
                "decade": r["decade"],
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"wrote {len(picked)} -> {OUT}")


if __name__ == "__main__":
    main()

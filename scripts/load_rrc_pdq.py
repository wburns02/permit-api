#!/usr/bin/env python3
"""Load RRC PDQ dump (PDQ_DSV.zip) into the permits warehouse.

- OG_LEASE_CYCLE -> canonical.production_monthly (as-reported Form PR lease
  monthly volumes, Jan 1993+). Identity: (well_type, district, lease_number,
  prod_month). Full reload per run (delete source='rrc_pdq', COPY back in);
  the dump is a complete monthly refresh, not a delta.
- Dimension/bridge tables -> rrc_raw.* as TEXT columns (lineage-preserving
  landing zone): GP_COUNTY, OG_OPERATOR_DW, OG_REGULATORY_LEASE_DW,
  OG_WELL_COMPLETION, OG_SUMMARY_MASTER_LARGE.
- OG_OPERATOR_DW also upserts canonical.operators (state TX, P-5 number).

Delimiter is '}' with a header row, no quoting (PDQ dump manual).

Usage: load_rrc_pdq.py [--zip PATH] [--skip-production]
"""
import argparse
import io
import sys
import time
import zipfile
from datetime import date

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
ZIP = "/mnt/win11/Fedora/raw-public-data/rrc/pdq/PDQ_DSV.zip"
SOURCE = "rrc_pdq"

RAW_TABLES = [
    "GP_COUNTY",
    "OG_OPERATOR_DW",
    "OG_REGULATORY_LEASE_DW",
    "OG_WELL_COMPLETION",
    "OG_SUMMARY_MASTER_LARGE",
]

PROD_INDEXES = {
    "ix_canon_prod_lease": "CREATE INDEX ix_canon_prod_lease ON canonical.production_monthly (state, district, lease_number, prod_month)",
    "ix_canon_prod_month": "CREATE INDEX ix_canon_prod_month ON canonical.production_monthly (prod_month)",
    "ix_canon_prod_operator": "CREATE INDEX ix_canon_prod_operator ON canonical.production_monthly (operator_number)",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dsv_lines(zf, member):
    """Yield decoded lines from a zip member without extracting to disk."""
    with zf.open(member) as f:
        for raw in io.TextIOWrapper(f, encoding="utf-8", errors="replace"):
            yield raw.replace("\x00", "").rstrip("\r\n")


def load_raw_table(conn, zf, table):
    member = f"{table}_DATA_TABLE.dsv"
    lines = dsv_lines(zf, member)
    header = next(lines).split("}")
    cols = ", ".join(f'"{c.lower()}" TEXT' for c in header)
    qcols = ", ".join(f'"{c.lower()}"' for c in header)
    tname = f"rrc_raw.{table.lower()}"
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS rrc_raw")
        cur.execute(f"DROP TABLE IF EXISTS {tname}")
        cur.execute(f"CREATE TABLE {tname} ({cols})")
        buf, n, skipped = [], 0, 0
        ncol = len(header)

        def flush():
            if buf:
                cur.copy_expert(
                    f"COPY {tname} ({qcols}) FROM STDIN",
                    io.StringIO("".join(buf)),
                )
                buf.clear()

        for line in lines:
            parts = line.split("}")
            if len(parts) != ncol:
                skipped += 1
                continue
            buf.append(
                "\t".join(
                    p.strip().replace("\\", "\\\\").replace("\t", " ")
                    or "\\N"
                    for p in parts
                )
                + "\n"
            )
            n += 1
            if n % 100_000 == 0:
                flush()
        flush()
    conn.commit()
    log(f"{tname}: {n:,} rows ({skipped} skipped)")


def upsert_operators(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO canonical.operators
                (state, operator_number, name, p5_status, lineage, freshness_at)
            SELECT 'TX', operator_no, operator_name, p5_status_code,
                   jsonb_build_object('source', %s), now()
            FROM rrc_raw.og_operator_dw
            WHERE operator_no IS NOT NULL
            ON CONFLICT (state, operator_number) DO UPDATE SET
                name = EXCLUDED.name,
                p5_status = EXCLUDED.p5_status,
                freshness_at = now(),
                updated_at = now()
        """, (SOURCE,))
        log(f"canonical.operators upserted: {cur.rowcount:,}")
    conn.commit()


def load_production(conn, zf):
    member = "OG_LEASE_CYCLE_DATA_TABLE.dsv"
    lines = dsv_lines(zf, member)
    header = next(lines).split("}")
    idx = {c: i for i, c in enumerate(header)}
    need = [
        "OIL_GAS_CODE", "DISTRICT_NO", "LEASE_NO", "CYCLE_YEAR", "CYCLE_MONTH",
        "OPERATOR_NO", "OPERATOR_NAME", "FIELD_NO", "FIELD_NAME", "LEASE_NAME",
        "LEASE_OIL_PROD_VOL", "LEASE_GAS_PROD_VOL", "LEASE_COND_PROD_VOL",
        "LEASE_CSGD_PROD_VOL",
    ]
    missing = [c for c in need if c not in idx]
    if missing:
        sys.exit(f"missing columns in {member}: {missing}")
    ncol = len(header)

    with conn.cursor() as cur:
        log("dropping production indexes for bulk load")
        for name in PROD_INDEXES:
            cur.execute(f"DROP INDEX IF EXISTS canonical.{name}")
        cur.execute(
            "DELETE FROM canonical.production_monthly WHERE source = %s",
            (SOURCE,),
        )
        log(f"deleted {cur.rowcount:,} prior {SOURCE} rows")
        conn.commit()

        copy_sql = """
            COPY canonical.production_monthly
            (state, district, lease_number, lease_name, well_type,
             operator_number, operator_name, field_number, field_name,
             prod_month, oil_bbl, gas_mcf, condensate_bbl, casinghead_mcf,
             source)
            FROM STDIN
        """
        buf, n, skipped = [], 0, 0
        t0 = time.time()

        def g(parts, col):
            v = parts[idx[col]].strip()
            return v if v else None

        def flush():
            if buf:
                cur.copy_expert(copy_sql, io.StringIO("".join(buf)))
                buf.clear()

        for line in lines:
            parts = line.split("}")
            if len(parts) != ncol:
                skipped += 1
                continue
            try:
                y, m = int(parts[idx["CYCLE_YEAR"]]), int(parts[idx["CYCLE_MONTH"]])
                prod_month = date(y, m, 1).isoformat()
            except ValueError:
                skipped += 1
                continue
            row = [
                "TX",
                g(parts, "DISTRICT_NO"),
                g(parts, "LEASE_NO"),
                g(parts, "LEASE_NAME"),
                g(parts, "OIL_GAS_CODE"),
                g(parts, "OPERATOR_NO"),
                g(parts, "OPERATOR_NAME"),
                g(parts, "FIELD_NO"),
                g(parts, "FIELD_NAME"),
                prod_month,
                g(parts, "LEASE_OIL_PROD_VOL"),
                g(parts, "LEASE_GAS_PROD_VOL"),
                g(parts, "LEASE_COND_PROD_VOL"),
                g(parts, "LEASE_CSGD_PROD_VOL"),
                SOURCE,
            ]
            buf.append(
                "\t".join(
                    (v.replace("\\", "\\\\").replace("\t", " ") if v else "\\N")
                    for v in row
                )
                + "\n"
            )
            n += 1
            if n % 250_000 == 0:
                flush()
                if n % 5_000_000 == 0:
                    conn.commit()
                    rate = n / (time.time() - t0)
                    log(f"production: {n:,} rows ({rate:,.0f}/s)")
        flush()
        conn.commit()
        log(f"production loaded: {n:,} rows ({skipped} skipped), rebuilding indexes")
        for ddl in PROD_INDEXES.values():
            cur.execute(ddl)
        conn.commit()
    log("production indexes rebuilt")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", default=ZIP)
    ap.add_argument("--skip-production", action="store_true")
    args = ap.parse_args()

    zf = zipfile.ZipFile(args.zip)
    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    for t in RAW_TABLES:
        load_raw_table(conn, zf, t)
    upsert_operators(conn)
    if not args.skip_production:
        load_production(conn, zf)
    conn.close()
    log("done")


if __name__ == "__main__":
    main()

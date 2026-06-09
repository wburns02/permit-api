#!/usr/bin/env python3
"""Load RRC P-5 organization file (orf850.txt.gz) into the warehouse.

Fixed-width ASCII, LRECL 350, record type in cols 1-2 (layout: ora001 manual,
extracted spec in docs/permit-intelligence-blueprint notes). Loads:
  - 'A ' org master  -> rrc_raw.p5_organizations (typed) + enrich
                        canonical.operators (org kind, renewal, status)
  - 'K ' officers    -> rrc_raw.p5_officers

Defensive parsing: 2 known records contain embedded newlines; any line whose
first two chars are not a known record id is a continuation fragment, skipped.

Run AFTER load_rrc_pdq.py (PDQ seeds canonical.operators; this enriches).
"""
import gzip
import io
import time

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
SRC = "/mnt/win11/Fedora/raw-public-data/rrc/p5/orf850.txt.gz"
SOURCE = "rrc_p5"

KNOWN = {"1T", "A ", "F ", "H ", "J ", "K ", "P ", "U ", "R "}

ORG_KIND = {
    "A": "corporation", "B": "limited_partnership", "C": "sole_proprietorship",
    "D": "partnership", "E": "trust", "F": "joint_venture", "G": "other",
    "H": "llc", "I": "unknown_legacy",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fw(line, start, length):
    """1-based fixed-width slice, stripped; None if blank."""
    v = line[start - 1:start - 1 + length].strip()
    return v or None


def d8(v):
    """CCYYMMDD -> ISO date or None (zeroes mean no date)."""
    if not v or v == "00000000" or len(v) != 8 or not v.isdigit():
        return None
    y, m, d = int(v[:4]), int(v[4:6]), int(v[6:8])
    if y < 1900 or not (1 <= m <= 12) or not (1 <= d <= 31):
        return None
    return f"{y:04d}-{m:02d}-{d:02d}"


def esc(v):
    return v.replace("\\", "\\\\").replace("\t", " ") if v else "\\N"


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS rrc_raw")
    cur.execute("DROP TABLE IF EXISTS rrc_raw.p5_organizations")
    cur.execute("""
        CREATE TABLE rrc_raw.p5_organizations (
            operator_number TEXT PRIMARY KEY,
            name TEXT,
            p5_status TEXT,
            refiling_required TEXT,
            organization_code TEXT,
            organization_kind TEXT,
            gatherer_code TEXT,
            mail_addr1 TEXT, mail_addr2 TEXT, mail_city TEXT,
            mail_state TEXT, mail_zip TEXT,
            loc_addr1 TEXT, loc_addr2 TEXT, loc_city TEXT,
            loc_state TEXT, loc_zip TEXT,
            date_built DATE,
            date_inactive DATE,
            phone TEXT,
            refile_notice_month TEXT,
            refile_received_date DATE,
            last_p5_received_date DATE,
            other_operator_number TEXT,
            tax_cert TEXT
        )
    """)
    cur.execute("DROP TABLE IF EXISTS rrc_raw.p5_officers")
    cur.execute("""
        CREATE TABLE rrc_raw.p5_officers (
            operator_number TEXT,
            officer_name TEXT,
            officer_title TEXT,
            addr1 TEXT, addr2 TEXT, city TEXT, state TEXT, zip TEXT,
            officer_or_agent TEXT
        )
    """)

    orgs, officers = [], []
    n_org = n_off = n_frag = 0
    with gzip.open(SRC, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.replace("\x00", "").rstrip("\r\n").ljust(350)
            rid = line[:2]
            if rid not in KNOWN:
                n_frag += 1
                continue
            if rid == "A ":
                code = fw(line, 45, 1)
                orgs.append("\t".join(esc(v) for v in [
                    fw(line, 3, 6), fw(line, 9, 32), fw(line, 42, 1),
                    fw(line, 41, 1), code, ORG_KIND.get(code or "", None),
                    fw(line, 66, 5),
                    fw(line, 71, 31), fw(line, 102, 31), fw(line, 133, 13),
                    fw(line, 146, 2), fw(line, 148, 5),
                    fw(line, 157, 31), fw(line, 188, 31), fw(line, 219, 13),
                    fw(line, 232, 2), fw(line, 234, 5),
                    d8(fw(line, 243, 8)), d8(fw(line, 251, 8)),
                    fw(line, 259, 10), fw(line, 269, 2),
                    d8(fw(line, 287, 8)), d8(fw(line, 295, 8)),
                    fw(line, 303, 6), fw(line, 324, 1),
                ]) + "\n")
                n_org += 1
            elif rid == "K ":
                officers.append("\t".join(esc(v) for v in [
                    fw(line, 3, 6), fw(line, 41, 32), fw(line, 73, 32),
                    fw(line, 105, 31), fw(line, 136, 31), fw(line, 167, 13),
                    fw(line, 180, 2), fw(line, 182, 5), fw(line, 300, 1),
                ]) + "\n")
                n_off += 1

    cur.copy_expert(
        "COPY rrc_raw.p5_organizations FROM STDIN",
        io.StringIO("".join(orgs)),
    )
    cur.copy_expert(
        "COPY rrc_raw.p5_officers FROM STDIN",
        io.StringIO("".join(officers)),
    )
    conn.commit()
    log(f"p5_organizations: {n_org:,}, p5_officers: {n_off:,}, fragments skipped: {n_frag}")

    cur.execute("""
        INSERT INTO canonical.operators
            (state, operator_number, name, p5_status, p5_renewal_date,
             organization_kind, lineage, freshness_at)
        SELECT 'TX', o.operator_number, o.name, o.p5_status,
               o.refile_received_date, o.organization_kind,
               jsonb_build_object('source', %s), now()
        FROM rrc_raw.p5_organizations o
        ON CONFLICT (state, operator_number) DO UPDATE SET
            p5_status = EXCLUDED.p5_status,
            p5_renewal_date = EXCLUDED.p5_renewal_date,
            organization_kind = EXCLUDED.organization_kind,
            lineage = canonical.operators.lineage || EXCLUDED.lineage,
            freshness_at = now(),
            updated_at = now()
    """, (SOURCE,))
    log(f"canonical.operators enriched: {cur.rowcount:,}")
    conn.commit()
    cur.execute("CREATE INDEX IF NOT EXISTS ix_p5_officers_op ON rrc_raw.p5_officers (operator_number)")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    main()

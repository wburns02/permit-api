"""Postgres access helper for the statewide permit loop.

WHY psql instead of psycopg2:
    In this environment psycopg2 / asyncpg hang indefinitely on the Tailscale
    path to the T430 (100.122.216.15) — the TCP port is open but the libpq
    GSSAPI/SSL negotiation never completes. The `psql` client connects in
    <1s with the identical DSN. So every DB touch in this harness shells out
    to `psql`, which is also trivially "gentle": we always pass a hard
    PGCONNECT_TIMEOUT and a per-statement timeout, and we NEVER full-scan.

Gentleness rules baked in here:
    - statement_timeout set per session (default 20s) so no query can run away
    - every read is filtered by an indexed column (`source`) or bounded by LIMIT
    - NO count(*) over the whole table, NO pg_terminate / pg_cancel, NO DDL
      beyond the optional registry table create (which is IF NOT EXISTS).
"""
from __future__ import annotations

import csv
import io
import os
import subprocess
from typing import Iterable, Sequence

# T430 over Tailscale. `will` role, no password (trust on the LAN/TS path).
PERMITS_DSN = os.getenv(
    "PERMITS_DSN",
    "postgresql://will@100.122.216.15:5432/permits",
)

# Hard ceilings. A statement that exceeds these is killed by PG, not by us.
CONNECT_TIMEOUT = os.getenv("PGCONNECT_TIMEOUT", "8")
STATEMENT_TIMEOUT_MS = os.getenv("PG_STATEMENT_TIMEOUT_MS", "20000")
SUBPROCESS_TIMEOUT = 45  # wall-clock kill for the psql process itself


class DBError(RuntimeError):
    pass


def _base_env() -> dict:
    env = dict(os.environ)
    env["PGCONNECT_TIMEOUT"] = CONNECT_TIMEOUT
    # Force the fast negotiation path; mirrors what the working psql does.
    env.setdefault("PGGSSENCMODE", "disable")
    env.setdefault("PGSSLMODE", "prefer")
    # Bound EVERY statement at the server via the session default. This is the
    # belt to the subprocess-timeout suspenders, and it keeps us off the
    # forbidden territory (no runaway scans).
    env["PGOPTIONS"] = f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"
    return env


def _run_psql(args: Sequence[str], stdin: str | None = None) -> str:
    cmd = [
        "psql",
        PERMITS_DSN,
        "-v",
        "ON_ERROR_STOP=1",
    ]
    cmd.extend(args)
    try:
        proc = subprocess.run(
            cmd,
            input=stdin,
            env=_base_env(),
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired as e:  # pragma: no cover - env dependent
        raise DBError(f"psql wall-clock timeout after {SUBPROCESS_TIMEOUT}s") from e
    if proc.returncode != 0:
        raise DBError(f"psql failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


def query(sql: str, *, tuples: bool = True) -> list[list[str]]:
    """Run a read-only SELECT. Returns rows as lists of strings.

    Caller is responsible for keeping the query bounded/indexed. This helper
    refuses anything that is not a SELECT/WITH to avoid accidental writes.
    """
    head = sql.lstrip().split(None, 1)[0].lower() if sql.strip() else ""
    if head not in {"select", "with"}:
        raise DBError(f"query() only runs SELECT/WITH, got: {head!r}")
    out = _run_psql(["-At", "-F", "\x1f", "-c", sql])
    rows: list[list[str]] = []
    for line in out.splitlines():
        if line == "":
            continue
        rows.append(line.split("\x1f"))
    return rows


def scalar(sql: str) -> str | None:
    rows = query(sql)
    if not rows:
        return None
    return rows[0][0]


def copy_in(table: str, columns: Sequence[str], records: Iterable[dict]) -> int:
    r"""Bulk-load rows via `\copy ... FROM STDIN CSV`.

    Gentle: this is a single bulk COPY, not row-by-row inserts, and it never
    locks the table beyond the COPY itself. Returns the number of rows sent.
    Values are written as CSV with NULL marked by an explicit token so empty
    strings and real NULLs stay distinguishable.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    n = 0
    for rec in records:
        row = []
        for col in columns:
            val = rec.get(col)
            row.append(r"\N" if val is None else str(val))
        writer.writerow(row)
        n += 1
    if n == 0:
        return 0
    collist = ", ".join(columns)
    copy_sql = (
        f"\\copy {table} ({collist}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
    )
    _run_psql(["-c", copy_sql], stdin=buf.getvalue())
    return n


def load_hot_leads(records: list[dict], columns: Sequence[str]) -> int:
    r"""Idempotent bulk load into hot_leads that survives the dedup unique index.

    COPY is all-or-nothing and will abort the whole batch on the first dup
    (hot_leads has a unique index on (permit_number, address, state)). So we
    COPY into a TEMP table first, then INSERT ... SELECT ... ON CONFLICT DO
    NOTHING. Single round-trip, single transaction, gentle: no row-by-row
    inserts, no locks beyond the insert, no full scan. Returns rows attempted.
    """
    if not records:
        return 0
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    n = 0
    for rec in records:
        writer.writerow([r"\N" if rec.get(c) is None else str(rec.get(c)) for c in columns])
        n += 1
    collist = ", ".join(columns)
    # One psql script read from STDIN: create a TEMP staging table shaped like
    # hot_leads, \copy the CSV into it (the data block follows the \copy line),
    # then INSERT ... SELECT ... ON CONFLICT DO NOTHING. \copy reads inline data
    # up to a terminating \. line.
    data_block = buf.getvalue()
    if not data_block.endswith("\n"):
        data_block += "\n"
    # ON COMMIT DROP requires an explicit transaction, otherwise psql autocommits
    # the CREATE and the temp table vanishes before \copy runs.
    script = (
        f"BEGIN;\n"
        f"CREATE TEMP TABLE _stage (LIKE hot_leads INCLUDING DEFAULTS) ON COMMIT DROP;\n"
        f"\\copy _stage ({collist}) FROM STDIN WITH (FORMAT csv, NULL '\\N')\n"
        f"{data_block}"
        f"\\.\n"
        f"INSERT INTO hot_leads ({collist}) SELECT {collist} FROM _stage "
        f"ON CONFLICT DO NOTHING;\n"
        f"COMMIT;\n"
    )
    _run_psql(["-f", "-"], stdin=script)
    return n


def ping() -> bool:
    try:
        return scalar("SELECT 1") == "1"
    except DBError:
        return False


if __name__ == "__main__":  # quick manual smoke test
    import json

    print(json.dumps({"ping": ping(), "dsn": PERMITS_DSN}))

"""The registry IS the state of the loop.

One SQLite row per TX jurisdiction. The harness is resumable because nothing
lives in memory: `run_loop.py` reads `pending` rows, processes them, writes the
outcome back here. Kill it at any point and re-run — it picks up where it left
off.

State machine
-------------
    pending      -> not yet attempted
    classified   -> vendor identified (intermediate; agent may skip straight on)
    built        -> agent loaded rows & self-reported success (NOT trusted yet)
    verified     -> the DETERMINISTIC verifier confirmed real data landed  <-- done
    walled       -> no obtainable permits (county w/ no permits, hard captcha,
                    paid-only portal, etc). barrier_note says why.

Only `verified` and `walled` are terminal. `built` is explicitly a way-station:
a jurisdiction the agent claims it built but the verifier has not yet blessed.
If the verifier fails a `built` row it goes back to `pending` (one retry) or to
`walled` with a "verifier rejected" note.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).with_name("registry.db")

TERMINAL = {"verified", "walled"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS jurisdictions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    jtype        TEXT NOT NULL CHECK (jtype IN ('city','county')),
    fips         TEXT,
    portal_url   TEXT,
    vendor       TEXT,
    state        TEXT NOT NULL DEFAULT 'pending',
    source_tag   TEXT UNIQUE,
    rows_loaded  INTEGER DEFAULT 0,
    has_reroof   INTEGER,            -- 0/1/NULL
    barrier_note TEXT,
    attempts     INTEGER DEFAULT 0,
    agent_status TEXT,               -- what the agent self-reported (audit trail)
    updated_at   REAL
);
CREATE INDEX IF NOT EXISTS ix_state ON jurisdictions(state);
"""


def connect(path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def slug(name: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in name).strip("_")


def make_source_tag(name: str, jtype: str) -> str:
    """Unique, stable source_tag used both as hot_leads.source and the verifier
    key. Namespaced so it can never collide with existing scraper sources."""
    return f"statewide_loop:tx_{jtype}_{slug(name)}"


def seed(conn: sqlite3.Connection, rows: list[dict]) -> int:
    added = 0
    for r in rows:
        tag = make_source_tag(r["name"], r["jtype"])
        existing = conn.execute(
            "SELECT id FROM jurisdictions WHERE source_tag = ?", (tag,)
        ).fetchone()
        if existing:
            continue
        conn.execute(
            """INSERT INTO jurisdictions
               (name, jtype, fips, portal_url, vendor, state, source_tag, updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                r["name"],
                r["jtype"],
                r.get("fips"),
                r.get("portal_url"),
                r.get("vendor"),
                r.get("state", "pending"),
                tag,
                time.time(),
            ),
        )
        added += 1
    conn.commit()
    return added


def next_pending(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM jurisdictions WHERE state = 'pending' "
        "ORDER BY id LIMIT ?",
        (limit,),
    ).fetchall()


def update(conn: sqlite3.Connection, jid: int, **fields) -> None:
    fields["updated_at"] = time.time()
    cols = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(
        f"UPDATE jurisdictions SET {cols} WHERE id = ?",
        (*fields.values(), jid),
    )
    conn.commit()


def counts(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT state, COUNT(*) c FROM jurisdictions GROUP BY state"
    ).fetchall()
    return {r["state"]: r["c"] for r in rows}


def all_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM jurisdictions ORDER BY id").fetchall()

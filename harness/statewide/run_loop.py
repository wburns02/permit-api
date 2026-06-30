#!/usr/bin/env python3
"""The harness driver for the statewide TX permit-scraping loop.

A real autonomous loop is a harness with BACKPRESSURE. For each jurisdiction:

    1. fill the per-jurisdiction prompt template
    2. spawn `claude -p --model sonnet <prompt>`  (Claude SUBSCRIPTION CLI,
       flat-rate; NEVER the metered Anthropic API)
    3. parse the agent's final JSON (its self-reported status is NOT trusted)
    4. run the DETERMINISTIC VERIFIER (verify.py) and GATE ON THAT
    5. write the outcome to the registry (the resumable state)

Model: per-jurisdiction agent calls use Sonnet (config constant MODEL below).
Sonnet is the right tier for bulk recon/build and conserves subscription quota
(Opus would burn it ~5x faster across 1,200 jurisdictions). The verifier and
this driver are deterministic code — no model involved.

Resumable: kill it anytime; re-run picks up `pending` rows from registry.db.

Bounded resource use (hardened after the 2026-06-29 memory hard-lock):
  - At most MAX_PARALLEL concurrent `claude -p` processes (default 2).
  - Each agent runs with `--disallowed-tools Task`, so a driver cannot spawn
    its own subagent pool — total claude procs == parallel, not parallel × N.
  - wait_for_memory() holds new spawns while MemAvailable is below
    MIN_AVAIL_GIB, so the loop never drives the host into swap.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

# portal_url allowlist: only .gov, .us, and known SaaS permit vendors.
# Prevents prompt-injection payloads at arbitrary attacker-controlled URLs from
# reaching an agent running with bypassPermissions.
_SAFE_PORTAL_DOMAINS = re.compile(
    r"^([a-zA-Z0-9._-]+\.(gov|us)"
    r"|[a-zA-Z0-9._-]+\.viewpointcloud\.com"
    r"|[a-zA-Z0-9._-]+\.mygovonline\.com"
    r"|aca-prod\.accela\.com"
    r"|[a-zA-Z0-9._-]+\.citizenserve\.com"
    r"|[a-zA-Z0-9._-]+\.energovprod\.com"
    r"|[a-zA-Z0-9._-]+\.tylertech\.com"
    r")$",
    re.IGNORECASE,
)

import registry
import seed_data
import verify

HERE = Path(__file__).resolve().parent
# Run `claude -p` from the permit-api repo root so the agent has the full
# framework (scripts/, db.py, Playwright, psql) on hand.
REPO_ROOT = HERE.parent.parent
PROMPT_TEMPLATE = (HERE / "jurisdiction_prompt.md").read_text()

# ── Model / cost config ──────────────────────────────────────────────────────
# Sonnet for the bulk per-jurisdiction work. Change here to retune tier.
MODEL = os.getenv("STATEWIDE_LOOP_MODEL", "sonnet")
# We invoke the Claude SUBSCRIPTION CLI (`claude -p`), flat-rate. We explicitly
# do NOT set ANTHROPIC_API_KEY (it's disabled for cost); claude-cli uses the
# logged-in subscription session.
CLAUDE_BIN = os.getenv("CLAUDE_BIN", "claude")

MAX_PARALLEL = int(os.getenv("STATEWIDE_LOOP_PARALLEL", "2"))
AGENT_TIMEOUT_S = int(os.getenv("STATEWIDE_LOOP_AGENT_TIMEOUT", "900"))  # 15 min/juris
MAX_ATTEMPTS = 2  # a built-but-failed-verify row gets one retry, then walled

# Memory backstop: never spawn a new agent while the box is low on RAM. The
# 2026-06-29 hard-lock came from claude sessions stacked on QIDI+Chrome until
# swap saturated. systemd-oomd backstops the host now, but this loop should not
# be the thing that pushes it there. Floor is RAM-available, not total.
MIN_AVAIL_GIB = float(os.getenv("STATEWIDE_LOOP_MIN_AVAIL_GB", "6"))
MEM_WAIT_POLL_S = 15
MEM_WAIT_MAX_POLLS = 20  # ~5 min of throttling, then proceed (oomd will catch it)


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def _avail_gib() -> float:
    """RAM currently available (GiB), from /proc/meminfo. Returns +inf if it
    can't be read so a parse failure never blocks the loop."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024 / 1024
    except (OSError, ValueError, IndexError):
        pass
    return float("inf")


def wait_for_memory() -> None:
    """Throttle new spawns when RAM is tight. Blocks the refill of a freed slot
    (in-flight agents keep running) until MemAvailable recovers above the floor,
    or until the poll budget is spent — then proceeds and lets oomd backstop."""
    for _ in range(MEM_WAIT_MAX_POLLS):
        avail = _avail_gib()
        if avail >= MIN_AVAIL_GIB:
            return
        log(f"  [mem  ] avail={avail:.1f}G < {MIN_AVAIL_GIB}G floor — holding new spawn {MEM_WAIT_POLL_S}s")
        time.sleep(MEM_WAIT_POLL_S)
    log(f"  [mem  ] still low after {MEM_WAIT_MAX_POLLS * MEM_WAIT_POLL_S}s — proceeding (oomd backstops)")


def safe_portal_url(url: str | None) -> str:
    """Only pass a portal_url straight to the agent if its host is on the
    allowlist (.gov/.us + known permit-SaaS vendors). Anything else is flagged
    so a poisoned registry row can't silently hand an attacker URL to an agent
    running with bypassPermissions — the agent is told to confirm it first."""
    if not url:
        return "? (none on file — discover it)"
    try:
        host = urllib.parse.urlparse(url).hostname or ""
    except ValueError:
        host = ""
    if host and _SAFE_PORTAL_DOMAINS.match(host):
        return url
    return f"{url} (UNVERIFIED host — confirm via official .gov before trusting)"


def fill_prompt(row) -> str:
    repl = {
        "{{NAME}}": row["name"],
        "{{JTYPE}}": row["jtype"],
        "{{FIPS}}": row["fips"] or "?",
        "{{PORTAL_URL}}": safe_portal_url(row["portal_url"]),
        "{{VENDOR}}": row["vendor"] or "unknown",
        "{{SOURCE_TAG}}": row["source_tag"],
    }
    out = PROMPT_TEMPLATE
    for k, v in repl.items():
        out = out.replace(k, str(v))
    return out


def extract_agent_json(text: str) -> dict | None:
    """The agent's final message should contain one JSON object. Pull the last
    valid JSON object that has a `source_tag` key."""
    candidates = re.findall(r"\{[^{}]*\"source_tag\"[^{}]*\}", text, re.DOTALL)
    for c in reversed(candidates):
        try:
            return json.loads(c)
        except json.JSONDecodeError:
            continue
    # Fallback: try the whole thing if it's pure JSON.
    try:
        obj = json.loads(text.strip())
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def run_agent(row) -> dict:
    """Spawn `claude -p --model sonnet` for one jurisdiction. Returns the parsed
    agent JSON (or a synthetic error dict). NEVER trusted for the verdict.

    Note on the portal_url allowlist: we do NOT hard-reject an off-allowlist URL
    here, because the whole point of the unknown/walled bucket is that the agent
    rediscovers the real portal via search even when our seed URL is a `.org` or
    a guess. Instead `safe_portal_url()` (in fill_prompt) FLAGS an unverified
    host in the prompt so the agent treats it as untrusted. The hard isolation
    that bypassPermissions really wants is a sandbox (Docker/firejail) — that is
    the next hardening step before unleashing this statewide.
    """
    prompt = fill_prompt(row)
    # bypassPermissions is required: sub-agents must run psql, curl, and
    # Playwright unattended. --strict-mcp-config + empty MCP config strips the
    # heavy user-level MCP servers (cognee-over-SSH, context7, sequential-thinking)
    # that otherwise add ~minutes of per-spawn startup tax and broaden the tool
    # surface. A scraper agent needs Bash/curl/psql/Playwright, not those.
    cmd = [
        CLAUDE_BIN, "-p", prompt,
        "--model", MODEL,
        "--output-format", "json",
        "--permission-mode", "bypassPermissions",
        "--strict-mcp-config",
        "--mcp-config", '{"mcpServers": {}}',
        # No recursive subagent fan-out. A single-jurisdiction scrape is
        # sequential work (curl/Playwright/psql); letting each driver spawn its
        # own Task subagent pool was the unbounded process multiplier behind the
        # 2026-06-29 memory hard-lock. This pins total claude procs at parallel.
        "--disallowed-tools", "Task",
    ]
    env = dict(os.environ)
    env.pop("ANTHROPIC_API_KEY", None)  # force subscription path, never metered API
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(REPO_ROOT), env=env,
            capture_output=True, text=True, timeout=AGENT_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        return {"_error": f"agent timeout after {AGENT_TIMEOUT_S}s", "_elapsed": AGENT_TIMEOUT_S}
    elapsed = round(time.time() - t0, 1)
    if proc.returncode != 0:
        return {"_error": f"claude rc={proc.returncode}: {proc.stderr[:400]}", "_elapsed": elapsed}

    # --output-format json wraps the result; the agent's text is in .result
    final_text = proc.stdout
    try:
        wrapper = json.loads(proc.stdout)
        final_text = wrapper.get("result", proc.stdout)
    except json.JSONDecodeError:
        pass
    agent = extract_agent_json(final_text) or {}
    agent["_elapsed"] = elapsed
    return agent


def process(row_dict: dict) -> dict:
    """Full pipeline for one jurisdiction: agent -> verifier -> verdict.
    Pure function of its input dict; the caller writes the result to registry."""
    name = row_dict["name"]
    tag = row_dict["source_tag"]
    log(f"  [start] {name} ({row_dict['jtype']}, guess={row_dict['vendor']})")

    agent = run_agent(row_dict)
    agent_status = agent.get("status") or ("error" if "_error" in agent else "?")
    source_url = agent.get("source_url")
    elapsed = agent.get("_elapsed", 0)

    # GATE: run the deterministic verifier regardless of what the agent claimed.
    vres = verify.verify(tag, source_url)
    rows_loaded = vres.stats.get("row_count_bounded", 0)

    if vres.passed:
        final_state = "verified"
    else:
        # Verifier rejected. If the agent honestly said "walled", honor that
        # with its barrier. Otherwise it's a built-but-garbage rejection.
        if agent.get("status") == "walled":
            final_state = "walled"
        else:
            final_state = "verify_failed"

    log(
        f"  [done ] {name}: agent={agent_status} verify={'PASS' if vres.passed else 'FAIL'} "
        f"rows={rows_loaded} ({elapsed}s) :: {vres.reason}"
    )
    return {
        "name": name,
        "source_tag": tag,
        "final_state": final_state,
        "agent_status": agent_status,
        "vendor": agent.get("vendor") or row_dict["vendor"],
        "rows_loaded": rows_loaded,
        "has_reroof": 1 if agent.get("has_reroof") else 0,
        "barrier": (
            agent.get("barrier_if_walled")
            if final_state == "walled"
            else (None if vres.passed else f"verifier: {vres.reason}")
        ),
        "verify_reason": vres.reason,
    }


def commit_result(conn, jid: int, res: dict) -> None:
    state = res["final_state"]
    if state == "verify_failed":
        # one retry, then wall it
        row = conn.execute("SELECT attempts FROM jurisdictions WHERE id=?", (jid,)).fetchone()
        attempts = (row["attempts"] or 0) + 1
        if attempts < MAX_ATTEMPTS:
            registry.update(
                conn, jid, state="pending", attempts=attempts,
                agent_status=res["agent_status"],
                barrier_note=f"retry {attempts}: {res['verify_reason']}",
            )
            return
        registry.update(
            conn, jid, state="walled", attempts=attempts,
            agent_status=res["agent_status"], vendor=res["vendor"],
            rows_loaded=res["rows_loaded"],
            barrier_note=f"verifier rejected after {attempts} attempts: {res['verify_reason']}",
        )
        return
    registry.update(
        conn, jid, state=state, vendor=res["vendor"],
        rows_loaded=res["rows_loaded"], has_reroof=res["has_reroof"],
        agent_status=res["agent_status"], barrier_note=res["barrier"],
        attempts=(conn.execute("SELECT attempts FROM jurisdictions WHERE id=?", (jid,)).fetchone()["attempts"] or 0) + 1,
    )


def run(limit: int | None, parallel: int) -> None:
    """Continuous-refill pool: keep `parallel` agents in flight at all times.

    A jurisdiction with a slow/over-exploring agent must NOT stall the others, so
    we refill a freed slot with the next `pending` row immediately rather than
    draining a whole batch first. The registry is the queue; `in_flight` tracks
    rows handed out this run so we never double-submit one.
    """
    conn = registry.connect()
    log(f"model={MODEL!r} parallel={parallel} repo_root={REPO_ROOT}")
    log(f"registry state at start: {registry.counts(conn)}")
    processed = 0
    in_flight: set[int] = set()

    def claimable() -> dict | None:
        if limit is not None and processed >= limit:
            return None
        for r in registry.next_pending(conn, parallel * 4):
            if r["id"] not in in_flight:
                return dict(r)
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as ex:
        futs: dict[concurrent.futures.Future, dict] = {}
        # prime the pool
        while len(futs) < parallel:
            row = claimable()
            if row is None:
                break
            wait_for_memory()
            in_flight.add(row["id"])
            futs[ex.submit(process, row)] = row
        # drain + refill
        while futs:
            done, _ = concurrent.futures.wait(
                futs, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                d = futs.pop(fut)
                in_flight.discard(d["id"])
                try:
                    res = fut.result()
                    commit_result(conn, d["id"], res)
                except Exception as e:  # never let one juris kill the loop
                    log(f"  [error] {d['name']}: {e!r}")
                    registry.update(conn, d["id"], state="pending",
                                    barrier_note=f"harness error: {e!r}",
                                    attempts=(d.get("attempts") or 0) + 1)
                processed += 1
                # refill the freed slot
                row = claimable()
                if row is not None:
                    wait_for_memory()
                    in_flight.add(row["id"])
                    futs[ex.submit(process, row)] = row
    log(f"registry state at end: {registry.counts(conn)}")


def cmd_seed(_args) -> None:
    conn = registry.connect()
    added = registry.seed(conn, seed_data.SEED)
    log(f"seeded {added} new jurisdictions (total rows: {len(registry.all_rows(conn))})")
    log(f"state: {registry.counts(conn)}")


def cmd_status(_args) -> None:
    conn = registry.connect()
    print(json.dumps(registry.counts(conn), indent=2))
    for r in registry.all_rows(conn):
        print(
            f"  {r['id']:>2} {r['state']:<13} {r['jtype']:<6} {r['name']:<18} "
            f"vendor={r['vendor'] or '?':<12} rows={r['rows_loaded'] or 0:<4} "
            f"{(r['barrier_note'] or '')[:60]}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Statewide TX permit-scraping loop")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("seed", help="seed the registry with the pilot jurisdictions")
    sub.add_parser("status", help="print registry state")

    pr = sub.add_parser("run", help="drive the loop over pending jurisdictions")
    pr.add_argument("--limit", type=int, default=None, help="max jurisdictions this run")
    pr.add_argument("--parallel", type=int, default=MAX_PARALLEL)

    args = ap.parse_args()
    if args.cmd == "seed":
        cmd_seed(args)
    elif args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "run":
        run(args.limit, args.parallel)
    return 0


if __name__ == "__main__":
    sys.exit(main())

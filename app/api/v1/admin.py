"""Admin-only Scraper Operations Dashboard endpoints."""

import json
import os
import platform
import re
import subprocess
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, BackgroundTasks, Query, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, async_session_maker
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser

router = APIRouter(prefix="/admin", tags=["Admin"])

# ── Config ────────────────────────────────────────────────────────────────────
ADMIN_EMAILS = ["will@ecbtx.com", "admin@ecbtx.com", "willwalterburns@gmail.com"]
SCRIPTS_DIR = Path("/home/will/CrownHardware/backend/scripts")
LOGS_DIR = Path("/mnt/win11/Fedora/crown_scrapers/logs")
DATA_DIR = Path("/mnt/win11/Fedora/crown_scrapers")
SCHEDULES_FILE = DATA_DIR / "schedules.json"

# State abbreviation map for parsing filenames
STATE_ABBREVS = {
    "al": "AL", "ak": "AK", "az": "AZ", "ar": "AR", "ca": "CA",
    "co": "CO", "ct": "CT", "de": "DE", "fl": "FL", "ga": "GA",
    "hi": "HI", "id": "ID", "il": "IL", "in": "IN", "ia": "IA",
    "ks": "KS", "ky": "KY", "la": "LA", "me": "ME", "md": "MD",
    "ma": "MA", "mi": "MI", "mn": "MN", "ms": "MS", "mo": "MO",
    "mt": "MT", "ne": "NE", "nv": "NV", "nh": "NH", "nj": "NJ",
    "nm": "NM", "ny": "NY", "nc": "NC", "nd": "ND", "oh": "OH",
    "ok": "OK", "or": "OR", "pa": "PA", "ri": "RI", "sc": "SC",
    "sd": "SD", "tn": "TN", "tx": "TX", "ut": "UT", "vt": "VT",
    "va": "VA", "wa": "WA", "wv": "WV", "wi": "WI", "wy": "WY",
    "dc": "DC",
}

ALL_US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
]

# Running jobs tracker (in-memory)
_running_jobs: dict[str, dict] = {}


# ── Auth helper ───────────────────────────────────────────────────────────────

async def require_admin(user: ApiUser = Depends(get_current_user)) -> ApiUser:
    """Dependency that ensures the authenticated user is an admin."""
    if not user or user.email not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user


# ── Helpers ───────────────────────────────────────────────────────────────────

def _categorize_script(filename: str) -> str:
    """Categorize a script based on its filename."""
    name = filename.lower()
    if name.startswith("scrape_") and "parcel" in name:
        return "parcel"
    if name.startswith("scrape_") and ("permit" in name or "socrata" in name):
        return "permit"
    if "b2b" in name or "lead" in name:
        return "b2b"
    if "enrich" in name or "batchdata" in name or "reverse_lookup" in name or "phone" in name or "contact" in name:
        return "enrichment"
    if "qwen" in name or "classifier" in name or "scorer" in name:
        return "ai"
    if name.startswith("import_"):
        return "import"
    return "utility"


def _parse_state(filename: str) -> Optional[str]:
    """Try to extract a US state abbreviation from the filename."""
    name = filename.lower().replace(".py", "")
    # Patterns: scrape_al_parcels, scrape_fl_duval, scrape_tx_harris
    parts = name.split("_")
    for part in parts:
        if part in STATE_ABBREVS:
            return STATE_ABBREVS[part]
    # Check for city-based names that imply a state
    city_state_map = {
        "austin": "TX", "dallas": "TX", "harris": "TX", "bexar": "TX", "tarrant": "TX",
        "chicago": "IL", "cook": "IL",
        "nyc": "NY", "ny": "NY",
        "sf": "CA", "sacramento": "CA", "sandiego": "CA",
        "denver": "CO",
        "duval": "FL", "hillsborough": "FL", "palmbeach": "FL",
        "fulton": "GA",
        "ada": "ID",
        "douglas": "NE",
        "clark": "NV",
        "wake": "NC", "onemap": "NC",
        "multnomah": "OR",
        "philadelphia": "PA",
        "greenville": "SC",
        "shelby": "TN",
        "saltlake": "UT",
        "richmond": "VA",
        "king": "WA",
        "milwaukee": "WI",
        "southern": None,  # multi-state
    }
    for part in parts:
        if part in city_state_map:
            return city_state_map[part]
    return None


def _find_latest_log(scraper_name: str) -> Optional[Path]:
    """Find the most recent log file for a scraper."""
    if not LOGS_DIR.exists():
        return None
    base = scraper_name.replace(".py", "")
    candidates = []
    for f in LOGS_DIR.iterdir():
        if f.is_file() and f.suffix == ".log":
            # Match scraper_name*.log or base*.log
            fname = f.stem.lower()
            if fname.startswith(base.lower()) or base.lower().replace("scrape_", "") in fname:
                candidates.append(f)
    if not candidates:
        return None
    return max(candidates, key=lambda f: f.stat().st_mtime)


def _parse_log_info(log_path: Path) -> dict:
    """Parse basic info from a log file."""
    info = {
        "last_run_time": None,
        "success": None,
        "records_count": None,
        "duration_seconds": None,
        "log_file": log_path.name,
    }
    try:
        stat = log_path.stat()
        info["last_run_time"] = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()

        # Read last 50 lines for status parsing
        content = log_path.read_text(errors="replace")
        lines = content.strip().split("\n")
        last_lines = "\n".join(lines[-50:]).lower()

        # Determine success/failure
        if "error" in last_lines or "traceback" in last_lines or "failed" in last_lines:
            info["success"] = False
        elif "complete" in last_lines or "finished" in last_lines or "done" in last_lines or "success" in last_lines:
            info["success"] = True

        # Parse record counts
        for pattern in [
            r"(\d[\d,]*)\s*(?:records?|rows?|permits?|parcels?)\s*(?:processed|inserted|scraped|imported|found|extracted)",
            r"(?:total|count|processed|inserted|scraped|imported|found|extracted)[:\s]*(\d[\d,]*)",
            r"(\d[\d,]*)\s*total",
        ]:
            match = re.search(pattern, last_lines)
            if match:
                info["records_count"] = int(match.group(1).replace(",", ""))
                break

        # Parse duration
        for pattern in [
            r"(?:duration|elapsed|took|time)[:\s]*(\d+(?:\.\d+)?)\s*(?:s|sec|seconds)",
            r"(\d+(?:\.\d+)?)\s*seconds",
            r"(\d+)m\s*(\d+)s",
        ]:
            match = re.search(pattern, last_lines)
            if match:
                groups = match.groups()
                if len(groups) == 2:  # minutes + seconds
                    info["duration_seconds"] = int(groups[0]) * 60 + int(groups[1])
                else:
                    info["duration_seconds"] = round(float(groups[0]))
                break

    except Exception:
        pass
    return info


def _get_scraper_info(filename: str) -> dict:
    """Build info dict for a single scraper script."""
    filepath = SCRIPTS_DIR / filename
    stat = filepath.stat()
    category = _categorize_script(filename)
    state = _parse_state(filename)

    info = {
        "filename": filename,
        "category": category,
        "target_state": state,
        "file_size": stat.st_size,
        "last_modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "last_run_time": None,
        "success": None,
        "records_count": None,
        "duration_seconds": None,
        "log_file": None,
    }

    log = _find_latest_log(filename)
    if log:
        log_info = _parse_log_info(log)
        info.update(log_info)

    return info


def _get_disk_usage(path: Path) -> dict:
    """Get disk usage for a path."""
    try:
        result = subprocess.run(
            ["du", "-sb", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            total_bytes = int(result.stdout.split()[0])
        else:
            total_bytes = 0

        # Also get the filesystem info
        result2 = subprocess.run(
            ["df", "-B1", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        fs_total = fs_used = fs_free = 0
        if result2.returncode == 0:
            lines = result2.stdout.strip().split("\n")
            if len(lines) > 1:
                parts = lines[1].split()
                fs_total = int(parts[1])
                fs_used = int(parts[2])
                fs_free = int(parts[3])

        return {
            "path": str(path),
            "scrapers_bytes": total_bytes,
            "scrapers_human": _human_bytes(total_bytes),
            "fs_total_bytes": fs_total,
            "fs_used_bytes": fs_used,
            "fs_free_bytes": fs_free,
            "fs_total_human": _human_bytes(fs_total),
            "fs_used_human": _human_bytes(fs_used),
            "fs_free_human": _human_bytes(fs_free),
            "fs_used_pct": round(fs_used / fs_total * 100, 1) if fs_total else 0,
        }
    except Exception as e:
        return {"path": str(path), "error": str(e)}


def _human_bytes(n: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _get_system_info() -> dict:
    """Get system information."""
    info = {
        "hostname": platform.node(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
    }
    try:
        with open("/proc/uptime") as f:
            uptime_seconds = float(f.read().split()[0])
            days = int(uptime_seconds // 86400)
            hours = int((uptime_seconds % 86400) // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            info["uptime"] = f"{days}d {hours}h {minutes}m"
            info["uptime_seconds"] = round(uptime_seconds)
    except Exception:
        info["uptime"] = "unknown"
        info["uptime_seconds"] = 0

    try:
        load = os.getloadavg()
        info["load_average"] = [round(l, 2) for l in load]
    except Exception:
        info["load_average"] = [0, 0, 0]

    return info


def _run_scraper_background(filename: str, job_id: str):
    """Run a scraper script in a subprocess, logging output."""
    script_path = SCRIPTS_DIR / filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"{filename.replace('.py', '')}_{timestamp}.log"

    # Ensure logs dir exists
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    _running_jobs[job_id]["status"] = "running"
    _running_jobs[job_id]["log_file"] = str(log_file)
    _running_jobs[job_id]["started_at"] = datetime.now(timezone.utc).isoformat()

    try:
        with open(log_file, "w") as lf:
            lf.write(f"=== Scraper: {filename} ===\n")
            lf.write(f"=== Job ID: {job_id} ===\n")
            lf.write(f"=== Started: {datetime.now(timezone.utc).isoformat()} ===\n\n")

            result = subprocess.run(
                ["python3", str(script_path)],
                capture_output=False,
                stdout=lf,
                stderr=subprocess.STDOUT,
                timeout=3600,  # 1 hour max
                cwd=str(SCRIPTS_DIR.parent),
            )

            lf.write(f"\n\n=== Exit code: {result.returncode} ===\n")
            lf.write(f"=== Finished: {datetime.now(timezone.utc).isoformat()} ===\n")

            if result.returncode == 0:
                lf.write("=== Status: complete ===\n")
                _running_jobs[job_id]["status"] = "complete"
            else:
                lf.write("=== Status: error ===\n")
                _running_jobs[job_id]["status"] = "error"

    except subprocess.TimeoutExpired:
        _running_jobs[job_id]["status"] = "timeout"
        with open(log_file, "a") as lf:
            lf.write("\n=== TIMEOUT after 3600s ===\n")
    except Exception as e:
        _running_jobs[job_id]["status"] = "error"
        with open(log_file, "a") as lf:
            lf.write(f"\n=== ERROR: {e} ===\n")

    _running_jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


# ── Schemas ───────────────────────────────────────────────────────────────────

class ScheduleRequest(BaseModel):
    filename: str
    hour: int
    minute: int
    enabled: bool = True


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/scrapers")
async def list_scrapers(user: ApiUser = Depends(require_admin)):
    """List all scraper scripts with status info."""
    if not SCRIPTS_DIR.exists():
        raise HTTPException(status_code=500, detail="Scripts directory not found.")

    scrapers = []
    for f in sorted(SCRIPTS_DIR.glob("*.py")):
        if f.name.startswith("__"):
            continue
        scrapers.append(_get_scraper_info(f.name))

    # Sort by category, then by last_run (most recent first within category)
    category_order = {"parcel": 0, "permit": 1, "b2b": 2, "enrichment": 3, "ai": 4, "import": 5, "utility": 6}
    scrapers.sort(key=lambda s: (
        category_order.get(s["category"], 99),
        -(datetime.fromisoformat(s["last_run_time"]).timestamp() if s["last_run_time"] else 0),
    ))

    return {"scrapers": scrapers, "total": len(scrapers)}


@router.get("/scrapers/{filename}/logs")
async def get_scraper_logs(filename: str, user: ApiUser = Depends(require_admin)):
    """Get last 200 lines of the most recent log for a scraper."""
    if not filename.endswith(".py"):
        filename += ".py"

    log = _find_latest_log(filename)
    if not log:
        raise HTTPException(status_code=404, detail=f"No logs found for {filename}")

    try:
        content = log.read_text(errors="replace")
        lines = content.strip().split("\n")
        last_200 = lines[-200:]
        return {
            "filename": filename,
            "log_file": log.name,
            "lines": last_200,
            "total_lines": len(lines),
            "truncated": len(lines) > 200,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading log: {e}")


@router.post("/scrapers/{filename}/run")
async def run_scraper(
    filename: str,
    background_tasks: BackgroundTasks,
    user: ApiUser = Depends(require_admin),
):
    """Trigger a scraper to run in the background."""
    if not filename.endswith(".py"):
        filename += ".py"

    script_path = SCRIPTS_DIR / filename
    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"Script {filename} not found.")

    # Check if already running
    for jid, job in _running_jobs.items():
        if job["filename"] == filename and job["status"] == "running":
            raise HTTPException(
                status_code=409,
                detail=f"Scraper {filename} is already running (job {jid}).",
            )

    job_id = str(uuid.uuid4())[:8]
    _running_jobs[job_id] = {
        "job_id": job_id,
        "filename": filename,
        "status": "queued",
        "started_at": None,
        "finished_at": None,
        "log_file": None,
    }

    background_tasks.add_task(_run_scraper_background, filename, job_id)

    return {
        "job_id": job_id,
        "filename": filename,
        "status": "queued",
        "message": f"Scraper {filename} queued for execution.",
    }


@router.get("/jobs")
async def list_jobs(user: ApiUser = Depends(require_admin)):
    """List all tracked scraper jobs."""
    return {"jobs": list(_running_jobs.values())}


@router.get("/dashboard")
async def admin_dashboard(user: ApiUser = Depends(require_admin)):
    """Admin overview with system stats, scraper health, and database info."""
    # Gather scrapers
    scrapers = []
    if SCRIPTS_DIR.exists():
        for f in sorted(SCRIPTS_DIR.glob("*.py")):
            if not f.name.startswith("__"):
                scrapers.append(_get_scraper_info(f.name))

    now = datetime.now(timezone.utc)
    day_ago = now - timedelta(hours=24)
    week_ago = now - timedelta(days=7)

    # Scrapers by category
    by_category = {}
    for s in scrapers:
        cat = s["category"]
        by_category[cat] = by_category.get(cat, 0) + 1

    # Scrapers by state
    by_state = {}
    for s in scrapers:
        st = s["target_state"] or "unknown"
        by_state[st] = by_state.get(st, 0) + 1

    # Last 24h runs
    last_24h = []
    stale = []
    failing = []
    for s in scrapers:
        if s["last_run_time"]:
            run_time = datetime.fromisoformat(s["last_run_time"])
            if run_time >= day_ago:
                last_24h.append({
                    "filename": s["filename"],
                    "last_run_time": s["last_run_time"],
                    "success": s["success"],
                    "records_count": s["records_count"],
                    "duration_seconds": s["duration_seconds"],
                })
            if run_time < week_ago:
                stale.append({
                    "filename": s["filename"],
                    "last_run_time": s["last_run_time"],
                    "category": s["category"],
                    "target_state": s["target_state"],
                })
            if s["success"] is False:
                failing.append({
                    "filename": s["filename"],
                    "last_run_time": s["last_run_time"],
                    "category": s["category"],
                })

    # Database record count (approximate for speed)
    total_records = 0
    records_by_state = {}
    try:
        async with async_session_maker() as db:
            # Use reltuples for fast approximate count
            r = await db.execute(text(
                "SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'"
            ))
            row = r.scalar()
            total_records = int(row) if row and row > 0 else 0

            # If approximate is 0 or very small, do exact count
            if total_records < 100:
                r2 = await db.execute(text("SELECT COUNT(*) FROM permits"))
                total_records = r2.scalar() or 0

            # Records by state - top 20
            r3 = await db.execute(text(
                "SELECT state, COUNT(*) as cnt FROM permits "
                "WHERE state IS NOT NULL "
                "GROUP BY state ORDER BY cnt DESC LIMIT 20"
            ))
            records_by_state = {row.state: row.cnt for row in r3.fetchall()}
    except Exception:
        pass

    # Disk usage
    disk = _get_disk_usage(DATA_DIR) if DATA_DIR.exists() else {"path": str(DATA_DIR), "error": "not found"}

    # System info
    sys_info = _get_system_info()

    return {
        "total_scrapers": len(scrapers),
        "scrapers_by_category": by_category,
        "scrapers_by_state": by_state,
        "total_records": total_records,
        "records_by_state": records_by_state,
        "last_24h_runs": sorted(last_24h, key=lambda x: x["last_run_time"], reverse=True),
        "stale_scrapers": stale,
        "failing_scrapers": failing,
        "disk_usage": disk,
        "system_info": sys_info,
        "running_jobs": [j for j in _running_jobs.values() if j["status"] == "running"],
    }


@router.get("/schedules")
async def get_schedules(user: ApiUser = Depends(require_admin)):
    """Get cron schedule info and saved schedule configs."""
    # Read saved schedules from JSON
    saved = {}
    if SCHEDULES_FILE.exists():
        try:
            saved = json.loads(SCHEDULES_FILE.read_text())
        except Exception:
            pass

    # Try to read system crontab
    cron_entries = []
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(None, 5)
                if len(parts) >= 6:
                    cron_entries.append({
                        "minute": parts[0],
                        "hour": parts[1],
                        "day": parts[2],
                        "month": parts[3],
                        "weekday": parts[4],
                        "command": parts[5],
                        "raw": line,
                    })
    except Exception:
        pass

    return {
        "saved_schedules": saved,
        "system_crontab": cron_entries,
    }


@router.post("/schedules")
async def save_schedule(body: ScheduleRequest, user: ApiUser = Depends(require_admin)):
    """Save a schedule config for a scraper (stored in JSON, does not modify crontab)."""
    # Load existing
    saved = {}
    if SCHEDULES_FILE.exists():
        try:
            saved = json.loads(SCHEDULES_FILE.read_text())
        except Exception:
            pass

    saved[body.filename] = {
        "hour": body.hour,
        "minute": body.minute,
        "enabled": body.enabled,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": user.email,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SCHEDULES_FILE.write_text(json.dumps(saved, indent=2))

    return {"message": f"Schedule saved for {body.filename}", "schedule": saved[body.filename]}


@router.get("/lifecycle")
async def lifecycle_stats(
    user: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Data lifecycle: HOT/WARM/MILD/COLD counts with clickable lead access."""
    now = datetime.now(timezone.utc)
    tiers = {
        "hot": {"label": "HOT", "days": 30, "emoji": "🔥", "color": "#ef4444"},
        "warm": {"label": "WARM", "days": 90, "emoji": "☀️", "color": "#f59e0b"},
        "mild": {"label": "MILD", "days": 180, "emoji": "🌤️", "color": "#06b6d4"},
        "cold": {"label": "COLD", "days": 99999, "emoji": "❄️", "color": "#64748b"},
    }

    results = {}
    prev_cutoff = None
    for tier_key, tier_info in tiers.items():
        cutoff = now - timedelta(days=tier_info["days"])
        try:
            if prev_cutoff is None:
                # HOT: date_created >= now - 30 days
                r = await db.execute(text(
                    "SELECT COUNT(*) FROM permits WHERE date_created >= :cutoff"
                ), {"cutoff": cutoff})
            elif tier_key == "cold":
                # COLD: date_created < now - 180 days
                r = await db.execute(text(
                    "SELECT COUNT(*) FROM permits WHERE date_created < :cutoff"
                ), {"cutoff": prev_cutoff})
            else:
                # WARM/MILD: between cutoffs
                r = await db.execute(text(
                    "SELECT COUNT(*) FROM permits WHERE date_created >= :cutoff AND date_created < :prev"
                ), {"cutoff": cutoff, "prev": prev_cutoff})
            count = r.scalar() or 0
        except Exception:
            count = 0
        results[tier_key] = {**tier_info, "count": count}
        prev_cutoff = cutoff

    return {"tiers": results, "generated_at": now.isoformat()}


@router.get("/leads/{tier}")
async def get_leads_by_tier(
    tier: str,
    user: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(50, ge=1, le=50),
):
    """Get permit leads by freshness tier — clickable from admin dashboard."""
    now = datetime.now(timezone.utc)
    tier_days = {"hot": 30, "warm": 90, "mild": 180, "cold": 99999}
    if tier not in tier_days:
        raise HTTPException(status_code=400, detail="Invalid tier. Use: hot, warm, mild, cold")

    days = tier_days[tier]
    cutoff = now - timedelta(days=days)

    # Build WHERE clause (T430 uses date_created, not issue_date)
    if tier == "hot":
        where = "date_created >= :cutoff"
        params = {"cutoff": cutoff}
    elif tier == "cold":
        cold_cutoff = now - timedelta(days=180)
        where = "date_created < :cutoff"
        params = {"cutoff": cold_cutoff}
    else:
        prev_days = {"warm": 30, "mild": 90}
        prev_cutoff = now - timedelta(days=prev_days[tier])
        where = "date_created >= :cutoff AND date_created < :prev"
        params = {"cutoff": cutoff, "prev": prev_cutoff}

    offset = (page - 1) * page_size
    try:
        # Get total count
        count_r = await db.execute(text(f"SELECT COUNT(*) FROM permits WHERE {where}"), params)
        total = count_r.scalar() or 0

        # Get page of results (T430 schema: state_code, zip_code, date_created,
        # project_type, applicant_name — no valuation/jurisdiction/contractor_name)
        r = await db.execute(text(
            f"SELECT permit_number, address, city, state_code, zip_code, project_type, status, "
            f"applicant_name, owner_name, date_created, county "
            f"FROM permits WHERE {where} "
            f"ORDER BY date_created DESC LIMIT :limit OFFSET :offset"
        ), {**params, "limit": page_size, "offset": offset})
        rows = r.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    leads = []
    for row in rows:
        leads.append({
            "permit_number": row[0],
            "address": row[1],
            "city": row[2],
            "state": row[3],
            "zip": row[4],
            "permit_type": row[5],
            "status": row[6],
            "contractor": row[7],
            "owner": row[8],
            "issue_date": row[9].isoformat() if row[9] else None,
            "county": row[10],
        })

    return {
        "tier": tier,
        "total": total,
        "page": page,
        "page_size": page_size,
        "leads": leads,
    }


@router.get("/coverage")
async def coverage_analysis(user: ApiUser = Depends(require_admin)):
    """Coverage analysis: which states have scrapers and data."""
    # Gather scrapers by state
    scraper_map: dict[str, list[dict]] = {}
    if SCRIPTS_DIR.exists():
        for f in sorted(SCRIPTS_DIR.glob("*.py")):
            if f.name.startswith("__"):
                continue
            info = _get_scraper_info(f.name)
            state = info["target_state"]
            if state:
                if state not in scraper_map:
                    scraper_map[state] = []
                scraper_map[state].append(info)

    # Database record counts by state
    db_counts = {}
    try:
        async with async_session_maker() as db:
            r = await db.execute(text(
                "SELECT state, COUNT(*) as cnt FROM permits "
                "WHERE state IS NOT NULL GROUP BY state ORDER BY cnt DESC"
            ))
            db_counts = {row.state: row.cnt for row in r.fetchall()}
    except Exception:
        pass

    # Build coverage for all 50 states + DC
    coverage = []
    for state in ALL_US_STATES:
        scrapers_for_state = scraper_map.get(state, [])
        last_run = None
        if scrapers_for_state:
            runs = [s["last_run_time"] for s in scrapers_for_state if s["last_run_time"]]
            if runs:
                last_run = max(runs)

        coverage.append({
            "state": state,
            "has_scraper": len(scrapers_for_state) > 0,
            "scraper_count": len(scrapers_for_state),
            "scraper_names": [s["filename"] for s in scrapers_for_state],
            "last_run": last_run,
            "total_records": db_counts.get(state, 0),
        })

    # Summary stats
    covered = sum(1 for c in coverage if c["has_scraper"])
    with_data = sum(1 for c in coverage if c["total_records"] > 0)
    missing = [c["state"] for c in coverage if not c["has_scraper"]]

    return {
        "states": coverage,
        "summary": {
            "total_states": len(ALL_US_STATES),
            "covered": covered,
            "with_data": with_data,
            "missing_states": missing,
        },
    }


@router.post("/trace")
async def trace_data(request: Request, body: dict = Body(...), user: ApiUser = Depends(require_admin)):
    """Admin-only: trace a data record back to the API key that downloaded it."""
    from app.services.fingerprint import trace_fingerprint
    result = trace_fingerprint(body)
    return {"trace_result": result}


@router.get("/abuse-alerts")
async def abuse_alerts(request: Request, user: ApiUser = Depends(require_admin)):
    """Admin-only: view recent abuse detection alerts."""
    from app.services.abuse_detector import get_recent_alerts
    alerts = await get_recent_alerts()
    return {"alerts": alerts, "total": len(alerts)}

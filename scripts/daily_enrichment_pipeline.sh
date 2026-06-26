#!/usr/bin/env bash
# =============================================================================
# daily_enrichment_pipeline.sh — Master orchestrator for PermitLookup enrichment
#
# Runs ALL enrichment steps in the correct order with logging, timing,
# and a summary email at the end.
#
# Replaces individual enrichment crons:
#   - lead_scoring_v1.py
#   - enrich_hot_leads_fast.sql
#   - enrich_fuzzy_match.sql
#   - enrich_norm_addr.sql
#   - enrich_with_sales.sql
#   - enrich_sos_linkage.sql
#   - enrich_permit_linkage.sql
#   - bridge_hot_leads_to_permits.py
#   - data_quality_report.py (if exists)
#
# Usage:
#   ./daily_enrichment_pipeline.sh
#   ./daily_enrichment_pipeline.sh --db-host 100.122.216.15
#
# Cron (replace individual enrichment crons):
#   39 6 * * * /home/will/permit-api-live/scripts/daily_enrichment_pipeline.sh >> /home/will/permit-api-live/logs/pipeline.log 2>&1
# =============================================================================

set -o pipefail

# -----------------------------------------------------------------------------
# SINGLE-INSTANCE GUARD (added 2026-06-26 after a stacked-run lock-storm).
# Each pipeline run can take a long time; without a lock, a new cron firing
# every day while the previous run is still going stacks N copies that all
# hammer the same tables and pile up locks. flock makes a second invocation
# exit immediately instead of stacking.
# -----------------------------------------------------------------------------
LOCK_FILE="/tmp/daily_enrichment_pipeline.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Another enrichment pipeline run is already active (lock: $LOCK_FILE). Exiting."
    exit 0
fi

# -----------------------------------------------------------------------------
# GLOBAL DB SAFETY CAPS (defense-in-depth alongside the per-script SET caps).
# Every psql launched by this pipeline inherits these via PGOPTIONS, so even a
# script that forgets its own caps can never hold a multi-hour lock. The SQL
# scripts raise statement_timeout locally only where they must (e.g. a
# CONCURRENTLY index build).
# -----------------------------------------------------------------------------
export PGOPTIONS="-c statement_timeout=10min -c lock_timeout=30s -c idle_in_transaction_session_timeout=2min"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
DATE_TAG=$(date +%Y%m%d)
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/enrichment_${DATE_TAG}.log"

# Default DB host
DB_HOST="100.122.216.15"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-host)
            DB_HOST="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Create logs directory if needed
mkdir -p "$LOG_DIR"

# Gmail credentials (same pattern as daily_scraper_report.py)
GMAIL_APP_PASSWORD=$(cat /home/will/.gmail_app_password 2>/dev/null || echo "")

# Tracking arrays
declare -a STEP_NAMES
declare -a STEP_DURATIONS
declare -a STEP_STATUSES
declare -a STEP_OUTPUTS

PIPELINE_START=$(date +%s)
TOTAL_STEPS=0
PASSED_STEPS=0
FAILED_STEPS=0

# =============================================================================
# Logging
# =============================================================================
log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "$LOG_FILE"
}

# =============================================================================
# Run a step: run_step "Step Name" command [args...]
# =============================================================================
run_step() {
    local step_name="$1"
    shift
    local cmd="$*"

    TOTAL_STEPS=$((TOTAL_STEPS + 1))
    local step_num=$TOTAL_STEPS

    log "=========================================="
    log "STEP $step_num: $step_name"
    log "CMD: $cmd"
    log "=========================================="

    local step_start=$(date +%s)
    local output
    output=$(eval "$cmd" 2>&1)
    local exit_code=$?
    local step_end=$(date +%s)
    local duration=$((step_end - step_start))

    STEP_NAMES+=("$step_name")
    STEP_DURATIONS+=("$duration")

    # Capture last 20 lines of output for the summary
    local tail_output
    tail_output=$(echo "$output" | tail -20)
    STEP_OUTPUTS+=("$tail_output")

    # Log full output
    echo "$output" >> "$LOG_FILE"

    if [ $exit_code -eq 0 ]; then
        STEP_STATUSES+=("OK")
        PASSED_STEPS=$((PASSED_STEPS + 1))
        log "STEP $step_num COMPLETED in ${duration}s (exit 0)"
    else
        STEP_STATUSES+=("FAILED (exit $exit_code)")
        FAILED_STEPS=$((FAILED_STEPS + 1))
        log "STEP $step_num FAILED in ${duration}s (exit $exit_code)"
        log "Continuing to next step..."
    fi

    echo "" >> "$LOG_FILE"
    return 0  # Always return 0 so pipeline continues
}

# =============================================================================
# Format duration as Xm Ys
# =============================================================================
fmt_duration() {
    local secs=$1
    if [ "$secs" -ge 60 ]; then
        echo "$((secs / 60))m $((secs % 60))s"
    else
        echo "${secs}s"
    fi
}

# =============================================================================
# Send summary email via Gmail SMTP (Python one-liner, same as daily_scraper_report)
# =============================================================================
send_summary_email() {
    local subject="$1"
    local body="$2"

    if [ -z "$GMAIL_APP_PASSWORD" ]; then
        log "No GMAIL_APP_PASSWORD found, skipping email"
        return 1
    fi

    python3 - "$subject" "$body" "$GMAIL_APP_PASSWORD" << 'PYEOF'
import smtplib, sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

subject = sys.argv[1]
body = sys.argv[2]
gmail_pw = sys.argv[3]

gmail_user = "willwalterburns@gmail.com"
to_email = "willwalterburns@gmail.com"

msg = MIMEMultipart("alternative")
msg["Subject"] = subject
msg["From"] = f"PermitLookup <{gmail_user}>"
msg["To"] = to_email
msg.attach(MIMEText(body, "html"))

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_pw)
        server.sendmail(gmail_user, to_email, msg.as_string())
    print(f"Email sent to {to_email}")
except Exception as e:
    print(f"Email failed: {e}")
    sys.exit(1)
PYEOF
}

# =============================================================================
# MAIN PIPELINE
# =============================================================================
log "============================================================"
log "ENRICHMENT PIPELINE START — $(date '+%Y-%m-%d %H:%M:%S')"
log "DB_HOST: $DB_HOST"
log "PGOPTIONS: $PGOPTIONS"
log "LOG: $LOG_FILE"
log "============================================================"

# Step 1: Lead scoring (A/B/C)
run_step "Lead Scoring (A/B/C)" \
    "cd $BASE_DIR && python3 scripts/lead_scoring_v1.py --db-host $DB_HOST"

# Step 2: Exact match contractor phones
run_step "Enrich Hot Leads (exact match phones)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_hot_leads_fast.sql"

# Step 3: Fuzzy match phones
run_step "Enrich Fuzzy Match (fuzzy phones)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_fuzzy_match.sql"

# Step 4: Property sales → owner names
run_step "Enrich Normalized Address (sales → owners)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_norm_addr.sql"

# Step 5: Alternate sales match
run_step "Enrich With Sales (alternate match)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_with_sales.sql"

# Step 6: SOS business entities
run_step "Enrich SOS Linkage (business entities)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_sos_linkage.sql"

# Step 7: Propagate phones across permits
run_step "Enrich Permit Linkage (propagate phones)" \
    "psql -h $DB_HOST -U will -d permits -f $SCRIPT_DIR/enrich_permit_linkage.sql"

# Step 8: Bridge hot_leads → permits table
run_step "Bridge Hot Leads → Permits" \
    "cd $BASE_DIR && python3 scripts/bridge_hot_leads_to_permits.py --db-host $DB_HOST"

# Step 9: Data quality report (optional)
if [ -f "$SCRIPT_DIR/data_quality_report.py" ]; then
    run_step "Data Quality Report" \
        "cd $BASE_DIR && python3 scripts/data_quality_report.py --db-host $DB_HOST"
else
    log "SKIP: data_quality_report.py not found (optional)"
fi

# =============================================================================
# SUMMARY
# =============================================================================
PIPELINE_END=$(date +%s)
PIPELINE_DURATION=$((PIPELINE_END - PIPELINE_START))

log ""
log "============================================================"
log "ENRICHMENT PIPELINE COMPLETE"
log "Total time: $(fmt_duration $PIPELINE_DURATION)"
log "Steps: $TOTAL_STEPS total, $PASSED_STEPS passed, $FAILED_STEPS failed"
log "============================================================"

# Build summary table
SUMMARY_TEXT=""
SUMMARY_TEXT+="<h2>Enrichment Pipeline Summary — $(date '+%b %d, %Y')</h2>"
SUMMARY_TEXT+="<p>Total time: <b>$(fmt_duration $PIPELINE_DURATION)</b> | "
SUMMARY_TEXT+="Steps: $TOTAL_STEPS total, "
SUMMARY_TEXT+="<span style='color:green'>$PASSED_STEPS passed</span>, "
if [ $FAILED_STEPS -gt 0 ]; then
    SUMMARY_TEXT+="<span style='color:red;font-weight:bold'>$FAILED_STEPS failed</span>"
else
    SUMMARY_TEXT+="<span style='color:green'>0 failed</span>"
fi
SUMMARY_TEXT+="</p>"

SUMMARY_TEXT+="<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-family:monospace;font-size:13px'>"
SUMMARY_TEXT+="<tr style='background:#f0f0f0'><th>#</th><th>Step</th><th>Status</th><th>Duration</th></tr>"

for i in "${!STEP_NAMES[@]}"; do
    local_status="${STEP_STATUSES[$i]}"
    local_color="green"
    if [[ "$local_status" == FAILED* ]]; then
        local_color="red"
    fi
    SUMMARY_TEXT+="<tr>"
    SUMMARY_TEXT+="<td>$((i+1))</td>"
    SUMMARY_TEXT+="<td>${STEP_NAMES[$i]}</td>"
    SUMMARY_TEXT+="<td style='color:$local_color'>${STEP_STATUSES[$i]}</td>"
    SUMMARY_TEXT+="<td>$(fmt_duration ${STEP_DURATIONS[$i]})</td>"
    SUMMARY_TEXT+="</tr>"
done

SUMMARY_TEXT+="</table>"
SUMMARY_TEXT+="<p style='font-size:11px;color:#888'>Log: $LOG_FILE</p>"

# Print summary to log
log ""
for i in "${!STEP_NAMES[@]}"; do
    printf "  %-3s %-45s %-20s %s\n" "$((i+1))." "${STEP_NAMES[$i]}" "${STEP_STATUSES[$i]}" "$(fmt_duration ${STEP_DURATIONS[$i]})" | tee -a "$LOG_FILE"
done

# Email subject
EMAIL_SUBJECT="Enrichment Pipeline"
if [ $FAILED_STEPS -gt 0 ]; then
    EMAIL_SUBJECT+=" [$FAILED_STEPS FAILED]"
else
    EMAIL_SUBJECT+=" [ALL OK]"
fi
EMAIL_SUBJECT+=" — $(date '+%b %d')"

# Send email
send_summary_email "$EMAIL_SUBJECT" "$SUMMARY_TEXT"

log "Done."

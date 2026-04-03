#!/bin/bash
# Check hot_leads freshness — alert if stale
LATEST=$(psql -h 100.122.216.15 -U will -d permits -t -A -c "SELECT MAX(issue_date) FROM hot_leads")
TOTAL=$(psql -h 100.122.216.15 -U will -d permits -t -A -c "SELECT COUNT(*) FROM hot_leads")
TODAY=$(date +%Y-%m-%d)

echo "$(date) Hot leads: $TOTAL records, latest: $LATEST"

# Alert if latest date is more than 3 days old
if [ -n "$LATEST" ]; then
    DAYS_OLD=$(( ($(date -d "$TODAY" +%s) - $(date -d "$LATEST" +%s)) / 86400 ))
    if [ "$DAYS_OLD" -gt 3 ]; then
        echo "WARNING: Hot leads data is $DAYS_OLD days stale! Latest: $LATEST"
    else
        echo "OK: Data is $DAYS_OLD days old"
    fi
fi

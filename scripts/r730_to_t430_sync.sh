#!/bin/bash
# R730 → T430 Data Sync
# Ensures all scraped data on R730 gets loaded into T430 PostgreSQL.
# Runs after scrapers finish, or on a schedule.
#
# Add to R730 crontab:
#   0 6 * * * /home/will/r730_to_t430_sync.sh >> /tmp/r730_sync.log 2>&1
#
# What it does:
# 1. Checks all scraper log files for completion
# 2. Runs ANALYZE on all tables to update statistics
# 3. Reports total record counts across all tables
# 4. Cleans up old log files (>7 days)

DB_HOST="100.122.216.15"
LOGS_DIR="/tmp"

echo "=============================================="
echo "R730 → T430 SYNC — $(date)"
echo "=============================================="

# 1. Check scraper status
echo ""
echo "=== SCRAPER STATUS ==="
for logfile in $LOGS_DIR/weekend_scraper.log $LOGS_DIR/enrichment_scraper.log $LOGS_DIR/unique_intel.log $LOGS_DIR/federal_intel.log $LOGS_DIR/scrape_acris.log $LOGS_DIR/scrape_violations_la.log $LOGS_DIR/train_predictive_v2.log; do
    if [ -f "$logfile" ]; then
        name=$(basename $logfile .log)
        last_line=$(tail -1 "$logfile" 2>/dev/null)
        size=$(wc -l < "$logfile" 2>/dev/null)
        echo "  $name: $size lines | Last: $last_line"
    fi
done

# 2. Check for any still-running scraper processes
echo ""
echo "=== RUNNING PROCESSES ==="
running=$(ps aux | grep -E '(weekend_|enrichment_|unique_intel|federal_intel|scrape_|train_predictive)' | grep python | grep -v grep | wc -l)
echo "  Active scraper processes: $running"
if [ "$running" -gt 0 ]; then
    ps aux | grep -E '(weekend_|enrichment_|unique_intel|federal_intel|scrape_|train_predictive)' | grep python | grep -v grep | awk '{print "  " $12, $13}'
fi

# 3. Run ANALYZE on all tables (updates pg_class.reltuples for fast counts)
echo ""
echo "=== RUNNING ANALYZE ==="
psql -h $DB_HOST -U will -d permits -c "
ANALYZE permits;
ANALYZE business_entities;
ANALYZE code_violations;
ANALYZE property_sales;
ANALYZE property_liens;
ANALYZE septic_systems;
ANALYZE property_valuations;
ANALYZE contractor_licenses;
ANALYZE epa_facilities;
ANALYZE fema_flood_zones;
ANALYZE census_demographics;
ANALYZE permit_predictions;
ANALYZE acris_parties;
ANALYZE property_assessments;
ANALYZE utility_connections;
ANALYZE sba_ppp_loans;
ANALYZE irs_exempt_orgs;
ANALYZE professional_licenses;
ANALYZE hmda_mortgages;
ANALYZE noaa_storm_events;
ANALYZE bls_construction_costs;
ANALYZE bls_construction_employment;
ANALYZE federal_projects;
ANALYZE federal_spending;
ANALYZE solar_installations;
ANALYZE census_building_permits;
" 2>&1 | grep -v "^$"
echo "  ANALYZE complete"

# 4. Report total record counts
echo ""
echo "=== RECORD COUNTS ==="
psql -h $DB_HOST -U will -d permits -c "
SELECT relname AS table_name, reltuples::bigint AS approx_rows
FROM pg_class
WHERE reltuples > 0 AND relkind = 'r'
ORDER BY reltuples DESC
LIMIT 30;
"

# 5. Calculate total
echo ""
echo "=== TOTAL ==="
psql -h $DB_HOST -U will -d permits -c "
SELECT sum(reltuples)::bigint AS total_records
FROM pg_class
WHERE reltuples > 0 AND relkind = 'r'
AND relname NOT LIKE 'pg_%' AND relname NOT LIKE 'sql_%';
"

# 6. Clean up old logs (>7 days)
echo ""
echo "=== CLEANUP ==="
find $LOGS_DIR -name "*.log" -mtime +7 -exec rm -v {} \; 2>/dev/null
echo "  Old logs cleaned"

echo ""
echo "=============================================="
echo "SYNC COMPLETE — $(date)"
echo "=============================================="

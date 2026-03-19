#!/bin/bash
# Weekly Data Refresh — runs every Sunday at 2 AM on R730
# Refreshes all data layers that have update frequencies faster than monthly
#
# Add to R730 crontab:
#   0 2 * * 0 /home/will/weekly_refresh.sh >> /tmp/weekly_refresh.log 2>&1
#
# Check status: tail -50 /tmp/weekly_refresh.log

DB_HOST="100.122.216.15"
LOG="/tmp/weekly_refresh.log"
SCRIPTS="/home/will"

echo "=============================================="
echo "WEEKLY REFRESH — $(date)"
echo "=============================================="

# 1. Code Violations — refresh all cities (weekly)
echo "[$(date)] Refreshing code violations..."
python3 -u $SCRIPTS/scrape_code_violations.py --city all --db-host $DB_HOST 2>&1 | tail -5

# 2. Business Entities — refresh Socrata states (monthly but check weekly)
echo "[$(date)] Refreshing business entities (CO, NY, OR, CT, TX, IA)..."
python3 -u $SCRIPTS/scrape_sos_socrata.py --state all --db-host $DB_HOST 2>&1 | tail -5

# 3. Canadian Permits — refresh all cities (weekly)
echo "[$(date)] Refreshing Canadian permits..."
python3 -u $SCRIPTS/scrape_canadian_permits.py --city all --db-host $DB_HOST 2>&1 | tail -5

# 4. Property Sales — refresh (monthly)
echo "[$(date)] Refreshing property sales..."
python3 -u $SCRIPTS/scrape_property_sales.py --source all --db-host $DB_HOST 2>&1 | tail -5

# 5. Property Liens — refresh (monthly)
echo "[$(date)] Refreshing property liens..."
python3 -u $SCRIPTS/scrape_property_liens.py --source all --db-host $DB_HOST 2>&1 | tail -5

# 6. NOAA Storm Events — refresh (monthly)
echo "[$(date)] Refreshing NOAA storm events..."
python3 -u $SCRIPTS/scrape_unique_intelligence.py --db-host $DB_HOST 2>&1 | tail -5

# 7. Retrain predictive model (weekly)
echo "[$(date)] Retraining predictive model..."
python3 -u $SCRIPTS/train_predictive_model.py --db-host $DB_HOST 2>&1 | tail -5

# 8. Run ANALYZE on all tables
echo "[$(date)] Running ANALYZE on all tables..."
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
" 2>&1 | tail -3

echo "=============================================="
echo "WEEKLY REFRESH COMPLETE — $(date)"
echo "=============================================="

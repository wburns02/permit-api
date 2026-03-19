#!/bin/bash
# Continuous AI Learning Loop
# Retrains the predictive model every 4 hours as new data arrives.
# Each cycle, the model trains on a larger dataset (scrapers running in parallel).
#
# Usage: nohup /home/will/continuous_learning.sh > /tmp/continuous_learning.log 2>&1 &
#
# The model gets progressively better because:
# 1. More permit records = better historical patterns
# 2. More valuation data = better market correlation
# 3. More census data = better demographic features
# 4. Each retraining produces fresh predictions for all ZIPs

DB_HOST="100.122.216.15"
INTERVAL=14400  # 4 hours in seconds
SCRIPTS="/home/will"
MAX_CYCLES=20   # ~3.3 days of continuous learning

echo "=============================================="
echo "CONTINUOUS LEARNING LOOP STARTED — $(date)"
echo "Interval: ${INTERVAL}s (4 hours)"
echo "Max cycles: ${MAX_CYCLES}"
echo "=============================================="

cycle=1
while [ $cycle -le $MAX_CYCLES ]; do
    echo ""
    echo "====== CYCLE $cycle/$MAX_CYCLES — $(date) ======"

    # Check how much data we have now
    echo "[$(date)] Checking current data volume..."
    psql -h $DB_HOST -U will -d permits -t -c "
        SELECT 'permits: ' || reltuples::bigint FROM pg_class WHERE relname = 'permits'
        UNION ALL SELECT 'valuations: ' || reltuples::bigint FROM pg_class WHERE relname = 'property_valuations'
        UNION ALL SELECT 'census: ' || reltuples::bigint FROM pg_class WHERE relname = 'census_demographics'
        UNION ALL SELECT 'predictions: ' || reltuples::bigint FROM pg_class WHERE relname = 'permit_predictions'
        UNION ALL SELECT 'entities: ' || reltuples::bigint FROM pg_class WHERE relname = 'business_entities'
        UNION ALL SELECT 'violations: ' || reltuples::bigint FROM pg_class WHERE relname = 'code_violations'
        UNION ALL SELECT 'mortgages: ' || reltuples::bigint FROM pg_class WHERE relname = 'hmda_mortgages'
        UNION ALL SELECT 'storms: ' || reltuples::bigint FROM pg_class WHERE relname = 'noaa_storm_events';
    " 2>/dev/null

    # Run ANALYZE first so the model queries use fresh stats
    echo "[$(date)] Running ANALYZE on key tables..."
    psql -h $DB_HOST -U will -d permits -c "
        ANALYZE permits;
        ANALYZE property_valuations;
        ANALYZE census_demographics;
        ANALYZE permit_predictions;
    " 2>&1 | tail -1

    # Train the model
    echo "[$(date)] Starting model training cycle $cycle..."
    python3 -u $SCRIPTS/train_predictive_model.py --db-host $DB_HOST 2>&1

    train_exit=$?
    if [ $train_exit -eq 0 ]; then
        echo "[$(date)] Training cycle $cycle COMPLETE"

        # Check predictions
        pred_count=$(psql -h $DB_HOST -U will -d permits -t -c "SELECT count(*) FROM permit_predictions;" 2>/dev/null | tr -d ' ')
        echo "[$(date)] Predictions in database: $pred_count"

        # Log model performance if available
        if [ -f /home/will/permit_predict_model.joblib ]; then
            model_size=$(du -h /home/will/permit_predict_model.joblib | cut -f1)
            echo "[$(date)] Model file size: $model_size"
        fi
    else
        echo "[$(date)] Training cycle $cycle FAILED (exit code $train_exit)"
        echo "[$(date)] Will retry next cycle..."
    fi

    cycle=$((cycle + 1))

    if [ $cycle -le $MAX_CYCLES ]; then
        echo "[$(date)] Sleeping ${INTERVAL}s until next cycle..."
        sleep $INTERVAL
    fi
done

echo ""
echo "=============================================="
echo "CONTINUOUS LEARNING COMPLETE — $(date)"
echo "$((MAX_CYCLES)) cycles executed"
echo "=============================================="

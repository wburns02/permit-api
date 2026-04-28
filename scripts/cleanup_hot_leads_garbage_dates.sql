-- Clean up future-dated garbage in hot_leads + install rejection trigger.
--
-- Why: ~1042 rows have issue_date > CURRENT_DATE + 30 days (e.g., 5045-01-11
-- from mgo_sebastian, 2099 dates from round_rock_cityworks template permits).
-- They poison the daily report's per-state MAX(issue_date) aggregations.
--
-- Runs at 3 AM CDT to avoid lock contention from daytime scrapers.
-- Invocation:
--     psql -h 100.122.216.15 -p 5432 -U will -d permits \
--          -f /home/will/permit-api/scripts/cleanup_hot_leads_garbage_dates.sql
--
-- Idempotent: safe to re-run.

\set ON_ERROR_STOP on
\timing on

-- Generous lock timeout because table has heavy write load even at 3 AM.
SET lock_timeout = '5min';

-- ---------------------------------------------------------------------------
-- 1. Archive (JSONB sidecar so schema drift doesn't break us)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hot_leads_garbage_dates_archive (
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_data    JSONB NOT NULL
);

INSERT INTO hot_leads_garbage_dates_archive (raw_data)
SELECT row_to_json(hl)::jsonb
  FROM hot_leads hl
 WHERE issue_date > CURRENT_DATE + INTERVAL '30 days';

\echo '--- archive size after this run ---'
SELECT COUNT(*) AS total_archived,
       COUNT(*) FILTER (WHERE archived_at::date = CURRENT_DATE) AS archived_today
  FROM hot_leads_garbage_dates_archive;

-- ---------------------------------------------------------------------------
-- 2. DELETE in batches of 100 with brief sleeps so concurrent INSERTs can slip
-- ---------------------------------------------------------------------------
DO $cleanup$
DECLARE
    total_deleted INT := 0;
    batch_count   INT;
BEGIN
    LOOP
        DELETE FROM hot_leads
         WHERE ctid IN (
             SELECT ctid FROM hot_leads
              WHERE issue_date > CURRENT_DATE + INTERVAL '30 days'
              LIMIT 100
         );
        GET DIAGNOSTICS batch_count = ROW_COUNT;
        total_deleted := total_deleted + batch_count;
        EXIT WHEN batch_count = 0;
        PERFORM pg_sleep(0.1);
    END LOOP;
    RAISE NOTICE 'cleanup_hot_leads_garbage_dates: deleted % rows', total_deleted;
END;
$cleanup$;

-- ---------------------------------------------------------------------------
-- 3. Install rejection trigger so this can never happen again
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION reject_bogus_issue_date() RETURNS trigger
    LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.issue_date IS NOT NULL
       AND NEW.issue_date > CURRENT_DATE + INTERVAL '30 days' THEN
        RAISE EXCEPTION
            'hot_leads.issue_date % is more than 30 days in the future (source=%, jurisdiction=%, permit_number=%)',
            NEW.issue_date, NEW.source, NEW.jurisdiction, NEW.permit_number;
    END IF;
    RETURN NEW;
END;
$body$;

DROP TRIGGER IF EXISTS hot_leads_reject_bogus_issue_date ON hot_leads;
CREATE TRIGGER hot_leads_reject_bogus_issue_date
    BEFORE INSERT OR UPDATE OF issue_date ON hot_leads
    FOR EACH ROW EXECUTE FUNCTION reject_bogus_issue_date();

-- ---------------------------------------------------------------------------
-- 4. Verify
-- ---------------------------------------------------------------------------
\echo '--- bad rows remaining (should be 0) ---'
SELECT COUNT(*) FROM hot_leads WHERE issue_date > CURRENT_DATE + INTERVAL '30 days';

\echo '--- trigger present (should be 1) ---'
SELECT tgname FROM pg_trigger WHERE tgname = 'hot_leads_reject_bogus_issue_date';

-- ---------------------------------------------------------------------------
-- 5. CONCURRENTLY indexes on hail_leads MV — required to make list queries
--    finish under Cloudflare's 100s gateway. Without these, MAX(storm_date)
--    takes 109s and county-filtered list takes 55-83s on cold cache.
--    CONCURRENTLY can't be inside a transaction, so reset autocommit first.
-- ---------------------------------------------------------------------------
\echo '--- building indexes on hail_leads (CONCURRENTLY, ~5-10 min each) ---'
\set AUTOCOMMIT on

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_hail_leads_storm_date
    ON hail_leads (storm_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_hail_leads_county_storm_date
    ON hail_leads (county, storm_date);

\echo '--- indexes present ---'
SELECT indexname FROM pg_indexes
 WHERE tablename = 'hail_leads'
   AND indexname IN ('ix_hail_leads_storm_date', 'ix_hail_leads_county_storm_date');

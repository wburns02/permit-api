-- enrich_fuzzy_match.sql
-- Second-pass enrichment: fuzzy name matching for contractor phones
-- Uses pg_trgm similarity to find near-matches missed by exact matching
-- Run AFTER enrich_hot_leads.sql (exact match pass)
--
-- Usage: psql -h 100.122.216.15 -U will -d permits -f enrich_fuzzy_match.sql

-- ============================================================================
-- SAFETY CAPS (added 2026-06-26 after a 20h lock-storm incident)
-- ----------------------------------------------------------------------------
-- statement_timeout guillotines any single statement that runs away.
-- lock_timeout makes us back off fast instead of queueing behind a long writer
-- and holding a RowExclusiveLock on hot_leads for hours.
-- These are SESSION settings; they apply to this psql connection only.
-- ============================================================================
SET statement_timeout = '10min';
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = '2min';

-- Ensure pg_trgm is available
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ----------------------------------------------------------------------------
-- The GIN trigram index on contractor_licenses.business_name is what turns
-- this join from a 20h parallel seq scan into a sub-second bitmap index scan.
-- It MUST be built with CREATE INDEX CONCURRENTLY so it never locks writers,
-- and CONCURRENTLY cannot run inside a transaction block / DO block.
-- The index expression here MUST match the % operand expression below EXACTLY
-- (lower(business_name), NO trim) or the planner will ignore the index and
-- fall back to a seq scan. That expression mismatch was the original bug.
--
-- A from-scratch concurrent GIN build on millions of rows can take longer than
-- the 10min statement cap, so lift the cap just for the build. In steady state
-- the index already exists and IF NOT EXISTS makes this an instant no-op.
-- ----------------------------------------------------------------------------
SET statement_timeout = '60min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cl_business_name_trgm
    ON contractor_licenses USING gin (LOWER(business_name) gin_trgm_ops);
SET statement_timeout = '10min';

-- Set similarity threshold (pg_trgm default is 0.3, we want stricter)
SET pg_trgm.similarity_threshold = 0.7;

\echo '=== Fuzzy Match Enrichment ==='
\echo ''

-- Count before
\echo 'Before: rows with contractor_phone IS NULL and contractor_company IS NOT NULL'
SELECT count(*) AS missing_phones_before
FROM hot_leads
WHERE contractor_phone IS NULL
  AND contractor_company IS NOT NULL;

BEGIN;

-- Fuzzy match on contractor_company -> contractor_licenses.business_name
-- Process in batches of 100K to avoid runaway queries
--
-- TWO things make this use the GIN trigram index instead of a 20h seq scan:
--
-- 1. The % operand on contractor_licenses.business_name is
--      LOWER(cl.business_name)  -- NO TRIM
--    so it matches idx_cl_business_name_trgm exactly. Wrapping it in TRIM()
--    does not match the index expression and forces a parallel seq scan
--    (that expression mismatch was the original bug). TRIM on the hot_leads
--    side (the non-indexed operand) is fine -- only the indexed column side
--    must match the index definition.
--
-- 2. The match is expressed as a CROSS JOIN LATERAL so each candidate row
--    drives ONE per-row GIN bitmap index probe (Nested Loop -> Bitmap Index
--    Scan on idx_cl_business_name_trgm). Written as a plain equijoin on state,
--    the planner instead picks a Merge Join on state and demotes the % match
--    to a post-join filter over ~900K license rows -- which is also slow.
--    The LATERAL form forces the correct nested-loop-over-GIN plan.
WITH candidates AS (
    SELECT hl.id, hl.contractor_company, hl.state
    FROM hot_leads hl
    WHERE hl.contractor_phone IS NULL
      AND hl.contractor_company IS NOT NULL
      AND TRIM(hl.contractor_company) != ''
    LIMIT 100000
),
fuzzy_matched AS (
    SELECT
        c.id,
        m.phone,
        m.contractor_addr,
        m.contractor_city
    FROM candidates c
    CROSS JOIN LATERAL (
        SELECT
            cl.phone,
            cl.address AS contractor_addr,
            cl.city    AS contractor_city,
            similarity(LOWER(cl.business_name), LOWER(TRIM(c.contractor_company))) AS sim_score
        FROM contractor_licenses cl
        WHERE LOWER(cl.business_name) % LOWER(TRIM(c.contractor_company))
          AND cl.state = UPPER(c.state)
          AND cl.phone IS NOT NULL
          AND similarity(LOWER(cl.business_name), LOWER(TRIM(c.contractor_company))) > 0.7
        ORDER BY sim_score DESC, cl.issue_date DESC NULLS LAST
        LIMIT 1   -- best single match per candidate (replaces DISTINCT ON)
    ) m
)
UPDATE hot_leads hl
SET
    contractor_phone   = fm.phone,
    contractor_address = COALESCE(hl.contractor_address, fm.contractor_addr),
    contractor_city    = COALESCE(hl.contractor_city, fm.contractor_city)
FROM fuzzy_matched fm
WHERE hl.id = fm.id;

\echo ''
\echo 'Fuzzy match (contractor_company) updated:'
SELECT count(*) AS rows_updated FROM (
    SELECT 1
) x WHERE EXISTS (SELECT 1);
-- Use GET DIAGNOSTICS in a DO block for accurate count

COMMIT;

-- Count after
\echo ''
\echo 'After: rows with contractor_phone IS NULL and contractor_company IS NOT NULL'
SELECT count(*) AS missing_phones_after
FROM hot_leads
WHERE contractor_phone IS NULL
  AND contractor_company IS NOT NULL;

-- Summary report
\echo ''
\echo '=== Fuzzy Match Report ==='
WITH stats AS (
    SELECT
        count(*) FILTER (WHERE contractor_phone IS NOT NULL) AS with_phone,
        count(*) FILTER (WHERE contractor_phone IS NULL AND contractor_company IS NOT NULL) AS still_missing,
        count(*) AS total
    FROM hot_leads
)
SELECT
    with_phone,
    still_missing,
    total,
    ROUND(100.0 * with_phone / NULLIF(total, 0), 1) AS phone_coverage_pct
FROM stats;

\echo ''
\echo 'Top fuzzy matches (sample of recent updates for QA):'
SELECT
    hl.contractor_company,
    hl.contractor_phone,
    hl.state,
    hl.issue_date
FROM hot_leads hl
WHERE hl.contractor_phone IS NOT NULL
  AND hl.issue_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY hl.issue_date DESC
LIMIT 10;

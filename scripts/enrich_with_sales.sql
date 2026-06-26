-- Enrich hot_leads with property_sales data
-- Match on normalized address + state
-- Only use the MOST RECENT sale for each address

-- ============================================================================
-- SAFETY CAPS (added 2026-06-26 after a 20h lock-storm incident)
-- statement_timeout guillotines a runaway statement; lock_timeout makes us
-- back off instead of holding a RowExclusiveLock on hot_leads for hours.
-- The UPDATE on hot_leads is batched (10K rows / commit) so a single
-- statement is always short and never holds a long lock against the
-- steady scraper / lead-scoring writers.
-- ============================================================================
SET statement_timeout = '10min';
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = '2min';

\echo '=== Enrich With Sales (property_sales -> owners) ==='
\echo ''

-- ----------------------------------------------------------------------------
-- Step 1: Build the set of matches into a TEMP table.
-- This is read-only against hot_leads / property_sales (AccessShareLock only),
-- so it never blocks writers no matter how long the DISTINCT ON scan takes.
-- It is bounded by statement_timeout above.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS _sales_matches;

CREATE TEMP TABLE _sales_matches AS
WITH recent_sales AS (
    SELECT DISTINCT ON (LOWER(TRIM(ps.address)), UPPER(ps.state))
        LOWER(TRIM(ps.address)) AS norm_addr,
        UPPER(ps.state) AS norm_state,
        ps.grantee AS new_owner,
        ps.grantor AS prev_owner,
        ps.sale_date,
        ps.sale_price,
        ps.property_type,
        ps.building_class
    FROM property_sales ps
    WHERE ps.address IS NOT NULL
        AND ps.sale_date IS NOT NULL
    ORDER BY LOWER(TRIM(ps.address)), UPPER(ps.state), ps.sale_date DESC
)
SELECT DISTINCT ON (hl.id)
    hl.id,
    rs.new_owner,
    rs.prev_owner,
    rs.sale_date,
    rs.sale_price,
    rs.property_type,
    rs.building_class
FROM hot_leads hl
JOIN recent_sales rs
    ON LOWER(TRIM(hl.address)) = rs.norm_addr
    AND UPPER(hl.state) = rs.norm_state
WHERE hl.address IS NOT NULL
    AND hl.owner_buy_date IS NULL  -- Only update unenriched
ORDER BY hl.id;

-- Give the batch loop a fast ordered scan over the match set
ALTER TABLE _sales_matches ADD COLUMN rn bigint;
UPDATE _sales_matches SET rn = t.rn
FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM _sales_matches) t
WHERE _sales_matches.id = t.id;
CREATE INDEX ON _sales_matches (rn);

\echo 'Matches staged:'
SELECT count(*) AS staged_matches FROM _sales_matches;

-- ----------------------------------------------------------------------------
-- Step 2: Apply the matches to hot_leads in 10K-row batches, COMMIT per batch.
-- A temporary procedure is used so we can COMMIT inside the loop (PG11+).
-- Each UPDATE touches at most 10K rows, so it can never hold a long lock.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE _apply_sales_matches()
LANGUAGE plpgsql AS $proc$
DECLARE
    batch       bigint := 10000;
    lo          bigint := 0;
    max_rn      bigint;
    affected    bigint;
    total       bigint := 0;
BEGIN
    SELECT max(rn) INTO max_rn FROM _sales_matches;
    IF max_rn IS NULL THEN
        RAISE NOTICE 'No matches to apply.';
        RETURN;
    END IF;

    WHILE lo < max_rn LOOP
        UPDATE hot_leads hl
        SET
            owner_name     = COALESCE(hl.owner_name, m.new_owner),
            owner_buy_date = m.sale_date,
            owner_buy_price = m.sale_price,
            previous_owner = m.prev_owner,
            property_type  = COALESCE(hl.property_type, m.property_type),
            building_class = COALESCE(hl.building_class, m.building_class)
        FROM _sales_matches m
        WHERE hl.id = m.id
          AND m.rn > lo
          AND m.rn <= lo + batch;

        GET DIAGNOSTICS affected = ROW_COUNT;
        total := total + affected;
        COMMIT;  -- release locks between batches so writers can interleave

        lo := lo + batch;
    END LOOP;

    RAISE NOTICE 'Applied % sales-enrichment updates in % batches.',
        total, CEIL(max_rn::numeric / batch);
END;
$proc$;

CALL _apply_sales_matches();

DROP PROCEDURE _apply_sales_matches();

-- Report
SELECT
    COUNT(*) AS total,
    COUNT(owner_name) AS with_owner,
    COUNT(owner_buy_date) AS with_buy_date,
    COUNT(owner_buy_price) AS with_price,
    COUNT(*) FILTER (WHERE contractor_phone IS NOT NULL) AS with_contractor_phone
FROM hot_leads;

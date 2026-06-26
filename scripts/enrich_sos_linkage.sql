-- enrich_sos_linkage.sql
-- Link Secretary of State business entities to hot_leads
-- Extracts registered agent info, principal address, and officer data
-- to fill contractor details where missing
--
-- Usage: psql -h 100.122.216.15 -U will -d permits -f enrich_sos_linkage.sql
--
-- Note: business_entities has no phone/email columns directly.
-- Useful data: registered_agent_name, registered_agent_address,
-- principal_address, mailing_address, officers (JSONB with name/title/address)

-- ============================================================================
-- SAFETY CAPS (added 2026-06-26 after a 20h lock-storm incident)
-- statement_timeout guillotines a runaway statement; lock_timeout makes us
-- back off instead of holding a RowExclusiveLock on hot_leads for hours.
-- Session-scoped: applies to this psql connection only.
-- ============================================================================
SET statement_timeout = '10min';
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = '2min';

\echo '=== SOS Business Entity Linkage ==='
\echo ''

-- Snapshot before
\echo 'Before enrichment:'
SELECT
    count(*) FILTER (WHERE contractor_phone IS NOT NULL) AS has_contractor_phone,
    count(*) FILTER (WHERE contractor_name IS NOT NULL) AS has_contractor_name,
    count(*) FILTER (WHERE contractor_address IS NOT NULL) AS has_contractor_address,
    count(*) AS total
FROM hot_leads;

BEGIN;

-- ============================================================
-- Step 1: Exact name match — hot_leads.contractor_company to business_entities.entity_name
-- Pull registered agent name and address to fill contractor details
-- ============================================================
\echo ''
\echo 'Step 1: Exact name match to business_entities...'

WITH matched AS (
    SELECT DISTINCT ON (hl.id)
        hl.id,
        be.entity_name,
        be.registered_agent_name,
        be.registered_agent_address,
        be.principal_address,
        be.mailing_address,
        be.status AS entity_status,
        -- Extract first officer name as fallback contact
        be.officers->0->>'name' AS first_officer_name,
        be.officers->0->>'address' AS first_officer_address
    FROM hot_leads hl
    JOIN business_entities be
        ON LOWER(TRIM(be.entity_name)) = LOWER(TRIM(hl.contractor_company))
       AND LOWER(be.state) = LOWER(hl.state)
    WHERE hl.contractor_company IS NOT NULL
      AND TRIM(hl.contractor_company) != ''
      -- Only enrich rows missing key data
      AND (hl.contractor_name IS NULL OR hl.contractor_address IS NULL)
    ORDER BY hl.id, be.formation_date DESC NULLS LAST
),
step1_update AS (
    UPDATE hot_leads hl
    SET
        -- Use registered agent as contractor name if missing
        contractor_name = COALESCE(
            hl.contractor_name,
            m.registered_agent_name,
            m.first_officer_name
        ),
        -- Use principal address, falling back to registered agent address
        contractor_address = COALESCE(
            hl.contractor_address,
            m.principal_address,
            m.registered_agent_address,
            m.first_officer_address
        )
    FROM matched m
    WHERE hl.id = m.id
    RETURNING hl.id
)
SELECT count(*) AS step1_exact_matches FROM step1_update;

-- ============================================================
-- Step 2: Normalized name match
-- Try matching after stripping common suffixes (LLC, INC, CORP, etc.)
-- ============================================================
\echo ''
\echo 'Step 2: Normalized name match (strip LLC/INC/CORP suffixes)...'

WITH normalized_leads AS (
    SELECT
        id,
        contractor_company,
        state,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                UPPER(TRIM(contractor_company)),
                '\s*(LLC|L\.L\.C\.|INC\.?|INCORPORATED|CORP\.?|CORPORATION|CO\.?|COMPANY|LTD\.?|LIMITED|LP|L\.P\.|LLP|L\.L\.P\.)\s*$',
                '', 'i'
            ),
            '\s+', ' ', 'g'
        ) AS norm_name
    FROM hot_leads
    WHERE contractor_company IS NOT NULL
      AND TRIM(contractor_company) != ''
      AND contractor_name IS NULL
      AND contractor_address IS NULL
),
normalized_entities AS (
    SELECT
        entity_name,
        state,
        registered_agent_name,
        registered_agent_address,
        principal_address,
        officers->0->>'name' AS first_officer_name,
        officers->0->>'address' AS first_officer_address,
        formation_date,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                UPPER(TRIM(entity_name)),
                '\s*(LLC|L\.L\.C\.|INC\.?|INCORPORATED|CORP\.?|CORPORATION|CO\.?|COMPANY|LTD\.?|LIMITED|LP|L\.P\.|LLP|L\.L\.P\.)\s*$',
                '', 'i'
            ),
            '\s+', ' ', 'g'
        ) AS norm_name
    FROM business_entities
    WHERE entity_name IS NOT NULL
      AND (registered_agent_name IS NOT NULL OR principal_address IS NOT NULL)
),
matched AS (
    SELECT DISTINCT ON (nl.id)
        nl.id,
        ne.registered_agent_name,
        ne.registered_agent_address,
        ne.principal_address,
        ne.first_officer_name,
        ne.first_officer_address
    FROM normalized_leads nl
    JOIN normalized_entities ne
        ON ne.norm_name = nl.norm_name
       AND LOWER(ne.state) = LOWER(nl.state)
    ORDER BY nl.id, ne.formation_date DESC NULLS LAST
),
step2_update AS (
    UPDATE hot_leads hl
    SET
        contractor_name = COALESCE(
            hl.contractor_name,
            m.registered_agent_name,
            m.first_officer_name
        ),
        contractor_address = COALESCE(
            hl.contractor_address,
            m.principal_address,
            m.registered_agent_address,
            m.first_officer_address
        )
    FROM matched m
    WHERE hl.id = m.id
      AND (hl.contractor_name IS NULL OR hl.contractor_address IS NULL)
    RETURNING hl.id
)
SELECT count(*) AS step2_normalized_matches FROM step2_update;

-- ============================================================
-- Step 3: Cross-reference — use registered agent name to find phones
-- If we now have a contractor_name from SOS data, try matching it
-- back to contractor_licenses to get a phone number
-- ============================================================
\echo ''
\echo 'Step 3: Cross-reference SOS agent names to contractor_licenses for phones...'

WITH sos_enriched AS (
    -- Rows that got contractor_name from SOS but still have no phone
    SELECT hl.id, hl.contractor_name, hl.state
    FROM hot_leads hl
    WHERE hl.contractor_phone IS NULL
      AND hl.contractor_name IS NOT NULL
      AND TRIM(hl.contractor_name) != ''
),
phone_match AS (
    SELECT DISTINCT ON (se.id)
        se.id,
        cl.phone
    FROM sos_enriched se
    JOIN contractor_licenses cl
        ON (
            LOWER(TRIM(cl.business_name)) = LOWER(TRIM(se.contractor_name))
            OR LOWER(TRIM(cl.full_business_name)) = LOWER(TRIM(se.contractor_name))
        )
       AND cl.state = UPPER(se.state)
       AND cl.phone IS NOT NULL
    ORDER BY se.id, cl.issue_date DESC NULLS LAST
),
step3_update AS (
    UPDATE hot_leads hl
    SET contractor_phone = pm.phone
    FROM phone_match pm
    WHERE hl.id = pm.id
    RETURNING hl.id
)
SELECT count(*) AS step3_phone_matches FROM step3_update;

COMMIT;

-- ============================================================
-- Report
-- ============================================================
\echo ''
\echo '=== SOS Linkage Report ==='
SELECT
    count(*) FILTER (WHERE contractor_phone IS NOT NULL) AS has_contractor_phone,
    count(*) FILTER (WHERE contractor_name IS NOT NULL) AS has_contractor_name,
    count(*) FILTER (WHERE contractor_address IS NOT NULL) AS has_contractor_address,
    count(*) AS total,
    ROUND(100.0 * count(*) FILTER (WHERE contractor_phone IS NOT NULL) / count(*), 1) AS phone_pct,
    ROUND(100.0 * count(*) FILTER (WHERE contractor_name IS NOT NULL) / count(*), 1) AS name_pct,
    ROUND(100.0 * count(*) FILTER (WHERE contractor_address IS NOT NULL) / count(*), 1) AS address_pct
FROM hot_leads;

\echo ''
\echo 'Coverage delta (compare with before snapshot above):'
\echo 'Done.'

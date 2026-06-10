-- Backfill canonical.permits from hot_leads (12.3M rows, 631 sources).
-- Idempotent: re-runs delete prior backfill rows first (permit_keys persist;
-- permit_id is stable across re-runs).
-- Run: psql "host=100.122.216.15 dbname=permits user=will" -f backfill_canonical_permits.sql
\timing on
SET statement_timeout = 0;
SET work_mem = '2GB';
SET maintenance_work_mem = '4GB';

-- 1. Jurisdictions from distinct (jurisdiction, state) pairs.
INSERT INTO canonical.jurisdictions (name, state, kind)
SELECT DISTINCT h.jurisdiction, upper(h.state), 'city'
FROM hot_leads h
WHERE h.jurisdiction IS NOT NULL
  AND h.state IS NOT NULL AND length(trim(h.state)) = 2
ON CONFLICT (state, name, kind) DO NOTHING;

-- 2. Identity map. source_record_id = permit number, falling back to the
--    hot_leads uuid for sources that don't carry one.
INSERT INTO canonical.permit_keys (source_id, source_record_id)
SELECT DISTINCT h.source, COALESCE(NULLIF(trim(h.permit_number), ''), h.id::text)
FROM hot_leads h
WHERE h.source IS NOT NULL
ON CONFLICT (source_id, source_record_id) DO NOTHING;

-- 3. Main backfill. Latest scrape wins per (source, record).
DELETE FROM canonical.permits WHERE lineage->>'origin' = 'hot_leads_backfill';

INSERT INTO canonical.permits
    (permit_id, source_id, source_record_id, jurisdiction_id, permit_type,
     description_raw, status, status_raw, applied_date, issued_date,
     declared_value, address_raw, address_norm, geom, geocode_confidence,
     confidence_score, freshness_at, lineage)
SELECT DISTINCT ON (h.source, COALESCE(NULLIF(trim(h.permit_number), ''), h.id::text))
    pk.permit_id,
    h.source,
    COALESCE(NULLIF(trim(h.permit_number), ''), h.id::text),
    j.id,
    NULLIF(trim(concat_ws(' / ', h.permit_type, h.work_class)), ''),
    h.description,
    CASE
        WHEN h.status ~* 'final|complete|closed'      THEN 'finaled'
        WHEN h.status ~* 'issued|active|approved'     THEN 'issued'
        WHEN h.status ~* 'applied|pending|review|submit' THEN 'applied'
        WHEN h.status ~* 'expire'                     THEN 'expired'
        WHEN h.status ~* 'cancel|withdraw|void|denied' THEN 'cancelled'
        ELSE NULL
    END,
    h.status,
    h.applied_date,
    h.issue_date,
    h.valuation,
    NULLIF(trim(concat_ws(', ', h.address, h.city, upper(h.state), h.zip)), ''),
    h.norm_addr,
    CASE WHEN h.lat BETWEEN -90 AND 90 AND h.lng BETWEEN -180 AND 180
              AND (h.lat <> 0 OR h.lng <> 0)
         THEN ST_SetSRID(ST_MakePoint(h.lng, h.lat), 4326) END,
    CASE WHEN h.lat IS NOT NULL AND (h.lat <> 0 OR h.lng <> 0) THEN 0.9 END,
    -- crude completeness score for v1; the enrichment pass replaces it
    (  (h.permit_number IS NOT NULL)::int
     + (h.issue_date IS NOT NULL)::int
     + (h.lat IS NOT NULL AND h.lat <> 0)::int
     + (h.description IS NOT NULL)::int
     + (h.valuation IS NOT NULL)::int )::real / 5.0,
    COALESCE(h.scraped_at::timestamptz, now()),
    jsonb_build_object('origin', 'hot_leads_backfill', 'hot_leads_id', h.id)
FROM hot_leads h
JOIN canonical.permit_keys pk
  ON pk.source_id = h.source
 AND pk.source_record_id = COALESCE(NULLIF(trim(h.permit_number), ''), h.id::text)
LEFT JOIN canonical.jurisdictions j
  ON j.name = h.jurisdiction AND j.state = upper(h.state) AND j.kind = 'city'
WHERE h.source IS NOT NULL
ORDER BY h.source, COALESCE(NULLIF(trim(h.permit_number), ''), h.id::text),
         h.scraped_at DESC NULLS LAST;

SELECT count(*) AS canonical_permits, count(geom) AS with_geom,
       count(jurisdiction_id) AS with_jurisdiction
FROM canonical.permits;

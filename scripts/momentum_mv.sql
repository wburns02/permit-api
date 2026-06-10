-- Momentum MV: county x month x category aggregates for TX permit enrichment.
-- Rig & Permit Radar Phase 4, task 6. Written 2026-06-10; NOT yet scheduled.
-- Ship it (CREATE + first REFRESH + cron) once canonical.permit_enrichment has
-- enough coverage to be meaningful (suggested: >= 250K rows or >= 90% of the
-- trailing 24 months of TX permits).
--
-- Run:  psql "host=100.122.216.15 port=5432 dbname=permits user=will" -f momentum_mv.sql
-- Refresh (cron, later): REFRESH MATERIALIZED VIEW CONCURRENTLY canonical.permit_momentum_county_month;

-- ---------------------------------------------------------------------------
-- 1. TX county geometries (one-time helper; tiger_counties only has WKT text).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS canonical.tx_county_geoms AS
SELECT geoid,
       name,
       namelsad,
       ST_SetSRID(ST_GeomFromText(geom_wkt), 4326) AS geom
FROM public.tiger_counties
WHERE statefp = '48' AND geom_wkt IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_tx_county_geoms_geom
    ON canonical.tx_county_geoms USING gist (geom);
DO $$
BEGIN
    ALTER TABLE canonical.tx_county_geoms
        ADD CONSTRAINT tx_county_geoms_pk PRIMARY KEY (geoid);
EXCEPTION WHEN duplicate_table OR duplicate_object OR invalid_table_definition THEN
    NULL;  -- already constrained on re-run
END $$;

-- ---------------------------------------------------------------------------
-- 2. Momentum MV: county x month x category.
--    County resolution: spatial join on permit geom (94%+ of major TX sources
--    are geocoded); permits without geom fall into county_geoid NULL /
--    county_name 'UNKNOWN' so totals still reconcile.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS canonical.permit_momentum_county_month AS
SELECT
    COALESCE(c.geoid, 'UNKNOWN')                    AS county_geoid,
    COALESCE(c.name,  'UNKNOWN')                    AS county_name,
    date_trunc('month', p.issued_date)::date        AS month,
    e.category,
    count(*)                                        AS n_permits,
    count(*) FILTER (WHERE e.category_confidence >= 0.8) AS n_high_conf,
    sum(p.declared_value)                           AS total_declared_value,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY p.declared_value)
        FILTER (WHERE p.declared_value > 0)         AS median_declared_value,
    count(DISTINCT p.source_id)                     AS n_sources
FROM canonical.permits p
JOIN canonical.permit_enrichment e
  ON e.source_id = p.source_id AND e.source_record_id = p.source_record_id
LEFT JOIN canonical.tx_county_geoms c
  ON p.geom IS NOT NULL AND ST_Contains(c.geom, p.geom)
WHERE p.issued_date IS NOT NULL
GROUP BY 1, 2, 3, 4
WITH NO DATA;

-- Unique index required for REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS ix_momentum_mv_pk
    ON canonical.permit_momentum_county_month (county_geoid, month, category);
CREATE INDEX IF NOT EXISTS ix_momentum_mv_month
    ON canonical.permit_momentum_county_month (month);
CREATE INDEX IF NOT EXISTS ix_momentum_mv_category
    ON canonical.permit_momentum_county_month (category, month);

-- First population (run manually when coverage is sufficient):
-- REFRESH MATERIALIZED VIEW canonical.permit_momentum_county_month;

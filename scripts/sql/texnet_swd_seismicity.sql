-- texnet.swd_seismicity: commercial SWD disposal wells x TexNet earthquakes
-- within 25 km. One row per (disposal well, event) pair.
--
-- Sources: canonical.disposal_wells (lineage->>'commercial'='true', geom
-- required) and texnet.events (load_texnet.py). Rebuild with
-- REFRESH MATERIALIZED VIEW texnet.swd_seismicity; after either side reloads.
--
-- The && ST_Expand prefilter (0.3 deg ~ 33 km at TX latitudes) lets the
-- events gist index prune before the exact geography ST_DWithin test.

DROP MATERIALIZED VIEW IF EXISTS texnet.swd_seismicity;

CREATE MATERIALIZED VIEW texnet.swd_seismicity AS
SELECT
    dw.id                AS disposal_well_id,
    dw.uic_number,
    dw.api10             AS well_api10,
    dw.county            AS well_county,
    dw.operator_id,
    o.name               AS well_operator,
    dw.status            AS well_status,
    e.event_id,
    e.magnitude,
    e.origin_time,
    e.depth_km,
    e.county             AS event_county,
    ST_Distance(dw.geom::geography, e.geom::geography)::numeric(10,1)
                         AS distance_m,
    CASE
        WHEN ST_Distance(dw.geom::geography, e.geom::geography) <= 5000
            THEN '5km'
        WHEN ST_Distance(dw.geom::geography, e.geom::geography) <= 10000
            THEN '10km'
        ELSE '25km'
    END                  AS radius_bucket
FROM canonical.disposal_wells dw
LEFT JOIN canonical.operators o ON o.id = dw.operator_id
JOIN texnet.events e
    ON e.geom && ST_Expand(dw.geom, 0.3)
   AND ST_DWithin(dw.geom::geography, e.geom::geography, 25000)
WHERE dw.lineage->>'commercial' = 'true'
  AND dw.geom IS NOT NULL
  AND e.geom IS NOT NULL;

CREATE INDEX ix_swd_seis_uic ON texnet.swd_seismicity (uic_number);
CREATE INDEX ix_swd_seis_event ON texnet.swd_seismicity (event_id);
ANALYZE texnet.swd_seismicity;

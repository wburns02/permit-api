-- broadband.ecbtx.com /v1/broadband/lookup runs a GROUP BY query on
-- fcc_bdc_locations_tx JOIN fcc_bdc_providers WHERE bdc.block_geoid range.
-- 86M rows + JOIN was hitting 20s statement_timeout. A covering index on
-- (block_geoid, provider_id, technology) lets the join be served from the
-- index without heap lookups for the common path.

CREATE INDEX CONCURRENTLY IF NOT EXISTS fcc_bdc_locations_tx_block_provider_tech_idx
  ON public.fcc_bdc_locations_tx (block_geoid, provider_id, technology)
  INCLUDE (max_advertised_download_speed, max_advertised_upload_speed,
           low_latency, business_residential_code);

ANALYZE public.fcc_bdc_locations_tx;

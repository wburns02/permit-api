# TexNet Earthquake Catalog

## Source
- Public catalog UI: https://catalog.texnet.beg.utexas.edu (Angular SPA,
  Bureau of Economic Geology, UT Austin)
- Actual data backend (discovered in the SPA bundle): ArcGIS REST
  `https://maps.texnet.beg.utexas.edu/arcgis/rest/services/catalog/catalog_all/MapServer`
  - layer 0 = `Earthquake` (reviewed catalog, 2017-present)
  - layer 1 = `Station` (seismometer network, not loaded)
  - layer 2 = `Earthquakes (Preliminary)` (recent unreviewed events)
- Dead ends, for the record: `catalog.texnet.beg.utexas.edu/fdsnws/...`
  serves the SPA index (no FDSN service); the
  `catalog/TexNetCatalog/FeatureServer/2` URL in the bundle 500s
  ("Server object extension 'featureserver' not found"). Use the MapServer
  query endpoint.

## Format discovered
ArcGIS JSON, `maxRecordCount=2000`, pagination supported
(`resultOffset` + `orderByFields=EarthquakeId`). 39 attribute fields; we
keep the seismologically useful subset. `Event_Date` is epoch milliseconds
UTC; `Depth` is km below sea level (`DepthSurface` = below ground surface).

## Download + load (single step, API-paged)
```bash
python3 /home/will/permit-api-live/scripts/load_texnet.py
```
Full reload (~25 pages, <1 min) into `texnet.events`:
`event_id` PK, `origin_time timestamptz`, `magnitude`, `mag_type`,
`depth_km`, `lat`, `lon`, `county`, `region`, `event_type`,
`evaluation_status`, `catalog_layer` ('reviewed'|'preliminary'),
`geom Point 4326`. Indexes: gist(geom), origin_time, magnitude.
Reviewed layer loads first; preliminary inserts with ON CONFLICT DO NOTHING
so promoted events keep their reviewed row.

## Row counts (2026-06-10 load)
- reviewed: 47,666
- preliminary: 807
- `texnet.events` total: 48,473 (span 2016-12-20 .. present; all geom set)

## Downstream join: texnet.swd_seismicity
`/home/will/permit-api-live/scripts/sql/texnet_swd_seismicity.sql`
(also in repo `scripts/sql/`). Materialized view: commercial SWDs
(`canonical.disposal_wells` where `lineage->>'commercial'='true'`, geom
required; 3,284 wells) x `texnet.events` within 25 km on geography, with
`distance_m` and `radius_bucket` ('5km'/'10km'/'25km').

2026-06-10 build: 1,918,598 rows (2,086 wells, 47,469 events);
buckets 5km=76,654 / 10km=218,596 / 25km=1,623,348. Permian-dominated
(Reeves, Martin, Culberson, Midland, Loving top counties), as expected.
Refresh: `REFRESH MATERIALIZED VIEW texnet.swd_seismicity;` after either
side reloads (~1 min).

## Join keys
- Spatial only (geom x geom). TexNet has no API numbers.
- `event_id` (e.g. `texnet2022jxkz`, newer `tx2025...` prefix) is the stable
  public ID, same one shown in the catalog UI and USGS ComCat for TX events.

## Refresh cadence recommendation
Daily (it's one quick API sweep) or weekly to match FracFocus; TexNet posts
preliminary events within minutes-to-hours and reviews within days. Chain
the MV refresh after the load.

## Gotchas
- Do not trust the FeatureServer URL in the SPA bundle; MapServer/0 + /2 are
  the working endpoints.
- 2000-row page cap: loader paginates on EarthquakeId; a plain query
  silently returns exactly 2000 (round-number cap smell).
- `Magnitude` is ML(TexNet) mostly; `MomentMagnitude` exists for larger
  events but is sparse. We load `Magnitude` + `mag_type`.
- Preliminary events can later be revised/deleted; full reload handles this.
- `CountyName` casing is inconsistent ("KARNES" vs "Culberson") and
  sometimes "Unknown"; normalize at query time if needed.
- Catalog includes small out-of-state border events (`region` covers NM
  edge); filter on county/region if TX-only is required.

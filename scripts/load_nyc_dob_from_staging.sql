-- Load NYC DOB jobs + NYC PLUTO from R730 staging CSVs.
-- Companion to load_nyc_dob_from_staging.py for the case where you'd rather
-- run psql \copy by hand. Execute from R730 (where the CSVs live) like:
--
--   psql postgresql://will@100.122.216.15:5432/permits \
--     -v dob_path=/mnt/data/staging/nyc_dob_jobs_fixed.csv \
--     -v pluto_path=/mnt/data/staging/nyc_pluto_fixed.csv \
--     -f load_nyc_dob_from_staging.sql
--
-- Headers:
--   DOB:   address,city,state,zip,county,land_use
--   PLUTO: address,city,state,zip,county,owner_name,land_use,year_built,lot_size_sqft,lat,lng

\timing on
BEGIN;

-- ============================================================
-- DOB JOBS  →  permits (partition permits_ny)
-- ============================================================
DELETE FROM permits WHERE state_code = 'NY' AND source = 'nyc_dob_jobs';

CREATE TEMP TABLE nyc_dob_raw (
    address  TEXT,
    city     TEXT,
    state    TEXT,
    zip      TEXT,
    county   TEXT,
    land_use TEXT
) ON COMMIT DROP;

\copy nyc_dob_raw FROM :'dob_path' WITH (FORMAT csv, HEADER true)

INSERT INTO permits (
    address, city, state_code, zip_code, county, category,
    source, source_file, loaded_at
)
SELECT
    NULLIF(address, ''),
    NULLIF(city, ''),
    'NY',
    NULLIF(zip, ''),
    NULLIF(county, ''),
    NULLIF(land_use, ''),
    'nyc_dob_jobs',
    'nyc_dob_jobs_fixed.csv',
    NOW()
FROM nyc_dob_raw;

-- ============================================================
-- PLUTO  →  nyc_pluto (new table)
-- ============================================================
CREATE TABLE IF NOT EXISTS nyc_pluto (
    id           BIGSERIAL PRIMARY KEY,
    address      TEXT,
    city         TEXT,
    state        CHAR(2),
    zip_code     TEXT,
    county       TEXT,
    owner_name   TEXT,
    land_use     TEXT,
    year_built   INTEGER,
    lot_size_sqft NUMERIC,
    lat          DOUBLE PRECISION,
    lng          DOUBLE PRECISION,
    loaded_at    TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_nyc_pluto_zip ON nyc_pluto (zip_code);
CREATE INDEX IF NOT EXISTS ix_nyc_pluto_geo ON nyc_pluto USING gist (point(lng, lat));

TRUNCATE nyc_pluto RESTART IDENTITY;

CREATE TEMP TABLE pluto_raw (
    address       TEXT,
    city          TEXT,
    state         TEXT,
    zip           TEXT,
    county        TEXT,
    owner_name    TEXT,
    land_use      TEXT,
    year_built    TEXT,
    lot_size_sqft TEXT,
    lat           TEXT,
    lng           TEXT
) ON COMMIT DROP;

\copy pluto_raw FROM :'pluto_path' WITH (FORMAT csv, HEADER true)

INSERT INTO nyc_pluto (
    address, city, state, zip_code, county, owner_name, land_use,
    year_built, lot_size_sqft, lat, lng
)
SELECT
    NULLIF(address, ''),
    NULLIF(city, ''),
    'NY',
    NULLIF(zip, ''),
    NULLIF(county, ''),
    NULLIF(owner_name, ''),
    NULLIF(land_use, ''),
    NULLIF(year_built, '')::INTEGER,
    NULLIF(lot_size_sqft, '')::NUMERIC,
    NULLIF(lat, '')::DOUBLE PRECISION,
    NULLIF(lng, '')::DOUBLE PRECISION
FROM pluto_raw;

COMMIT;

-- verification
SELECT 'permits_ny total' AS metric, COUNT(*) FROM permits_ny
UNION ALL
SELECT 'permits_ny nyc_dob_jobs', COUNT(*) FROM permits_ny WHERE source = 'nyc_dob_jobs'
UNION ALL
SELECT 'nyc_pluto total', COUNT(*) FROM nyc_pluto;

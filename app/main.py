"""PermitLookup API — Building permit data for contractors, investors, and insurers."""

import asyncio
import logging
import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import init_db

# Import models to register with SQLAlchemy
from app.models.permit import Permit, Jurisdiction  # noqa: F401
from app.models.api_key import ApiUser, ApiKey, UsageLog  # noqa: F401
from app.models.alert import PermitAlert  # noqa: F401
from app.models.alert_history import AlertExecutionHistory  # noqa: F401
from app.models.saved_search import SavedSearch  # noqa: F401
from app.models.data_layers import (  # noqa: F401
    ContractorLicense, EpaFacility, FemaFloodZone,
    CensusDemographics, SepticSystem, PropertyValuation,
    BusinessEntity, CodeViolation, PermitPrediction,
    PropertySale, PropertyLien,
)
from app.models.dialer import CallLog, LeadStatus  # noqa: F401
from app.models.crm import Contact, Deal, Note, Commission, Activity, Webhook, BatchJob  # noqa: F401
from app.models.quote import Quote  # noqa: F401
from app.models.team import Team, TeamMember  # noqa: F401
from app.models.email_campaign import EmailCampaign, EmailRecipient, EmailUnsubscribe  # noqa: F401
from app.models.pricing import PricingBenchmark  # noqa: F401

# Import routers
from app.api.v1.permits import router as permits_router
from app.api.v1.auth import router as auth_router
from app.api.v1.billing import router as billing_router
from app.api.v1.coverage import router as coverage_router
from app.api.v1.contractors import router as contractors_router
from app.api.v1.alerts import router as alerts_router
from app.api.v1.properties import router as properties_router
from app.api.v1.market import router as market_router
from app.api.v1.saved_searches import router as saved_searches_router
from app.api.v1.admin import router as admin_router
from app.api.v1.licenses import router as licenses_router
from app.api.v1.environmental import router as environmental_router
from app.api.v1.septic import router as septic_router
from app.api.v1.demographics import router as demographics_router
from app.api.v1.valuations import router as valuations_router
from app.api.v1.entities import router as entities_router
from app.api.v1.pipeline import router as pipeline_router
from app.api.v1.violations import router as violations_router
from app.api.v1.predictions import router as predictions_router
from app.api.v1.sales import router as sales_router
from app.api.v1.liens import router as liens_router
from app.api.v1.dialer import router as dialer_router
from app.api.v1.crm import router as crm_router
from app.api.v1.quotes import router as quotes_router
from app.api.v1.analyst import router as analyst_router
from app.api.v1.trends import router as trends_router
from app.api.v1.batch import router as batch_router
from app.api.v1.campaigns import router as campaigns_router
from app.api.v1.dialer_ws import router as dialer_ws_router
from app.api.v1.freshness import router as freshness_router
from app.api.v1.data_freshness import router as data_freshness_router
from app.api.v1.hman_auth import router as hman_auth_router
from app.api.v1.pricing import router as pricing_router
from app.api.v1.hail_leads import router as hail_leads_router
from app.api.v1.permit_leads import router as permit_leads_router
from app.api.v1.parcel_screen import router as parcel_screen_router
from app.api.v1.broadband import router as broadband_router
from app.api.v1.internal_rural_v5 import router as internal_rural_v5_router
from app.api.v1.roofer_leads import router as roofer_leads_router
from app.api.v1.enrichment import router as enrichment_router
from app.api.v1.rural_score import router as rural_score_router
from app.api.v1.wells import wells_router, well_permits_router
from app.api.v1.map_tiles import router as map_tiles_router
from app.models.parcel_screen import (  # noqa: F401 — registers tables for Base.metadata.create_all
    ParcelJurisdiction,
    ParcelStateLaw,
    ParcelZoneDensity,
    ParcelScreen,
    ParcelHotPick,
    ParcelOwnerEnrichment,
)

logger = logging.getLogger(__name__)


async def _run_startup_migrations() -> None:
    """Background-task wrapper for the auto-migration block.

    Runs detached from lifespan so a single hung migration (e.g. the
    `hail_leads_spc` MV build, which has a 30-minute statement_timeout
    because it walks 17M rows on a cold DB) can't block uvicorn from
    binding the port. Each migration is independent and has its own
    try/except — failures are logged and skipped.
    """
    import sys
    print("[migrations] starting background auto-migration run", flush=True, file=sys.stderr)

    from sqlalchemy import text as _text
    from app.database import primary_engine

    # ---------------------------------------------------------------------------
    # MULTI-WORKER DDL SERIALIZATION (advisory lock).
    #
    # uvicorn runs 4 workers; without a guard all 4 race these startup
    # migrations concurrently. The DROP/CREATE MATERIALIZED VIEW pairs below are
    # NOT mutually safe under concurrency — racing CREATEs collide on
    # pg_type_typname_nsp_index ("duplicate key value violates unique
    # constraint") and the losers error out, logging noise and risking an
    # inconsistent object state.
    #
    # Fix: gate the whole routine behind a Postgres advisory lock. The first
    # worker to grab it runs the migrations; every other worker sees the lock
    # held and skips entirely (the DDL is idempotent, so one run suffices).
    #
    # pg_try_advisory_lock is non-blocking — losing workers return immediately
    # instead of piling up waiting (which would stall their startup). The lock
    # is held on a dedicated connection kept open for the whole routine and
    # released in `finally`; a session-level lock on the pooled per-migration
    # connections would not span them. If the worker crashes mid-run the
    # session ends and Postgres drops the lock automatically.
    #
    # MIGRATION_ADVISORY_LOCK_KEY: stable arbitrary bigint = ascii "prmit"
    # (0x70726d6974). Fixed constant so every worker contends on the same key.
    MIGRATION_ADVISORY_LOCK_KEY = 0x70726D6974  # ascii "prmit"

    lock_conn = None
    try:
        lock_conn = await primary_engine.connect()
        got_lock = await lock_conn.scalar(
            _text("SELECT pg_try_advisory_lock(:k)").bindparams(
                k=MIGRATION_ADVISORY_LOCK_KEY
            )
        )
        if not got_lock:
            print(
                "[migrations] another worker holds the lock, skipping",
                flush=True,
                file=sys.stderr,
            )
            logger.info("migrations: another worker holds the lock, skipping")
            await lock_conn.close()
            return
    except Exception as e:
        # If we can't even acquire the lock connection, fall back to running
        # unguarded rather than skipping migrations on every worker (which
        # would leave the schema unmigrated). The DDL's own try/excepts and
        # IF [NOT] EXISTS clauses tolerate a rare race.
        logger.warning("migrations: advisory-lock acquisition failed (%s) — running unguarded", e)
        if lock_conn is not None:
            try:
                await lock_conn.close()
            except Exception:
                pass
            lock_conn = None

    try:
        await _run_startup_migrations_body(_text, primary_engine)
    finally:
        if lock_conn is not None:
            try:
                await lock_conn.scalar(
                    _text("SELECT pg_advisory_unlock(:k)").bindparams(
                        k=MIGRATION_ADVISORY_LOCK_KEY
                    )
                )
            except Exception:
                pass
            try:
                await lock_conn.close()
            except Exception:
                pass


async def _run_startup_migrations_body(_text, primary_engine) -> None:
    """The actual auto-migration DDL, serialized by the advisory lock in
    `_run_startup_migrations`. Each migration is independent with its own
    try/except — failures are logged and skipped.
    """
    import sys

    # Auto-migrate: add webhook_url column if it doesn't exist
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE api_users ADD COLUMN IF NOT EXISTS webhook_url VARCHAR(500)"
            ))
    except Exception as e:
        logger.warning("Could not apply webhook_url migration: %s", e)

    # Auto-migrate: add password_hash column for H-Man CRM JWT auth
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE api_users ADD COLUMN IF NOT EXISTS password_hash VARCHAR(64)"
            ))
    except Exception as e:
        logger.warning("Could not apply password_hash migration: %s", e)

    # Auto-migrate: alert source_type (building permits vs W-1 drilling permits)
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE permit_alerts ADD COLUMN IF NOT EXISTS "
                "source_type TEXT NOT NULL DEFAULT 'permits'"
            ))
    except Exception as e:
        logger.warning("Could not apply alert source_type migration: %s", e)

    # Auto-migrate: add softphone columns to call_logs
    try:
        async with primary_engine.begin() as conn:
            for col, typ in [
                ("twilio_call_sid", "VARCHAR(64)"),
                ("recording_url", "TEXT"),
                ("recording_duration", "INTEGER"),
                ("transcript", "TEXT"),
            ]:
                await conn.execute(_text(
                    f"ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS {col} {typ}"
                ))
    except Exception as e:
        logger.warning("Could not apply softphone migration: %s", e)

    # Auto-migrate: add geometry_wgs84 column to parcel_hot_picks
    # (Ladder 3 map view needs the polygon, not just the centroid).
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE parcel_hot_picks ADD COLUMN IF NOT EXISTS geometry_wgs84 JSONB"
            ))
    except Exception as e:
        logger.warning("Could not apply parcel_hot_picks.geometry_wgs84 migration: %s", e)

    # Auto-migrate: cron_heartbeat table for the hail-leads observability page.
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text("""
                CREATE TABLE IF NOT EXISTS cron_heartbeat (
                    name TEXT PRIMARY KEY,
                    beat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    duration_seconds REAL,
                    row_count BIGINT,
                    last_error TEXT
                )
            """))
    except Exception as e:
        logger.warning("Could not create cron_heartbeat table: %s", e)

    # Auto-migrate: storm_events table for the NOAA storm-events loader.
    # DDL is byte-identical to CREATE_TABLE_SQL in
    # permit-api-live/scripts/backfill_noaa_storm_events.py so the new
    # APScheduler-driven loader writes to the same shape T430 was using.
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text("""
                CREATE TABLE IF NOT EXISTS storm_events (
                    event_id BIGINT PRIMARY KEY,
                    episode_id BIGINT,
                    state TEXT,
                    state_fips INT,
                    year INT,
                    event_type TEXT,
                    cz_type TEXT,
                    cz_fips INT,
                    cz_name TEXT,
                    wfo TEXT,
                    begin_datetime TIMESTAMP,
                    end_datetime TIMESTAMP,
                    cz_timezone TEXT,
                    injuries_direct INT,
                    injuries_indirect INT,
                    deaths_direct INT,
                    deaths_indirect INT,
                    damage_property TEXT,
                    damage_crops TEXT,
                    source TEXT,
                    magnitude DOUBLE PRECISION,
                    magnitude_type TEXT,
                    flood_cause TEXT,
                    tor_f_scale TEXT,
                    begin_location TEXT,
                    end_location TEXT,
                    begin_lat DOUBLE PRECISION,
                    begin_lon DOUBLE PRECISION,
                    end_lat DOUBLE PRECISION,
                    end_lon DOUBLE PRECISION,
                    episode_narrative TEXT,
                    event_narrative TEXT,
                    scraped_at DATE NOT NULL
                )
            """))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_storm_begin ON storm_events (begin_datetime)"
            ))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_storm_type ON storm_events (event_type)"
            ))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_storm_cz ON storm_events (state, cz_name)"
            ))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_storm_geo ON storm_events (begin_lat, begin_lon) "
                "WHERE begin_lat IS NOT NULL AND begin_lon IS NOT NULL"
            ))
    except Exception as e:
        logger.warning("Could not create storm_events table: %s", e)

    # Auto-migrate: hail_leads_spc materialized view + hail_leads_unified view.
    # `hail_leads_spc` mirrors `hail_leads`'s output shape but joins
    # spc_storm_reports × hot_leads, so SPC-sourced storms (loaded daily) reach
    # the product even when storm_events (NOAA, weekly) is stale.
    # `hail_leads_unified` is a cheap UNION ALL fronted by the API so a single
    # query surfaces leads from BOTH sources with a `storm_source` discriminator.
    # CREATE MATERIALIZED VIEW IF NOT EXISTS is idempotent — safe across redeploys.
    # On a fresh DB this MV creation walks 17M+ hot_leads rows and inserts ~2.4M
    # — the `IF NOT EXISTS` short-circuits cleanly when the MV already exists,
    # but if it has to actually build, give it a generous timeout. Lock timeout
    # stays short so we don't hang if some other session holds locks.
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30min'"))
            await conn.execute(_text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS hail_leads_spc AS
                WITH storms AS (
                    SELECT report_id, report_type, size_in AS magnitude,
                           report_date AS storm_date,
                           UPPER(county) AS county_upper,
                           state, lat AS begin_lat, lon AS begin_lon,
                           comments AS damage_property
                      FROM spc_storm_reports
                     WHERE report_type IN ('hail','wind','torn')
                       AND state = 'TX'
                       AND report_date >= '2020-01-01'
                ), leads AS (
                    SELECT id, permit_number, address, city, state, zip, county,
                           upper(county) AS county_upper, lat, lng, issue_date,
                           permit_type, work_class, description, valuation,
                           contractor_company, contractor_phone, owner_name, source, jurisdiction,
                           lower(coalesce(description,''))~'(roof|shingle|siding|gutter|fence|awning|hail)'
                        OR lower(coalesce(work_class,''))~'(roof|re-?roof|reroof|siding)'
                        OR lower(coalesce(permit_type,''))~'(roof|siding)' AS is_roofish
                      FROM hot_leads
                     WHERE state = 'TX' AND issue_date IS NOT NULL AND issue_date >= '2020-01-01'
                )
                SELECT l.id AS lead_id, l.permit_number, l.address, l.city, l.state, l.zip, l.county,
                       l.lat, l.lng, l.issue_date, l.permit_type, l.work_class, l.description,
                       l.valuation, l.contractor_company, l.contractor_phone, l.owner_name,
                       l.source, l.jurisdiction, l.is_roofish,
                       NULL::bigint AS storm_event_id,
                       s.report_id AS storm_report_id,
                       CASE s.report_type
                           WHEN 'hail' THEN 'Hail'
                           WHEN 'wind' THEN 'Thunderstorm Wind'
                           WHEN 'torn' THEN 'Tornado'
                           ELSE INITCAP(s.report_type)
                       END AS storm_type,
                       s.magnitude AS storm_magnitude, s.storm_date,
                       (l.issue_date - s.storm_date)::integer AS days_after_storm,
                       s.damage_property AS storm_damage_report,
                       GREATEST(0, 365 - (CURRENT_DATE - s.storm_date))::double precision / 365.0 *
                           coalesce(s.magnitude,1.0) *
                           CASE WHEN l.is_roofish THEN 2.5 ELSE 1.0 END AS hail_lead_score,
                       CASE
                           WHEN lower(coalesce(l.description,''))~'(solar|photovoltaic|\\bpv\\b|pv system|solar panel)' THEN 'solar'
                           WHEN lower(coalesce(l.description,''))~'(\\bre-?roof\\b|reroof|new roof|replace.*roof(?!.*solar)|tear off|tear-off|roof.*replac|replace.*shingle|shingle.*replace|re-?shingl|strip.*roof|roof.*strip|cover.*replace|deck.*replace.*roof|full roof replace)' THEN 'roof_replace'
                           WHEN lower(coalesce(l.description,''))~'(siding.*replace|replace.*siding|new siding|reside\\s)' THEN 'siding'
                           WHEN lower(coalesce(l.description,''))~'(gutter|downspout|leader)' THEN 'gutter'
                           WHEN lower(coalesce(l.description,''))~'(fence.*replace|replace.*fence|new fence|storm damage.*fence)' THEN 'fence'
                           WHEN lower(coalesce(l.description,''))~'(window.*replace|replace.*window|storm window|hail.*window)' THEN 'window'
                           WHEN l.is_roofish THEN 'other_roof'
                           ELSE 'non_roof'
                       END AS lead_category
                  FROM leads l
                  JOIN storms s ON s.county_upper = l.county_upper
                              AND l.issue_date >= s.storm_date
                              AND l.issue_date <= s.storm_date + INTERVAL '120 days'
            """))
            await conn.execute(_text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_hail_leads_spc_lead_report "
                "ON hail_leads_spc (lead_id, storm_report_id)"
            ))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_hail_leads_spc_storm_date "
                "ON hail_leads_spc (storm_date)"
            ))
            await conn.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_hail_leads_spc_county "
                "ON hail_leads_spc (county)"
            ))
    except Exception as e:
        logger.warning("Could not create hail_leads_spc MV: %s", e)

    # Unified view: UNION ALL of hail_leads_categorized (NOAA) + hail_leads_spc (SPC).
    # Adds a `storm_source` discriminator. Created with CREATE OR REPLACE so
    # column changes propagate cleanly across redeploys.
    # CREATE OR REPLACE VIEW needs an AccessExclusiveLock; if the production
    # API has a long-running list query holding an AccessShareLock on the view,
    # the migration would hang lifespan indefinitely (Railway never exits the
    # "Waiting for application startup" state). A short statement_timeout makes
    # it fail fast — the existing view stays in place, since we use IF NOT
    # EXISTS / OR REPLACE patterns and the prior session created it via psql.
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30s'"))
            await conn.execute(_text("""
                CREATE OR REPLACE VIEW hail_leads_unified AS
                SELECT
                    lead_id, permit_number, address, city, state, zip, county, lat, lng, issue_date,
                    permit_type, work_class, description, valuation, contractor_company, contractor_phone,
                    owner_name, source, jurisdiction, is_roofish, storm_event_id, storm_type, storm_magnitude,
                    storm_date, days_after_storm, storm_damage_report, hail_lead_score, lead_category,
                    'storm_events'::text AS storm_source,
                    NULL::text           AS storm_report_id
                  FROM hail_leads_categorized
                UNION ALL
                SELECT
                    lead_id, permit_number, address, city, state, zip, county, lat, lng, issue_date,
                    permit_type, work_class, description, valuation, contractor_company, contractor_phone,
                    owner_name, source, jurisdiction, is_roofish, storm_event_id, storm_type, storm_magnitude,
                    storm_date, days_after_storm, storm_damage_report, hail_lead_score, lead_category,
                    'spc_storm_reports'::text AS storm_source,
                    storm_report_id
                  FROM hail_leads_spc
            """))
    except Exception as e:
        logger.warning("Could not create hail_leads_unified view: %s", e)

    # Deduplicated list MV: one row per lead_id (best storm by score), the
    # source the list/export endpoints read. `hail_leads_unified` fans every
    # lead out across storm-event matches (~862k leads → ~20M rows), so the
    # old DISTINCT-ON-over-the-whole-view list query took 50-60s and hit the
    # statement_timeout, returning EMPTY. This MV does that collapse ONCE at
    # refresh time; the endpoint then reads it with plain WHERE/ORDER/LIMIT.
    # Created WITH NO DATA so a fresh DB doesn't block startup walking 20M
    # rows — the refresh job (mv_refresh._MVS) populates it. On the existing
    # prod DB it's already built + populated out-of-band.
    try:
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30s'"))
            await conn.execute(_text(r"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS hail_leads_list AS
                SELECT DISTINCT ON (lead_id)
                    lead_id, permit_number, address, city, state, zip, county, lat, lng,
                    issue_date, permit_type, work_class, description, valuation,
                    contractor_company, contractor_phone, owner_name, source, jurisdiction,
                    storm_event_id, storm_type, storm_magnitude, storm_date,
                    days_after_storm, lead_category, hail_lead_score, storm_source
                  FROM hail_leads_unified
                 WHERE storm_type = 'Hail'
                   AND address IS NOT NULL
                   AND address !~ '^[0-9]+$'
                 ORDER BY lead_id, hail_lead_score DESC NULLS LAST
                WITH NO DATA
            """))
        # Indexes in their own txn (the unique index also enables a later
        # REFRESH MATERIALIZED VIEW CONCURRENTLY).
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            for ddl in (
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_hail_leads_list_lead "
                "ON hail_leads_list (lead_id)",
                "CREATE INDEX IF NOT EXISTS ix_hll_county_score "
                "ON hail_leads_list (county, hail_lead_score DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS ix_hll_score "
                "ON hail_leads_list (hail_lead_score DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS ix_hll_storm_date "
                "ON hail_leads_list (storm_date DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS ix_hll_category "
                "ON hail_leads_list (lead_category)",
                "CREATE INDEX IF NOT EXISTS ix_hll_magnitude "
                "ON hail_leads_list (storm_magnitude)",
            ):
                try:
                    await conn.execute(_text(ddl))
                except Exception as ie:  # noqa: BLE001
                    logger.warning("hail_leads_list index skipped: %s", ie)
    except Exception as e:
        logger.warning("Could not create hail_leads_list MV: %s", e)

    # ---------------------------------------------------------------------------
    # unserviced_hail_leads MV — Tarrant + Dallas + Hays + Comal + Bexar (canvass).
    #
    # Produces one row per parcel that: (a) sits within 3 km of a recent TX
    # hail-storm report (SPC, last 18 months), (b) has NOT already had
    # contractor activity recorded in hail_leads_list for the same
    # address+county. This is the "canvass list" roofers want.
    #
    # Per-county pattern (identical for both, UNION ALL'd into one MV):
    #   Storm source : spc_storm_reports hail points, last 18 months, county bbox.
    #   Spatial driver: <county>_parcel_geometries.geom GIST index via LATERAL
    #                   join with ST_DWithin in geometry (degrees) space —
    #                   0.027° ≈ 3 km at TX latitudes. NOT ::geography (that
    #                   would defeat the GIST index).
    #   Address source: tx_cad_parcels.situs_* via account_no = parcel_id and
    #                   cad_source = <county code>.
    #   Serviced exclusion: hail_leads_list address match by county. Both sides
    #                   normalized (UPPER, strip unit designators, strip
    #                   punctuation, collapse whitespace) — mirrors
    #                   normalize_address() in app/services/search_service.py.
    #
    #   Tarrant: geom=tad_parcel_geometries, cad_source='TAD', county='Tarrant',
    #            county_source='Tarrant', bbox lat 32.5-33.0, lon -97.5..-97.0.
    #   Dallas : geom=dcad_parcel_geometries, cad_source='DCAD', county='Dallas',
    #            county_source='Dallas', bbox lat 32.60-33.04, lon -97.02..-96.44.
    #            Address join verified: 417,793 / 464,180 geoms join DCAD (~90%);
    #            bounded 6-month probe gave 99.4% situs-address coverage.
    #   Hays   : geom=hays_parcel_geometries, cad_source='HaysCAD', county='hays',
    #            county_source='Hays', bbox lat 29.8-30.25, lon -98.25..-97.6
    #            (San Marcos / Kyle / Buda). NOTE the join key differs: Hays
    #            geometries join tx_cad_parcels on geom.PARCEL_ID = parcel_id
    #            (NOT account_no — account_no joins 0%). Verified 68,854/119,359
    #            geoms carry a HaysCAD situs address (~57.7%); within the storm
    #            candidate set, 8,024 carry an address and only ~1 is serviced.
    #            CAVEAT: hail_leads_list has ~394 Hays rows but they barely
    #            overlap the storm-near candidates, so the serviced-exclusion is
    #            weak for Hays (same as Dallas) — treat the list as a fresh
    #            canvass list, not a de-duped one.
    #   Comal  : geom=comal_parcel_geometries, cad_source='CCAD', county='comal',
    #            county_source='Comal', bbox lat 29.6-29.95, lon -98.5..-98.0
    #            (New Braunfels / Canyon Lake / Spring Branch / Bulverde).
    #            Same join shape as Hays: geom.parcel_id = tx_cad_parcels.parcel_id
    #            (NOT account_no — account_no joins ~46%, parcel_id ~98%).
    #            cad_source = 'CCAD'. hail_leads_list has ZERO Comal rows, so the
    #            serviced-exclusion is a no-op today — this is a pure fresh canvass
    #            list. Completes the Central TX I-35 corridor (Hays=San Marcos +
    #            Comal=New Braunfels). NOTE: recent hail in the Comal bbox is
    #            SPARSE (a handful of small SPC reports, latest ~2025-05), so
    #            expect FEW, LOW-SCORING rows today. This add is for territory
    #            completeness / future-proofing (coverage is live before the next
    #            New Braunfels hit), not immediate hot-lead volume — low volume
    #            here is EXPECTED, not a failure.
    #
    #   Bexar  : geom=bexar_parcel_geometries, cad_source='BCAD', county='bexar',
    #            county_source='Bexar', bbox lat 29.1-29.7, lon -98.9..-98.2
    #            (San Antonio metro). Same join shape as Hays/Comal:
    #            geom.parcel_id = tx_cad_parcels.parcel_id (BCAD PropID stored
    #            identically as text in both tables, joins ~100%). cad_source =
    #            'BCAD'. Source is the FREE Bexar County GIS REST layer
    #            (~710,772 parcels). hail_leads_list has no Bexar rows today so the
    #            serviced-exclusion is a no-op — pure fresh canvass list. This adds
    #            the un-serviced (parcels-minus-permits) signal for the largest TX
    #            metro previously uncovered.
    #
    # Travis is still EXCLUDED: there is no TCAD/Travis cad_source in
    # tx_cad_parcels (only HCAD/DCAD/TAD/WCAD/HaysCAD/CCAD/BCAD/Gillespie/Kerr/
    # Bandera) and travis_parcel_geometries has no situs columns of its own — so
    # there is no address join for Travis parcels. Adding it would ship rows with
    # NULL addresses, which is worse than no Travis coverage. (Bexar previously
    # shared this failure mode; BCAD now supplies the address join, so it is in.)
    #
    # STORM WINDOW BOUND: spc_storm_reports limited to the last 18 months. Old
    # hail damage is stale (already serviced) AND the unbounded scan is what made
    # the nightly full refresh OOM-risky on T430. 18 months keeps the product
    # fresh and the refresh bounded.
    #
    # Created WITH NO DATA — startup never blocks. Refresh job populates it.
    # A UNIQUE index on (parcel_id, county_source) enables REFRESH CONCURRENTLY
    # (parcel_id is only unique within a county's CAD, so the synthetic key pairs
    # it with county_source).
    # ---------------------------------------------------------------------------
    try:
        # SELF-HEAL stale definition. The MV already exists in prod from an
        # EARLY buggy definition (Tarrant-only, permits_tx serviced-exclusion,
        # 2023 storm cutoff). `CREATE MATERIALIZED VIEW IF NOT EXISTS` silently
        # skips when the object exists, so every later fix to this SQL never
        # reached the DB object — the endpoint served ~284K WRONG rows.
        #
        # Detect staleness by inspecting the LIVE view definition for the
        # current sentinels (hail_leads_list serviced-exclusion + the Dallas
        # dcad_parcel_geometries CTE). If either is absent the live def predates
        # the corrected SQL below, so DROP it and let the CREATE rebuild it.
        # DROP only fires when stale, so steady-state redeploys don't churn.
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30s'"))
            try:
                live_def = await conn.scalar(_text(
                    "SELECT pg_get_viewdef('unserviced_hail_leads'::regclass)"
                ))
            except Exception:  # noqa: BLE001 — MV does not exist yet (first deploy)
                live_def = None
            if live_def is not None and not (
                "hail_leads_list" in live_def
                and "dcad_parcel_geometries" in live_def
                and "hays_parcel_geometries" in live_def
                and "comal_parcel_geometries" in live_def
                and "bexar_parcel_geometries" in live_def
            ):
                logger.warning(
                    "unserviced_hail_leads: stale live definition detected "
                    "(missing hail_leads_list/dcad/hays/comal/bexar sentinels) — "
                    "dropping to rebuild"
                )
                await conn.execute(_text(
                    "DROP MATERIALIZED VIEW IF EXISTS unserviced_hail_leads CASCADE"
                ))
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30s'"))
            await conn.execute(_text(r"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS unserviced_hail_leads AS
                -- ============================ TARRANT ============================
                WITH tarrant_storms AS (
                    SELECT report_id, report_date, lat, lon,
                           COALESCE(size_in, 0.75) AS size_in
                      FROM spc_storm_reports
                     WHERE report_type = 'hail'
                       AND state = 'TX'
                       AND report_date >= CURRENT_DATE - INTERVAL '18 months'
                       AND lat  BETWEEN 32.5  AND 33.0
                       AND lon  BETWEEN -97.5 AND -97.0
                ),
                -- Best (largest, most recent) storm per parcel — DISTINCT ON
                -- ordered by size_in DESC, report_date DESC.
                tarrant_candidate_parcels AS (
                    SELECT DISTINCT ON (tg.parcel_id)
                           tg.parcel_id,
                           tg.account_no,
                           tg.centroid_lat,
                           tg.centroid_lon,
                           sr.report_id     AS storm_report_id,
                           sr.report_date   AS matched_storm_date,
                           sr.size_in       AS hail_size_in
                      FROM tarrant_storms sr
                      CROSS JOIN LATERAL (
                          SELECT tg.parcel_id, tg.account_no,
                                 tg.centroid_lat, tg.centroid_lon
                            FROM tad_parcel_geometries tg
                           WHERE ST_DWithin(
                                     tg.geom,
                                     ST_SetSRID(ST_MakePoint(sr.lon, sr.lat), 4326),
                                     0.027
                                 )
                      ) tg
                     ORDER BY tg.parcel_id, sr.size_in DESC, sr.report_date DESC
                ),
                -- Candidate parcels with TAD address data attached.
                -- Normalization mirrors normalize_address() in search_service.py:
                --   UPPER + strip unit designators (SUITE/STE/UNIT/APT/# + token)
                --   + strip punctuation (.,#) + collapse whitespace.
                -- situs_address is already stored uppercase-abbreviated in TAD;
                -- hail_leads_list.address is mixed-case, so we UPPER both sides.
                -- Unit-designator strip uses POSIX ERE (^|\\s) word-boundary since
                -- POSIX ERE does not support \\b; replacement is ' ' (space) so the
                -- leading whitespace is cleaned by the outer REGEXP_REPLACE('\\s+',' ').
                tarrant_candidate_with_addr AS (
                    SELECT cp.*,
                           tcp.situs_address AS address,
                           tcp.situs_city    AS city,
                           tcp.situs_zip     AS zip,
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(tcp.situs_address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_situs
                      FROM tarrant_candidate_parcels cp
                      JOIN tx_cad_parcels tcp
                            ON tcp.parcel_id = cp.account_no
                           AND tcp.cad_source = 'TAD'
                     WHERE tcp.situs_address IS NOT NULL
                ),
                -- Normalized hail_leads_list addresses for Tarrant — pre-computed
                -- so the EXISTS subquery hits a small keyed set rather than 398K rows.
                hll_tarrant_norm AS (
                    SELECT DISTINCT
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_addr
                      FROM hail_leads_list
                     WHERE county ILIKE 'tarrant'
                ),
                tarrant_rows AS (
                    SELECT
                        ca.parcel_id,
                        ca.address,
                        ca.city,
                        ca.zip,
                        'tarrant'::text                             AS county,
                        ca.centroid_lat,
                        ca.centroid_lon,
                        ca.matched_storm_date,
                        ca.hail_size_in,
                        (CURRENT_DATE - ca.matched_storm_date)::integer AS days_since_storm,
                        GREATEST(0, 365 - (CURRENT_DATE - ca.matched_storm_date))::double precision
                            / 365.0 * ca.hail_size_in                AS lead_score,
                        'Tarrant'::text                             AS county_source
                      FROM tarrant_candidate_with_addr ca
                     WHERE NOT EXISTS (
                         SELECT 1 FROM hll_tarrant_norm htn
                          WHERE htn.norm_addr = ca.norm_situs
                     )
                ),
                -- ============================= DALLAS ============================
                dallas_storms AS (
                    SELECT report_id, report_date, lat, lon,
                           COALESCE(size_in, 0.75) AS size_in
                      FROM spc_storm_reports
                     WHERE report_type = 'hail'
                       AND state = 'TX'
                       AND report_date >= CURRENT_DATE - INTERVAL '18 months'
                       AND lat  BETWEEN 32.60  AND 33.04
                       AND lon  BETWEEN -97.02 AND -96.44
                ),
                dallas_candidate_parcels AS (
                    SELECT DISTINCT ON (tg.parcel_id)
                           tg.parcel_id,
                           tg.account_no,
                           tg.centroid_lat,
                           tg.centroid_lon,
                           sr.report_id     AS storm_report_id,
                           sr.report_date   AS matched_storm_date,
                           sr.size_in       AS hail_size_in
                      FROM dallas_storms sr
                      CROSS JOIN LATERAL (
                          SELECT tg.parcel_id, tg.account_no,
                                 tg.centroid_lat, tg.centroid_lon
                            FROM dcad_parcel_geometries tg
                           WHERE ST_DWithin(
                                     tg.geom,
                                     ST_SetSRID(ST_MakePoint(sr.lon, sr.lat), 4326),
                                     0.027
                                 )
                      ) tg
                     ORDER BY tg.parcel_id, sr.size_in DESC, sr.report_date DESC
                ),
                dallas_candidate_with_addr AS (
                    SELECT cp.*,
                           tcp.situs_address AS address,
                           tcp.situs_city    AS city,
                           tcp.situs_zip     AS zip,
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(tcp.situs_address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_situs
                      FROM dallas_candidate_parcels cp
                      JOIN tx_cad_parcels tcp
                            ON tcp.parcel_id = cp.account_no
                           AND tcp.cad_source = 'DCAD'
                     WHERE tcp.situs_address IS NOT NULL
                ),
                hll_dallas_norm AS (
                    SELECT DISTINCT
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_addr
                      FROM hail_leads_list
                     WHERE county ILIKE 'dallas'
                ),
                dallas_rows AS (
                    SELECT
                        ca.parcel_id,
                        ca.address,
                        ca.city,
                        ca.zip,
                        'dallas'::text                              AS county,
                        ca.centroid_lat,
                        ca.centroid_lon,
                        ca.matched_storm_date,
                        ca.hail_size_in,
                        (CURRENT_DATE - ca.matched_storm_date)::integer AS days_since_storm,
                        GREATEST(0, 365 - (CURRENT_DATE - ca.matched_storm_date))::double precision
                            / 365.0 * ca.hail_size_in                AS lead_score,
                        'Dallas'::text                              AS county_source
                      FROM dallas_candidate_with_addr ca
                     WHERE NOT EXISTS (
                         SELECT 1 FROM hll_dallas_norm hdn
                          WHERE hdn.norm_addr = ca.norm_situs
                     )
                ),
                -- ============================== HAYS =============================
                -- San Marcos / Kyle / Buda (the warm-roofer pilot area). Mirrors
                -- Dallas exactly EXCEPT the address join key: Hays geometries join
                -- tx_cad_parcels on geom.parcel_id = parcel_id (account_no joins
                -- 0%, parcel_id joins ~57.7%). cad_source = 'HaysCAD'.
                hays_storms AS (
                    SELECT report_id, report_date, lat, lon,
                           COALESCE(size_in, 0.75) AS size_in
                      FROM spc_storm_reports
                     WHERE report_type = 'hail'
                       AND state = 'TX'
                       AND report_date >= CURRENT_DATE - INTERVAL '18 months'
                       AND lat  BETWEEN 29.8   AND 30.25
                       AND lon  BETWEEN -98.25 AND -97.6
                ),
                hays_candidate_parcels AS (
                    SELECT DISTINCT ON (tg.parcel_id)
                           tg.parcel_id,
                           tg.centroid_lat,
                           tg.centroid_lon,
                           sr.report_id     AS storm_report_id,
                           sr.report_date   AS matched_storm_date,
                           sr.size_in       AS hail_size_in
                      FROM hays_storms sr
                      CROSS JOIN LATERAL (
                          SELECT tg.parcel_id,
                                 tg.centroid_lat, tg.centroid_lon
                            FROM hays_parcel_geometries tg
                           WHERE ST_DWithin(
                                     tg.geom,
                                     ST_SetSRID(ST_MakePoint(sr.lon, sr.lat), 4326),
                                     0.027
                                 )
                      ) tg
                     ORDER BY tg.parcel_id, sr.size_in DESC, sr.report_date DESC
                ),
                hays_candidate_with_addr AS (
                    SELECT cp.*,
                           tcp.situs_address AS address,
                           tcp.situs_city    AS city,
                           tcp.situs_zip     AS zip,
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(tcp.situs_address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_situs
                      FROM hays_candidate_parcels cp
                      JOIN tx_cad_parcels tcp
                            ON tcp.parcel_id = cp.parcel_id
                           AND tcp.cad_source = 'HaysCAD'
                     WHERE tcp.situs_address IS NOT NULL
                ),
                hll_hays_norm AS (
                    SELECT DISTINCT
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_addr
                      FROM hail_leads_list
                     WHERE county ILIKE 'hays'
                ),
                hays_rows AS (
                    SELECT
                        ca.parcel_id,
                        ca.address,
                        ca.city,
                        ca.zip,
                        'hays'::text                                AS county,
                        ca.centroid_lat,
                        ca.centroid_lon,
                        ca.matched_storm_date,
                        ca.hail_size_in,
                        (CURRENT_DATE - ca.matched_storm_date)::integer AS days_since_storm,
                        GREATEST(0, 365 - (CURRENT_DATE - ca.matched_storm_date))::double precision
                            / 365.0 * ca.hail_size_in                AS lead_score,
                        'Hays'::text                                AS county_source
                      FROM hays_candidate_with_addr ca
                     WHERE NOT EXISTS (
                         SELECT 1 FROM hll_hays_norm hhn
                          WHERE hhn.norm_addr = ca.norm_situs
                     )
                ),
                -- ============================== COMAL ============================
                -- New Braunfels / Canyon Lake / Spring Branch / Bulverde. Mirrors
                -- Hays exactly: geom.parcel_id = tx_cad_parcels.parcel_id (account_no
                -- joins ~46%, parcel_id ~98%). cad_source = 'CCAD'. hail_leads_list
                -- has zero Comal rows so the serviced-exclusion is a no-op today.
                comal_storms AS (
                    SELECT report_id, report_date, lat, lon,
                           COALESCE(size_in, 0.75) AS size_in
                      FROM spc_storm_reports
                     WHERE report_type = 'hail'
                       AND state = 'TX'
                       AND report_date >= CURRENT_DATE - INTERVAL '18 months'
                       AND lat  BETWEEN 29.6   AND 29.95
                       AND lon  BETWEEN -98.5  AND -98.0
                ),
                comal_candidate_parcels AS (
                    SELECT DISTINCT ON (tg.parcel_id)
                           tg.parcel_id,
                           tg.centroid_lat,
                           tg.centroid_lon,
                           sr.report_id     AS storm_report_id,
                           sr.report_date   AS matched_storm_date,
                           sr.size_in       AS hail_size_in
                      FROM comal_storms sr
                      CROSS JOIN LATERAL (
                          SELECT tg.parcel_id,
                                 tg.centroid_lat, tg.centroid_lon
                            FROM comal_parcel_geometries tg
                           WHERE ST_DWithin(
                                     tg.geom,
                                     ST_SetSRID(ST_MakePoint(sr.lon, sr.lat), 4326),
                                     0.027
                                 )
                      ) tg
                     ORDER BY tg.parcel_id, sr.size_in DESC, sr.report_date DESC
                ),
                comal_candidate_with_addr AS (
                    SELECT cp.*,
                           tcp.situs_address AS address,
                           tcp.situs_city    AS city,
                           tcp.situs_zip     AS zip,
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(tcp.situs_address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_situs
                      FROM comal_candidate_parcels cp
                      JOIN tx_cad_parcels tcp
                            ON tcp.parcel_id = cp.parcel_id
                           AND tcp.cad_source = 'CCAD'
                     WHERE tcp.situs_address IS NOT NULL
                ),
                hll_comal_norm AS (
                    SELECT DISTINCT
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_addr
                      FROM hail_leads_list
                     WHERE county ILIKE 'comal'
                ),
                comal_rows AS (
                    SELECT
                        ca.parcel_id,
                        ca.address,
                        ca.city,
                        ca.zip,
                        'comal'::text                               AS county,
                        ca.centroid_lat,
                        ca.centroid_lon,
                        ca.matched_storm_date,
                        ca.hail_size_in,
                        (CURRENT_DATE - ca.matched_storm_date)::integer AS days_since_storm,
                        GREATEST(0, 365 - (CURRENT_DATE - ca.matched_storm_date))::double precision
                            / 365.0 * ca.hail_size_in                AS lead_score,
                        'Comal'::text                               AS county_source
                      FROM comal_candidate_with_addr ca
                     WHERE NOT EXISTS (
                         SELECT 1 FROM hll_comal_norm hcn
                          WHERE hcn.norm_addr = ca.norm_situs
                     )
                ),
                -- ============================== BEXAR ============================
                -- San Antonio (FREE Bexar County GIS source, BCAD). Mirrors Hays/
                -- Comal exactly: geom.parcel_id = tx_cad_parcels.parcel_id (BCAD
                -- PropID is stored identically as text in both tables, joins ~100%).
                -- cad_source = 'BCAD'. bbox lat 29.1-29.7, lon -98.9..-98.2 covers
                -- the San Antonio metro. hail_leads_list has no Bexar rows today so
                -- the serviced-exclusion is a no-op (pure fresh canvass list). This
                -- finally gives the un-serviced (parcels-minus-permits) signal for
                -- Bexar, which was previously EXCLUDED for lack of an address join.
                bexar_storms AS (
                    SELECT report_id, report_date, lat, lon,
                           COALESCE(size_in, 0.75) AS size_in
                      FROM spc_storm_reports
                     WHERE report_type = 'hail'
                       AND state = 'TX'
                       AND report_date >= CURRENT_DATE - INTERVAL '18 months'
                       AND lat  BETWEEN 29.1  AND 29.7
                       AND lon  BETWEEN -98.9 AND -98.2
                ),
                bexar_candidate_parcels AS (
                    SELECT DISTINCT ON (tg.parcel_id)
                           tg.parcel_id,
                           tg.centroid_lat,
                           tg.centroid_lon,
                           sr.report_id     AS storm_report_id,
                           sr.report_date   AS matched_storm_date,
                           sr.size_in       AS hail_size_in
                      FROM bexar_storms sr
                      CROSS JOIN LATERAL (
                          SELECT tg.parcel_id,
                                 tg.centroid_lat, tg.centroid_lon
                            FROM bexar_parcel_geometries tg
                           WHERE ST_DWithin(
                                     tg.geom,
                                     ST_SetSRID(ST_MakePoint(sr.lon, sr.lat), 4326),
                                     0.027
                                 )
                      ) tg
                     ORDER BY tg.parcel_id, sr.size_in DESC, sr.report_date DESC
                ),
                bexar_candidate_with_addr AS (
                    SELECT cp.*,
                           tcp.situs_address AS address,
                           tcp.situs_city    AS city,
                           tcp.situs_zip     AS zip,
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(tcp.situs_address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_situs
                      FROM bexar_candidate_parcels cp
                      JOIN tx_cad_parcels tcp
                            ON tcp.parcel_id = cp.parcel_id
                           AND tcp.cad_source = 'BCAD'
                     WHERE tcp.situs_address IS NOT NULL
                ),
                hll_bexar_norm AS (
                    SELECT DISTINCT
                           TRIM(REGEXP_REPLACE(
                               REGEXP_REPLACE(
                                   REGEXP_REPLACE(UPPER(address),
                                       '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
                               '[.,#]', '', 'g'),
                           '\\s+', ' ')) AS norm_addr
                      FROM hail_leads_list
                     WHERE county ILIKE 'bexar'
                ),
                bexar_rows AS (
                    SELECT
                        ca.parcel_id,
                        ca.address,
                        ca.city,
                        ca.zip,
                        'bexar'::text                               AS county,
                        ca.centroid_lat,
                        ca.centroid_lon,
                        ca.matched_storm_date,
                        ca.hail_size_in,
                        (CURRENT_DATE - ca.matched_storm_date)::integer AS days_since_storm,
                        GREATEST(0, 365 - (CURRENT_DATE - ca.matched_storm_date))::double precision
                            / 365.0 * ca.hail_size_in                AS lead_score,
                        'Bexar'::text                               AS county_source
                      FROM bexar_candidate_with_addr ca
                     WHERE NOT EXISTS (
                         SELECT 1 FROM hll_bexar_norm hbn
                          WHERE hbn.norm_addr = ca.norm_situs
                     )
                )
                SELECT parcel_id, address, city, zip, county,
                       centroid_lat, centroid_lon, matched_storm_date,
                       hail_size_in, days_since_storm, lead_score, county_source
                  FROM tarrant_rows
                UNION ALL
                SELECT parcel_id, address, city, zip, county,
                       centroid_lat, centroid_lon, matched_storm_date,
                       hail_size_in, days_since_storm, lead_score, county_source
                  FROM dallas_rows
                UNION ALL
                SELECT parcel_id, address, city, zip, county,
                       centroid_lat, centroid_lon, matched_storm_date,
                       hail_size_in, days_since_storm, lead_score, county_source
                  FROM hays_rows
                UNION ALL
                SELECT parcel_id, address, city, zip, county,
                       centroid_lat, centroid_lon, matched_storm_date,
                       hail_size_in, days_since_storm, lead_score, county_source
                  FROM comal_rows
                UNION ALL
                SELECT parcel_id, address, city, zip, county,
                       centroid_lat, centroid_lon, matched_storm_date,
                       hail_size_in, days_since_storm, lead_score, county_source
                  FROM bexar_rows
                WITH NO DATA
            """))
        # Indexes in their own txn — unique index enables REFRESH CONCURRENTLY.
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            for ddl in (
                # parcel_id is only unique within a county's CAD, so the unique
                # key needed for REFRESH CONCURRENTLY pairs it with county_source.
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_unserviced_hail_leads_parcel "
                "ON unserviced_hail_leads (parcel_id, county_source)",
                "CREATE INDEX IF NOT EXISTS ix_uhl_county "
                "ON unserviced_hail_leads (county)",
                "CREATE INDEX IF NOT EXISTS ix_uhl_storm_date "
                "ON unserviced_hail_leads (matched_storm_date DESC)",
                "CREATE INDEX IF NOT EXISTS ix_uhl_score "
                "ON unserviced_hail_leads (lead_score DESC NULLS LAST)",
            ):
                try:
                    await conn.execute(_text(ddl))
                except Exception as ie:  # noqa: BLE001
                    logger.warning("unserviced_hail_leads index skipped: %s", ie)
    except Exception as e:
        logger.warning("Could not create unserviced_hail_leads MV: %s", e)

    # ---------------------------------------------------------------------------
    # brazoria_permit_leads — Phase 3 of the TX permit-lead feed.
    #
    # Turns the Brazoria `hot_leads` source rows into a deduplicated, classified,
    # geocoded lead view: one row per normalized property address, classified to
    # new_construction / addition / remodel / other, with coords backfilled from
    # the geocoded_addresses cache and ALL contributing sources aggregated.
    #
    # SAFE on the 12.9M-row hot_leads table: filtered on `source IN (...)` via the
    # indexed ix_hot_leads_source — NEVER a county/full scan. The source list and
    # the classification rules live in app/services/permit_lead_classify.py and
    # are injected here so the SQL and the Python classifier stay in lockstep.
    #
    # Dedup: DISTINCT ON (address_norm), keeping the RICHEST row (most populated
    # fields), then aggregating the distinct sources and the EARLIEST event_date
    # (the leading-indicator trigger) across all rows at that address.
    #
    # Created WITH NO DATA — startup never blocks. The nightly mv_refresh job
    # (registered in app/services/mv_refresh._MVS) populates it. A UNIQUE index on
    # address_norm enables REFRESH ... CONCURRENTLY.
    # ---------------------------------------------------------------------------
    try:
        from app.services.permit_lead_classify import (
            brazoria_sources_sql,
            lead_class_sql,
            source_county_sql,
            trigger_sources_sql,
        )

        _blob = (
            "concat_ws(' ', hl.permit_type, hl.work_class, hl.description)"
        )
        _class_sql = lead_class_sql(blob_expr=_blob, source_col="hl.source")
        _county_sql = source_county_sql("hl.source")
        _src_in = brazoria_sources_sql()
        _trig_in = trigger_sources_sql()

        # SELF-HEAL stale definition: if the live MV predates the current source
        # registry or class rules (detected by a sentinel marker comment), drop
        # and rebuild. CREATE ... IF NOT EXISTS is a no-op when the object exists,
        # so without this a later rule change would never reach the DB object.
        _SENTINEL = "brazoria_permit_leads_v2"
        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '30s'"))
            try:
                live_def = await conn.scalar(_text(
                    "SELECT pg_get_viewdef('brazoria_permit_leads'::regclass)"
                ))
            except Exception:  # noqa: BLE001 — MV does not exist yet
                live_def = None
            if live_def is not None and _SENTINEL not in live_def:
                logger.warning(
                    "brazoria_permit_leads: stale live definition (missing %s "
                    "sentinel) — dropping to rebuild", _SENTINEL
                )
                await conn.execute(_text(
                    "DROP MATERIALIZED VIEW IF EXISTS brazoria_permit_leads CASCADE"
                ))

        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            await conn.execute(_text("SET LOCAL statement_timeout = '60s'"))
            await conn.execute(_text(f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS brazoria_permit_leads AS
                -- sentinel:{_SENTINEL}
                WITH src AS (
                    SELECT
                        hl.id,
                        hl.source,
                        hl.permit_number,
                        hl.permit_type,
                        hl.work_class,
                        hl.description,
                        hl.address,
                        hl.city,
                        hl.zip,
                        -- Canonicalize county so sources that tag it
                        -- differently ('Brazoria' vs 'BRAZORIA COUNTY')
                        -- collapse to ONE filterable value. Strip a trailing
                        -- ' COUNTY' then INITCAP.
                        INITCAP(
                            REGEXP_REPLACE(
                                COALESCE(hl.county, {_county_sql}),
                                '\\s+COUNTY\\s*$', '', 'i'
                            )
                        )                                              AS county,
                        hl.lat,
                        hl.lng,
                        hl.owner_name,
                        hl.valuation,
                        COALESCE(hl.issue_date, hl.applied_date)        AS event_date,
                        ({_class_sql})                                  AS lead_class,
                        UPPER(REGEXP_REPLACE(hl.address, '[^A-Za-z0-9 ]', ' ', 'g'))
                                                                        AS address_norm
                      FROM hot_leads hl
                     WHERE hl.source IN {_src_in}
                       AND hl.address IS NOT NULL
                       AND length(trim(hl.address)) > 3
                ),
                -- Per-address rollups: every distinct source + earliest trigger.
                agg AS (
                    SELECT
                        address_norm,
                        array_agg(DISTINCT source ORDER BY source)      AS sources,
                        min(event_date)                                 AS first_event_date,
                        max(event_date)                                 AS last_event_date,
                        bool_or(source IN {_trig_in})                   AS has_911_trigger,
                        count(*)                                        AS contributing_rows
                      FROM src
                     GROUP BY address_norm
                ),
                -- Richest row per address: prefer the row with the most populated
                -- fields (owner, coords, valuation, county), tie-break to the
                -- earliest event_date, then the smallest id for determinism.
                best AS (
                    SELECT DISTINCT ON (address_norm)
                        address_norm,
                        source                                          AS primary_source,
                        permit_number,
                        permit_type,
                        work_class,
                        description,
                        address,
                        city,
                        zip,
                        county,
                        lat,
                        lng,
                        owner_name,
                        valuation,
                        lead_class
                      FROM src
                     ORDER BY
                        address_norm,
                        ( (owner_name IS NOT NULL)::int
                        + (lat IS NOT NULL AND lng IS NOT NULL)::int
                        + (valuation IS NOT NULL)::int
                        + (county IS NOT NULL)::int
                        + (zip IS NOT NULL)::int ) DESC,
                        event_date ASC NULLS LAST,
                        id ASC
                )
                SELECT
                    b.address_norm,
                    b.address,
                    b.city,
                    b.zip,
                    b.county,
                    b.owner_name,
                    -- If ANY contributing source is a 911 new-address trigger the
                    -- property is new construction, regardless of the richest
                    -- row's class (mirrors classify_permit's source precedence).
                    CASE WHEN a.has_911_trigger THEN 'new_construction'
                         ELSE b.lead_class END                          AS lead_class,
                    a.first_event_date                                  AS event_date,
                    a.last_event_date,
                    b.primary_source,
                    a.sources,
                    a.contributing_rows,
                    -- coords: source coords first, else geocode cache backfill.
                    COALESCE(b.lat, g.lat::double precision)            AS lat,
                    COALESCE(b.lng, g.lon::double precision)            AS lng,
                    (b.lat IS NULL AND g.lat IS NOT NULL)               AS geocoded,
                    b.permit_number,
                    b.permit_type,
                    b.work_class,
                    b.description,
                    b.valuation
                  FROM best b
                  JOIN agg a USING (address_norm)
                  LEFT JOIN geocoded_addresses g
                         ON g.address_norm = b.address_norm
                        AND g.lat IS NOT NULL
                WITH NO DATA
            """))

        async with primary_engine.begin() as conn:
            await conn.execute(_text("SET LOCAL lock_timeout = '15s'"))
            for ddl in (
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_brazoria_permit_leads_addr "
                "ON brazoria_permit_leads (address_norm)",
                "CREATE INDEX IF NOT EXISTS ix_bpl_county "
                "ON brazoria_permit_leads (county)",
                "CREATE INDEX IF NOT EXISTS ix_bpl_class "
                "ON brazoria_permit_leads (lead_class)",
                "CREATE INDEX IF NOT EXISTS ix_bpl_event_date "
                "ON brazoria_permit_leads (event_date DESC NULLS LAST)",
            ):
                try:
                    await conn.execute(_text(ddl))
                except Exception as ie:  # noqa: BLE001
                    logger.warning("brazoria_permit_leads index skipped: %s", ie)
    except Exception as e:
        logger.warning("Could not create brazoria_permit_leads MV: %s", e)

    print("[migrations] background auto-migration run complete", flush=True, file=sys.stderr)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events.

    init_db is bounded with a 20s timeout so a flaky Tailscale-DB route
    can't hang the container forever. Auto-migrations run as a detached
    background task — any single hung migration (the hail_leads_spc MV
    has a 30-minute statement_timeout because it walks 17M rows) leaves
    the API serving traffic instead of bricking startup.
    """
    import sys
    print(f"[lifespan] starting v{settings.VERSION}", flush=True, file=sys.stderr)
    logger.info("Starting PermitLookup API v%s", settings.VERSION)

    try:
        await asyncio.wait_for(init_db(), timeout=20)
        print("[lifespan] init_db done", flush=True, file=sys.stderr)
        logger.info("Database initialized")
    except (asyncio.TimeoutError, Exception) as e:
        print(f"[lifespan] init_db skipped: {type(e).__name__}: {e}", flush=True, file=sys.stderr)
        logger.warning("Database not available at startup: %s", e)

    # Fire-and-forget background migrations. Keep a reference so the task
    # isn't garbage-collected mid-run (asyncio quirk).
    app.state.migrations_task = asyncio.create_task(_run_startup_migrations())

    from app.services.scheduler import start_scheduler, stop_scheduler
    SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "true").lower() == "true"
    if SCHEDULER_ENABLED:
        try:
            start_scheduler()
            logger.info("Scheduler started")
        except Exception as e:
            logger.warning("Failed to start alert scheduler: %s", e)
    else:
        logger.warning("SCHEDULER_ENABLED=false — scheduler skipped (alert batches will not run)")

    print("[lifespan] yielding to uvicorn", flush=True, file=sys.stderr)
    yield

    try:
        stop_scheduler()
    except Exception:
        pass
    logger.info("Shutting down PermitLookup API")


app = FastAPI(
    title="PermitLookup API",
    description="Search 1B+ property and permit records from 180+ jurisdictions across 50+ states. "
    "Includes building permits, contractor licenses, EPA environmental risk, FEMA flood zones, "
    "septic systems, census demographics, and property valuations. "
    "Address lookup, bulk search, geo search, and filtering by permit type, date, status, and more.",
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "Authorization"],
    max_age=3600,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# ---------------------------------------------------------------------------
# /docs + /openapi.json rate limiting
# ---------------------------------------------------------------------------
# Docs are intentionally public (sales asset) but scrapers should be throttled.
# 60 requests/minute per IP is generous for developers and tight for bots.

import time as _time  # noqa: E402

_docs_rate_store: dict[str, list[float]] = {}
_DOCS_RATE_WINDOW = 60    # seconds
_DOCS_RATE_LIMIT = 60     # requests per window per IP
_DOCS_PATHS = {"/docs", "/redoc", "/openapi.json"}


@app.middleware("http")
async def docs_rate_limit_middleware(request: Request, call_next):
    if request.url.path in _DOCS_PATHS:
        ip = getattr(request.client, "host", None) or "unknown"
        now = _time.monotonic()
        bucket = _docs_rate_store.get(ip, [])
        bucket = [t for t in bucket if now - t < _DOCS_RATE_WINDOW]
        bucket.append(now)
        _docs_rate_store[ip] = bucket
        if len(bucket) > _DOCS_RATE_LIMIT:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too many requests. Try again later.",
                    "retry_after_seconds": _DOCS_RATE_WINDOW,
                },
                headers={"Retry-After": str(_DOCS_RATE_WINDOW)},
            )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Transient-DB-disconnect retry middleware
# ---------------------------------------------------------------------------
# Railway's egress NAT rebinds its UDP port after an outage, which flaps the
# Tailscale *direct* WireGuard path and drops in-flight PG connections
# (asyncpg ConnectionDoesNotExistError, usually wrapped by SQLAlchemy). Only
# idempotent GETs are retried — never POST/PATCH/PUT/DELETE, to avoid double
# writes and double activity-tracking.
import asyncpg  # noqa: E402
import sqlalchemy.exc  # noqa: E402

_TRANSIENT_EXC = (
    sqlalchemy.exc.DBAPIError,
    sqlalchemy.exc.OperationalError,
    sqlalchemy.exc.InterfaceError,
    asyncpg.exceptions.ConnectionDoesNotExistError,
    asyncpg.exceptions.InterfaceError,
    asyncpg.exceptions.CannotConnectNowError,
    ConnectionResetError,
    OSError,
    asyncio.TimeoutError,
)


def _is_transient_disconnect(exc: BaseException) -> bool:
    """True if exc — or any link in its __cause__/__context__ chain — is a
    transient DB disconnect we can safely retry."""
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, _TRANSIENT_EXC):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


@app.middleware("http")
async def db_retry_middleware(request, call_next):
    if request.method != "GET":
        return await call_next(request)

    attempts = 4
    for attempt in range(1, attempts + 1):
        try:
            return await call_next(request)
        except BaseException as exc:
            if attempt >= attempts or not _is_transient_disconnect(exc):
                raise
            await asyncio.sleep(0.3 * attempt)


# Mount v1 routers
app.include_router(permits_router, prefix="/v1")
app.include_router(auth_router, prefix="/v1")
app.include_router(billing_router, prefix="/v1")
app.include_router(coverage_router, prefix="/v1")
app.include_router(contractors_router, prefix="/v1")
app.include_router(alerts_router, prefix="/v1")
app.include_router(properties_router, prefix="/v1")
app.include_router(market_router, prefix="/v1")
app.include_router(saved_searches_router, prefix="/v1")
app.include_router(admin_router, prefix="/v1")
app.include_router(licenses_router, prefix="/v1")
app.include_router(environmental_router, prefix="/v1")
app.include_router(septic_router, prefix="/v1")
app.include_router(demographics_router, prefix="/v1")
app.include_router(valuations_router, prefix="/v1")
app.include_router(entities_router, prefix="/v1")
app.include_router(pipeline_router, prefix="/v1")
app.include_router(violations_router, prefix="/v1")
app.include_router(predictions_router, prefix="/v1")
app.include_router(sales_router, prefix="/v1")
app.include_router(liens_router, prefix="/v1")
app.include_router(dialer_router, prefix="/v1")
app.include_router(crm_router, prefix="/v1")
app.include_router(quotes_router, prefix="/v1")
app.include_router(analyst_router, prefix="/v1")
app.include_router(trends_router, prefix="/v1")
app.include_router(batch_router, prefix="/v1")
app.include_router(campaigns_router, prefix="/v1")
app.include_router(dialer_ws_router)  # WebSocket routes at root (no /v1 prefix)
app.include_router(freshness_router, prefix="/v1")
app.include_router(data_freshness_router, prefix="/v1")
app.include_router(hman_auth_router, prefix="/v1")
app.include_router(pricing_router, prefix="/v1")
app.include_router(hail_leads_router, prefix="/v1")
app.include_router(permit_leads_router, prefix="/v1")
app.include_router(parcel_screen_router, prefix="/v1")
app.include_router(broadband_router, prefix="/v1")
app.include_router(internal_rural_v5_router, prefix="/v1")
app.include_router(roofer_leads_router, prefix="/v1")
app.include_router(enrichment_router, prefix="/v1")
app.include_router(rural_score_router, prefix="/v1")
app.include_router(wells_router, prefix="/v1")
app.include_router(well_permits_router, prefix="/v1")
app.include_router(map_tiles_router, prefix="/v1")


@app.get("/healthz")
async def healthz():
    """Liveness probe — NO DB dependency. Returns 200 as soon as uvicorn is
    serving requests. Used by Railway's deploy healthcheck (railway.toml) so
    the previous container keeps serving traffic until the new one is up,
    eliminating the public 502 window during deploys. For DB-aware health,
    use /health."""
    return {"status": "ok", "version": settings.VERSION}


@app.get("/health")
async def health():
    """Health check — returns 503 only if PRIMARY DB is unreachable. Replica failure is non-fatal."""
    import asyncio
    from app.database import primary_session_maker, replica_session_maker, _replica_is_separate
    from sqlalchemy import text

    primary_ok = False
    try:
        async with primary_session_maker() as db:
            await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=5.0)
        primary_ok = True
    except Exception:
        pass

    replica_ok = False
    if _replica_is_separate:
        try:
            async with replica_session_maker() as db:
                await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=5.0)
            replica_ok = True
        except Exception:
            pass
    else:
        replica_ok = primary_ok

    # App is healthy if primary works (replica failure is degraded, not down)
    status_code = 200 if primary_ok else 503
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if primary_ok else "unhealthy",
            "database": "connected" if primary_ok else "unreachable",
            "replica": "connected" if replica_ok else "down (using primary fallback)",
            "version": settings.VERSION,
            "environment": settings.ENVIRONMENT,
            "build": "cf-tunnel-v1",
        },
    )


@app.post("/health/db/migrate-expansion")
async def migrate_expansion():
    """Add new columns and tables for industry expansion."""
    from app.database import primary_session_maker as async_session_maker
    from sqlalchemy import text
    migrations = []
    async with async_session_maker() as db:
        # Add columns to permit_alerts
        for col, typ, default in [
            ("last_error", "TEXT", None),
            ("consecutive_failures", "INTEGER", "0"),
        ]:
            try:
                defstr = f" DEFAULT {default}" if default else ""
                await db.execute(text(f"ALTER TABLE permit_alerts ADD COLUMN {col} {typ}{defstr}"))
                migrations.append(f"permit_alerts.{col} added")
            except Exception:
                migrations.append(f"permit_alerts.{col} already exists")
                await db.rollback()

        # Create alert_execution_history table
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS alert_execution_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    alert_id UUID REFERENCES permit_alerts(id) ON DELETE CASCADE,
                    run_at TIMESTAMPTZ DEFAULT NOW(),
                    match_count INTEGER DEFAULT 0,
                    delivery_method VARCHAR(20),
                    delivery_status VARCHAR(20),
                    error TEXT,
                    matches_sample JSONB
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_history_alert_run ON alert_execution_history (alert_id, run_at)"))
            migrations.append("alert_execution_history table created")
        except Exception as e:
            migrations.append(f"alert_execution_history: {e}")
            await db.rollback()

        # Create saved_searches table
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS saved_searches (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    name VARCHAR(200) NOT NULL,
                    filters JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_run_at TIMESTAMPTZ
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_saved_searches_user ON saved_searches (user_id)"))
            migrations.append("saved_searches table created")
        except Exception as e:
            migrations.append(f"saved_searches: {e}")
            await db.rollback()

        # ---- UsageLog new columns (security services) ----
        for col, typ in [
            ("result_count", "INTEGER"),
            ("response_bytes", "INTEGER"),
            ("query_hash", "VARCHAR(64)"),
            ("abuse_score", "INTEGER"),
        ]:
            try:
                await db.execute(text(f"ALTER TABLE usage_logs ADD COLUMN {col} {typ}"))
                migrations.append(f"usage_logs.{col} added")
            except Exception:
                migrations.append(f"usage_logs.{col} already exists")
                await db.rollback()

        # ---- Data expansion tables (Phase 1) ----
        new_tables = {
            "contractor_licenses": """
                CREATE TABLE IF NOT EXISTS contractor_licenses (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    license_number VARCHAR(100) NOT NULL,
                    business_name VARCHAR(500) NOT NULL,
                    full_business_name VARCHAR(500),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    phone VARCHAR(20),
                    business_type VARCHAR(50),
                    issue_date DATE,
                    expiration_date DATE,
                    status VARCHAR(50),
                    secondary_status VARCHAR(100),
                    classifications TEXT,
                    workers_comp_type VARCHAR(100),
                    workers_comp_company VARCHAR(255),
                    surety_company VARCHAR(255),
                    surety_amount FLOAT,
                    source VARCHAR(50) NOT NULL,
                    last_updated DATE
                )
            """,
            "epa_facilities": """
                CREATE TABLE IF NOT EXISTS epa_facilities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    registry_id VARCHAR(50) NOT NULL UNIQUE,
                    name VARCHAR(500) NOT NULL,
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    epa_region VARCHAR(5),
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) DEFAULT 'epa_frs'
                )
            """,
            "fema_flood_zones": """
                CREATE TABLE IF NOT EXISTS fema_flood_zones (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    dfirm_id VARCHAR(20) NOT NULL,
                    fld_zone VARCHAR(20) NOT NULL,
                    zone_subtype VARCHAR(100),
                    sfha_tf VARCHAR(1),
                    static_bfe FLOAT,
                    state_fips VARCHAR(2) NOT NULL,
                    state_abbrev VARCHAR(2) NOT NULL,
                    county_fips VARCHAR(5)
                )
            """,
            "census_demographics": """
                CREATE TABLE IF NOT EXISTS census_demographics (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    state_fips VARCHAR(2) NOT NULL,
                    county_fips VARCHAR(3) NOT NULL,
                    tract VARCHAR(6) NOT NULL,
                    block_group VARCHAR(1),
                    name VARCHAR(500),
                    population INTEGER,
                    median_income INTEGER,
                    median_home_value INTEGER,
                    homeownership_rate FLOAT,
                    median_year_built INTEGER,
                    total_housing_units INTEGER,
                    occupied_units INTEGER,
                    vacancy_rate FLOAT
                )
            """,
            "septic_systems": """
                CREATE TABLE IF NOT EXISTS septic_systems (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    parcel_id VARCHAR(200),
                    lat FLOAT,
                    lng FLOAT,
                    system_type VARCHAR(100),
                    wastewater_source VARCHAR(200),
                    install_date DATE,
                    last_inspection DATE,
                    land_use VARCHAR(50),
                    status VARCHAR(50),
                    source VARCHAR(50) NOT NULL
                )
            """,
            "property_valuations": """
                CREATE TABLE IF NOT EXISTS property_valuations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    zip VARCHAR(10) NOT NULL,
                    state VARCHAR(2),
                    state_code VARCHAR(2),
                    city VARCHAR(100),
                    region VARCHAR(200),
                    property_type VARCHAR(50),
                    period_begin DATE NOT NULL,
                    period_end DATE NOT NULL,
                    median_sale_price FLOAT,
                    median_list_price FLOAT,
                    median_ppsf FLOAT,
                    median_list_ppsf FLOAT,
                    homes_sold INTEGER,
                    pending_sales INTEGER,
                    new_listings INTEGER,
                    inventory INTEGER,
                    months_of_supply FLOAT,
                    median_dom INTEGER,
                    avg_sale_to_list FLOAT,
                    sold_above_list FLOAT,
                    price_drops FLOAT,
                    parent_metro VARCHAR(200)
                )
            """,
            "business_entities": """
                CREATE TABLE IF NOT EXISTS business_entities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entity_name VARCHAR(500) NOT NULL,
                    entity_type VARCHAR(50),
                    state VARCHAR(2) NOT NULL,
                    filing_number VARCHAR(100),
                    status VARCHAR(50),
                    formation_date DATE,
                    dissolution_date DATE,
                    registered_agent_name VARCHAR(500),
                    registered_agent_address VARCHAR(500),
                    principal_address VARCHAR(500),
                    mailing_address VARCHAR(500),
                    officers JSONB,
                    source VARCHAR(50) NOT NULL,
                    scraped_at DATE
                )
            """,
            "code_violations": """
                CREATE TABLE IF NOT EXISTS code_violations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    violation_id VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    violation_type VARCHAR(200),
                    violation_code VARCHAR(100),
                    description TEXT,
                    status VARCHAR(50),
                    violation_date DATE,
                    inspection_date DATE,
                    resolution_date DATE,
                    fine_amount FLOAT,
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) NOT NULL
                )
            """,
            "permit_predictions": """
                CREATE TABLE IF NOT EXISTS permit_predictions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    zip VARCHAR(10) NOT NULL,
                    state VARCHAR(2),
                    prediction_score FLOAT,
                    predicted_permits INTEGER,
                    confidence FLOAT,
                    features JSONB,
                    risk_factors JSONB,
                    model_version VARCHAR(50),
                    scored_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "property_sales": """
                CREATE TABLE IF NOT EXISTS property_sales (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_id VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    borough VARCHAR(50),
                    sale_price FLOAT,
                    sale_date DATE,
                    recorded_date DATE,
                    doc_type VARCHAR(50),
                    grantor VARCHAR(500),
                    grantee VARCHAR(500),
                    property_type VARCHAR(100),
                    building_class VARCHAR(50),
                    residential_units INTEGER,
                    land_sqft FLOAT,
                    gross_sqft FLOAT,
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) NOT NULL
                )
            """,
            "property_liens": """
                CREATE TABLE IF NOT EXISTS property_liens (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_id VARCHAR(100),
                    lien_type VARCHAR(100),
                    filing_number VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    borough VARCHAR(50),
                    amount FLOAT,
                    filing_date DATE,
                    lapse_date DATE,
                    status VARCHAR(50),
                    debtor_name VARCHAR(500),
                    creditor_name VARCHAR(500),
                    description TEXT,
                    source VARCHAR(50) NOT NULL
                )
            """,
        }

        for table_name, ddl in new_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Sales Dialer tables ----
        dialer_tables = {
            "call_logs": """
                CREATE TABLE IF NOT EXISTS call_logs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    lead_id UUID,
                    phone_number VARCHAR(20),
                    duration_seconds INTEGER,
                    disposition VARCHAR(50),
                    notes TEXT,
                    ai_summary TEXT,
                    action_items JSONB,
                    callback_date TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "lead_statuses": """
                CREATE TABLE IF NOT EXISTS lead_statuses (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    lead_id UUID NOT NULL,
                    status VARCHAR(50) DEFAULT 'new',
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in dialer_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- CRM tables ----
        crm_tables = {
            "contacts": """
                CREATE TABLE IF NOT EXISTS contacts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    name TEXT NOT NULL,
                    company TEXT,
                    phone VARCHAR(20),
                    email VARCHAR(255),
                    address TEXT,
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip VARCHAR(10),
                    lead_source VARCHAR(50) DEFAULT 'permit',
                    lead_id UUID,
                    tags JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "deals": """
                CREATE TABLE IF NOT EXISTS deals (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    title TEXT,
                    stage VARCHAR(50) DEFAULT 'new',
                    value FLOAT,
                    expected_close_date DATE,
                    actual_close_date DATE,
                    lost_reason TEXT,
                    notes TEXT,
                    permit_number VARCHAR(100),
                    permit_type VARCHAR(50),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "crm_notes": """
                CREATE TABLE IF NOT EXISTS crm_notes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    deal_id UUID REFERENCES deals(id),
                    content TEXT NOT NULL,
                    note_type VARCHAR(20) DEFAULT 'note',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "commissions": """
                CREATE TABLE IF NOT EXISTS commissions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    deal_id UUID REFERENCES deals(id),
                    amount FLOAT,
                    rate FLOAT DEFAULT 0.10,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in crm_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Add review_requested_at to deals ----
        try:
            await db.execute(text("ALTER TABLE deals ADD COLUMN review_requested_at TIMESTAMPTZ"))
            migrations.append("deals.review_requested_at added")
        except Exception:
            migrations.append("deals.review_requested_at already exists")
            await db.rollback()

        # ---- Quote/Estimate tables ----
        quote_tables = {
            "quotes": """
                CREATE TABLE IF NOT EXISTS quotes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    deal_id UUID REFERENCES deals(id),
                    items JSONB,
                    subtotal FLOAT DEFAULT 0.0,
                    tax_rate FLOAT DEFAULT 0.0,
                    tax_amount FLOAT DEFAULT 0.0,
                    total FLOAT DEFAULT 0.0,
                    status VARCHAR(20) DEFAULT 'draft',
                    valid_until DATE,
                    sent_at TIMESTAMPTZ,
                    accepted_at TIMESTAMPTZ,
                    notes TEXT,
                    terms TEXT,
                    company_name VARCHAR(200),
                    company_phone VARCHAR(20),
                    company_email VARCHAR(200),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in quote_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Team Management tables ----
        team_tables = {
            "teams": """
                CREATE TABLE IF NOT EXISTS teams (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(200) NOT NULL,
                    owner_id UUID NOT NULL REFERENCES api_users(id),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "team_members": """
                CREATE TABLE IF NOT EXISTS team_members (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    role VARCHAR(20) DEFAULT 'member',
                    territories JSONB
                )
            """,
        }
        for table_name, ddl in team_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Activities table (collaboration feed) ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS activities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    team_id UUID REFERENCES teams(id) ON DELETE SET NULL,
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    activity_type VARCHAR(50) NOT NULL,
                    description TEXT,
                    entity_type VARCHAR(20),
                    entity_id UUID,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            migrations.append("activities table created")
        except Exception as e:
            migrations.append(f"activities: {e}")
            await db.rollback()

        # ---- Webhooks table ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS webhooks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    name VARCHAR(200),
                    url TEXT NOT NULL,
                    event_types JSONB DEFAULT '[]'::jsonb,
                    filters JSONB DEFAULT '{}'::jsonb,
                    is_active BOOLEAN DEFAULT TRUE,
                    secret VARCHAR(100),
                    last_triggered TIMESTAMPTZ,
                    failure_count INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            migrations.append("webhooks table created")
        except Exception as e:
            migrations.append(f"webhooks: {e}")
            await db.rollback()

        # ---- Batch Jobs table ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    status VARCHAR(20) DEFAULT 'pending',
                    total_addresses INTEGER DEFAULT 0,
                    processed INTEGER DEFAULT 0,
                    results JSONB,
                    error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                )
            """))
            migrations.append("batch_jobs table created")
        except Exception as e:
            migrations.append(f"batch_jobs: {e}")
            await db.rollback()

        # ---- Email Campaign tables ----
        email_tables = {
            "email_campaigns": """
                CREATE TABLE IF NOT EXISTS email_campaigns (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(200) NOT NULL,
                    subject VARCHAR(500) NOT NULL,
                    body_html TEXT,
                    body_text TEXT,
                    target_audience VARCHAR(100),
                    target_state VARCHAR(2),
                    status VARCHAR(20) DEFAULT 'draft',
                    sent_count INTEGER DEFAULT 0,
                    open_count INTEGER DEFAULT 0,
                    click_count INTEGER DEFAULT 0,
                    unsubscribe_count INTEGER DEFAULT 0,
                    signup_count INTEGER DEFAULT 0,
                    bounce_count INTEGER DEFAULT 0,
                    send_rate INTEGER DEFAULT 200,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """,
            "email_recipients": """
                CREATE TABLE IF NOT EXISTS email_recipients (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    campaign_id UUID REFERENCES email_campaigns(id) ON DELETE CASCADE,
                    email VARCHAR(255) NOT NULL,
                    name VARCHAR(500),
                    company VARCHAR(500),
                    state VARCHAR(2),
                    license_type VARCHAR(100),
                    status VARCHAR(20) DEFAULT 'pending',
                    sent_at TIMESTAMPTZ,
                    opened_at TIMESTAMPTZ,
                    clicked_at TIMESTAMPTZ,
                    unsubscribed_at TIMESTAMPTZ
                )
            """,
            "email_unsubscribes": """
                CREATE TABLE IF NOT EXISTS email_unsubscribes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email VARCHAR(255) NOT NULL UNIQUE,
                    reason TEXT,
                    unsubscribed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in email_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # Indexes for new tables
        indexes = [
            # Email campaign indexes
            "CREATE INDEX IF NOT EXISTS ix_ec_status ON email_campaigns (status)",
            "CREATE INDEX IF NOT EXISTS ix_ec_audience ON email_campaigns (target_audience)",
            "CREATE INDEX IF NOT EXISTS ix_er_campaign_status ON email_recipients (campaign_id, status)",
            "CREATE INDEX IF NOT EXISTS ix_er_email ON email_recipients (email)",
            "CREATE INDEX IF NOT EXISTS ix_er_sent_at ON email_recipients (sent_at)",
            "CREATE INDEX IF NOT EXISTS ix_eu_email ON email_unsubscribes (email)",
            "CREATE INDEX IF NOT EXISTS ix_cl_license ON contractor_licenses (license_number)",
            "CREATE INDEX IF NOT EXISTS ix_cl_name ON contractor_licenses (business_name)",
            "CREATE INDEX IF NOT EXISTS ix_cl_state ON contractor_licenses (state)",
            "CREATE INDEX IF NOT EXISTS ix_cl_status ON contractor_licenses (state, status)",
            "CREATE INDEX IF NOT EXISTS ix_epa_registry ON epa_facilities (registry_id)",
            "CREATE INDEX IF NOT EXISTS ix_epa_geo ON epa_facilities (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_epa_state ON epa_facilities (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_fema_state ON fema_flood_zones (state_abbrev, fld_zone)",
            "CREATE INDEX IF NOT EXISTS ix_fema_dfirm ON fema_flood_zones (dfirm_id)",
            "CREATE INDEX IF NOT EXISTS ix_census_geo ON census_demographics (state_fips, county_fips, tract, block_group)",
            "CREATE INDEX IF NOT EXISTS ix_census_state ON census_demographics (state_fips, county_fips)",
            "CREATE INDEX IF NOT EXISTS ix_septic_state ON septic_systems (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_septic_geo ON septic_systems (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_septic_addr ON septic_systems (address)",
            "CREATE INDEX IF NOT EXISTS ix_val_zip ON property_valuations (zip, period_end)",
            "CREATE INDEX IF NOT EXISTS ix_val_state ON property_valuations (state, zip)",
            "CREATE INDEX IF NOT EXISTS ix_entity_name ON business_entities (entity_name)",
            "CREATE INDEX IF NOT EXISTS ix_entity_filing ON business_entities (filing_number, state)",
            "CREATE INDEX IF NOT EXISTS ix_entity_state ON business_entities (state, entity_type)",
            "CREATE INDEX IF NOT EXISTS ix_entity_agent ON business_entities (registered_agent_name)",
            "CREATE INDEX IF NOT EXISTS ix_violations_vid ON code_violations (violation_id)",
            "CREATE INDEX IF NOT EXISTS ix_violations_addr ON code_violations (address)",
            "CREATE INDEX IF NOT EXISTS ix_violations_city ON code_violations (city)",
            "CREATE INDEX IF NOT EXISTS ix_violations_state ON code_violations (state)",
            "CREATE INDEX IF NOT EXISTS ix_violations_status ON code_violations (status)",
            "CREATE INDEX IF NOT EXISTS ix_violations_date ON code_violations (violation_date)",
            "CREATE INDEX IF NOT EXISTS ix_violations_geo ON code_violations (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_violations_source_vid ON code_violations (source, violation_id)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_zip ON permit_predictions (zip)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_state_score ON permit_predictions (state, prediction_score DESC)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_scored_at ON permit_predictions (scored_at)",
            # property_sales indexes
            "CREATE INDEX IF NOT EXISTS ix_sales_doc_id ON property_sales (document_id)",
            "CREATE INDEX IF NOT EXISTS ix_sales_address ON property_sales (address)",
            "CREATE INDEX IF NOT EXISTS ix_sales_city ON property_sales (city)",
            "CREATE INDEX IF NOT EXISTS ix_sales_state ON property_sales (state)",
            "CREATE INDEX IF NOT EXISTS ix_sales_zip ON property_sales (zip)",
            "CREATE INDEX IF NOT EXISTS ix_sales_state_city ON property_sales (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_sales_zip_date ON property_sales (zip, sale_date)",
            "CREATE INDEX IF NOT EXISTS ix_sales_sale_date ON property_sales (sale_date)",
            "CREATE INDEX IF NOT EXISTS ix_sales_grantor ON property_sales (grantor)",
            "CREATE INDEX IF NOT EXISTS ix_sales_grantee ON property_sales (grantee)",
            # property_liens indexes
            "CREATE INDEX IF NOT EXISTS ix_liens_doc_id ON property_liens (document_id)",
            "CREATE INDEX IF NOT EXISTS ix_liens_address ON property_liens (address)",
            "CREATE INDEX IF NOT EXISTS ix_liens_lien_type ON property_liens (lien_type)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_number ON property_liens (filing_number)",
            "CREATE INDEX IF NOT EXISTS ix_liens_state ON property_liens (state)",
            "CREATE INDEX IF NOT EXISTS ix_liens_state_type ON property_liens (state, lien_type)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_date ON property_liens (filing_date)",
            "CREATE INDEX IF NOT EXISTS ix_liens_debtor ON property_liens (debtor_name)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_state ON property_liens (filing_number, state)",
            "CREATE INDEX IF NOT EXISTS ix_liens_zip ON property_liens (zip)",
            # call_logs indexes
            "CREATE INDEX IF NOT EXISTS ix_call_logs_user ON call_logs (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_lead ON call_logs (lead_id)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_user_date ON call_logs (user_id, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_callback ON call_logs (user_id, callback_date) WHERE callback_date IS NOT NULL",
            # lead_statuses indexes
            "CREATE INDEX IF NOT EXISTS ix_lead_statuses_lead ON lead_statuses (lead_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_lead_status_user_lead ON lead_statuses (user_id, lead_id)",
            # CRM indexes
            "CREATE INDEX IF NOT EXISTS ix_contacts_user ON contacts (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_contacts_user_phone ON contacts (user_id, phone)",
            "CREATE INDEX IF NOT EXISTS ix_contacts_user_email ON contacts (user_id, email)",
            "CREATE INDEX IF NOT EXISTS ix_deals_user ON deals (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_deals_user_stage ON deals (user_id, stage)",
            "CREATE INDEX IF NOT EXISTS ix_deals_contact ON deals (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_crm_notes_contact ON crm_notes (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_crm_notes_deal ON crm_notes (deal_id)",
            "CREATE INDEX IF NOT EXISTS ix_commissions_user ON commissions (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_commissions_deal ON commissions (deal_id)",
            # quotes indexes
            "CREATE INDEX IF NOT EXISTS ix_quotes_user ON quotes (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_contact ON quotes (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_deal ON quotes (deal_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_user_status ON quotes (user_id, status)",
            # teams indexes
            "CREATE INDEX IF NOT EXISTS ix_teams_owner ON teams (owner_id)",
            "CREATE INDEX IF NOT EXISTS ix_team_members_team ON team_members (team_id)",
            "CREATE INDEX IF NOT EXISTS ix_team_members_user ON team_members (user_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_team_members_team_user ON team_members (team_id, user_id)",
            # activities indexes
            "CREATE INDEX IF NOT EXISTS ix_activities_team ON activities (team_id)",
            "CREATE INDEX IF NOT EXISTS ix_activities_user ON activities (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_activities_team_created ON activities (team_id, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_activities_user_created ON activities (user_id, created_at)",
            # webhooks indexes
            "CREATE INDEX IF NOT EXISTS ix_webhooks_user ON webhooks (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_webhooks_active ON webhooks (user_id, is_active)",
            # batch_jobs indexes
            "CREATE INDEX IF NOT EXISTS ix_batch_jobs_user ON batch_jobs (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_batch_jobs_user_created ON batch_jobs (user_id, created_at)",
        ]
        for idx_sql in indexes:
            try:
                await db.execute(text(idx_sql))
            except Exception:
                pass

        await db.commit()
    return {"migrations": migrations}


@app.get("/health/db")
async def health_db():
    """Test database connectivity for both primary and replica."""
    import time
    from app.database import primary_session_maker, replica_session_maker, _replica_is_separate
    from sqlalchemy import text
    result = {}

    # Test primary (T430)
    t0 = time.time()
    try:
        async with primary_session_maker() as db:
            r = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'"))
            count = r.scalar()
        result["primary"] = {"status": "ok", "permits": count, "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        result["primary"] = {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}

    # Test replica (R730-2)
    if _replica_is_separate:
        t0 = time.time()
        try:
            async with replica_session_maker() as db:
                r = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'"))
                count = r.scalar()
            result["replica"] = {"status": "ok", "permits": count, "latency_ms": round((time.time() - t0) * 1000)}
        except Exception as e:
            result["replica"] = {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}
    else:
        result["replica"] = {"status": "not_configured", "note": "Using primary for all queries"}

    overall = "ok" if result["primary"]["status"] == "ok" else "degraded"
    return {"status": overall, **result}


STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# PWA: service worker must be served from root scope
@app.get("/sw.js", include_in_schema=False)
async def service_worker():
    return FileResponse(
        STATIC_DIR / "sw.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"},
    )


_INDEX_HTML_CACHE: str | None = None

def _get_index_html() -> str:
    """Read index.html and patch the dead Twilio SDK URL at runtime."""
    global _INDEX_HTML_CACHE
    if _INDEX_HTML_CACHE is None:
        raw = (STATIC_DIR / "index.html").read_text()
        # Old SDK 1.14.3 is 403'd — replace with Voice SDK 2.x
        raw = raw.replace(
            'https://sdk.twilio.com/js/client/releases/1.14.3/twilio.js',
            'https://cdn.jsdelivr.net/npm/@twilio/voice-sdk@2/dist/twilio.min.js',
        )
        _INDEX_HTML_CACHE = raw
    return _INDEX_HTML_CACHE


@app.get("/")
async def root():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(_get_index_html(), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/map", include_in_schema=False)
async def map_page():
    return FileResponse(STATIC_DIR / "map.html",
                        headers={"Cache-Control": "no-cache, must-revalidate"})


# SEO: robots.txt + sitemap. Both previously 404'd (no route + explicit SPA
# allowlist), so crawlers had no sitemap and the site was reachable on two
# hostnames with no canonical → Google's duplicate-content/canonical noise.
# The self-referential canonical now lives in index.html's <head>.
_CANONICAL_ORIGIN = "https://permits.ecbtx.com"


@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    from fastapi.responses import PlainTextResponse
    body = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /v1/\n"
        "Disallow: /docs\n"
        f"Sitemap: {_CANONICAL_ORIGIN}/sitemap.xml\n"
    )
    return PlainTextResponse(body, media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml():
    from fastapi.responses import Response
    urls = ["/", "/search", "/coverage", "/pricing"]
    locs = "".join(
        f"<url><loc>{_CANONICAL_ORIGIN}{u}</loc></url>" for u in urls
    )
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{locs}</urlset>"
    )
    return Response(body, media_type="application/xml")


# SPA catch-all routes — serve index.html for frontend pages
async def _spa_page():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(_get_index_html(), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

for _path in ("/search", "/coverage", "/pricing", "/dashboard", "/contractors", "/alerts", "/properties", "/market", "/saved-searches", "/admin", "/dialer", "/crm", "/quotes", "/analyst", "/trends", "/batch", "/campaigns", "/unsubscribe"):
    app.get(_path, include_in_schema=False)(_spa_page)


@app.get("/api")
async def api_info():
    return {
        "name": "PermitLookup API",
        "version": settings.VERSION,
        "docs": "/docs",
        "description": "Building permit data API — search ~1B records from 180+ jurisdictions",
        "endpoints": {
            "search": "GET /v1/permits/search?address=...",
            "bulk": "POST /v1/permits/bulk",
            "coverage": "GET /v1/coverage",
            "usage": "GET /v1/usage",
            "signup": "POST /v1/signup",
            "alerts": "GET /v1/alerts",
            "properties": "GET /v1/properties/history?address=...",
            "market": "GET /v1/market/activity?zip=78701&months=6",
            "saved_searches": "GET /v1/saved-searches",
            "licenses": "GET /v1/licenses/verify?name=...&state=CA",
            "environmental": "GET /v1/environmental/risk?lat=...&lng=...&state=TX",
            "septic": "GET /v1/septic/lookup?address=...&state=FL",
            "demographics": "GET /v1/demographics/county?state=TX&county_fips=201",
            "valuations": "GET /v1/valuations/zip?zip=78701",
            "entities": "GET /v1/entities/search?name=Sunrise+Holdings&state=TX",
            "pipeline": "GET /v1/pipeline/permit-to-sale?zip=78701&months=12",
            "hot_zips": "GET /v1/pipeline/hot-zips?state=TX&limit=25",
            "violations_search": "GET /v1/violations/search?address=...&city=...&state=NY",
            "violations_property": "GET /v1/violations/property?address=123+Main+St&state=NY",
            "violations_stats": "GET /v1/violations/stats",
            "predictions_zip": "GET /v1/predictions/zip?zip=78701",
            "predictions_hotspots": "GET /v1/predictions/hotspots?state=TX&limit=50",
            "predictions_stats": "GET /v1/predictions/stats",
            "sales_search": "GET /v1/sales/search?address=...&state=NY",
            "sales_property": "GET /v1/sales/property?address=123+Main+St&state=NY",
            "sales_stats": "GET /v1/sales/stats",
            "liens_search": "GET /v1/liens/search?debtor=...&state=NY&lien_type=Tax+Lien",
            "liens_property": "GET /v1/liens/property?address=123+Main+St&state=NY",
            "liens_stats": "GET /v1/liens/stats",
            "dialer_queue": "GET /v1/dialer/queue?trade=roofing&state=TX&limit=25",
            "dialer_log": "POST /v1/dialer/log",
            "dialer_disposition": "POST /v1/dialer/disposition",
            "dialer_callbacks": "GET /v1/dialer/callbacks",
            "dialer_stats": "GET /v1/dialer/stats",
            "dialer_history": "GET /v1/dialer/history?page=1&page_size=25",
            "crm_contacts": "GET /v1/crm/contacts?q=...&page=1",
            "crm_contact_create": "POST /v1/crm/contacts",
            "crm_contact_detail": "GET /v1/crm/contacts/{id}",
            "crm_contact_from_lead": "POST /v1/crm/contacts/from-lead",
            "crm_deals": "GET /v1/crm/deals?stage=new",
            "crm_deal_create": "POST /v1/crm/deals",
            "crm_notes": "POST /v1/crm/notes",
            "crm_pipeline": "GET /v1/crm/pipeline",
            "crm_dashboard": "GET /v1/crm/dashboard",
            "crm_leaderboard": "GET /v1/crm/leaderboard?period=week",
            "crm_commissions": "GET /v1/crm/commissions",
            "crm_commissions_summary": "GET /v1/crm/commissions/summary",
            "crm_teams": "GET /v1/crm/teams",
            "crm_team_create": "POST /v1/crm/teams",
            "crm_team_members": "GET /v1/crm/teams/{id}/members",
            "crm_team_add_member": "POST /v1/crm/teams/{id}/members",
            "crm_team_update_member": "PUT /v1/crm/teams/{id}/members/{member_id}",
            "crm_team_dashboard": "GET /v1/crm/teams/{id}/dashboard",
            "crm_territories": "GET /v1/crm/territories",
            "quotes_list": "GET /v1/quotes",
            "quotes_create": "POST /v1/quotes",
            "quotes_detail": "GET /v1/quotes/{id}",
            "quotes_update": "PUT /v1/quotes/{id}",
            "quotes_send": "POST /v1/quotes/{id}/send",
            "analyst_query": "POST /v1/analyst/query",
            "analyst_suggestions": "GET /v1/analyst/suggestions",
            "analyst_report": "GET /v1/analyst/report?address=123+Main+St&city=Austin&state=TX",
            "trends_zip": "GET /v1/trends/zip?zip=78701&months=12",
            "trends_contractor": "GET /v1/trends/contractor?name=ABC+Builders&months=24",
            "trends_market": "GET /v1/trends/market?state=TX&months=12",
            "trends_entity": "GET /v1/trends/entity?name=Sunrise+Holdings+LLC",
            "trends_stats": "GET /v1/trends/stats",
            "crm_activity_feed": "GET /v1/crm/activity-feed",
            "crm_leads_assign": "POST /v1/crm/leads/assign",
            "crm_leads_assigned": "GET /v1/crm/leads/assigned",
            "webhooks_list": "GET /v1/crm/webhooks",
            "webhooks_create": "POST /v1/crm/webhooks",
            "webhooks_update": "PUT /v1/crm/webhooks/{id}",
            "webhooks_delete": "DELETE /v1/crm/webhooks/{id}",
            "webhooks_test": "POST /v1/crm/webhooks/{id}/test",
            "permits_export_csv": "GET /v1/permits/export?address=...&state=TX",
            "batch_submit": "POST /v1/batch/submit",
            "batch_status": "GET /v1/batch/{job_id}",
            "batch_history": "GET /v1/batch/history",
            "data_freshness": "GET /v1/freshness",
        },
    }

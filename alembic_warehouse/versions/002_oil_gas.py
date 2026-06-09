"""oil & gas layer: operators, wells, well_permits, production_monthly, disposal_wells

Sources (blueprint section 2.6): RRC daf420 (W-1 master+trailer), wellbore EWA
report, PDQ dump, P-5 organizations (orf850), UIC (uif700a). Schema is
state-agnostic (state column everywhere) so NM OCD / OK OCC load into the
same tables later.

production_monthly is partitioned by prod_month (PDQ alone is lease-month
back to 1993, tens of millions of rows). Same no-PK warehouse pattern as
canonical.permits; lease identity is (state, district, lease_number,
well_type, prod_month).

Revision ID: 002_oil_gas
Revises: 001_canonical_core
Create Date: 2026-06-09
"""
from alembic import op

revision = "002_oil_gas"
down_revision = "001_canonical_core"
branch_labels = None
depends_on = None

PROD_YEARS = list(range(1993, 2028))


def upgrade() -> None:
    op.execute("""
        CREATE TABLE canonical.operators (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id       UUID REFERENCES canonical.entities(id),
            state           VARCHAR(2) NOT NULL DEFAULT 'TX',
            operator_number TEXT,           -- RRC P-5 number
            name            TEXT NOT NULL,
            p5_status       TEXT,
            p5_renewal_date DATE,
            organization_kind TEXT,
            lineage         JSONB NOT NULL DEFAULT '{}',
            freshness_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (state, operator_number)
        )
    """)
    op.execute("""
        CREATE INDEX ix_canon_operators_name_trgm ON canonical.operators
        USING gin (name gin_trgm_ops)
    """)

    op.execute("""
        CREATE TABLE canonical.wells (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state             VARCHAR(2) NOT NULL DEFAULT 'TX',
            api14             TEXT,
            api10             TEXT,
            well_name         TEXT,
            well_number       TEXT,
            operator_id       UUID REFERENCES canonical.operators(id),
            operator_name_raw TEXT,
            lease_name        TEXT,
            lease_number      TEXT,
            district          TEXT,
            county            TEXT,
            field_name        TEXT,
            field_number      TEXT,
            well_type         TEXT,          -- oil|gas|injection|disposal|...
            status            TEXT,
            wellbore_profile  TEXT,          -- vertical|horizontal|directional
            spud_date         DATE,
            completion_date   DATE,
            plug_date         DATE,
            total_depth       NUMERIC,
            lat               DOUBLE PRECISION,
            lng               DOUBLE PRECISION,
            geom              geometry(Point, 4326),
            source            TEXT NOT NULL,
            lineage           JSONB NOT NULL DEFAULT '{}',
            freshness_at      TIMESTAMPTZ,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX ux_canon_wells_api14 ON canonical.wells (state, api14)
        WHERE api14 IS NOT NULL
    """)
    op.execute("CREATE INDEX ix_canon_wells_api10 ON canonical.wells (api10)")
    op.execute("CREATE INDEX ix_canon_wells_county ON canonical.wells (state, county)")
    op.execute("CREATE INDEX ix_canon_wells_operator ON canonical.wells (operator_id)")
    op.execute("CREATE INDEX ix_canon_wells_geom ON canonical.wells USING gist (geom)")

    op.execute("""
        CREATE TABLE canonical.well_permits (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state             VARCHAR(2) NOT NULL DEFAULT 'TX',
            permit_number     TEXT NOT NULL,  -- RRC status/tracking number
            api10             TEXT,
            operator_id       UUID REFERENCES canonical.operators(id),
            operator_number   TEXT,
            operator_name_raw TEXT,
            lease_name        TEXT,
            well_number       TEXT,
            district          TEXT,
            county            TEXT,
            field_name        TEXT,
            wellbore_profile  TEXT,
            filing_purpose    TEXT,           -- new drill|recompletion|reentry|amend...
            amended           BOOLEAN DEFAULT FALSE,
            total_depth       NUMERIC,
            current_status    TEXT,           -- submitted|approved|spudded|w1 cancelled...
            status_date       DATE,
            submitted_date    DATE,
            approved_date     DATE,
            spud_date         DATE,
            lat               DOUBLE PRECISION,
            lng               DOUBLE PRECISION,
            geom              geometry(Point, 4326),
            source            TEXT NOT NULL,
            source_file       TEXT,
            lineage           JSONB NOT NULL DEFAULT '{}',
            freshness_at      TIMESTAMPTZ,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (state, permit_number)
        )
    """)
    op.execute("CREATE INDEX ix_canon_wpermits_county ON canonical.well_permits (state, county)")
    op.execute("CREATE INDEX ix_canon_wpermits_operator ON canonical.well_permits (operator_id)")
    op.execute("CREATE INDEX ix_canon_wpermits_approved ON canonical.well_permits (approved_date)")
    op.execute("CREATE INDEX ix_canon_wpermits_api10 ON canonical.well_permits (api10)")
    op.execute("CREATE INDEX ix_canon_wpermits_geom ON canonical.well_permits USING gist (geom)")

    op.execute("""
        CREATE TABLE canonical.production_monthly (
            state           VARCHAR(2) NOT NULL DEFAULT 'TX',
            district        TEXT,
            lease_number    TEXT NOT NULL,
            lease_name      TEXT,
            well_type       TEXT,             -- oil|gas (RRC reports by lease type)
            operator_number TEXT,
            operator_name   TEXT,
            field_number    TEXT,
            field_name      TEXT,
            county          TEXT,
            prod_month      DATE NOT NULL,    -- first of month
            oil_bbl         NUMERIC,
            gas_mcf         NUMERIC,
            condensate_bbl  NUMERIC,
            casinghead_mcf  NUMERIC,
            source          TEXT NOT NULL,
            lineage         JSONB NOT NULL DEFAULT '{}'
        ) PARTITION BY RANGE (prod_month)
    """)
    for y in PROD_YEARS:
        op.execute(f"""
            CREATE TABLE canonical.production_monthly_p{y}
            PARTITION OF canonical.production_monthly
            FOR VALUES FROM ('{y}-01-01') TO ('{y + 1}-01-01')
        """)
    op.execute("""
        CREATE TABLE canonical.production_monthly_pdefault
        PARTITION OF canonical.production_monthly DEFAULT
    """)
    op.execute("""
        CREATE INDEX ix_canon_prod_lease ON canonical.production_monthly
        (state, district, lease_number, prod_month)
    """)
    op.execute("CREATE INDEX ix_canon_prod_month ON canonical.production_monthly (prod_month)")
    op.execute("CREATE INDEX ix_canon_prod_operator ON canonical.production_monthly (operator_number)")

    op.execute("""
        CREATE TABLE canonical.disposal_wells (
            id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state              VARCHAR(2) NOT NULL DEFAULT 'TX',
            uic_number         TEXT,
            permit_number      TEXT,
            api10              TEXT,
            operator_id        UUID REFERENCES canonical.operators(id),
            operator_name_raw  TEXT,
            district           TEXT,
            county             TEXT,
            well_kind          TEXT,          -- disposal|injection|hydrocarbon storage
            status             TEXT,
            formation          TEXT,
            depth_interval     TEXT,
            max_injection_pressure NUMERIC,
            max_injection_bpd  NUMERIC,
            lat                DOUBLE PRECISION,
            lng                DOUBLE PRECISION,
            geom               geometry(Point, 4326),
            source             TEXT NOT NULL,
            lineage            JSONB NOT NULL DEFAULT '{}',
            freshness_at       TIMESTAMPTZ,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX ux_canon_disposal_uic ON canonical.disposal_wells (state, uic_number)
        WHERE uic_number IS NOT NULL
    """)
    op.execute("CREATE INDEX ix_canon_disposal_county ON canonical.disposal_wells (state, county)")
    op.execute("CREATE INDEX ix_canon_disposal_geom ON canonical.disposal_wells USING gist (geom)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS canonical.disposal_wells CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.production_monthly CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.well_permits CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.wells CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.operators CASCADE")

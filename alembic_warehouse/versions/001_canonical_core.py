"""canonical core: jurisdictions, entities, contractors, parcels, permits

Blueprint: docs/permit-intelligence-blueprint-2026-06-09.md sections 2.1/2.2.

Design notes (deliberate deviations from a naive ORM schema):
- canonical.permits is range-partitioned by issued_date. Partitioned tables
  cannot carry a UNIQUE constraint that omits the partition key, and
  issued_date is nullable, so identity lives in canonical.permit_keys
  (source_id, source_record_id) -> permit_id. Loaders upsert the key map
  first, then the data row.
- No FKs on canonical.permits: bulk backfill of 100M+ rows with per-row FK
  checks against parcels/contractors is a multiplier we do not want. The
  small tables keep their FKs.
- No embedding column yet: pgvector is not packaged on T430 PG18. Added in a
  later migration when the extension lands.

Revision ID: 001_canonical_core
Revises:
Create Date: 2026-06-09
"""
from alembic import op

revision = "001_canonical_core"
down_revision = None
branch_labels = None
depends_on = None

PARTITION_YEARS = list(range(1990, 2028))


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS canonical")

    op.execute("""
        CREATE TABLE canonical.jurisdictions (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name        TEXT NOT NULL,
            state       VARCHAR(2) NOT NULL,
            kind        TEXT NOT NULL DEFAULT 'city',  -- city|county|state_agency|special_district
            fips        TEXT,
            geom        geometry(MultiPolygon, 4326),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (state, name, kind)
        )
    """)

    op.execute("""
        CREATE TABLE canonical.entities (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name        TEXT NOT NULL,
            name_norm   TEXT,
            kind        TEXT NOT NULL DEFAULT 'business',  -- business|person|government
            state       VARCHAR(2),
            identifiers JSONB NOT NULL DEFAULT '{}',  -- ein, sos file no, rrc p5, tdlr...
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX ix_canon_entities_name_trgm ON canonical.entities
        USING gin (name_norm gin_trgm_ops)
    """)

    op.execute("""
        CREATE TABLE canonical.contractors (
            id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id      UUID REFERENCES canonical.entities(id),
            name           TEXT NOT NULL,
            license_no     TEXT,
            license_type   TEXT,
            license_state  VARCHAR(2),
            license_status TEXT,
            phone          TEXT,
            email          TEXT,
            risk_score     REAL,
            lineage        JSONB NOT NULL DEFAULT '{}',
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ix_canon_contractors_license ON canonical.contractors (license_state, license_no)")
    op.execute("""
        CREATE INDEX ix_canon_contractors_name_trgm ON canonical.contractors
        USING gin (name gin_trgm_ops)
    """)

    op.execute("""
        CREATE TABLE canonical.parcels (
            id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state              VARCHAR(2) NOT NULL,
            county             TEXT,
            parcel_number      TEXT,
            situs_address      TEXT,
            situs_address_norm TEXT,
            owner_id           UUID REFERENCES canonical.entities(id),
            owner_name_raw     TEXT,
            geom               geometry(Geometry, 4326),
            acreage            NUMERIC,
            land_value         NUMERIC,
            improvement_value  NUMERIC,
            year_built         INTEGER,
            lineage            JSONB NOT NULL DEFAULT '{}',
            freshness_at       TIMESTAMPTZ,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ix_canon_parcels_lookup ON canonical.parcels (state, county, parcel_number)")
    op.execute("CREATE INDEX ix_canon_parcels_geom ON canonical.parcels USING gist (geom)")
    op.execute("""
        CREATE INDEX ix_canon_parcels_situs_trgm ON canonical.parcels
        USING gin (situs_address_norm gin_trgm_ops)
    """)

    op.execute("""
        CREATE TABLE canonical.permit_keys (
            source_id        TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            permit_id        UUID NOT NULL DEFAULT gen_random_uuid(),
            PRIMARY KEY (source_id, source_record_id)
        )
    """)
    op.execute("CREATE INDEX ix_canon_permit_keys_pid ON canonical.permit_keys (permit_id)")

    op.execute("""
        CREATE TABLE canonical.permits (
            permit_id          UUID NOT NULL DEFAULT gen_random_uuid(),
            source_id          TEXT NOT NULL,
            source_record_id   TEXT NOT NULL,
            jurisdiction_id    UUID,
            permit_type        TEXT,
            category           TEXT,
            subcategory        TEXT,
            description_raw    TEXT,
            description_ai     TEXT,
            status             TEXT,
            status_raw         TEXT,
            applied_date       DATE,
            issued_date        DATE,
            finaled_date       DATE,
            declared_value     NUMERIC,
            estimated_value_low  NUMERIC,
            estimated_value_high NUMERIC,
            address_raw        TEXT,
            address_norm       TEXT,
            geom               geometry(Point, 4326),
            geocode_confidence REAL,
            parcel_id          UUID,
            contractor_id      UUID,
            owner_id           UUID,
            complexity_score   REAL,
            confidence_score   REAL,
            freshness_at       TIMESTAMPTZ,
            lineage            JSONB NOT NULL DEFAULT '{}',
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        ) PARTITION BY RANGE (issued_date)
    """)
    for y in PARTITION_YEARS:
        op.execute(f"""
            CREATE TABLE canonical.permits_p{y}
            PARTITION OF canonical.permits
            FOR VALUES FROM ('{y}-01-01') TO ('{y + 1}-01-01')
        """)
    # NULL issued_date, pre-1990, and future years land here
    op.execute("""
        CREATE TABLE canonical.permits_pdefault
        PARTITION OF canonical.permits DEFAULT
    """)

    op.execute("CREATE INDEX ix_canon_permits_source ON canonical.permits (source_id, source_record_id)")
    op.execute("CREATE INDEX ix_canon_permits_pid ON canonical.permits (permit_id)")
    op.execute("CREATE INDEX ix_canon_permits_juris ON canonical.permits (jurisdiction_id)")
    op.execute("CREATE INDEX ix_canon_permits_category ON canonical.permits (category)")
    op.execute("CREATE INDEX ix_canon_permits_issued ON canonical.permits (issued_date)")
    op.execute("CREATE INDEX ix_canon_permits_parcel ON canonical.permits (parcel_id)")
    op.execute("CREATE INDEX ix_canon_permits_contractor ON canonical.permits (contractor_id)")
    op.execute("CREATE INDEX ix_canon_permits_geom ON canonical.permits USING gist (geom)")
    op.execute("""
        CREATE INDEX ix_canon_permits_addr_trgm ON canonical.permits
        USING gin (address_norm gin_trgm_ops)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS canonical.permits CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.permit_keys CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.parcels CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.contractors CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.entities CASCADE")
    op.execute("DROP TABLE IF EXISTS canonical.jurisdictions CASCADE")
    op.execute("DROP SCHEMA IF EXISTS canonical")

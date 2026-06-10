"""Vector tile endpoints for the map UI (PostGIS ST_AsMVT).

Public, no auth: the map teaser is the acquisition funnel; freshness and
search depth stay behind the API key. Tiles carry minimal properties to keep
payloads small. canonical.* lives only on the primary, so get_db.

Layers:
  well-permits  W-1 drilling permits with coords (recent first per tile)
  wells         wellbores (1M, zoom-gated)
  permits       building permits (12.5M, zoom-gated harder)
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

router = APIRouter(prefix="/map", tags=["Map Tiles"])

TILE_SQL = {
    "well-permits": """
        WITH bounds AS (SELECT ST_TileEnvelope(:z, :x, :y) AS b),
        rows AS (
            SELECT permit_number, county, operator_name_raw AS operator,
                   wellbore_profile AS profile, filing_purpose AS purpose,
                   current_status AS status, approved_date::text AS approved,
                   total_depth::float AS depth, geom
            FROM canonical.well_permits, bounds
            WHERE geom IS NOT NULL
              AND geom && ST_Transform((SELECT b FROM bounds), 4326)
              {date_filter}
            ORDER BY approved_date DESC NULLS LAST
            LIMIT :lim
        )
        SELECT ST_AsMVT(t, 'well-permits', 4096, 'mvt') FROM (
            SELECT permit_number, county, operator, profile, purpose, status,
                   approved, depth,
                   ST_AsMVTGeom(ST_Transform(geom, 3857),
                                (SELECT b FROM bounds), 4096, 64, true) AS mvt
            FROM rows
        ) t WHERE t.mvt IS NOT NULL
    """,
    "wells": """
        WITH bounds AS (SELECT ST_TileEnvelope(:z, :x, :y) AS b),
        rows AS (
            SELECT api10, well_name, county, operator_name_raw AS operator,
                   well_type, status, completion_date::text AS completed, geom
            FROM canonical.wells, bounds
            WHERE geom IS NOT NULL
              AND geom && ST_Transform((SELECT b FROM bounds), 4326)
              {date_filter}
            ORDER BY completion_date DESC NULLS LAST
            LIMIT :lim
        )
        SELECT ST_AsMVT(t, 'wells', 4096, 'mvt') FROM (
            SELECT api10, well_name, county, operator, well_type, status,
                   completed,
                   ST_AsMVTGeom(ST_Transform(geom, 3857),
                                (SELECT b FROM bounds), 4096, 64, true) AS mvt
            FROM rows
        ) t WHERE t.mvt IS NOT NULL
    """,
    "permits": """
        WITH bounds AS (SELECT ST_TileEnvelope(:z, :x, :y) AS b),
        rows AS (
            SELECT source_record_id AS permit_number, permit_type,
                   status, issued_date::text AS issued,
                   declared_value::float AS value, address_raw AS address,
                   geom
            FROM canonical.permits, bounds
            WHERE geom IS NOT NULL
              AND geom && ST_Transform((SELECT b FROM bounds), 4326)
              {date_filter}
            ORDER BY issued_date DESC NULLS LAST
            LIMIT :lim
        )
        SELECT ST_AsMVT(t, 'permits', 4096, 'mvt') FROM (
            SELECT permit_number, permit_type, status, issued, value, address,
                   ST_AsMVTGeom(ST_Transform(geom, 3857),
                                (SELECT b FROM bounds), 4096, 64, true) AS mvt
            FROM rows
        ) t WHERE t.mvt IS NOT NULL
    """,
}

# zoom gates + per-tile feature budgets: low zooms only get the sparse
# layers; the 12.5M-row permits layer needs z>=8 to stay cheap
LAYER_RULES = {
    "well-permits": {"min_zoom": 4, "limit": 8000, "date_col": "approved_date"},
    "wells": {"min_zoom": 6, "limit": 10000, "date_col": "completion_date"},
    "permits": {"min_zoom": 8, "limit": 8000, "date_col": "issued_date"},
}


@router.get("/tiles/{layer}/{z}/{x}/{y}.pbf")
async def vector_tile(
    layer: str,
    z: int,
    x: int,
    y: int,
    days: int = Query(0, ge=0, le=3650, description="Only features from the last N days (0 = all)"),
    db: AsyncSession = Depends(get_db),
):
    rules = LAYER_RULES.get(layer)
    if not rules:
        raise HTTPException(status_code=404, detail=f"Unknown layer: {layer}. Layers: {list(LAYER_RULES)}")
    if not (0 <= z <= 22) or not (0 <= x < 2 ** z) or not (0 <= y < 2 ** z):
        raise HTTPException(status_code=400, detail="Invalid tile coordinates.")
    if z < rules["min_zoom"]:
        return Response(content=b"", media_type="application/x-protobuf",
                        headers={"Cache-Control": "public, max-age=86400"})

    # days is inlined as a validated int literal, never user text: a bind
    # param inside an OR would defeat partition pruning on issued_date and
    # turn metro permit tiles into multi-second scans
    if layer == "permits" and days == 0:
        days = 3650  # all-time on 12.5M building permits = 7s/450KB tiles
    date_filter = (
        f"AND {rules['date_col']} >= current_date - {int(days)}" if days else ""
    )
    result = await db.execute(
        text(TILE_SQL[layer].format(date_filter=date_filter)),
        {"z": z, "x": x, "y": y, "lim": rules["limit"]},
    )
    mvt = result.scalar()
    return Response(
        content=bytes(mvt) if mvt else b"",
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=3600"},
    )

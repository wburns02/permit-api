#!/usr/bin/env python3
"""Load the TexNet earthquake catalog (Bureau of Economic Geology, UT Austin)
into texnet.events.

Source: ArcGIS REST services behind catalog.texnet.beg.utexas.edu:
  https://maps.texnet.beg.utexas.edu/arcgis/rest/services/catalog/catalog_all/MapServer
    layer 0 = Earthquake (reviewed catalog, 2017-present)
    layer 2 = Earthquakes (Preliminary)
(The TexNetCatalog/FeatureServer URL in the SPA bundle 500s; the MapServer
query endpoint works and supports pagination, maxRecordCount=2000.)

Paged with resultOffset/orderByFields=EarthquakeId. Full reload each run.
Reviewed loads first; preliminary inserted with ON CONFLICT DO NOTHING so a
promoted event keeps its reviewed row. Depth is km; Event_Date is epoch ms
(UTC). Events with null lat/lon are kept without geom (none observed).

Usage: python3 load_texnet.py
"""
import io
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
BASE = ("https://maps.texnet.beg.utexas.edu/arcgis/rest/services/"
        "catalog/catalog_all/MapServer")
LAYERS = [(0, "reviewed"), (2, "preliminary")]
PAGE = 2000

DDL = """
CREATE SCHEMA IF NOT EXISTS texnet;
DROP TABLE IF EXISTS texnet.events CASCADE;
CREATE TABLE texnet.events (
    event_id text PRIMARY KEY,
    origin_time timestamptz,
    magnitude double precision,
    mag_type text,
    depth_km double precision,
    lat double precision,
    lon double precision,
    county text,
    region text,
    event_type text,
    evaluation_status text,
    catalog_layer text,
    geom geometry(Point, 4326)
);
"""


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def epoch_ms(v):
    if v is None:
        return None
    return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).isoformat()


def esc(v):
    if v is None or v == "":
        return "\\N"
    return (str(v).replace("\x00", "").replace("\\", "\\\\")
            .replace("\t", " ").replace("\n", " ").replace("\r", " "))


def fetch_layer(layer_id):
    sess = requests.Session()
    offset = 0
    while True:
        for attempt in range(4):
            try:
                r = sess.get(f"{BASE}/{layer_id}/query", params={
                    "where": "1=1",
                    "outFields": "EventId,Magnitude,MagType,Latitude,Longitude,"
                                 "Depth,Event_Date,CountyName,RegionName,"
                                 "EventType,EvaluationStatus",
                    "orderByFields": "EarthquakeId",
                    "resultOffset": offset,
                    "resultRecordCount": PAGE,
                    "returnGeometry": "false",
                    "f": "json",
                }, timeout=120)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 3:
                    raise
                log(f"  retry offset={offset}: {e}")
                time.sleep(5 * (attempt + 1))
        feats = data.get("features", [])
        if not feats:
            return
        yield from (f["attributes"] for f in feats)
        offset += len(feats)
        if not data.get("exceededTransferLimit") and len(feats) < PAGE:
            return


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    for layer_id, label in LAYERS:
        cur.execute("""
            CREATE TEMP TABLE tx_stage (
                event_id text, origin_time timestamptz,
                magnitude float8, mag_type text, depth_km float8,
                lat float8, lon float8, county text, region text,
                event_type text, evaluation_status text
            ) ON COMMIT DROP
        """)
        buf, n = [], 0
        for a in fetch_layer(layer_id):
            if not a.get("EventId"):
                continue
            vals = [
                a["EventId"], epoch_ms(a.get("Event_Date")),
                a.get("Magnitude"), a.get("MagType"), a.get("Depth"),
                a.get("Latitude"), a.get("Longitude"),
                a.get("CountyName"), a.get("RegionName"),
                a.get("EventType"), a.get("EvaluationStatus"),
            ]
            buf.append("\t".join(esc(v) for v in vals) + "\n")
            n += 1
            if n % 10_000 == 0:
                cur.copy_expert("COPY tx_stage FROM STDIN", io.StringIO("".join(buf)))
                buf.clear()
                log(f"  {label}: {n:,} fetched")
        if buf:
            cur.copy_expert("COPY tx_stage FROM STDIN", io.StringIO("".join(buf)))
        cur.execute("""
            INSERT INTO texnet.events
                (event_id, origin_time, magnitude, mag_type, depth_km,
                 lat, lon, county, region, event_type, evaluation_status,
                 catalog_layer, geom)
            SELECT DISTINCT ON (event_id)
                   event_id, origin_time, magnitude, mag_type, depth_km,
                   lat, lon, county, region, event_type, evaluation_status,
                   %s,
                   CASE WHEN lat IS NOT NULL AND lon IS NOT NULL
                        THEN ST_SetSRID(ST_MakePoint(lon, lat), 4326) END
            FROM tx_stage
            ORDER BY event_id
            ON CONFLICT (event_id) DO NOTHING
        """, (label,))
        log(f"{label}: fetched {n:,}, inserted {cur.rowcount:,}")
        conn.commit()

    cur.execute("CREATE INDEX ON texnet.events USING gist (geom)")
    cur.execute("CREATE INDEX ON texnet.events (origin_time)")
    cur.execute("CREATE INDEX ON texnet.events (magnitude)")
    cur.execute("ANALYZE texnet.events")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    sys.exit(main())

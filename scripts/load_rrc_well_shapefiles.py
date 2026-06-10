#!/usr/bin/env python3
"""Backfill canonical.wells coordinates from RRC county well shapefiles.

Input: /mnt/win11/Fedora/raw-public-data/rrc/well_shapefiles/well<ccc>.zip
(MFT link d551fb20, twice-weekly). Each zip holds three layers; we read
well<ccc>s = surface points (API 8-digit, LAT83/LONG83 NAD83 ~ WGS84 at this
precision, RELIAB = location reliability code).

Stages (api8, lat, lng, reliab) for all counties, then one UPDATE join on
api10. Dedup: keep highest RELIAB per API (bigger = better per RRC docs).
"""
import glob
import io
import time
import zipfile

import psycopg2
import shapefile

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
DIR = "/mnt/win11/Fedora/raw-public-data/rrc/well_shapefiles"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TEMP TABLE shp_stage (
            api8 TEXT, lat DOUBLE PRECISION, lng DOUBLE PRECISION, reliab TEXT
        )
    """)

    total = 0
    zips = sorted(glob.glob(f"{DIR}/well*.zip"))
    log(f"{len(zips)} county zips")
    for zpath in zips:
        try:
            z = zipfile.ZipFile(zpath)
            bases = sorted({n[:-4] for n in z.namelist()
                            if n.endswith("s.shp")})
            buf = []
            for base in bases:
                try:
                    r = shapefile.Reader(
                        shp=io.BytesIO(z.read(base + ".shp")),
                        dbf=io.BytesIO(z.read(base + ".dbf")),
                        shx=io.BytesIO(z.read(base + ".shx")),
                    )
                except Exception as e:
                    log(f"{base}: unreadable ({e})")
                    continue
                fields = [f[0] for f in r.fields[1:]]
                try:
                    i_api = fields.index("API")
                    i_lat = fields.index("LAT83")
                    i_lng = fields.index("LONG83")
                    i_rel = fields.index("RELIAB")
                except ValueError:
                    log(f"{base}: missing expected fields {fields}")
                    continue
                for rec in r.iterRecords():
                    api = (rec[i_api] or "").strip()
                    lat, lng = rec[i_lat], rec[i_lng]
                    if not api or not lat or not lng:
                        continue
                    if not (25 < lat < 37 and -107 < lng < -93):
                        continue
                    buf.append(f"{api}\t{lat}\t{lng}\t{rec[i_rel] or ''}\n")
            if buf:
                cur.copy_expert("COPY shp_stage FROM STDIN",
                                io.StringIO("".join(buf)))
                total += len(buf)
        except zipfile.BadZipFile:
            log(f"{zpath}: bad zip, skipped")
    conn.commit()
    log(f"staged {total:,} surface points; updating wells")

    cur.execute("""
        WITH best AS (
            SELECT DISTINCT ON (api8) api8, lat, lng
            FROM shp_stage
            ORDER BY api8, reliab DESC NULLS LAST
        )
        UPDATE canonical.wells w
        SET lat = b.lat, lng = b.lng,
            geom = ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326)
        FROM best b
        WHERE w.state = 'TX' AND w.api10 = '42' || b.api8
    """)
    log(f"updated geom on {cur.rowcount:,} wells")
    conn.commit()

    cur.execute("""
        SELECT count(*) FILTER (WHERE geom IS NOT NULL), count(*)
        FROM canonical.wells WHERE state = 'TX'
    """)
    got, allw = cur.fetchone()
    log(f"coverage: {got:,}/{allw:,} TX wells with geom")
    conn.close()


if __name__ == "__main__":
    main()

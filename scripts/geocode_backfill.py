"""Batch geocode permits missing lat/lng via Census Geocoder (free, 10K/batch)."""

import asyncio
import csv
import io
import logging
import httpx
from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings
from app.models.permit import Permit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_SIZE = 1000  # Census API max is 10K but smaller is more reliable


async def fetch_missing_permits(db: AsyncSession, limit: int = 10000) -> list:
    """Get permits missing lat/lng that have a parseable address."""
    result = await db.execute(
        select(Permit.id, Permit.address, Permit.city, Permit.state, Permit.zip)
        .where(and_(
            Permit.lat.is_(None),
            Permit.address.isnot(None),
            Permit.city.isnot(None),
            Permit.state.isnot(None),
        ))
        .limit(limit)
    )
    return result.all()


def build_csv_batch(permits: list) -> str:
    """Build CSV for Census Geocoder batch API."""
    output = io.StringIO()
    writer = csv.writer(output)
    for p in permits:
        # Format: unique_id, street, city, state, zip
        writer.writerow([str(p.id), p.address, p.city, p.state, p.zip or ""])
    return output.getvalue()


async def geocode_batch(csv_data: str) -> dict:
    """Submit batch to Census Geocoder, return {id: (lat, lng)} map."""
    results = {}
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            CENSUS_BATCH_URL,
            data={"benchmark": "Public_AR_Current", "returntype": "locations"},
            files={"addressFile": ("addresses.csv", csv_data, "text/csv")},
        )
        resp.raise_for_status()

    reader = csv.reader(io.StringIO(resp.text))
    for row in reader:
        if len(row) >= 6 and row[2] == "Match":
            uid = row[0].strip('"')
            coords = row[5].strip('"').split(",")
            if len(coords) == 2:
                try:
                    lng, lat = float(coords[0]), float(coords[1])
                    results[uid] = (lat, lng)
                except ValueError:
                    pass
    return results


async def update_permits(db: AsyncSession, geocoded: dict):
    """Update permits with geocoded lat/lng."""
    for permit_id, (lat, lng) in geocoded.items():
        await db.execute(
            update(Permit)
            .where(Permit.id == permit_id)
            .values(lat=lat, lng=lng)
        )
    await db.commit()


async def main():
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as db:
        permits = await fetch_missing_permits(db)
        logger.info("Found %d permits missing coordinates", len(permits))

        total_geocoded = 0
        for i in range(0, len(permits), BATCH_SIZE):
            batch = permits[i:i + BATCH_SIZE]
            csv_data = build_csv_batch(batch)

            try:
                geocoded = await geocode_batch(csv_data)
                if geocoded:
                    await update_permits(db, geocoded)
                    total_geocoded += len(geocoded)
                logger.info("Batch %d-%d: %d/%d geocoded", i, i + len(batch), len(geocoded), len(batch))
            except Exception as e:
                logger.error("Batch %d failed: %s", i, e)

            await asyncio.sleep(1)  # Rate limit courtesy

        logger.info("Done. Total geocoded: %d/%d", total_geocoded, len(permits))

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

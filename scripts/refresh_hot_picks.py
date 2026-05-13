"""Refresh the Hot Picks leaderboard for one (or all) registered cities.

Pulls every parcel from the city's Esri FeatureServer, scores each against
the CA state-law menu, upserts the results into `parcel_hot_picks`, and
deletes any APN not seen this run.

Usage:
    cd /home/will/permit-api-live   # or /home/will/permit-api locally
    PYTHONPATH=. python3 scripts/refresh_hot_picks.py --city rialto
    PYTHONPATH=. python3 scripts/refresh_hot_picks.py --all

Same invocation pattern as scripts/seed_parcel_screen.py.
"""

import argparse
import asyncio
import logging
import os
import sys

from sqlalchemy import select

from app.database import async_session_maker
from app.models.parcel_screen import ParcelJurisdiction
from app.services.parcel_hot_picks import refresh_city

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("refresh_hot_picks")


async def _refresh_one(state: str, city_slug: str) -> dict:
    async with async_session_maker() as session:
        result = await session.execute(
            select(ParcelJurisdiction).where(
                ParcelJurisdiction.state == state,
                ParcelJurisdiction.city_slug == city_slug,
            )
        )
        jurisdiction = result.scalar_one_or_none()
        if not jurisdiction:
            log.error(f"jurisdiction not registered: {state}/{city_slug}")
            return {"error": f"not registered: {state}/{city_slug}"}

        log.info(f"refreshing {state}/{city_slug} ({jurisdiction.display_name})")
        stats = await refresh_city(session, jurisdiction)
        log.info(f"done {state}/{city_slug}: {stats}")
        return {"state": state, "city_slug": city_slug, **stats}


async def main(args) -> int:
    if args.db_host:
        os.environ["DATABASE_URL"] = f"postgresql+asyncpg://will@{args.db_host}:5432/permits"
        from importlib import reload
        from app import database
        reload(database)

    if args.all:
        async with async_session_maker() as session:
            result = await session.execute(
                select(ParcelJurisdiction).order_by(
                    ParcelJurisdiction.state, ParcelJurisdiction.city_slug
                )
            )
            juris_list = result.scalars().all()
        if not juris_list:
            log.error("no jurisdictions registered")
            return 1
        for j in juris_list:
            await _refresh_one(j.state, j.city_slug)
        return 0

    if not args.city:
        log.error("must pass --city <slug> or --all")
        return 2

    state = args.state.upper()
    city_slug = args.city.lower()
    out = await _refresh_one(state, city_slug)
    if out.get("error"):
        return 1
    print("---")
    for k, v in out.items():
        print(f"{k}: {v}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="CA", help="2-letter state code (default CA)")
    ap.add_argument("--city", default=None, help="city_slug (e.g. rialto)")
    ap.add_argument("--all", action="store_true", help="refresh every registered jurisdiction")
    ap.add_argument("--db-host", default=None)
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args)))

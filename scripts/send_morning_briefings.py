#!/usr/bin/env python3
"""
Morning Briefing Sender — sends daily AI-powered sales briefing emails.

Connects to the T430 production database, loops through all active users
with paid plans, and sends each one a personalized morning briefing email.

Usage:
    python3 send_morning_briefings.py --db-host 100.122.216.15
    python3 send_morning_briefings.py --db-host 100.122.216.15 --email will@example.com  # test single user

Cron (weekdays 7 AM):
    0 7 * * 1-5 cd /home/will/permit-api && source backend_venv/bin/activate && python3 scripts/send_morning_briefings.py --db-host 100.122.216.15 >> /tmp/morning_briefings.log 2>&1
"""

import argparse
import asyncio
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("morning_briefings")


async def main():
    parser = argparse.ArgumentParser(description="Send morning briefing emails")
    parser.add_argument("--db-host", default="100.122.216.15", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="permit_api", help="Database name")
    parser.add_argument("--db-user", default="will", help="Database user")
    parser.add_argument("--db-pass", default="", help="Database password")
    parser.add_argument("--email", default=None, help="Send to specific user email only (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without sending")
    args = parser.parse_args()

    # Build connection URL
    password_part = f":{args.db_pass}" if args.db_pass else ""
    db_url = f"postgresql+asyncpg://{args.db_user}{password_part}@{args.db_host}:{args.db_port}/{args.db_name}"

    # Override settings
    os.environ["DATABASE_URL"] = db_url

    # Now import app modules (after setting env)
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from sqlalchemy import select, text

    engine = create_async_engine(db_url, pool_size=3, pool_pre_ping=True)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Import after engine is ready
    from app.models.api_key import ApiUser, PlanTier, resolve_plan
    from app.services.morning_briefing import send_morning_briefing, _gather_user_stats, _generate_ai_insight

    sent = 0
    failed = 0
    skipped = 0

    async with session_maker() as db:
        # Get all active paid users
        if args.email:
            result = await db.execute(
                select(ApiUser).where(ApiUser.email == args.email, ApiUser.is_active == True)
            )
        else:
            result = await db.execute(
                select(ApiUser).where(
                    ApiUser.is_active == True,
                    ApiUser.plan.notin_(["free", None]),
                )
            )
        users = result.scalars().all()
        logger.info("Found %d active paid users", len(users))

        for user in users:
            plan = resolve_plan(user.plan)
            if plan == PlanTier.FREE:
                skipped += 1
                continue

            if not user.email:
                logger.warning("User %s has no email, skipping", user.id)
                skipped += 1
                continue

            logger.info("Processing briefing for %s (%s)", user.email, plan.value)

            if args.dry_run:
                stats = await _gather_user_stats(user, db)
                insight = _generate_ai_insight(stats)
                logger.info("  Stats: %s", stats)
                logger.info("  AI Insight: %s", insight)
                sent += 1
                continue

            try:
                success = await send_morning_briefing(user.id, db)
                if success:
                    sent += 1
                    logger.info("  Briefing sent to %s", user.email)
                else:
                    failed += 1
                    logger.warning("  Failed to send to %s", user.email)
            except Exception as e:
                failed += 1
                logger.error("  Error for %s: %s", user.email, e)

    await engine.dispose()

    prefix = "[DRY RUN] " if args.dry_run else ""
    logger.info("%sDone: %d sent, %d failed, %d skipped", prefix, sent, failed, skipped)


if __name__ == "__main__":
    asyncio.run(main())

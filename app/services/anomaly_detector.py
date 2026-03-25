"""Anomaly Detection Engine — surface unusual patterns across data layers.

Detects permit velocity spikes, storm-permit correlations, price anomalies,
violation surges, and new entity clusters. Feeds the /v1/trends/anomalies
public endpoint — great marketing + genuinely useful intelligence.
"""

import logging
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.fast_counts import safe_query

logger = logging.getLogger(__name__)


async def detect_anomalies(
    db: AsyncSession,
    state: str | None = None,
    zip_code: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Find unusual patterns across recent data. Returns anomalies sorted by severity."""
    anomalies = []

    # ── 1. Permit velocity spikes ────────────────────────────────────────
    # ZIPs where hot_leads permit count this month is 200%+ of last month
    try:
        state_filter = "AND state = :state" if state else ""
        params: dict = {}
        if state:
            params["state"] = state.upper()

        velocity_sql = f"""
            WITH this_month AS (
                SELECT zip, state, COUNT(*) AS cnt
                FROM hot_leads
                WHERE issue_date >= CURRENT_DATE - INTERVAL '30 days'
                {state_filter}
                GROUP BY zip, state
                HAVING COUNT(*) >= 10
            ),
            last_month AS (
                SELECT zip, state, COUNT(*) AS cnt
                FROM hot_leads
                WHERE issue_date >= CURRENT_DATE - INTERVAL '60 days'
                  AND issue_date < CURRENT_DATE - INTERVAL '30 days'
                {state_filter}
                GROUP BY zip, state
            )
            SELECT t.zip, t.state, t.cnt AS current_count, COALESCE(l.cnt, 1) AS prior_count,
                   ROUND((t.cnt::numeric / GREATEST(COALESCE(l.cnt, 1), 1)) * 100 - 100) AS change_pct
            FROM this_month t
            LEFT JOIN last_month l ON t.zip = l.zip AND t.state = l.state
            WHERE t.cnt > COALESCE(l.cnt, 1) * 2
            ORDER BY change_pct DESC
            LIMIT 10
        """
        rows = await safe_query(db, text(velocity_sql).bindparams(**params), timeout_ms=10000)
        if rows:
            for r in rows:
                city_sql = text(
                    "SELECT city FROM hot_leads WHERE zip = :zip AND state = :state LIMIT 1"
                ).bindparams(zip=r.zip, state=r.state)
                city_rows = await safe_query(db, city_sql, timeout_ms=3000)
                city_name = city_rows[0].city if city_rows else ""

                pct = int(r.change_pct) if r.change_pct else 0
                severity = "high" if pct >= 300 else "medium" if pct >= 200 else "low"
                anomalies.append({
                    "type": "permit_velocity_spike",
                    "description": f"{r.zip} {city_name} {r.state} — Permit velocity up {pct}% "
                                   f"({r.current_count} permits vs {r.prior_count} prior month)",
                    "zip": r.zip,
                    "state": r.state,
                    "city": city_name,
                    "metric": "permits_per_month",
                    "current_value": int(r.current_count),
                    "prior_value": int(r.prior_count),
                    "change_pct": pct,
                    "severity": severity,
                })
    except Exception as e:
        logger.warning("Anomaly: permit velocity check failed: %s", e)

    # ── 2. Storm → permit correlation ────────────────────────────────────
    # States/areas with NOAA events in last 90 days that overlap with permit spikes
    try:
        storm_params: dict = {}
        storm_state_filter = "AND n.state = :storm_state" if state else ""
        if state:
            storm_params["storm_state"] = state.upper()

        storm_sql = f"""
            WITH recent_storms AS (
                SELECT state, county, event_type, begin_date,
                       damage_property
                FROM noaa_storm_events
                WHERE begin_date >= CURRENT_DATE - INTERVAL '90 days'
                {storm_state_filter}
                ORDER BY begin_date DESC
                LIMIT 50
            ),
            storm_areas AS (
                SELECT DISTINCT state, event_type, begin_date
                FROM recent_storms
            ),
            permit_spikes AS (
                SELECT h.state, COUNT(*) AS permit_count
                FROM hot_leads h
                INNER JOIN storm_areas s ON h.state = s.state
                WHERE h.issue_date >= s.begin_date
                  AND h.issue_date <= s.begin_date + INTERVAL '30 days'
                GROUP BY h.state
                HAVING COUNT(*) >= 20
            )
            SELECT s.state, s.event_type, s.begin_date, p.permit_count
            FROM storm_areas s
            INNER JOIN permit_spikes p ON s.state = p.state
            ORDER BY p.permit_count DESC
            LIMIT 5
        """
        storm_rows = await safe_query(db, text(storm_sql).bindparams(**storm_params), timeout_ms=10000)
        if storm_rows:
            for r in storm_rows:
                anomalies.append({
                    "type": "storm_permit_correlation",
                    "description": f"{r.state} — {r.permit_count} permits filed after "
                                   f"{r.event_type} on {r.begin_date}",
                    "zip": None,
                    "state": r.state,
                    "city": None,
                    "metric": "storm_correlated_permits",
                    "current_value": int(r.permit_count),
                    "prior_value": None,
                    "change_pct": None,
                    "severity": "high" if r.permit_count >= 100 else "medium",
                })
    except Exception as e:
        logger.warning("Anomaly: storm correlation check failed: %s", e)

    # ── 3. Price anomalies ───────────────────────────────────────────────
    # ZIPs where median sale price changed 20%+ YoY
    try:
        price_params: dict = {}
        price_state_filter = "AND state = :price_state" if state else ""
        if state:
            price_params["price_state"] = state.upper()

        price_sql = f"""
            WITH recent AS (
                SELECT zip, state, AVG(median_sale_price) AS avg_price
                FROM property_valuations
                WHERE period_end >= CURRENT_DATE - INTERVAL '3 months'
                  AND median_sale_price IS NOT NULL AND median_sale_price > 0
                {price_state_filter}
                GROUP BY zip, state
            ),
            year_ago AS (
                SELECT zip, state, AVG(median_sale_price) AS avg_price
                FROM property_valuations
                WHERE period_end >= CURRENT_DATE - INTERVAL '15 months'
                  AND period_end < CURRENT_DATE - INTERVAL '9 months'
                  AND median_sale_price IS NOT NULL AND median_sale_price > 0
                {price_state_filter}
                GROUP BY zip, state
            )
            SELECT r.zip, r.state, r.avg_price AS current_price, y.avg_price AS prior_price,
                   ROUND(((r.avg_price - y.avg_price) / GREATEST(y.avg_price, 1)) * 100) AS change_pct
            FROM recent r
            INNER JOIN year_ago y ON r.zip = y.zip AND r.state = y.state
            WHERE ABS(r.avg_price - y.avg_price) / GREATEST(y.avg_price, 1) >= 0.20
            ORDER BY ABS(change_pct) DESC
            LIMIT 8
        """
        price_rows = await safe_query(db, text(price_sql).bindparams(**price_params), timeout_ms=10000)
        if price_rows:
            for r in price_rows:
                pct = int(r.change_pct) if r.change_pct else 0
                direction = "up" if pct > 0 else "down"
                severity = "high" if abs(pct) >= 40 else "medium" if abs(pct) >= 25 else "low"
                anomalies.append({
                    "type": "price_anomaly",
                    "description": f"{r.zip} {r.state} — Median sale price {direction} {abs(pct)}% YoY "
                                   f"(${int(r.current_price):,} vs ${int(r.prior_price):,})",
                    "zip": r.zip,
                    "state": r.state,
                    "city": None,
                    "metric": "median_sale_price_yoy",
                    "current_value": int(r.current_price) if r.current_price else None,
                    "prior_value": int(r.prior_price) if r.prior_price else None,
                    "change_pct": pct,
                    "severity": severity,
                })
    except Exception as e:
        logger.warning("Anomaly: price check failed: %s", e)

    # ── 4. Violation surges ──────────────────────────────────────────────
    # Cities with violation count spikes (200%+ vs prior month)
    try:
        viol_params: dict = {}
        viol_state_filter = "AND state = :viol_state" if state else ""
        if state:
            viol_params["viol_state"] = state.upper()

        viol_sql = f"""
            WITH this_month AS (
                SELECT city, state, COUNT(*) AS cnt
                FROM code_violations
                WHERE violation_date >= CURRENT_DATE - INTERVAL '30 days'
                {viol_state_filter}
                GROUP BY city, state
                HAVING COUNT(*) >= 5
            ),
            last_month AS (
                SELECT city, state, COUNT(*) AS cnt
                FROM code_violations
                WHERE violation_date >= CURRENT_DATE - INTERVAL '60 days'
                  AND violation_date < CURRENT_DATE - INTERVAL '30 days'
                {viol_state_filter}
                GROUP BY city, state
            )
            SELECT t.city, t.state, t.cnt AS current_count,
                   COALESCE(l.cnt, 1) AS prior_count,
                   ROUND((t.cnt::numeric / GREATEST(COALESCE(l.cnt, 1), 1)) * 100 - 100) AS change_pct
            FROM this_month t
            LEFT JOIN last_month l ON t.city = l.city AND t.state = l.state
            WHERE t.cnt > COALESCE(l.cnt, 1) * 2
            ORDER BY change_pct DESC
            LIMIT 5
        """
        viol_rows = await safe_query(db, text(viol_sql).bindparams(**viol_params), timeout_ms=10000)
        if viol_rows:
            for r in viol_rows:
                pct = int(r.change_pct) if r.change_pct else 0
                severity = "high" if pct >= 300 else "medium"
                anomalies.append({
                    "type": "violation_surge",
                    "description": f"{r.city}, {r.state} — Violation count up {pct}% "
                                   f"({r.current_count} vs {r.prior_count} prior month)",
                    "zip": None,
                    "state": r.state,
                    "city": r.city,
                    "metric": "violations_per_month",
                    "current_value": int(r.current_count),
                    "prior_value": int(r.prior_count),
                    "change_pct": pct,
                    "severity": severity,
                })
    except Exception as e:
        logger.warning("Anomaly: violation surge check failed: %s", e)

    # ── 5. New entity clusters ───────────────────────────────────────────
    # States/areas with unusually high new LLC formation in last 30 days
    try:
        ent_params: dict = {}
        ent_state_filter = "AND state = :ent_state" if state else ""
        if state:
            ent_params["ent_state"] = state.upper()

        entity_sql = f"""
            WITH this_month AS (
                SELECT state, COUNT(*) AS cnt
                FROM business_entities
                WHERE formation_date >= CURRENT_DATE - INTERVAL '30 days'
                {ent_state_filter}
                GROUP BY state
                HAVING COUNT(*) >= 10
            ),
            last_month AS (
                SELECT state, COUNT(*) AS cnt
                FROM business_entities
                WHERE formation_date >= CURRENT_DATE - INTERVAL '60 days'
                  AND formation_date < CURRENT_DATE - INTERVAL '30 days'
                {ent_state_filter}
                GROUP BY state
            )
            SELECT t.state, t.cnt AS current_count,
                   COALESCE(l.cnt, 1) AS prior_count,
                   ROUND((t.cnt::numeric / GREATEST(COALESCE(l.cnt, 1), 1)) * 100 - 100) AS change_pct
            FROM this_month t
            LEFT JOIN last_month l ON t.state = l.state
            WHERE t.cnt > COALESCE(l.cnt, 1) * 2
            ORDER BY t.cnt DESC
            LIMIT 5
        """
        ent_rows = await safe_query(db, text(entity_sql).bindparams(**ent_params), timeout_ms=10000)
        if ent_rows:
            for r in ent_rows:
                pct = int(r.change_pct) if r.change_pct else 0
                severity = "high" if pct >= 300 else "medium" if pct >= 150 else "low"
                anomalies.append({
                    "type": "entity_cluster",
                    "description": f"{r.state} — {r.current_count} new LLCs formed in 30 days "
                                   f"({pct}% above prior month)",
                    "zip": None,
                    "state": r.state,
                    "city": None,
                    "metric": "new_entities_per_month",
                    "current_value": int(r.current_count),
                    "prior_value": int(r.prior_count),
                    "change_pct": pct,
                    "severity": severity,
                })
    except Exception as e:
        logger.warning("Anomaly: entity cluster check failed: %s", e)

    # Sort by severity (high first), then by change_pct descending
    severity_order = {"high": 0, "medium": 1, "low": 2}
    anomalies.sort(key=lambda a: (severity_order.get(a["severity"], 2), -(a.get("change_pct") or 0)))

    return anomalies[:limit]

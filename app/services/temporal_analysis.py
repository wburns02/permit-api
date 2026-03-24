"""Temporal Analysis Engine — time-series trends across all data layers.

Pre-computes and serves permit velocity, price trends, contractor trajectories,
market momentum, and entity timelines. Palantir-grade temporal intelligence.
"""

import logging
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.fast_counts import safe_query

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Trend computation helpers
# ---------------------------------------------------------------------------

def _compute_mom_change(current: float | None, previous: float | None) -> float | None:
    """Month-over-month percentage change."""
    if current is None or previous is None or previous == 0:
        return None
    return round(((current - previous) / abs(previous)) * 100, 2)


def _compute_yoy_change(current: float | None, year_ago: float | None) -> float | None:
    """Year-over-year percentage change."""
    if current is None or year_ago is None or year_ago == 0:
        return None
    return round(((current - year_ago) / abs(year_ago)) * 100, 2)


def _compute_trend_direction(values: list[float]) -> str:
    """Determine trend direction via simple linear regression slope."""
    n = len(values)
    if n < 3:
        return "insufficient_data"

    # Simple linear regression: y = mx + b
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return "stable"

    slope = numerator / denominator
    # Normalize slope relative to mean
    if y_mean == 0:
        return "stable"

    normalized = slope / abs(y_mean)
    if normalized > 0.05:
        return "accelerating"
    elif normalized < -0.05:
        return "decelerating"
    return "stable"


def _forecast_next_3mo(values: list[float]) -> list[float]:
    """Simple linear projection for next 3 months."""
    n = len(values)
    if n < 2:
        last = values[-1] if values else 0
        return [round(last, 2)] * 3

    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    slope = numerator / denominator if denominator != 0 else 0
    intercept = y_mean - slope * x_mean

    forecasts = []
    for i in range(n, n + 3):
        projected = max(0, intercept + slope * i)
        forecasts.append(round(projected, 2))
    return forecasts


# ---------------------------------------------------------------------------
# ZIP Trends
# ---------------------------------------------------------------------------

async def get_zip_trends(db: AsyncSession, zip_code: str, months: int = 12) -> dict:
    """Compute permit velocity, price trends, violation trends for a ZIP over time."""

    # 1. Permits from permits table (state_code, zip_code, date_created)
    permits_sql = text("""
        SELECT date_trunc('month', date_created) as month,
               count(*) as permits
        FROM permits
        WHERE zip_code = :zip AND date_created >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    permits_rows = await safe_query(
        db, permits_sql.bindparams(zip=zip_code, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # 2. Hot leads (state, zip, issue_date, valuation)
    hot_leads_sql = text("""
        SELECT date_trunc('month', issue_date) as month,
               count(*) as permits,
               avg(valuation) as avg_val
        FROM hot_leads
        WHERE zip = :zip AND issue_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    hot_leads_rows = await safe_query(
        db, hot_leads_sql.bindparams(zip=zip_code, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # 3. Code violations
    violations_sql = text("""
        SELECT date_trunc('month', violation_date) as month,
               count(*) as violations
        FROM code_violations
        WHERE zip = :zip AND violation_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    violations_rows = await safe_query(
        db, violations_sql.bindparams(zip=zip_code, months=f"{months} months"),
        timeout_ms=8000, fallback=[],
    )

    # 4. Property sales
    sales_sql = text("""
        SELECT date_trunc('month', sale_date) as month,
               count(*) as sales,
               avg(sale_price) as avg_sale_price
        FROM property_sales
        WHERE zip = :zip AND sale_date >= NOW() - INTERVAL :months
              AND sale_price > 0
        GROUP BY 1 ORDER BY 1
    """)
    sales_rows = await safe_query(
        db, sales_sql.bindparams(zip=zip_code, months=f"{months} months"),
        timeout_ms=8000, fallback=[],
    )

    # 5. Merge into monthly data points
    monthly_data: dict[str, dict] = {}

    for row in permits_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            monthly_data.setdefault(key, {})["permit_count"] = int(row[1])

    for row in hot_leads_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            d = monthly_data.setdefault(key, {})
            d["hot_lead_count"] = int(row[1])
            d["avg_valuation"] = round(float(row[2]), 2) if row[2] else None

    for row in violations_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            monthly_data.setdefault(key, {})["violation_count"] = int(row[1])

    for row in sales_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            d = monthly_data.setdefault(key, {})
            d["sale_count"] = int(row[1])
            d["avg_sale_price"] = round(float(row[2]), 2) if row[2] else None

    # Sort and build output
    sorted_months = sorted(monthly_data.keys())
    data_points = []
    permit_values = []

    for i, month_key in enumerate(sorted_months):
        d = monthly_data[month_key]
        pc = d.get("permit_count", 0) + d.get("hot_lead_count", 0)
        permit_values.append(float(pc))

        prev_pc = None
        if i > 0:
            prev = monthly_data[sorted_months[i - 1]]
            prev_pc = prev.get("permit_count", 0) + prev.get("hot_lead_count", 0)

        yoy_pc = None
        if i >= 12:
            yoy_d = monthly_data.get(sorted_months[i - 12])
            if yoy_d:
                yoy_pc = yoy_d.get("permit_count", 0) + yoy_d.get("hot_lead_count", 0)

        data_points.append({
            "month": month_key,
            "permit_count": pc,
            "avg_valuation": d.get("avg_valuation"),
            "violation_count": d.get("violation_count", 0),
            "sale_count": d.get("sale_count", 0),
            "avg_sale_price": d.get("avg_sale_price"),
            "mom_change": _compute_mom_change(pc, prev_pc),
            "yoy_change": _compute_yoy_change(pc, yoy_pc),
        })

    trend_direction = _compute_trend_direction(permit_values)
    forecast = _forecast_next_3mo(permit_values)

    return {
        "zip_code": zip_code,
        "months": months,
        "data_points": data_points,
        "trend_direction": trend_direction,
        "forecast_next_3mo": forecast,
        "total_permits": sum(int(d.get("permit_count", 0) + d.get("hot_lead_count", 0)) for d in monthly_data.values()),
        "total_violations": sum(int(d.get("violation_count", 0)) for d in monthly_data.values()),
        "total_sales": sum(int(d.get("sale_count", 0)) for d in monthly_data.values()),
    }


# ---------------------------------------------------------------------------
# Contractor Trajectory
# ---------------------------------------------------------------------------

async def get_contractor_trajectory(db: AsyncSession, contractor_name: str, months: int = 24) -> dict:
    """Track how a contractor's activity and risk change over time."""

    search = f"%{contractor_name}%"

    # Permits from hot_leads
    permits_sql = text("""
        SELECT date_trunc('month', issue_date) as month,
               count(*) as permits,
               count(DISTINCT state) as jurisdictions,
               avg(valuation) as avg_valuation
        FROM hot_leads
        WHERE (contractor_company ILIKE :name OR contractor_name ILIKE :name
               OR applicant_name ILIKE :name)
              AND issue_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    permits_rows = await safe_query(
        db, permits_sql.bindparams(name=search, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # License status
    license_sql = text("""
        SELECT status, issue_date, expiration_date, state, classifications
        FROM contractor_licenses
        WHERE business_name ILIKE :name
        ORDER BY issue_date DESC NULLS LAST
        LIMIT 10
    """)
    license_rows = await safe_query(
        db, license_sql.bindparams(name=search),
        timeout_ms=8000, fallback=[],
    )

    # Violations linked to contractor
    violations_sql = text("""
        SELECT date_trunc('month', violation_date) as month,
               count(*) as violations
        FROM code_violations
        WHERE (address IN (
                SELECT address FROM hot_leads
                WHERE contractor_company ILIKE :name OR contractor_name ILIKE :name
                LIMIT 500
              ))
              AND violation_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    violations_rows = await safe_query(
        db, violations_sql.bindparams(name=search, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # Build monthly data
    monthly_data: dict[str, dict] = {}
    permit_values = []

    for row in permits_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            monthly_data.setdefault(key, {})
            monthly_data[key]["permit_count"] = int(row[1])
            monthly_data[key]["jurisdiction_count"] = int(row[2])
            monthly_data[key]["avg_valuation"] = round(float(row[3]), 2) if row[3] else None

    violations_map: dict[str, int] = {}
    for row in violations_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            violations_map[key] = int(row[1])

    sorted_months = sorted(monthly_data.keys())
    data_points = []
    for month_key in sorted_months:
        d = monthly_data[month_key]
        pc = d.get("permit_count", 0)
        permit_values.append(float(pc))
        data_points.append({
            "month": month_key,
            "permit_count": pc,
            "jurisdiction_count": d.get("jurisdiction_count", 0),
            "avg_valuation": d.get("avg_valuation"),
            "violation_count": violations_map.get(month_key, 0),
        })

    # Licenses
    licenses = []
    for row in license_rows:
        licenses.append({
            "status": row[0],
            "issue_date": row[1].isoformat() if row[1] else None,
            "expiration_date": row[2].isoformat() if row[2] else None,
            "state": row[3],
            "classifications": row[4],
        })

    trend_direction = _compute_trend_direction(permit_values)

    # Determine risk trajectory
    total_violations = sum(violations_map.values())
    expired_licenses = sum(1 for l in licenses if l["status"] and "expir" in l["status"].lower())
    risk_level = "low"
    if total_violations > 5 or expired_licenses > 0:
        risk_level = "elevated"
    if total_violations > 15 or expired_licenses > 1:
        risk_level = "high"

    return {
        "contractor_name": contractor_name,
        "months": months,
        "data_points": data_points,
        "trend": trend_direction,
        "licenses": licenses,
        "risk_level": risk_level,
        "total_permits": sum(d.get("permit_count", 0) for d in monthly_data.values()),
        "total_violations": total_violations,
        "active_jurisdictions": len(set(
            d.get("jurisdiction_count", 0) for d in monthly_data.values()
        )),
    }


# ---------------------------------------------------------------------------
# Market Momentum
# ---------------------------------------------------------------------------

async def get_market_momentum(db: AsyncSession, state: str, months: int = 12) -> dict:
    """State-level market momentum combining multiple signals."""
    state = state.upper()

    # 1. Permits filed trend (from permits table: state_code, date_created)
    permits_sql = text("""
        SELECT date_trunc('month', date_created) as month,
               count(*) as permits
        FROM permits
        WHERE state_code = :state AND date_created >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    permits_rows = await safe_query(
        db, permits_sql.bindparams(state=state, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # 2. Hot leads permits trend
    hot_sql = text("""
        SELECT date_trunc('month', issue_date) as month,
               count(*) as permits,
               avg(valuation) as avg_val
        FROM hot_leads
        WHERE state = :state AND issue_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    hot_rows = await safe_query(
        db, hot_sql.bindparams(state=state, months=f"{months} months"),
        timeout_ms=10000, fallback=[],
    )

    # 3. Home price trend (from property_valuations)
    price_sql = text("""
        SELECT date_trunc('month', period_end) as month,
               avg(median_sale_price) as avg_price,
               sum(homes_sold) as homes_sold
        FROM property_valuations
        WHERE (state = :state OR state_code = :state)
              AND period_end >= NOW() - INTERVAL :months
              AND median_sale_price > 0
        GROUP BY 1 ORDER BY 1
    """)
    price_rows = await safe_query(
        db, price_sql.bindparams(state=state, months=f"{months} months"),
        timeout_ms=8000, fallback=[],
    )

    # 4. Code violations trend
    violations_sql = text("""
        SELECT date_trunc('month', violation_date) as month,
               count(*) as violations
        FROM code_violations
        WHERE state = :state AND violation_date >= NOW() - INTERVAL :months
        GROUP BY 1 ORDER BY 1
    """)
    violations_rows = await safe_query(
        db, violations_sql.bindparams(state=state, months=f"{months} months"),
        timeout_ms=8000, fallback=[],
    )

    # 5. Property sales trend
    sales_sql = text("""
        SELECT date_trunc('month', sale_date) as month,
               count(*) as sales,
               avg(sale_price) as avg_price
        FROM property_sales
        WHERE state = :state AND sale_date >= NOW() - INTERVAL :months
              AND sale_price > 0
        GROUP BY 1 ORDER BY 1
    """)
    sales_rows = await safe_query(
        db, sales_sql.bindparams(state=state, months=f"{months} months"),
        timeout_ms=8000, fallback=[],
    )

    # Merge
    monthly_data: dict[str, dict] = {}

    for row in permits_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            monthly_data.setdefault(key, {})["permit_count"] = int(row[1])

    for row in hot_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            d = monthly_data.setdefault(key, {})
            d["hot_lead_count"] = int(row[1])
            d["avg_valuation"] = round(float(row[2]), 2) if row[2] else None

    for row in price_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            d = monthly_data.setdefault(key, {})
            d["avg_home_price"] = round(float(row[1]), 2) if row[1] else None
            d["homes_sold"] = int(row[2]) if row[2] else 0

    for row in violations_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            monthly_data.setdefault(key, {})["violation_count"] = int(row[1])

    for row in sales_rows:
        key = row[0].strftime("%Y-%m") if row[0] else None
        if key:
            d = monthly_data.setdefault(key, {})
            d["sale_count"] = int(row[1])
            d["avg_sale_price"] = round(float(row[2]), 2) if row[2] else None

    sorted_months = sorted(monthly_data.keys())
    data_points = []
    permit_values = []
    price_values = []

    for month_key in sorted_months:
        d = monthly_data[month_key]
        pc = d.get("permit_count", 0) + d.get("hot_lead_count", 0)
        permit_values.append(float(pc))
        if d.get("avg_home_price"):
            price_values.append(d["avg_home_price"])

        data_points.append({
            "month": month_key,
            "permit_count": pc,
            "avg_valuation": d.get("avg_valuation"),
            "avg_home_price": d.get("avg_home_price"),
            "homes_sold": d.get("homes_sold", 0),
            "violation_count": d.get("violation_count", 0),
            "sale_count": d.get("sale_count", 0),
            "avg_sale_price": d.get("avg_sale_price"),
        })

    # Compute momentum score (0-100)
    permit_trend = _compute_trend_direction(permit_values)
    price_trend = _compute_trend_direction(price_values)

    # Score components
    permit_score = 0
    if permit_values:
        recent = permit_values[-3:] if len(permit_values) >= 3 else permit_values
        earlier = permit_values[:3] if len(permit_values) >= 6 else permit_values[:len(permit_values)//2 or 1]
        recent_avg = sum(recent) / len(recent) if recent else 0
        earlier_avg = sum(earlier) / len(earlier) if earlier else 0
        if earlier_avg > 0:
            permit_change = (recent_avg - earlier_avg) / earlier_avg
            permit_score = min(50, max(0, int(25 + permit_change * 50)))
        else:
            permit_score = 25  # neutral

    price_score = 0
    if price_values:
        recent_p = price_values[-3:] if len(price_values) >= 3 else price_values
        earlier_p = price_values[:3] if len(price_values) >= 6 else price_values[:len(price_values)//2 or 1]
        recent_p_avg = sum(recent_p) / len(recent_p) if recent_p else 0
        earlier_p_avg = sum(earlier_p) / len(earlier_p) if earlier_p else 0
        if earlier_p_avg > 0:
            price_change = (recent_p_avg - earlier_p_avg) / earlier_p_avg
            price_score = min(50, max(0, int(25 + price_change * 100)))
        else:
            price_score = 25  # neutral

    momentum_score = min(100, permit_score + price_score)

    return {
        "state": state,
        "months": months,
        "data_points": data_points,
        "momentum_score": momentum_score,
        "permit_trend": permit_trend,
        "price_trend": price_trend,
        "forecast_permits": _forecast_next_3mo(permit_values),
        "forecast_prices": _forecast_next_3mo(price_values) if price_values else [],
        "total_permits": sum(permit_values),
        "total_sales": sum(d.get("sale_count", 0) for d in monthly_data.values()),
    }


# ---------------------------------------------------------------------------
# Entity Timeline
# ---------------------------------------------------------------------------

async def get_entity_timeline(db: AsyncSession, entity_name: str) -> dict:
    """Track an LLC/entity's activity timeline across all data layers."""

    search = f"%{entity_name}%"
    events = []

    # 1. Business entity formation
    entity_sql = text("""
        SELECT entity_name, entity_type, state, filing_number, status,
               formation_date, dissolution_date, registered_agent_name
        FROM business_entities
        WHERE entity_name ILIKE :name
        ORDER BY formation_date DESC NULLS LAST
        LIMIT 20
    """)
    entity_rows = await safe_query(
        db, entity_sql.bindparams(name=search),
        timeout_ms=8000, fallback=[],
    )
    for row in entity_rows:
        if row[5]:  # formation_date
            events.append({
                "date": row[5].isoformat(),
                "type": "entity_formed",
                "description": f"{row[0]} ({row[1] or 'entity'}) formed in {row[2]}",
                "details": {
                    "entity_name": row[0],
                    "entity_type": row[1],
                    "state": row[2],
                    "filing_number": row[3],
                    "status": row[4],
                },
            })
        if row[6]:  # dissolution_date
            events.append({
                "date": row[6].isoformat(),
                "type": "entity_dissolved",
                "description": f"{row[0]} dissolved in {row[2]}",
                "details": {"entity_name": row[0], "state": row[2]},
            })

    # 2. Property purchases (grantor/grantee)
    sales_sql = text("""
        SELECT address, city, state, sale_price, sale_date, grantor, grantee
        FROM property_sales
        WHERE (grantor ILIKE :name OR grantee ILIKE :name)
              AND sale_date IS NOT NULL
        ORDER BY sale_date DESC
        LIMIT 50
    """)
    sales_rows = await safe_query(
        db, sales_sql.bindparams(name=search),
        timeout_ms=8000, fallback=[],
    )
    for row in sales_rows:
        if row[4]:  # sale_date
            role = "buyer" if row[6] and entity_name.lower() in row[6].lower() else "seller"
            events.append({
                "date": row[4].isoformat(),
                "type": f"property_{role == 'buyer' and 'purchased' or 'sold'}",
                "description": f"{'Purchased' if role == 'buyer' else 'Sold'} {row[0]}, {row[1]}, {row[2]} for ${row[3]:,.0f}" if row[3] else f"Property transaction at {row[0]}",
                "details": {
                    "address": row[0],
                    "city": row[1],
                    "state": row[2],
                    "sale_price": float(row[3]) if row[3] else None,
                    "role": role,
                    "grantor": row[5],
                    "grantee": row[6],
                },
            })

    # 3. Permits filed
    permits_sql = text("""
        SELECT permit_number, address, city, state, zip, issue_date,
               valuation, permit_type, contractor_company
        FROM hot_leads
        WHERE (contractor_company ILIKE :name OR applicant_name ILIKE :name
               OR owner_name ILIKE :name)
              AND issue_date IS NOT NULL
        ORDER BY issue_date DESC
        LIMIT 50
    """)
    permits_rows = await safe_query(
        db, permits_sql.bindparams(name=search),
        timeout_ms=10000, fallback=[],
    )
    for row in permits_rows:
        if row[5]:  # issue_date
            events.append({
                "date": row[5].isoformat(),
                "type": "permit_filed",
                "description": f"Permit {row[0] or 'N/A'} filed at {row[1]}, {row[2]}, {row[3]}",
                "details": {
                    "permit_number": row[0],
                    "address": row[1],
                    "city": row[2],
                    "state": row[3],
                    "zip": row[4],
                    "valuation": float(row[6]) if row[6] else None,
                    "permit_type": row[7],
                    "contractor": row[8],
                },
            })

    # 4. Liens filed/released
    liens_sql = text("""
        SELECT lien_type, address, city, state, amount, filing_date,
               lapse_date, status, debtor_name, creditor_name
        FROM property_liens
        WHERE (debtor_name ILIKE :name OR creditor_name ILIKE :name)
              AND filing_date IS NOT NULL
        ORDER BY filing_date DESC
        LIMIT 30
    """)
    liens_rows = await safe_query(
        db, liens_sql.bindparams(name=search),
        timeout_ms=8000, fallback=[],
    )
    for row in liens_rows:
        if row[5]:  # filing_date
            events.append({
                "date": row[5].isoformat(),
                "type": "lien_filed",
                "description": f"{row[0] or 'Lien'} filed: ${row[4]:,.0f}" if row[4] else f"{row[0] or 'Lien'} filed at {row[1]}",
                "details": {
                    "lien_type": row[0],
                    "address": row[1],
                    "city": row[2],
                    "state": row[3],
                    "amount": float(row[4]) if row[4] else None,
                    "status": row[7],
                    "debtor": row[8],
                    "creditor": row[9],
                },
            })
        if row[6]:  # lapse_date
            events.append({
                "date": row[6].isoformat(),
                "type": "lien_lapsed",
                "description": f"{row[0] or 'Lien'} lapsed/released at {row[1]}",
                "details": {
                    "lien_type": row[0],
                    "address": row[1],
                    "status": row[7],
                },
            })

    # 5. Code violations
    violations_sql = text("""
        SELECT address, city, state, violation_type, violation_date,
               status, fine_amount, description
        FROM code_violations
        WHERE address IN (
            SELECT DISTINCT address FROM property_sales
            WHERE (grantor ILIKE :name OR grantee ILIKE :name)
            LIMIT 100
        )
        AND violation_date IS NOT NULL
        ORDER BY violation_date DESC
        LIMIT 30
    """)
    violations_rows = await safe_query(
        db, violations_sql.bindparams(name=search),
        timeout_ms=8000, fallback=[],
    )
    for row in violations_rows:
        if row[4]:  # violation_date
            events.append({
                "date": row[4].isoformat(),
                "type": "violation",
                "description": f"{row[3] or 'Violation'} at {row[0]}, {row[1]}, {row[2]}",
                "details": {
                    "address": row[0],
                    "city": row[1],
                    "state": row[2],
                    "violation_type": row[3],
                    "status": row[5],
                    "fine_amount": float(row[6]) if row[6] else None,
                    "description": row[7],
                },
            })

    # Sort all events chronologically
    events.sort(key=lambda e: e["date"], reverse=True)

    # Compute summary
    event_types = {}
    for e in events:
        event_types[e["type"]] = event_types.get(e["type"], 0) + 1

    # Entity info from business_entities
    entity_info = None
    if entity_rows:
        row = entity_rows[0]
        entity_info = {
            "entity_name": row[0],
            "entity_type": row[1],
            "state": row[2],
            "filing_number": row[3],
            "status": row[4],
            "formation_date": row[5].isoformat() if row[5] else None,
            "registered_agent": row[7],
        }

    return {
        "entity_name": entity_name,
        "entity_info": entity_info,
        "total_events": len(events),
        "event_types": event_types,
        "events": events[:100],  # Cap at 100 events
    }

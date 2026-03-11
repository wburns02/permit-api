"""Risk signal computation for insurance/underwriting."""

from datetime import date, timedelta


def compute_risk_signals(permits: list[dict]) -> dict:
    """Compute property risk signals from a list of permits."""
    if not permits:
        return {
            "permit_count": 0,
            "last_permit_date": None,
            "years_since_last_permit": None,
            "has_unpermitted_gap": False,
            "renovation_intensity": 0.0,
            "permit_type_breakdown": {},
        }

    # Parse dates
    dates = []
    for p in permits:
        d = p.get("issue_date")
        if d:
            if isinstance(d, str):
                try:
                    d = date.fromisoformat(d)
                except ValueError:
                    continue
            dates.append(d)

    dates.sort()
    today = date.today()

    last_permit_date = dates[-1] if dates else None
    years_since = None
    if last_permit_date:
        years_since = round((today - last_permit_date).days / 365.25, 1)

    # Unpermitted gap: any 10+ year gap between consecutive permits
    has_gap = False
    for i in range(1, len(dates)):
        if (dates[i] - dates[i - 1]).days > 3652:  # ~10 years
            has_gap = True
            break

    # Renovation intensity: permits per year over active span
    intensity = 0.0
    if len(dates) >= 2:
        span_years = max((dates[-1] - dates[0]).days / 365.25, 1.0)
        intensity = round(len(permits) / span_years, 2)
    elif len(dates) == 1:
        intensity = 1.0

    # Type breakdown
    breakdown = {}
    for p in permits:
        pt = p.get("permit_type") or "unknown"
        breakdown[pt] = breakdown.get(pt, 0) + 1

    return {
        "permit_count": len(permits),
        "last_permit_date": last_permit_date.isoformat() if last_permit_date else None,
        "years_since_last_permit": years_since,
        "has_unpermitted_gap": has_gap,
        "renovation_intensity": intensity,
        "permit_type_breakdown": breakdown,
    }

"""Owner enrichment via BatchData skip-trace.

Caches results in parcel_owner_enrichment by (state, city_slug, apn) for
90 days. Re-enriching the same parcel within the TTL returns the cached
row. Beyond TTL, we refetch.

Daily soft cap per user (default 50) enforced before any outbound call.
"""

import logging
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.parcel_screen import ParcelOwnerEnrichment, ParcelJurisdiction
from app.services.parcel_screen_service import pull_parcel_facts

logger = logging.getLogger(__name__)

_BATCHDATA_URL = "https://api.batchdata.com/api/v1/property/skip-trace"
CACHE_TTL_DAYS = 90
DEFAULT_DAILY_CAP = 50
COST_CENTS_PER_LOOKUP = 25  # BatchData skip-trace residential — Will's rate


async def get_daily_count(db: AsyncSession, user_id) -> int:
    """How many enrichments this user has paid for today (UTC).

    Only counts FRESH lookups (rows fetched today). Cache hits do not bump
    fetched_at, so they are naturally excluded.
    """
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    stmt = select(func.count()).select_from(ParcelOwnerEnrichment).where(
        ParcelOwnerEnrichment.fetched_by_user_id == user_id,
        ParcelOwnerEnrichment.fetched_at >= today_start,
    )
    result = await db.execute(stmt)
    return int(result.scalar() or 0)


def _extract_persons(raw: dict) -> list:
    """BatchData has shifted response shape historically. Handle both.

    Known shapes:
      - {"results": {"persons": [...]}}              (current — hail_leads.py)
      - {"results": [{"persons": [...]}, ...]}       (per-request list)
      - {"data": {"results": {...}}}                 (legacy wrapper)
    """
    try:
        results = raw.get("results")
        if results is None:
            results = (raw.get("data") or {}).get("results")
        if results is None:
            return []
        # Dict shape: {"persons": [...]}
        if isinstance(results, dict):
            return results.get("persons") or []
        # List shape: [{"persons": [...]}, ...]
        if isinstance(results, list) and results:
            first = results[0]
            if isinstance(first, dict):
                return first.get("persons") or []
    except Exception as e:
        logger.warning("[enrich-owner] unexpected response shape: %s", e)
    return []


def _normalize_persons(persons_raw: list) -> list[dict]:
    """Flatten BatchData person objects into our stored shape."""
    persons_out: list[dict] = []
    for p in persons_raw or []:
        if not isinstance(p, dict):
            continue
        name = p.get("name") or {}
        phones_in = p.get("phoneNumbers") or []
        phones_sorted = sorted(
            phones_in,
            key=lambda ph: (
                -int(ph.get("score") or 0),
                0 if (ph.get("type") == "Mobile") else 1,
            ),
        )
        phones_out = [
            {
                "number": ph.get("number"),
                "type": ph.get("type"),
                "score": ph.get("score"),
                "dnc": bool(ph.get("dnc")) if ph.get("dnc") is not None else False,
            }
            for ph in phones_sorted[:5]
        ]
        emails_in = p.get("emails") or []
        emails_out = [
            e.get("email") if isinstance(e, dict) else e
            for e in emails_in
            if e
        ]
        mail = p.get("mailingAddress") or {}
        demog = p.get("demographics") or {}
        persons_out.append({
            "name": {
                "first": name.get("first"),
                "last":  name.get("last"),
                "full":  name.get("full"),
            },
            "phones": phones_out,
            "emails": emails_out[:5],
            "mailing_address": {
                "street": mail.get("street"),
                "city":   mail.get("city"),
                "state":  mail.get("state"),
                "zip":    mail.get("zip"),
            },
            "demographics": {
                "age":      demog.get("age"),
                "deceased": demog.get("deceased"),
            },
        })
    return persons_out


async def enrich_owner(
    db: AsyncSession,
    user_id,
    state: str,
    city_slug: str,
    apn: str,
    force_refresh: bool = False,
) -> dict:
    """Look up owner contact info for the given parcel.

    Returns:
      {
        "state", "city_slug", "apn",
        "owner_name", "property_addr",
        "persons": [...],
        "hit": bool,
        "is_cached": bool,
        "cached_age_days": int | None,
        "fetched_at": iso,
        "cost_cents": int (0 if cached, else COST_CENTS_PER_LOOKUP),
      }
    """
    # 1) Cache lookup
    stmt = select(ParcelOwnerEnrichment).where(
        ParcelOwnerEnrichment.state == state,
        ParcelOwnerEnrichment.city_slug == city_slug,
        ParcelOwnerEnrichment.apn == apn,
    )
    result = await db.execute(stmt)
    cached = result.scalar_one_or_none()

    now = datetime.now(timezone.utc)
    if cached and not force_refresh:
        cached_at = cached.fetched_at
        # PG returns aware datetimes for `timestamp with time zone` columns, but
        # be defensive in case any row was inserted naive.
        if cached_at and cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        if cached_at:
            age = now - cached_at
            if age <= timedelta(days=CACHE_TTL_DAYS):
                return {
                    "state": state, "city_slug": city_slug, "apn": apn,
                    "owner_name": cached.owner_name,
                    "property_addr": cached.property_addr,
                    "persons": cached.persons or [],
                    "hit": bool(cached.hit),
                    "is_cached": True,
                    "cached_age_days": age.days,
                    "fetched_at": cached_at.isoformat(),
                    "cost_cents": 0,
                }

    # 2) Need to fetch — first resolve the parcel's address by pulling facts
    jur_stmt = select(ParcelJurisdiction).where(
        ParcelJurisdiction.state == state,
        ParcelJurisdiction.city_slug == city_slug,
    )
    jur = (await db.execute(jur_stmt)).scalar_one_or_none()
    if not jur:
        raise ValueError(f"jurisdiction not registered: {state}/{city_slug}")

    facts = await pull_parcel_facts(jur, address=None, apn=apn)
    if facts.get("error"):
        raise ValueError(facts["error"])

    # Build the BatchData request — we need street/city/state/zip
    addr_str = (facts.get("address") or "").strip()
    # Crude street parse: take everything before the first comma; the rest is city/state/zip.
    # BatchData accepts free-form addresses too, so this is best-effort.
    parts = [p.strip() for p in addr_str.split(",")]
    street = parts[0] if parts else addr_str
    city_full = parts[1] if len(parts) >= 2 else ""
    # The third part is usually "STATE ZIP" — split on whitespace
    state_zip = parts[2].split() if len(parts) >= 3 else []
    st = state_zip[0] if state_zip else state.upper()
    zp = state_zip[1] if len(state_zip) >= 2 else ""

    property_addr = {"street": street, "city": city_full, "state": st, "zip": zp}

    # 3) Call BatchData
    api_key = (settings.BATCHDATA_API_KEY or "").strip()
    if not api_key:
        raise RuntimeError("BATCHDATA_API_KEY not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(
                _BATCHDATA_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json={"requests": [{"propertyAddress": property_addr}]},
            )
    except httpx.HTTPError as e:
        raise RuntimeError(f"BatchData request error: {e}")

    if r.status_code == 401:
        # Don't retry on auth failure — surface clearly.
        raise RuntimeError("BatchData credential not configured or revoked")

    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"BatchData HTTP {r.status_code}: {e}")

    raw = r.json()

    # 4) Normalize the response.
    persons_raw = _extract_persons(raw)
    persons_out = _normalize_persons(persons_raw)

    hit = bool(
        persons_out
        and (persons_out[0].get("phones") or persons_out[0].get("emails"))
    )

    # 5) Upsert into cache
    from sqlalchemy.dialects.postgresql import insert
    stmt = insert(ParcelOwnerEnrichment).values(
        state=state, city_slug=city_slug, apn=apn,
        owner_name=facts.get("owner_name"),
        property_addr=property_addr,
        persons=persons_out,
        raw_response=raw,
        hit=hit,
        cost_cents=COST_CENTS_PER_LOOKUP,
        fetched_at=now,
        fetched_by_user_id=user_id,
    ).on_conflict_do_update(
        index_elements=["state", "city_slug", "apn"],
        set_={
            "owner_name": facts.get("owner_name"),
            "property_addr": property_addr,
            "persons": persons_out,
            "raw_response": raw,
            "hit": hit,
            "cost_cents": COST_CENTS_PER_LOOKUP,
            "fetched_at": now,
            "fetched_by_user_id": user_id,
        },
    )
    await db.execute(stmt)
    await db.commit()

    # Safety: don't ever return the raw key or full numbers to logs
    n_phones = sum(len(p.get("phones") or []) for p in persons_out)
    n_emails = sum(len(p.get("emails") or []) for p in persons_out)
    logger.info(
        "[enrich-owner] %s/%s/%s fresh fetch: %d persons, %d phones, %d emails, hit=%s",
        state, city_slug, apn, len(persons_out), n_phones, n_emails, hit,
    )

    return {
        "state": state, "city_slug": city_slug, "apn": apn,
        "owner_name": facts.get("owner_name"),
        "property_addr": property_addr,
        "persons": persons_out,
        "hit": hit,
        "is_cached": False,
        "cached_age_days": 0,
        "fetched_at": now.isoformat(),
        "cost_cents": COST_CENTS_PER_LOOKUP,
    }

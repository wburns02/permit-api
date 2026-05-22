"""High-level helpers for emitting `permitlookup.*` events.

Call ``emit_permit_detected(...)`` after a permit row has been enriched.
Gated. Safe to call unconditionally — if the flag is off, this is a no-op.
"""

from __future__ import annotations

import logging
from typing import Any

from .emitter import EmitResult, get_emitter, is_enabled

logger = logging.getLogger("burns.l4.permit_api")

_PERMITLOOKUP_SOURCE = "permitlookup.enrichment_worker"
_PERMIT_DETECTED_TYPE = "permitlookup.permit.detected"

# Map the enrichment worker's internal trade strings to the schema enum.
_TRADE_ENUM = {"septic", "electrical", "plumbing", "roofing", "hvac", "other"}


def _coerce_trade(raw: str | None) -> str:
    if not raw:
        return "other"
    norm = raw.strip().lower()
    return norm if norm in _TRADE_ENUM else "other"


def _build_permit_subject(permit_key: str | None) -> str:
    return permit_key or "permit:unknown"


def emit_permit_detected(
    *,
    permit_id: str,
    address: str,
    trade: str,
    county: str,
    state: str,
    owner_name_raw: str,
    permit_number: str | None = None,
    permit_date: str | None = None,
    property_apn: str | None = None,
    property_id: str | None = None,
    person_id: str | None = None,
) -> EmitResult:
    """Emit `permitlookup.permit.detected` if Burns L4 is enabled.

    All identifying strings are passed straight through to the envelope's
    ``data`` block. Set ``person_id=None`` when identity hasn't been resolved
    yet — the downstream bridge does the resolution.

    Returns an ``EmitResult`` whose ``emitted=False`` cases the caller can
    safely ignore (logged at warn level inside the emitter).
    """
    if not is_enabled():
        return get_emitter().emit(
            event_type=_PERMIT_DETECTED_TYPE,
            source=_PERMITLOOKUP_SOURCE,
            subject=_build_permit_subject(permit_id),
            data={},
            links={},
        )

    # Note: the schema rejects unknown keys (additionalProperties: false),
    # so we never include optional fields when the caller didn't pass them.
    data: dict[str, Any] = {
        "permit_id": permit_id,
        "address": address,
        "trade": _coerce_trade(trade),
        "county": county,
        "state": state,
        "owner_name_raw": owner_name_raw,
    }
    if permit_number:
        data["permit_number"] = permit_number
    if permit_date:
        data["permit_date"] = permit_date
    if property_apn:
        data["property_apn"] = property_apn

    links: dict[str, Any] = {
        "property_id": property_id,
        "permit_id": permit_id,
        "person_id": person_id,
    }

    return get_emitter().emit(
        event_type=_PERMIT_DETECTED_TYPE,
        source=_PERMITLOOKUP_SOURCE,
        subject=_build_permit_subject(permit_id),
        data=data,
        links=links,
    )

"""Burns Industries Layer 4 emitter — enrichment-worker vendored copy.

Self-contained CloudEvents 1.0 emitter that writes `permitlookup.permit.detected`
events to the Burns substrate (event_log on r730-2:5432/burns_events) and pushes
them to Hatchet so the `permit_to_crm_bridge` workflow can fire.

Mirrors the pattern from /home/will/permit-api/app/burns_events (vendored there
for the FastAPI side) and /home/will/burns-layer-4/events (the substrate's own
emitter). Kept inline here so the enrichment-worker daemon ships without depending
on either of those repos.

GATED. Default OFF. If BURNS_L4_EMIT_ENABLED is not "true" (case-insensitive),
every emit_permit_detected() call returns None immediately. If Hatchet env vars
are missing, the event still lands in event_log and a warning is logged ONCE
per process. If BURNS_EVENTS_DSN itself is unreachable, the function logs an
error and returns None — the caller's enrichment loop is never broken.

See: /home/will/docs/superpowers/specs/2026-05-21-burns-industries-layer-4-design.md
"""

from .emitter import (
    BurnsEmitter,
    EmitResult,
    emit_permit_detected,
    get_emitter,
    is_enabled,
    reset_emitter,
)

__all__ = [
    "BurnsEmitter",
    "EmitResult",
    "emit_permit_detected",
    "get_emitter",
    "is_enabled",
    "reset_emitter",
]

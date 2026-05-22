"""Burns Industries Layer 4 event emitter (permit-api side).

Self-contained CloudEvents 1.0 emitter for the `permitlookup.*` event family.
Mirrors /home/will/burns-layer-4/events but vendored here so permit-api can
deploy independently to Railway without external paths.

GATED BEHIND `BURNS_L4_EMIT_ENABLED=true`.

Default off. If the flag is off OR any of the required env vars are missing
(HATCHET_CLIENT_HOST_PORT, HATCHET_CLIENT_TOKEN, BURNS_EVENTS_DSN), every
emit() call is a no-op and a warning is logged ONCE on first attempt.

See:
  /home/will/docs/superpowers/specs/2026-05-21-burns-industries-layer-4-design.md
"""

from .emitter import BurnsEmitter, EmitResult, get_emitter, is_enabled, reset_emitter

__all__ = [
    "BurnsEmitter",
    "EmitResult",
    "get_emitter",
    "is_enabled",
    "reset_emitter",
]

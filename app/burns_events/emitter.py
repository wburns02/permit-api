"""Burns Layer 4 event emitter — permit-api vendored copy.

The emitter is a thin wrapper around Hatchet's event.push() plus a JSON Schema
validation step against the vendored schema in
``app/burns_events/schemas/<event_type>/v1.json``.

All connectivity is OPTIONAL. The emitter exposes ``is_enabled()`` which
short-circuits when:
  - ``BURNS_L4_EMIT_ENABLED`` is not ``"true"`` (case-insensitive); or
  - any of ``HATCHET_CLIENT_HOST_PORT``, ``HATCHET_CLIENT_TOKEN``,
    ``BURNS_EVENTS_DSN`` are missing.

Missing-env warnings log ONCE per process via a module-level flag.

Hatchet SDK import is deferred to ``_lazy_client`` so unit tests can patch it
without the SDK being installed.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger("burns.l4.permit_api")

# One-shot warning flag. Module-level so it survives across calls in one
# process but resets when the process restarts (which is when we want to
# warn again about misconfigured env).
_warned_missing_env = False

_SCHEMAS_ROOT = Path(__file__).resolve().parent / "schemas"


def _flag_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def is_enabled() -> bool:
    """Return True iff the flag is on AND all required env vars are present."""
    global _warned_missing_env
    if not _flag_truthy(os.environ.get("BURNS_L4_EMIT_ENABLED")):
        return False
    missing = [
        name for name in (
            "HATCHET_CLIENT_HOST_PORT",
            "HATCHET_CLIENT_TOKEN",
            "BURNS_EVENTS_DSN",
        )
        if not os.environ.get(name)
    ]
    if missing:
        if not _warned_missing_env:
            logger.warning(
                "BURNS_L4_EMIT_ENABLED=true but missing env vars %s; "
                "burns-l4 emitter will no-op until set.",
                missing,
            )
            _warned_missing_env = True
        return False
    return True


@dataclass(slots=True)
class EmitResult:
    """Outcome of a single emit() call."""
    emitted: bool
    event_id: str
    hatchet_event_id: str | None
    envelope: dict[str, Any] | None
    reason: str | None = None  # set when emitted=False to explain why


# --- envelope ---------------------------------------------------------------

def _new_event_id() -> str:
    # ULID would be nicer but we don't want a new dep on permit-api just for this.
    # uuid4 hex is fine; event ordering still comes from `time`.
    return uuid.uuid4().hex


def _utcnow_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _build_envelope(
    *,
    event_type: str,
    source: str,
    subject: str,
    data: dict[str, Any],
    links: dict[str, Any],
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "specversion": "1.0",
        "id": event_id or _new_event_id(),
        "source": source,
        "type": event_type,
        "time": _utcnow_iso(),
        "subject": subject,
        "datacontenttype": "application/json",
        "data": data,
        "links": links,
    }


# --- schema validation ------------------------------------------------------

@lru_cache(maxsize=8)
def _load_schema(event_type: str) -> dict[str, Any]:
    path = _SCHEMAS_ROOT / event_type / "v1.json"
    if not path.exists():
        raise FileNotFoundError(f"Burns L4 schema not vendored: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _validate_or_raise(event_type: str, envelope: dict[str, Any]) -> None:
    """Validate the envelope against the vendored JSON Schema.

    Imports jsonschema lazily — if the dep is missing we skip validation and
    log a warning; better to ship the event than crash production over a dev
    dependency.
    """
    try:
        from jsonschema import Draft202012Validator  # type: ignore
    except ImportError:
        logger.warning(
            "jsonschema not installed; skipping schema validation for %s",
            event_type,
        )
        return
    schema = _load_schema(event_type)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(envelope), key=lambda e: tuple(e.path))
    if errors:
        first = errors[0]
        raise ValueError(
            f"event {event_type} failed schema validation: "
            f"{list(first.absolute_path)}: {first.message}"
        )


# --- emitter ---------------------------------------------------------------

class BurnsEmitter:
    """Wrap Hatchet's event.push() with validation + opt-in gating.

    Construction never crashes when the env is misconfigured. ``emit()``
    returns ``EmitResult(emitted=False, reason=...)`` in that case.

    Hatchet client is constructed lazily inside ``emit()`` so import-time
    failures in the SDK can't take down the FastAPI app.
    """

    def __init__(self, *, hatchet_client: Any | None = None) -> None:
        # Tests pass a pre-built mock client. Production leaves this None and
        # the lazy path picks up env vars on the first emit().
        self._client = hatchet_client
        self._client_attempted = hatchet_client is not None

    def _lazy_client(self) -> Any | None:
        if self._client is not None or self._client_attempted:
            return self._client
        self._client_attempted = True
        try:
            from hatchet_sdk import Hatchet  # type: ignore
            # Hatchet reads HATCHET_CLIENT_TOKEN + HATCHET_CLIENT_HOST_PORT from env.
            self._client = Hatchet()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Hatchet client init failed: %s", exc)
            self._client = None
        return self._client

    def emit(
        self,
        *,
        event_type: str,
        source: str,
        subject: str,
        data: dict[str, Any],
        links: dict[str, Any],
        event_id: str | None = None,
    ) -> EmitResult:
        if not is_enabled():
            return EmitResult(
                emitted=False,
                event_id=event_id or _new_event_id(),
                hatchet_event_id=None,
                envelope=None,
                reason="burns_l4_disabled",
            )

        envelope = _build_envelope(
            event_type=event_type,
            source=source,
            subject=subject,
            data=data,
            links=links,
            event_id=event_id,
        )

        try:
            _validate_or_raise(event_type, envelope)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "burns-l4 envelope validation failed for %s id=%s: %s",
                event_type,
                envelope["id"],
                exc,
            )
            return EmitResult(
                emitted=False,
                event_id=envelope["id"],
                hatchet_event_id=None,
                envelope=envelope,
                reason=f"validation_failed: {exc}",
            )

        client = self._lazy_client()
        if client is None:
            return EmitResult(
                emitted=False,
                event_id=envelope["id"],
                hatchet_event_id=None,
                envelope=envelope,
                reason="hatchet_client_unavailable",
            )

        try:
            pushed = client.event.push(
                event_key=event_type,
                payload=envelope,
                additional_metadata={
                    "burns_event_id": envelope["id"],
                    "burns_source": source,
                    "burns_subject": subject,
                },
                scope=subject,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "burns-l4 hatchet push failed for %s id=%s: %s",
                event_type,
                envelope["id"],
                exc,
            )
            return EmitResult(
                emitted=False,
                event_id=envelope["id"],
                hatchet_event_id=None,
                envelope=envelope,
                reason=f"hatchet_push_failed: {exc}",
            )

        hatchet_event_id = getattr(pushed, "event_id", None)
        return EmitResult(
            emitted=True,
            event_id=envelope["id"],
            hatchet_event_id=hatchet_event_id,
            envelope=envelope,
            reason=None,
        )


# --- module-level singleton ------------------------------------------------

_emitter_singleton: BurnsEmitter | None = None


def get_emitter() -> BurnsEmitter:
    """Process-wide singleton. Tests can replace via ``reset_emitter``."""
    global _emitter_singleton
    if _emitter_singleton is None:
        _emitter_singleton = BurnsEmitter()
    return _emitter_singleton


def reset_emitter(replacement: BurnsEmitter | None = None) -> None:
    """Replace or clear the singleton — for tests."""
    global _emitter_singleton, _warned_missing_env
    _emitter_singleton = replacement
    _warned_missing_env = False

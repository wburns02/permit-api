"""Burns Layer 4 event emitter for the enrichment worker.

Design:
  - emit_permit_detected(...) is the public entry point. Calls are SAFE to make
    unconditionally. If the flag is off the function returns None immediately.
  - When the flag is on, we always try to write to event_log first. If the
    write succeeds, we attempt a Hatchet push. If Hatchet creds are missing
    we warn ONCE per process and skip the push (the event is still durably
    recorded — the substrate's poll-based bridge could re-process from the
    event_log if we ever add one).
  - Any failure inside the emitter is swallowed at the public boundary and
    logged at error/warning. The enrichment loop never sees an exception
    from us.

Connection re-use:
  - BurnsEmitter is a process-wide singleton (functools.lru_cache(maxsize=1)
    on get_emitter()). One psycopg connection lives on the singleton; we
    keep it open with autocommit so each INSERT is its own txn. On any
    OperationalError we drop the conn and reconnect on next emit.
  - Hatchet client is also lazy-init on the singleton.

Schema validation:
  - We validate the envelope against the vendored JSON Schema at
    schemas/<event_type>/v1.json. If jsonschema isn't installed we skip
    validation with a warning — never block emission on a dev dep.

ULID:
  - We inline a tiny ULID generator (48-bit ms timestamp + 80-bit random,
    Crockford base32) so this module has zero extra pip deps beyond what
    the worker already imports (psycopg, httpx). hatchet_sdk and jsonschema
    are optional and imported lazily.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
# lru_cache is used on _load_schema below.
from typing import Any

logger = logging.getLogger("burns.l4.enrichment_worker")

_SCHEMAS_ROOT = Path(__file__).resolve().parent / "schemas"
_PERMITLOOKUP_SOURCE = "permitlookup.enrichment_worker"
_PERMIT_DETECTED_TYPE = "permitlookup.permit.detected"
_TRADE_ENUM = {"septic", "electrical", "plumbing", "roofing", "hvac", "other"}

# Crockford base32 alphabet (per ULID spec).
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# Module-level one-shot warning flags. These reset only on process restart,
# which is exactly when we want to warn again about misconfigured env.
_warned_missing_hatchet_env = False
_warned_missing_burns_dsn = False


# --- env gating -------------------------------------------------------------

def _flag_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def is_enabled() -> bool:
    """Return True iff BURNS_L4_EMIT_ENABLED is truthy.

    Hatchet env vars are *not* required for is_enabled() to be True — we want
    to record events to event_log even when Hatchet is misconfigured. Hatchet
    creds are checked at push time and result in a warn-and-skip.
    """
    return _flag_truthy(os.environ.get("BURNS_L4_EMIT_ENABLED"))


# --- envelope ---------------------------------------------------------------

def _new_event_id() -> str:
    """Generate a ULID (26-char Crockford base32).

    48-bit ms-since-epoch timestamp + 80-bit cryptographic random.
    """
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = secrets.randbits(80)
    value = (ms << 80) | rand
    # 26 chars * 5 bits = 130 bits, top 2 bits always 0.
    out = []
    for i in range(25, -1, -1):
        out.append(_CROCKFORD[(value >> (i * 5)) & 0x1F])
    return "".join(out)


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


# --- helpers ---------------------------------------------------------------

def _coerce_trade(raw: str | None) -> str:
    if not raw:
        return "other"
    norm = raw.strip().lower()
    return norm if norm in _TRADE_ENUM else "other"


def _build_subject(permit_key: str | None) -> str:
    return permit_key or "permit:unknown"


# --- result -----------------------------------------------------------------

@dataclass(slots=True)
class EmitResult:
    """Outcome of a single emit() call.

    ``emitted`` is True iff either path succeeded (event_log write OR Hatchet
    push). ``logged`` is True iff the event_log INSERT committed.
    ``pushed`` is True iff Hatchet.event.push() returned without raising.
    ``reason`` carries a short string explaining why a partial/no emit happened.
    """
    emitted: bool
    event_id: str
    envelope: dict[str, Any] | None
    logged: bool = False
    pushed: bool = False
    hatchet_event_id: str | None = None
    reason: str | None = None


# --- emitter ---------------------------------------------------------------

class BurnsEmitter:
    """Process-wide event emitter. Reuses a single Postgres connection and
    a single Hatchet client.

    Construction NEVER raises. All connectivity is deferred to emit().
    """

    def __init__(
        self,
        *,
        hatchet_client: Any | None = None,
        db_writer: Any | None = None,
    ) -> None:
        # Tests inject mock clients. Production leaves both None and the
        # lazy paths pick up env on first emit.
        self._hatchet = hatchet_client
        self._hatchet_attempted = hatchet_client is not None
        self._db_writer = db_writer  # callable(envelope) -> None; tests inject
        self._db_attempted = db_writer is not None
        self._conn = None  # type: ignore[assignment]

    # --- Postgres path ---

    def _dsn(self) -> str | None:
        return os.environ.get("BURNS_EVENTS_DSN")

    def _ensure_conn(self):
        global _warned_missing_burns_dsn
        if self._conn is not None:
            return self._conn
        dsn = self._dsn()
        if not dsn:
            if not _warned_missing_burns_dsn:
                logger.error(
                    "BURNS_L4_EMIT_ENABLED=true but BURNS_EVENTS_DSN is not set; "
                    "events will be dropped until configured.",
                )
                _warned_missing_burns_dsn = True
            return None
        try:
            import psycopg  # local import; the worker also uses psycopg
            self._conn = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
        except Exception as exc:  # noqa: BLE001
            logger.error("burns-l4 BURNS_EVENTS_DSN connect failed: %s", exc)
            self._conn = None
        return self._conn

    def _write_event_log(self, envelope: dict[str, Any]) -> bool:
        """Write to event_log. Returns True if the INSERT committed.

        Uses the injected db_writer in tests; otherwise opens/reuses a
        psycopg connection from BURNS_EVENTS_DSN.
        """
        if self._db_writer is not None:
            self._db_writer(envelope)
            return True

        conn = self._ensure_conn()
        if conn is None:
            return False

        from psycopg.types.json import Jsonb
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO event_log
                      (event_id, event_type, source, subject, occurred_at, payload, links)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        envelope["id"],
                        envelope["type"],
                        envelope["source"],
                        envelope["subject"],
                        envelope["time"],
                        Jsonb(envelope),
                        Jsonb(envelope.get("links") or {}),
                    ),
                )
            return True
        except Exception as exc:  # noqa: BLE001
            # Drop the conn so the next emit reconnects.
            logger.error("burns-l4 event_log INSERT failed: %s", exc)
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None
            return False

    # --- Hatchet path ---

    def _lazy_hatchet(self) -> Any | None:
        global _warned_missing_hatchet_env
        if self._hatchet is not None or self._hatchet_attempted:
            return self._hatchet
        self._hatchet_attempted = True

        token = os.environ.get("HATCHET_CLIENT_TOKEN")
        host_port = os.environ.get("HATCHET_CLIENT_HOST_PORT")
        if not token or not host_port:
            if not _warned_missing_hatchet_env:
                logger.warning(
                    "burns-l4: Hatchet env missing (HATCHET_CLIENT_TOKEN=%s, "
                    "HATCHET_CLIENT_HOST_PORT=%s); will write event_log only, "
                    "skip Hatchet push.",
                    bool(token),
                    bool(host_port),
                )
                _warned_missing_hatchet_env = True
            return None
        try:
            from hatchet_sdk import Hatchet  # type: ignore
            self._hatchet = Hatchet()
        except Exception as exc:  # noqa: BLE001
            logger.warning("burns-l4 Hatchet client init failed: %s", exc)
            self._hatchet = None
        return self._hatchet

    def _push_hatchet(self, envelope: dict[str, Any]) -> tuple[bool, str | None]:
        client = self._lazy_hatchet()
        if client is None:
            return False, None
        try:
            pushed = client.event.push(
                event_key=envelope["type"],
                payload=envelope,
                additional_metadata={
                    "burns_event_id": envelope["id"],
                    "burns_source": envelope["source"],
                    "burns_subject": envelope["subject"],
                },
                scope=envelope["subject"],
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("burns-l4 hatchet push failed id=%s: %s", envelope["id"], exc)
            return False, None
        return True, getattr(pushed, "event_id", None)

    # --- public emit ---

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
                event_type, envelope["id"], exc,
            )
            return EmitResult(
                emitted=False,
                event_id=envelope["id"],
                envelope=envelope,
                reason=f"validation_failed: {exc}",
            )

        logged = self._write_event_log(envelope)
        pushed, hatchet_event_id = self._push_hatchet(envelope)

        emitted = logged or pushed
        reason: str | None = None
        if not emitted:
            reason = "event_log_unwritable_and_hatchet_unavailable"
        elif not pushed:
            reason = "hatchet_unavailable_event_logged_only"

        return EmitResult(
            emitted=emitted,
            event_id=envelope["id"],
            envelope=envelope,
            logged=logged,
            pushed=pushed,
            hatchet_event_id=hatchet_event_id,
            reason=reason,
        )


# --- singleton --------------------------------------------------------------

_singleton: BurnsEmitter | None = None


def get_emitter() -> BurnsEmitter:
    """Return the process-wide singleton, constructing it on first call."""
    global _singleton
    if _singleton is None:
        _singleton = BurnsEmitter()
    return _singleton


def reset_emitter(replacement: BurnsEmitter | None = None) -> None:
    """Replace or clear the singleton. Tests use this to inject mocks and
    to reset the one-shot warning flags between cases."""
    global _singleton, _warned_missing_hatchet_env, _warned_missing_burns_dsn
    _singleton = replacement
    _warned_missing_hatchet_env = False
    _warned_missing_burns_dsn = False


# --- high-level helper -----------------------------------------------------

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
) -> EmitResult | None:
    """Emit `permitlookup.permit.detected` after a successful enrichment.

    Returns None when the flag is OFF (cheapest possible no-op — no envelope
    built, no DB touched). Returns an EmitResult when enabled, even if the
    Hatchet push or event_log write failed (the result's reason explains why).

    NEVER raises. Callers can wrap in try/except as belt-and-suspenders but
    every internal exception is caught and logged here.
    """
    if not is_enabled():
        return None

    try:
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
            subject=_build_subject(permit_id),
            data=data,
            links=links,
        )
    except Exception as exc:  # noqa: BLE001
        # Last-line defence. Should be unreachable because emit() also
        # swallows everything, but keep it so a future refactor can't
        # turn an emitter bug into a worker outage.
        logger.error("burns-l4 emit_permit_detected unexpected error: %s", exc)
        return EmitResult(
            emitted=False,
            event_id="",
            envelope=None,
            reason=f"unexpected: {exc}",
        )

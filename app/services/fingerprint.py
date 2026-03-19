"""Row-level fingerprinting — makes each API key's data uniquely traceable.

Every record returned to a given API key receives subtle, deterministic
modifications: numeric rounding offsets, lat/lng micro-shifts, and zero-width
character steganography in text fields.  If leaked data appears on the web,
we can trace it back to the specific API key that received it.

Zero-width encoding scheme
--------------------------
Characters: U+200B (binary 0), U+200C (binary 1), U+200D (terminator).
The API key's seed is encoded as a binary string using these characters and
inserted after the 5th character of any qualifying text field.
"""

import hashlib
import logging
from copy import deepcopy

from app.config import settings

logger = logging.getLogger(__name__)

# Zero-width characters
ZWC_ZERO = "\u200b"      # ZERO WIDTH SPACE        → binary 0
ZWC_ONE = "\u200c"       # ZERO WIDTH NON-JOINER   → binary 1
ZWC_TERM = "\u200d"      # ZERO WIDTH JOINER       → terminator

# Fields eligible for each fingerprint type
_NUMERIC_FIELDS = {"valuation", "sale_price", "amount", "median_sale_price"}
_GEO_FIELDS = {"lat", "lng"}
_TEXT_FIELDS = {"description", "address"}


def _get_seed(api_key_id: str) -> int:
    """Deterministic seed from SHA-256 of (key_id + salt), first 8 hex chars."""
    raw = f"{api_key_id}{settings.FINGERPRINT_SALT}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return int(digest[:8], 16)


def _encode_zwc(text: str, seed: int) -> str:
    """
    Encode seed as zero-width char sequence inserted after the first 5 chars.

    If the text is shorter than 5 characters, the ZWC block is appended.
    """
    binary = bin(seed)[2:]  # strip '0b' prefix
    zwc_seq = ""
    for bit in binary:
        zwc_seq += ZWC_ONE if bit == "1" else ZWC_ZERO
    zwc_seq += ZWC_TERM

    insert_pos = min(5, len(text))
    return text[:insert_pos] + zwc_seq + text[insert_pos:]


def _decode_zwc(text: str) -> int | None:
    """
    Extract seed from zero-width chars embedded in text.

    Returns None if no valid ZWC sequence is found.
    """
    # Find ZWC characters in the text
    bits = []
    found_start = False

    for ch in text:
        if ch == ZWC_ZERO:
            found_start = True
            bits.append("0")
        elif ch == ZWC_ONE:
            found_start = True
            bits.append("1")
        elif ch == ZWC_TERM:
            if found_start and bits:
                try:
                    return int("".join(bits), 2)
                except ValueError:
                    return None
            return None

    return None


def apply_fingerprint(records: list[dict], api_key_id: str) -> list[dict]:
    """
    Apply deterministic fingerprint to every record for the given API key.

    Parameters
    ----------
    records : list[dict]
        Result rows (dicts).
    api_key_id : str
        The API key ID (UUID string).

    Returns
    -------
    list[dict] — fingerprinted copies (originals not mutated).
    """
    seed = _get_seed(api_key_id)
    offset = seed % 99
    geo_offset = (seed % 50) * 0.00001

    fingerprinted = []
    for record in records:
        row = deepcopy(record)

        # Numeric fields: round to nearest 100, add offset
        for field in _NUMERIC_FIELDS:
            if field in row and row[field] is not None:
                try:
                    val = float(row[field])
                    rounded = round(val / 100) * 100
                    row[field] = rounded + offset
                except (TypeError, ValueError):
                    pass

        # Geo fields: micro-offset
        for field in _GEO_FIELDS:
            if field in row and row[field] is not None:
                try:
                    row[field] = float(row[field]) + geo_offset
                except (TypeError, ValueError):
                    pass

        # Text fields: embed ZWC-encoded seed
        for field in _TEXT_FIELDS:
            if field in row and row[field] is not None:
                try:
                    row[field] = _encode_zwc(str(row[field]), seed)
                except Exception:
                    pass

        fingerprinted.append(row)

    return fingerprinted


def trace_fingerprint(record: dict) -> dict | None:
    """
    Given a suspected leaked record, extract the fingerprint seed.

    Checks all text fields for ZWC-encoded seeds.

    Returns
    -------
    dict with keys {"seed": int, "confidence": float} or None if not found.
    """
    seeds_found = []

    for field in _TEXT_FIELDS:
        if field in record and record[field] is not None:
            seed = _decode_zwc(str(record[field]))
            if seed is not None:
                seeds_found.append(seed)

    if not seeds_found:
        return None

    # If multiple fields have seeds, check consistency
    primary_seed = seeds_found[0]
    matching = sum(1 for s in seeds_found if s == primary_seed)
    confidence = matching / len(seeds_found)

    return {
        "seed": primary_seed,
        "confidence": confidence,
    }

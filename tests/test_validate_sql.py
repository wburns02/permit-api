"""Unit tests for the SQL safety validator in app/api/v1/analyst.py.

Regression: queries containing ALTER/DROP/UPDATE/etc. *inside string literals*
(e.g. ``ILIKE '%alteration%'``) or comments were being rejected with HTTP 422
"Query contains forbidden operations". This file pins the token-aware behavior.
"""

import pytest

from app.api.v1.analyst import _validate_sql


# --- Should be REJECTED (real DDL/DML keywords as tokens) --------------------

REJECT_CASES = [
    "ALTER TABLE foo ADD COLUMN bar text",
    "SELECT * FROM x; ALTER TABLE foo RENAME TO bar",
    "DROP TABLE foo",
    "  delete from x where id = 1",
    "WITH t AS (DELETE FROM x RETURNING *) SELECT * FROM t",
    "INSERT INTO foo VALUES (1)",
    "UPDATE foo SET x = 1 WHERE id = 2",
    "TRUNCATE TABLE foo",
    "GRANT SELECT ON foo TO public",
    "CREATE TABLE foo (id int)",
]


@pytest.mark.parametrize("sql", REJECT_CASES)
def test_validate_sql_rejects_destructive_statements(sql: str):
    with pytest.raises(ValueError):
        _validate_sql(sql)


# --- Should be ALLOWED (keywords appear only in literals / comments) ---------

ALLOW_CASES = [
    "SELECT * FROM permits WHERE description ILIKE '%alteration%'",
    "SELECT * FROM permits WHERE description ILIKE '%alter%'",
    (
        "SELECT * FROM permits WHERE description ILIKE '%dropped%' "
        "OR description ILIKE '%updated%'"
    ),
    "SELECT id FROM permits -- this query was updated by admin",
    "SELECT id FROM permits /* dropped, altered, inserted */ WHERE id = 1",
    "WITH t AS (SELECT * FROM x) SELECT * FROM t WHERE address ILIKE '%alter%'",
    "SELECT * FROM permits WHERE city = 'Cincinnati' ORDER BY issued_date DESC",
]


@pytest.mark.parametrize("sql", ALLOW_CASES)
def test_validate_sql_allows_safe_selects(sql: str):
    out = _validate_sql(sql)
    # Sanity: validator returns the cleaned SQL and always ends with a LIMIT cap.
    assert "LIMIT" in out.upper()


def test_validate_sql_caps_limit_at_50():
    out = _validate_sql("SELECT * FROM permits LIMIT 1000")
    assert "LIMIT 50" in out.upper()
    assert "LIMIT 1000" not in out.upper()


def test_validate_sql_adds_limit_when_missing():
    out = _validate_sql("SELECT * FROM permits WHERE city = 'Cincinnati'")
    assert out.upper().rstrip().endswith("LIMIT 50")

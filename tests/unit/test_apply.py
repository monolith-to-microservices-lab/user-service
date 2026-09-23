"""Unit tests for app/cdc/apply.py in isolation: pure upsert/delete logic
against an in-memory SQLite session (no Postgres, no Kafka). Semantic
validation lives here (as opposed to events.py's structural parsing) - e.g.
"a create event needs an `after` payload" is an apply-time rule, not a
parse-time one.
"""
from __future__ import annotations

import pytest

from app.cdc.apply import apply_user_event
from app.cdc.events import DebeziumUserEnvelope
from app.models import User
from tests.unit.conftest import get_user

AFTER = {"id": 1, "name": "Alice", "created_at": "2026-01-01T00:00:00Z"}


def _envelope(**kw) -> DebeziumUserEnvelope:
    return DebeziumUserEnvelope.model_validate(kw)


class TestCreate:
    def test_create_inserts_new_user(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        user = get_user(db_session, 1)
        assert user is not None
        assert user.name == "Alice"

    def test_create_missing_after_raises(self, db_session):
        with pytest.raises(ValueError, match="after"):
            apply_user_event(db_session, _envelope(after=None, op="c"))
        assert get_user(db_session, 1) is None

    def test_read_snapshot_behaves_like_create(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="r"))
        assert get_user(db_session, 1) is not None


class TestDuplicateCreate:
    def test_duplicate_create_upserts_instead_of_duplicating(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        apply_user_event(db_session, _envelope(after={**AFTER, "name": "Alice2"}, op="c"))

        rows = db_session.query(User).filter(User.id == 1).all()
        assert len(rows) == 1
        assert rows[0].name == "Alice2"


class TestUpdate:
    def test_update_existing_user_changes_fields(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        apply_user_event(
            db_session,
            _envelope(before=AFTER, after={**AFTER, "name": "Alice Updated"}, op="u"),
        )
        assert get_user(db_session, 1).name == "Alice Updated"

    def test_update_without_existing_row_upserts(self, db_session):
        """Chosen behavior for this codebase: `_upsert` (shared by c/r/u) does
        not distinguish "row already existed" - an update for a row this
        service has never seen is inserted, same as a create would be. This
        is the real, current implementation (not invented for the test) and
        is the same idempotent-convergence strategy used for out-of-order
        events (see tests/integration for the Kafka-level scenario).
        """
        apply_user_event(db_session, _envelope(before=None, after=AFTER, op="u"))
        user = get_user(db_session, 1)
        assert user is not None
        assert user.name == "Alice"

    def test_update_missing_after_raises(self, db_session):
        with pytest.raises(ValueError, match="after"):
            apply_user_event(db_session, _envelope(before=AFTER, after=None, op="u"))


class TestDelete:
    def test_delete_existing_user_removes_it(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        apply_user_event(db_session, _envelope(before=AFTER, op="d"))
        assert get_user(db_session, 1) is None

    def test_delete_nonexistent_user_is_a_safe_no_op(self, db_session):
        apply_user_event(db_session, _envelope(before=AFTER, op="d"))
        assert get_user(db_session, 1) is None  # no exception, no row

    def test_delete_missing_before_is_a_safe_no_op(self, db_session):
        """Documented, deliberate leniency: unlike create/update (which raise
        on a missing payload), delete with no `before` simply does nothing -
        there is no entity to identify and remove, and Debezium never
        actually produces a delete event without a `before` for a table with
        a replica identity, so this is a defensive no-op rather than an
        error path that could ever legitimately fire.
        """
        apply_user_event(db_session, _envelope(before=None, op="d"))


class TestUnknownOperation:
    def test_unknown_op_raises_valueerror(self, db_session):
        with pytest.raises(ValueError, match="unsupported CDC op"):
            apply_user_event(db_session, _envelope(after=AFTER, op="x"))
        # nothing was applied
        assert get_user(db_session, 1) is None


class TestIdempotency:
    def test_same_create_applied_twice_is_idempotent(self, db_session):
        env = _envelope(after=AFTER, op="c")
        apply_user_event(db_session, env)
        apply_user_event(db_session, env)
        assert db_session.query(User).filter(User.id == 1).count() == 1

    def test_same_update_applied_twice_is_idempotent(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        env = _envelope(before=AFTER, after={**AFTER, "name": "V2"}, op="u")
        apply_user_event(db_session, env)
        apply_user_event(db_session, env)
        assert get_user(db_session, 1).name == "V2"

    def test_same_delete_applied_twice_is_idempotent(self, db_session):
        apply_user_event(db_session, _envelope(after=AFTER, op="c"))
        env = _envelope(before=AFTER, op="d")
        apply_user_event(db_session, env)
        apply_user_event(db_session, env)  # must not raise
        assert get_user(db_session, 1) is None

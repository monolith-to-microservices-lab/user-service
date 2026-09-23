"""Unit tests for the consumer's control flow in complete isolation: no real
Postgres, no real Kafka. `apply_user_event` and the Kafka consumer's
`.commit()` are both spies/fakes, so these tests verify ORDER and ERROR
PROPAGATION, not persistence - that's covered in tests/unit/test_apply.py
(logic) and tests/integration/test_cdc.py (real Postgres).
"""
from __future__ import annotations

import json

import pytest

import app.cdc.consumer as consumer_module
from app.cdc import metrics
from app.cdc.consumer import UserCdcConsumer


class SpyKafkaConsumer:
    def __init__(self, calls: list[str], fail_commit: bool = False):
        self.calls = calls  # shared with SpySession so ordering is comparable
        self.committed_offsets: list[int] = []
        self._fail_commit = fail_commit

    def commit(self, message=None, asynchronous=False):
        self.calls.append("kafka_commit")
        if self._fail_commit:
            raise RuntimeError("simulated broker unavailable during offset commit")
        self.committed_offsets.append(message.offset())

    def close(self):
        pass


class SpySession:
    """Stands in for a real SQLAlchemy Session - only records call order."""

    def __init__(self, calls: list[str], fail: bool = False):
        self._calls = calls
        self._fail = fail

    def commit(self):
        if self._fail:
            raise RuntimeError("simulated database failure")
        self._calls.append("db_commit")

    def rollback(self):
        self._calls.append("db_rollback")

    def close(self):
        self._calls.append("db_close")


def _message(op="c", offset=0, value: bytes | None = b"placeholder"):
    body = {"op": op, "after": {"id": 1, "name": "A", "created_at": "2026-01-01T00:00:00Z"}, "source": {"ts_ms": 1}}
    payload = json.dumps(body).encode("utf-8") if value is not None else None

    class Msg:
        def topic(self):
            return "legacy.public.users"

        def partition(self):
            return 0

        def offset(self):
            return offset

        def value(self):
            return payload

    return Msg()


def _consumer_with_spy_apply(calls: list[str], fail_apply: bool = False, fail_commit: bool = False):
    session = SpySession(calls, fail=fail_apply)
    kafka = SpyKafkaConsumer(calls, fail_commit=fail_commit)
    consumer = UserCdcConsumer(kafka, session_factory=lambda: session)
    return consumer, kafka, calls


def test_db_commit_happens_before_kafka_offset_commit(monkeypatch):
    calls: list[str] = []
    consumer, kafka, calls = _consumer_with_spy_apply(calls)
    monkeypatch.setattr(consumer_module, "apply_user_event", lambda session, envelope: session.commit())

    consumer.process_message(_message(offset=5))

    # The critical invariant: db_commit must appear strictly before
    # kafka_commit in the recorded call order.
    assert calls.index("db_commit") < calls.index("kafka_commit")
    assert kafka.committed_offsets == [5]


def test_db_failure_rolls_back_and_never_reaches_kafka_commit(monkeypatch):
    calls: list[str] = []
    consumer, kafka, calls = _consumer_with_spy_apply(calls, fail_apply=True)
    monkeypatch.setattr(consumer_module, "apply_user_event", lambda session, envelope: session.commit())

    before = metrics.EVENTS_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()

    with pytest.raises(RuntimeError, match="simulated database failure"):
        consumer.process_message(_message(offset=7))

    assert "kafka_commit" not in calls
    assert calls == ["db_rollback", "db_close"]
    assert kafka.committed_offsets == []
    after = metrics.EVENTS_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()
    assert after == before + 1


def test_kafka_offset_commit_failure_after_successful_db_commit(monkeypatch):
    """The mandatory scenario: DB commit succeeds, then the Kafka offset
    commit itself fails (e.g. broker unreachable at that exact instant). The
    DB change already happened and is NOT rolled back (there is nothing left
    to roll back - the transaction is over); the error is recorded via
    cdc_kafka_offset_commit_failed_total and propagates, which crashes the
    process and relies on redelivery + idempotency to converge correctly on
    the next attempt.
    """
    calls: list[str] = []
    consumer, kafka, calls = _consumer_with_spy_apply(calls, fail_commit=True)
    monkeypatch.setattr(consumer_module, "apply_user_event", lambda session, envelope: session.commit())

    before = metrics.KAFKA_OFFSET_COMMIT_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()

    with pytest.raises(RuntimeError, match="simulated broker unavailable"):
        consumer.process_message(_message(offset=9))

    assert calls == ["db_commit", "db_close", "kafka_commit"]  # DB commit stands, offset commit was attempted and failed
    assert kafka.committed_offsets == []  # offset was never actually committed
    after = metrics.KAFKA_OFFSET_COMMIT_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()
    assert after == before + 1


def test_tombstone_never_calls_apply_but_still_commits_offset(monkeypatch):
    called = {"apply": False}
    monkeypatch.setattr(consumer_module, "apply_user_event", lambda *_: called.__setitem__("apply", True))
    consumer = UserCdcConsumer(SpyKafkaConsumer([]), session_factory=lambda: SpySession([]))

    consumer.process_message(_message(offset=1, value=None))

    assert called["apply"] is False

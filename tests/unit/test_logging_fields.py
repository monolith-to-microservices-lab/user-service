"""Confirms the CDC consumer's log calls carry the right structured fields
(topic/partition/offset/op/user_id) via record attributes - never by
matching a formatted log string.

Uses a handler attached directly to the "user_service.cdc" logger rather
than pytest's `caplog` (which captures via the root logger): importing
app.main elsewhere in the suite calls `configure_logging()`, which does
`root.handlers = [...]` - replacing the root handler set would silently
break caplog's own root handler for the rest of the session if these tests
relied on it.
"""

from __future__ import annotations

import contextlib
import json
import logging

import app.cdc.consumer as consumer_module
from app.cdc.consumer import UserCdcConsumer


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def _capture(logger_name: str, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    handler = _ListHandler()
    handler.setLevel(level)
    prev_level, prev_propagate = logger.level, logger.propagate
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)
        logger.propagate = prev_propagate


class _FakeKafkaConsumer:
    def __init__(self):
        self.committed_offsets = []

    def commit(self, message=None, asynchronous=False):
        self.committed_offsets.append(message.offset())

    def close(self):
        pass


class _FakeSession:
    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _message(op="c", offset=3, partition=0, value=b"x"):
    body = {
        "op": op,
        "after": {"id": 42, "name": "A", "created_at": "2026-01-01T00:00:00Z"},
        "source": {"ts_ms": 1},
    }
    payload = json.dumps(body).encode("utf-8") if value is not None else None

    class Msg:
        def topic(self):
            return "legacy.public.users"

        def partition(self):
            return partition

        def offset(self):
            return offset

        def value(self):
            return payload

    return Msg()


def test_applied_log_carries_topic_partition_offset_op_entity_id(monkeypatch):
    monkeypatch.setattr(
        consumer_module, "apply_user_event", lambda session, envelope: session.commit()
    )
    consumer = UserCdcConsumer(_FakeKafkaConsumer(), session_factory=lambda: _FakeSession())

    with _capture("user_service.cdc") as handler:
        consumer.process_message(_message(offset=17, partition=2))

    applied = [r for r in handler.records if r.getMessage() == "cdc.applied"]
    assert len(applied) == 1
    record = applied[0]
    assert record.topic == "legacy.public.users"
    assert record.partition == 2
    assert record.offset == 17
    assert record.op == "c"
    assert record.user_id == 42


def test_tombstone_log_carries_topic_partition_offset():
    consumer = UserCdcConsumer(_FakeKafkaConsumer(), session_factory=lambda: _FakeSession())

    with _capture("user_service.cdc") as handler:
        consumer.process_message(_message(offset=20, partition=1, value=None))

    tombstones = [r for r in handler.records if r.getMessage() == "cdc.tombstone_skipped"]
    assert len(tombstones) == 1
    assert tombstones[0].topic == "legacy.public.users"
    assert tombstones[0].partition == 1
    assert tombstones[0].offset == 20


def test_apply_failed_log_carries_context_and_is_not_silent(monkeypatch):
    def _boom(session, envelope):
        raise RuntimeError("boom")

    monkeypatch.setattr(consumer_module, "apply_user_event", _boom)
    consumer = UserCdcConsumer(_FakeKafkaConsumer(), session_factory=lambda: _FakeSession())

    with _capture("user_service.cdc") as handler:
        try:
            consumer.process_message(_message(offset=5))
        except RuntimeError:
            pass

    failed = [r for r in handler.records if r.getMessage() == "cdc.apply_failed"]
    assert len(failed) == 1
    assert failed[0].levelname == "ERROR"
    assert failed[0].offset == 5
    assert failed[0].op == "c"
    assert failed[0].user_id == 42

"""Integration tests against the REAL Kafka broker from cdc-infrastructure
(localhost:9092) and the real `user_service_test` Postgres. Every test uses
its own uniquely-named topic (`test.legacy.public.users.<uuid>`) and consumer
group, so tests never interfere with each other or with the real
`legacy.public.users` / `user-service-cdc` pipeline. Kafka's
KAFKA_AUTO_CREATE_TOPICS_ENABLE=true (see cdc-infrastructure) means these
topics are created on first produce - nothing to clean up on the broker
afterward (they're throwaway, uniquely named, and cost nothing sitting idle).

This is the layer that proves real integration: no FakeKafkaConsumer here.
"""
from __future__ import annotations

import json
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from sqlalchemy import select

from app.cdc.consumer import UserCdcConsumer
from app.cdc.events import DebeziumUserEnvelope
from app.database import SessionLocal
from app.models import User

pytestmark = pytest.mark.slow

KAFKA_BOOTSTRAP = "localhost:9092"


def _unique_topic() -> str:
    return f"test.legacy.public.users.{uuid.uuid4().hex[:8]}"


def _unique_group() -> str:
    return f"test-user-cdc-{uuid.uuid4().hex[:8]}"


def _envelope_bytes(op, before=None, after=None, source_ts_ms=None) -> bytes | None:
    if op is None:  # tombstone
        return None
    body = {
        "before": before,
        "after": after,
        "op": op,
        "source": {"ts_ms": source_ts_ms or int(time.time() * 1000)},
        "ts_ms": int(time.time() * 1000),
    }
    return json.dumps(body).encode("utf-8")


@pytest.fixture
def producer():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    yield p
    p.flush(10)


def _produce(producer, topic, entity_id, op, **kw):
    producer.produce(topic, key=str(entity_id).encode(), value=_envelope_bytes(op, **kw))
    producer.flush(10)


def _make_consumer(topic: str, group: str, session_factory=SessionLocal) -> tuple[UserCdcConsumer, Consumer]:
    raw = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    wrapper = UserCdcConsumer(raw, session_factory=session_factory, topic=topic)
    wrapper.subscribe()
    return wrapper, raw


def _drain_one(wrapper: UserCdcConsumer, raw: Consumer, timeout: float = 15.0):
    """Poll until exactly one real message arrives, then hand it to
    process_message. Returns the raw message (so a test can replay it for a
    duplicate-delivery scenario) or raises TimeoutError.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = raw.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        wrapper.process_message(msg)
        return msg
    raise TimeoutError(f"no message arrived within {timeout}s on {wrapper._topic}")


def _get_user(session, user_id):
    session.expire_all()
    return session.get(User, user_id)


def _payload(id_, name="Real Kafka Test", created_at="2026-01-01T00:00:00Z"):
    return {"id": id_, "name": name, "created_at": created_at}


class TestRealKafkaCreateUpdateDeleteTombstone:
    def test_full_cycle_through_a_real_broker(self, producer, db_session):
        topic, group = _unique_topic(), _unique_group()
        wrapper, raw = _make_consumer(topic, group)
        entity_id = 90001
        try:
            _produce(producer, topic, entity_id, "c", after=_payload(entity_id, "Created"))
            _drain_one(wrapper, raw)
            assert _get_user(db_session, entity_id).name == "Created"

            _produce(producer, topic, entity_id, "u", before=_payload(entity_id, "Created"),
                      after=_payload(entity_id, "Updated"))
            _drain_one(wrapper, raw)
            assert _get_user(db_session, entity_id).name == "Updated"

            _produce(producer, topic, entity_id, "d", before=_payload(entity_id, "Updated"))
            _drain_one(wrapper, raw)
            assert _get_user(db_session, entity_id) is None

            # tombstone: null value, same key
            producer.produce(topic, key=str(entity_id).encode(), value=None)
            producer.flush(10)
            _drain_one(wrapper, raw)  # must not raise, no DB effect
            assert _get_user(db_session, entity_id) is None
        finally:
            raw.close()


class TestDuplicateDelivery:
    def test_same_message_processed_twice_does_not_duplicate(self, producer, db_session):
        topic, group = _unique_topic(), _unique_group()
        wrapper, raw = _make_consumer(topic, group)
        entity_id = 90002
        try:
            _produce(producer, topic, entity_id, "c", after=_payload(entity_id))
            msg = _drain_one(wrapper, raw)

            wrapper.process_message(msg)  # replay the exact same real Kafka message

            rows = db_session.scalars(select(User).where(User.id == entity_id)).all()
            assert len(rows) == 1
        finally:
            raw.close()


class TestConsumerRestart:
    def test_new_consumer_instance_same_group_continues_from_committed_offset(self, producer, db_session):
        topic, group = _unique_topic(), _unique_group()
        wrapper1, raw1 = _make_consumer(topic, group)
        e1, e2 = 90003, 90004
        try:
            _produce(producer, topic, e1, "c", after=_payload(e1, "First"))
            _drain_one(wrapper1, raw1)
        finally:
            raw1.close()  # simulates the process stopping after committing e1's offset

        wrapper2, raw2 = _make_consumer(topic, group)  # "restart": fresh instance, same group
        try:
            _produce(producer, topic, e2, "c", after=_payload(e2, "Second"))
            _drain_one(wrapper2, raw2)
            # e1 must NOT be redelivered (its offset was committed by wrapper1)
            assert _get_user(db_session, e1).name == "First"
            assert _get_user(db_session, e2).name == "Second"
        finally:
            raw2.close()


class TestCrashBeforeDbCommit:
    def test_redelivery_after_apply_failure_ends_in_exactly_one_correct_row(self, producer, db_session, monkeypatch):
        topic, group = _unique_topic(), _unique_group()
        entity_id = 90005

        import app.cdc.consumer as consumer_module

        real_apply = consumer_module.apply_user_event
        monkeypatch.setattr(
            consumer_module, "apply_user_event",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("simulated crash before db commit")),
        )
        wrapper1, raw1 = _make_consumer(topic, group)
        try:
            _produce(producer, topic, entity_id, "c", after=_payload(entity_id, "Crashed"))
            with pytest.raises(RuntimeError, match="simulated crash"):
                _drain_one(wrapper1, raw1)
            assert _get_user(db_session, entity_id) is None  # nothing applied
        finally:
            raw1.close()

        monkeypatch.setattr(consumer_module, "apply_user_event", real_apply)
        wrapper2, raw2 = _make_consumer(topic, group)  # "restart" without the fault
        try:
            _drain_one(wrapper2, raw2)  # redelivered: offset was never committed
            assert _get_user(db_session, entity_id).name == "Crashed"
        finally:
            raw2.close()


class _FlakyCommitConsumer:
    """Thin delegating wrapper around a real confluent_kafka.Consumer - the
    Cython extension type's attributes can't be monkeypatched directly, so
    this wrapper lets `.commit()` fail on demand while `.poll()`/`.close()`
    still hit the real broker for real.
    """

    def __init__(self, real_consumer, fail_times: int = 1):
        self._real = real_consumer
        self._fail_times = fail_times

    def subscribe(self, *a, **kw):
        return self._real.subscribe(*a, **kw)

    def poll(self, *a, **kw):
        return self._real.poll(*a, **kw)

    def commit(self, *a, **kw):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("simulated broker unavailable at offset-commit time")
        return self._real.commit(*a, **kw)

    def close(self):
        return self._real.close()


class TestCrashAfterDbCommitBeforeOffsetCommit:
    """The critical scenario: the DB write is real and already committed, but
    the Kafka offset commit itself fails/never happens - simulated via
    _FlakyCommitConsumer.commit(). On restart, Kafka redelivers the same
    message; idempotent upsert means the end state is still exactly one
    correct row, never a duplicate or corrupted one.
    """

    def test_offset_not_committed_then_redelivered_converges_correctly(self, producer, db_session):
        topic, group = _unique_topic(), _unique_group()
        entity_id = 90006

        real_raw1 = Consumer(
            {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group,
             "auto.offset.reset": "earliest", "enable.auto.commit": False}
        )
        flaky1 = _FlakyCommitConsumer(real_raw1, fail_times=1)
        wrapper1 = UserCdcConsumer(flaky1, session_factory=SessionLocal, topic=topic)
        wrapper1.subscribe()
        try:
            _produce(producer, topic, entity_id, "c", after=_payload(entity_id, "Committed"))
            with pytest.raises(RuntimeError, match="simulated broker unavailable"):
                _drain_one(wrapper1, flaky1)
            # the DB write is real and already there, despite the offset-commit failure
            assert _get_user(db_session, entity_id).name == "Committed"
        finally:
            real_raw1.close()

        wrapper2, raw2 = _make_consumer(topic, group)  # "restart": offset still uncommitted
        try:
            _drain_one(wrapper2, raw2)  # redelivered and safely re-applied (idempotent upsert)
            rows = db_session.scalars(select(User).where(User.id == entity_id)).all()
            assert len(rows) == 1
            assert rows[0].name == "Committed"
        finally:
            raw2.close()


class TestOutOfOrder:
    def test_update_before_create_still_converges(self, producer, db_session):
        topic, group = _unique_topic(), _unique_group()
        wrapper, raw = _make_consumer(topic, group)
        entity_id = 90007
        try:
            _produce(producer, topic, entity_id, "u", before=_payload(entity_id, "Never-existed"),
                      after=_payload(entity_id, "Updated-First"))
            _drain_one(wrapper, raw)
            assert _get_user(db_session, entity_id).name == "Updated-First"

            _produce(producer, topic, entity_id, "c", after=_payload(entity_id, "Created-Second"))
            _drain_one(wrapper, raw)
            assert _get_user(db_session, entity_id).name == "Created-Second"
        finally:
            raw.close()


class TestDestinationDatabaseDown:
    def test_broken_session_factory_fails_without_advancing_offset(self, producer):
        """Simulates the destination DB being unreachable: the session
        factory itself raises (as a real broken connection pool would).
        Offset must not advance; the message stays available for redelivery.
        """
        topic, group = _unique_topic(), _unique_group()

        def _broken_session_factory():
            raise RuntimeError("simulated destination database down")

        raw = Consumer(
            {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group,
             "auto.offset.reset": "earliest", "enable.auto.commit": False}
        )
        wrapper = UserCdcConsumer(raw, session_factory=_broken_session_factory, topic=topic)
        wrapper.subscribe()
        try:
            _produce(producer, topic, 90008, "c", after=_payload(90008))
            with pytest.raises(RuntimeError, match="database down"):
                _drain_one(wrapper, raw)
        finally:
            raw.close()


class TestObservabilityAfterRealPublish:
    def test_metrics_increment_after_a_real_produce_consume_cycle(self, producer, db_session):
        from app.cdc import metrics

        topic, group = _unique_topic(), _unique_group()
        wrapper, raw = _make_consumer(topic, group)
        entity_id = 90009
        before = metrics.EVENTS_PROCESSED_TOTAL.labels(service=metrics.SERVICE_NAME, operation="c")._value.get()
        try:
            _produce(producer, topic, entity_id, "c", after=_payload(entity_id))
            _drain_one(wrapper, raw)
        finally:
            raw.close()

        after = metrics.EVENTS_PROCESSED_TOTAL.labels(service=metrics.SERVICE_NAME, operation="c")._value.get()
        assert after == before + 1

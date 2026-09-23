"""Tests for the CDC consumer (app/cdc). No Kafka broker involved: Kafka
messages and the consumer client are faked in-process, and events are applied
to the same real test PostgreSQL the rest of the suite uses (see conftest.py).
"""

import json

import pytest
from sqlalchemy import select

import app.cdc.consumer as consumer_module
from app.cdc.consumer import UserCdcConsumer
from app.database import SessionLocal
from app.models import User


class FakeMessage:
    def __init__(self, value: bytes | None, *, topic="legacy.public.users", partition=0, offset=0):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return None


class FakeKafkaConsumer:
    """Minimal double for confluent_kafka.Consumer: records committed offsets."""

    def __init__(self):
        self.committed_offsets: list[int] = []

    def subscribe(self, topics):
        pass

    def commit(self, message=None, asynchronous=False):
        self.committed_offsets.append(message.offset())

    def close(self):
        pass


def _user_row(id: int, name: str, created_at: str = "2026-01-01T00:00:00Z") -> dict:
    return {"id": id, "name": name, "created_at": created_at}


def _message(op: str, offset: int, *, before: dict | None = None, after: dict | None = None) -> FakeMessage:
    body = {"op": op, "before": before, "after": after, "source": {"table": "users"}, "ts_ms": 1}
    return FakeMessage(json.dumps(body).encode("utf-8"), offset=offset)


@pytest.fixture
def db_session():
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def cdc_consumer():
    fake = FakeKafkaConsumer()
    return UserCdcConsumer(fake, session_factory=SessionLocal), fake


def _get(session, user_id: int) -> User | None:
    session.expire_all()
    return session.scalar(select(User).where(User.id == user_id))


def test_create_event_inserts_user(cdc_consumer, db_session):
    consumer, fake = cdc_consumer
    consumer.process_message(_message("c", 0, after=_user_row(601, "Alice")))

    user = _get(db_session, 601)
    assert user is not None
    assert user.name == "Alice"
    assert fake.committed_offsets == [0]


def test_update_event_updates_existing_user(cdc_consumer, db_session):
    consumer, fake = cdc_consumer
    consumer.process_message(_message("c", 0, after=_user_row(602, "Bob")))
    consumer.process_message(
        _message("u", 1, before=_user_row(602, "Bob"), after=_user_row(602, "Bobby"))
    )

    user = _get(db_session, 602)
    assert user.name == "Bobby"
    assert fake.committed_offsets == [0, 1]


def test_delete_event_removes_user(cdc_consumer, db_session):
    consumer, fake = cdc_consumer
    consumer.process_message(_message("c", 0, after=_user_row(603, "Carol")))
    consumer.process_message(_message("d", 1, before=_user_row(603, "Carol")))

    assert _get(db_session, 603) is None
    assert fake.committed_offsets == [0, 1]


def test_reprocessing_same_message_is_idempotent(cdc_consumer, db_session):
    """Redelivery of the exact same message (e.g. consumer crashed right after
    the DB commit but before the Kafka offset commit landed) must not create a
    duplicate row.
    """
    consumer, fake = cdc_consumer
    msg = _message("c", 0, after=_user_row(604, "Dana"))

    consumer.process_message(msg)
    consumer.process_message(msg)

    rows = db_session.scalars(select(User).where(User.id == 604)).all()
    assert len(rows) == 1
    assert rows[0].name == "Dana"


def test_db_error_is_not_committed_and_offset_is_not_advanced(cdc_consumer, db_session, monkeypatch):
    consumer, fake = cdc_consumer

    def _boom(session, envelope):
        raise RuntimeError("simulated database failure")

    monkeypatch.setattr(consumer_module, "apply_user_event", _boom)

    msg = _message("c", 0, after=_user_row(605, "Frank"))
    with pytest.raises(RuntimeError):
        consumer.process_message(msg)

    assert fake.committed_offsets == []
    assert _get(db_session, 605) is None


def test_restart_redelivery_does_not_duplicate_user(db_session):
    """Simulates a consumer restart: a brand new UserCdcConsumer instance
    (fresh Kafka client, same database) reprocesses a message whose offset was
    never committed by the previous run. End state must still be one row.
    """
    msg = _message("c", 0, after=_user_row(606, "Grace"))

    first_run = UserCdcConsumer(FakeKafkaConsumer(), session_factory=SessionLocal)
    first_run.process_message(msg)

    second_run = UserCdcConsumer(FakeKafkaConsumer(), session_factory=SessionLocal)
    second_run.process_message(msg)

    rows = db_session.scalars(select(User).where(User.id == 606)).all()
    assert len(rows) == 1
    assert rows[0].name == "Grace"


def test_tombstone_is_skipped_but_offset_is_committed(cdc_consumer, db_session):
    consumer, fake = cdc_consumer
    consumer.process_message(_message("c", 0, after=_user_row(607, "Heidi")))
    tombstone = FakeMessage(None, offset=1)
    consumer.process_message(tombstone)

    assert fake.committed_offsets == [0, 1]
    # tombstone carries no operation - the row from the prior create is untouched.
    assert _get(db_session, 607) is not None

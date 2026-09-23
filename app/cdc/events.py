"""Parsing for Debezium change events on `legacy.public.users`.

The connector runs with `schemas.enable=false` and no unwrap SMT, so a Kafka
value is exactly the raw Debezium envelope: `{before, after, source, op,
ts_ms, transaction}`. Only the fields this service needs are modeled; the
rest are ignored.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class UserCdcPayload(BaseModel):
    """Shape of `before` / `after` for the `users` table."""

    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    created_at: datetime


class DebeziumSource(BaseModel):
    """The subset of Debezium's `source` block used for CDC observability.

    `ts_ms` here is the timestamp of the transaction commit in the *source*
    Postgres database (from the WAL record) - the most accurate "when did
    this change really happen" available, used to compute end-to-end CDC
    latency. This is distinct from the envelope's top-level `ts_ms`, which is
    when Kafka Connect processed the record.
    """

    model_config = ConfigDict(extra="ignore")

    ts_ms: int | None = None


class DebeziumUserEnvelope(BaseModel):
    """A single Kafka value from `legacy.public.users`.

    `op`: "c" (create), "r" (snapshot read - never produced by this pipeline),
    "u" (update), "d" (delete). Deletes are followed by a separate tombstone
    message (Kafka value = null), which never reaches this model - the
    consumer handles that case before parsing.
    """

    model_config = ConfigDict(extra="ignore")

    op: str
    before: UserCdcPayload | None = None
    after: UserCdcPayload | None = None
    source: DebeziumSource | None = None
    ts_ms: int | None = None

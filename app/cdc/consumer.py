"""Standalone CDC consumer: `legacy.public.users` (Kafka/Debezium) -> this
service's own database.

Runs as a separate process from the FastAPI server (`python -m app.cdc`) and
never touches the HTTP routes. The monolith stays the source of truth; this
only keeps the User Service's copy in sync.

Offset handling: `enable.auto.commit=False`. The Kafka offset for a message is
committed only after the database transaction for that message has committed
successfully (see `apply_user_event`). If the database write fails, the
offset is not committed and the exception propagates, stopping the process -
on restart, Kafka redelivers from the last committed offset. Applying is
idempotent, so redelivery never duplicates data.
"""

import json
import logging
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError
from sqlalchemy.orm import Session

from ..config import settings
from ..database import SessionLocal
from . import metrics
from .apply import apply_user_event
from .events import DebeziumUserEnvelope

logger = logging.getLogger("user_service.cdc")

try:
    from opentelemetry import trace

    _tracer: trace.Tracer | None = trace.get_tracer("user_service.cdc")
except ImportError:  # pragma: no cover - otel is an optional dependency
    _tracer = None


class _NullSpan:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def set_attribute(self, *a, **k):
        pass

    def record_exception(self, *a, **k):
        pass

    def set_status(self, *a, **k):
        pass


def _start_span(name: str, attributes: dict):
    if _tracer is None:
        return _NullSpan()
    span = _tracer.start_as_current_span(name)
    return span


def build_kafka_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
        }
    )


class UserCdcConsumer:
    def __init__(
        self,
        consumer,
        session_factory: Callable[[], Session] = SessionLocal,
        topic: str | None = None,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._topic = topic or settings.kafka_users_topic
        # Tracks the highest offset seen per partition, purely for the
        # cdc_events_retried_total metric (detecting redelivery/replay).
        # Never used for correctness - Kafka's committed offset is still the
        # only source of truth for where to resume.
        self._max_offset_seen: dict[int, int] = {}

    def subscribe(self) -> None:
        self._consumer.subscribe([self._topic])

    def process_message(self, message) -> None:
        """Apply one Kafka message. Raises if the database write fails; the
        Kafka offset is committed only when it succeeds.
        """
        metrics.EVENTS_RECEIVED_TOTAL.labels(service=metrics.SERVICE_NAME).inc()
        process_start = time.perf_counter()

        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        log_base = {"topic": topic, "partition": partition, "offset": offset}

        previous_max = self._max_offset_seen.get(partition, -1)
        if offset <= previous_max:
            metrics.EVENTS_RETRIED_TOTAL.labels(service=metrics.SERVICE_NAME).inc()
        self._max_offset_seen[partition] = max(previous_max, offset)

        raw_value = message.value()

        if raw_value is None:
            # Delete tombstone (log-compaction marker): no operation to apply.
            logger.info("cdc.tombstone_skipped", extra=log_base)
            metrics.EVENTS_PROCESSED_TOTAL.labels(
                service=metrics.SERVICE_NAME, operation="tombstone"
            ).inc()
            self._commit_offset(message)
            metrics.LAST_EVENT_TIMESTAMP.labels(service=metrics.SERVICE_NAME).set(time.time())
            metrics.LAST_EVENT_OFFSET.labels(
                service=metrics.SERVICE_NAME, topic=topic, partition=str(partition)
            ).set(offset)
            metrics.EVENT_PROCESSING_DURATION.labels(service=metrics.SERVICE_NAME).observe(
                time.perf_counter() - process_start
            )
            return

        envelope = DebeziumUserEnvelope.model_validate(json.loads(raw_value))
        payload = envelope.after or envelope.before
        entity_id = payload.id if payload else None
        log_extra = {**log_base, "op": envelope.op, "user_id": entity_id}

        with _start_span("cdc.process_message", {}) as span:
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.destination.name", topic)
            span.set_attribute("messaging.kafka.partition", partition)
            span.set_attribute("messaging.kafka.offset", offset)
            span.set_attribute("cdc.operation", envelope.op)
            if entity_id is not None:
                span.set_attribute("cdc.entity_id", entity_id)

            session = self._session_factory()
            db_start = time.perf_counter()
            try:
                apply_user_event(session, envelope)
            except Exception as exc:
                session.rollback()
                metrics.EVENTS_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME).inc()
                span.record_exception(exc)
                logger.exception("cdc.apply_failed", extra=log_extra)
                raise
            finally:
                session.close()
            db_duration = time.perf_counter() - db_start
            metrics.DB_COMMIT_DURATION.labels(service=metrics.SERVICE_NAME).observe(db_duration)

        source_ts_ms = envelope.source.ts_ms if envelope.source else envelope.ts_ms
        metrics.record_end_to_end_latency(source_ts_ms)

        metrics.EVENTS_PROCESSED_TOTAL.labels(
            service=metrics.SERVICE_NAME, operation=envelope.op
        ).inc()
        logger.info("cdc.applied", extra=log_extra)
        self._commit_offset(message)
        metrics.LAST_EVENT_TIMESTAMP.labels(service=metrics.SERVICE_NAME).set(time.time())
        metrics.LAST_EVENT_OFFSET.labels(
            service=metrics.SERVICE_NAME, topic=topic, partition=str(partition)
        ).set(offset)
        metrics.EVENT_PROCESSING_DURATION.labels(service=metrics.SERVICE_NAME).observe(
            time.perf_counter() - process_start
        )

    def _commit_offset(self, message) -> None:
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except Exception:
            metrics.KAFKA_OFFSET_COMMIT_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME).inc()
            raise
        else:
            metrics.KAFKA_OFFSET_COMMIT_TOTAL.labels(service=metrics.SERVICE_NAME).inc()

    def run_forever(self, poll_timeout: float = 1.0) -> None:
        self.subscribe()
        logger.info(
            "cdc.consumer.started",
            extra={"topic": self._topic, "group": settings.kafka_consumer_group},
        )
        try:
            while True:
                message = self._consumer.poll(poll_timeout)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("cdc.kafka_error", extra={"error": str(message.error())})
                    continue
                self.process_message(message)
        finally:
            self._consumer.close()

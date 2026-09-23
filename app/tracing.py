"""Shared OpenTelemetry bootstrap for both the FastAPI process and the CDC
consumer process. Best-effort: if the otel packages aren't installed or the
collector is unreachable, this silently no-ops - it never blocks startup or
breaks a request/message.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger("user_service.tracing")


def setup_tracing(service_name: str, fastapi_app=None, engine=None):
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        from opentelemetry.sdk.resources import SERVICE_NAME as OTEL_SERVICE_NAME
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("tracing.dependencies_missing")
        return None

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    resource = Resource.create({OTEL_SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    # Injects trace_id/span_id into every LogRecord (read by JsonFormatter).
    LoggingInstrumentor().instrument(set_logging_format=False)

    if fastapi_app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(fastapi_app)
    if engine is not None:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        SQLAlchemyInstrumentor().instrument(engine=engine)

    logger.info("tracing.enabled", extra={"otlp_endpoint": endpoint, "service": service_name})
    return trace.get_tracer(service_name)

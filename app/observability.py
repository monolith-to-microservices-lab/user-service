"""Prometheus HTTP metrics + /metrics endpoint for the user-service FastAPI
app. Kept separate from the existing RequestContextMiddleware (correlation id)
so that middleware's tested behavior is untouched.

Label discipline: method / route (path template) / status_code only. Never
user_id or any other high-cardinality value as a label.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import event
from starlette.middleware.base import BaseHTTPMiddleware

SERVICE_NAME = "user-service"

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total", "Total HTTP requests", ["service", "method", "route", "status_code"]
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["service", "method", "route"],
)
HTTP_REQUESTS_IN_FLIGHT = Gauge(
    "http_requests_in_flight", "HTTP requests currently being processed", ["service"]
)
DB_OPERATION_DURATION = Histogram(
    "db_operation_duration_seconds", "Duration of individual SQL statements", ["service"]
)
DB_ERRORS_TOTAL = Counter("db_errors_total", "Total database errors", ["service"])


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        HTTP_REQUESTS_IN_FLIGHT.labels(service=SERVICE_NAME).inc()
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration = time.perf_counter() - start
            route = request.scope.get("route")
            route_path = (
                route.path if route is not None and hasattr(route, "path") else request.url.path
            )
            HTTP_REQUESTS_TOTAL.labels(
                service=SERVICE_NAME,
                method=request.method,
                route=route_path,
                status_code=str(status_code),
            ).inc()
            HTTP_REQUEST_DURATION.labels(
                service=SERVICE_NAME, method=request.method, route=route_path
            ).observe(duration)
            HTTP_REQUESTS_IN_FLIGHT.labels(service=SERVICE_NAME).dec()


def metrics_endpoint() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def instrument_app(app: FastAPI) -> None:
    app.add_middleware(MetricsMiddleware)
    app.add_api_route("/metrics", metrics_endpoint, methods=["GET"], include_in_schema=False)


def instrument_db_metrics(engine) -> None:
    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._observability_start = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start = getattr(context, "_observability_start", None)
        if start is not None:
            DB_OPERATION_DURATION.labels(service=SERVICE_NAME).observe(time.perf_counter() - start)

    @event.listens_for(engine, "handle_error")
    def _handle_error(exception_context):
        DB_ERRORS_TOTAL.labels(service=SERVICE_NAME).inc()

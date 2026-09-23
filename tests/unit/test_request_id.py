"""Unit test for the X-Request-ID correlation middleware in isolation - a
minimal Starlette app wrapping only RequestContextMiddleware, no database, no
full FastAPI app wiring (that's covered at the integration level by
tests/integration/test_users.py::test_request_id_is_echoed against the real
app).
"""

from __future__ import annotations

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from app.main import REQUEST_ID_HEADER, RequestContextMiddleware


def _make_app():
    async def echo(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/ping", echo)])
    app.add_middleware(RequestContextMiddleware)
    return app


def test_request_with_existing_id_preserves_it():
    client = TestClient(_make_app())
    r = client.get("/ping", headers={REQUEST_ID_HEADER: "caller-supplied-id"})
    assert r.headers[REQUEST_ID_HEADER] == "caller-supplied-id"


def test_request_without_id_generates_a_new_one():
    client = TestClient(_make_app())
    r = client.get("/ping")
    generated = r.headers.get(REQUEST_ID_HEADER)
    assert generated is not None
    assert len(generated) > 0


def test_two_requests_without_id_get_different_ids():
    client = TestClient(_make_app())
    r1 = client.get("/ping")
    r2 = client.get("/ping")
    assert r1.headers[REQUEST_ID_HEADER] != r2.headers[REQUEST_ID_HEADER]

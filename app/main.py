import logging
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .database import engine
from .errors import ImportConflictError, UserNotFoundError
from .logging_config import request_id_ctx, setup_logging
from .observability import instrument_app, instrument_db_metrics
from .routes import health, internal, users
from .tracing import setup_tracing

setup_logging(settings.log_level)
logger = logging.getLogger("user_service")

REQUEST_ID_HEADER = "X-Request-ID"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("service.startup", extra={"env": settings.app_env})
    yield
    # Graceful shutdown: stop accepting the pool's connections cleanly.
    engine.dispose()
    logger.info("service.shutdown")


app = FastAPI(
    title="user-service",
    version="0.1.0",
    summary="User domain microservice (Strangler Fig migration - step 1)",
    lifespan=lifespan,
)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns/propagates a correlation id and logs one line per request."""

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
        token = request_id_ctx.set(rid)
        logger.info(
            "request.start",
            extra={"method": request.method, "path": request.url.path},
        )
        try:
            response = await call_next(request)
            response.headers[REQUEST_ID_HEADER] = rid
            logger.info(
                "request.end",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                },
            )
            return response
        finally:
            request_id_ctx.reset(token)


app.add_middleware(RequestContextMiddleware)
instrument_app(app)
instrument_db_metrics(engine)
setup_tracing("user-service", fastapi_app=app, engine=engine)


def _error(status_code: int, code: str, message: str, **extra) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "request_id": request_id_ctx.get(), **extra}}
    return JSONResponse(status_code=status_code, content=body)


@app.exception_handler(UserNotFoundError)
async def _not_found_handler(request: Request, exc: UserNotFoundError):
    return _error(404, "not_found", f"User {exc.user_id} not found")


@app.exception_handler(ImportConflictError)
async def _import_conflict_handler(request: Request, exc: ImportConflictError):
    return _error(
        409,
        "import_conflict",
        f"User {exc.user_id} already exists with conflicting data",
        conflicts=exc.conflicts,
    )


@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError):
    return _error(
        422,
        "validation_error",
        "Request validation failed",
        details=jsonable_encoder(exc.errors()),
    )


@app.exception_handler(Exception)
async def _unhandled_handler(request: Request, exc: Exception):
    logger.exception("request.unhandled_error")
    return _error(500, "internal_error", "Internal server error")


app.include_router(health.router)
app.include_router(users.router)
app.include_router(internal.router)

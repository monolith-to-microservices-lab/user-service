#!/usr/bin/env sh
set -e

echo "Running database migrations..."
alembic upgrade head

echo "Starting user-service..."
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --timeout-graceful-shutdown "${GRACEFUL_SHUTDOWN_SECONDS:-20}"

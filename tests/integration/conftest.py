"""Fixtures for the real-Postgres integration layer. Everything here touches
the actual `user_service_test` database (schema drop/recreate + TRUNCATE
between tests) - kept out of the root conftest.py precisely so tests/unit/**
never pays that cost or that risk.
"""
import pytest
import sqlalchemy
from fastapi.testclient import TestClient
from sqlalchemy import text

from tests.conftest import assert_test_database

from app.config import settings
from app.database import Base, SessionLocal, engine
from app.main import app


def _ensure_database_exists() -> None:
    url = sqlalchemy.make_url(settings.database_url)
    admin = sqlalchemy.create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": url.database}
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    admin.dispose()


@pytest.fixture(scope="session", autouse=True)
def _schema():
    assert_test_database(str(engine.url))
    _ensure_database_exists()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture(autouse=True)
def _clean_table():
    assert_test_database(str(engine.url))
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))
    yield


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def db_session():
    session = SessionLocal()
    yield session
    session.close()

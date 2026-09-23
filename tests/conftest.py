import os

# Tests run against their OWN database so they never disturb the dev/compose
# data. Override with TEST_DATABASE_URL if needed. Set here (root conftest)
# because it must happen before ANY `app.*` import, regardless of which test
# level (unit/integration) ends up importing something from `app`.
_DEFAULT_TEST_DB = "postgresql+psycopg://user_service:user_service@localhost:5433/user_service_test"
os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", _DEFAULT_TEST_DB)
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("LOG_LEVEL", "WARNING")

import pytest  # noqa: E402
import sqlalchemy  # noqa: E402


def assert_test_database(url: str) -> None:
    """Defense in depth: even if TEST_DATABASE_URL is overridden by whoever
    runs the suite, refuse to run against anything that isn't unambiguously a
    test database. tests/integration TRUNCATEs and drops/recreates schema -
    see the sales-service incident this mirrors the fix for. Unit tests never
    touch Postgres at all (see tests/unit/conftest.py, SQLite in-memory).
    """
    db_name = sqlalchemy.make_url(url).database or ""
    if "test" not in db_name.lower():
        raise RuntimeError(
            f"Refusing to run tests against database {db_name!r} - its name does not "
            "contain 'test'. Set TEST_DATABASE_URL to a database whose name contains "
            "'test' (e.g. 'user_service_test') before running pytest."
        )


assert_test_database(os.environ["DATABASE_URL"])


def pytest_collection_modifyitems(config, items):
    """Auto-mark by directory instead of hand-annotating every test file:
    tests/unit/** -> unit, tests/integration/** -> integration, tests/e2e/**
    (if any) -> e2e.
    """
    for item in items:
        path = str(item.fspath).replace("\\", "/")
        if "/tests/unit/" in path:
            item.add_marker(pytest.mark.unit)
        elif "/tests/integration/" in path:
            item.add_marker(pytest.mark.integration)
        elif "/tests/e2e/" in path:
            item.add_marker(pytest.mark.e2e)

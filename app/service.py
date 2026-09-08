"""Business logic. Route -> service -> SQLAlchemy -> PostgreSQL.

Deliberately no repository/usecase/gateway layers: this is a small CRUD plus one
idempotent import operation.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .errors import ImportConflictError, UserNotFoundError
from .models import User
from .schemas import UserCreate, UserImport, UserUpdate

logger = logging.getLogger("user_service.service")

# Tolerance when comparing legacy vs stored timestamps during an idempotent
# import (drivers / serialization can shift sub-second precision).
_CREATED_AT_TOLERANCE_SECONDS = 1.0


def list_users(session: Session) -> list[User]:
    return list(session.scalars(select(User).order_by(User.id)))


def get_user(session: Session, user_id: int) -> User:
    user = session.get(User, user_id)
    if user is None:
        raise UserNotFoundError(user_id)
    return user


def create_user(session: Session, data: UserCreate) -> User:
    user = User(name=data.name)
    session.add(user)
    session.commit()
    session.refresh(user)
    logger.info("user.created", extra={"user_id": user.id})
    return user


def update_user(session: Session, user_id: int, data: UserUpdate) -> User:
    user = get_user(session, user_id)
    user.name = data.name
    session.commit()
    session.refresh(user)
    logger.info("user.updated", extra={"user_id": user.id})
    return user


def delete_user(session: Session, user_id: int) -> None:
    user = get_user(session, user_id)
    session.delete(user)
    session.commit()
    logger.info("user.deleted", extra={"user_id": user_id})


def _aware_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _diff(existing: User, data: UserImport) -> dict[str, dict[str, str]]:
    conflicts: dict[str, dict[str, str]] = {}
    if existing.name != data.name:
        conflicts["name"] = {"existing": existing.name, "incoming": data.name}
    if data.created_at is not None:
        delta = abs((_aware_utc(existing.created_at) - _aware_utc(data.created_at)).total_seconds())
        if delta > _CREATED_AT_TOLERANCE_SECONDS:
            conflicts["created_at"] = {
                "existing": _aware_utc(existing.created_at).isoformat(),
                "incoming": _aware_utc(data.created_at).isoformat(),
            }
    return conflicts


def import_user(session: Session, data: UserImport) -> tuple[str, User]:
    """Idempotently import a user coming from the legacy monolith.

    - id not present            -> insert with the explicit id, return ("created", user)
    - id present, same data     -> no-op, return ("unchanged", user)
    - id present, different data -> raise ImportConflictError (no silent overwrite)
    """
    existing = session.get(User, data.id)
    if existing is not None:
        conflicts = _diff(existing, data)
        if conflicts:
            raise ImportConflictError(data.id, conflicts)
        logger.info("user.import.unchanged", extra={"user_id": data.id})
        return "unchanged", existing

    user = User(id=data.id, name=data.name)
    if data.created_at is not None:
        user.created_at = data.created_at
    session.add(user)
    try:
        session.flush()
    except IntegrityError:
        # Concurrent import of the same id: fall back to the idempotency check.
        session.rollback()
        existing = session.get(User, data.id)
        if existing is None:
            raise
        conflicts = _diff(existing, data)
        if conflicts:
            raise ImportConflictError(data.id, conflicts)
        return "unchanged", existing

    resync_identity_sequence(session)
    session.commit()
    session.refresh(user)
    logger.info("user.import.created", extra={"user_id": user.id})
    return "created", user


def resync_identity_sequence(session: Session) -> None:
    """Keep the PostgreSQL identity sequence ahead of every explicit id.

    After inserting rows with explicit ids (imports), the identity sequence still
    points at its old value, so the next normal POST /users would try to reuse an
    id that already exists. We push the sequence to MAX(id) with is_called=true,
    meaning the next generated value is MAX(id) + 1.

    This is safe to run inside the same transaction as the insert and is cheap.
    """
    session.execute(
        text(
            """
            SELECT setval(
                pg_get_serial_sequence('users', 'id'),
                GREATEST((SELECT COALESCE(MAX(id), 1) FROM users), 1),
                true
            )
            """
        )
    )


def ping_database(session: Session) -> None:
    session.execute(select(func.now()))

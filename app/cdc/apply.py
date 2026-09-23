"""Apply one Debezium `users` change event to this service's own database.

Reuses the `User` model and `resync_identity_sequence` from app/service.py -
the same sequence-drift problem that affects `POST /internal/users/import`
(explicit-id inserts) applies here too.
"""

from sqlalchemy.orm import Session

from ..models import User
from ..service import resync_identity_sequence
from .events import DebeziumUserEnvelope, UserCdcPayload

_UPSERT_OPS = {"c", "r", "u"}


def apply_user_event(session: Session, envelope: DebeziumUserEnvelope) -> None:
    """Idempotent: applying the same event any number of times leaves the same
    end state (upsert-by-id for create/update, delete-if-present for delete).
    Commits on success; raises and leaves nothing committed on failure.
    """
    if envelope.op in _UPSERT_OPS:
        _upsert(session, envelope.after)
    elif envelope.op == "d":
        _delete(session, envelope.before)
    else:
        raise ValueError(f"unsupported CDC op: {envelope.op!r}")
    session.commit()


def _upsert(session: Session, payload: UserCdcPayload | None) -> None:
    if payload is None:
        raise ValueError("upsert event is missing its 'after' payload")
    user = session.get(User, payload.id)
    if user is None:
        session.add(User(id=payload.id, name=payload.name, created_at=payload.created_at))
        session.flush()
        resync_identity_sequence(session)
    else:
        user.name = payload.name
        user.created_at = payload.created_at


def _delete(session: Session, payload: UserCdcPayload | None) -> None:
    if payload is None:
        return
    user = session.get(User, payload.id)
    if user is not None:
        session.delete(user)

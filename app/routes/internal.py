from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from .. import service
from ..database import get_session
from ..schemas import ImportResult, UserImport, UserOut

# Internal, migration-only surface. Not meant for the public frontend.
router = APIRouter(prefix="/internal/users", tags=["internal"])


@router.post(
    "/import",
    response_model=ImportResult,
    responses={
        200: {"description": "User already existed with identical data"},
        201: {"description": "User created with the explicit legacy id"},
        409: {"description": "An existing user with this id has conflicting data"},
    },
)
def import_user(
    payload: UserImport, response: Response, session: Session = Depends(get_session)
) -> ImportResult:
    outcome, user = service.import_user(session, payload)
    response.status_code = status.HTTP_201_CREATED if outcome == "created" else status.HTTP_200_OK
    return ImportResult(status=outcome, user=UserOut.model_validate(user))

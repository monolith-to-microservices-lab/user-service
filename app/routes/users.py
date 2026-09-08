from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from .. import service
from ..database import get_session
from ..schemas import UserCreate, UserOut, UserUpdate

router = APIRouter(prefix="/users", tags=["users"])


@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(payload: UserCreate, session: Session = Depends(get_session)) -> UserOut:
    return UserOut.model_validate(service.create_user(session, payload))


@router.get("", response_model=list[UserOut])
def list_users(session: Session = Depends(get_session)) -> list[UserOut]:
    return [UserOut.model_validate(u) for u in service.list_users(session)]


@router.get("/{user_id}", response_model=UserOut)
def get_user(user_id: int, session: Session = Depends(get_session)) -> UserOut:
    return UserOut.model_validate(service.get_user(session, user_id))


@router.put("/{user_id}", response_model=UserOut)
def update_user(
    user_id: int, payload: UserUpdate, session: Session = Depends(get_session)
) -> UserOut:
    return UserOut.model_validate(service.update_user(session, user_id, payload))


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: int, session: Session = Depends(get_session)) -> Response:
    service.delete_user(session, user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class UserCreate(BaseModel):
    """Payload for normal user creation. The id is managed by the service."""

    name: str = Field(min_length=1, max_length=255)


class UserUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=255)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    created_at: datetime


class UserImport(BaseModel):
    """Payload for POST /internal/users/import.

    The id (and optionally created_at) comes from the legacy monolith and MUST be
    preserved exactly.
    """

    id: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=255)
    created_at: datetime | None = None


class ImportResult(BaseModel):
    # "created"   -> a new row was inserted with the supplied id
    # "unchanged" -> a row with the same id and identical data already existed
    status: str
    user: UserOut

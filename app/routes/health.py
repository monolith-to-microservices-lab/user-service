import logging

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from .. import service
from ..config import settings
from ..database import get_session

router = APIRouter(tags=["health"])
logger = logging.getLogger("user_service.health")


@router.get("/health")
def health(response: Response, session: Session = Depends(get_session)) -> dict:
    checks = {"database": "ok"}
    try:
        service.ping_database(session)
    except Exception:  # noqa: BLE001 - health must not raise
        logger.exception("health.database_check_failed")
        checks["database"] = "error"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ok" if all(v == "ok" for v in checks.values()) else "degraded",
        "service": settings.service_name,
        "env": settings.app_env,
        "checks": checks,
    }

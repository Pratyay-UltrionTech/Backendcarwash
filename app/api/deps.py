from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.core.security import decode_token
from app.database import get_db

security = HTTPBearer(auto_error=False)


def _require_bearer(credentials: HTTPAuthorizationCredentials | None) -> str:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail={"detail": "Missing bearer token", "code": "auth_required"})
    return credentials.credentials


def require_admin(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail={"detail": "Admin only", "code": "forbidden"})
    return payload


def require_manager(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "manager":
        raise HTTPException(status_code=403, detail={"detail": "Manager only", "code": "forbidden"})
    branch_id = payload.get("branch_id")
    manager_id = payload.get("sub")
    if not branch_id or not manager_id:
        raise HTTPException(status_code=401, detail={"detail": "Invalid manager token", "code": "invalid_token"})
    return payload


def require_customer(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "customer":
        raise HTTPException(status_code=403, detail={"detail": "Customer only", "code": "forbidden"})
    cid = payload.get("sub")
    if not cid:
        raise HTTPException(status_code=401, detail={"detail": "Invalid customer token", "code": "invalid_token"})
    return payload


def require_washer(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "washer":
        raise HTTPException(status_code=403, detail={"detail": "Washer only", "code": "forbidden"})
    branch_id = payload.get("branch_id")
    washer_id = payload.get("sub")
    if not branch_id or not washer_id:
        raise HTTPException(status_code=401, detail={"detail": "Invalid washer token", "code": "invalid_token"})
    return payload


def require_mobile_manager(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "mobile_manager":
        raise HTTPException(status_code=403, detail={"detail": "Mobile manager only", "code": "forbidden"})
    city_pin_code = payload.get("city_pin_code")
    manager_id = payload.get("sub")
    if not city_pin_code or not manager_id:
        raise HTTPException(status_code=401, detail={"detail": "Invalid mobile manager token", "code": "invalid_token"})
    return payload


def require_mobile_washer(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    token = _require_bearer(credentials)
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail={"detail": "Invalid token", "code": "invalid_token"})
    if payload.get("role") != "mobile_washer":
        raise HTTPException(status_code=403, detail={"detail": "Mobile washer only", "code": "forbidden"})
    city_pin_code = payload.get("city_pin_code")
    washer_id = payload.get("sub")
    if not city_pin_code or not washer_id:
        raise HTTPException(status_code=401, detail={"detail": "Invalid mobile washer token", "code": "invalid_token"})
    return payload


DbSession = Annotated[Session, Depends(get_db)]
AdminUser = Annotated[dict, Depends(require_admin)]
ManagerUser = Annotated[dict, Depends(require_manager)]
WasherUser = Annotated[dict, Depends(require_washer)]
CustomerAuth = Annotated[dict, Depends(require_customer)]
MobileManagerUser = Annotated[dict, Depends(require_mobile_manager)]
MobileWasherUser = Annotated[dict, Depends(require_mobile_washer)]

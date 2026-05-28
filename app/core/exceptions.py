from typing import Any
import logging

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

_log = logging.getLogger("uvicorn.error")


class AppError(Exception):
    """Domain / validation error with stable code for clients."""

    def __init__(self, message: str, code: str = "app_error", status_code: int = 400) -> None:
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(AppError):
    def __init__(self, message: str = "Resource not found", code: str = "not_found") -> None:
        super().__init__(message, code=code, status_code=404)


class ConflictError(AppError):
    def __init__(self, message: str, code: str = "conflict") -> None:
        super().__init__(message, code=code, status_code=409)


class UnauthorizedError(AppError):
    def __init__(self, message: str = "Unauthorized", code: str = "unauthorized") -> None:
        super().__init__(message, code=code, status_code=401)


class ForbiddenError(AppError):
    def __init__(self, message: str = "Forbidden", code: str = "forbidden") -> None:
        super().__init__(message, code=code, status_code=403)


async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
    request_id = getattr(_request.state, "request_id", "n/a")
    _log.warning(
        "app_error code=%s status_code=%s request_id=%s message=%s",
        exc.code,
        exc.status_code,
        request_id,
        exc.message,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message, "code": exc.code, "request_id": request_id},
    )


async def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    request_id = getattr(_request.state, "request_id", "n/a")
    detail: Any = exc.detail
    if isinstance(detail, dict):
        body = dict(detail)
    else:
        body = {"detail": str(detail), "code": "http_error"}
    body.setdefault("request_id", request_id)
    _log.warning(
        "http_error status_code=%s code=%s request_id=%s",
        exc.status_code,
        body.get("code", "http_error"),
        request_id,
    )
    return JSONResponse(status_code=exc.status_code, content=body)


async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(_request.state, "request_id", "n/a")
    _log.exception("unhandled_error request_id=%s", request_id, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "code": "internal_error", "request_id": request_id},
    )

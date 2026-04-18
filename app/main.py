import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1.router import api_router
from app.config import get_settings
from app.core.exceptions import AppError, app_error_handler, http_exception_handler, unhandled_exception_handler
from app.core.observability import configure_logging
from app.database import engine
from app.models.base import Base

_log = logging.getLogger("uvicorn.error")


def _ensure_tip_cents_column() -> None:
    """Add tip_cents to existing DBs created before that column existed."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("branch_bookings"):
        return
    cols = {c["name"] for c in insp.get_columns("branch_bookings")}
    if "tip_cents" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE branch_bookings ADD COLUMN tip_cents INTEGER NOT NULL DEFAULT 0"))
    _log.info("Applied schema patch: branch_bookings.tip_cents")


def _ensure_catalog_service_recommended_column() -> None:
    """Add recommended to existing DBs created before that column existed."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("catalog_service_items")}
    if "recommended" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text("ALTER TABLE catalog_service_items ADD COLUMN recommended BOOLEAN NOT NULL DEFAULT FALSE")
        )
    _log.info("Applied schema patch: catalog_service_items.recommended")


def _ensure_loyalty_ledger_and_booking_columns() -> None:
    """Loyalty ledger table + booking columns for service_id / completion time."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if insp.has_table("branch_bookings"):
        cols = {c["name"] for c in insp.get_columns("branch_bookings")}
        stmts = []
        if "service_id" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN service_id VARCHAR(36) NULL")
        if "completed_at" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN completed_at TIMESTAMPTZ NULL")
        if stmts:
            with engine.begin() as conn:
                for s in stmts:
                    conn.execute(text(s))
            _log.info("Applied schema patch: branch_bookings loyalty columns")
    if insp.has_table("mobile_bookings"):
        mcols = {c["name"] for c in insp.get_columns("mobile_bookings")}
        if "completed_at" not in mcols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_bookings ADD COLUMN completed_at TIMESTAMPTZ NULL"))
            _log.info("Applied schema patch: mobile_bookings.completed_at")


def _ensure_mobile_catalog_service_recommended_column() -> None:
    """Add recommended to existing mobile catalog DBs created before that column existed."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("mobile_catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("mobile_catalog_service_items")}
    if "recommended" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text("ALTER TABLE mobile_catalog_service_items ADD COLUMN recommended BOOLEAN NOT NULL DEFAULT FALSE")
        )
    _log.info("Applied schema patch: mobile_catalog_service_items.recommended")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    import app.models  # noqa: F401 — register ORM mappers

    from sqlalchemy.exc import OperationalError

    try:
        Base.metadata.create_all(bind=engine)
        _ensure_tip_cents_column()
        _ensure_loyalty_ledger_and_booking_columns()
        _ensure_catalog_service_recommended_column()
        _ensure_mobile_catalog_service_recommended_column()
    except OperationalError as e:
        _log.error("PostgreSQL connection failed: %s", e)
        _log.error(
            "If the host is Azure Database for PostgreSQL: open Azure Portal → your server → "
            "Networking, and add your current public IP (or enable public access appropriately). "
            "Private-endpoint-only servers cannot be reached from your laptop without VPN/peering. "
            "TLS uses sslmode=require for *.database.azure.com (set POSTGRES_SSLMODE to override)."
        )
        raise

    yield


app = FastAPI(title="Car Wash API", version="1.0.0", lifespan=lifespan)

settings = get_settings()
configure_logging(settings.log_level)
_cors_kw: dict = {
    "allow_origins": settings.cors_origin_list(),
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}
# Covers USER (and other Vite apps) on any localhost port; avoids OPTIONS 400 when Origin is e.g. http://localhost:5175.
if settings.cors_allow_localhost_regex:
    _cors_kw["allow_origin_regex"] = r"https?://(localhost|127\.0\.0\.1)(:\d+)?$"
app.add_middleware(CORSMiddleware, **_cors_kw)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = request_id
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        _log.exception(
            "request_failed method=%s path=%s request_id=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            request_id,
            duration_ms,
        )
        raise
    duration_ms = (time.perf_counter() - start) * 1000
    _log.info(
        "request_completed method=%s path=%s status_code=%s request_id=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        request_id,
        duration_ms,
    )
    response.headers["x-request-id"] = request_id
    return response

app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = getattr(_request.state, "request_id", "n/a")
    _log.warning("validation_error request_id=%s", request_id)
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "code": "validation_error", "request_id": request_id},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(api_router)

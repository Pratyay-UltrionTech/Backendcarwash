import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

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


def _ensure_branch_booking_selected_addon_ids_column() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("branch_bookings"):
        return
    cols = {c["name"] for c in insp.get_columns("branch_bookings")}
    if "selected_addon_ids_json" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE branch_bookings ADD COLUMN selected_addon_ids_json TEXT NOT NULL DEFAULT '[]'"))
    _log.info("Applied schema patch: branch_bookings.selected_addon_ids_json")


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
        if "cancelled_at" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN cancelled_at TIMESTAMPTZ NULL")
        if "cancelled_by" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN cancelled_by VARCHAR(36) NULL")
        if "updated_by" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN updated_by VARCHAR(36) NULL")
        if "updated_by_role" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN updated_by_role VARCHAR(32) NULL")
        if stmts:
            with engine.begin() as conn:
                for s in stmts:
                    conn.execute(text(s))
            _log.info("Applied schema patch: branch_bookings loyalty columns")
    if insp.has_table("mobile_bookings"):
        mcols = {c["name"] for c in insp.get_columns("mobile_bookings")}
        mstmts = []
        if "completed_at" not in mcols:
            mstmts.append("ALTER TABLE mobile_bookings ADD COLUMN completed_at TIMESTAMPTZ NULL")
        if "cancelled_at" not in mcols:
            mstmts.append("ALTER TABLE mobile_bookings ADD COLUMN cancelled_at TIMESTAMPTZ NULL")
        if "cancelled_by" not in mcols:
            mstmts.append("ALTER TABLE mobile_bookings ADD COLUMN cancelled_by VARCHAR(36) NULL")
        if "updated_by" not in mcols:
            mstmts.append("ALTER TABLE mobile_bookings ADD COLUMN updated_by VARCHAR(36) NULL")
        if "updated_by_role" not in mcols:
            mstmts.append("ALTER TABLE mobile_bookings ADD COLUMN updated_by_role VARCHAR(32) NULL")
        if mstmts:
            with engine.begin() as conn:
                for s in mstmts:
                    conn.execute(text(s))
            _log.info("Applied schema patch: mobile_bookings audit columns")


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


def _ensure_catalog_service_catalog_group_id_column() -> None:
    """Add catalog_group_id for service-centric multi-vehicle rows."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("catalog_service_items")}
    if "catalog_group_id" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE catalog_service_items ADD COLUMN catalog_group_id VARCHAR(36) NULL"))
    _log.info("Applied schema patch: catalog_service_items.catalog_group_id")


def _ensure_mobile_catalog_service_catalog_group_id_column() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("mobile_catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("mobile_catalog_service_items")}
    if "catalog_group_id" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE mobile_catalog_service_items ADD COLUMN catalog_group_id VARCHAR(36) NULL"))
    _log.info("Applied schema patch: mobile_catalog_service_items.catalog_group_id")


def _ensure_catalog_service_duration_minutes_column() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("catalog_service_items")}
    if "duration_minutes" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE catalog_service_items ADD COLUMN duration_minutes INTEGER NOT NULL DEFAULT 60"))
    _log.info("Applied schema patch: catalog_service_items.duration_minutes")


def _ensure_catalog_service_category_column() -> None:
    """Add category to existing DBs created before that column existed."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("catalog_service_items")}
    if "category" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE catalog_service_items ADD COLUMN category VARCHAR(64) NOT NULL DEFAULT 'Washing'"))
    _log.info("Applied schema patch: catalog_service_items.category")


def _ensure_mobile_catalog_service_category_column() -> None:
    """Add category to mobile catalog (must match MobileCatalogServiceItem ORM)."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("mobile_catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("mobile_catalog_service_items")}
    if "category" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE mobile_catalog_service_items ADD COLUMN category VARCHAR(64) NOT NULL DEFAULT 'Washing'"
            )
        )
    _log.info("Applied schema patch: mobile_catalog_service_items.category")


def _ensure_mobile_catalog_service_duration_minutes_column() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("mobile_catalog_service_items"):
        return
    cols = {c["name"] for c in insp.get_columns("mobile_catalog_service_items")}
    if "duration_minutes" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text("ALTER TABLE mobile_catalog_service_items ADD COLUMN duration_minutes INTEGER NOT NULL DEFAULT 60")
        )
    _log.info("Applied schema patch: mobile_catalog_service_items.duration_minutes")


def _ensure_booking_reporting_columns() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    for table in ["branch_bookings", "mobile_bookings"]:
        if not insp.has_table(table):
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        with engine.begin() as conn:
            if "payment_method" not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN payment_method VARCHAR(32) NOT NULL DEFAULT 'cash'"))
            if "promo_code" not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN promo_code VARCHAR(128)"))
            if "promo_discount_cents" not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN promo_discount_cents INTEGER NOT NULL DEFAULT 0"))
    _log.info("Applied schema patch: booking reporting columns")


def _ensure_booking_vehicle_model_column() -> None:
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    for table in ["branch_bookings", "mobile_bookings"]:
        if not insp.has_table(table):
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        if "vehicle_model" in cols:
            continue
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN vehicle_model VARCHAR(255) NOT NULL DEFAULT ''"))
        _log.info("Applied schema patch: %s.vehicle_model", table)


def _ensure_customer_id_for_bookings_and_ledger() -> None:
    """Stable member id on bookings + ledger (email/phone can change without losing history)."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    stmts: list[str] = []
    if insp.has_table("branch_bookings"):
        cols = {c["name"] for c in insp.get_columns("branch_bookings")}
        if "customer_id" not in cols:
            stmts.append("ALTER TABLE branch_bookings ADD COLUMN customer_id VARCHAR(36) NULL")
    if insp.has_table("mobile_bookings"):
        cols = {c["name"] for c in insp.get_columns("mobile_bookings")}
        if "customer_id" not in cols:
            stmts.append("ALTER TABLE mobile_bookings ADD COLUMN customer_id VARCHAR(36) NULL")
    if insp.has_table("loyalty_ledger_entries"):
        cols = {c["name"] for c in insp.get_columns("loyalty_ledger_entries")}
        if "customer_id" not in cols:
            stmts.append("ALTER TABLE loyalty_ledger_entries ADD COLUMN customer_id VARCHAR(36) NULL")
    if stmts:
        with engine.begin() as conn:
            for s in stmts:
                conn.execute(text(s))
        _log.info("Applied schema patch: customer_id on bookings / loyalty ledger")


def _ensure_mobile_booking_requested_zip_column() -> None:
    """Add requested_zip_code to mobile_bookings for zip-based service routing."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    if not insp.has_table("mobile_bookings"):
        return
    cols = {c["name"] for c in insp.get_columns("mobile_bookings")}
    if "requested_zip_code" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE mobile_bookings ADD COLUMN requested_zip_code VARCHAR(32) NOT NULL DEFAULT ''"))
    _log.info("Applied schema patch: mobile_bookings.requested_zip_code")


def _ensure_booking_customer_email_columns() -> None:
    """Add customer_email on branch and mobile booking tables (guest checkout contact)."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    for table in ["branch_bookings", "mobile_bookings"]:
        if not insp.has_table(table):
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        if "customer_email" in cols:
            continue
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN customer_email VARCHAR(320) NOT NULL DEFAULT ''"))
        _log.info("Applied schema patch: %s.customer_email", table)


def _ensure_catalog_service_excluded_points_column() -> None:
    """Add excluded_points to both branch and mobile catalog service tables."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    for table in ["catalog_service_items", "mobile_catalog_service_items"]:
        if not insp.has_table(table):
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        if "excluded_points" in cols:
            continue
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN excluded_points TEXT NOT NULL DEFAULT '[]'"))
        _log.info("Applied schema patch: %s.excluded_points", table)


def _ensure_catalog_service_sequence_column() -> None:
    """Add sequence (display order) to branch and mobile catalog service tables."""
    from sqlalchemy import inspect, text

    insp = inspect(engine)
    for table in ["catalog_service_items", "mobile_catalog_service_items"]:
        if not insp.has_table(table):
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        if "sequence" in cols:
            continue
        with engine.begin() as conn:
            conn.execute(
                text(f"ALTER TABLE {table} ADD COLUMN sequence INTEGER NOT NULL DEFAULT 999")
            )
        _log.info("Applied schema patch: %s.sequence", table)


def _ensure_washer_unavailability_table() -> None:
    """washer_unavailability is created by Base.metadata.create_all; no ALTER needed."""
    pass


def _ensure_user_addresses_table() -> None:
    """user_addresses is created by Base.metadata.create_all; log if already present."""
    from sqlalchemy import inspect
    insp = inspect(engine)
    if insp.has_table("user_addresses"):
        _log.debug("Schema: user_addresses table already exists")
    else:
        _log.info("Schema: user_addresses table will be created by create_all")


def _backfill_loyalty_ledger_customer_ids() -> None:
    from app.database import SessionLocal
    from app.models import BranchBooking, LoyaltyLedgerEntry, MobileBooking

    db = SessionLocal()
    try:
        changed = 0
        for le in (
            db.query(LoyaltyLedgerEntry)
            .filter(LoyaltyLedgerEntry.customer_id.is_(None), LoyaltyLedgerEntry.channel == "branch")
            .all()
        ):
            b = db.query(BranchBooking).filter(BranchBooking.id == le.booking_id).one_or_none()
            if b and b.customer_id:
                le.customer_id = str(b.customer_id)
                changed += 1
        for le in (
            db.query(LoyaltyLedgerEntry)
            .filter(LoyaltyLedgerEntry.customer_id.is_(None), LoyaltyLedgerEntry.channel == "mobile")
            .all()
        ):
            m = db.query(MobileBooking).filter(MobileBooking.id == le.booking_id).one_or_none()
            if m and m.customer_id:
                le.customer_id = str(m.customer_id)
                changed += 1
        db.commit()
        if changed:
            _log.info("Backfilled customer_id on %s loyalty ledger rows from bookings", changed)
    except Exception as e:
        db.rollback()
        _log.warning("Ledger customer_id backfill failed: %s", e)
    finally:
        db.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    import app.models  # noqa: F401 — register ORM mappers

    from sqlalchemy.exc import OperationalError

    try:
        Base.metadata.create_all(bind=engine)
        _ensure_tip_cents_column()
        _ensure_branch_booking_selected_addon_ids_column()
        _ensure_loyalty_ledger_and_booking_columns()
        _ensure_catalog_service_recommended_column()
        _ensure_mobile_catalog_service_recommended_column()
        _ensure_catalog_service_catalog_group_id_column()
        _ensure_mobile_catalog_service_catalog_group_id_column()
        _ensure_catalog_service_duration_minutes_column()
        _ensure_mobile_catalog_service_duration_minutes_column()
        _ensure_catalog_service_category_column()
        _ensure_mobile_catalog_service_category_column()
        _ensure_booking_reporting_columns()
        _ensure_booking_vehicle_model_column()
        _ensure_customer_id_for_bookings_and_ledger()
        _ensure_mobile_booking_requested_zip_column()
        _ensure_booking_customer_email_columns()
        _ensure_catalog_service_excluded_points_column()
        _ensure_catalog_service_sequence_column()
        _ensure_washer_unavailability_table()
        _ensure_user_addresses_table()
        _backfill_loyalty_ledger_customer_ids()
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

    def _json_safe_detail(val: Any) -> Any:
        """Ensure response is JSON-serializable (e.g. Pydantic may put ValueError instances in ``ctx``)."""
        if val is None or isinstance(val, (str, int, float, bool)):
            return val
        if isinstance(val, Exception):
            return str(val)
        if isinstance(val, dict):
            return {str(k): _json_safe_detail(v) for k, v in val.items()}
        if isinstance(val, (list, tuple)):
            return [_json_safe_detail(v) for v in val]
        return str(val)

    return JSONResponse(
        status_code=422,
        content={
            "detail": _json_safe_detail(exc.errors()),
            "code": "validation_error",
            "request_id": request_id,
        },
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(api_router)

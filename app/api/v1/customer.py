"""Authenticated customer profile (USER app)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import CustomerAuth, DbSession
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import CustomerUser
from app.services import loyalty_service
from app.services.loyalty_service import normalize_phone
from app.services.customer_history_service import service_history_items_for_phone
from app.schemas.customer_auth import CustomerProfileUpdate
from app.services.jsonutil import dumps_json, loads_json_array

router = APIRouter(prefix="/customer", tags=["customer"])
print(">>> CUSTOMER API MODULE LOADED SUCCESSFULLY")


def _customer_or_404(db, customer_id: str) -> CustomerUser:
    u = db.query(CustomerUser).filter(CustomerUser.id == customer_id).one_or_none()
    if not u:
        raise HTTPException(status_code=404, detail={"detail": "Customer not found", "code": "not_found"})
    return u


def _serialize(u: CustomerUser) -> dict[str, Any]:
    return {
        "id": u.id,
        "email": u.email,
        "full_name": u.full_name,
        "phone": u.phone,
        "address": u.address_line,
        "vehicles": loads_json_array(u.vehicles_json),
        "profile_completed": u.profile_completed,
    }


@router.get("/loyalty/overview")
def get_loyalty_overview(db: DbSession, auth: CustomerAuth, request: Request) -> dict[str, Any]:
    """Spend window + reward status from completed loyalty-eligible services (phone on profile)."""
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    out = loyalty_service.loyalty_overview_for_customer(db, u.phone or "")
    action_log(
        "customer_loyalty_overview",
        "success",
        request,
        customer_id=customer_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return out


@router.get("/service-history")
def get_service_history(db: DbSession, auth: CustomerAuth, request: Request) -> dict[str, Any]:
    """Bookings whose phone matches the customer profile (branch + mobile), newest first."""
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    items = service_history_items_for_phone(db, u.phone or "", limit=100)
    action_log(
        "customer_service_history",
        "success",
        request,
        customer_id=customer_id,
        row_count=len(items),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return {"items": items}


@router.get("/me")
def get_me(db: DbSession, auth: CustomerAuth, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    out = _serialize(u)
    action_log("customer_get_me", "success", request, customer_id=customer_id, latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.patch("/me")
def patch_profile(body: CustomerProfileUpdate, db: DbSession, auth: CustomerAuth, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    u.full_name = body.full_name.strip()
    u.phone = body.phone.strip()
    u.address_line = body.address.strip()
    u.vehicles_json = dumps_json([v.model_dump() for v in body.vehicles])
    u.profile_completed = True
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log(
            "customer_patch_profile",
            "failed",
            request,
            customer_id=customer_id,
            error_code="db_commit_failed",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(u)
    audit_log("customer", customer_id, "update_profile", request)
    action_log("customer_patch_profile", "success", request, customer_id=customer_id, latency_ms=round(monotonic_ms() - started, 2))
    return _serialize(u)


@router.patch("/bookings/{booking_id}/reschedule")
def reschedule_booking(
    booking_id: str, body: dict[str, Any], db: DbSession, auth: CustomerAuth, request: Request
) -> dict[str, Any]:
    """Reschedule a branch or mobile booking. Checks proximity by phone number."""
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    phone = normalize_phone(u.phone or "")

    sd = str(body.get("slot_date") or "")
    st = str(body.get("start_time") or "")
    et = str(body.get("end_time") or "")

    if not sd or not st or not et:
        raise HTTPException(status_code=422, detail={"detail": "Incomplete slot info", "code": "missing_fields"})

    # Try branch first
    from app.models import BranchBooking, MobileBooking

    job = db.query(BranchBooking).filter(BranchBooking.id == booking_id).one_or_none()
    if job:
        if normalize_phone(job.phone) != phone:
            raise HTTPException(status_code=403, detail={"detail": "Forbidden", "code": "forbidden"})
        job.slot_date = sd
        job.start_time = st
        job.end_time = et
        try:
            db.commit()
            db.refresh(job)
            action_log("reschedule_branch_booking", "success", request, booking_id=booking_id)
            return {"status": "ok", "booking_id": job.id}
        except SQLAlchemyError:
            db.rollback()
            raise HTTPException(status_code=500, detail={"detail": "DB error", "code": "db_error"})

    # Try mobile
    mjob = db.query(MobileBooking).filter(MobileBooking.id == booking_id).one_or_none()
    if mjob:
        if normalize_phone(mjob.phone) != phone:
            raise HTTPException(status_code=403, detail={"detail": "Forbidden", "code": "forbidden"})
        mjob.slot_date = sd
        mjob.start_time = st
        mjob.end_time = et
        try:
            db.commit()
            db.refresh(mjob)
            action_log("reschedule_mobile_booking", "success", request, booking_id=booking_id)
            return {"status": "ok", "booking_id": mjob.id}
        except SQLAlchemyError:
            db.rollback()
            raise HTTPException(status_code=500, detail={"detail": "DB error", "code": "db_error"})

    raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})

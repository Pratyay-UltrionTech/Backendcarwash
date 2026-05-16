"""Authenticated customer profile (USER app)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import CustomerAuth, DbSession
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import CustomerUser
from app.models.user_address import UserAddress
from app.services import loyalty_service
from app.services.loyalty_service import normalize_phone
from app.services.customer_history_service import service_history_items_for_customer
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
    """Spend window + reward status from completed loyalty-eligible services (member id + phone)."""
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    out = loyalty_service.loyalty_overview_for_customer(db, customer_id=customer_id, phone=u.phone or "")
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
    """Bookings linked to this member's customer_id (signed-in bookings), newest first."""
    started = monotonic_ms()
    customer_id = str(auth["sub"])
    u = _customer_or_404(db, customer_id)
    items = service_history_items_for_customer(db, customer_id, u.phone or "", limit=100)
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

    if body.email is not None:
        new_email = str(body.email).strip().lower()
        if new_email != (u.email or "").strip().lower():
            taken = (
                db.query(CustomerUser.id)
                .filter(CustomerUser.email == new_email, CustomerUser.id != u.id)
                .first()
            )
            if taken:
                raise HTTPException(
                    status_code=409,
                    detail={"detail": "That email is already in use", "code": "email_taken"},
                )
            u.email = new_email

    # Replace vehicles with exactly what the client sent — deletions must be honoured.
    from datetime import datetime, timezone
    new_timestamp = datetime.now(timezone.utc).isoformat()
    replaced_vehicles = []
    for nv in body.vehicles:
        v_dict = nv.dict()
        v_dict["last_used"] = new_timestamp
        replaced_vehicles.append(v_dict)

    u.vehicles_json = dumps_json(replaced_vehicles)
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
        owns = job.customer_id and str(job.customer_id) == customer_id
        if not owns and normalize_phone(job.phone) != phone:
            raise HTTPException(status_code=403, detail={"detail": "Forbidden", "code": "forbidden"})
        job.slot_date = sd
        job.start_time = st
        job.end_time = et
        try:
            db.commit()
            db.refresh(job)
            action_log("reschedule_branch_booking", "success", request, booking_id=booking_id)
            if u.email:
                from app.services.email_service import send_booking_rescheduled_email
                send_booking_rescheduled_email(
                    to_email=u.email,
                    name=u.full_name or "",
                    new_date=job.slot_date,
                    new_start_time=job.start_time,
                    booking_id=job.id,
                )
            return {"status": "ok", "booking_id": job.id}
        except SQLAlchemyError:
            db.rollback()
            raise HTTPException(status_code=500, detail={"detail": "DB error", "code": "db_error"})

    # Try mobile
    mjob = db.query(MobileBooking).filter(MobileBooking.id == booking_id).one_or_none()
    if mjob:
        owns_m = mjob.customer_id and str(mjob.customer_id) == customer_id
        if not owns_m and normalize_phone(mjob.phone) != phone:
            raise HTTPException(status_code=403, detail={"detail": "Forbidden", "code": "forbidden"})
        mjob.slot_date = sd
        mjob.start_time = st
        mjob.end_time = et
        try:
            db.commit()
            db.refresh(mjob)
            action_log("reschedule_mobile_booking", "success", request, booking_id=booking_id)
            if u.email:
                from app.services.email_service import send_booking_rescheduled_email
                send_booking_rescheduled_email(
                    to_email=u.email,
                    name=u.full_name or "",
                    new_date=mjob.slot_date,
                    new_start_time=mjob.start_time,
                    booking_id=mjob.id,
                )
            return {"status": "ok", "booking_id": mjob.id}
        except SQLAlchemyError:
            db.rollback()
            raise HTTPException(status_code=500, detail={"detail": "DB error", "code": "db_error"})

    raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})


# ── Saved addresses ──────────────────────────────────────────────────────────

def _serialize_address(a: UserAddress) -> dict[str, Any]:
    return {
        "id": a.id,
        "label": a.label,
        "street_address": a.street_address,
        "suburb": a.suburb,
        "state": a.state,
        "postcode": a.postcode,
        "is_default": a.is_default,
    }


@router.get("/addresses")
def list_addresses(db: DbSession, auth: CustomerAuth) -> dict[str, Any]:
    customer_id = str(auth["sub"])
    rows = (
        db.query(UserAddress)
        .filter(UserAddress.user_id == customer_id)
        .order_by(UserAddress.is_default.desc(), UserAddress.created_at)
        .all()
    )
    return {"addresses": [_serialize_address(a) for a in rows]}


@router.post("/addresses", status_code=201)
def create_address(body: dict[str, Any], db: DbSession, auth: CustomerAuth) -> dict[str, Any]:
    customer_id = str(auth["sub"])
    label = str(body.get("label") or "Home").strip()[:64]
    street = str(body.get("street_address") or "").strip()
    suburb = str(body.get("suburb") or "").strip()
    state = str(body.get("state") or "").strip()
    postcode = str(body.get("postcode") or "").strip()
    is_default = bool(body.get("is_default", False))

    if not street:
        raise HTTPException(status_code=422, detail={"detail": "street_address is required", "code": "missing_field"})

    if is_default:
        db.query(UserAddress).filter(
            UserAddress.user_id == customer_id, UserAddress.is_default.is_(True)
        ).update({"is_default": False})

    # First address is automatically default
    existing_count = db.query(UserAddress).filter(UserAddress.user_id == customer_id).count()
    if existing_count == 0:
        is_default = True

    addr = UserAddress(
        user_id=customer_id, label=label, street_address=street,
        suburb=suburb, state=state, postcode=postcode, is_default=is_default,
    )
    db.add(addr)
    try:
        db.commit()
        db.refresh(addr)
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    return _serialize_address(addr)


@router.patch("/addresses/{address_id}")
def update_address(address_id: str, body: dict[str, Any], db: DbSession, auth: CustomerAuth) -> dict[str, Any]:
    customer_id = str(auth["sub"])
    addr = db.query(UserAddress).filter(UserAddress.id == address_id, UserAddress.user_id == customer_id).one_or_none()
    if not addr:
        raise HTTPException(status_code=404, detail={"detail": "Address not found", "code": "not_found"})

    if "label" in body:
        addr.label = str(body["label"]).strip()[:64]
    if "street_address" in body:
        addr.street_address = str(body["street_address"]).strip()
    if "suburb" in body:
        addr.suburb = str(body["suburb"]).strip()
    if "state" in body:
        addr.state = str(body["state"]).strip()
    if "postcode" in body:
        addr.postcode = str(body["postcode"]).strip()
    if body.get("is_default"):
        db.query(UserAddress).filter(
            UserAddress.user_id == customer_id, UserAddress.is_default.is_(True)
        ).update({"is_default": False})
        addr.is_default = True

    try:
        db.commit()
        db.refresh(addr)
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    return _serialize_address(addr)


@router.delete("/addresses/{address_id}", status_code=204)
def delete_address(address_id: str, db: DbSession, auth: CustomerAuth) -> None:
    customer_id = str(auth["sub"])
    addr = db.query(UserAddress).filter(UserAddress.id == address_id, UserAddress.user_id == customer_id).one_or_none()
    if not addr:
        raise HTTPException(status_code=404, detail={"detail": "Address not found", "code": "not_found"})
    was_default = addr.is_default
    db.delete(addr)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    # Promote next address to default if we deleted the default
    if was_default:
        next_addr = (
            db.query(UserAddress)
            .filter(UserAddress.user_id == customer_id)
            .order_by(UserAddress.created_at)
            .first()
        )
        if next_addr:
            next_addr.is_default = True
            db.commit()


@router.patch("/addresses/{address_id}/set-default")
def set_default_address(address_id: str, db: DbSession, auth: CustomerAuth) -> dict[str, Any]:
    customer_id = str(auth["sub"])
    addr = db.query(UserAddress).filter(UserAddress.id == address_id, UserAddress.user_id == customer_id).one_or_none()
    if not addr:
        raise HTTPException(status_code=404, detail={"detail": "Address not found", "code": "not_found"})
    db.query(UserAddress).filter(
        UserAddress.user_id == customer_id, UserAddress.is_default.is_(True)
    ).update({"is_default": False})
    addr.is_default = True
    try:
        db.commit()
        db.refresh(addr)
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    return _serialize_address(addr)

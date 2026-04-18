"""Mobile washer/driver API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, MobileWasherUser
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import MobileBooking, MobileServiceDriver, MobileServiceManager
from app.schemas.mobile import MobileBookingUpdate
from app.services.jsonutil import loads_json_array
from app.services import loyalty_service, mobile_slot_service

router = APIRouter(prefix="/washer/mobile", tags=["washer-mobile"])


def _booking_to_dict(b: MobileBooking) -> dict[str, Any]:
    return {
        "id": b.id,
        "city_pin_code": b.city_pin_code,
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "selected_addon_ids": loads_json_array(b.selected_addon_ids_json),
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "status": b.status,
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "completed_at": b.completed_at.isoformat() if getattr(b, "completed_at", None) else None,
    }


@router.get("/jobs/available")
def list_available_jobs(
    pin_code: str,
    db: DbSession,
    washer: MobileWasherUser,
    request: Request,
) -> list[dict[str, Any]]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    driver = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
        .one_or_none()
    )
    if not driver:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    manager, _ = mobile_slot_service.manager_for_service_pin(db, pin_code)
    if not manager:
        return []
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.city_pin_code == manager.city_pin_code,
            MobileBooking.assigned_driver_id.is_(None),
            MobileBooking.status == "scheduled",
        )
        .order_by(MobileBooking.slot_date.asc(), MobileBooking.start_time.asc())
        .all()
    )
    action_log("washer_mobile_list_available_jobs", "success", request, city_pin_code=manager.city_pin_code, driver_id=driver_id, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(r) for r in rows]


@router.get("/jobs")
def list_jobs(db: DbSession, washer: MobileWasherUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.city_pin_code == city_pin_code,
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status != "cancelled",
        )
        .order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time)
        .all()
    )
    action_log("washer_mobile_list_jobs", "success", request, city_pin_code=city_pin_code, driver_id=driver_id, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(r) for r in rows]


@router.get("/jobs/history")
def list_history(db: DbSession, washer: MobileWasherUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.city_pin_code == city_pin_code,
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status.in_(["completed", "cancelled"]),
        )
        .order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time)
        .all()
    )
    action_log("washer_mobile_list_history", "success", request, city_pin_code=city_pin_code, driver_id=driver_id, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(r) for r in rows]


@router.get("/earnings")
def get_earnings(db: DbSession, washer: MobileWasherUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.city_pin_code == city_pin_code,
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status == "completed",
        )
        .all()
    )
    total_tip_cents = sum(int(r.tip_cents or 0) for r in rows)
    out = {
        "completed_jobs": len(rows),
        "total_tip_cents": total_tip_cents,
        "total_tip_amount": round(total_tip_cents / 100.0, 2),
    }
    action_log("washer_mobile_get_earnings", "success", request, city_pin_code=city_pin_code, driver_id=driver_id, completed_jobs=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.post("/jobs/{booking_id}/accept")
def accept_job(booking_id: str, db: DbSession, washer: MobileWasherUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    driver = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
        .one_or_none()
    )
    if not driver:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    row = (
        db.query(MobileBooking)
        .filter(MobileBooking.id == booking_id, MobileBooking.city_pin_code == city_pin_code)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    if row.assigned_driver_id and row.assigned_driver_id != driver_id:
        raise HTTPException(status_code=409, detail={"detail": "Booking already assigned", "code": "already_assigned"})
    mgr = db.query(MobileServiceManager).filter(MobileServiceManager.id == row.manager_id).one_or_none()
    if not mgr:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    try:
        mobile_slot_service.assert_driver_assignable(
            db,
            mgr,
            row.slot_date,
            row.start_time,
            row.end_time,
            driver_id,
            exclude_booking_id=row.id,
        )
    except ValueError as e:
        code = str(e)
        if code == "driver_not_open":
            raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
        if code == "driver_busy":
            raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
        if code == "slot_unavailable":
            raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
        raise HTTPException(status_code=400, detail={"detail": "Invalid driver assignment", "code": "invalid_slot"})
    row.assigned_driver_id = driver_id
    if row.status == "scheduled":
        row.status = "checked_in"
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("washer_mobile_accept_job", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_washer", driver_id, "accept_booking", request, booking_id=booking_id, city_pin_code=city_pin_code)
    action_log("washer_mobile_accept_job", "success", request, booking_id=booking_id, city_pin_code=city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _booking_to_dict(row)


@router.patch("/jobs/{booking_id}")
def patch_job(
    booking_id: str, body: MobileBookingUpdate, db: DbSession, washer: MobileWasherUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    row = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.id == booking_id,
            MobileBooking.city_pin_code == city_pin_code,
            MobileBooking.assigned_driver_id == driver_id,
        )
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    prev_status = row.status
    if "status" in data and data["status"] is not None:
        st = str(data["status"])
        if st not in ("scheduled", "checked_in", "in_progress", "completed", "cancelled"):
            raise HTTPException(status_code=400, detail={"detail": "Invalid status", "code": "invalid_status"})
        row.status = st
    if "notes" in data and data["notes"] is not None:
        row.notes = str(data["notes"])
    if "tip_cents" in data and data["tip_cents"] is not None:
        row.tip_cents = max(0, int(data["tip_cents"]))
    loyalty_service.on_mobile_booking_status_change(db, row, prev_status)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("washer_mobile_patch_job", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_washer", driver_id, "update_booking_status", request, booking_id=booking_id, city_pin_code=city_pin_code, status=row.status)
    action_log("washer_mobile_patch_job", "success", request, booking_id=booking_id, city_pin_code=city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _booking_to_dict(row)

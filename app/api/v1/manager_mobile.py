"""Mobile manager API."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, MobileManagerUser
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import MobileBooking, MobileDriverLeaveRequest, MobileServiceDriver, MobileServiceManager, MobileSlotSettings
from app.models.base import new_id
from app.schemas.mobile import (
    MobileBookingCreate,
    MobileBookingUpdate,
    MobileDriverLeaveRequestStatusUpdate,
    MobileSlotSettingsPatch,
)
from app.services.jsonutil import dumps_json, loads_json_array, loads_json_object
from app.services import loyalty_service, mobile_slot_service
from app.services.booking_status import effective_status
from app.services.duration_slots import snap_duration_to_base_slots
from app.services.slot_service import add_minutes_to_hhmm

router = APIRouter(prefix="/manager/mobile", tags=["manager-mobile"])


def _extract_requested_pin(address: str) -> str:
    digits = "".join(ch if ch.isdigit() else " " for ch in str(address or ""))
    for token in digits.split():
        if len(token) in (5, 6):
            return token
    return ""


def _driver_to_dict(d: MobileServiceDriver) -> dict[str, Any]:
    return {
        "id": d.id,
        "manager_id": d.manager_id,
        "city_pin_code": d.city_pin_code,
        "service_pin_code": d.service_pin_code,
        "emp_name": d.emp_name,
        "address": d.address,
        "zip_code": d.zip_code,
        "serviceable_zip_codes": loads_json_array(d.serviceable_zip_codes_json),
        "email": d.email,
        "mobile": d.mobile,
        "doj": d.doj,
        "login_id": d.login_id,
        "active": d.active,
    }


def _booking_to_dict(b: MobileBooking) -> dict[str, Any]:
    s0, s1 = mobile_slot_service.booking_span_minutes(b.start_time, b.end_time)
    duration_minutes = snap_duration_to_base_slots(s1 - s0)
    return {
        "id": b.id,
        "city_pin_code": b.city_pin_code,
        "requested_zip_code": getattr(b, "requested_zip_code", "") or "",
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "selected_addon_ids": loads_json_array(b.selected_addon_ids_json),
        "duration_minutes": duration_minutes,
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "status": effective_status(b.status, b.slot_date, b.end_time),
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "completed_at": b.completed_at.isoformat() if getattr(b, "completed_at", None) else None,
        "cancelled_at": getattr(b, "cancelled_at", None) and b.cancelled_at.isoformat(),
        "cancelled_by": getattr(b, "cancelled_by", None),
        "updated_by": getattr(b, "updated_by", None),
        "updated_by_role": getattr(b, "updated_by_role", None),
    }


def _slot_settings_to_dict(s: MobileSlotSettings) -> dict[str, Any]:
    return {
        "city_pin_code": s.city_pin_code,
        "slot_duration_minutes": s.slot_duration_minutes,
        "open_time": s.open_time,
        "close_time": s.close_time,
        "slot_window_active_by_key": loads_json_object(s.slot_window_active_by_key_json),
        "slot_driver_open_by_window": loads_json_object(s.slot_driver_open_by_window_json),
        "slot_day_states": loads_json_object(s.slot_day_states_json),
    }


def _slot_settings_or_create(db: DbSession, manager: MobileServiceManager) -> MobileSlotSettings:
    row = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == manager.id).one_or_none()
    if row is None:
        row = MobileSlotSettings(manager_id=manager.id, city_pin_code=manager.city_pin_code)
        db.add(row)
        db.flush()
    return row


@router.get("/drivers")
def list_drivers(db: DbSession, manager: MobileManagerUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    city_pin_code = str(manager["city_pin_code"])
    rows = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.city_pin_code == city_pin_code)
        .order_by(MobileServiceDriver.emp_name)
        .all()
    )
    action_log("manager_mobile_list_drivers", "success", request, city_pin_code=city_pin_code, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_driver_to_dict(r) for r in rows]


def _parse_mobile_booking_id(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return new_id()
    s = str(raw).strip()
    try:
        uuid.UUID(s)
    except ValueError:
        raise HTTPException(status_code=400, detail={"detail": "Invalid booking id", "code": "validation_error"})
    return s


@router.get("/bookings")
def list_bookings(db: DbSession, manager: MobileManagerUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    city_pin_code = str(manager["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(MobileBooking.city_pin_code == city_pin_code)
        .order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time)
        .all()
    )
    action_log("manager_mobile_list_bookings", "success", request, city_pin_code=city_pin_code, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(r) for r in rows]


@router.get("/customer-lookup")
def lookup_customer(
    db: DbSession,
    manager: MobileManagerUser,
    request: Request,
    phone: str | None = Query(None),
    email: str | None = Query(None),
) -> dict[str, Any]:
    from app.services.manager_customer_lookup import (
        customer_user_to_lookup_dict,
        find_customer_user_for_manager_lookup,
        find_guest_booking_for_manager_lookup,
        guest_booking_to_lookup_dict,
    )

    started = monotonic_ms()
    city_pin_code = str(manager["city_pin_code"])
    u = find_customer_user_for_manager_lookup(db, phone=phone, email=email)
    if u:
        action_log(
            "manager_mobile_customer_lookup",
            "success",
            request,
            city_pin_code=city_pin_code,
            found=True,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        return customer_user_to_lookup_dict(u, db=db)

    guest = find_guest_booking_for_manager_lookup(db, phone=phone, email=email)
    action_log(
        "manager_mobile_customer_lookup",
        "success",
        request,
        city_pin_code=city_pin_code,
        found=guest is not None,
        guest=guest is not None,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    if guest:
        return guest_booking_to_lookup_dict(guest)
    return {}


@router.post("/bookings")
def create_booking(
    body: MobileBookingCreate, db: DbSession, manager: MobileManagerUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    manager_id = str(manager["sub"])
    city_pin_code = str(manager["city_pin_code"])
    
    # --- Date/Time Validation ---
    from datetime import datetime
    now = datetime.now()
    today_iso = now.date().isoformat()
    if str(body.slot_date) < today_iso:
        raise HTTPException(status_code=400, detail={"detail": "Cannot book a past date", "code": "past_date"})
    if str(body.slot_date) == today_iso:
        current_time = now.strftime("%H:%M")
        if str(body.start_time) < current_time:
            raise HTTPException(status_code=400, detail={"detail": "Cannot book a past time slot", "code": "past_time"})
    # ----------------------------

    manager_row = db.query(MobileServiceManager).filter(MobileServiceManager.id == manager_id).one_or_none()
    if not manager_row:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    dur_m = mobile_slot_service.resolve_mobile_booking_duration_minutes(db, body.service_id, body.selected_addon_ids)
    end_time = body.end_time or add_minutes_to_hhmm(body.start_time, dur_m)
    assigned_driver_id = body.assigned_driver_id
    requested_pin = _extract_requested_pin(body.address)
    try:
        mobile_slot_service.assert_slot_available(db, manager_row, body.slot_date, body.start_time, end_time)
        if assigned_driver_id:
            mobile_slot_service.assert_driver_assignable(
                db,
                manager_row,
                body.slot_date,
                body.start_time,
                end_time,
                assigned_driver_id,
                requested_pin=requested_pin,
            )
        else:
            assigned_driver_id = mobile_slot_service.allocate_driver_for_interval(
                db,
                manager_row,
                body.slot_date,
                body.start_time,
                end_time,
                requested_pin=requested_pin,
            )
            if assigned_driver_id is None:
                raise ValueError("slot_unavailable")
    except ValueError as e:
        code = str(e)
        if code == "slot_unavailable":
            raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
        if code == "driver_not_open":
            raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
        if code == "driver_busy":
            raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
        if code == "driver_zip_mismatch":
            raise HTTPException(status_code=409, detail={"detail": "Driver is not serviceable for booking zip", "code": "driver_zip_mismatch"})
        raise HTTPException(status_code=400, detail={"detail": "Invalid slot assignment", "code": "invalid_slot"})
    bid = _parse_mobile_booking_id(body.booking_id)

    row = MobileBooking(
        id=bid,
        manager_id=manager_id,
        city_pin_code=city_pin_code,
        customer_id=(body.customer_id or "").strip() or None,
        customer_name=body.customer_name,
        address=body.address,
        phone=body.phone,
        customer_email=(body.customer_email or "").strip()[:320],
        vehicle_summary=body.vehicle_summary,
        service_id=body.service_id,
        vehicle_type=body.vehicle_type,
        vehicle_model=body.vehicle_model or "",
        registration_number=body.registration_number or "",
        selected_addon_ids_json=dumps_json(body.selected_addon_ids),
        slot_date=body.slot_date,
        start_time=body.start_time,
        end_time=end_time,
        assigned_driver_id=assigned_driver_id,
        status="scheduled",
        source=body.source,
        notes=body.notes,
        tip_cents=max(0, int(body.tip_cents or 0)),
    )
    if body.assigned_driver_id:
        d = (
            db.query(MobileServiceDriver)
            .filter(MobileServiceDriver.id == body.assigned_driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
            .one_or_none()
        )
        if not d:
            raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("manager_mobile_create_booking", "failed", request, city_pin_code=city_pin_code, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_manager", manager_id, "create_booking", request, booking_id=row.id, city_pin_code=city_pin_code)
    action_log("manager_mobile_create_booking", "success", request, booking_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    from app.services.email_service import lookup_customer_email, send_booking_confirmed_email
    cust_email, cust_name = lookup_customer_email(db, row.customer_id, row.phone)
    if cust_email:
        send_booking_confirmed_email(
            to_email=cust_email,
            name=cust_name or row.customer_name or "",
            date=row.slot_date,
            start_time=row.start_time,
            service_summary=getattr(row, "vehicle_summary", "") or "",
            booking_id=row.id,
        )
    return _booking_to_dict(row)


@router.patch("/bookings/{booking_id}")
def patch_booking(
    booking_id: str, body: MobileBookingUpdate, db: DbSession, manager: MobileManagerUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    manager_id = str(manager["sub"])
    city_pin_code = str(manager["city_pin_code"])
    row = (
        db.query(MobileBooking)
        .filter(MobileBooking.id == booking_id, MobileBooking.city_pin_code == city_pin_code)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    prev_status = row.status
    old_slot = (row.slot_date, row.start_time, row.end_time)
    manager_row = db.query(MobileServiceManager).filter(MobileServiceManager.id == manager_id).one_or_none()
    if not manager_row:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})

    if any(k in data for k in ("slot_date", "start_time", "end_time")):
        sd = str(data.get("slot_date", row.slot_date))
        st = str(data.get("start_time", row.start_time))
        et = str(data.get("end_time", row.end_time))

        # Validate that the new slot is in the future
        from datetime import datetime
        now = datetime.now()
        today_iso = now.date().isoformat()
        if sd < today_iso:
            raise HTTPException(status_code=400, detail={"detail": "Cannot reschedule to a past date", "code": "past_date"})
        if sd == today_iso:
            current_time = now.strftime("%H:%M")
            if st < current_time:
                raise HTTPException(status_code=400, detail={"detail": "Cannot reschedule to a past time slot", "code": "past_time"})

        try:
            mobile_slot_service.assert_slot_available_for_booking_update(db, manager_row, sd, st, et, row.id)
        except ValueError:
            raise HTTPException(
                status_code=409,
                detail={"detail": "Selected slot is not available", "code": "slot_unavailable"},
            )
        row.slot_date = sd
        row.start_time = st
        row.end_time = et
    requested_pin = getattr(row, "requested_zip_code", "") or ""

    if "customer_name" in data and data["customer_name"] is not None:
        row.customer_name = str(data["customer_name"]).strip()
    if "phone" in data and data["phone"] is not None:
        row.phone = str(data["phone"]).strip()
    if "address" in data and data["address"] is not None:
        row.address = str(data["address"]).strip()
    if "vehicle_type" in data and data["vehicle_type"] is not None:
        row.vehicle_type = str(data["vehicle_type"]).strip()
    if "vehicle_summary" in data and data["vehicle_summary"] is not None:
        row.vehicle_summary = str(data["vehicle_summary"]).strip()
    if "service_id" in data:
        sid = data["service_id"]
        row.service_id = sid.strip() if isinstance(sid, str) and sid.strip() else None
    if "selected_addon_ids" in data and data["selected_addon_ids"] is not None:
        row.selected_addon_ids_json = dumps_json(list(data["selected_addon_ids"]))

    if "assigned_driver_id" in data:
        driver_id = data["assigned_driver_id"]
        if driver_id is not None:
            d = (
                db.query(MobileServiceDriver)
                .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
                .one_or_none()
            )
            if not d:
                raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
            try:
                mobile_slot_service.assert_driver_assignable(
                    db,
                    manager_row,
                    row.slot_date,
                    row.start_time,
                    row.end_time,
                    driver_id,
                    exclude_booking_id=row.id,
                    requested_pin=requested_pin,
                )
            except ValueError as e:
                code = str(e)
                if code == "driver_not_open":
                    raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
                if code == "driver_busy":
                    raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
                if code == "slot_unavailable":
                    raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
                if code == "driver_zip_mismatch":
                    raise HTTPException(status_code=409, detail={"detail": "Driver is not serviceable for booking zip", "code": "driver_zip_mismatch"})
                raise HTTPException(status_code=400, detail={"detail": "Invalid driver assignment", "code": "invalid_slot"})
        row.assigned_driver_id = driver_id
        # Auto-advance to assigned when a driver is set and status is still scheduled.
        if row.assigned_driver_id and row.status == "scheduled" and "status" not in data:
            row.status = "assigned"
    if "status" in data and data["status"] is not None:
        st = str(data["status"])
        if st not in ("scheduled", "assigned", "arrived", "checked_in", "in_progress", "completed", "cancelled"):
            raise HTTPException(status_code=400, detail={"detail": "Invalid status", "code": "invalid_status"})
        prev_st = row.status
        row.status = st
        from datetime import datetime, timezone
        _now = datetime.now(timezone.utc)
        row.updated_by = manager_id
        row.updated_by_role = "manager"
        if st == "cancelled" and prev_st != "cancelled":
            row.cancelled_at = _now
            row.cancelled_by = manager_id
        elif st != "cancelled":
            row.cancelled_at = None
            row.cancelled_by = None
    if "notes" in data and data["notes"] is not None:
        row.notes = str(data["notes"])
    if "tip_cents" in data and data["tip_cents"] is not None:
        row.tip_cents = max(0, int(data["tip_cents"]))
    loyalty_service.on_mobile_booking_status_change(db, row, prev_status)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("manager_mobile_patch_booking", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_manager", manager_id, "update_booking", request, booking_id=booking_id, city_pin_code=city_pin_code)
    action_log("manager_mobile_patch_booking", "success", request, booking_id=booking_id, latency_ms=round(monotonic_ms() - started, 2))
    if (row.slot_date, row.start_time, row.end_time) != old_slot:
        from app.services.email_service import lookup_customer_email, send_booking_rescheduled_email
        cust_email, cust_name = lookup_customer_email(db, row.customer_id, row.phone)
        if cust_email:
            send_booking_rescheduled_email(
                to_email=cust_email,
                name=cust_name or row.customer_name or "",
                new_date=row.slot_date,
                new_start_time=row.start_time,
                booking_id=row.id,
            )
    return _booking_to_dict(row)


@router.get("/slot-settings")
def get_slot_settings(db: DbSession, manager: MobileManagerUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    manager_id = str(manager["sub"])
    manager_row = db.query(MobileServiceManager).filter(MobileServiceManager.id == manager_id).one_or_none()
    if not manager_row:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    row = _slot_settings_or_create(db, manager_row)
    action_log("manager_mobile_get_slot_settings", "success", request, city_pin_code=manager_row.city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _slot_settings_to_dict(row)


@router.patch("/slot-settings")
def patch_slot_settings(
    body: MobileSlotSettingsPatch, db: DbSession, manager: MobileManagerUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    manager_id = str(manager["sub"])
    city_pin_code = str(manager["city_pin_code"])
    manager_row = db.query(MobileServiceManager).filter(MobileServiceManager.id == manager_id).one_or_none()
    if not manager_row:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    row = _slot_settings_or_create(db, manager_row)
    data = body.model_dump(exclude_unset=True)
    if "slot_duration_minutes" in data and data["slot_duration_minutes"] is not None:
        row.slot_duration_minutes = int(data["slot_duration_minutes"])
    if "open_time" in data and data["open_time"] is not None:
        row.open_time = str(data["open_time"])
    if "close_time" in data and data["close_time"] is not None:
        row.close_time = str(data["close_time"])
    if "slot_window_active_by_key" in data and data["slot_window_active_by_key"] is not None:
        row.slot_window_active_by_key_json = dumps_json(data["slot_window_active_by_key"])
    if "slot_driver_open_by_window" in data and data["slot_driver_open_by_window"] is not None:
        row.slot_driver_open_by_window_json = dumps_json(data["slot_driver_open_by_window"])
    if "slot_day_states" in data and data["slot_day_states"] is not None:
        row.slot_day_states_json = dumps_json(data["slot_day_states"])
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("manager_mobile_patch_slot_settings", "failed", request, city_pin_code=city_pin_code, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_manager", manager_id, "update_slot_settings", request, city_pin_code=city_pin_code)
    action_log("manager_mobile_patch_slot_settings", "success", request, city_pin_code=city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _slot_settings_to_dict(row)


@router.get("/driver-unavailability")
def list_driver_unavailability(
    db: DbSession,
    manager: MobileManagerUser,
    request: Request,
    date: str = Query(..., description="YYYY-MM-DD"),
) -> list[dict[str, Any]]:
    """Return per-driver blocked time windows for the given date (from driver self-marking)."""
    city_pin_code = str(manager["city_pin_code"])
    manager_row = db.query(MobileServiceManager).filter(MobileServiceManager.city_pin_code == city_pin_code, MobileServiceManager.active.is_(True)).one_or_none()
    if not manager_row:
        return []
    slot_settings = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == manager_row.id).one_or_none()
    if not slot_settings:
        return []
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.manager_id == manager_row.id, MobileServiceDriver.active.is_(True))
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    if not active_drivers:
        return []
    day_states = loads_json_object(slot_settings.slot_day_states_json)
    result: dict[str, list[str]] = {d.id: [] for d in active_drivers}
    for key, state in day_states.items():
        if not key.startswith(f"{date}|"):
            continue
        if not isinstance(state, dict):
            continue
        masks = state.get("driversOpen")
        if not isinstance(masks, list):
            continue
        parts = key.split("|")
        if len(parts) < 3:
            continue
        window = f"{parts[1]}|{parts[2]}"
        for idx, drv in enumerate(active_drivers):
            if idx < len(masks) and not masks[idx]:
                result[drv.id].append(window)
    return [{"driver_id": did, "blocked_windows": windows} for did, windows in result.items() if windows]


# ---------------------------------------------------------------------------
# Manager — driver leave requests
# ---------------------------------------------------------------------------

def _mgr_mobile_leave_to_dict(r: MobileDriverLeaveRequest, driver_name: str) -> dict:
    return {
        "id": r.id,
        "mobile_manager_id": r.mobile_manager_id,
        "driver_id": r.driver_id,
        "driver_name": driver_name,
        "leave_date": r.leave_date,
        "leave_type": r.leave_type,
        "start_time": r.start_time,
        "end_time": r.end_time,
        "reason": r.reason,
        "status": r.status,
        "reviewed_by_manager_id": r.reviewed_by_manager_id,
        "reviewed_at": r.reviewed_at,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


@router.get("/leave-requests")
def manager_list_driver_leave_requests(
    db: DbSession, manager: MobileManagerUser, request: Request
) -> list[dict]:
    city_pin_code = str(manager["city_pin_code"])
    manager_row = (
        db.query(MobileServiceManager)
        .filter(MobileServiceManager.city_pin_code == city_pin_code, MobileServiceManager.active.is_(True))
        .one_or_none()
    )
    if not manager_row:
        return []
    driver_map = {
        d.id: d.emp_name
        for d in db.query(MobileServiceDriver).filter(MobileServiceDriver.manager_id == manager_row.id).all()
    }
    rows = (
        db.query(MobileDriverLeaveRequest)
        .filter(MobileDriverLeaveRequest.mobile_manager_id == manager_row.id)
        .order_by(MobileDriverLeaveRequest.leave_date.desc(), MobileDriverLeaveRequest.created_at.desc())
        .all()
    )
    return [_mgr_mobile_leave_to_dict(r, driver_map.get(r.driver_id, "Driver")) for r in rows]


@router.patch("/leave-requests/{leave_request_id}")
def manager_update_driver_leave_status(
    leave_request_id: str,
    body: MobileDriverLeaveRequestStatusUpdate,
    db: DbSession,
    manager: MobileManagerUser,
    request: Request,
) -> dict:
    from datetime import datetime, timezone
    city_pin_code = str(manager["city_pin_code"])
    manager_id = str(manager["sub"])
    manager_row = (
        db.query(MobileServiceManager)
        .filter(MobileServiceManager.city_pin_code == city_pin_code, MobileServiceManager.active.is_(True))
        .one_or_none()
    )
    if not manager_row:
        raise HTTPException(status_code=404, detail={"detail": "Manager not found", "code": "not_found"})
    row = (
        db.query(MobileDriverLeaveRequest)
        .filter(
            MobileDriverLeaveRequest.id == leave_request_id,
            MobileDriverLeaveRequest.mobile_manager_id == manager_row.id,
        )
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Leave request not found", "code": "not_found"})
    status = body.status.strip()
    if status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail={"detail": "status must be approved or rejected", "code": "validation_error"})
    row.status = status
    row.reviewed_by_manager_id = manager_id
    row.reviewed_at = datetime.now(timezone.utc).isoformat()
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    db.refresh(row)
    driver_row = db.query(MobileServiceDriver).filter(MobileServiceDriver.id == row.driver_id).one_or_none()
    driver_name = driver_row.emp_name if driver_row else "Driver"
    return _mgr_mobile_leave_to_dict(row, driver_name)

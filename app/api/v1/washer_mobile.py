"""Mobile washer/driver API."""

from __future__ import annotations

from typing import Any
from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, MobileWasherUser
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import MobileBooking, MobileDriverLeaveRequest, MobileServiceDriver, MobileServiceManager
from app.models.mobile import MobileSlotSettings
from app.schemas.mobile import MobileBookingUpdate, MobileDriverLeaveRequestCreate
from app.services.booking_pricing import mobile_booking_customer_service_total_cents
from app.services.booking_status import effective_status
from app.services.duration_slots import resolve_operating_day_end_minutes
from app.services.jsonutil import dumps_json, loads_json_array, loads_json_object
from app.services import loyalty_service, mobile_slot_service

router = APIRouter(prefix="/washer/mobile", tags=["washer-mobile"])

def _extract_zip_from_address(address: str) -> str:
    digits = "".join(ch if ch.isdigit() else " " for ch in str(address or "")).split()
    for token in digits:
        if 5 <= len(token) <= 6:
            return token
    return ""


def _booking_to_dict(db, b: MobileBooking) -> dict[str, Any]:
    addon_ids = loads_json_array(b.selected_addon_ids_json)
    addon_names: list[str] = []
    if addon_ids:
        from app.models.mobile import MobileCatalogAddonItem, MobileGlobalAddonItem
        catalog_map = {r.id: r.name for r in db.query(MobileCatalogAddonItem).filter(MobileCatalogAddonItem.id.in_(addon_ids)).all()}
        global_map = {r.id: r.name for r in db.query(MobileGlobalAddonItem).filter(MobileGlobalAddonItem.id.in_(addon_ids)).all()}
        merged = {**catalog_map, **global_map}
        addon_names = [merged[aid] for aid in addon_ids if aid in merged]
    d: dict[str, Any] = {
        "id": b.id,
        "city_pin_code": b.city_pin_code,
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_summary": b.vehicle_summary,
        # Align with branch booking payloads / washer UI field name
        "service_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "selected_addon_ids": addon_ids,
        "addon_names": addon_names,
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "status": effective_status(b.status, b.slot_date, b.end_time),
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "customer_id": str(b.customer_id) if getattr(b, "customer_id", None) else None,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "completed_at": b.completed_at.isoformat() if getattr(b, "completed_at", None) else None,
        "cancelled_at": getattr(b, "cancelled_at", None) and b.cancelled_at.isoformat(),
        "cancelled_by": getattr(b, "cancelled_by", None),
        "updated_by": getattr(b, "updated_by", None),
        "updated_by_role": getattr(b, "updated_by_role", None),
    }
    try:
        d["customer_service_total_cents"] = mobile_booking_customer_service_total_cents(db, b)
    except Exception:
        d["customer_service_total_cents"] = 0
    return d


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
    manager = db.query(MobileServiceManager).filter(MobileServiceManager.id == driver.manager_id, MobileServiceManager.active.is_(True)).one_or_none()
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
    pin = "".join(ch for ch in str(pin_code or "") if ch.isdigit())
    eligible_rows: list[MobileBooking] = []
    for r in rows:
        booking_zip = _extract_zip_from_address(r.address)
        if pin and booking_zip and booking_zip != pin:
            continue
        try:
            mobile_slot_service.assert_driver_assignable(
                db,
                manager,
                r.slot_date,
                r.start_time,
                r.end_time,
                driver_id,
                exclude_booking_id=r.id,
                requested_pin=booking_zip or pin or None,
            )
        except ValueError:
            continue
        eligible_rows.append(r)
    action_log("washer_mobile_list_available_jobs", "success", request, city_pin_code=manager.city_pin_code, driver_id=driver_id, row_count=len(eligible_rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(db, r) for r in eligible_rows]


@router.get("/jobs")
def list_jobs(db: DbSession, washer: MobileWasherUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status != "cancelled",
        )
        .order_by(MobileBooking.slot_date.asc(), MobileBooking.start_time.asc())
        .all()
    )
    action_log("washer_mobile_list_jobs", "success", request, city_pin_code=city_pin_code, driver_id=driver_id, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(db, r) for r in rows]


@router.get("/unavailability")
def list_mobile_unavailability(db: DbSession, washer: MobileWasherUser, request: Request) -> list[dict[str, Any]]:
    """Return the driver's own blocked dates (derived from slot_day_states_json)."""
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    driver = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
        .one_or_none()
    )
    if not driver:
        return []
    slot_settings = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == driver.manager_id).one_or_none()
    if not slot_settings:
        return []
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.manager_id == driver.manager_id, MobileServiceDriver.active.is_(True))
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    driver_ids = [d.id for d in active_drivers]
    if driver_id not in driver_ids:
        return []
    driver_idx = driver_ids.index(driver_id)
    day_states = loads_json_object(slot_settings.slot_day_states_json)
    blocked_dates: set[str] = set()
    for key, state in day_states.items():
        if not isinstance(state, dict):
            continue
        masks = state.get("driversOpen")
        if not isinstance(masks, list):
            continue
        if driver_idx < len(masks) and not masks[driver_idx]:
            date_part = key.split("|")[0]
            blocked_dates.add(date_part)
    return [{"id": d, "date": d, "all_day": True, "start_time": "", "end_time": ""} for d in sorted(blocked_dates)]


@router.delete("/unavailability/date/{date}")
def delete_mobile_unavailability_date(date: str, db: DbSession, washer: MobileWasherUser, request: Request) -> dict[str, Any]:
    """Unblock a previously self-marked unavailable date."""
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    driver = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
        .one_or_none()
    )
    if not driver:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    slot_settings = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == driver.manager_id).one_or_none()
    if not slot_settings:
        return {"ok": True}
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.manager_id == driver.manager_id, MobileServiceDriver.active.is_(True))
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    driver_ids = [d.id for d in active_drivers]
    if driver_id not in driver_ids:
        return {"ok": True}
    driver_idx = driver_ids.index(driver_id)
    day_states = loads_json_object(slot_settings.slot_day_states_json)
    for key in list(day_states.keys()):
        if not key.startswith(f"{date}|"):
            continue
        state = day_states[key]
        if not isinstance(state, dict):
            continue
        masks = state.get("driversOpen")
        if isinstance(masks, list) and driver_idx < len(masks):
            masks[driver_idx] = True
            state["driversOpen"] = masks
            day_states[key] = state
    slot_settings.slot_day_states_json = dumps_json(day_states)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    action_log("washer_mobile_delete_unavailability", "success", request, driver_id=driver_id, date=date)
    return {"ok": True}


@router.post("/availability/unavailable-dates")
def set_unavailable_date(
    body: dict[str, Any],
    db: DbSession,
    washer: MobileWasherUser,
    request: Request,
) -> dict[str, Any]:
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    day = str(body.get("date") or "").strip()
    if not day:
        raise HTTPException(status_code=400, detail={"detail": "date is required", "code": "validation_error"})
    driver = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == city_pin_code, MobileServiceDriver.active.is_(True))
        .one_or_none()
    )
    if not driver:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    slot_settings = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == driver.manager_id).one_or_none()
    if not slot_settings:
        raise HTTPException(status_code=404, detail={"detail": "Slot settings not found", "code": "not_found"})

    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.manager_id == driver.manager_id, MobileServiceDriver.active.is_(True))
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    driver_ids = [d.id for d in active_drivers]
    if driver_id not in driver_ids:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    driver_idx = driver_ids.index(driver_id)

    open_m = mobile_slot_service._parse_hhmm(slot_settings.open_time or "09:00")
    close_raw = mobile_slot_service._parse_hhmm(slot_settings.close_time or "17:00")
    close_m = resolve_operating_day_end_minutes(open_m, close_raw)

    day_states = loads_json_object(slot_settings.slot_day_states_json)
    t = open_m
    while t < close_m:
        st = mobile_slot_service._fmt_hhmm(t)
        et = mobile_slot_service._fmt_hhmm(t + mobile_slot_service.BASE_SLOT_MINUTES)
        k = mobile_slot_service._day_key(day, st, et)
        state = day_states.get(k, {})
        if not isinstance(state, dict):
            state = {}
        mask = state.get("driversOpen")
        if not isinstance(mask, list):
            mask = [True] * len(driver_ids)
        if len(mask) < len(driver_ids):
            mask.extend([True] * (len(driver_ids) - len(mask)))
        mask[driver_idx] = False
        state["driversOpen"] = mask
        day_states[k] = state
        t += mobile_slot_service.BASE_SLOT_MINUTES
    slot_settings.slot_day_states_json = dumps_json(day_states)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("washer_mobile_set_unavailable", "failed", request, driver_id=driver_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    action_log("washer_mobile_set_unavailable", "success", request, driver_id=driver_id, day=day)
    return {"ok": True, "date": day}


@router.get("/jobs/history")
def list_history(db: DbSession, washer: MobileWasherUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status.in_(["completed", "cancelled"]),
        )
        .order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time)
        .all()
    )
    action_log("washer_mobile_list_history", "success", request, city_pin_code=city_pin_code, driver_id=driver_id, row_count=len(rows), latency_ms=round(monotonic_ms() - started, 2))
    return [_booking_to_dict(db, r) for r in rows]


@router.get("/earnings")
def get_earnings(db: DbSession, washer: MobileWasherUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    driver_id = str(washer["sub"])
    city_pin_code = str(washer["city_pin_code"])
    rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.assigned_driver_id == driver_id,
            MobileBooking.status == "completed",
        )
        .all()
    )
    total_tip_cents = sum(int(r.tip_cents or 0) for r in rows)
    total_service_cents = sum(mobile_booking_customer_service_total_cents(db, r) for r in rows)
    out = {
        "completed_jobs": len(rows),
        "total_tip_cents": total_tip_cents,
        "total_tip_amount": round(total_tip_cents / 100.0, 2),
        "total_service_revenue_cents": total_service_cents,
        "total_service_revenue_amount": round(total_service_cents / 100.0, 2),
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
        .filter(MobileBooking.id == booking_id)
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
    # Keep status as scheduled so the driver can explicitly "Mark arrived" in the app after accepting.
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("washer_mobile_accept_job", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_washer", driver_id, "accept_booking", request, booking_id=booking_id, city_pin_code=city_pin_code)
    action_log("washer_mobile_accept_job", "success", request, booking_id=booking_id, city_pin_code=city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _booking_to_dict(db, row)


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
        if st not in ("scheduled", "assigned", "arrived", "checked_in", "in_progress", "completed", "cancelled"):
            raise HTTPException(status_code=400, detail={"detail": "Invalid status", "code": "invalid_status"})
        row.status = st
        from datetime import datetime, timezone
        _now = datetime.now(timezone.utc)
        row.updated_by = driver_id
        row.updated_by_role = "driver"
        if st == "cancelled" and prev_status != "cancelled":
            row.cancelled_at = _now
            row.cancelled_by = driver_id
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
        action_log("washer_mobile_patch_job", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("mobile_washer", driver_id, "update_booking_status", request, booking_id=booking_id, city_pin_code=city_pin_code, status=row.status)
    action_log("washer_mobile_patch_job", "success", request, booking_id=booking_id, city_pin_code=city_pin_code, latency_ms=round(monotonic_ms() - started, 2))
    return _booking_to_dict(db, row)


# ---------------------------------------------------------------------------
# Driver leave requests
# ---------------------------------------------------------------------------

def _mobile_leave_to_dict(r: MobileDriverLeaveRequest, driver_name: str) -> dict[str, Any]:
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
def driver_list_leave_requests(
    db: DbSession, washer: MobileWasherUser, request: Request
) -> list[dict[str, Any]]:
    driver_id = str(washer["sub"])
    driver_row = db.query(MobileServiceDriver).filter(MobileServiceDriver.id == driver_id).one_or_none()
    driver_name = driver_row.emp_name if driver_row else ""
    rows = (
        db.query(MobileDriverLeaveRequest)
        .filter(MobileDriverLeaveRequest.driver_id == driver_id)
        .order_by(MobileDriverLeaveRequest.leave_date.desc())
        .all()
    )
    return [_mobile_leave_to_dict(r, driver_name) for r in rows]


@router.post("/leave-requests", status_code=201)
def driver_submit_leave_request(
    body: MobileDriverLeaveRequestCreate,
    db: DbSession,
    washer: MobileWasherUser,
    request: Request,
) -> dict[str, Any]:
    from datetime import date as _date
    driver_id = str(washer["sub"])
    leave_date = body.leave_date.strip()
    leave_type = body.leave_type.strip()
    start_time = body.start_time.strip()
    end_time = body.end_time.strip()
    reason = body.reason.strip()

    if not leave_date:
        raise HTTPException(status_code=400, detail={"detail": "leave_date is required", "code": "validation_error"})
    try:
        _date.fromisoformat(leave_date)
    except ValueError:
        raise HTTPException(status_code=400, detail={"detail": "Invalid leave_date format", "code": "validation_error"})
    if leave_type not in ("full_day", "partial_day"):
        raise HTTPException(status_code=400, detail={"detail": "leave_type must be full_day or partial_day", "code": "validation_error"})
    if leave_type == "partial_day" and (not start_time or not end_time or start_time >= end_time):
        raise HTTPException(status_code=400, detail={"detail": "Partial day leave requires valid start_time and end_time", "code": "validation_error"})

    driver_row = db.query(MobileServiceDriver).filter(MobileServiceDriver.id == driver_id).one_or_none()
    if not driver_row:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})

    row = MobileDriverLeaveRequest(
        mobile_manager_id=driver_row.manager_id,
        driver_id=driver_id,
        leave_date=leave_date,
        leave_type=leave_type,
        start_time=start_time if leave_type == "partial_day" else "",
        end_time=end_time if leave_type == "partial_day" else "",
        reason=reason,
        status="pending",
    )
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    db.refresh(row)
    return _mobile_leave_to_dict(row, driver_row.emp_name)


@router.delete("/leave-requests/{leave_request_id}", status_code=200)
def driver_cancel_leave_request(
    leave_request_id: str,
    db: DbSession,
    washer: MobileWasherUser,
    request: Request,
) -> dict[str, Any]:
    driver_id = str(washer["sub"])
    row = (
        db.query(MobileDriverLeaveRequest)
        .filter(
            MobileDriverLeaveRequest.id == leave_request_id,
            MobileDriverLeaveRequest.driver_id == driver_id,
        )
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Leave request not found", "code": "not_found"})
    if row.status != "pending":
        raise HTTPException(status_code=400, detail={"detail": "Only pending requests can be cancelled", "code": "already_reviewed"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database error", "code": "db_error"})
    return {"ok": True}

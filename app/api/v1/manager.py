"""Branch manager API — bookings, washers, slot configuration."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, ManagerUser
from app.core.exceptions import AppError
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import Branch, BranchBooking, BranchSlotSettings, Washer, WasherUnavailability, WasherLeaveRequest
from fastapi import Query
from app.api.v1.serialize import booking_to_dict, slot_settings_to_dict, washer_to_dict
from app.schemas.booking import BookingCreate, BookingUpdate
from app.schemas.catalog import SlotSettingsPatch
from app.services import booking_service
from app.services import loyalty_service

router = APIRouter(prefix="/manager", tags=["manager"])


def _branch_or_404(db, branch_id: str) -> Branch:
    b = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
    if not b:
        raise HTTPException(status_code=404, detail={"detail": "Branch not found", "code": "not_found"})
    return b


def _ensure_slot_row(db, branch: Branch) -> BranchSlotSettings:
    row = db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
    if row is None:
        row = BranchSlotSettings(branch_id=branch.id)
        db.add(row)
        db.flush()
    return row


@router.get("/bookings")
def list_bookings(db: DbSession, manager: ManagerUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    _branch_or_404(db, branch_id)
    rows = (
        db.query(BranchBooking)
        .filter(BranchBooking.branch_id == branch_id)
        .order_by(BranchBooking.slot_date.desc(), BranchBooking.start_time)
        .all()
    )
    action_log(
        "manager_list_bookings",
        "success",
        request,
        branch_id=branch_id,
        row_count=len(rows),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return [booking_to_dict(x) for x in rows]


@router.get("/customer-lookup")
def lookup_customer(
    db: DbSession,
    manager: ManagerUser,
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
    branch_id = str(manager["branch_id"])
    _branch_or_404(db, branch_id)

    u = find_customer_user_for_manager_lookup(db, phone=phone, email=email)
    if u:
        action_log(
            "manager_customer_lookup",
            "success",
            request,
            branch_id=branch_id,
            found=True,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        return customer_user_to_lookup_dict(u, db=db)

    guest = find_guest_booking_for_manager_lookup(db, phone=phone, email=email)
    action_log(
        "manager_customer_lookup",
        "success",
        request,
        branch_id=branch_id,
        found=guest is not None,
        guest=guest is not None,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    if guest:
        return guest_booking_to_lookup_dict(guest)
    return {}


@router.post("/bookings")
def create_walk_in(db: DbSession, manager: ManagerUser, body: BookingCreate, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    manager_id = str(manager["sub"])
    b = _branch_or_404(db, branch_id)
    try:
        job = booking_service.create_booking(
            db,
            b,
            customer_name=body.customer_name,
            phone=body.phone,
            customer_email=(body.customer_email or "").strip(),
            address=body.address,
            vehicle_type=body.vehicle_type,
            vehicle_model=body.vehicle_model or "",
            registration_number=body.registration_number or "",
            service_summary=body.service_summary,
            service_id=body.service_id,
            selected_addon_ids=body.selected_addon_ids,
            slot_date=body.slot_date,
            start_time=body.start_time,
            end_time=body.end_time,
            source="walk_in",
            tip_cents=body.tip_cents,
            service_charged_cents=body.service_charged_cents,
            notes=body.notes,
            bay_number=body.bay_number,
            assigned_washer_id=body.assigned_washer_id,
            booking_id=body.booking_id,
            customer_id=body.customer_id,
            payment_method=body.payment_method,
        )
        db.commit()
        audit_log(
            "manager",
            manager_id,
            "create_walk_in_booking",
            request,
            branch_id=branch_id,
            booking_id=job.id,
        )
        action_log(
            "manager_create_walk_in",
            "success",
            request,
            branch_id=branch_id,
            booking_id=job.id,
            latency_ms=round(monotonic_ms() - started, 2),
        )
    except AppError as e:
        db.rollback()
        action_log(
            "manager_create_walk_in",
            "failed",
            request,
            branch_id=branch_id,
            error_code=e.code,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=e.status_code, detail={"detail": e.message, "code": e.code})
    db.refresh(job)
    from app.services.email_service import lookup_customer_email, send_booking_confirmed_email, send_staff_booking_notification
    cust_email, cust_name = lookup_customer_email(db, job.customer_id, job.phone)
    if cust_email:
        send_booking_confirmed_email(
            to_email=cust_email,
            name=cust_name or job.customer_name or "",
            date=job.slot_date,
            start_time=job.start_time,
            service_summary=job.service_summary or "",
            booking_id=job.id,
            customer_id=str(job.customer_id) if job.customer_id else None,
            phone=job.phone or None,
            end_time=job.end_time or None,
            channel="branch",
            payment_method=getattr(job, "payment_method", None),
        )
    send_staff_booking_notification(
        db,
        event="new_booking",
        booking_type="branch",
        booking_id=job.id,
        customer_name=job.customer_name or "",
        phone=job.phone or "",
        vehicle_type=job.vehicle_type or "",
        vehicle_model=job.vehicle_model or "",
        registration_number=job.registration_number or "",
        service_summary=job.service_summary or "",
        slot_date=job.slot_date,
        start_time=job.start_time,
        end_time=job.end_time,
        branch_id=str(job.branch_id),
        customer_id=str(job.customer_id) if job.customer_id else None,
        payment_method=getattr(job, "payment_method", None),
    )
    return booking_to_dict(job)


@router.patch("/bookings/{booking_id}")
def patch_booking(
    booking_id: str, body: BookingUpdate, db: DbSession, manager: ManagerUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    manager_id = str(manager["sub"])
    b = _branch_or_404(db, branch_id)
    job = (
        db.query(BranchBooking)
        .filter(BranchBooking.id == booking_id, BranchBooking.branch_id == branch_id)
        .one_or_none()
    )
    if not job:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    data["_actor_id"] = manager_id
    data["_actor_role"] = "manager"
    prev_status = job.status
    old_slot = (job.slot_date, job.start_time, job.end_time)
    try:
        booking_service.patch_branch_booking_fields(db, b, job, data)
        loyalty_service.on_branch_booking_status_change(db, job, prev_status)
        db.commit()
        audit_log(
            "manager",
            manager_id,
            "update_booking",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
        )
        action_log(
            "manager_patch_booking",
            "success",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            latency_ms=round(monotonic_ms() - started, 2),
        )
    except AppError as e:
        db.rollback()
        action_log(
            "manager_patch_booking",
            "failed",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            error_code=e.code,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=e.status_code, detail={"detail": e.message, "code": e.code})
    except SQLAlchemyError:
        db.rollback()
        action_log(
            "manager_patch_booking",
            "failed",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            error_code="db_error",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(
            status_code=500,
            detail={"detail": "Database operation failed", "code": "db_error"},
        )
    if (job.slot_date, job.start_time, job.end_time) != old_slot:
        try:
            from app.services.email_service import lookup_customer_email, send_booking_rescheduled_email, send_staff_booking_notification

            cust_email, cust_name = lookup_customer_email(db, job.customer_id, job.phone)
            if cust_email:
                send_booking_rescheduled_email(
                    to_email=cust_email,
                    name=cust_name or job.customer_name or "",
                    new_date=job.slot_date,
                    new_start_time=job.start_time,
                    booking_id=job.id,
                    customer_id=str(job.customer_id) if job.customer_id else None,
                    phone=job.phone or None,
                    new_end_time=job.end_time or None,
                    service_summary=job.service_summary or "",
                    channel="branch",
                )
            send_staff_booking_notification(
                db,
                event="rescheduled",
                booking_type="branch",
                booking_id=job.id,
                customer_name=job.customer_name or "",
                phone=job.phone or "",
                vehicle_type=job.vehicle_type or "",
                vehicle_model=job.vehicle_model or "",
                registration_number=job.registration_number or "",
                service_summary=job.service_summary or "",
                slot_date=job.slot_date,
                start_time=job.start_time,
                end_time=job.end_time,
                branch_id=str(job.branch_id),
                old_slot_date=old_slot[0],
                old_start_time=old_slot[1],
                customer_id=str(job.customer_id) if job.customer_id else None,
                payment_method=getattr(job, "payment_method", None),
            )
        except Exception:
            import logging

            logging.getLogger("uvicorn.error").warning(
                "Reschedule saved but confirmation email failed for booking %s",
                booking_id,
                exc_info=True,
            )
    return booking_to_dict(job, db)


@router.get("/washers")
def list_washers(db: DbSession, manager: ManagerUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    _branch_or_404(db, branch_id)
    rows = db.query(Washer).filter(Washer.branch_id == branch_id).all()
    action_log(
        "manager_list_washers",
        "success",
        request,
        branch_id=branch_id,
        row_count=len(rows),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return [washer_to_dict(w) for w in rows]


@router.get("/slot-settings")
def get_slot_settings(db: DbSession, manager: ManagerUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    b = _branch_or_404(db, branch_id)
    s = _ensure_slot_row(db, b)
    action_log(
        "manager_get_slot_settings",
        "success",
        request,
        branch_id=branch_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return slot_settings_to_dict(s)


@router.patch("/slot-settings")
def patch_slot_settings(db: DbSession, manager: ManagerUser, body: SlotSettingsPatch, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    manager_id = str(manager["sub"])
    b = _branch_or_404(db, branch_id)
    s = _ensure_slot_row(db, b)
    data = body.model_dump(exclude_unset=True)
    if "manager_slot_duration_minutes" in data and data["manager_slot_duration_minutes"] is not None:
        s.manager_slot_duration_minutes = data["manager_slot_duration_minutes"]
    if "slot_bay_open_by_window" in data and data["slot_bay_open_by_window"] is not None:
        s.slot_bay_open_by_window_json = json.dumps(data["slot_bay_open_by_window"])
    if "slot_window_active_by_key" in data and data["slot_window_active_by_key"] is not None:
        s.slot_window_active_by_key_json = json.dumps(data["slot_window_active_by_key"])
    if "slot_day_states" in data and data["slot_day_states"] is not None:
        s.slot_day_states_json = json.dumps(data["slot_day_states"])
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log(
            "manager_patch_slot_settings",
            "failed",
            request,
            branch_id=branch_id,
            error_code="db_commit_failed",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(s)
    audit_log("manager", manager_id, "update_slot_settings", request, branch_id=branch_id)
    action_log(
        "manager_patch_slot_settings",
        "success",
        request,
        branch_id=branch_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return slot_settings_to_dict(s)


@router.get("/washer-unavailability")
def list_washer_unavailability(
    db: DbSession,
    manager: ManagerUser,
    request: Request,
    date: str = Query(..., description="YYYY-MM-DD"),
) -> list[dict[str, Any]]:
    """Return all washers' unavailability records for the given date in the manager's branch."""
    branch_id = str(manager["branch_id"])
    washer_ids = [str(w.id) for w in db.query(Washer).filter(Washer.branch_id == branch_id, Washer.active.is_(True)).all()]
    if not washer_ids:
        return []
    rows = (
        db.query(WasherUnavailability)
        .filter(WasherUnavailability.washer_id.in_(washer_ids), WasherUnavailability.date == date)
        .all()
    )
    return [
        {
            "id": r.id,
            "washer_id": r.washer_id,
            "date": r.date,
            "all_day": r.all_day,
            "start_time": r.start_time or "",
            "end_time": r.end_time or "",
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Washer leave requests — manager view
# ---------------------------------------------------------------------------

def _leave_request_to_dict_mgr(r: WasherLeaveRequest, washer_name: str = "") -> dict[str, Any]:
    return {
        "id": r.id,
        "branch_id": r.branch_id,
        "washer_id": r.washer_id,
        "washer_name": washer_name,
        "leave_date": r.leave_date,
        "leave_type": r.leave_type,
        "start_time": r.start_time or "",
        "end_time": r.end_time or "",
        "reason": r.reason or "",
        "status": r.status,
        "reviewed_by_manager_id": r.reviewed_by_manager_id,
        "reviewed_at": r.reviewed_at,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


@router.get("/leave-requests")
def list_leave_requests(db: DbSession, manager: ManagerUser, request: Request) -> list[dict[str, Any]]:
    """List all leave requests submitted by washers in this branch."""
    branch_id = str(manager["branch_id"])
    washers = {w.id: (w.name or w.login_id) for w in db.query(Washer).filter(Washer.branch_id == branch_id).all()}
    rows = (
        db.query(WasherLeaveRequest)
        .filter(WasherLeaveRequest.branch_id == branch_id)
        .order_by(WasherLeaveRequest.leave_date.desc(), WasherLeaveRequest.created_at.desc())
        .all()
    )
    action_log("manager_list_leave_requests", "success", request, branch_id=branch_id, row_count=len(rows))
    return [_leave_request_to_dict_mgr(r, washers.get(r.washer_id, "")) for r in rows]


@router.patch("/leave-requests/{leave_request_id}")
def update_leave_request_status(
    leave_request_id: str,
    body: dict[str, Any],
    db: DbSession,
    manager: ManagerUser,
    request: Request,
) -> dict[str, Any]:
    """Approve or reject a washer leave request."""
    from datetime import datetime, timezone
    branch_id = str(manager["branch_id"])
    manager_id = str(manager["sub"])
    row = (
        db.query(WasherLeaveRequest)
        .filter(WasherLeaveRequest.id == leave_request_id, WasherLeaveRequest.branch_id == branch_id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Leave request not found", "code": "not_found"})
    new_status = str(body.get("status", "")).strip()
    if new_status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail={"detail": "status must be approved or rejected", "code": "validation_error"})
    row.status = new_status
    row.reviewed_by_manager_id = manager_id
    row.reviewed_at = datetime.now(timezone.utc).isoformat()
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    washer = db.query(Washer).filter(Washer.id == row.washer_id).one_or_none()
    washer_name = (washer.name or washer.login_id) if washer else ""
    audit_log("manager", manager_id, "update_leave_request_status", request, branch_id=branch_id, leave_request_id=leave_request_id, status=new_status)
    action_log("manager_update_leave_request", "success", request, branch_id=branch_id, leave_request_id=leave_request_id, status=new_status)
    return _leave_request_to_dict_mgr(row, washer_name)

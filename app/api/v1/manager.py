"""Branch manager API — bookings, washers, slot configuration."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, ManagerUser
from app.core.exceptions import AppError
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import Branch, BranchBooking, BranchSlotSettings, Washer
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
            address=body.address,
            vehicle_type=body.vehicle_type,
            service_summary=body.service_summary,
            service_id=body.service_id,
            slot_date=body.slot_date,
            start_time=body.start_time,
            end_time=body.end_time,
            source="walk_in",
            tip_cents=body.tip_cents,
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
    return booking_to_dict(job)


@router.patch("/bookings/{booking_id}")
def patch_booking(
    booking_id: str, body: BookingUpdate, db: DbSession, manager: ManagerUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    branch_id = str(manager["branch_id"])
    manager_id = str(manager["sub"])
    _branch_or_404(db, branch_id)
    job = (
        db.query(BranchBooking)
        .filter(BranchBooking.id == booking_id, BranchBooking.branch_id == branch_id)
        .one_or_none()
    )
    if not job:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    prev_status = job.status
    try:
        if "assigned_washer_id" in data:
            booking_service.assign_washer(db, job, data["assigned_washer_id"])
        if "bay_number" in data:
            booking_service.set_bay(db, job, data["bay_number"])
        if "status" in data and data["status"] is not None:
            st = str(data["status"])
            if st not in ("scheduled", "checked_in", "in_progress", "completed", "cancelled"):
                raise HTTPException(
                    status_code=400,
                    detail={"detail": "Invalid status", "code": "invalid_status"},
                )
            job.status = st
        if "notes" in data and data["notes"] is not None:
            job.notes = data["notes"]
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
    db.refresh(job)
    return booking_to_dict(job)


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

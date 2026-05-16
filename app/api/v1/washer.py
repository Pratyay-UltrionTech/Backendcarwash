"""Branch washer API — view assigned jobs and update status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, WasherUser
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import BranchBooking, WasherUnavailability, WasherLeaveRequest
from app.api.v1.serialize import booking_to_dict
from app.services.booking_pricing import branch_booking_customer_service_total_cents
from app.models.base import new_id
from app.schemas.staff import WasherLeaveRequestCreate
from app.services import loyalty_service

router = APIRouter(prefix="/washer", tags=["washer"])


@router.get("/jobs")
def list_assigned_jobs(db: DbSession, washer: WasherUser, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    washer_id = str(washer["sub"])
    branch_id = str(washer["branch_id"])
    rows = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch_id,
            BranchBooking.assigned_washer_id == washer_id,
            BranchBooking.status != "cancelled",
        )
        .order_by(BranchBooking.slot_date.desc(), BranchBooking.start_time)
        .all()
    )
    action_log(
        "washer_list_jobs",
        "success",
        request,
        branch_id=branch_id,
        washer_id=washer_id,
        row_count=len(rows),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    audit_log("washer", washer_id, "list_assigned_jobs", request, branch_id=branch_id, row_count=len(rows))
    out: list[dict[str, Any]] = []
    for x in rows:
        d = booking_to_dict(x, db)
        d["customer_service_total_cents"] = branch_booking_customer_service_total_cents(db, x)
        out.append(d)
    return out


@router.patch("/jobs/{booking_id}")
def patch_job_status(
    booking_id: str,
    body: dict[str, Any],
    db: DbSession,
    washer: WasherUser,
    request: Request,
) -> dict[str, Any]:
    started = monotonic_ms()
    washer_id = str(washer["sub"])
    branch_id = str(washer["branch_id"])
    job = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.id == booking_id,
            BranchBooking.branch_id == branch_id,
            BranchBooking.assigned_washer_id == washer_id,
        )
        .one_or_none()
    )
    if not job:
        action_log(
            "washer_patch_job_status",
            "failed",
            request,
            branch_id=branch_id,
            washer_id=washer_id,
            booking_id=booking_id,
            error_code="not_found",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})

    prev_status = job.status
    status = body.get("status")
    notes = body.get("notes")
    if status is not None:
        if status not in ("scheduled", "assigned", "arrived", "checked_in", "in_progress", "completed", "cancelled"):
            action_log(
                "washer_patch_job_status",
                "failed",
                request,
                branch_id=branch_id,
                washer_id=washer_id,
                booking_id=booking_id,
                error_code="invalid_status",
                latency_ms=round(monotonic_ms() - started, 2),
            )
            raise HTTPException(status_code=400, detail={"detail": "Invalid status", "code": "invalid_status"})
        job.status = status
        from datetime import datetime, timezone
        _now = datetime.now(timezone.utc)
        job.updated_by = washer_id
        job.updated_by_role = "washer"
        if status == "cancelled" and prev_status != "cancelled":
            job.cancelled_at = _now
            job.cancelled_by = washer_id
        elif status != "cancelled":
            job.cancelled_at = None
            job.cancelled_by = None
    if notes is not None:
        job.notes = str(notes)

    loyalty_service.on_branch_booking_status_change(db, job, prev_status)

    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log(
            "washer_patch_job_status",
            "failed",
            request,
            branch_id=branch_id,
            washer_id=washer_id,
            booking_id=booking_id,
            error_code="db_commit_failed",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(job)
    audit_log(
        "washer",
        washer_id,
        "update_job_status",
        request,
        branch_id=branch_id,
        booking_id=booking_id,
        status=job.status,
    )
    action_log(
        "washer_patch_job_status",
        "success",
        request,
        branch_id=branch_id,
        washer_id=washer_id,
        booking_id=booking_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    d = booking_to_dict(job, db)
    d["customer_service_total_cents"] = branch_booking_customer_service_total_cents(db, job)
    return d


def _unavailability_to_dict(u: WasherUnavailability) -> dict:
    return {
        "id": u.id,
        "washer_id": u.washer_id,
        "date": u.date,
        "all_day": u.all_day,
        "start_time": u.start_time or "",
        "end_time": u.end_time or "",
    }


@router.get("/unavailability")
def list_unavailability(db: DbSession, washer: WasherUser, request: Request) -> list[dict]:
    washer_id = str(washer["sub"])
    rows = (
        db.query(WasherUnavailability)
        .filter(WasherUnavailability.washer_id == washer_id)
        .order_by(WasherUnavailability.date.asc(), WasherUnavailability.start_time.asc())
        .all()
    )
    return [_unavailability_to_dict(r) for r in rows]


@router.post("/unavailability")
def add_unavailability(body: dict, db: DbSession, washer: WasherUser, request: Request) -> dict:
    washer_id = str(washer["sub"])
    date = str(body.get("date") or "").strip()
    if not date:
        raise HTTPException(status_code=400, detail={"detail": "date is required", "code": "validation_error"})
    all_day = bool(body.get("all_day", True))
    start_time = str(body.get("start_time") or "").strip()
    end_time = str(body.get("end_time") or "").strip()
    if not all_day and (not start_time or not end_time or start_time >= end_time):
        raise HTTPException(status_code=400, detail={"detail": "Valid start_time and end_time required for custom hours", "code": "validation_error"})
    row = WasherUnavailability(
        id=new_id(),
        washer_id=washer_id,
        date=date,
        all_day=all_day,
        start_time="" if all_day else start_time,
        end_time="" if all_day else end_time,
    )
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    action_log("washer_add_unavailability", "success", request, washer_id=washer_id, date=date)
    return _unavailability_to_dict(row)


@router.delete("/unavailability/{unavailability_id}")
def delete_unavailability(unavailability_id: str, db: DbSession, washer: WasherUser, request: Request) -> dict:
    washer_id = str(washer["sub"])
    row = (
        db.query(WasherUnavailability)
        .filter(WasherUnavailability.id == unavailability_id, WasherUnavailability.washer_id == washer_id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Not found", "code": "not_found"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    action_log("washer_delete_unavailability", "success", request, washer_id=washer_id, id=unavailability_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Leave requests
# ---------------------------------------------------------------------------

def _leave_request_to_dict(r: WasherLeaveRequest, washer_name: str = "") -> dict:
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
def list_leave_requests(db: DbSession, washer: WasherUser, request: Request) -> list[dict]:
    washer_id = str(washer["sub"])
    rows = (
        db.query(WasherLeaveRequest)
        .filter(WasherLeaveRequest.washer_id == washer_id)
        .order_by(WasherLeaveRequest.leave_date.desc())
        .all()
    )
    action_log("washer_list_leave_requests", "success", request, washer_id=washer_id, row_count=len(rows))
    return [_leave_request_to_dict(r) for r in rows]


@router.post("/leave-requests")
def submit_leave_request(body: WasherLeaveRequestCreate, db: DbSession, washer: WasherUser, request: Request) -> dict:
    washer_id = str(washer["sub"])
    branch_id = str(washer["branch_id"])
    leave_date = body.leave_date.strip()
    if not leave_date:
        raise HTTPException(status_code=400, detail={"detail": "leave_date is required", "code": "validation_error"})
    leave_type = body.leave_type.strip() if body.leave_type else "full_day"
    if leave_type not in ("full_day", "partial_day"):
        raise HTTPException(status_code=400, detail={"detail": "leave_type must be full_day or partial_day", "code": "validation_error"})
    start_time = body.start_time.strip() if body.start_time else ""
    end_time = body.end_time.strip() if body.end_time else ""
    if leave_type == "partial_day" and (not start_time or not end_time or start_time >= end_time):
        raise HTTPException(status_code=400, detail={"detail": "Valid start_time and end_time required for partial day leave", "code": "validation_error"})
    row = WasherLeaveRequest(
        id=new_id(),
        branch_id=branch_id,
        washer_id=washer_id,
        leave_date=leave_date,
        leave_type=leave_type,
        start_time="" if leave_type == "full_day" else start_time,
        end_time="" if leave_type == "full_day" else end_time,
        reason=body.reason.strip() if body.reason else "",
        status="pending",
    )
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    action_log("washer_submit_leave_request", "success", request, washer_id=washer_id, leave_date=leave_date)
    return _leave_request_to_dict(row)


@router.delete("/leave-requests/{leave_request_id}")
def cancel_leave_request(leave_request_id: str, db: DbSession, washer: WasherUser, request: Request) -> dict:
    washer_id = str(washer["sub"])
    row = (
        db.query(WasherLeaveRequest)
        .filter(WasherLeaveRequest.id == leave_request_id, WasherLeaveRequest.washer_id == washer_id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Leave request not found", "code": "not_found"})
    if row.status != "pending":
        raise HTTPException(status_code=400, detail={"detail": "Only pending requests can be cancelled", "code": "not_pending"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    action_log("washer_cancel_leave_request", "success", request, washer_id=washer_id, id=leave_request_id)
    return {"ok": True}

"""Branch washer API — view assigned jobs and update status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, WasherUser
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import BranchBooking
from app.api.v1.serialize import booking_to_dict
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
    return [booking_to_dict(x) for x in rows]


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
        if status not in ("scheduled", "checked_in", "in_progress", "completed", "cancelled"):
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
    return booking_to_dict(job)


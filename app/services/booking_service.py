from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from app.core.exceptions import AppError, ConflictError, NotFoundError
from app.models import Branch, BranchBooking, CatalogServiceItem, VehicleCatalogBlock, Washer
from app.services import slot_service
from app.services.duration_slots import snap_duration_to_base_slots
from app.services.jsonutil import dumps_json


def resolve_branch_booking_duration_minutes(
    db: Session, branch_id: str, service_id: str | None, addon_ids: list[str] | None
) -> int:
    from app.services.duration_slots import total_minutes_for_service_and_addons

    n_addons = len(addon_ids or [])
    if not service_id:
        return total_minutes_for_service_and_addons(60, n_addons)
    row = (
        db.query(CatalogServiceItem)
        .join(VehicleCatalogBlock, CatalogServiceItem.vehicle_block_id == VehicleCatalogBlock.id)
        .filter(VehicleCatalogBlock.branch_id == branch_id, CatalogServiceItem.id == service_id)
        .one_or_none()
    )
    if not row:
        return total_minutes_for_service_and_addons(60, n_addons)
    return total_minutes_for_service_and_addons(int(row.duration_minutes or 60), n_addons)


def _intervals_overlap_booking(
    start_a: str, end_a: str, start_b: str, end_b: str
) -> bool:
    a0, a1 = slot_service.booking_span_minutes(start_a, end_a)
    b0, b1 = slot_service.booking_span_minutes(start_b, end_b)
    return slot_service.intervals_overlap_minutes(a0, a1, b0, b1)


def washer_ids_busy_in_slot(
    db: Session,
    branch_id: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    exclude_booking_id: str | None = None,
) -> set[str]:
    q = db.query(BranchBooking).filter(
        BranchBooking.branch_id == branch_id,
        BranchBooking.slot_date == slot_date,
        BranchBooking.status != "cancelled",
        BranchBooking.assigned_washer_id.isnot(None),
    )
    if exclude_booking_id:
        q = q.filter(BranchBooking.id != exclude_booking_id)
    busy: set[str] = set()
    for b in q.all():
        if not b.assigned_washer_id:
            continue
        if _intervals_overlap_booking(start_time, end_time, b.start_time, b.end_time):
            busy.add(str(b.assigned_washer_id))
    return busy


def assert_slot_available(
    db: Session, branch: Branch, slot_date: str, start_time: str, end_time: str
) -> None:
    s0, s1 = slot_service.booking_span_minutes(start_time, end_time)
    dur = snap_duration_to_base_slots(s1 - s0)
    slot_service.assert_start_duration_bookable(db, branch, slot_date, start_time, dur)


def _parse_client_booking_id(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return str(uuid.uuid4())
    s = str(raw).strip()
    try:
        uuid.UUID(s)
    except ValueError as e:
        raise AppError("Invalid booking id", code="validation_error", status_code=400) from e
    return s


def create_booking(
    db: Session,
    branch: Branch,
    *,
    customer_name: str,
    phone: str,
    address: str,
    vehicle_type: str,
    service_summary: str,
    service_id: str | None = None,
    selected_addon_ids: list[str] | None = None,
    slot_date: str,
    start_time: str,
    end_time: str | None = None,
    source: str,
    bay_number: int | None = None,
    notes: str = "",
    tip_cents: int = 0,
    assigned_washer_id: str | None = None,
    booking_id: str | None = None,
) -> BranchBooking:
    addons = list(selected_addon_ids or [])
    if end_time:
        s0, s1 = slot_service.booking_span_minutes(start_time, end_time)
        dur = snap_duration_to_base_slots(s1 - s0)
        et = end_time
    else:
        dur = resolve_branch_booking_duration_minutes(db, branch.id, service_id, addons)
        et = slot_service.add_minutes_to_hhmm(start_time, dur)

    if bay_number is not None:
        if bay_number < 1 or bay_number > branch.bay_count:
            raise AppError("Bay number out of range", code="invalid_bay", status_code=400)
        if not slot_service.is_bay_available_for_interval(
            db, branch, slot_date, start_time, et, bay_number, exclude_booking_id=None
        ):
            raise ConflictError("Selected slot is not available", code="slot_unavailable")
        bay = bay_number
    else:
        bay = slot_service.allocate_bay_for_interval(db, branch, slot_date, start_time, et)
        if bay is None:
            raise ConflictError("Selected slot is not available", code="slot_unavailable")

    tip = max(0, int(tip_cents or 0))
    bid = _parse_client_booking_id(booking_id)

    job = BranchBooking(
        id=bid,
        branch_id=branch.id,
        customer_name=customer_name,
        phone=phone,
        address=address,
        vehicle_type=vehicle_type,
        service_summary=service_summary,
        service_id=(service_id.strip() if isinstance(service_id, str) and service_id.strip() else None),
        selected_addon_ids_json=dumps_json(addons),
        slot_date=slot_date,
        start_time=start_time,
        end_time=et,
        bay_number=bay,
        assigned_washer_id=None,
        status="scheduled",
        source=source,
        notes=notes,
        tip_cents=tip,
    )
    db.add(job)
    db.flush()
    if assigned_washer_id:
        assign_washer(db, job, assigned_washer_id.strip() if isinstance(assigned_washer_id, str) else None)
    return job


def assign_washer(
    db: Session,
    booking: BranchBooking,
    washer_id: str | None,
) -> BranchBooking:
    if washer_id is None:
        booking.assigned_washer_id = None
        return booking
    washer = (
        db.query(Washer)
        .filter(Washer.id == washer_id, Washer.branch_id == booking.branch_id, Washer.active.is_(True))
        .one_or_none()
    )
    if not washer:
        raise NotFoundError("Washer not found for this branch")
    busy = washer_ids_busy_in_slot(
        db,
        booking.branch_id,
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        exclude_booking_id=booking.id,
    )
    if washer_id in busy:
        raise ConflictError("Washer is already assigned in this time window", code="washer_busy")
    booking.assigned_washer_id = washer_id
    return booking


def set_bay(db: Session, booking: BranchBooking, bay_number: int | None) -> BranchBooking:
    if bay_number is None:
        booking.bay_number = None
        return booking
    branch = db.query(Branch).filter(Branch.id == booking.branch_id).one()
    if bay_number < 1 or bay_number > branch.bay_count:
        raise AppError("Bay number out of range", code="invalid_bay", status_code=400)
    booking.bay_number = bay_number
    return booking


def assert_branch_bay_interval_free(
    db: Session,
    branch_id: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    bay_number: int,
    exclude_booking_id: str | None,
) -> None:
    q = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch_id,
            BranchBooking.slot_date == slot_date,
            BranchBooking.status != "cancelled",
            BranchBooking.bay_number == bay_number,
        )
        .all()
    )
    if exclude_booking_id:
        q = [b for b in q if b.id != exclude_booking_id]
    for other in q:
        if _intervals_overlap_booking(start_time, end_time, other.start_time, other.end_time):
            raise ConflictError("That bay is already assigned for this time window", code="bay_unavailable")
    # Bookings with null bay still overlap everyone on time overlap
    floating = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch_id,
            BranchBooking.slot_date == slot_date,
            BranchBooking.status != "cancelled",
            BranchBooking.bay_number.is_(None),
        )
        .all()
    )
    if exclude_booking_id:
        floating = [b for b in floating if b.id != exclude_booking_id]
    for other in floating:
        if _intervals_overlap_booking(start_time, end_time, other.start_time, other.end_time):
            raise ConflictError("That bay is already assigned for this time window", code="bay_unavailable")


def assert_branch_slot_has_capacity_after_update(
    db: Session,
    branch: Branch,
    slot_date: str,
    start_time: str,
    end_time: str,
    *,
    exclude_booking_id: str,
) -> None:
    s0, s1 = slot_service.booking_span_minutes(start_time, end_time)
    dur = snap_duration_to_base_slots(s1 - s0)
    bay = slot_service.allocate_bay_for_interval(
        db, branch, slot_date, start_time, end_time, exclude_booking_id=exclude_booking_id
    )
    if bay is None:
        raise ConflictError("Selected slot is not available", code="slot_unavailable")


def assert_branch_bay_free_excluding(
    db: Session,
    branch_id: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    bay_number: int,
    exclude_booking_id: str,
) -> None:
    assert_branch_bay_interval_free(
        db, branch_id, slot_date, start_time, end_time, bay_number, exclude_booking_id=exclude_booking_id
    )


def patch_branch_booking_fields(db: Session, branch: Branch, job: BranchBooking, data: dict[str, Any]) -> None:
    """Apply mutable fields from ``data`` (BookingUpdate.model_dump(exclude_unset=True)) onto ``job``."""
    slot_keys = ("slot_date", "start_time", "end_time")
    if any(k in data for k in slot_keys):
        sd = str(data.get("slot_date", job.slot_date))
        st = str(data.get("start_time", job.start_time))
        et = str(data.get("end_time", job.end_time))
        assert_branch_slot_has_capacity_after_update(db, branch, sd, st, et, exclude_booking_id=job.id)
        job.slot_date = sd
        job.start_time = st
        job.end_time = et

    if "bay_number" in data:
        set_bay(db, job, data["bay_number"])
    if job.bay_number is not None:
        assert_branch_bay_free_excluding(
            db, branch.id, job.slot_date, job.start_time, job.end_time, job.bay_number, job.id
        )

    if "customer_name" in data and data["customer_name"] is not None:
        job.customer_name = str(data["customer_name"]).strip()
    if "phone" in data and data["phone"] is not None:
        job.phone = str(data["phone"]).strip()
    if "address" in data and data["address"] is not None:
        job.address = str(data["address"]).strip()
    if "vehicle_type" in data and data["vehicle_type"] is not None:
        job.vehicle_type = str(data["vehicle_type"]).strip()
    if "service_summary" in data and data["service_summary"] is not None:
        job.service_summary = str(data["service_summary"]).strip()
    if "service_id" in data:
        sid = data["service_id"]
        job.service_id = sid.strip() if isinstance(sid, str) and sid.strip() else None
    if "selected_addon_ids" in data and data["selected_addon_ids"] is not None:
        job.selected_addon_ids_json = dumps_json(list(data["selected_addon_ids"]))
    if "tip_cents" in data and data["tip_cents"] is not None:
        job.tip_cents = max(0, int(data["tip_cents"]))

    if "assigned_washer_id" in data:
        assign_washer(db, job, data["assigned_washer_id"])

    if "status" in data and data["status"] is not None:
        st = str(data["status"])
        if st not in ("scheduled", "checked_in", "in_progress", "completed", "cancelled"):
            raise AppError("Invalid status", code="invalid_status", status_code=400)
        job.status = st

    if "notes" in data and data["notes"] is not None:
        job.notes = str(data["notes"])

from __future__ import annotations

from sqlalchemy.orm import Session

from app.core.exceptions import AppError, ConflictError, NotFoundError
from app.models import Branch, BranchBooking, BranchSlotSettings, Washer
from app.services import slot_service


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
        BranchBooking.start_time == start_time,
        BranchBooking.end_time == end_time,
        BranchBooking.status != "cancelled",
        BranchBooking.assigned_washer_id.isnot(None),
    )
    if exclude_booking_id:
        q = q.filter(BranchBooking.id != exclude_booking_id)
    return {str(b.assigned_washer_id) for b in q.all() if b.assigned_washer_id}


def assert_slot_available(
    db: Session, branch: Branch, slot_date: str, start_time: str, end_time: str
) -> None:
    slots = slot_service.list_available_slots(db, branch, slot_date)
    slot = next((s for s in slots if s["startTime"] == start_time and s["endTime"] == end_time), None)
    if not slot or slot["available"] <= 0:
        raise ConflictError("Selected slot is not available", code="slot_unavailable")


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
    slot_date: str,
    start_time: str,
    end_time: str,
    source: str,
    bay_number: int | None = None,
    notes: str = "",
    tip_cents: int = 0,
) -> BranchBooking:
    assert_slot_available(db, branch, slot_date, start_time, end_time)
    tip = max(0, int(tip_cents or 0))
    job = BranchBooking(
        branch_id=branch.id,
        customer_name=customer_name,
        phone=phone,
        address=address,
        vehicle_type=vehicle_type,
        service_summary=service_summary,
        service_id=(service_id.strip() if isinstance(service_id, str) and service_id.strip() else None),
        slot_date=slot_date,
        start_time=start_time,
        end_time=end_time,
        bay_number=bay_number,
        assigned_washer_id=None,
        status="scheduled",
        source=source,
        notes=notes,
        tip_cents=tip,
    )
    db.add(job)
    db.flush()
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


def count_bookings_in_branch_slot(
    db: Session,
    branch_id: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    *,
    exclude_booking_id: str | None = None,
) -> int:
    q = db.query(BranchBooking).filter(
        BranchBooking.branch_id == branch_id,
        BranchBooking.slot_date == slot_date,
        BranchBooking.start_time == start_time,
        BranchBooking.end_time == end_time,
        BranchBooking.status != "cancelled",
    )
    if exclude_booking_id:
        q = q.filter(BranchBooking.id != exclude_booking_id)
    return int(q.count())


def assert_branch_slot_has_capacity_after_update(
    db: Session,
    branch: Branch,
    slot_date: str,
    start_time: str,
    end_time: str,
    *,
    exclude_booking_id: str,
) -> None:
    settings_row = (
        db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
    )
    capacity = slot_service.get_open_bays_for_slot(settings_row, slot_date, start_time, end_time, branch.bay_count)
    if capacity <= 0:
        raise ConflictError("That bay or window is closed for this date", code="slot_unavailable")
    booked = count_bookings_in_branch_slot(
        db, branch.id, slot_date, start_time, end_time, exclude_booking_id=exclude_booking_id
    )
    if booked >= capacity:
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
    other = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch_id,
            BranchBooking.slot_date == slot_date,
            BranchBooking.start_time == start_time,
            BranchBooking.end_time == end_time,
            BranchBooking.bay_number == bay_number,
            BranchBooking.status != "cancelled",
            BranchBooking.id != exclude_booking_id,
        )
        .first()
    )
    if other:
        raise ConflictError("That bay is already assigned for this time window", code="bay_unavailable")


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

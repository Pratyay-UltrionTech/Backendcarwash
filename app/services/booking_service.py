from __future__ import annotations

from sqlalchemy.orm import Session

from app.core.exceptions import AppError, ConflictError, NotFoundError
from app.models import Branch, BranchBooking, Washer
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

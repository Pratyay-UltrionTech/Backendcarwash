"""Shared booking status presentation rules."""

from __future__ import annotations

from datetime import datetime


FINAL_COMPLETED = "completed"
FINAL_CANCELLED = "cancelled"

# Intermediate operational statuses set by washer/driver — these represent active work
# and must never be auto-cancelled by the time-based fallback logic.
_INTERMEDIATE_STATUSES = frozenset({"assigned", "arrived", "checked_in", "in_progress"})

# Manager portal: no reschedule or staff reassignment once work has started.
_SCHEDULE_STAFF_LOCKED_STATUSES = frozenset({"in_progress", "checked_in"})


def effective_status(raw_status: str | None, slot_date: str | None, end_time: str | None) -> str:
    """
    Return the effective display status for a booking.

    Rules:
    - Explicit final statuses (completed, cancelled) always win — never overridden.
    - Intermediate operational statuses (arrived, checked_in, in_progress) are returned
      as-is; they represent active staff work and must not be auto-cancelled.
    - Only unstarted (scheduled) bookings whose end datetime has passed are treated as
      cancelled automatically.
    - Otherwise return normalized raw status (fallback: scheduled).
    """
    s = str(raw_status or "scheduled").strip().lower()
    if s == FINAL_COMPLETED:
        return FINAL_COMPLETED
    if s in (FINAL_CANCELLED, "canceled"):
        return FINAL_CANCELLED

    # Active intermediate statuses must not be auto-cancelled by the time fallback.
    if s in _INTERMEDIATE_STATUSES:
        return s

    # Only auto-cancel unstarted (scheduled) bookings whose slot has elapsed.
    if s == "scheduled":
        sd = str(slot_date or "").strip()
        et = str(end_time or "").strip()
        if sd and et:
            try:
                booking_end = datetime.fromisoformat(f"{sd}T{et}")
                if booking_end < datetime.now():
                    return FINAL_CANCELLED
            except ValueError:
                pass

    return s or "scheduled"


def assert_schedule_and_staff_editable(
    current_status: str | None,
    data: dict,
    *,
    current_slot: tuple[str, str, str],
    current_staff_id: str | None,
    current_bay: int | None = None,
    staff_field: str = "assigned_washer_id",
) -> None:
    """Reject PATCH attempts to reschedule or reassign staff while work is in progress."""
    from app.core.exceptions import AppError

    st = str(current_status or "scheduled").strip().lower()
    if st not in _SCHEDULE_STAFF_LOCKED_STATUSES:
        return

    slot_keys = ("slot_date", "start_time", "end_time")
    if any(k in data for k in slot_keys):
        sd = str(data.get("slot_date", current_slot[0]))
        start_t = str(data.get("start_time", current_slot[1]))
        end_t = str(data.get("end_time", current_slot[2]))
        if (sd, start_t, end_t) != current_slot:
            raise AppError(
                "Cannot reschedule while work is in progress",
                code="schedule_locked",
                status_code=409,
            )

    if current_bay is not None and "bay_number" in data and data["bay_number"] is not None:
        try:
            new_bay = int(data["bay_number"])
        except (TypeError, ValueError):
            new_bay = None
        if new_bay is not None and new_bay != current_bay:
            raise AppError(
                "Cannot change bay while work is in progress",
                code="schedule_locked",
                status_code=409,
            )

    if staff_field in data:
        new_staff = data[staff_field]
        if isinstance(new_staff, str) and not new_staff.strip():
            new_staff = None
        if new_staff != current_staff_id:
            label = "driver" if staff_field == "assigned_driver_id" else "washer"
            raise AppError(
                f"Cannot change {label} while work is in progress",
                code="staff_locked",
                status_code=409,
            )

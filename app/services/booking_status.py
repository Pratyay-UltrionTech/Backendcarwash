"""Shared booking status presentation rules."""

from __future__ import annotations

from datetime import datetime


FINAL_COMPLETED = "completed"
FINAL_CANCELLED = "cancelled"

# Intermediate operational statuses set by washer/driver — these represent active work
# and must never be auto-cancelled by the time-based fallback logic.
_INTERMEDIATE_STATUSES = frozenset({"assigned", "arrived", "checked_in", "in_progress"})


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

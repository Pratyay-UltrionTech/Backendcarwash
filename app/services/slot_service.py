"""Slot generation and bay availability — 30-minute base grid + multi-slot bookings."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models import Branch, BranchBooking, BranchSlotSettings

_MISSING = object()
from app.services.duration_slots import BASE_SLOT_MINUTES, snap_duration_to_base_slots, slots_needed_for_duration
from app.services.jsonutil import loads_json_object


def parse_time_to_minutes(t: str) -> int:
    parts = t.split(":")
    h = int(parts[0]) if parts and parts[0].isdigit() else 0
    m = int(parts[1]) if len(parts) > 1 and str(parts[1]).isdigit() else 0
    return h * 60 + m


def format_minutes_to_hhmm(total: int) -> str:
    m = ((total % (24 * 60)) + 24 * 60) % (24 * 60)
    h = m // 60
    mn = m % 60
    return f"{h:02d}:{mn:02d}"


def add_minutes_to_hhmm(hhmm: str, delta_minutes: int) -> str:
    return format_minutes_to_hhmm(parse_time_to_minutes(hhmm) + int(delta_minutes))


def slot_window_key(start_time: str, end_time: str) -> str:
    return f"{start_time}|{end_time}"


def day_window_key(iso_date: str, window_key: str) -> str:
    return f"{iso_date}|{window_key}"


def _operating_range_minutes(open_time: str, close_time: str) -> tuple[int, int]:
    open_m = parse_time_to_minutes(open_time)
    close_m = parse_time_to_minutes(close_time)
    if close_m <= open_m:
        close_m += 24 * 60
    return open_m, close_m


def booking_span_minutes(start_time: str, end_time: str) -> tuple[int, int]:
    s = parse_time_to_minutes(start_time)
    e = parse_time_to_minutes(end_time)
    if e <= s:
        e += 24 * 60
    return s, e


def intervals_overlap_minutes(a0: int, a1: int, b0: int, b1: int) -> bool:
    """Half-open [a0,a1) vs [b0,b1)."""
    return a0 < b1 and b0 < a1


def generate_operating_day_slots(
    open_time: str, close_time: str, bay_count: int, duration_minutes: int
) -> list[dict[str, str]]:
    """Legacy helper: step the day by ``duration_minutes`` (used by admin previews)."""
    _ = bay_count
    open_m, close_m = _operating_range_minutes(open_time, close_time)
    dur = max(BASE_SLOT_MINUTES, snap_duration_to_base_slots(int(duration_minutes or BASE_SLOT_MINUTES)))
    slots: list[dict[str, str]] = []
    for t in range(open_m, close_m - dur + 1, dur):
        slots.append(
            {
                "startTime": format_minutes_to_hhmm(t),
                "endTime": format_minutes_to_hhmm(t + dur),
            }
        )
    return slots


def bay_open_flags_for_window(
    settings_row: BranchSlotSettings | None,
    date_iso: str,
    start_time: str,
    end_time: str,
    bay_count: int,
) -> list[bool]:
    wk = slot_window_key(start_time, end_time)
    slot_active = True
    if settings_row:
        active_map = loads_json_object(settings_row.slot_window_active_by_key_json)
        if wk in active_map and active_map[wk] is False:
            slot_active = False
    bays = [True] * max(1, bay_count)
    if settings_row:
        recurring_raw = loads_json_object(settings_row.slot_bay_open_by_window_json)
        recurring = recurring_raw.get(wk) if isinstance(recurring_raw.get(wk), list) else None
        if isinstance(recurring, list):
            bays = [
                (recurring[i] is not False) if i < len(recurring) else True for i in range(len(bays))
            ]
        day_states = loads_json_object(settings_row.slot_day_states_json)
        dk = day_window_key(date_iso, wk)
        override = day_states.get(dk)
        if isinstance(override, dict):
            if isinstance(override.get("slotActive"), bool):
                slot_active = bool(override["slotActive"])
            bo = override.get("baysOpen")
            if isinstance(bo, list):
                bays = [
                    bays[i] and (bo[i] is not False) if i < len(bo) else bays[i] for i in range(len(bays))
                ]
    if not slot_active:
        return [False] * len(bays)
    return bays


def get_open_bays_for_slot(
    settings_row: BranchSlotSettings | None,
    date_iso: str,
    start_time: str,
    end_time: str,
    bay_count: int,
) -> int:
    return sum(1 for b in bay_open_flags_for_window(settings_row, date_iso, start_time, end_time, bay_count) if b)


def _segment_blocks_bay(booking: BranchBooking, seg_s: int, seg_e: int, bay: int) -> bool:
    if booking.status == "cancelled":
        return False
    bs, be = booking_span_minutes(booking.start_time, booking.end_time)
    if not intervals_overlap_minutes(seg_s, seg_e, bs, be):
        return False
    if booking.bay_number is None:
        return True
    return int(booking.bay_number) == bay


def is_bay_available_for_interval(
    db: Session,
    branch: Branch,
    date_iso: str,
    start_time: str,
    end_time: str,
    bay: int,
    *,
    exclude_booking_id: str | None = None,
    settings_row: Any = _MISSING,
    bookings: Any = _MISSING,
) -> bool:
    """True if ``bay`` (1-based) is open for every base segment and not overlapped by bookings."""
    if settings_row is _MISSING:
        settings_row = (
            db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
        )
    if bookings is _MISSING:
        bookings = (
            db.query(BranchBooking)
            .filter(
                BranchBooking.branch_id == branch.id,
                BranchBooking.slot_date == date_iso,
                BranchBooking.status != "cancelled",
            )
            .all()
        )
    if exclude_booking_id:
        bookings = [b for b in bookings if b.id != exclude_booking_id]

    s0, s1 = booking_span_minutes(start_time, end_time)
    open_m, close_m = _operating_range_minutes(branch.open_time, branch.close_time)
    if s0 < open_m or s1 > close_m:
        return False

    bays_n = max(1, int(branch.bay_count or 1))
    if bay < 1 or bay > bays_n:
        return False

    t = s0
    while t < s1:
        t_next = t + BASE_SLOT_MINUTES
        st = format_minutes_to_hhmm(t)
        et = format_minutes_to_hhmm(t_next)
        flags = bay_open_flags_for_window(settings_row, date_iso, st, et, bays_n)
        if not flags[bay - 1]:
            return False
        for j in bookings:
            if _segment_blocks_bay(j, t, t_next, bay):
                return False
        t = t_next
    return True


def allocate_bay_for_interval(
    db: Session,
    branch: Branch,
    date_iso: str,
    start_time: str,
    end_time: str,
    *,
    exclude_booking_id: str | None = None,
) -> int | None:
    """Pick the lowest bay number that is open for every 30-minute segment and not overlapped by bookings."""
    s0, s1 = booking_span_minutes(start_time, end_time)
    open_m, close_m = _operating_range_minutes(branch.open_time, branch.close_time)
    if s0 < open_m or s1 > close_m:
        return None

    bays_n = max(1, int(branch.bay_count or 1))
    for bay in range(1, bays_n + 1):
        if is_bay_available_for_interval(
            db, branch, date_iso, start_time, end_time, bay, exclude_booking_id=exclude_booking_id
        ):
            return bay
    return None


def is_bay_open_schedule_only(
    settings_row: BranchSlotSettings | None,
    branch: Branch,
    date_iso: str,
    start_time: str,
    end_time: str,
    bay: int,
) -> bool:
    """Bay is within branch hours and open in slot settings for every base segment (ignores bookings)."""
    s0, s1 = booking_span_minutes(start_time, end_time)
    open_m, close_m = _operating_range_minutes(branch.open_time, branch.close_time)
    if s0 < open_m or s1 > close_m:
        return False
    bays_n = max(1, int(branch.bay_count or 1))
    if bay < 1 or bay > bays_n:
        return False
    t = s0
    while t < s1:
        t_next = t + BASE_SLOT_MINUTES
        st = format_minutes_to_hhmm(t)
        et = format_minutes_to_hhmm(t_next)
        flags = bay_open_flags_for_window(settings_row, date_iso, st, et, bays_n)
        if not flags[bay - 1]:
            return False
        t = t_next
    return True


def count_schedule_open_bays(
    settings_row: BranchSlotSettings | None,
    branch: Branch,
    date_iso: str,
    start_time: str,
    end_time: str,
) -> int:
    bays_n = max(1, int(branch.bay_count or 1))
    n = 0
    for bay in range(1, bays_n + 1):
        if is_bay_open_schedule_only(settings_row, branch, date_iso, start_time, end_time, bay):
            n += 1
    return n


def count_bays_available_for_start_duration(
    db: Session,
    branch: Branch,
    date_iso: str,
    start_time: str,
    duration_minutes: int,
    *,
    settings_row: Any = _MISSING,
    bookings: Any = _MISSING,
) -> int:
    dur = snap_duration_to_base_slots(duration_minutes)
    end_time = add_minutes_to_hhmm(start_time, dur)
    if settings_row is _MISSING:
        settings_row = (
            db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
        )
    if bookings is _MISSING:
        bookings = (
            db.query(BranchBooking)
            .filter(
                BranchBooking.branch_id == branch.id,
                BranchBooking.slot_date == date_iso,
                BranchBooking.status != "cancelled",
            )
            .all()
        )
    bays_n = max(1, int(branch.bay_count or 1))
    count = 0
    for bay in range(1, bays_n + 1):
        if is_bay_available_for_interval(
            db, branch, date_iso, start_time, end_time, bay, settings_row=settings_row, bookings=bookings
        ):
            count += 1
    return count


def list_available_slots(
    db: Session,
    branch: Branch,
    date_iso: str,
    *,
    booking_duration_minutes: int | None = None,
) -> list[dict[str, Any]]:
    """List bookable **start** times; each row's ``endTime`` is start + ``booking_duration_minutes``."""
    dur = snap_duration_to_base_slots(int(booking_duration_minutes or BASE_SLOT_MINUTES))
    slots_needed = slots_needed_for_duration(dur)
    open_m, close_m = _operating_range_minutes(branch.open_time, branch.close_time)
    settings_row = (
        db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
    )
    day_bookings = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch.id,
            BranchBooking.slot_date == date_iso,
            BranchBooking.status != "cancelled",
        )
        .all()
    )
    bays_n = max(1, int(branch.bay_count or 1))
    out: list[dict[str, Any]] = []
    for t in range(open_m, close_m - dur + 1, BASE_SLOT_MINUTES):
        st = format_minutes_to_hhmm(t)
        et = format_minutes_to_hhmm(t + dur)
        available = count_bays_available_for_start_duration(
            db, branch, date_iso, st, dur, settings_row=settings_row, bookings=day_bookings
        )
        schedule_open = count_schedule_open_bays(settings_row, branch, date_iso, st, et)
        out.append(
            {
                "startTime": st,
                "endTime": et,
                "label": f"{_format_12h(st)} – {_format_12h(et)} ({dur} min)",
                "capacity": bays_n,
                "booked": max(0, bays_n - available),
                "available": available,
                "scheduleOpenBays": schedule_open,
                "durationMinutes": dur,
                "slotsNeeded": slots_needed,
            }
        )
    return out


def assert_start_duration_bookable(
    db: Session,
    branch: Branch,
    slot_date: str,
    start_time: str,
    duration_minutes: int,
    *,
    exclude_booking_id: str | None = None,
) -> None:
    from app.core.exceptions import ConflictError

    dur = snap_duration_to_base_slots(duration_minutes)
    end_time = add_minutes_to_hhmm(start_time, dur)
    bay = allocate_bay_for_interval(
        db, branch, slot_date, start_time, end_time, exclude_booking_id=exclude_booking_id
    )
    if bay is None:
        raise ConflictError("Selected slot is not available", code="slot_unavailable")


def _format_12h(hhmm: str) -> str:
    parts = hhmm.split(":")
    h = int(parts[0]) if parts and str(parts[0]).isdigit() else 0
    m = parts[1] if len(parts) > 1 else "00"
    suffix = "PM" if h >= 12 else "AM"
    if h == 0:
        h12 = 12
    elif h > 12:
        h12 = h - 12
    else:
        h12 = h
    return f"{h12:02d}:{m} {suffix}"


def in_date_range(date_iso: str, start: str | None, end: str | None) -> bool:
    if start and date_iso < start:
        return False
    if end and date_iso > end:
        return False
    return True


def in_time_range(time_hhmm: str, start: str | None, end: str | None) -> bool:
    if not start or not end:
        return True
    t = parse_time_to_minutes(time_hhmm)
    return parse_time_to_minutes(start) <= t < parse_time_to_minutes(end)


def day_short_name(date_iso: str) -> str:
    from datetime import datetime

    d = datetime.fromisoformat(f"{date_iso}T00:00:00")
    return d.strftime("%a")

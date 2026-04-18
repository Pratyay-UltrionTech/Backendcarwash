"""Slot generation and bay availability — mirrors USER adminPortalBridge slot logic."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models import Branch, BranchBooking, BranchSlotSettings
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


def slot_window_key(start_time: str, end_time: str) -> str:
    return f"{start_time}|{end_time}"


def day_window_key(iso_date: str, window_key: str) -> str:
    return f"{iso_date}|{window_key}"


def generate_operating_day_slots(
    open_time: str, close_time: str, bay_count: int, duration_minutes: int
) -> list[dict[str, str]]:
    _ = bay_count
    open_m = parse_time_to_minutes(open_time)
    close_m = parse_time_to_minutes(close_time)
    if close_m <= open_m:
        close_m += 24 * 60
    dur = max(15, duration_minutes or 60)
    slots: list[dict[str, str]] = []
    for t in range(open_m, close_m - dur + 1, dur):
        slots.append(
            {
                "startTime": format_minutes_to_hhmm(t),
                "endTime": format_minutes_to_hhmm(t + dur),
            }
        )
    return slots


def get_open_bays_for_slot(
    settings_row: BranchSlotSettings | None,
    date_iso: str,
    start_time: str,
    end_time: str,
    bay_count: int,
) -> int:
    wk = slot_window_key(start_time, end_time)
    slot_active = True
    if settings_row:
        active_map = loads_json_object(settings_row.slot_window_active_by_key_json)
        if wk in active_map and active_map[wk] is False:
            slot_active = False
    bays = [True] * max(1, bay_count)
    if settings_row:
        recurring_raw = loads_json_object(settings_row.slot_bay_open_by_window_json)
        # stored as {"HH:mm|HH:mm": [bool,...]}
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
        return 0
    return sum(1 for b in bays if b)


def list_available_slots(
    db: Session,
    branch: Branch,
    date_iso: str,
) -> list[dict[str, Any]]:
    settings_row = (
        db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none()
    )
    duration = 60
    if settings_row:
        duration = max(15, settings_row.manager_slot_duration_minutes or 60)
    slots = generate_operating_day_slots(
        branch.open_time, branch.close_time, branch.bay_count, duration
    )
    bookings = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch.id,
            BranchBooking.slot_date == date_iso,
            BranchBooking.status != "cancelled",
        )
        .all()
    )
    out: list[dict[str, Any]] = []
    for slot in slots:
        st, et = slot["startTime"], slot["endTime"]
        capacity = get_open_bays_for_slot(settings_row, date_iso, st, et, branch.bay_count)
        booked = sum(
            1
            for j in bookings
            if j.start_time == st and j.end_time == et
        )
        available = max(0, capacity - booked)
        out.append(
            {
                "startTime": st,
                "endTime": et,
                "label": f"{_format_12h(st)} - {_format_12h(et)}",
                "capacity": capacity,
                "booked": booked,
                "available": available,
            }
        )
    return out


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

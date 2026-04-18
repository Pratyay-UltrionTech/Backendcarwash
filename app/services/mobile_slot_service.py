from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.models import MobileBooking, MobileServiceDriver, MobileServiceManager, MobileSlotSettings
from app.services.jsonutil import loads_json_array, loads_json_object


def _parse_hhmm(v: str) -> int:
    try:
        h, m = str(v).split(":")
        hh = max(0, min(23, int(h)))
        mm = max(0, min(59, int(m)))
        return hh * 60 + mm
    except Exception:
        return 0


def _fmt_hhmm(total: int) -> str:
    total = total % (24 * 60)
    h = total // 60
    m = total % 60
    return f"{h:02d}:{m:02d}"


def _slot_key(start_time: str, end_time: str) -> str:
    return f"{start_time}|{end_time}"


def _day_key(date_iso: str, start_time: str, end_time: str) -> str:
    return f"{date_iso}|{_slot_key(start_time, end_time)}"


def _driver_busy(
    db: Session,
    city_pin_code: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    driver_id: str,
    exclude_booking_id: str | None = None,
) -> bool:
    q = db.query(MobileBooking).filter(
        MobileBooking.city_pin_code == city_pin_code,
        MobileBooking.slot_date == slot_date,
        MobileBooking.start_time == start_time,
        MobileBooking.end_time == end_time,
        MobileBooking.assigned_driver_id == driver_id,
        MobileBooking.status != "cancelled",
    )
    if exclude_booking_id:
        q = q.filter(MobileBooking.id != exclude_booking_id)
    return q.first() is not None


@dataclass
class SlotAvailability:
    start_time: str
    end_time: str
    open_driver_ids: list[str]
    booked: int

    @property
    def capacity(self) -> int:
        return len(self.open_driver_ids)

    @property
    def available(self) -> int:
        return max(0, self.capacity - self.booked)


def _open_driver_ids_for_slot(
    slot_settings: MobileSlotSettings | None,
    active_drivers: list[MobileServiceDriver],
    slot_date: str,
    start_time: str,
    end_time: str,
) -> list[str]:
    if not active_drivers:
        return []

    ids = [d.id for d in active_drivers]
    if slot_settings is None:
        return ids

    wk = _slot_key(start_time, end_time)
    window_active = loads_json_object(slot_settings.slot_window_active_by_key_json).get(wk, True)
    if window_active is False:
        return []

    mask = loads_json_object(slot_settings.slot_driver_open_by_window_json).get(wk)
    open_mask = [True] * len(ids)
    if isinstance(mask, list):
        open_mask = [bool(mask[i]) if i < len(mask) else True for i in range(len(ids))]

    day_state = loads_json_object(slot_settings.slot_day_states_json).get(_day_key(slot_date, start_time, end_time), {})
    if isinstance(day_state, dict):
        if day_state.get("slotActive") is False:
            return []
        day_mask = day_state.get("driversOpen")
        if isinstance(day_mask, list):
            open_mask = [open_mask[i] and (bool(day_mask[i]) if i < len(day_mask) else True) for i in range(len(ids))]

    return [ids[i] for i in range(len(ids)) if open_mask[i]]


def list_slot_availability(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
) -> list[SlotAvailability]:
    slot_settings = (
        db.query(MobileSlotSettings)
        .filter(MobileSlotSettings.manager_id == manager.id)
        .one_or_none()
    )
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(
            MobileServiceDriver.manager_id == manager.id,
            MobileServiceDriver.active.is_(True),
        )
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )

    duration = max(15, int(slot_settings.slot_duration_minutes if slot_settings else 60))
    open_time = _parse_hhmm(slot_settings.open_time if slot_settings else "08:00")
    close_time = _parse_hhmm(slot_settings.close_time if slot_settings else "18:00")
    if close_time <= open_time:
        close_time += 24 * 60

    out: list[SlotAvailability] = []
    t = open_time
    while t + duration <= close_time:
        st = _fmt_hhmm(t)
        et = _fmt_hhmm(t + duration)
        open_ids = _open_driver_ids_for_slot(slot_settings, active_drivers, slot_date, st, et)
        booked = (
            db.query(MobileBooking)
            .filter(
                MobileBooking.city_pin_code == manager.city_pin_code,
                MobileBooking.slot_date == slot_date,
                MobileBooking.start_time == st,
                MobileBooking.end_time == et,
                MobileBooking.status != "cancelled",
            )
            .count()
        )
        out.append(SlotAvailability(start_time=st, end_time=et, open_driver_ids=open_ids, booked=booked))
        t += duration
    return out


def assert_slot_available(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
) -> None:
    rows = list_slot_availability(db, manager, slot_date)
    row = next((r for r in rows if r.start_time == start_time and r.end_time == end_time), None)
    if not row or row.available <= 0:
        raise ValueError("slot_unavailable")


def assert_driver_assignable(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
    driver_id: str,
    exclude_booking_id: str | None = None,
) -> None:
    rows = list_slot_availability(db, manager, slot_date)
    row = next((r for r in rows if r.start_time == start_time and r.end_time == end_time), None)
    if not row:
        raise ValueError("slot_unavailable")
    if driver_id not in row.open_driver_ids:
        raise ValueError("driver_not_open")
    if _driver_busy(
        db,
        manager.city_pin_code,
        slot_date,
        start_time,
        end_time,
        driver_id,
        exclude_booking_id=exclude_booking_id,
    ):
        raise ValueError("driver_busy")


def manager_for_service_pin(
    db: Session,
    pin_code: str,
) -> tuple[MobileServiceManager | None, int]:
    pin = "".join(ch for ch in str(pin_code) if ch.isdigit())
    if not pin:
        return None, 0
    managers = db.query(MobileServiceManager).filter(MobileServiceManager.active.is_(True)).all()
    best: MobileServiceManager | None = None
    best_count = 0
    for m in managers:
        drivers = (
            db.query(MobileServiceDriver)
            .filter(
                MobileServiceDriver.manager_id == m.id,
                MobileServiceDriver.active.is_(True),
            )
            .all()
        )
        count = 0
        for d in drivers:
            zips = set(loads_json_array(d.serviceable_zip_codes_json))
            if pin == d.service_pin_code or pin in zips:
                count += 1
        if count > best_count:
            best = m
            best_count = count
        elif count == best_count and count > 0 and best is not None:
            if m.city_pin_code < best.city_pin_code:
                best = m
                best_count = count
    return best, best_count

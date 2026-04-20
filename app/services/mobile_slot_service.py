from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.models import MobileBooking, MobileCatalogServiceItem, MobileServiceDriver, MobileServiceManager, MobileSlotSettings
from app.services.duration_slots import (
    BASE_SLOT_MINUTES,
    snap_duration_to_base_slots,
    slots_needed_for_duration,
    total_minutes_for_service_and_addons,
)
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


def _add_minutes_hhmm(hhmm: str, delta: int) -> str:
    return _fmt_hhmm(_parse_hhmm(hhmm) + int(delta))


def _slot_key(start_time: str, end_time: str) -> str:
    return f"{start_time}|{end_time}"


def _day_key(date_iso: str, start_time: str, end_time: str) -> str:
    return f"{date_iso}|{_slot_key(start_time, end_time)}"


def booking_span_minutes(start_time: str, end_time: str) -> tuple[int, int]:
    s = _parse_hhmm(start_time)
    e = _parse_hhmm(end_time)
    if e <= s:
        e += 24 * 60
    return s, e


def intervals_overlap_minutes(a0: int, a1: int, b0: int, b1: int) -> bool:
    return a0 < b1 and b0 < a1


def resolve_mobile_booking_duration_minutes(
    db: Session, service_id: str | None, addon_ids: list[str] | None
) -> int:
    n_addons = len(addon_ids or [])
    if not service_id:
        return total_minutes_for_service_and_addons(60, n_addons)
    row = db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id == service_id).one_or_none()
    if not row:
        return total_minutes_for_service_and_addons(60, n_addons)
    return total_minutes_for_service_and_addons(int(row.duration_minutes or 60), n_addons)


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

    day_state = loads_json_object(slot_settings.slot_day_states_json).get(
        _day_key(slot_date, start_time, end_time), {}
    )
    if isinstance(day_state, dict):
        if day_state.get("slotActive") is False:
            return []
        day_mask = day_state.get("driversOpen")
        if isinstance(day_mask, list):
            open_mask = [open_mask[i] and (bool(day_mask[i]) if i < len(day_mask) else True) for i in range(len(ids))]

    return [ids[i] for i in range(len(ids)) if open_mask[i]]


def _segment_blocks_driver(booking: MobileBooking, seg_s: int, seg_e: int, driver_id: str) -> bool:
    if booking.status == "cancelled":
        return False
    bs, be = booking_span_minutes(booking.start_time, booking.end_time)
    if not intervals_overlap_minutes(seg_s, seg_e, bs, be):
        return False
    if booking.assigned_driver_id is None:
        return True
    return str(booking.assigned_driver_id) == driver_id


def is_driver_available_for_interval(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
    driver_id: str,
    *,
    exclude_booking_id: str | None = None,
) -> bool:
    slot_settings = (
        db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == manager.id).one_or_none()
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
    if not any(d.id == driver_id for d in active_drivers):
        return False

    bookings = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.city_pin_code == manager.city_pin_code,
            MobileBooking.slot_date == slot_date,
            MobileBooking.status != "cancelled",
        )
        .all()
    )
    if exclude_booking_id:
        bookings = [b for b in bookings if b.id != exclude_booking_id]

    open_m = _parse_hhmm(slot_settings.open_time if slot_settings else "08:00")
    close_m = _parse_hhmm(slot_settings.close_time if slot_settings else "18:00")
    if close_m <= open_m:
        close_m += 24 * 60

    s0, s1 = booking_span_minutes(start_time, end_time)
    if s0 < open_m or s1 > close_m:
        return False

    t = s0
    while t < s1:
        t_next = t + BASE_SLOT_MINUTES
        st = _fmt_hhmm(t)
        et = _fmt_hhmm(t_next)
        open_ids = set(_open_driver_ids_for_slot(slot_settings, active_drivers, slot_date, st, et))
        if driver_id not in open_ids:
            return False
        for j in bookings:
            if _segment_blocks_driver(j, t, t_next, driver_id):
                return False
        t = t_next
    return True


def allocate_driver_for_interval(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
    *,
    exclude_booking_id: str | None = None,
) -> str | None:
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(
            MobileServiceDriver.manager_id == manager.id,
            MobileServiceDriver.active.is_(True),
        )
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    for d in active_drivers:
        if is_driver_available_for_interval(
            db, manager, slot_date, start_time, end_time, d.id, exclude_booking_id=exclude_booking_id
        ):
            return d.id
    return None


def count_drivers_available_for_start_duration(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    duration_minutes: int,
) -> int:
    dur = snap_duration_to_base_slots(duration_minutes)
    end_time = _add_minutes_hhmm(start_time, dur)
    active_drivers = (
        db.query(MobileServiceDriver)
        .filter(
            MobileServiceDriver.manager_id == manager.id,
            MobileServiceDriver.active.is_(True),
        )
        .order_by(MobileServiceDriver.created_at.asc(), MobileServiceDriver.id.asc())
        .all()
    )
    n = 0
    for d in active_drivers:
        if is_driver_available_for_interval(db, manager, slot_date, start_time, end_time, d.id):
            n += 1
    return n


@dataclass
class SlotAvailability:
    start_time: str
    end_time: str
    eligible_driver_ids: list[str]
    total_active_drivers: int
    duration_minutes: int
    slots_needed: int

    @property
    def open_driver_ids(self) -> list[str]:
        """Drivers who can take a new booking for this window (compat alias)."""
        return self.eligible_driver_ids

    @property
    def capacity(self) -> int:
        return self.total_active_drivers

    @property
    def booked(self) -> int:
        return max(0, self.total_active_drivers - len(self.eligible_driver_ids))

    @property
    def available(self) -> int:
        return len(self.eligible_driver_ids)


def list_slot_availability(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    *,
    booking_duration_minutes: int | None = None,
) -> list[SlotAvailability]:
    slot_settings = (
        db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == manager.id).one_or_none()
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

    dur = snap_duration_to_base_slots(int(booking_duration_minutes or BASE_SLOT_MINUTES))
    slots_n = slots_needed_for_duration(dur)
    open_time = _parse_hhmm(slot_settings.open_time if slot_settings else "08:00")
    close_time = _parse_hhmm(slot_settings.close_time if slot_settings else "18:00")
    if close_time <= open_time:
        close_time += 24 * 60

    total_d = len(active_drivers)
    out: list[SlotAvailability] = []
    t = open_time
    while t + dur <= close_time:
        st = _fmt_hhmm(t)
        et = _add_minutes_hhmm(st, dur)
        eligible = [
            d.id
            for d in active_drivers
            if is_driver_available_for_interval(db, manager, slot_date, st, et, d.id)
        ]
        out.append(
            SlotAvailability(
                start_time=st,
                end_time=et,
                eligible_driver_ids=eligible,
                total_active_drivers=total_d,
                duration_minutes=dur,
                slots_needed=slots_n,
            )
        )
        t += BASE_SLOT_MINUTES
    return out


def assert_slot_available(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
) -> None:
    if allocate_driver_for_interval(db, manager, slot_date, start_time, end_time) is None:
        raise ValueError("slot_unavailable")


def assert_slot_available_for_booking_update(
    db: Session,
    manager: MobileServiceManager,
    slot_date: str,
    start_time: str,
    end_time: str,
    exclude_booking_id: str,
) -> None:
    if (
        allocate_driver_for_interval(
            db, manager, slot_date, start_time, end_time, exclude_booking_id=exclude_booking_id
        )
        is None
    ):
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
    d = (
        db.query(MobileServiceDriver)
        .filter(
            MobileServiceDriver.id == driver_id,
            MobileServiceDriver.manager_id == manager.id,
            MobileServiceDriver.active.is_(True),
        )
        .one_or_none()
    )
    if not d:
        raise ValueError("driver_not_open")
    if not is_driver_available_for_interval(
        db, manager, slot_date, start_time, end_time, driver_id, exclude_booking_id=exclude_booking_id
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

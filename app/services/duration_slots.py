"""Shared booking duration + 30-minute base slot helpers."""

from __future__ import annotations

BASE_SLOT_MINUTES = 30
ADDON_EXTRA_MINUTES = 30


def resolve_operating_day_end_minutes(open_m: int, close_m: int) -> int:
    """
    When ``close_m`` is not after ``open_m`` on a 24h clock, either:

    * Same calendar day with PM close mis-keyed as AM (e.g. 09:00–05:00 meaning 09:00–17:00):
      add 12 hours to ``close_m`` once if that yields ``close > open``.
    * True overnight (e.g. 22:00–02:00): extend ``close_m`` by 24 hours.
    """
    if close_m > open_m:
        return close_m
    candidate = close_m + 12 * 60
    if candidate > open_m:
        return candidate
    return close_m + 24 * 60


def snap_duration_to_base_slots(minutes: int) -> int:
    """Round up to a multiple of BASE_SLOT_MINUTES, minimum one slot."""
    m = max(BASE_SLOT_MINUTES, int(minutes or 0))
    rem = m % BASE_SLOT_MINUTES
    if rem:
        m += BASE_SLOT_MINUTES - rem
    return m


def slots_needed_for_duration(total_minutes: int) -> int:
    return snap_duration_to_base_slots(total_minutes) // BASE_SLOT_MINUTES


def total_minutes_for_service_and_addons(service_duration_minutes: int, addon_count: int) -> int:
    base = snap_duration_to_base_slots(max(BASE_SLOT_MINUTES, int(service_duration_minutes or 0)))
    extras = max(0, int(addon_count or 0)) * ADDON_EXTRA_MINUTES
    return base + extras

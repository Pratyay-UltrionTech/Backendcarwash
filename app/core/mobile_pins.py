"""Normalize mobile manager/driver city and service PIN codes (digits, 4–6 valid length)."""

from __future__ import annotations

MOBILE_CITY_PIN_LEN_MIN = 4
MOBILE_CITY_PIN_LEN_MAX = 6


def normalize_mobile_city_pin(raw: str | None) -> str:
    """Digits only, at most MOBILE_CITY_PIN_LEN_MAX (e.g. AU 4-digit postcode or 6-digit PIN)."""
    if raw is None:
        return ""
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    return digits[:MOBILE_CITY_PIN_LEN_MAX]


def is_valid_mobile_city_pin(pin: str) -> bool:
    n = len(pin)
    return MOBILE_CITY_PIN_LEN_MIN <= n <= MOBILE_CITY_PIN_LEN_MAX

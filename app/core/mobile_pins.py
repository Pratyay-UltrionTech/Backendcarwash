"""Normalize mobile manager/driver city and service PIN codes (6-digit operational convention)."""

from __future__ import annotations


def normalize_mobile_city_pin(raw: str | None) -> str:
    """Digits only, first 6 (matches admin portal and customer apps)."""
    if raw is None:
        return ""
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    return digits[:6] if len(digits) >= 6 else digits

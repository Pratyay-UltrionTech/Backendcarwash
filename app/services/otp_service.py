"""
Shared in-memory OTP store used by auth flows (password reset, signup
verification, email change).  Imported by both auth.py and customer.py so
all flows share the same store without circular imports.
"""

import secrets
import threading
import time

_otp_lock = threading.Lock()
_otp_store: dict[str, dict] = {}

OTP_TTL = 600         # 10 min — window to enter the OTP after it is sent
RESET_TTL = 300       # 5 min  — window to submit new password after OTP verified
SIGNUP_TTL = 1800     # 30 min — window to complete registration after OTP verified
EMAIL_CHANGE_TTL = 900  # 15 min — window to save new email after OTP verified


def make_key(scope: str, identifier: str) -> str:
    return f"{scope}:{identifier.strip().lower()}"


def generate_otp() -> str:
    return str(secrets.randbelow(900000) + 100000)


def store_otp(scope: str, identifier: str, email: str) -> str:
    """Generate, persist, and return a fresh 6-digit OTP."""
    otp = generate_otp()
    with _otp_lock:
        _otp_store[make_key(scope, identifier)] = {
            "otp": otp,
            "expires_at": time.time() + OTP_TTL,
            "verified": False,
            "verified_at": None,
            "email": email,
        }
    return otp


def verify_otp(scope: str, identifier: str, otp: str) -> bool:
    """Return True and mark as verified when the code matches and is not expired."""
    key = make_key(scope, identifier)
    with _otp_lock:
        entry = _otp_store.get(key)
        if not entry or time.time() > entry["expires_at"]:
            return False
        if entry["otp"] != otp.strip():
            return False
        entry["verified"] = True
        entry["verified_at"] = time.time()
        return True


def check_verified(scope: str, identifier: str, ttl: int = RESET_TTL) -> str | None:
    """Return the stored email if the OTP was verified within *ttl* seconds, else None."""
    key = make_key(scope, identifier)
    with _otp_lock:
        entry = _otp_store.get(key)
        if not entry or not entry["verified"]:
            return None
        if time.time() > (entry["verified_at"] or 0) + ttl:
            return None
        return entry["email"]


def clear_otp(scope: str, identifier: str) -> None:
    with _otp_lock:
        _otp_store.pop(make_key(scope, identifier), None)

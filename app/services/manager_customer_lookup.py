"""Shared customer profile lookup for branch and mobile manager booking flows."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.models import CustomerUser
from app.models.booking import BranchBooking
from app.models.mobile import MobileBooking
from app.models.user_address import UserAddress
from app.services.jsonutil import loads_json_array

# CustomerUser PKs are generated as "CUST_XXXXX". If one ever got appended to a
# full_name (e.g. from a UI bug during profile setup), strip it before returning.
_CUST_ID_SUFFIX_RE = re.compile(r"\s*\bCUST_[A-Z0-9]{3,8}\b", re.IGNORECASE)


def sanitize_full_name(raw: str | None) -> str:
    """Return the customer's display name with any trailing CUST_XXXXX token removed."""
    s = str(raw or "").strip()
    return _CUST_ID_SUFFIX_RE.sub("", s).strip()


def _au_mobile_national(phone: str) -> str | None:
    """Nine-digit AU mobile national number (without country/leading zero)."""
    digits = re.sub(r"\D", "", str(phone or ""))
    if not digits:
        return None
    if digits.startswith("61") and len(digits) >= 11:
        return digits[-9:]
    if digits.startswith("0") and len(digits) >= 10:
        return digits[-9:]
    if len(digits) == 9:
        return digits
    if len(digits) > 9:
        return digits[-9:]
    return None


def _phone_variants(phone: str) -> list[str]:
    """Return common stored variants of a phone number for broad matching."""
    raw = str(phone or "").strip()
    if not raw:
        return []
    digits = re.sub(r"\D", "", raw)
    variants: list[str] = []
    if raw:
        variants.append(raw)
    if digits:
        variants.append(digits)
    national = _au_mobile_national(raw)
    if national:
        variants.extend(
            [
                national,
                f"+61{national}",
                f"61{national}",
                f"0{national}",
                f"+61-{national}",
            ]
        )
    if digits.startswith("61") and len(digits) >= 11:
        variants.append("0" + digits[2:])
        variants.append("+" + digits)
    elif digits.startswith("0") and len(digits) >= 10:
        variants.append("+61" + digits[1:])
        variants.append("61" + digits[1:])
    elif len(digits) == 9:
        variants.append("+61" + digits)
        variants.append("61" + digits)
        variants.append("0" + digits)
    return list(dict.fromkeys(v for v in variants if v))


def _phone_match_clause(column, phone: str):
    """SQL clause matching stored phone values across AU formatting variants."""
    variants = _phone_variants(phone)
    national = _au_mobile_national(phone)
    parts: list[Any] = []
    if variants:
        parts.append(column.in_(variants))
    if national:
        parts.append(column.like(f"%{national}"))
    if not parts:
        return None
    return or_(*parts) if len(parts) > 1 else parts[0]


def find_customer_user_for_manager_lookup(
    db: Session, *, phone: str | None = None, email: str | None = None
) -> CustomerUser | None:
    """Match by phone (many AU formats) first, else by email (case-insensitive)."""
    if phone and str(phone).strip():
        clause = _phone_match_clause(CustomerUser.phone, str(phone).strip())
        if clause is not None:
            u = db.query(CustomerUser).filter(clause).first()
            if u:
                return u
    if email and str(email).strip():
        em = str(email).strip().lower()
        u = db.query(CustomerUser).filter(func.lower(CustomerUser.email) == em).first()
        if u:
            return u
    return None


def find_guest_booking_for_manager_lookup(
    db: Session, *, phone: str | None = None, email: str | None = None
) -> BranchBooking | MobileBooking | None:
    """Search past guest bookings (customer_id IS NULL) by phone or email.
    Returns the most recently created booking, preferring BranchBooking over MobileBooking."""
    phone_raw = str(phone).strip() if phone and str(phone).strip() else None
    em = str(email).strip().lower() if email and str(email).strip() else None

    def _guest_customer_clause():
        return or_(BranchBooking.customer_id.is_(None), BranchBooking.customer_id == "")

    def _guest_mobile_customer_clause():
        return or_(MobileBooking.customer_id.is_(None), MobileBooking.customer_id == "")

    def _query_branch() -> BranchBooking | None:
        filters: list[Any] = [_guest_customer_clause()]
        phone_clause = _phone_match_clause(BranchBooking.phone, phone_raw) if phone_raw else None
        if phone_clause is not None and em:
            filters.append(or_(phone_clause, func.lower(BranchBooking.customer_email) == em))
        elif phone_clause is not None:
            filters.append(phone_clause)
        elif em:
            filters.append(func.lower(BranchBooking.customer_email) == em)
        else:
            return None
        return db.query(BranchBooking).filter(*filters).order_by(BranchBooking.created_at.desc()).first()

    def _query_mobile() -> MobileBooking | None:
        filters: list[Any] = [_guest_mobile_customer_clause()]
        phone_clause = _phone_match_clause(MobileBooking.phone, phone_raw) if phone_raw else None
        if phone_clause is not None and em:
            filters.append(or_(phone_clause, func.lower(MobileBooking.customer_email) == em))
        elif phone_clause is not None:
            filters.append(phone_clause)
        elif em:
            filters.append(func.lower(MobileBooking.customer_email) == em)
        else:
            return None
        return db.query(MobileBooking).filter(*filters).order_by(MobileBooking.created_at.desc()).first()

    branch_booking = _query_branch()
    if branch_booking:
        return branch_booking
    return _query_mobile()


def guest_booking_to_lookup_dict(booking: BranchBooking | MobileBooking) -> dict[str, Any]:
    """Build a lookup response from a guest booking record (no CustomerUser account)."""
    vehicle: dict[str, str] = {}
    vtype = str(getattr(booking, "vehicle_type", "") or "").strip()
    vmodel = str(getattr(booking, "vehicle_model", "") or "").strip()
    vreg = str(getattr(booking, "registration_number", "") or "").strip()
    if vtype or vmodel or vreg:
        vehicle = {"type": vtype, "model": vmodel, "number": vreg}

    return {
        "id": None,
        "guest": True,
        "full_name": sanitize_full_name(booking.customer_name),
        "email": str(booking.customer_email or "").strip(),
        "phone": str(booking.phone or "").strip(),
        "address": str(getattr(booking, "address", "") or "").strip(),
        "vehicles": [vehicle] if vehicle else [],
        "saved_addresses": [],
    }


def customer_user_to_lookup_dict(u: CustomerUser, db: Session | None = None) -> dict[str, Any]:
    saved_addresses: list[dict[str, Any]] = []
    if db is not None:
        rows = (
            db.query(UserAddress)
            .filter(UserAddress.user_id == u.id)
            .order_by(UserAddress.is_default.desc(), UserAddress.created_at.asc())
            .all()
        )
        saved_addresses = [
            {
                "id": a.id,
                "label": a.label,
                "street_address": a.street_address,
                "suburb": a.suburb,
                "state": a.state,
                "postcode": a.postcode,
                "is_default": a.is_default,
            }
            for a in rows
        ]
    return {
        "id": u.id,
        "email": u.email,
        "full_name": sanitize_full_name(u.full_name),
        "phone": u.phone,
        "address": u.address_line,
        "vehicles": loads_json_array(u.vehicles_json),
        "saved_addresses": saved_addresses,
    }

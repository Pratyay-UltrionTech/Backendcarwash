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
from app.services.loyalty_service import normalize_phone

# CustomerUser PKs are generated as "CUST_XXXXX". If one ever got appended to a
# full_name (e.g. from a UI bug during profile setup), strip it before returning.
_CUST_ID_SUFFIX_RE = re.compile(r"\s*\bCUST_[A-Z0-9]{3,8}\b", re.IGNORECASE)


def sanitize_full_name(raw: str | None) -> str:
    """Return the customer's display name with any trailing CUST_XXXXX token removed."""
    s = str(raw or "").strip()
    return _CUST_ID_SUFFIX_RE.sub("", s).strip()


def find_customer_user_for_manager_lookup(
    db: Session, *, phone: str | None = None, email: str | None = None
) -> CustomerUser | None:
    """Match by normalized phone first (if phone provided), else by email (case-insensitive)."""
    u: CustomerUser | None = None
    if phone and str(phone).strip():
        p = str(phone).strip()
        norm_phone = normalize_phone(p)
        if norm_phone:
            u = db.query(CustomerUser).filter(CustomerUser.phone == norm_phone).first()
        if not u:
            u = db.query(CustomerUser).filter(CustomerUser.phone == p).first()
    if not u and email and str(email).strip():
        em = str(email).strip().lower()
        u = db.query(CustomerUser).filter(func.lower(CustomerUser.email) == em).first()
    return u


def _phone_variants(phone: str) -> list[str]:
    """Return common stored variants of a phone number for broad matching."""
    variants = [phone]
    digits = re.sub(r"\D", "", phone)
    if digits:
        variants.append(digits)
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
    return list(dict.fromkeys(variants))  # deduplicate, preserve order


def find_guest_booking_for_manager_lookup(
    db: Session, *, phone: str | None = None, email: str | None = None
) -> BranchBooking | MobileBooking | None:
    """Search past guest bookings (customer_id IS NULL) by phone or email.
    Tries phone variants first, then email, then combined OR filter.
    Returns the most recently created booking, preferring BranchBooking over MobileBooking."""
    phone_variants = _phone_variants(phone) if phone and str(phone).strip() else []
    em = str(email).strip().lower() if email and str(email).strip() else None

    def _query_branch(phone_vars: list[str], email_val: str | None) -> BranchBooking | None:
        filters = [BranchBooking.customer_id.is_(None)]
        if phone_vars and email_val:
            filters.append(or_(BranchBooking.phone.in_(phone_vars), func.lower(BranchBooking.customer_email) == email_val))
        elif phone_vars:
            filters.append(BranchBooking.phone.in_(phone_vars))
        elif email_val:
            filters.append(func.lower(BranchBooking.customer_email) == email_val)
        else:
            return None
        return db.query(BranchBooking).filter(*filters).order_by(BranchBooking.created_at.desc()).first()

    def _query_mobile(phone_vars: list[str], email_val: str | None) -> MobileBooking | None:
        filters = [MobileBooking.customer_id.is_(None)]
        if phone_vars and email_val:
            filters.append(or_(MobileBooking.phone.in_(phone_vars), func.lower(MobileBooking.customer_email) == email_val))
        elif phone_vars:
            filters.append(MobileBooking.phone.in_(phone_vars))
        elif email_val:
            filters.append(func.lower(MobileBooking.customer_email) == email_val)
        else:
            return None
        return db.query(MobileBooking).filter(*filters).order_by(MobileBooking.created_at.desc()).first()

    branch_booking = _query_branch(phone_variants, em)
    if branch_booking:
        return branch_booking
    mobile_booking = _query_mobile(phone_variants, em)
    return mobile_booking


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

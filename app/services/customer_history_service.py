"""Branch + mobile bookings: by customer_id for members; legacy phone-only when no member id."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models import Branch, BranchBooking, MobileBooking
from app.services.booking_pricing import (
    branch_booking_customer_service_total_cents,
    mobile_booking_customer_service_total_cents,
)
from app.services.booking_status import effective_status
from app.services.jsonutil import loads_json_array
from app.services.loyalty_service import loyalty_ledger_booking_keys_for_customer, normalize_phone


def _branch_amount_cents(db: Session, b: BranchBooking) -> int:
    """Service + add-ons − promo + tip (what the customer pays for this booking)."""
    svc = branch_booking_customer_service_total_cents(db, b)
    tip = int(getattr(b, "tip_cents", 0) or 0)
    return max(0, svc + tip)


def _mobile_amount_cents(db: Session, m: MobileBooking) -> int:
    svc = mobile_booking_customer_service_total_cents(db, m)
    tip = int(getattr(m, "tip_cents", 0) or 0)
    return max(0, svc + tip)


def _booking_row_visible(
    booking_customer_id: str | None,
    booking_phone: str,
    logged_in_customer_id: str | None,
    profile_phone_n: str,
) -> bool:
    """Signed-in members: only bookings with the same customer_id. Legacy phone-only lookup: phone match."""
    if logged_in_customer_id:
        return bool(booking_customer_id) and str(booking_customer_id) == str(logged_in_customer_id)
    if profile_phone_n and normalize_phone(booking_phone) == profile_phone_n:
        return True
    return False


def service_history_items_for_customer(
    db: Session, customer_id: str, phone: str, *, limit: int = 100
) -> list[dict[str, Any]]:
    logged = (customer_id or "").strip() or None
    pn = normalize_phone(phone or "")
    if not logged and not pn:
        return []

    loyalty_points_by_booking = loyalty_ledger_booking_keys_for_customer(db, customer_id=logged, phone=phone or "")

    scored: list[tuple[datetime, dict[str, Any]]] = []

    q_branch = db.query(BranchBooking, Branch).join(Branch, BranchBooking.branch_id == Branch.id)
    if logged:
        q_branch = q_branch.filter(BranchBooking.customer_id == logged)
    q_branch = q_branch.order_by(desc(BranchBooking.created_at)).limit(400)
    for b, br in q_branch:
        bcid = getattr(b, "customer_id", None)
        if not _booking_row_visible(bcid, b.phone or "", logged, pn):
            continue
        ca = b.created_at
        scored.append(
            (
                ca,
                {
                    "id": b.id,
                    "channel": "branch",
                    "status": effective_status(b.status, b.slot_date, b.end_time),
                    "slot_date": b.slot_date,
                    "start_time": b.start_time,
                    "end_time": b.end_time,
                    "location_label": br.name,
                    "branch_id": br.id,
                    "service_id": b.service_id,
                    "selected_addon_ids": loads_json_array(getattr(b, "selected_addon_ids_json", "[]") or "[]"),
                    "service_summary": (b.service_summary or "").strip(),
                    "vehicle_type": (b.vehicle_type or "").strip(),
                    "loyalty_points_earned": 1 if ("branch", str(b.id)) in loyalty_points_by_booking else 0,
                    "customer_id": str(bcid) if bcid else None,
                    "phone": b.phone or "",
                    "created_at": ca.isoformat() if ca else None,
                    "total_cents": _branch_amount_cents(db, b),
                },
            )
        )

    q_mobile = db.query(MobileBooking)
    if logged:
        q_mobile = q_mobile.filter(MobileBooking.customer_id == logged)
    q_mobile = q_mobile.order_by(desc(MobileBooking.created_at)).limit(400)
    for m in q_mobile:
        mcid = getattr(m, "customer_id", None)
        if not _booking_row_visible(mcid, m.phone or "", logged, pn):
            continue
        ca = m.created_at
        scored.append(
            (
                ca,
                {
                    "id": m.id,
                    "channel": "mobile",
                    "status": effective_status(m.status, m.slot_date, m.end_time),
                    "slot_date": m.slot_date,
                    "start_time": m.start_time,
                    "end_time": m.end_time,
                    "location_label": f"Mobile · PIN {m.city_pin_code}",
                    "branch_id": f"mobile-{m.city_pin_code}",
                    "city_pin_code": m.city_pin_code,
                    "service_id": m.service_id,
                    "service_summary": (m.vehicle_summary or "").strip(),
                    "vehicle_type": (m.vehicle_type or "").strip(),
                    "loyalty_points_earned": 1 if ("mobile", str(m.id)) in loyalty_points_by_booking else 0,
                    "customer_id": str(mcid) if mcid else None,
                    "phone": m.phone or "",
                    "created_at": ca.isoformat() if ca else None,
                    "total_cents": _mobile_amount_cents(db, m),
                },
            )
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for _, row in scored[:limit]]


def service_history_items_for_phone(db: Session, phone: str, *, limit: int = 100) -> list[dict[str, Any]]:
    """Phone-only history (no member id), for backward compatibility."""
    return service_history_items_for_customer(db, "", phone, limit=limit)

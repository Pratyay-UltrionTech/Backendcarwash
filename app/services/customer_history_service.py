"""Branch + mobile bookings for a customer, matched by profile phone (same rules as loyalty)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models import Branch, BranchBooking, MobileBooking
from app.services.jsonutil import loads_json_array
from app.services.loyalty_service import normalize_phone


def service_history_items_for_phone(db: Session, phone: str, *, limit: int = 100) -> list[dict[str, Any]]:
    pn = normalize_phone(phone or "")
    if not pn:
        return []

    scored: list[tuple[datetime, dict[str, Any]]] = []

    q_branch = (
        db.query(BranchBooking, Branch)
        .join(Branch, BranchBooking.branch_id == Branch.id)
        .order_by(desc(BranchBooking.created_at))
        .limit(400)
    )
    for b, br in q_branch:
        if normalize_phone(b.phone) != pn:
            continue
        ca = b.created_at
        scored.append(
            (
                ca,
                {
                    "id": b.id,
                    "channel": "branch",
                    "status": b.status,
                    "slot_date": b.slot_date,
                    "start_time": b.start_time,
                    "end_time": b.end_time,
                    "location_label": br.name,
                    "branch_id": br.id,
                    "service_id": b.service_id,
                    "selected_addon_ids": loads_json_array(getattr(b, "selected_addon_ids_json", "[]") or "[]"),
                    "service_summary": (b.service_summary or "").strip(),
                    "vehicle_type": (b.vehicle_type or "").strip(),
                    "created_at": ca.isoformat() if ca else None,
                },
            )
        )

    for m in db.query(MobileBooking).order_by(desc(MobileBooking.created_at)).limit(400):
        if normalize_phone(m.phone) != pn:
            continue
        ca = m.created_at
        scored.append(
            (
                ca,
                {
                    "id": m.id,
                    "channel": "mobile",
                    "status": m.status,
                    "slot_date": m.slot_date,
                    "start_time": m.start_time,
                    "end_time": m.end_time,
                    "location_label": f"Mobile · PIN {m.city_pin_code}",
                    "branch_id": f"mobile_{m.city_pin_code}",
                    "city_pin_code": m.city_pin_code,
                    "service_id": m.service_id,
                    "service_summary": (m.vehicle_summary or "").strip(),
                    "vehicle_type": (m.vehicle_type or "").strip(),
                    "created_at": ca.isoformat() if ca else None,
                },
            )
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for _, row in scored[:limit]]

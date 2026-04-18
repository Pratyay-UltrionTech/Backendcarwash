"""Loyalty ledger + spend window (last N eligible completed services) for branch and mobile."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models import (
    Branch,
    BranchBooking,
    BranchLoyalty,
    CatalogServiceItem,
    LoyaltyLedgerEntry,
    MobileBooking,
    MobileCatalogServiceItem,
    MobileLoyaltyProgram,
    VehicleCatalogBlock,
)


def normalize_phone(phone: str) -> str:
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _branch_catalog_service(db: Session, branch_id: str, service_id: str) -> CatalogServiceItem | None:
    return (
        db.query(CatalogServiceItem)
        .join(VehicleCatalogBlock, CatalogServiceItem.vehicle_block_id == VehicleCatalogBlock.id)
        .filter(VehicleCatalogBlock.branch_id == branch_id, CatalogServiceItem.id == service_id)
        .one_or_none()
    )


def _mobile_catalog_service(db: Session, service_id: str) -> MobileCatalogServiceItem | None:
    return db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id == service_id).one_or_none()


def _delete_ledger_for_booking(db: Session, channel: str, booking_id: str) -> None:
    db.query(LoyaltyLedgerEntry).filter(
        LoyaltyLedgerEntry.channel == channel, LoyaltyLedgerEntry.booking_id == booking_id
    ).delete(synchronize_session=False)


def _ledger_exists(db: Session, channel: str, booking_id: str) -> bool:
    return (
        db.query(LoyaltyLedgerEntry.id)
        .filter(LoyaltyLedgerEntry.channel == channel, LoyaltyLedgerEntry.booking_id == booking_id)
        .limit(1)
        .scalar()
        is not None
    )


def on_branch_booking_status_change(db: Session, job: BranchBooking, previous_status: str) -> None:
    """Call after mutating job.status (before or after commit; caller commits)."""
    if previous_status == "completed" and job.status != "completed":
        _delete_ledger_for_booking(db, "branch", job.id)
        job.completed_at = None
        return
    if job.status != "completed" or previous_status == "completed":
        return
    if _ledger_exists(db, "branch", job.id):
        return
    sid = getattr(job, "service_id", None)
    if not sid:
        job.completed_at = _utcnow()
        return
    svc = _branch_catalog_service(db, job.branch_id, str(sid))
    if not svc or not bool(svc.eligible_for_loyalty_points):
        job.completed_at = _utcnow()
        return
    phone_n = normalize_phone(job.phone or "")
    if not phone_n:
        job.completed_at = _utcnow()
        return
    now = _utcnow()
    job.completed_at = now
    db.add(
        LoyaltyLedgerEntry(
            channel="branch",
            branch_id=job.branch_id,
            city_pin_code=None,
            customer_phone_normalized=phone_n,
            booking_id=job.id,
            service_id=str(sid),
            amount=float(svc.price or 0),
            completed_at=now,
        )
    )


def on_mobile_booking_status_change(db: Session, row: MobileBooking, previous_status: str) -> None:
    if previous_status == "completed" and row.status != "completed":
        _delete_ledger_for_booking(db, "mobile", row.id)
        row.completed_at = None
        return
    if row.status != "completed" or previous_status == "completed":
        return
    if _ledger_exists(db, "mobile", row.id):
        return
    sid = row.service_id
    if not sid:
        row.completed_at = _utcnow()
        return
    svc = _mobile_catalog_service(db, str(sid))
    if not svc or not bool(svc.eligible_for_loyalty_points):
        row.completed_at = _utcnow()
        return
    phone_n = normalize_phone(row.phone or "")
    if not phone_n:
        row.completed_at = _utcnow()
        return
    now = _utcnow()
    row.completed_at = now
    db.add(
        LoyaltyLedgerEntry(
            channel="mobile",
            branch_id=None,
            city_pin_code=row.city_pin_code,
            customer_phone_normalized=phone_n,
            booking_id=row.id,
            service_id=str(sid),
            amount=float(svc.price or 0),
            completed_at=now,
        )
    )


def _parse_tiers(tiers_json: str) -> list[dict[str, Any]]:
    try:
        raw = json.loads(tiers_json or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(raw, list):
        return []
    return [t for t in raw if isinstance(t, dict)]


def _tier_matches_spend(spend: float, tier: dict[str, Any]) -> bool:
    try:
        mn = float(tier.get("minSpendInWindow", 0))
    except (TypeError, ValueError):
        mn = 0.0
    if spend < mn:
        return False
    mx = tier.get("maxSpendInWindow")
    if mx is None:
        return True
    try:
        return spend <= float(mx)
    except (TypeError, ValueError):
        return False


def _best_matching_tier(spend: float, tiers: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches = [t for t in tiers if _tier_matches_spend(spend, t)]
    if not matches:
        return None
    matches.sort(key=lambda t: float(t.get("minSpendInWindow", 0) or 0), reverse=True)
    return matches[0]


def _next_tier_gap(spend: float, tiers: list[dict[str, Any]]) -> tuple[float | None, dict[str, Any] | None]:
    """Minimum additional spend needed to reach the next tier's min (if any)."""
    upcoming: list[tuple[float, dict[str, Any]]] = []
    for t in tiers:
        try:
            mn = float(t.get("minSpendInWindow", 0))
        except (TypeError, ValueError):
            continue
        if spend < mn:
            upcoming.append((mn - spend, t))
    if not upcoming:
        return None, None
    upcoming.sort(key=lambda x: x[0])
    return upcoming[0]


def _service_name_branch(db: Session, branch_id: str, service_id: str) -> str:
    svc = _branch_catalog_service(db, branch_id, service_id)
    return (svc.name if svc else service_id) or service_id


def _service_name_mobile(db: Session, service_id: str) -> str:
    svc = _mobile_catalog_service(db, service_id)
    return (svc.name if svc else service_id) or service_id


def compute_branch_loyalty_overview(db: Session, branch_id: str, phone: str) -> dict[str, Any]:
    phone_n = normalize_phone(phone)
    loyalty = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one_or_none()
    branch = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
    branch_name = branch.name if branch else branch_id
    if not loyalty or not phone_n:
        return _empty_overview("branch", branch_id, branch_name, None)

    n = max(1, int(loyalty.qualifying_service_count or 10))
    tiers = _parse_tiers(loyalty.tiers_json)

    rows = (
        db.query(LoyaltyLedgerEntry)
        .filter(
            LoyaltyLedgerEntry.channel == "branch",
            LoyaltyLedgerEntry.branch_id == branch_id,
            LoyaltyLedgerEntry.customer_phone_normalized == phone_n,
        )
        .order_by(desc(LoyaltyLedgerEntry.completed_at))
        .limit(n)
        .all()
    )
    window = list(reversed(rows))
    eligible_count = len(window)
    spend = sum(float(r.amount) for r in window)
    matched = _best_matching_tier(spend, tiers)
    gap, _next = _next_tier_gap(spend, tiers)

    reward_name = None
    matched_payload = None
    if matched:
        rid = str(matched.get("rewardServiceId") or "")
        reward_name = _service_name_branch(db, branch_id, rid) if rid else None
        matched_payload = {
            "tier_id": matched.get("id"),
            "reward_service_id": rid,
            "reward_service_name": reward_name,
        }

    remaining_window_slots = max(0, n - eligible_count)
    parts: list[str] = []
    if eligible_count > 0:
        parts.append(
            f"You have {eligible_count} eligible wash(es) in your last {n} toward loyalty "
            f"({spend:.0f} total qualifying spend in that window)."
        )
    else:
        parts.append(
            f"Complete a wash at this branch with a service marked for loyalty to start your {n}-service window."
        )
    if matched_payload and reward_name:
        parts.append(f"You qualify for a free reward: {reward_name}.")
    elif gap is not None and gap > 0:
        parts.append(
            f"About {gap:.0f} more in qualifying spend across your last {n} eligible washes to reach the next reward tier."
        )
    elif remaining_window_slots > 0 and not tiers:
        parts.append(f"{remaining_window_slots} more eligible wash(es) can still be added to your current window.")

    return {
        "has_loyalty_activity": eligible_count > 0,
        "scope": "branch",
        "branch_id": branch_id,
        "branch_name": branch_name,
        "city_pin_code": None,
        "qualifying_service_count": n,
        "eligible_services_in_window": eligible_count,
        "spend_in_window": round(spend, 2),
        "window_progress_label": f"{eligible_count}/{n}",
        "remaining_eligible_slots_in_window": remaining_window_slots,
        "progress_fraction": min(1.0, eligible_count / n) if n else 0.0,
        "matched_reward": matched_payload,
        "next_reward_message": " ".join(parts),
    }


def compute_mobile_loyalty_overview(db: Session, city_pin: str, phone: str) -> dict[str, Any]:
    phone_n = normalize_phone(phone)
    pin = (city_pin or "").strip()
    loyalty = db.query(MobileLoyaltyProgram).order_by(MobileLoyaltyProgram.created_at.asc()).first()
    if not loyalty or not phone_n or not pin:
        return _empty_overview("mobile", None, "Mobile", pin or None)

    n = max(1, int(loyalty.qualifying_service_count or 10))
    tiers = _parse_tiers(loyalty.tiers_json)

    rows = (
        db.query(LoyaltyLedgerEntry)
        .filter(
            LoyaltyLedgerEntry.channel == "mobile",
            LoyaltyLedgerEntry.city_pin_code == pin,
            LoyaltyLedgerEntry.customer_phone_normalized == phone_n,
        )
        .order_by(desc(LoyaltyLedgerEntry.completed_at))
        .limit(n)
        .all()
    )
    window = list(reversed(rows))
    eligible_count = len(window)
    spend = sum(float(r.amount) for r in window)
    matched = _best_matching_tier(spend, tiers)
    gap, _next = _next_tier_gap(spend, tiers)

    reward_name = None
    matched_payload = None
    if matched:
        rid = str(matched.get("rewardServiceId") or "")
        reward_name = _service_name_mobile(db, rid) if rid else None
        matched_payload = {
            "tier_id": matched.get("id"),
            "reward_service_id": rid,
            "reward_service_name": reward_name,
        }

    remaining_window_slots = max(0, n - eligible_count)
    parts: list[str] = []
    if eligible_count > 0:
        parts.append(
            f"You have {eligible_count} eligible wash(es) in your last {n} toward loyalty "
            f"({spend:.0f} total qualifying spend in that window)."
        )
    else:
        parts.append(
            f"Complete a mobile wash in PIN {pin} with a loyalty-counted service to start your {n}-service window."
        )
    if matched_payload and reward_name:
        parts.append(f"You qualify for a free reward: {reward_name}.")
    elif gap is not None and gap > 0:
        parts.append(
            f"About {gap:.0f} more in qualifying spend across your last {n} eligible washes to reach the next reward tier."
        )

    return {
        "has_loyalty_activity": eligible_count > 0,
        "scope": "mobile",
        "branch_id": None,
        "branch_name": f"Mobile — PIN {pin}",
        "city_pin_code": pin,
        "qualifying_service_count": n,
        "eligible_services_in_window": eligible_count,
        "spend_in_window": round(spend, 2),
        "window_progress_label": f"{eligible_count}/{n}",
        "remaining_eligible_slots_in_window": remaining_window_slots,
        "progress_fraction": min(1.0, eligible_count / n) if n else 0.0,
        "matched_reward": matched_payload,
        "next_reward_message": " ".join(parts),
    }


def _empty_overview(
    scope: str, branch_id: str | None, branch_name: str, city_pin: str | None
) -> dict[str, Any]:
    return {
        "has_loyalty_activity": False,
        "scope": scope,
        "branch_id": branch_id,
        "branch_name": branch_name,
        "city_pin_code": city_pin,
        "qualifying_service_count": 10,
        "eligible_services_in_window": 0,
        "spend_in_window": 0.0,
        "window_progress_label": "0/10",
        "remaining_eligible_slots_in_window": 10,
        "progress_fraction": 0.0,
        "matched_reward": None,
        "next_reward_message": "Book a service marked “count toward loyalty” and complete it to build rewards.",
    }


def loyalty_overview_for_customer(db: Session, phone: str) -> dict[str, Any]:
    """Pick the most recent ledger scope as primary detail for home UI."""
    phone_n = normalize_phone(phone)
    if not phone_n:
        return {"has_any_loyalty": False, "primary": None}

    last = (
        db.query(LoyaltyLedgerEntry)
        .filter(LoyaltyLedgerEntry.customer_phone_normalized == phone_n)
        .order_by(desc(LoyaltyLedgerEntry.completed_at))
        .limit(1)
        .one_or_none()
    )
    if not last:
        return {"has_any_loyalty": False, "primary": None}

    if last.channel == "branch" and last.branch_id:
        primary = compute_branch_loyalty_overview(db, str(last.branch_id), phone_n)
    elif last.channel == "mobile" and last.city_pin_code:
        primary = compute_mobile_loyalty_overview(db, str(last.city_pin_code), phone_n)
    else:
        return {"has_any_loyalty": False, "primary": None}

    return {"has_any_loyalty": True, "primary": primary}

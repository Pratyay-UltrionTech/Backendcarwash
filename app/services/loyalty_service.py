"""Loyalty ledger + spend window (last N eligible completed services) for branch and mobile."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, desc, false, func, or_
from sqlalchemy.orm import Session

from app.models import (
    Branch,
    BranchBooking,
    BranchLoyalty,
    CatalogServiceItem,
    LoyaltyLedgerEntry,
    LoyaltyReward,
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


def _ledger_customer_clause(customer_id: str | None, phone_n: str):
    """Ledger row belongs to this member (customer_id) or legacy phone-only match."""
    parts: list[Any] = []
    if customer_id:
        parts.append(LoyaltyLedgerEntry.customer_id == customer_id)
    if phone_n:
        parts.append(
            and_(LoyaltyLedgerEntry.customer_id.is_(None), LoyaltyLedgerEntry.customer_phone_normalized == phone_n)
        )
    if not parts:
        return false()
    return or_(*parts)


def loyalty_ledger_booking_keys_for_customer(
    db: Session, *, customer_id: str | None, phone: str
) -> set[tuple[str, str]]:
    phone_n = normalize_phone(phone or "")
    if not customer_id and not phone_n:
        return set()
    rows = (
        db.query(LoyaltyLedgerEntry.channel, LoyaltyLedgerEntry.booking_id)
        .filter(_ledger_customer_clause(customer_id, phone_n))
        .all()
    )
    return {(str(ch), str(bid)) for ch, bid in rows}


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


def _last_redemption_dt(
    db: Session, channel: str, branch_id: str | None, city_pin: str | None, customer_id: str
) -> datetime | None:
    """Return the redeemed_at of the most recent redeemed reward for this customer+channel."""
    q = (
        db.query(LoyaltyReward.redeemed_at)
        .filter(
            LoyaltyReward.customer_id == customer_id,
            LoyaltyReward.channel == channel,
            LoyaltyReward.status == "redeemed",
            LoyaltyReward.redeemed_at.isnot(None),
        )
    )
    if channel == "branch" and branch_id:
        q = q.filter(LoyaltyReward.branch_id == branch_id)
    elif channel == "mobile" and city_pin:
        q = q.filter(LoyaltyReward.city_pin_code == city_pin)
    row = q.order_by(desc(LoyaltyReward.redeemed_at)).first()
    return row[0] if row else None


def _count_ledger_since(
    db: Session,
    channel: str,
    branch_id: str | None,
    city_pin: str | None,
    customer_id: str,
    phone_n: str,
    since_dt: datetime | None = None,
) -> int:
    """Count ledger entries for this customer in this channel, optionally only after since_dt."""
    q = db.query(func.count(LoyaltyLedgerEntry.id)).filter(
        LoyaltyLedgerEntry.channel == channel,
        _ledger_customer_clause(customer_id, phone_n),
    )
    if channel == "branch" and branch_id:
        q = q.filter(LoyaltyLedgerEntry.branch_id == branch_id)
    elif channel == "mobile" and city_pin:
        q = q.filter(LoyaltyLedgerEntry.city_pin_code == city_pin)
    if since_dt:
        q = q.filter(LoyaltyLedgerEntry.completed_at > since_dt)
    return q.scalar() or 0


def _maybe_grant_reward(
    db: Session,
    channel: str,
    branch_id: str | None,
    city_pin: str | None,
    customer_id: str,
    phone_n: str,
    qualifying_count: int,
    tiers: list[dict[str, Any]],
    svc_name_fn,  # callable(service_id) -> str
) -> "LoyaltyReward | None":
    """Grant a reward if threshold met and no pending reward already exists. Returns new reward or None."""
    if not customer_id or not tiers:
        return None
    # Only count entries since last redemption (supports counter reset)
    last_redeemed = _last_redemption_dt(db, channel, branch_id, city_pin, customer_id)
    count = _count_ledger_since(db, channel, branch_id, city_pin, customer_id, phone_n, since_dt=last_redeemed)
    if count < qualifying_count:
        return None
    # Already has a pending (un-redeemed) reward → don't double-grant
    pending_q = db.query(LoyaltyReward).filter(
        LoyaltyReward.customer_id == customer_id,
        LoyaltyReward.channel == channel,
        LoyaltyReward.status == "pending",
    )
    if channel == "branch" and branch_id:
        pending_q = pending_q.filter(LoyaltyReward.branch_id == branch_id)
    elif channel == "mobile" and city_pin:
        pending_q = pending_q.filter(LoyaltyReward.city_pin_code == city_pin)
    if pending_q.first():
        return None
    # Pick the first tier that has a rewardServiceId
    tier = next((t for t in tiers if t.get("rewardServiceId")), None)
    if not tier:
        return None
    reward_sid = str(tier["rewardServiceId"])
    reward_name = svc_name_fn(reward_sid)
    now = _utcnow()
    reward = LoyaltyReward(
        customer_id=customer_id,
        channel=channel,
        branch_id=branch_id,
        city_pin_code=city_pin,
        tier_id=str(tier.get("id", "")),
        reward_service_id=reward_sid,
        reward_service_name=reward_name,
        status="pending",
        email_sent=False,
        granted_at=now,
    )
    db.add(reward)
    return reward


def get_active_rewards_for_customer(db: Session, customer_id: str) -> list[dict[str, Any]]:
    """Return all pending (unredeemed) rewards for a registered customer."""
    rows = (
        db.query(LoyaltyReward)
        .filter(LoyaltyReward.customer_id == customer_id, LoyaltyReward.status == "pending")
        .order_by(desc(LoyaltyReward.granted_at))
        .all()
    )
    return [
        {
            "id": r.id,
            "channel": r.channel,
            "branch_id": r.branch_id,
            "city_pin_code": r.city_pin_code,
            "reward_service_id": r.reward_service_id,
            "reward_service_name": r.reward_service_name,
            "granted_at": r.granted_at.isoformat(),
        }
        for r in rows
    ]


def consume_reward(
    db: Session, reward_id: str, customer_id: str, booking_id: str
) -> bool:
    """Mark a pending reward as redeemed. Returns True if successfully consumed."""
    reward = (
        db.query(LoyaltyReward)
        .filter(
            LoyaltyReward.id == reward_id,
            LoyaltyReward.customer_id == customer_id,
            LoyaltyReward.status == "pending",
        )
        .with_for_update()
        .one_or_none()
    )
    if not reward:
        return False
    reward.status = "redeemed"
    reward.redeemed_at = _utcnow()
    reward.redeemed_booking_id = booking_id
    return True


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
    cid = str(job.customer_id) if getattr(job, "customer_id", None) else None
    db.add(
        LoyaltyLedgerEntry(
            channel="branch",
            branch_id=job.branch_id,
            city_pin_code=None,
            customer_phone_normalized=phone_n,
            customer_id=cid,
            booking_id=job.id,
            service_id=str(sid),
            amount=float(svc.price or 0),
            completed_at=now,
        )
    )
    # Check if customer hit the loyalty threshold → grant reward
    if cid:
        loyalty = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == job.branch_id).one_or_none()
        if loyalty:
            n = max(1, int(loyalty.qualifying_service_count or 10))
            tiers = _parse_tiers(loyalty.tiers_json)
            reward = _maybe_grant_reward(
                db, "branch", job.branch_id, None, cid, phone_n, n, tiers,
                lambda sid_: _service_name_branch(db, job.branch_id, sid_),
            )
            if reward:
                db.flush()  # ensure reward.id is populated
                from app.services.email_service import lookup_customer_email, send_loyalty_reward_email
                to_email, cust_name = lookup_customer_email(db, cid, job.phone)
                if to_email:
                    send_loyalty_reward_email(
                        to_email,
                        cust_name or job.customer_name,
                        reward.reward_service_name or reward.reward_service_id,
                        channel="branch",
                    )
                reward.email_sent = bool(to_email)


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
    cid = str(row.customer_id) if getattr(row, "customer_id", None) else None
    db.add(
        LoyaltyLedgerEntry(
            channel="mobile",
            branch_id=None,
            city_pin_code=row.city_pin_code,
            customer_phone_normalized=phone_n,
            customer_id=cid,
            booking_id=row.id,
            service_id=str(sid),
            amount=float(svc.price or 0),
            completed_at=now,
        )
    )
    # Check if customer hit the loyalty threshold → grant reward
    if cid:
        loyalty = db.query(MobileLoyaltyProgram).order_by(MobileLoyaltyProgram.created_at.asc()).first()
        if loyalty:
            n = max(1, int(loyalty.qualifying_service_count or 10))
            tiers = _parse_tiers(loyalty.tiers_json)
            reward = _maybe_grant_reward(
                db, "mobile", None, row.city_pin_code, cid, phone_n, n, tiers,
                lambda sid_: _service_name_mobile(db, sid_),
            )
            if reward:
                db.flush()
                from app.services.email_service import lookup_customer_email, send_loyalty_reward_email
                to_email, cust_name = lookup_customer_email(db, cid, row.phone)
                if to_email:
                    send_loyalty_reward_email(
                        to_email,
                        cust_name or row.customer_name,
                        reward.reward_service_name or reward.reward_service_id,
                        channel="mobile",
                    )
                reward.email_sent = bool(to_email)


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


def compute_branch_loyalty_overview(
    db: Session, branch_id: str, phone: str, *, customer_id: str | None = None
) -> dict[str, Any]:
    phone_n = normalize_phone(phone)
    loyalty = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one_or_none()
    branch = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
    branch_name = branch.name if branch else branch_id
    if not loyalty or not (customer_id or phone_n):
        return _empty_overview("branch", branch_id, branch_name, None)

    n = max(1, int(loyalty.qualifying_service_count or 10))
    tiers = _parse_tiers(loyalty.tiers_json)

    rows = (
        db.query(LoyaltyLedgerEntry)
        .filter(
            LoyaltyLedgerEntry.channel == "branch",
            LoyaltyLedgerEntry.branch_id == branch_id,
            _ledger_customer_clause(customer_id, phone_n),
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


def compute_mobile_loyalty_overview(
    db: Session, city_pin: str, phone: str, *, customer_id: str | None = None
) -> dict[str, Any]:
    phone_n = normalize_phone(phone)
    pin = (city_pin or "").strip()
    loyalty = db.query(MobileLoyaltyProgram).order_by(MobileLoyaltyProgram.created_at.asc()).first()
    if not loyalty or not (customer_id or phone_n) or not pin:
        return _empty_overview("mobile", None, "Mobile", pin or None)

    n = max(1, int(loyalty.qualifying_service_count or 10))
    tiers = _parse_tiers(loyalty.tiers_json)

    rows = (
        db.query(LoyaltyLedgerEntry)
        .filter(
            LoyaltyLedgerEntry.channel == "mobile",
            LoyaltyLedgerEntry.city_pin_code == pin,
            _ledger_customer_clause(customer_id, phone_n),
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


def loyalty_overview_for_customer(
    db: Session, *, customer_id: str | None, phone: str
) -> dict[str, Any]:
    """Pick the most recent ledger scope as primary detail for home UI."""
    cid = customer_id or None
    phone_n = normalize_phone(phone)
    if not cid and not phone_n:
        return {"has_any_loyalty": False, "primary": None}

    last = (
        db.query(LoyaltyLedgerEntry)
        .filter(_ledger_customer_clause(cid, phone_n))
        .order_by(desc(LoyaltyLedgerEntry.completed_at))
        .limit(1)
        .one_or_none()
    )
    if not last:
        return {"has_any_loyalty": False, "primary": None}

    if last.channel == "branch" and last.branch_id:
        primary = compute_branch_loyalty_overview(db, str(last.branch_id), phone, customer_id=cid)
    elif last.channel == "mobile" and last.city_pin_code:
        primary = compute_mobile_loyalty_overview(db, str(last.city_pin_code), phone, customer_id=cid)
    else:
        return {"has_any_loyalty": False, "primary": None}

    return {"has_any_loyalty": True, "primary": primary}

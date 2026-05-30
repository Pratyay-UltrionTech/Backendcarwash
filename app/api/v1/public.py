"""Public read + online booking (USER web app)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, OptionalCustomerAuth
from app.core.exceptions import AppError
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import Branch, BranchAddonItem, BranchBooking, BranchLoyalty, BranchSlotSettings, VehicleCatalogBlock
from app.api.v1.serialize import (
    addon_to_dict,
    booking_to_dict,
    branch_to_dict,
    day_rule_to_dict,
    free_coffee_to_dict,
    loyalty_to_dict,
    promo_to_dict,
    slot_settings_to_dict,
    vehicle_block_to_dict,
)
from app.schemas.booking import BookingCreate
from app.services import slot_service
from app.services import booking_service
from app.services.branch_defaults import ensure_branch_defaults

router = APIRouter(prefix="/public", tags=["public"])


def _branch_or_404(db, branch_id: str) -> Branch:
    b = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
    if not b:
        raise HTTPException(status_code=404, detail={"detail": "Branch not found", "code": "not_found"})
    return b


@router.get("/branches")
def list_branches(db: DbSession, request: Request, q: str = "") -> list[dict[str, Any]]:
    started = monotonic_ms()
    query = db.query(Branch).order_by(Branch.name)
    rows = query.all()
    out = [branch_to_dict(b) for b in rows]
    if not q.strip():
        action_log(
            "public_list_branches",
            "success",
            request,
            row_count=len(out),
            latency_ms=round(monotonic_ms() - started, 2),
        )
        return out
    ql = q.strip().lower()
    filtered = [b for b in out if ql in b["name"].lower() or ql in b["location"].lower() or ql in b["zip_code"].lower()]
    action_log(
        "public_list_branches",
        "success",
        request,
        row_count=len(filtered),
        query=q,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return filtered


@router.get("/branches/{branch_id}")
def get_branch(branch_id: str, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    out = branch_to_dict(_branch_or_404(db, branch_id))
    action_log("public_get_branch", "success", request, branch_id=branch_id, latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/vehicle-blocks")
def list_vehicle_blocks(branch_id: str, db: DbSession, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    _branch_or_404(db, branch_id)
    rows = db.query(VehicleCatalogBlock).filter(VehicleCatalogBlock.branch_id == branch_id).all()
    out = [
        vehicle_block_to_dict(v) for v in rows
        if any(
            getattr(s, "active", True) is not False and getattr(s, "catalog_group_id", None)
            for s in v.services
        )
    ]
    action_log("public_list_vehicle_blocks", "success", request, branch_id=branch_id, row_count=len(out), latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/addons")
def list_branch_addons(branch_id: str, db: DbSession, request: Request) -> list[dict[str, Any]]:
    started = monotonic_ms()
    _branch_or_404(db, branch_id)
    rows = db.query(BranchAddonItem).filter(BranchAddonItem.branch_id == branch_id).order_by(BranchAddonItem.name).all()
    out = [addon_to_dict(a) for a in rows]
    action_log("public_list_branch_addons", "success", request, branch_id=branch_id, row_count=len(out), latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/promotions")
def list_promotions(branch_id: str, db: DbSession, request: Request) -> list[dict[str, Any]]:
    from app.models import Promotion

    _branch_or_404(db, branch_id)
    rows = db.query(Promotion).filter(Promotion.branch_id == branch_id).all()
    out = [promo_to_dict(p) for p in rows]
    action_log("public_list_promotions", "success", request, branch_id=branch_id, row_count=len(out))
    return out


@router.get("/branches/{branch_id}/day-time-rules")
def list_day_rules(branch_id: str, db: DbSession, request: Request) -> list[dict[str, Any]]:
    from app.models import DayTimePriceRule

    _branch_or_404(db, branch_id)
    rows = db.query(DayTimePriceRule).filter(DayTimePriceRule.branch_id == branch_id).all()
    out = [day_rule_to_dict(r) for r in rows]
    action_log("public_list_day_rules", "success", request, branch_id=branch_id, row_count=len(out))
    return out


@router.get("/branches/{branch_id}/free-coffee-rules")
def list_free_coffee(branch_id: str, db: DbSession, request: Request) -> list[dict[str, Any]]:
    from app.models import FreeCoffeeRule

    _branch_or_404(db, branch_id)
    rows = db.query(FreeCoffeeRule).filter(FreeCoffeeRule.branch_id == branch_id).all()
    out = [free_coffee_to_dict(f) for f in rows]
    action_log("public_list_free_coffee_rules", "success", request, branch_id=branch_id, row_count=len(out))
    return out


@router.get("/branches/{branch_id}/loyalty")
def get_loyalty(branch_id: str, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            action_log("public_get_loyalty", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
            raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    row = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one()
    out = loyalty_to_dict(row)
    action_log("public_get_loyalty", "success", request, branch_id=branch_id, latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/slots")
def list_slots(
    branch_id: str,
    db: DbSession,
    request: Request,
    date: str = Query(..., description="ISO date YYYY-MM-DD"),
    duration_minutes: int | None = Query(
        default=None,
        ge=30,
        description="Total booking duration (service + add-ons). Defaults to one 30-minute base slot.",
    ),
) -> list[dict[str, Any]]:
    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    out = slot_service.list_available_slots(db, b, date, booking_duration_minutes=duration_minutes)
    action_log("public_list_slots", "success", request, branch_id=branch_id, date=date, row_count=len(out), latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/snapshot")
def branch_snapshot(
    branch_id: str,
    db: DbSession,
    request: Request,
    date: str | None = Query(default=None, description="ISO date YYYY-MM-DD for slot list"),
    duration_minutes: int | None = Query(
        default=None,
        ge=30,
        description="When ``date`` is set, total booking duration for slot availability.",
    ),
) -> dict[str, Any]:
    from app.models import DayTimePriceRule, FreeCoffeeRule, Promotion

    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        try:
            db.commit()
        except SQLAlchemyError:
            db.rollback()
            action_log("public_branch_snapshot", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
            raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    blocks = db.query(VehicleCatalogBlock).filter(VehicleCatalogBlock.branch_id == branch_id).all()
    addons = db.query(BranchAddonItem).filter(BranchAddonItem.branch_id == branch_id).all()
    promos = db.query(Promotion).filter(Promotion.branch_id == branch_id).all()
    rules = db.query(DayTimePriceRule).filter(DayTimePriceRule.branch_id == branch_id).all()
    coffee = db.query(FreeCoffeeRule).filter(FreeCoffeeRule.branch_id == branch_id).all()
    loyalty = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one()
    slot_row = db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch_id).one()
    slots = (
        slot_service.list_available_slots(db, b, date, booking_duration_minutes=duration_minutes) if date else []
    )
    out = {
        "branch": branch_to_dict(b),
        "vehicle_blocks": [
            vehicle_block_to_dict(v) for v in blocks
            if any(
                getattr(s, "active", True) is not False and getattr(s, "catalog_group_id", None)
                for s in v.services
            )
        ],
        "branch_addons": [addon_to_dict(a) for a in addons],
        "promotions": [promo_to_dict(p) for p in promos],
        "day_time_rules": [day_rule_to_dict(r) for r in rules],
        "free_coffee_rules": [free_coffee_to_dict(f) for f in coffee],
        "loyalty": loyalty_to_dict(loyalty),
        "slot_settings": slot_settings_to_dict(slot_row),
        "slots": slots,
    }
    action_log("public_branch_snapshot", "success", request, branch_id=branch_id, date=date or "", latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/bookings/{booking_id}")
def get_public_booking(branch_id: str, booking_id: str, db: DbSession, request: Request) -> dict[str, Any]:
    """Customer-facing booking lookup (e.g. status + tip) using branch id and booking id."""
    started = monotonic_ms()
    _branch_or_404(db, branch_id)
    job = (
        db.query(BranchBooking)
        .filter(BranchBooking.id == booking_id, BranchBooking.branch_id == branch_id)
        .one_or_none()
    )
    if not job:
        action_log(
            "public_get_booking",
            "failed",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            error_code="not_found",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(
            status_code=404,
            detail={"detail": "Booking not found", "code": "not_found"},
        )
    action_log(
        "public_get_booking",
        "success",
        request,
        branch_id=branch_id,
        booking_id=booking_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return booking_to_dict(job)


@router.post("/branches/{branch_id}/bookings")
def create_online_booking(
    branch_id: str, body: BookingCreate, db: DbSession, request: Request, auth: OptionalCustomerAuth = None
) -> dict[str, Any]:
    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    try:
        job = booking_service.create_booking(
            db,
            b,
            customer_name=body.customer_name,
            phone=body.phone,
            customer_email=(body.customer_email or "").strip(),
            address=body.address,
            vehicle_type=body.vehicle_type,
            vehicle_model=body.vehicle_model,
            registration_number=body.registration_number or "",
            service_summary=body.service_summary,
            service_id=body.service_id,
            selected_addon_ids=body.selected_addon_ids,
            slot_date=body.slot_date,
            start_time=body.start_time,
            end_time=body.end_time,
            source="online",
            tip_cents=body.tip_cents,
            service_charged_cents=body.service_charged_cents,
            booking_id=body.booking_id,
            customer_id=str(auth["sub"]) if auth and auth.get("sub") else None,
            promo_code=body.promo_code,
            payment_method=body.payment_method,
        )

        if auth and auth.get("sub"):
            from app.services import customer_service
            customer_service.record_customer_vehicle(
                db,
                str(auth["sub"]),
                body.vehicle_type,
                body.vehicle_model,
                body.registration_number or "",
            )

        # Consume loyalty reward if one was applied for this booking
        if body.loyalty_reward_id and auth and auth.get("sub"):
            from app.services.loyalty_service import consume_reward
            consume_reward(db, body.loyalty_reward_id, str(auth["sub"]), job.id)

        db.commit()
        audit_log(
            "customer_public",
            "anonymous",
            "create_online_booking",
            request,
            branch_id=branch_id,
            booking_id=job.id,
            source="online",
        )
        action_log(
            "public_create_online_booking",
            "success",
            request,
            branch_id=branch_id,
            booking_id=job.id,
            latency_ms=round(monotonic_ms() - started, 2),
        )
    except AppError as e:
        db.rollback()
        action_log(
            "public_create_online_booking",
            "failed",
            request,
            branch_id=branch_id,
            error_code=e.code,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=e.status_code, detail={"detail": e.message, "code": e.code})
    except SQLAlchemyError:
        db.rollback()
        action_log(
            "public_create_online_booking",
            "failed",
            request,
            branch_id=branch_id,
            error_code="db_commit_failed",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(job)
    from app.services.email_service import send_booking_confirmed_email, send_staff_booking_notification

    if auth and auth.get("email"):
        send_booking_confirmed_email(
            to_email=str(auth["email"]),
            name=job.customer_name or "",
            date=job.slot_date,
            start_time=job.start_time,
            service_summary=job.service_summary or "",
            booking_id=job.id,
            customer_id=str(job.customer_id) if job.customer_id else None,
            phone=job.phone or None,
            end_time=job.end_time or None,
            channel="branch",
            payment_method=getattr(job, "payment_method", None),
        )
    elif (getattr(job, "customer_email", None) or "").strip():
        send_booking_confirmed_email(
            to_email=str(job.customer_email).strip(),
            name=job.customer_name or "",
            date=job.slot_date,
            start_time=job.start_time,
            service_summary=job.service_summary or "",
            booking_id=job.id,
            customer_id=str(job.customer_id) if job.customer_id else None,
            phone=job.phone or None,
            end_time=job.end_time or None,
            channel="branch",
            payment_method=getattr(job, "payment_method", None),
        )
    send_staff_booking_notification(
        db,
        event="new_booking",
        booking_type="branch",
        booking_id=job.id,
        customer_name=job.customer_name or "",
        phone=job.phone or "",
        vehicle_type=job.vehicle_type or "",
        vehicle_model=job.vehicle_model or "",
        registration_number=job.registration_number or "",
        service_summary=job.service_summary or "",
        slot_date=job.slot_date,
        start_time=job.start_time,
        end_time=job.end_time,
        branch_id=str(job.branch_id),
        customer_id=str(job.customer_id) if job.customer_id else None,
        payment_method=getattr(job, "payment_method", None),
    )
    return booking_to_dict(job)


@router.get("/promo/usage")
def get_promo_usage(
    promo_code: str = Query(...),
    email: str = Query(default=""),
    phone: str = Query(default=""),
    db: DbSession = None,
    request: Request = None,
) -> dict[str, int]:
    """Return how many times a given identity (email and/or phone) has used a promo code
    across both branch and mobile bookings, excluding cancelled bookings.
    Allows the user portal to enforce per-customer promo limits server-side.
    """
    from app.models.mobile import MobileBooking

    if not promo_code or (not email and not phone):
        return {"uses": 0}

    code_upper = promo_code.strip().upper()
    email_clean = (email or "").strip().lower()
    phone_clean = (phone or "").strip()

    count = 0

    # Branch bookings
    branch_rows = (
        db.query(BranchBooking)
        .filter(
            BranchBooking.promo_code == code_upper,
            BranchBooking.status.notin_(["cancelled", "rejected", "failed"]),
        )
        .all()
    )
    for b in branch_rows:
        b_email = (getattr(b, "customer_email", "") or "").strip().lower()
        b_phone = (getattr(b, "phone", "") or "").strip()
        if (email_clean and b_email == email_clean) or (phone_clean and b_phone == phone_clean):
            count += 1

    # Mobile bookings
    mobile_rows = (
        db.query(MobileBooking)
        .filter(
            MobileBooking.promo_code == code_upper,
            MobileBooking.status.notin_(["cancelled", "rejected", "failed"]),
        )
        .all()
    )
    for m in mobile_rows:
        m_email = (getattr(m, "customer_email", "") or "").strip().lower()
        m_phone = (getattr(m, "phone", "") or "").strip()
        if (email_clean and m_email == email_clean) or (phone_clean and m_phone == phone_clean):
            count += 1

    return {"uses": count}

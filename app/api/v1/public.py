"""Public read + online booking (USER web app)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession
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
    out = [vehicle_block_to_dict(v) for v in rows]
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
    branch_id: str, db: DbSession, request: Request, date: str = Query(..., description="ISO date YYYY-MM-DD")
) -> list[dict[str, Any]]:
    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    out = slot_service.list_available_slots(db, b, date)
    action_log("public_list_slots", "success", request, branch_id=branch_id, date=date, row_count=len(out), latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/branches/{branch_id}/snapshot")
def branch_snapshot(
    branch_id: str,
    db: DbSession,
    request: Request,
    date: str | None = Query(default=None, description="ISO date YYYY-MM-DD for slot list"),
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
    slots = slot_service.list_available_slots(db, b, date) if date else []
    out = {
        "branch": branch_to_dict(b),
        "vehicle_blocks": [vehicle_block_to_dict(v) for v in blocks],
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
def create_online_booking(branch_id: str, body: BookingCreate, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    b = _branch_or_404(db, branch_id)
    try:
        job = booking_service.create_booking(
            db,
            b,
            customer_name=body.customer_name,
            phone=body.phone,
            address=body.address,
            vehicle_type=body.vehicle_type,
            service_summary=body.service_summary,
            service_id=body.service_id,
            slot_date=body.slot_date,
            start_time=body.start_time,
            end_time=body.end_time,
            source="online",
            tip_cents=body.tip_cents,
        )
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
    return booking_to_dict(job)

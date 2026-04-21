"""Admin API — branches, staff, catalog, promos, slots, loyalty."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.api.deps import AdminUser, DbSession
from app.core.exceptions import AppError
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import (
    Branch,
    BranchLoyalty,
    BranchManager,
    BranchSlotSettings,
    BranchBooking,
    BranchAddonItem,
    CatalogAddonItem,
    CatalogServiceItem,
    DayTimePriceRule,
    FreeCoffeeRule,
    Promotion,
    VehicleCatalogBlock,
    Washer,
)
from app.models.base import new_id
from app.core.security import hash_password
from app.api.v1.serialize import (
    addon_to_dict,
    booking_to_dict,
    branch_to_dict,
    day_rule_to_dict,
    free_coffee_to_dict,
    loyalty_to_dict,
    manager_to_dict,
    promo_to_dict,
    slot_settings_to_dict,
    vehicle_block_to_dict,
    washer_to_dict,
)
from app.schemas.branch import BranchCreate, BranchUpdate
from app.schemas.booking import BookingCreate, BookingUpdate
from app.schemas.catalog import (
    AddonItemIn,
    DayTimeRuleIn,
    FreeCoffeeRuleIn,
    LoyaltyProgramIn,
    PromoIn,
    SlotSettingsPatch,
    VehicleBlockCreate,
)
from app.schemas.staff import ManagerCreate, ManagerUpdate, WasherCreate, WasherUpdate
from app.services import booking_service, loyalty_service
from app.services.branch_defaults import ensure_branch_defaults
from app.services.jsonutil import dumps_json

router = APIRouter(prefix="/admin", tags=["admin"])
sub = "sub"


def _branch_or_404(db: Session, branch_id: str) -> Branch:
    b = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
    if not b:
        raise HTTPException(status_code=404, detail={"detail": "Branch not found", "code": "not_found"})
    return b


def _has_active_bookings(db: Session, branch_id: str) -> bool:
    return (
        db.query(BranchBooking)
        .filter(
            BranchBooking.branch_id == branch_id,
            BranchBooking.status.in_(["scheduled", "checked_in", "in_progress"]),
        )
        .first()
        is not None
    )


def _ensure_unique_branch_name(db: Session, name: str, exclude_id: str | None = None) -> None:
    normalized_name = name.strip().lower()
    q = db.query(Branch).filter(func.lower(Branch.name) == normalized_name)
    if exclude_id:
        q = q.filter(Branch.id != exclude_id)
    if q.first():
        raise HTTPException(status_code=409, detail={"detail": "Branch name already exists", "code": "conflict"})


def _ensure_unique_manager_email(db: Session, branch_id: str, email: str, exclude_id: str | None = None) -> None:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return
    q = db.query(BranchManager).filter(
        BranchManager.branch_id == branch_id,
        func.lower(BranchManager.email) == normalized_email,
    )
    if exclude_id:
        q = q.filter(BranchManager.id != exclude_id)
    if q.first():
        raise HTTPException(status_code=409, detail={"detail": "Manager email already exists", "code": "conflict"})


def _date_ranges_overlap(start_a: str, end_a: str, start_b: str, end_b: str) -> bool:
    if not start_a or not end_a or not start_b or not end_b:
        return True
    a_start = date.fromisoformat(start_a)
    a_end = date.fromisoformat(end_a)
    b_start = date.fromisoformat(start_b)
    b_end = date.fromisoformat(end_b)
    return max(a_start, b_start) <= min(a_end, b_end)


def _times_overlap(start_a: str, end_a: str, start_b: str, end_b: str) -> bool:
    if not start_a or not end_a or not start_b or not end_b:
        return True
    return max(start_a, start_b) < min(end_a, end_b)


def _ensure_no_day_rule_overlap(
    db: Session, branch_id: str, body: DayTimeRuleIn, exclude_id: str | None = None
) -> None:
    rows = db.query(DayTimePriceRule).filter(DayTimePriceRule.branch_id == branch_id).all()
    for row in rows:
        if exclude_id and row.id == exclude_id:
            continue
        if not _date_ranges_overlap(body.validity_start, body.validity_end, row.validity_start, row.validity_end):
            continue
        other_days = json.loads(row.applicable_days or "[]")
        days_a = set(body.applicable_days or [])
        days_b = set(other_days or [])
        days_overlap = not days_a or not days_b or bool(days_a & days_b)
        if not days_overlap:
            continue
        if _times_overlap(body.time_window_start, body.time_window_end, row.time_window_start, row.time_window_end):
            raise HTTPException(
                status_code=409,
                detail={"detail": "Pricing rule overlaps with an existing rule", "code": "overlap_conflict"},
            )


@router.get("/branches")
def list_branches(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    rows = db.query(Branch).order_by(Branch.name).all()
    return [branch_to_dict(b) for b in rows]


@router.post("/branches")
def create_branch(body: BranchCreate, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _ensure_unique_branch_name(db, body.name)
    b = Branch(
        name=body.name,
        location=body.location,
        zip_code=body.zip_code,
        bay_count=body.bay_count,
        open_time=body.open_time,
        close_time=body.close_time,
    )
    db.add(b)
    db.flush()
    ensure_branch_defaults(db, b)
    db.commit()
    db.refresh(b)
    audit_log("admin", admin_id, "create_branch", request, branch_id=b.id)
    action_log(
        "admin_create_branch",
        "success",
        request,
        branch_id=b.id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return branch_to_dict(b)


@router.get("/branches/{branch_id}")
def get_branch(branch_id: str, db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    return branch_to_dict(_branch_or_404(db, branch_id))


@router.patch("/branches/{branch_id}")
def update_branch(
    branch_id: str, body: BranchUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    data = body.model_dump(exclude_unset=True)
    if "name" in data:
        _ensure_unique_branch_name(db, data["name"], exclude_id=branch_id)
    for k, v in data.items():
        setattr(b, k, v)
    db.commit()
    db.refresh(b)
    audit_log("admin", admin_id, "update_branch", request, branch_id=branch_id)
    action_log(
        "admin_update_branch",
        "success",
        request,
        branch_id=branch_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return branch_to_dict(b)


@router.delete("/branches/{branch_id}", status_code=204)
def delete_branch(branch_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    if _has_active_bookings(db, branch_id):
        raise HTTPException(
            status_code=409,
            detail={"detail": "Branch has active bookings and cannot be deleted", "code": "active_bookings"},
        )
    db.delete(b)
    db.commit()
    audit_log("admin", admin_id, "delete_branch", request, branch_id=branch_id)
    action_log(
        "admin_delete_branch",
        "success",
        request,
        branch_id=branch_id,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return Response(status_code=204)


# --- Managers ---


@router.get("/branches/{branch_id}/managers")
def list_managers(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(BranchManager).filter(BranchManager.branch_id == branch_id).all()
    return [manager_to_dict(m) for m in rows]


@router.post("/branches/{branch_id}/managers")
def create_manager(
    branch_id: str, body: ManagerCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    normalized_email = body.email.strip().lower()
    _ensure_unique_manager_email(db, branch_id, normalized_email)
    m = BranchManager(
        branch_id=branch_id,
        name=body.name,
        address=body.address,
        zip_code=body.zip_code,
        email=normalized_email,
        phone=body.phone,
        doj=body.doj,
        login_id=body.login_id,
        password_hash=hash_password(body.password),
        active=body.active,
    )
    db.add(m)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_create_manager", "failed", request, branch_id=branch_id, error_code="conflict")
        raise HTTPException(status_code=409, detail={"detail": "Duplicate login_id for branch", "code": "conflict"})
    db.refresh(m)
    audit_log("admin", admin_id, "create_manager", request, branch_id=branch_id, manager_id=m.id)
    action_log("admin_create_manager", "success", request, branch_id=branch_id, manager_id=m.id, latency_ms=round(monotonic_ms() - started, 2))
    return manager_to_dict(m)


@router.patch("/branches/{branch_id}/managers/{manager_id}")
def update_manager(
    branch_id: str, manager_id: str, body: ManagerUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    m = (
        db.query(BranchManager)
        .filter(BranchManager.id == manager_id, BranchManager.branch_id == branch_id)
        .one_or_none()
    )
    if not m:
        raise HTTPException(status_code=404, detail={"detail": "Manager not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    next_email = str(data.get("email", m.email) or "").strip().lower()
    _ensure_unique_manager_email(db, branch_id, next_email, exclude_id=manager_id)
    if "email" in data:
        data["email"] = next_email
    if "password" in data and data["password"]:
        m.password_hash = hash_password(data.pop("password"))
    for k, v in data.items():
        setattr(m, k, v)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_update_manager", "failed", request, branch_id=branch_id, manager_id=manager_id, error_code="conflict")
        raise HTTPException(status_code=409, detail={"detail": "Duplicate login_id for branch", "code": "conflict"})
    db.refresh(m)
    audit_log("admin", admin_id, "update_manager", request, branch_id=branch_id, manager_id=manager_id)
    action_log("admin_update_manager", "success", request, branch_id=branch_id, manager_id=manager_id, latency_ms=round(monotonic_ms() - started, 2))
    return manager_to_dict(m)


@router.delete("/branches/{branch_id}/managers/{manager_id}", status_code=204)
def delete_manager(branch_id: str, manager_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    m = (
        db.query(BranchManager)
        .filter(BranchManager.id == manager_id, BranchManager.branch_id == branch_id)
        .one_or_none()
    )
    if not m:
        raise HTTPException(status_code=404, detail={"detail": "Manager not found", "code": "not_found"})
    db.delete(m)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_manager", "failed", request, branch_id=branch_id, manager_id=manager_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_manager", request, branch_id=branch_id, manager_id=manager_id)
    action_log("admin_delete_manager", "success", request, branch_id=branch_id, manager_id=manager_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Washers ---


@router.get("/branches/{branch_id}/washers")
def list_washers(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(Washer).filter(Washer.branch_id == branch_id).all()
    return [washer_to_dict(w) for w in rows]


@router.post("/branches/{branch_id}/washers")
def create_washer(branch_id: str, body: WasherCreate, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    if body.assigned_bay > b.bay_count:
        raise HTTPException(status_code=400, detail={"detail": "assigned_bay exceeds bay_count", "code": "invalid_bay"})
    w = Washer(
        branch_id=branch_id,
        name=body.name,
        address=body.address,
        zip_code=body.zip_code,
        email=body.email,
        phone=body.phone,
        doj=body.doj,
        login_id=body.login_id,
        password_hash=hash_password(body.password),
        assigned_bay=body.assigned_bay,
        active=body.active,
    )
    db.add(w)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_create_washer", "failed", request, branch_id=branch_id, error_code="conflict")
        raise HTTPException(status_code=409, detail={"detail": "Duplicate login_id for branch", "code": "conflict"})
    db.refresh(w)
    audit_log("admin", admin_id, "create_washer", request, branch_id=branch_id, washer_id=w.id)
    action_log("admin_create_washer", "success", request, branch_id=branch_id, washer_id=w.id, latency_ms=round(monotonic_ms() - started, 2))
    return washer_to_dict(w)


@router.patch("/branches/{branch_id}/washers/{washer_id}")
def update_washer(
    branch_id: str, washer_id: str, body: WasherUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    w = db.query(Washer).filter(Washer.id == washer_id, Washer.branch_id == branch_id).one_or_none()
    if not w:
        raise HTTPException(status_code=404, detail={"detail": "Washer not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    if "password" in data and data["password"]:
        w.password_hash = hash_password(data.pop("password"))
    if "assigned_bay" in data and data["assigned_bay"] is not None and data["assigned_bay"] > b.bay_count:
        raise HTTPException(status_code=400, detail={"detail": "assigned_bay exceeds bay_count", "code": "invalid_bay"})
    for k, v in data.items():
        setattr(w, k, v)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_update_washer", "failed", request, branch_id=branch_id, washer_id=washer_id, error_code="conflict")
        raise HTTPException(status_code=409, detail={"detail": "Duplicate login_id for branch", "code": "conflict"})
    db.refresh(w)
    audit_log("admin", admin_id, "update_washer", request, branch_id=branch_id, washer_id=washer_id)
    action_log("admin_update_washer", "success", request, branch_id=branch_id, washer_id=washer_id, latency_ms=round(monotonic_ms() - started, 2))
    return washer_to_dict(w)


@router.delete("/branches/{branch_id}/washers/{washer_id}", status_code=204)
def delete_washer(branch_id: str, washer_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    w = db.query(Washer).filter(Washer.id == washer_id, Washer.branch_id == branch_id).one_or_none()
    if not w:
        raise HTTPException(status_code=404, detail={"detail": "Washer not found", "code": "not_found"})
    db.delete(w)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_washer", "failed", request, branch_id=branch_id, washer_id=washer_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_washer", request, branch_id=branch_id, washer_id=washer_id)
    action_log("admin_delete_washer", "success", request, branch_id=branch_id, washer_id=washer_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Vehicle catalog ---


@router.get("/branches/{branch_id}/vehicle-blocks")
def list_vehicle_blocks(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(VehicleCatalogBlock).filter(VehicleCatalogBlock.branch_id == branch_id).all()
    return [vehicle_block_to_dict(v) for v in rows]


@router.post("/branches/{branch_id}/vehicle-blocks")
def create_vehicle_block(
    branch_id: str, body: VehicleBlockCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    block = VehicleCatalogBlock(branch_id=branch_id, vehicle_type=body.vehicle_type)
    db.add(block)
    db.flush()
    for s in body.services:
        sid = s.id or new_id()
        db.add(
            CatalogServiceItem(
                id=sid,
                vehicle_block_id=block.id,
                name=s.name,
                price=s.price,
                free_coffee_count=s.free_coffee_count,
                eligible_for_loyalty_points=s.eligible_for_loyalty_points,
                recommended=s.recommended,
                description_points=dumps_json(s.description_points),
                active=s.active,
                catalog_group_id=s.catalog_group_id,
                duration_minutes=s.duration_minutes,
            )
        )
    for a in body.addons:
        aid = a.id or new_id()
        db.add(
            CatalogAddonItem(
                id=aid,
                vehicle_block_id=block.id,
                name=a.name,
                price=a.price,
                description_points=dumps_json(a.description_points),
                active=a.active,
            )
        )
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_create_vehicle_block", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(block)
    audit_log("admin", admin_id, "create_vehicle_block", request, branch_id=branch_id, block_id=block.id)
    action_log("admin_create_vehicle_block", "success", request, branch_id=branch_id, block_id=block.id, latency_ms=round(monotonic_ms() - started, 2))
    return vehicle_block_to_dict(block)


@router.put("/branches/{branch_id}/vehicle-blocks/{block_id}")
def replace_vehicle_block(
    branch_id: str, block_id: str, body: VehicleBlockCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    block = (
        db.query(VehicleCatalogBlock)
        .filter(VehicleCatalogBlock.id == block_id, VehicleCatalogBlock.branch_id == branch_id)
        .one_or_none()
    )
    if not block:
        raise HTTPException(status_code=404, detail={"detail": "Vehicle block not found", "code": "not_found"})
    db.query(CatalogServiceItem).filter(CatalogServiceItem.vehicle_block_id == block.id).delete()
    db.query(CatalogAddonItem).filter(CatalogAddonItem.vehicle_block_id == block.id).delete()
    block.vehicle_type = body.vehicle_type
    for s in body.services:
        sid = s.id or new_id()
        db.add(
            CatalogServiceItem(
                id=sid,
                vehicle_block_id=block.id,
                name=s.name,
                price=s.price,
                free_coffee_count=s.free_coffee_count,
                eligible_for_loyalty_points=s.eligible_for_loyalty_points,
                recommended=s.recommended,
                description_points=dumps_json(s.description_points),
                active=s.active,
                catalog_group_id=s.catalog_group_id,
                duration_minutes=s.duration_minutes,
            )
        )
    for a in body.addons:
        aid = a.id or new_id()
        db.add(
            CatalogAddonItem(
                id=aid,
                vehicle_block_id=block.id,
                name=a.name,
                price=a.price,
                description_points=dumps_json(a.description_points),
                active=a.active,
            )
        )
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_replace_vehicle_block", "failed", request, branch_id=branch_id, block_id=block_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(block)
    audit_log("admin", admin_id, "replace_vehicle_block", request, branch_id=branch_id, block_id=block_id)
    action_log("admin_replace_vehicle_block", "success", request, branch_id=branch_id, block_id=block_id, latency_ms=round(monotonic_ms() - started, 2))
    return vehicle_block_to_dict(block)


@router.delete("/branches/{branch_id}/vehicle-blocks/{block_id}", status_code=204)
def delete_vehicle_block(branch_id: str, block_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    block = (
        db.query(VehicleCatalogBlock)
        .filter(VehicleCatalogBlock.id == block_id, VehicleCatalogBlock.branch_id == branch_id)
        .one_or_none()
    )
    if not block:
        raise HTTPException(status_code=404, detail={"detail": "Vehicle block not found", "code": "not_found"})
    db.delete(block)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_vehicle_block", "failed", request, branch_id=branch_id, block_id=block_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_vehicle_block", request, branch_id=branch_id, block_id=block_id)
    action_log("admin_delete_vehicle_block", "success", request, branch_id=branch_id, block_id=block_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Branch add-ons ---


@router.get("/branches/{branch_id}/addons")
def list_branch_addons(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(BranchAddonItem).filter(BranchAddonItem.branch_id == branch_id).order_by(BranchAddonItem.name).all()
    return [addon_to_dict(a) for a in rows]


@router.post("/branches/{branch_id}/addons")
def create_branch_addon(
    branch_id: str, body: AddonItemIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    row = BranchAddonItem(
        id=body.id or new_id(),
        branch_id=branch_id,
        name=body.name,
        price=body.price,
        description_points=dumps_json(body.description_points),
        active=body.active,
    )
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_create_branch_addon", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "create_branch_addon", request, branch_id=branch_id, addon_id=row.id)
    action_log("admin_create_branch_addon", "success", request, branch_id=branch_id, addon_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    return addon_to_dict(row)


@router.patch("/branches/{branch_id}/addons/{addon_id}")
def update_branch_addon(
    branch_id: str, addon_id: str, body: AddonItemIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    row = (
        db.query(BranchAddonItem)
        .filter(BranchAddonItem.id == addon_id, BranchAddonItem.branch_id == branch_id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Add-on not found", "code": "not_found"})
    row.name = body.name
    row.price = body.price
    row.description_points = dumps_json(body.description_points)
    row.active = body.active
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_update_branch_addon", "failed", request, branch_id=branch_id, addon_id=addon_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_branch_addon", request, branch_id=branch_id, addon_id=addon_id)
    action_log("admin_update_branch_addon", "success", request, branch_id=branch_id, addon_id=addon_id, latency_ms=round(monotonic_ms() - started, 2))
    return addon_to_dict(row)


@router.delete("/branches/{branch_id}/addons/{addon_id}", status_code=204)
def delete_branch_addon(branch_id: str, addon_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    row = (
        db.query(BranchAddonItem)
        .filter(BranchAddonItem.id == addon_id, BranchAddonItem.branch_id == branch_id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Add-on not found", "code": "not_found"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_branch_addon", "failed", request, branch_id=branch_id, addon_id=addon_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_branch_addon", request, branch_id=branch_id, addon_id=addon_id)
    action_log("admin_delete_branch_addon", "success", request, branch_id=branch_id, addon_id=addon_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Promotions ---


@router.get("/branches/{branch_id}/promotions")
def list_promotions(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(Promotion).filter(Promotion.branch_id == branch_id).all()
    return [promo_to_dict(p) for p in rows]


@router.post("/branches/{branch_id}/promotions")
def create_promotion(branch_id: str, body: PromoIn, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    pid = body.id or new_id()
    p = Promotion(
        id=pid,
        branch_id=branch_id,
        code_name=body.code_name,
        discount_type=body.discount_type,
        discount_value=body.discount_value,
        validity_start=body.validity_start,
        validity_end=body.validity_end,
        max_uses_per_customer=body.max_uses_per_customer,
        applicable_service_ids=dumps_json(body.applicable_service_ids),
        applicable_vehicle_types=dumps_json(body.applicable_vehicle_types),
    )
    db.add(p)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_create_promotion", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(p)
    audit_log("admin", admin_id, "create_promotion", request, branch_id=branch_id, promo_id=p.id)
    action_log("admin_create_promotion", "success", request, branch_id=branch_id, promo_id=p.id, latency_ms=round(monotonic_ms() - started, 2))
    return promo_to_dict(p)


@router.patch("/branches/{branch_id}/promotions/{promo_id}")
def update_promotion(
    branch_id: str, promo_id: str, body: PromoIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    p = db.query(Promotion).filter(Promotion.id == promo_id, Promotion.branch_id == branch_id).one_or_none()
    if not p:
        raise HTTPException(status_code=404, detail={"detail": "Promotion not found", "code": "not_found"})
    p.code_name = body.code_name
    p.discount_type = body.discount_type
    p.discount_value = body.discount_value
    p.validity_start = body.validity_start
    p.validity_end = body.validity_end
    p.max_uses_per_customer = body.max_uses_per_customer
    p.applicable_service_ids = dumps_json(body.applicable_service_ids)
    p.applicable_vehicle_types = dumps_json(body.applicable_vehicle_types)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_update_promotion", "failed", request, branch_id=branch_id, promo_id=promo_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(p)
    audit_log("admin", admin_id, "update_promotion", request, branch_id=branch_id, promo_id=promo_id)
    action_log("admin_update_promotion", "success", request, branch_id=branch_id, promo_id=promo_id, latency_ms=round(monotonic_ms() - started, 2))
    return promo_to_dict(p)


@router.delete("/branches/{branch_id}/promotions/{promo_id}", status_code=204)
def delete_promotion(branch_id: str, promo_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    p = db.query(Promotion).filter(Promotion.id == promo_id, Promotion.branch_id == branch_id).one_or_none()
    if not p:
        raise HTTPException(status_code=404, detail={"detail": "Promotion not found", "code": "not_found"})
    db.delete(p)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_promotion", "failed", request, branch_id=branch_id, promo_id=promo_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_promotion", request, branch_id=branch_id, promo_id=promo_id)
    action_log("admin_delete_promotion", "success", request, branch_id=branch_id, promo_id=promo_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Day / time pricing ---


@router.get("/branches/{branch_id}/day-time-rules")
def list_day_rules(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(DayTimePriceRule).filter(DayTimePriceRule.branch_id == branch_id).all()
    return [day_rule_to_dict(r) for r in rows]


@router.post("/branches/{branch_id}/day-time-rules")
def create_day_rule(branch_id: str, body: DayTimeRuleIn, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    _ensure_no_day_rule_overlap(db, branch_id, body)
    rid = body.id or new_id()
    r = DayTimePriceRule(
        id=rid,
        branch_id=branch_id,
        title=body.title,
        description=body.description,
        discount_type=body.discount_type,
        discount_value=body.discount_value,
        applicable_service_ids=dumps_json(body.applicable_service_ids),
        applicable_vehicle_types=dumps_json(body.applicable_vehicle_types),
        applicable_days=dumps_json(body.applicable_days),
        time_window_start=body.time_window_start,
        time_window_end=body.time_window_end,
        validity_start=body.validity_start,
        validity_end=body.validity_end,
    )
    db.add(r)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_create_day_rule", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(r)
    audit_log("admin", admin_id, "create_day_rule", request, branch_id=branch_id, rule_id=r.id)
    action_log("admin_create_day_rule", "success", request, branch_id=branch_id, rule_id=r.id, latency_ms=round(monotonic_ms() - started, 2))
    return day_rule_to_dict(r)


@router.patch("/branches/{branch_id}/day-time-rules/{rule_id}")
def update_day_rule(
    branch_id: str, rule_id: str, body: DayTimeRuleIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    r = (
        db.query(DayTimePriceRule)
        .filter(DayTimePriceRule.id == rule_id, DayTimePriceRule.branch_id == branch_id)
        .one_or_none()
    )
    if not r:
        raise HTTPException(status_code=404, detail={"detail": "Rule not found", "code": "not_found"})
    _ensure_no_day_rule_overlap(db, branch_id, body, exclude_id=rule_id)
    r.title = body.title
    r.description = body.description
    r.discount_type = body.discount_type
    r.discount_value = body.discount_value
    r.applicable_service_ids = dumps_json(body.applicable_service_ids)
    r.applicable_vehicle_types = dumps_json(body.applicable_vehicle_types)
    r.applicable_days = dumps_json(body.applicable_days)
    r.time_window_start = body.time_window_start
    r.time_window_end = body.time_window_end
    r.validity_start = body.validity_start
    r.validity_end = body.validity_end
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_update_day_rule", "failed", request, branch_id=branch_id, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(r)
    audit_log("admin", admin_id, "update_day_rule", request, branch_id=branch_id, rule_id=rule_id)
    action_log("admin_update_day_rule", "success", request, branch_id=branch_id, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return day_rule_to_dict(r)


@router.delete("/branches/{branch_id}/day-time-rules/{rule_id}", status_code=204)
def delete_day_rule(branch_id: str, rule_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    r = (
        db.query(DayTimePriceRule)
        .filter(DayTimePriceRule.id == rule_id, DayTimePriceRule.branch_id == branch_id)
        .one_or_none()
    )
    if not r:
        raise HTTPException(status_code=404, detail={"detail": "Rule not found", "code": "not_found"})
    db.delete(r)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_day_rule", "failed", request, branch_id=branch_id, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_day_rule", request, branch_id=branch_id, rule_id=rule_id)
    action_log("admin_delete_day_rule", "success", request, branch_id=branch_id, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Free coffee ---


@router.get("/branches/{branch_id}/free-coffee-rules")
def list_free_coffee(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = db.query(FreeCoffeeRule).filter(FreeCoffeeRule.branch_id == branch_id).all()
    return [free_coffee_to_dict(f) for f in rows]


@router.post("/branches/{branch_id}/free-coffee-rules")
def create_free_coffee(
    branch_id: str, body: FreeCoffeeRuleIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    fid = body.id or new_id()
    f = FreeCoffeeRule(
        id=fid,
        branch_id=branch_id,
        kind=body.kind,
        service_name=body.service_name,
        services_count=body.services_count,
        notes=body.notes,
    )
    db.add(f)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_create_free_coffee_rule", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(f)
    audit_log("admin", admin_id, "create_free_coffee_rule", request, branch_id=branch_id, rule_id=f.id)
    action_log("admin_create_free_coffee_rule", "success", request, branch_id=branch_id, rule_id=f.id, latency_ms=round(monotonic_ms() - started, 2))
    return free_coffee_to_dict(f)


@router.patch("/branches/{branch_id}/free-coffee-rules/{rule_id}")
def update_free_coffee(
    branch_id: str, rule_id: str, body: FreeCoffeeRuleIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    f = (
        db.query(FreeCoffeeRule)
        .filter(FreeCoffeeRule.id == rule_id, FreeCoffeeRule.branch_id == branch_id)
        .one_or_none()
    )
    if not f:
        raise HTTPException(status_code=404, detail={"detail": "Rule not found", "code": "not_found"})
    f.kind = body.kind
    f.service_name = body.service_name
    f.services_count = body.services_count
    f.notes = body.notes
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_update_free_coffee_rule", "failed", request, branch_id=branch_id, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(f)
    audit_log("admin", admin_id, "update_free_coffee_rule", request, branch_id=branch_id, rule_id=rule_id)
    action_log("admin_update_free_coffee_rule", "success", request, branch_id=branch_id, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return free_coffee_to_dict(f)


@router.delete("/branches/{branch_id}/free-coffee-rules/{rule_id}", status_code=204)
def delete_free_coffee(branch_id: str, rule_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    _branch_or_404(db, branch_id)
    f = (
        db.query(FreeCoffeeRule)
        .filter(FreeCoffeeRule.id == rule_id, FreeCoffeeRule.branch_id == branch_id)
        .one_or_none()
    )
    if not f:
        raise HTTPException(status_code=404, detail={"detail": "Rule not found", "code": "not_found"})
    db.delete(f)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_delete_free_coffee_rule", "failed", request, branch_id=branch_id, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_free_coffee_rule", request, branch_id=branch_id, rule_id=rule_id)
    action_log("admin_delete_free_coffee_rule", "success", request, branch_id=branch_id, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


# --- Loyalty ---


@router.get("/branches/{branch_id}/loyalty")
def get_loyalty(branch_id: str, db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        db.commit()
    row = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one()
    return loyalty_to_dict(row)


@router.put("/branches/{branch_id}/loyalty")
def put_loyalty(branch_id: str, body: LoyaltyProgramIn, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        db.commit()
    row = db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch_id).one()
    row.qualifying_service_count = body.qualifying_service_count
    tiers_payload = [
        {
            "id": t.id,
            "minSpendInWindow": t.min_spend_in_window,
            "maxSpendInWindow": t.max_spend_in_window,
            "rewardServiceId": t.reward_service_id,
        }
        for t in body.tiers
    ]
    row.tiers_json = json.dumps(tiers_payload)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_put_loyalty", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_loyalty", request, branch_id=branch_id)
    action_log("admin_put_loyalty", "success", request, branch_id=branch_id, latency_ms=round(monotonic_ms() - started, 2))
    return loyalty_to_dict(row)


# --- Slot settings ---


@router.get("/branches/{branch_id}/slot-settings")
def get_slot_settings(branch_id: str, db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        db.commit()
    s = db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch_id).one()
    return slot_settings_to_dict(s)


@router.patch("/branches/{branch_id}/slot-settings")
def patch_slot_settings(
    branch_id: str, body: SlotSettingsPatch, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    if ensure_branch_defaults(db, b):
        db.commit()
    s = db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch_id).one()
    data = body.model_dump(exclude_unset=True)
    if "manager_slot_duration_minutes" in data and data["manager_slot_duration_minutes"] is not None:
        s.manager_slot_duration_minutes = data["manager_slot_duration_minutes"]
    if "slot_bay_open_by_window" in data and data["slot_bay_open_by_window"] is not None:
        s.slot_bay_open_by_window_json = json.dumps(data["slot_bay_open_by_window"])
    if "slot_window_active_by_key" in data and data["slot_window_active_by_key"] is not None:
        s.slot_window_active_by_key_json = json.dumps(data["slot_window_active_by_key"])
    if "slot_day_states" in data and data["slot_day_states"] is not None:
        s.slot_day_states_json = json.dumps(data["slot_day_states"])
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_patch_slot_settings", "failed", request, branch_id=branch_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(s)
    audit_log("admin", admin_id, "update_slot_settings", request, branch_id=branch_id)
    action_log("admin_patch_slot_settings", "success", request, branch_id=branch_id, latency_ms=round(monotonic_ms() - started, 2))
    return slot_settings_to_dict(s)


# --- Bookings (admin view / ops) ---


@router.get("/branches/{branch_id}/bookings")
def admin_list_bookings(branch_id: str, db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    _branch_or_404(db, branch_id)
    rows = (
        db.query(BranchBooking)
        .filter(BranchBooking.branch_id == branch_id)
        .order_by(BranchBooking.slot_date.desc(), BranchBooking.start_time)
        .all()
    )
    return [booking_to_dict(x) for x in rows]


@router.patch("/branches/{branch_id}/bookings/{booking_id}")
def admin_patch_booking(
    branch_id: str, booking_id: str, body: BookingUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    b = _branch_or_404(db, branch_id)
    job = (
        db.query(BranchBooking)
        .filter(BranchBooking.id == booking_id, BranchBooking.branch_id == branch_id)
        .one_or_none()
    )
    if not job:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    prev_status = job.status
    try:
        booking_service.patch_branch_booking_fields(db, b, job, data)
        loyalty_service.on_branch_booking_status_change(db, job, prev_status)
        db.commit()
        audit_log("admin", admin_id, "update_booking", request, branch_id=branch_id, booking_id=booking_id)
        action_log(
            "admin_patch_booking",
            "success",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            latency_ms=round(monotonic_ms() - started, 2),
        )
    except AppError as e:
        db.rollback()
        action_log(
            "admin_patch_booking",
            "failed",
            request,
            branch_id=branch_id,
            booking_id=booking_id,
            error_code=e.code,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=e.status_code, detail={"detail": e.message, "code": e.code})
    db.refresh(job)
    return booking_to_dict(job)


@router.post("/branches/{branch_id}/bookings")
def admin_create_booking(
    branch_id: str, body: BookingCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
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
            selected_addon_ids=body.selected_addon_ids,
            slot_date=body.slot_date,
            start_time=body.start_time,
            end_time=body.end_time,
            source=body.source,
            tip_cents=body.tip_cents,
            notes=body.notes,
            bay_number=body.bay_number,
            assigned_washer_id=body.assigned_washer_id,
            booking_id=body.booking_id,
        )
        db.commit()
        audit_log("admin", admin_id, "create_booking", request, branch_id=branch_id, booking_id=job.id)
        action_log(
            "admin_create_booking",
            "success",
            request,
            branch_id=branch_id,
            booking_id=job.id,
            latency_ms=round(monotonic_ms() - started, 2),
        )
    except AppError as e:
        db.rollback()
        action_log(
            "admin_create_booking",
            "failed",
            request,
            branch_id=branch_id,
            error_code=e.code,
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(status_code=e.status_code, detail={"detail": e.message, "code": e.code})
    db.refresh(job)
    return booking_to_dict(job)

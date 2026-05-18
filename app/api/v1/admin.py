"""Admin API — branches, staff, catalog, promos, slots, loyalty."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response, Query
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
    MobileBooking,
    MobileCatalogServiceItem,
    MobileVehicleCatalogBlock,
    MobileGlobalAddonItem,
    MobileServiceDriver,
    MobileCatalogAddonItem,
)
from app.models.customer import CustomerUser
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
from app.services.jsonutil import dumps_json, loads_json_array

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


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_login_id(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_phone(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _raise_staff_conflict(field: str, label: str) -> None:
    raise HTTPException(
        status_code=409,
        detail={"detail": f"{label} already used", "code": "conflict", "field": field},
    )


def _ensure_unique_staff_identity(
    db: Session,
    *,
    email: str,
    phone: str,
    login_id: str,
    exclude_manager_id: str | None = None,
    exclude_washer_id: str | None = None,
) -> None:
    managers = db.query(BranchManager).all()
    washers = db.query(Washer).all()

    target_email = _normalize_email(email)
    target_phone = _normalize_phone(phone)
    target_login = _normalize_login_id(login_id)

    if target_email:
        for m in managers:
            if exclude_manager_id and m.id == exclude_manager_id:
                continue
            if _normalize_email(m.email) == target_email:
                _raise_staff_conflict("email", "Email")
        for w in washers:
            if exclude_washer_id and w.id == exclude_washer_id:
                continue
            if _normalize_email(w.email) == target_email:
                _raise_staff_conflict("email", "Email")

    if target_phone:
        for m in managers:
            if exclude_manager_id and m.id == exclude_manager_id:
                continue
            if _normalize_phone(m.phone) == target_phone:
                _raise_staff_conflict("phone", "Phone")
        for w in washers:
            if exclude_washer_id and w.id == exclude_washer_id:
                continue
            if _normalize_phone(w.phone) == target_phone:
                _raise_staff_conflict("phone", "Phone")

    if target_login:
        for m in managers:
            if exclude_manager_id and m.id == exclude_manager_id:
                continue
            if _normalize_login_id(m.login_id) == target_login:
                _raise_staff_conflict("login_id", "Login ID")
        for w in washers:
            if exclude_washer_id and w.id == exclude_washer_id:
                continue
            if _normalize_login_id(w.login_id) == target_login:
                _raise_staff_conflict("login_id", "Login ID")


def _ensure_unique_branch_addon_name(
    db: Session, branch_id: str, name: str, exclude_id: str | None = None
) -> str:
    normalized_name = (name or "").strip()
    if not normalized_name:
        raise HTTPException(status_code=400, detail={"detail": "Add-on name is required", "code": "validation_error"})
    q = db.query(BranchAddonItem).filter(
        BranchAddonItem.branch_id == branch_id,
        func.lower(BranchAddonItem.name) == normalized_name.lower(),
    )
    if exclude_id:
        q = q.filter(BranchAddonItem.id != exclude_id)
    if q.first():
        raise HTTPException(
            status_code=409,
            detail={"detail": "Add-on name already used in this branch", "code": "conflict", "field": "name"},
        )
    return normalized_name


def _ensure_unique_branch_promotion_code_name(
    db: Session, branch_id: str, code_name: str, exclude_id: str | None = None
) -> str:
    normalized = (code_name or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail={"detail": "Promo code name is required", "code": "validation_error"})
    q = db.query(Promotion).filter(
        Promotion.branch_id == branch_id,
        func.lower(Promotion.code_name) == normalized.lower(),
    )
    if exclude_id:
        q = q.filter(Promotion.id != exclude_id)
    if q.first():
        raise HTTPException(
            status_code=409,
            detail={"detail": "Promo code name already used in this branch", "code": "conflict", "field": "code_name"},
        )
    return normalized


def _ensure_unique_branch_day_rule_title(
    db: Session, branch_id: str, title: str, exclude_id: str | None = None
) -> str:
    normalized = (title or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail={"detail": "Rule title is required", "code": "validation_error"})
    q = db.query(DayTimePriceRule).filter(
        DayTimePriceRule.branch_id == branch_id,
        func.lower(DayTimePriceRule.title) == normalized.lower(),
    )
    if exclude_id:
        q = q.filter(DayTimePriceRule.id != exclude_id)
    if q.first():
        raise HTTPException(
            status_code=409,
            detail={"detail": "Day/time pricing title already used in this branch", "code": "conflict", "field": "title"},
        )
    return normalized


def _ensure_unique_branch_service_names(
    db: Session, branch_id: str, services: list[Any], exclude_vehicle_block_id: str | None = None
) -> None:
    # For replace flows, allow legacy duplicate names that already existed in the same
    # block before strict uniqueness checks were introduced.
    legacy_names_in_same_block: set[str] = set()
    if exclude_vehicle_block_id:
        local_rows = (
            db.query(CatalogServiceItem)
            .filter(CatalogServiceItem.vehicle_block_id == exclude_vehicle_block_id)
            .all()
        )
        legacy_names_in_same_block = {
            str(row.name or "").strip().lower()
            for row in local_rows
            if str(row.name or "").strip()
        }

    incoming_names: set[str] = set()
    incoming_group_by_name: dict[str, str] = {}
    for svc in services:
        normalized = str(getattr(svc, "name", "") or "").strip().lower()
        if not normalized:
            continue
        if normalized in incoming_names:
            if exclude_vehicle_block_id and normalized in legacy_names_in_same_block:
                continue
            raise HTTPException(
                status_code=409,
                detail={"detail": "Service name already used in this branch", "code": "conflict", "field": "name"},
            )
        incoming_names.add(normalized)
        incoming_group_by_name[normalized] = str(getattr(svc, "catalog_group_id", "") or "")

    existing_blocks = db.query(VehicleCatalogBlock.id).filter(VehicleCatalogBlock.branch_id == branch_id).all()
    block_ids = [bid for (bid,) in existing_blocks if bid]
    if not block_ids:
        return

    existing_services = db.query(CatalogServiceItem).filter(CatalogServiceItem.vehicle_block_id.in_(block_ids)).all()
    for row in existing_services:
        if exclude_vehicle_block_id and row.vehicle_block_id == exclude_vehicle_block_id:
            continue
        normalized = str(row.name or "").strip().lower()
        if normalized not in incoming_names:
            continue
        if normalized in legacy_names_in_same_block:
            continue
        incoming_group_id = incoming_group_by_name.get(normalized, "")
        existing_group_id = str(getattr(row, "catalog_group_id", "") or "")
        if incoming_group_id and existing_group_id and incoming_group_id == existing_group_id:
            continue
        raise HTTPException(
            status_code=409,
            detail={"detail": "Service name already used in this branch", "code": "conflict", "field": "name"},
        )


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
            detail={"detail": "There are active bookings for this branch. It cannot be deleted.", "code": "active_bookings"},
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
    normalized_login_id = body.login_id.strip().lower()
    _ensure_unique_staff_identity(
        db,
        email=normalized_email,
        phone=body.phone,
        login_id=normalized_login_id,
    )
    m = BranchManager(
        branch_id=branch_id,
        name=body.name,
        address=body.address,
        zip_code=body.zip_code,
        email=normalized_email,
        phone=body.phone,
        doj=body.doj,
        login_id=normalized_login_id,
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
    next_phone = str(data.get("phone", m.phone) or "")
    next_login_id = str(data.get("login_id", m.login_id) or "").strip().lower()
    _ensure_unique_staff_identity(
        db,
        email=next_email,
        phone=next_phone,
        login_id=next_login_id,
        exclude_manager_id=manager_id,
    )
    if "email" in data:
        data["email"] = next_email
    if "login_id" in data:
        data["login_id"] = next_login_id
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
    normalized_email = body.email.strip().lower()
    normalized_login_id = body.login_id.strip().lower()
    _ensure_unique_staff_identity(
        db,
        email=normalized_email,
        phone=body.phone,
        login_id=normalized_login_id,
    )
    w = Washer(
        branch_id=branch_id,
        name=body.name,
        address=body.address,
        zip_code=body.zip_code,
        email=normalized_email,
        phone=body.phone,
        doj=body.doj,
        login_id=normalized_login_id,
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
    next_email = str(data.get("email", w.email) or "").strip().lower()
    next_phone = str(data.get("phone", w.phone) or "")
    next_login_id = str(data.get("login_id", w.login_id) or "").strip().lower()
    _ensure_unique_staff_identity(
        db,
        email=next_email,
        phone=next_phone,
        login_id=next_login_id,
        exclude_washer_id=washer_id,
    )
    if "email" in data:
        data["email"] = next_email
    if "login_id" in data:
        data["login_id"] = next_login_id
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
    _ensure_unique_branch_service_names(db, branch_id, body.services)
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
                excluded_points=dumps_json(s.excluded_points),
                active=s.active,
                catalog_group_id=s.catalog_group_id,
                duration_minutes=s.duration_minutes,
                category=s.category or "Washing",
                sequence=int(s.sequence or 999),
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
    _ensure_unique_branch_service_names(db, branch_id, body.services, exclude_vehicle_block_id=block.id)
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
                excluded_points=dumps_json(s.excluded_points),
                active=s.active,
                catalog_group_id=s.catalog_group_id,
                duration_minutes=s.duration_minutes,
                category=s.category or "Washing",
                sequence=int(s.sequence or 999),
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
    normalized_name = _ensure_unique_branch_addon_name(db, branch_id, body.name)
    row = BranchAddonItem(
        id=body.id or new_id(),
        branch_id=branch_id,
        name=normalized_name,
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
    row.name = _ensure_unique_branch_addon_name(db, branch_id, body.name, exclude_id=addon_id)
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
    normalized_code_name = _ensure_unique_branch_promotion_code_name(db, branch_id, body.code_name)
    pid = body.id or new_id()
    p = Promotion(
        id=pid,
        branch_id=branch_id,
        code_name=normalized_code_name,
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
    p.code_name = _ensure_unique_branch_promotion_code_name(db, branch_id, body.code_name, exclude_id=promo_id)
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
    normalized_title = _ensure_unique_branch_day_rule_title(db, branch_id, body.title)
    _ensure_no_day_rule_overlap(db, branch_id, body)
    rid = body.id or new_id()
    r = DayTimePriceRule(
        id=rid,
        branch_id=branch_id,
        title=normalized_title,
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
    r.title = _ensure_unique_branch_day_rule_title(db, branch_id, body.title, exclude_id=rule_id)
    _ensure_no_day_rule_overlap(db, branch_id, body, exclude_id=rule_id)
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
    data["_actor_id"] = admin_id
    data["_actor_role"] = "admin"
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
            customer_email=(body.customer_email or "").strip(),
            address=body.address,
            vehicle_type=body.vehicle_type,
            vehicle_model=body.vehicle_model or "",
            registration_number=body.registration_number or "",
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
            customer_id=body.customer_id,
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


# --- Revenue Summary ---


@router.get("/revenue-summary")
def get_revenue_summary(
    db: DbSession,
    _admin: AdminUser,
    request: Request,
    branch_id: str | None = Query(None, description="Filter by branch ID"),
    mobile: bool = Query(False, description="Include mobile services instead of branch"),
    service_type: str | None = Query(None, description="Filter by service type (service name)"),
    vehicle_type: str | None = Query(None, description="Filter by vehicle type"),
    period: str = Query("month", description="Time period: week, month, quarter, year, or custom"),
    start_date: str | None = Query(None, description="Start date for custom period (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="End date for custom period (YYYY-MM-DD)"),
) -> dict[str, Any]:
    from datetime import datetime, timedelta
    from sqlalchemy import func, or_

    started = monotonic_ms()

    # Determine date range
    now = datetime.now()
    if period == "week":
        start = now - timedelta(days=7)
    elif period == "month":
        start = now - timedelta(days=30)
    elif period == "quarter":
        start = now - timedelta(days=90)
    elif period == "year":
        start = now - timedelta(days=365)
    elif period == "custom" and start_date and end_date:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    else:
        start = now - timedelta(days=30)  # default to month

    if period != "custom":
        end = now
    elif not end_date:
        end = now

    total_revenue = 0
    booking_count = 0

    if not mobile:
        # Branch bookings
        q = db.query(BranchBooking).filter(
            BranchBooking.status == "completed",
            BranchBooking.completed_at.isnot(None),
            BranchBooking.completed_at >= start,
            BranchBooking.completed_at <= end,
        )
        if branch_id:
            q = q.filter(BranchBooking.branch_id == branch_id)
        if vehicle_type:
            q = q.filter(BranchBooking.vehicle_type.ilike(f"%{vehicle_type}%"))
        
        if service_type and service_type != "all":
            q = q.join(CatalogServiceItem, BranchBooking.service_id == CatalogServiceItem.id)
            q = q.filter(CatalogServiceItem.name == service_type)

        bookings = q.all()
        
        service_ids = {b.service_id for b in bookings if b.service_id}
        addon_ids_list = [loads_json_array(b.selected_addon_ids_json) for b in bookings]
        addon_ids = {aid for aids in addon_ids_list for aid in aids}
        
        service_prices = {}
        if service_ids:
            services = db.query(CatalogServiceItem).filter(CatalogServiceItem.id.in_(service_ids)).all()
            service_prices = {s.id: s.price for s in services}
            
        addon_prices = {}
        if addon_ids:
            addons = db.query(BranchAddonItem).filter(BranchAddonItem.id.in_(addon_ids)).all()
            addon_prices = {a.id: a.price for a in addons}

        for b, aids in zip(bookings, addon_ids_list):
            base_price = service_prices.get(b.service_id, 500)
            addon_total = sum(addon_prices.get(aid, 0) for aid in aids)
            discount = (getattr(b, "promo_discount_cents", 0) or 0) / 100
            tip = (getattr(b, "tip_cents", 0) or 0) / 100
            total_revenue += max(0, (base_price + addon_total) - discount) + tip
            booking_count += 1
    else:
        # Mobile bookings
        q = db.query(MobileBooking).filter(
            MobileBooking.status == "completed",
            MobileBooking.completed_at.isnot(None),
            MobileBooking.completed_at >= start,
            MobileBooking.completed_at <= end,
        )
        if vehicle_type:
            q = q.filter(MobileBooking.vehicle_type.ilike(f"%{vehicle_type}%"))

        if service_type and service_type != "all":
            q = q.join(MobileCatalogServiceItem, MobileBooking.service_id == MobileCatalogServiceItem.id)
            q = q.filter(MobileCatalogServiceItem.name == service_type)

        bookings = q.all()
        
        service_ids = {b.service_id for b in bookings if b.service_id}
        addon_ids_list = [loads_json_array(b.selected_addon_ids_json) for b in bookings]
        addon_ids = {aid for aids in addon_ids_list for aid in aids}
        
        service_prices = {}
        if service_ids:
            services = db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id.in_(service_ids)).all()
            service_prices = {s.id: s.price for s in services}
            
        addon_prices = {}
        if addon_ids:
            addons = db.query(MobileGlobalAddonItem).filter(MobileGlobalAddonItem.id.in_(addon_ids)).all()
            addon_prices = {a.id: a.price for a in addons}

        for b, aids in zip(bookings, addon_ids_list):
            base_price = service_prices.get(b.service_id, 500)
            addon_total = sum(addon_prices.get(aid, 0) for aid in aids)
            discount = (getattr(b, "promo_discount_cents", 0) or 0) / 100
            tip = (getattr(b, "tip_cents", 0) or 0) / 100
            total_revenue += max(0, (base_price + addon_total) - discount) + tip
            booking_count += 1

    action_log(
        "admin_get_revenue_summary",
        "success",
        request,
        branch_id=branch_id or "",
        mobile=mobile,
        period=period,
        total_revenue=total_revenue,
        booking_count=booking_count,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return {
        "total_revenue": round(total_revenue, 2),
        "booking_count": booking_count,
        "period": period,
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "filters": {
            "branch_id": branch_id,
            "mobile": mobile,
            "service_type": service_type,
            "vehicle_type": vehicle_type,
        },
    }


def _calculate_reports(
    db: DbSession,
    branch_id: str | None,
    mobile: bool,
    period: str,
    service_type: str | None,
    vehicle_type: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    try:
        now = datetime.now(timezone.utc)

        # Date range - Use aware datetimes for TIMESTAMPTZ comparison
        if period == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == "week":
            start = now - timedelta(days=7)
        elif period == "month":
            start = now - timedelta(days=30)
        elif period == "custom" and start_date and end_date:
            start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        else:
            start = now - timedelta(days=30)
        
        if period != "custom":
            end = now
        elif not end_date:
            end = now
        else:
            end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

        # Convert to YYYY-MM-DD for slot_date comparison (matching Service History)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        # Fetch data - Include all active bookings (not just completed)
        if not mobile:
            q = db.query(BranchBooking).filter(
                BranchBooking.status != "cancelled",
                BranchBooking.slot_date >= start_str,
                BranchBooking.slot_date <= end_str,
            )
            if branch_id and branch_id != "all":
                q = q.filter(BranchBooking.branch_id == branch_id)
            if vehicle_type and vehicle_type != "all":
                q = q.filter(BranchBooking.vehicle_type.ilike(f"%{vehicle_type}%"))
            
            if service_type and service_type != "all":
                # For branch, service name is usually in service_summary, but more reliably in CatalogServiceItem
                q = q.join(CatalogServiceItem, BranchBooking.service_id == CatalogServiceItem.id)
                q = q.filter(CatalogServiceItem.name == service_type)
            
            bookings = q.all()

            service_ids = {b.service_id for b in bookings if b.service_id}
            addon_ids_list = [loads_json_array(b.selected_addon_ids_json) for b in bookings]
            addon_ids = {aid for aids in addon_ids_list for aid in aids}
            service_prices = {s.id: s.price for s in db.query(CatalogServiceItem).filter(CatalogServiceItem.id.in_(service_ids)).all()} if service_ids else {}
            addon_prices = {a.id: a.price for a in db.query(BranchAddonItem).filter(BranchAddonItem.id.in_(addon_ids)).all()} if addon_ids else {}
            worker_names = {w.id: w.name for w in db.query(Washer).all()}
        else:
            q = db.query(MobileBooking).filter(
                MobileBooking.status != "cancelled",
                MobileBooking.slot_date >= start_str,
                MobileBooking.slot_date <= end_str,
            )
            if vehicle_type and vehicle_type != "all":
                q = q.filter(MobileBooking.vehicle_type.ilike(f"%{vehicle_type}%"))

            if service_type and service_type != "all":
                q = q.join(MobileCatalogServiceItem, MobileBooking.service_id == MobileCatalogServiceItem.id)
                q = q.filter(MobileCatalogServiceItem.name == service_type)

            bookings = q.all()

            service_ids = {b.service_id for b in bookings if b.service_id}
            addon_ids_list = [loads_json_array(b.selected_addon_ids_json) for b in bookings]
            addon_ids = {aid for aids in addon_ids_list for aid in aids}
            service_prices = {s.id: s.price for s in db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id.in_(service_ids)).all()} if service_ids else {}
            addon_prices = {a.id: a.price for a in db.query(MobileGlobalAddonItem).filter(MobileGlobalAddonItem.id.in_(addon_ids)).all()} if addon_ids else {}
            worker_names = {d.id: d.emp_name for d in db.query(MobileServiceDriver).all()}

        # Metrics initialization
        total_revenue = 0
        total_bookings = len(bookings)
        hour_distribution = {}
        promo_usage = {}
        payment_methods = {"cash": 0, "upi": 0, "card": 0}
        
        # Initialize worker_performance only for workers in the current branch/context
        worker_performance = {name: 0 for name in worker_names.values()}
        source_breakdown = {"online": 0, "walk_in": 0}

        for b, aids in zip(bookings, addon_ids_list):
            # 1. Operational Metrics (Calculated for ALL active bookings)
            # Hour distribution
            hour_key = f"{b.start_time[:2]}:00"
            hour_distribution[hour_key] = hour_distribution.get(hour_key, 0) + 1
            
            # Acquisition Source
            src = (getattr(b, "source", "online") or "online").lower()
            source_breakdown[src] = source_breakdown.get(src, 0) + 1

            # Promo Usage (tracking all applied codes)
            pcode = getattr(b, "promo_code", None)
            discount_val = (getattr(b, "promo_discount_cents", 0) or 0) / 100
            if pcode:
                stats = promo_usage.get(pcode, {"count": 0, "discount": 0})
                stats["count"] += 1
                stats["discount"] += discount_val
                promo_usage[pcode] = stats
                
            # 2. Financial & Performance Metrics (Strictly for COMPLETED bookings only)
            if b.status == "completed":
                # Revenue calculation: (Base + Addons - Discount) + Tip
                base_price = service_prices.get(b.service_id, 500)
                addon_total = sum(addon_prices.get(aid, 0) for aid in aids)
                tip = (getattr(b, "tip_cents", 0) or 0) / 100
                
                net_booking_revenue = max(0, (base_price + addon_total) - discount_val) + tip
                total_revenue += net_booking_revenue

                # Payment Method Breakdown
                pm = (getattr(b, "payment_method", "cash") or "cash").lower()
                payment_methods[pm] = payment_methods.get(pm, 0) + 1
                
                # Staff Performance
                washer_id = getattr(b, "assigned_washer_id", None) or getattr(b, "assigned_driver_id", None)
                if washer_id and washer_id in worker_names:
                    name = worker_names[washer_id]
                    worker_performance[name] = worker_performance.get(name, 0) + 1
                else:
                    worker_performance["Unassigned"] = worker_performance.get("Unassigned", 0) + 1

        all_hours = [f"{h:02d}:00" for h in range(8, 22)]
        busy_hours = sorted(hour_distribution.items(), key=lambda x: x[1], reverse=True)
        peak = [h for h, c in busy_hours[:3]] if busy_hours else []
        idle = [h for h in all_hours if h not in hour_distribution]

        return {
            "revenue": {"total": round(total_revenue, 2), "currency": "INR"},
            "bookings": {"total": total_bookings},
            "utilization": {"peak_hours": peak, "idle_hours": idle[:3]},
            "promo": {"usage": promo_usage},
            "payment": {"methods": payment_methods},
            "washer_performance": worker_performance,
            "source": source_breakdown,
            "period": {
                "start": start.date().isoformat(),
                "end": end.date().isoformat(),
            }
        }
    except Exception as e:
        import logging
        logging.exception("Error in reporting logic")
        return {
            "error": True,
            "message": str(e),
            "revenue": {"total": 0, "currency": "INR"},
            "bookings": {"total": 0},
            "utilization": {"peak_hours": [], "idle_hours": []},
            "promo": {"usage": {}},
            "payment": {"methods": {"cash": 0, "upi": 0, "card": 0}},
            "washer_performance": {},
            "source": {},
            "period": {"start": "", "end": ""}
        }


@router.get("/reports/summary")
def get_reports_summary(
    db: DbSession,
    _admin: AdminUser,
    request: Request,
    branch_id: str | None = Query(None),
    mobile: bool = Query(False),
    period: str = Query("month"),
    service_type: str | None = Query(None),
    vehicle_type: str | None = Query(None),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
) -> dict[str, Any]:
    started = monotonic_ms()
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, start_date, end_date)
    action_log("admin_get_reports_summary", "success", request, latency_ms=round(monotonic_ms() - started, 2))
    return data


@router.get("/reports/revenue")
def get_report_revenue(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/bookings")
def get_report_bookings(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/slots")
def get_report_slots(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/promos")
def get_report_promos(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/payments")
def get_report_payments(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/washers")
def get_report_washers(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/sources")
def get_report_sources(db: DbSession, _admin: AdminUser, branch_id: str | None = Query(None), mobile: bool = Query(False), period: str = Query("month"), service_type: str | None = Query(None), vehicle_type: str | None = Query(None)) -> dict[str, Any]:
    data = _calculate_reports(db, branch_id, mobile, period, service_type, vehicle_type, None, None)
    return data


@router.get("/reports/metadata")
def get_reports_metadata(db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    try:
        # Branches
        branches = db.query(Branch.id, Branch.name).order_by(Branch.name).all()
        branch_list = [{"id": b.id, "name": b.name} for b in branches]

        # Service Types (Distinct names from both catalogs)
        b_services = db.query(CatalogServiceItem.name).distinct().all()
        m_services = db.query(MobileCatalogServiceItem.name).distinct().all()
        services = sorted(list(set([s.name for s in b_services if s.name] + [s.name for s in m_services if s.name])))

        # Vehicle Types (Distinct types from both catalogs)
        b_vehicles = db.query(VehicleCatalogBlock.vehicle_type).distinct().all()
        m_vehicles = db.query(MobileVehicleCatalogBlock.vehicle_type).distinct().all()
        vehicles = sorted(list(set([v.vehicle_type for v in b_vehicles if v.vehicle_type] + [v.vehicle_type for v in m_vehicles if v.vehicle_type])))

        return {
            "branches": branch_list,
            "services": services,
            "vehicles": vehicles
        }
    except Exception as e:
        import logging
        logging.exception("Error fetching reports metadata")
        return {
            "branches": [],
            "services": [],
            "vehicles": [],
            "error": str(e)
        }

@router.get("/test-debug")
def test_debug(db: DbSession, period: str = "month"):
    try:
        data_branch = _calculate_reports(db, None, False, period, None, None, None, None)
        data_mobile = _calculate_reports(db, None, True, period, None, None, None, None)
        
        branch_count = db.query(BranchBooking).count()
        mobile_count = db.query(MobileBooking).count()
        branch_completed = db.query(BranchBooking).filter(BranchBooking.status == "completed").count()
        
        return {
            "branch_raw_count": branch_count,
            "mobile_raw_count": mobile_count,
            "branch_completed": branch_completed,
            "branch_reports": data_branch,
            "mobile_reports": data_mobile
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


# ──────────────────────────────────────────────
# Customer Management
# ──────────────────────────────────────────────

@router.get("/customers")
def list_customers(
    db: DbSession,
    _admin: AdminUser,
    search: str = Query(default=""),
    type_filter: str = Query(default="all"),
    sort_by: str = Query(default="recent_booking"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    from collections import defaultdict

    customers: list[dict[str, Any]] = []

    # ── Registered accounts ──
    if type_filter in ("all", "account"):
        users = db.query(CustomerUser).order_by(CustomerUser.full_name).all()
        for u in users:
            branch_count = db.query(BranchBooking).filter(BranchBooking.customer_id == u.id).count()
            mobile_count = db.query(MobileBooking).filter(MobileBooking.customer_id == u.id).count()

            last_b = db.query(BranchBooking.slot_date).filter(BranchBooking.customer_id == u.id).order_by(BranchBooking.slot_date.desc()).first()
            last_m = db.query(MobileBooking.slot_date).filter(MobileBooking.customer_id == u.id).order_by(MobileBooking.slot_date.desc()).first()
            last_dates = [d[0] for d in [last_b, last_m] if d and d[0]]
            last_booking_date = max(last_dates) if last_dates else None

            try:
                vraw = json.loads(u.vehicles_json or "[]")
                vehicles = [str(v.get("type") or v.get("vehicleType") or v.get("vehicle_type") or "") for v in vraw if isinstance(v, dict)]
                vehicles = [v for v in vehicles if v]
            except Exception:
                vehicles = []

            if not vehicles:
                bv = db.query(BranchBooking.vehicle_type).filter(BranchBooking.customer_id == u.id).distinct().all()
                mv = db.query(MobileBooking.vehicle_type).filter(MobileBooking.customer_id == u.id).distinct().all()
                vehicles = list({r[0] for r in bv + mv if r[0]})

            customers.append({
                "type": "account",
                "customer_id": u.id,
                "guest_key": None,
                "name": u.full_name or "",
                "email": u.email,
                "phone": u.phone or "",
                "vehicles": sorted(vehicles),
                "branch_booking_count": branch_count,
                "mobile_booking_count": mobile_count,
                "total_booking_count": branch_count + mobile_count,
                "last_booking_date": last_booking_date,
                "created_at": str(u.created_at) if getattr(u, "created_at", None) else None,
            })

    # ── Guest bookings ──
    if type_filter in ("all", "guest"):
        branch_guests = (
            db.query(
                BranchBooking.customer_name,
                BranchBooking.phone,
                BranchBooking.customer_email,
                BranchBooking.vehicle_type,
                BranchBooking.slot_date,
            )
            .filter(BranchBooking.customer_id == None)
            .all()
        )
        mobile_guests = (
            db.query(
                MobileBooking.customer_name,
                MobileBooking.phone,
                MobileBooking.customer_email,
                MobileBooking.vehicle_type,
                MobileBooking.slot_date,
            )
            .filter(MobileBooking.customer_id == None)
            .all()
        )

        groups: dict[str, dict] = defaultdict(lambda: {
            "names": set(),
            "phones": set(),
            "vehicles": set(),
            "emails": set(),
            "branch_count": 0,
            "mobile_count": 0,
            "dates": [],
        })

        for name, phone, email, vtype, sdate in branch_guests:
            key = (phone or "").strip() or (name or "").strip()
            if not key:
                continue
            g = groups[key]
            g["names"].add((name or "").strip())
            g["phones"].add((phone or "").strip())
            em = (email or "").strip()
            if em:
                g["emails"].add(em)
            if vtype:
                g["vehicles"].add(vtype.strip())
            g["branch_count"] += 1
            if sdate:
                g["dates"].append(sdate)

        for name, phone, email, vtype, sdate in mobile_guests:
            key = (phone or "").strip() or (name or "").strip()
            if not key:
                continue
            g = groups[key]
            g["names"].add((name or "").strip())
            g["phones"].add((phone or "").strip())
            em = (email or "").strip()
            if em:
                g["emails"].add(em)
            if vtype:
                g["vehicles"].add(vtype.strip())
            g["mobile_count"] += 1
            if sdate:
                g["dates"].append(sdate)

        for key, g in groups.items():
            best_name = next(iter(sorted(g["names"] - {""})), next(iter(g["names"]), "Unknown"))
            best_phone = next(iter(sorted(g["phones"] - {""})), "")
            sorted_emails = sorted(x for x in g["emails"] if x)
            guests_email = sorted_emails[0] if sorted_emails else None
            customers.append({
                "type": "guest",
                "customer_id": None,
                "guest_key": key,
                "name": best_name,
                "email": guests_email,
                "phone": best_phone,
                "vehicles": sorted(g["vehicles"]),
                "branch_booking_count": g["branch_count"],
                "mobile_booking_count": g["mobile_count"],
                "total_booking_count": g["branch_count"] + g["mobile_count"],
                "last_booking_date": max(g["dates"]) if g["dates"] else None,
                "created_at": None,
            })

    # ── Search filter ──
    if search:
        s = search.lower().strip()
        customers = [
            c for c in customers
            if s in (c["name"] or "").lower()
            or s in (c["email"] or "").lower()
            or s in (c["phone"] or "").lower()
            or s in (c.get("customer_id") or "").lower()
        ]

    # ── Sort ──
    # For multi-key sorts that mix ascending/descending directions we use Python's
    # stable-sort guarantee: sort by the secondary key first (name A→Z), then by
    # the primary key.  Equal primary keys keep the secondary order intact.
    if sort_by == "name_asc":
        customers.sort(key=lambda c: (c["name"] or "").lower())
    elif sort_by == "name_desc":
        customers.sort(key=lambda c: (c["name"] or "").lower(), reverse=True)
    elif sort_by == "oldest_booking":
        # Oldest last-booking date first (ascending); no-booking customers at the end.
        customers.sort(key=lambda c: (c["name"] or "").lower())  # secondary: name A→Z
        customers.sort(key=lambda c: c["last_booking_date"] or "9999-99-99")
    elif sort_by == "most_bookings":
        # Negating count keeps the primary descending while name stays ascending.
        customers.sort(key=lambda c: (-c["total_booking_count"], (c["name"] or "").lower()))
    elif sort_by == "latest_created":
        # Newest first; accounts use created_at, guests fall back to last_booking_date.
        # Slice to 10 chars so ISO datetime strings ("2024-01-15T…") compare correctly
        # with plain date strings ("2024-01-15").
        customers.sort(key=lambda c: (c["name"] or "").lower())  # secondary: name A→Z
        customers.sort(
            key=lambda c: (c["created_at"] or c["last_booking_date"] or "")[:10],
            reverse=True,
        )
    else:  # recent_booking (default)
        # Most-recent last-booking date first; ties broken by name A→Z.
        customers.sort(key=lambda c: (c["name"] or "").lower())  # secondary: name A→Z
        customers.sort(key=lambda c: c["last_booking_date"] or "0000-00-00", reverse=True)

    total = len(customers)
    start = (page - 1) * per_page
    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "customers": customers[start: start + per_page],
    }


@router.get("/customers/bookings")
def get_customer_bookings(
    db: DbSession,
    _admin: AdminUser,
    customer_id: str = Query(default=""),
    phone: str = Query(default=""),
    name: str = Query(default=""),
) -> list[dict[str, Any]]:
    if not customer_id and not phone and not name:
        return []

    bookings: list[dict[str, Any]] = []

    # ── Branch bookings ──
    bq = db.query(BranchBooking)
    if customer_id:
        bq = bq.filter(BranchBooking.customer_id == customer_id)
    elif phone:
        bq = bq.filter(BranchBooking.phone == phone, BranchBooking.customer_id == None)
    else:
        bq = bq.filter(BranchBooking.customer_name == name, BranchBooking.customer_id == None)
    branch_rows = bq.order_by(BranchBooking.slot_date.desc(), BranchBooking.start_time).all()

    if branch_rows:
        all_aid: set[str] = set()
        for b in branch_rows:
            all_aid.update(loads_json_array(b.selected_addon_ids_json or "[]"))
        cat_map = {r.id: r.name for r in db.query(CatalogAddonItem).filter(CatalogAddonItem.id.in_(all_aid)).all()} if all_aid else {}
        br_map = {r.id: r.name for r in db.query(BranchAddonItem).filter(BranchAddonItem.id.in_(all_aid)).all()} if all_aid else {}
        merged_addons = {**cat_map, **br_map}
        washer_map = {r.id: r.name for r in db.query(Washer).all()}

        for b in branch_rows:
            aids = loads_json_array(b.selected_addon_ids_json or "[]")
            addon_names = [merged_addons[a] for a in aids if a in merged_addons]
            try:
                from app.services.booking_pricing import branch_booking_customer_service_total_cents
                service_total_cents = branch_booking_customer_service_total_cents(db, b)
            except Exception:
                service_total_cents = 0
            bookings.append({
                "source": "branch",
                "id": b.id,
                "slot_date": b.slot_date,
                "start_time": b.start_time,
                "end_time": b.end_time,
                "service_summary": b.service_summary or "",
                "vehicle_type": b.vehicle_type or "",
                "vehicle_model": getattr(b, "vehicle_model", "") or "",
                "registration_number": getattr(b, "registration_number", "") or "",
                "addon_names": addon_names,
                "assigned_staff_name": washer_map.get(b.assigned_washer_id or "", "") if b.assigned_washer_id else "",
                "service_total_cents": service_total_cents,
                "tip_cents": int(b.tip_cents or 0),
                "status": b.status,
                "payment_method": b.payment_method or "cash",
            })

    # ── Mobile bookings ──
    mq = db.query(MobileBooking)
    if customer_id:
        mq = mq.filter(MobileBooking.customer_id == customer_id)
    elif phone:
        mq = mq.filter(MobileBooking.phone == phone, MobileBooking.customer_id == None)
    else:
        mq = mq.filter(MobileBooking.customer_name == name, MobileBooking.customer_id == None)
    mobile_rows = mq.order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time).all()

    if mobile_rows:
        all_mid: set[str] = set()
        for b in mobile_rows:
            all_mid.update(loads_json_array(b.selected_addon_ids_json or "[]"))
        mcat_map = {r.id: r.name for r in db.query(MobileCatalogAddonItem).filter(MobileCatalogAddonItem.id.in_(all_mid)).all()} if all_mid else {}
        mglo_map = {r.id: r.name for r in db.query(MobileGlobalAddonItem).filter(MobileGlobalAddonItem.id.in_(all_mid)).all()} if all_mid else {}
        merged_m = {**mcat_map, **mglo_map}
        driver_map = {r.id: r.emp_name for r in db.query(MobileServiceDriver).all()}

        for b in mobile_rows:
            aids = loads_json_array(b.selected_addon_ids_json or "[]")
            addon_names = [merged_m[a] for a in aids if a in merged_m]
            try:
                from app.services.booking_pricing import mobile_booking_customer_service_total_cents
                service_total_cents = mobile_booking_customer_service_total_cents(db, b)
            except Exception:
                service_total_cents = 0
            bookings.append({
                "source": "mobile",
                "id": b.id,
                "slot_date": b.slot_date,
                "start_time": b.start_time,
                "end_time": b.end_time,
                "service_summary": b.vehicle_summary or "",
                "vehicle_type": b.vehicle_type or "",
                "vehicle_model": getattr(b, "vehicle_model", "") or "",
                "registration_number": getattr(b, "registration_number", "") or "",
                "addon_names": addon_names,
                "assigned_staff_name": driver_map.get(b.assigned_driver_id or "", "") if b.assigned_driver_id else "",
                "service_total_cents": service_total_cents,
                "tip_cents": int(b.tip_cents or 0),
                "status": b.status,
                "payment_method": b.payment_method or "cash",
            })

    bookings.sort(key=lambda x: (x["slot_date"] or "0000-00-00", x["start_time"] or ""), reverse=True)
    return bookings

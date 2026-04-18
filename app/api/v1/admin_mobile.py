"""Admin mobile operations API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.api.deps import AdminUser, DbSession
from app.core.observability import action_log, audit_log, monotonic_ms
from app.core.security import hash_password
from app.models import (
    MobileBooking,
    MobileCatalogAddonItem,
    MobileCatalogServiceItem,
    MobileDayTimePriceRule,
    MobileGlobalAddonItem,
    MobileLoyaltyProgram,
    MobilePromotion,
    MobileServiceDriver,
    MobileServiceManager,
    MobileSlotSettings,
    MobileVehicleCatalogBlock,
)
from app.core.mobile_pins import normalize_mobile_city_pin
from app.models.base import new_id
from app.schemas.mobile import (
    MobileBookingUpdate,
    MobileDayTimeRuleIn,
    MobileDriverCreate,
    MobileDriverUpdate,
    MobileGlobalAddonsReplace,
    MobileLoyaltyProgramIn,
    MobileManagerCreate,
    MobileManagerUpdate,
    MobilePromoIn,
    MobileSlotSettingsPatch,
    MobileVehicleBlockCreate,
)
from app.services import loyalty_service, mobile_slot_service
from app.services.mobile_addon_migration import ensure_mobile_global_addons_migrated
from app.services.jsonutil import dumps_json, loads_json_array, loads_json_object

router = APIRouter(prefix="/admin/mobile", tags=["admin-mobile"])


def _pin(pin: str) -> str:
    return "".join(ch for ch in str(pin) if ch.isdigit())


def _manager_to_dict(m: MobileServiceManager) -> dict[str, Any]:
    return {
        "id": m.id,
        "city_pin_code": m.city_pin_code,
        "emp_name": m.emp_name,
        "address": m.address,
        "zip_code": m.zip_code,
        "email": m.email,
        "mobile": m.mobile,
        "doj": m.doj,
        "login_id": m.login_id,
        "active": m.active,
    }


def _driver_to_dict(d: MobileServiceDriver) -> dict[str, Any]:
    return {
        "id": d.id,
        "manager_id": d.manager_id,
        "city_pin_code": d.city_pin_code,
        "service_pin_code": d.service_pin_code,
        "emp_name": d.emp_name,
        "address": d.address,
        "zip_code": d.zip_code,
        "serviceable_zip_codes": loads_json_array(d.serviceable_zip_codes_json),
        "email": d.email,
        "mobile": d.mobile,
        "doj": d.doj,
        "login_id": d.login_id,
        "active": d.active,
    }


def _service_to_dict(s: MobileCatalogServiceItem) -> dict[str, Any]:
    return {
        "id": s.id,
        "name": s.name,
        "price": s.price,
        "free_coffee_count": s.free_coffee_count,
        "eligible_for_loyalty_points": s.eligible_for_loyalty_points,
        "recommended": s.recommended,
        "description_points": loads_json_array(s.description_points),
        "active": s.active,
    }


def _global_addon_to_dict(a: MobileGlobalAddonItem) -> dict[str, Any]:
    return {
        "id": a.id,
        "name": a.name,
        "price": a.price,
        "description_points": loads_json_array(a.description_points),
        "active": a.active,
    }


def _vehicle_block_to_dict(b: MobileVehicleCatalogBlock) -> dict[str, Any]:
    return {
        "id": b.id,
        "vehicle_type": b.vehicle_type,
        "services": [_service_to_dict(s) for s in b.services],
        "addons": [],
    }


def _promotion_to_dict(p: MobilePromotion) -> dict[str, Any]:
    return {
        "id": p.id,
        "code_name": p.code_name,
        "discount_type": p.discount_type,
        "discount_value": p.discount_value,
        "validity_start": p.validity_start,
        "validity_end": p.validity_end,
        "max_uses_per_customer": p.max_uses_per_customer,
        "applicable_service_ids": loads_json_array(p.applicable_service_ids),
        "applicable_vehicle_types": loads_json_array(p.applicable_vehicle_types),
    }


def _day_rule_to_dict(r: MobileDayTimePriceRule) -> dict[str, Any]:
    return {
        "id": r.id,
        "title": r.title,
        "description": r.description,
        "discount_type": r.discount_type,
        "discount_value": r.discount_value,
        "applicable_service_ids": loads_json_array(r.applicable_service_ids),
        "applicable_vehicle_types": loads_json_array(r.applicable_vehicle_types),
        "applicable_days": loads_json_array(r.applicable_days),
        "time_window_start": r.time_window_start,
        "time_window_end": r.time_window_end,
        "validity_start": r.validity_start,
        "validity_end": r.validity_end,
    }


def _loyalty_to_dict(row: MobileLoyaltyProgram) -> dict[str, Any]:
    return {
        "qualifying_service_count": row.qualifying_service_count,
        "tiers": loads_json_array(row.tiers_json),
    }


def _slot_settings_to_dict(s: MobileSlotSettings) -> dict[str, Any]:
    return {
        "city_pin_code": s.city_pin_code,
        "slot_duration_minutes": s.slot_duration_minutes,
        "open_time": s.open_time,
        "close_time": s.close_time,
        "slot_window_active_by_key": loads_json_object(s.slot_window_active_by_key_json),
        "slot_driver_open_by_window": loads_json_object(s.slot_driver_open_by_window_json),
        "slot_day_states": loads_json_object(s.slot_day_states_json),
    }


def _booking_to_dict(b: MobileBooking) -> dict[str, Any]:
    return {
        "id": b.id,
        "manager_id": b.manager_id,
        "city_pin_code": b.city_pin_code,
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "selected_addon_ids": loads_json_array(b.selected_addon_ids_json),
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "status": b.status,
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "completed_at": b.completed_at.isoformat() if getattr(b, "completed_at", None) else None,
    }


def _manager_or_404(db: DbSession, manager_id: str) -> MobileServiceManager:
    row = db.query(MobileServiceManager).filter(MobileServiceManager.id == manager_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    return row


def _ensure_loyalty_row(db: DbSession) -> MobileLoyaltyProgram:
    row = db.query(MobileLoyaltyProgram).order_by(MobileLoyaltyProgram.created_at.asc()).first()
    if row is None:
        row = MobileLoyaltyProgram(qualifying_service_count=10, tiers_json="[]")
        db.add(row)
        db.flush()
    return row


def _slot_settings_or_create(db: DbSession, manager: MobileServiceManager) -> MobileSlotSettings:
    row = (
        db.query(MobileSlotSettings)
        .filter(MobileSlotSettings.manager_id == manager.id)
        .one_or_none()
    )
    if row is None:
        row = MobileSlotSettings(manager_id=manager.id, city_pin_code=manager.city_pin_code)
        db.add(row)
        db.flush()
    return row


@router.get("/managers")
def list_managers(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    rows = db.query(MobileServiceManager).order_by(MobileServiceManager.city_pin_code).all()
    return [_manager_to_dict(r) for r in rows]


@router.post("/managers")
def create_manager(body: MobileManagerCreate, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    city_pin_code = normalize_mobile_city_pin(body.city_pin_code)
    if len(city_pin_code) != 6:
        raise HTTPException(status_code=400, detail={"detail": "Invalid city pin code", "code": "invalid_pin_code"})
    row = MobileServiceManager(
        city_pin_code=city_pin_code,
        emp_name=body.emp_name,
        address=body.address,
        zip_code=body.zip_code,
        email=body.email,
        mobile=body.mobile,
        doj=body.doj,
        login_id=str(body.login_id or "").strip(),
        password_hash=hash_password(body.password),
        active=body.active,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_create_manager", "failed", request, city_pin_code=city_pin_code, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Manager already exists for pin or login", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_create_manager", "failed", request, city_pin_code=city_pin_code, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "create_mobile_manager", request, manager_id=row.id, city_pin_code=city_pin_code)
    action_log("admin_mobile_create_manager", "success", request, manager_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    return _manager_to_dict(row)


@router.patch("/managers/{manager_id}")
def update_manager(
    manager_id: str, body: MobileManagerUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = _manager_or_404(db, manager_id)
    data = body.model_dump(exclude_unset=True)
    for key in ("emp_name", "address", "zip_code", "email", "mobile", "doj", "login_id", "active"):
        if key in data:
            val = data[key]
            if key == "login_id" and val is not None:
                val = str(val).strip()
            setattr(row, key, val)
    if "password" in data and data["password"]:
        row.password_hash = hash_password(str(data["password"]))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_update_manager", "failed", request, manager_id=manager_id, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Conflicting login or pin record", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_update_manager", "failed", request, manager_id=manager_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_manager", request, manager_id=manager_id)
    action_log("admin_mobile_update_manager", "success", request, manager_id=manager_id, latency_ms=round(monotonic_ms() - started, 2))
    return _manager_to_dict(row)


@router.delete("/managers/{manager_id}", status_code=204)
def delete_manager(manager_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = _manager_or_404(db, manager_id)
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_delete_manager", "failed", request, manager_id=manager_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_mobile_manager", request, manager_id=manager_id)
    action_log("admin_mobile_delete_manager", "success", request, manager_id=manager_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


@router.get("/drivers")
def list_drivers(db: DbSession, _admin: AdminUser, city_pin_code: str | None = None) -> list[dict[str, Any]]:
    q = db.query(MobileServiceDriver)
    if city_pin_code:
        q = q.filter(MobileServiceDriver.city_pin_code == normalize_mobile_city_pin(city_pin_code))
    rows = q.order_by(MobileServiceDriver.city_pin_code, MobileServiceDriver.emp_name).all()
    return [_driver_to_dict(r) for r in rows]


@router.post("/drivers")
def create_driver(body: MobileDriverCreate, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    city_pin_code = normalize_mobile_city_pin(body.city_pin_code)
    if len(city_pin_code) != 6:
        raise HTTPException(status_code=400, detail={"detail": "Invalid city pin code", "code": "invalid_pin_code"})
    service_pin_code = normalize_mobile_city_pin(body.service_pin_code or body.city_pin_code) or city_pin_code
    manager = db.query(MobileServiceManager).filter(MobileServiceManager.city_pin_code == city_pin_code).one_or_none()
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found for city pin", "code": "not_found"})
    row = MobileServiceDriver(
        manager_id=manager.id,
        city_pin_code=city_pin_code,
        service_pin_code=service_pin_code,
        emp_name=body.emp_name,
        address=body.address,
        zip_code=body.zip_code,
        serviceable_zip_codes_json=dumps_json([_pin(z) for z in body.serviceable_zip_codes if _pin(z)]),
        email=body.email,
        mobile=body.mobile,
        doj=body.doj,
        login_id=str(body.login_id or "").strip(),
        password_hash=hash_password(body.password),
        active=body.active,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_create_driver", "failed", request, city_pin_code=city_pin_code, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Driver login already exists", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_create_driver", "failed", request, city_pin_code=city_pin_code, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "create_mobile_driver", request, driver_id=row.id, city_pin_code=city_pin_code)
    action_log("admin_mobile_create_driver", "success", request, driver_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    return _driver_to_dict(row)


@router.patch("/drivers/{driver_id}")
def update_driver(
    driver_id: str, body: MobileDriverUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobileServiceDriver).filter(MobileServiceDriver.id == driver_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    for key in ("emp_name", "address", "zip_code", "email", "mobile", "doj", "active"):
        if key in data:
            setattr(row, key, data[key])
    if "login_id" in data and data["login_id"] is not None:
        row.login_id = str(data["login_id"]).strip()
    if "service_pin_code" in data and data["service_pin_code"] is not None:
        row.service_pin_code = normalize_mobile_city_pin(str(data["service_pin_code"])) or row.service_pin_code
    if "serviceable_zip_codes" in data and data["serviceable_zip_codes"] is not None:
        row.serviceable_zip_codes_json = dumps_json([_pin(z) for z in data["serviceable_zip_codes"] if _pin(z)])
    if "password" in data and data["password"]:
        row.password_hash = hash_password(str(data["password"]))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_update_driver", "failed", request, driver_id=driver_id, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Conflicting driver login", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_update_driver", "failed", request, driver_id=driver_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_driver", request, driver_id=driver_id)
    action_log("admin_mobile_update_driver", "success", request, driver_id=driver_id, latency_ms=round(monotonic_ms() - started, 2))
    return _driver_to_dict(row)


@router.delete("/drivers/{driver_id}", status_code=204)
def delete_driver(driver_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobileServiceDriver).filter(MobileServiceDriver.id == driver_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_delete_driver", "failed", request, driver_id=driver_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_mobile_driver", request, driver_id=driver_id)
    action_log("admin_mobile_delete_driver", "success", request, driver_id=driver_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


@router.get("/addons")
def list_mobile_addons(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    ensure_mobile_global_addons_migrated(db)
    rows = db.query(MobileGlobalAddonItem).order_by(MobileGlobalAddonItem.name.asc()).all()
    return [_global_addon_to_dict(r) for r in rows]


@router.put("/addons")
def replace_mobile_addons(
    body: MobileGlobalAddonsReplace, db: DbSession, _admin: AdminUser, request: Request
) -> list[dict[str, Any]]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    ensure_mobile_global_addons_migrated(db)
    db.query(MobileGlobalAddonItem).delete(synchronize_session=False)
    for item in body.items:
        db.add(
            MobileGlobalAddonItem(
                id=item.id or new_id(),
                name=item.name,
                price=item.price,
                description_points=dumps_json(item.description_points),
                active=item.active,
            )
        )
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_replace_addons", "failed", request, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "replace_mobile_addons", request, item_count=len(body.items))
    action_log("admin_mobile_replace_addons", "success", request, item_count=len(body.items), latency_ms=round(monotonic_ms() - started, 2))
    rows = db.query(MobileGlobalAddonItem).order_by(MobileGlobalAddonItem.name.asc()).all()
    return [_global_addon_to_dict(r) for r in rows]


@router.get("/vehicle-blocks")
def list_vehicle_blocks(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    ensure_mobile_global_addons_migrated(db)
    rows = db.query(MobileVehicleCatalogBlock).order_by(MobileVehicleCatalogBlock.vehicle_type).all()
    return [_vehicle_block_to_dict(r) for r in rows]


@router.post("/vehicle-blocks")
def create_vehicle_block(
    body: MobileVehicleBlockCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    ensure_mobile_global_addons_migrated(db)
    block = MobileVehicleCatalogBlock(vehicle_type=body.vehicle_type)
    db.add(block)
    db.flush()
    for s in body.services:
        db.add(
            MobileCatalogServiceItem(
                id=s.id or new_id(),
                vehicle_block_id=block.id,
                name=s.name,
                price=s.price,
                free_coffee_count=s.free_coffee_count,
                eligible_for_loyalty_points=s.eligible_for_loyalty_points,
                recommended=s.recommended,
                description_points=dumps_json(s.description_points),
                active=s.active,
            )
        )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_create_vehicle_block", "failed", request, vehicle_type=body.vehicle_type, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Vehicle type already exists", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_create_vehicle_block", "failed", request, vehicle_type=body.vehicle_type, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(block)
    audit_log("admin", admin_id, "create_mobile_vehicle_block", request, block_id=block.id)
    action_log("admin_mobile_create_vehicle_block", "success", request, block_id=block.id, latency_ms=round(monotonic_ms() - started, 2))
    return _vehicle_block_to_dict(block)


@router.put("/vehicle-blocks/{block_id}")
def replace_vehicle_block(
    block_id: str, body: MobileVehicleBlockCreate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    ensure_mobile_global_addons_migrated(db)
    block = db.query(MobileVehicleCatalogBlock).filter(MobileVehicleCatalogBlock.id == block_id).one_or_none()
    if not block:
        raise HTTPException(status_code=404, detail={"detail": "Vehicle block not found", "code": "not_found"})
    db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.vehicle_block_id == block.id).delete()
    db.query(MobileCatalogAddonItem).filter(MobileCatalogAddonItem.vehicle_block_id == block.id).delete()
    block.vehicle_type = body.vehicle_type
    for s in body.services:
        db.add(
            MobileCatalogServiceItem(
                id=s.id or new_id(),
                vehicle_block_id=block.id,
                name=s.name,
                price=s.price,
                free_coffee_count=s.free_coffee_count,
                eligible_for_loyalty_points=s.eligible_for_loyalty_points,
                recommended=s.recommended,
                description_points=dumps_json(s.description_points),
                active=s.active,
            )
        )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_replace_vehicle_block", "failed", request, block_id=block_id, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Vehicle type already exists", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_replace_vehicle_block", "failed", request, block_id=block_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(block)
    audit_log("admin", admin_id, "replace_mobile_vehicle_block", request, block_id=block_id)
    action_log("admin_mobile_replace_vehicle_block", "success", request, block_id=block_id, latency_ms=round(monotonic_ms() - started, 2))
    return _vehicle_block_to_dict(block)


@router.delete("/vehicle-blocks/{block_id}", status_code=204)
def delete_vehicle_block(block_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    block = db.query(MobileVehicleCatalogBlock).filter(MobileVehicleCatalogBlock.id == block_id).one_or_none()
    if not block:
        raise HTTPException(status_code=404, detail={"detail": "Vehicle block not found", "code": "not_found"})
    db.delete(block)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_delete_vehicle_block", "failed", request, block_id=block_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_mobile_vehicle_block", request, block_id=block_id)
    action_log("admin_mobile_delete_vehicle_block", "success", request, block_id=block_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


@router.get("/promotions")
def list_promotions(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    rows = db.query(MobilePromotion).order_by(MobilePromotion.created_at.desc()).all()
    return [_promotion_to_dict(r) for r in rows]


@router.post("/promotions")
def create_promotion(body: MobilePromoIn, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = MobilePromotion(
        id=body.id or new_id(),
        code_name=body.code_name,
        discount_type=body.discount_type,
        discount_value=body.discount_value,
        validity_start=body.validity_start,
        validity_end=body.validity_end,
        max_uses_per_customer=body.max_uses_per_customer,
        applicable_service_ids=dumps_json(body.applicable_service_ids),
        applicable_vehicle_types=dumps_json(body.applicable_vehicle_types),
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_create_promotion", "failed", request, code_name=body.code_name, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Promotion code already exists", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_create_promotion", "failed", request, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "create_mobile_promotion", request, promo_id=row.id)
    action_log("admin_mobile_create_promotion", "success", request, promo_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    return _promotion_to_dict(row)


@router.patch("/promotions/{promo_id}")
def update_promotion(
    promo_id: str, body: MobilePromoIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobilePromotion).filter(MobilePromotion.id == promo_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Promotion not found", "code": "not_found"})
    row.code_name = body.code_name
    row.discount_type = body.discount_type
    row.discount_value = body.discount_value
    row.validity_start = body.validity_start
    row.validity_end = body.validity_end
    row.max_uses_per_customer = body.max_uses_per_customer
    row.applicable_service_ids = dumps_json(body.applicable_service_ids)
    row.applicable_vehicle_types = dumps_json(body.applicable_vehicle_types)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        action_log("admin_mobile_update_promotion", "failed", request, promo_id=promo_id, error_code="duplicate_record")
        raise HTTPException(status_code=409, detail={"detail": "Promotion code already exists", "code": "conflict"})
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_update_promotion", "failed", request, promo_id=promo_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_promotion", request, promo_id=promo_id)
    action_log("admin_mobile_update_promotion", "success", request, promo_id=promo_id, latency_ms=round(monotonic_ms() - started, 2))
    return _promotion_to_dict(row)


@router.delete("/promotions/{promo_id}", status_code=204)
def delete_promotion(promo_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobilePromotion).filter(MobilePromotion.id == promo_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Promotion not found", "code": "not_found"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_delete_promotion", "failed", request, promo_id=promo_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_mobile_promotion", request, promo_id=promo_id)
    action_log("admin_mobile_delete_promotion", "success", request, promo_id=promo_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


@router.get("/day-time-rules")
def list_day_rules(db: DbSession, _admin: AdminUser) -> list[dict[str, Any]]:
    rows = db.query(MobileDayTimePriceRule).order_by(MobileDayTimePriceRule.created_at.desc()).all()
    return [_day_rule_to_dict(r) for r in rows]


@router.post("/day-time-rules")
def create_day_rule(body: MobileDayTimeRuleIn, db: DbSession, _admin: AdminUser, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = MobileDayTimePriceRule(
        id=body.id or new_id(),
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
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_create_day_rule", "failed", request, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "create_mobile_day_rule", request, rule_id=row.id)
    action_log("admin_mobile_create_day_rule", "success", request, rule_id=row.id, latency_ms=round(monotonic_ms() - started, 2))
    return _day_rule_to_dict(row)


@router.patch("/day-time-rules/{rule_id}")
def update_day_rule(
    rule_id: str, body: MobileDayTimeRuleIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobileDayTimePriceRule).filter(MobileDayTimePriceRule.id == rule_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Day/time rule not found", "code": "not_found"})
    row.title = body.title
    row.description = body.description
    row.discount_type = body.discount_type
    row.discount_value = body.discount_value
    row.applicable_service_ids = dumps_json(body.applicable_service_ids)
    row.applicable_vehicle_types = dumps_json(body.applicable_vehicle_types)
    row.applicable_days = dumps_json(body.applicable_days)
    row.time_window_start = body.time_window_start
    row.time_window_end = body.time_window_end
    row.validity_start = body.validity_start
    row.validity_end = body.validity_end
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_update_day_rule", "failed", request, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_day_rule", request, rule_id=rule_id)
    action_log("admin_mobile_update_day_rule", "success", request, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return _day_rule_to_dict(row)


@router.delete("/day-time-rules/{rule_id}", status_code=204)
def delete_day_rule(rule_id: str, db: DbSession, _admin: AdminUser, request: Request) -> Response:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobileDayTimePriceRule).filter(MobileDayTimePriceRule.id == rule_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Day/time rule not found", "code": "not_found"})
    db.delete(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_delete_day_rule", "failed", request, rule_id=rule_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    audit_log("admin", admin_id, "delete_mobile_day_rule", request, rule_id=rule_id)
    action_log("admin_mobile_delete_day_rule", "success", request, rule_id=rule_id, latency_ms=round(monotonic_ms() - started, 2))
    return Response(status_code=204)


@router.get("/loyalty")
def get_loyalty(db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    row = _ensure_loyalty_row(db)
    return _loyalty_to_dict(row)


@router.put("/loyalty")
def put_loyalty(
    body: MobileLoyaltyProgramIn, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = _ensure_loyalty_row(db)
    row.qualifying_service_count = body.qualifying_service_count
    row.tiers_json = dumps_json(
        [
            {
                "id": t.id,
                "minSpendInWindow": t.min_spend_in_window,
                "maxSpendInWindow": t.max_spend_in_window,
                "rewardServiceId": t.reward_service_id,
            }
            for t in body.tiers
        ]
    )
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_put_loyalty", "failed", request, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_loyalty", request)
    action_log("admin_mobile_put_loyalty", "success", request, latency_ms=round(monotonic_ms() - started, 2))
    return _loyalty_to_dict(row)


@router.get("/slot-settings/{city_pin_code}")
def get_slot_settings(city_pin_code: str, db: DbSession, _admin: AdminUser) -> dict[str, Any]:
    pin = normalize_mobile_city_pin(city_pin_code)
    manager = db.query(MobileServiceManager).filter(MobileServiceManager.city_pin_code == pin).one_or_none()
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    row = _slot_settings_or_create(db, manager)
    return _slot_settings_to_dict(row)


@router.patch("/slot-settings/{city_pin_code}")
def patch_slot_settings(
    city_pin_code: str, body: MobileSlotSettingsPatch, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    pin = normalize_mobile_city_pin(city_pin_code)
    manager = db.query(MobileServiceManager).filter(MobileServiceManager.city_pin_code == pin).one_or_none()
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
    row = _slot_settings_or_create(db, manager)
    data = body.model_dump(exclude_unset=True)
    if "slot_duration_minutes" in data and data["slot_duration_minutes"] is not None:
        row.slot_duration_minutes = int(data["slot_duration_minutes"])
    if "open_time" in data and data["open_time"] is not None:
        row.open_time = str(data["open_time"])
    if "close_time" in data and data["close_time"] is not None:
        row.close_time = str(data["close_time"])
    if "slot_window_active_by_key" in data and data["slot_window_active_by_key"] is not None:
        row.slot_window_active_by_key_json = dumps_json(data["slot_window_active_by_key"])
    if "slot_driver_open_by_window" in data and data["slot_driver_open_by_window"] is not None:
        row.slot_driver_open_by_window_json = dumps_json(data["slot_driver_open_by_window"])
    if "slot_day_states" in data and data["slot_day_states"] is not None:
        row.slot_day_states_json = dumps_json(data["slot_day_states"])
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_patch_slot_settings", "failed", request, city_pin_code=pin, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_slot_settings", request, city_pin_code=pin)
    action_log("admin_mobile_patch_slot_settings", "success", request, city_pin_code=pin, latency_ms=round(monotonic_ms() - started, 2))
    return _slot_settings_to_dict(row)


@router.get("/bookings")
def list_bookings(db: DbSession, _admin: AdminUser, city_pin_code: str | None = None) -> list[dict[str, Any]]:
    q = db.query(MobileBooking)
    if city_pin_code:
        q = q.filter(MobileBooking.city_pin_code == normalize_mobile_city_pin(city_pin_code))
    rows = q.order_by(MobileBooking.slot_date.desc(), MobileBooking.start_time).all()
    return [_booking_to_dict(r) for r in rows]


@router.patch("/bookings/{booking_id}")
def patch_booking(
    booking_id: str, body: MobileBookingUpdate, db: DbSession, _admin: AdminUser, request: Request
) -> dict[str, Any]:
    started = monotonic_ms()
    admin_id = str(_admin["sub"])
    row = db.query(MobileBooking).filter(MobileBooking.id == booking_id).one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail={"detail": "Booking not found", "code": "not_found"})
    data = body.model_dump(exclude_unset=True)
    prev_status = row.status
    if "assigned_driver_id" in data:
        driver_id = data["assigned_driver_id"]
        if driver_id is not None:
            driver = (
                db.query(MobileServiceDriver)
                .filter(MobileServiceDriver.id == driver_id, MobileServiceDriver.city_pin_code == row.city_pin_code, MobileServiceDriver.active.is_(True))
                .one_or_none()
            )
            if not driver:
                raise HTTPException(status_code=404, detail={"detail": "Driver not found", "code": "not_found"})
            manager = db.query(MobileServiceManager).filter(MobileServiceManager.id == row.manager_id).one_or_none()
            if not manager:
                raise HTTPException(status_code=404, detail={"detail": "Mobile manager not found", "code": "not_found"})
            try:
                mobile_slot_service.assert_driver_assignable(
                    db,
                    manager,
                    row.slot_date,
                    row.start_time,
                    row.end_time,
                    driver_id,
                    exclude_booking_id=row.id,
                )
            except ValueError as e:
                code = str(e)
                if code == "driver_not_open":
                    raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
                if code == "driver_busy":
                    raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
                if code == "slot_unavailable":
                    raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
                raise HTTPException(status_code=400, detail={"detail": "Invalid driver assignment", "code": "invalid_slot"})
        row.assigned_driver_id = driver_id
    if "status" in data and data["status"] is not None:
        st = str(data["status"])
        if st not in ("scheduled", "checked_in", "in_progress", "completed", "cancelled"):
            raise HTTPException(status_code=400, detail={"detail": "Invalid status", "code": "invalid_status"})
        row.status = st
    if "notes" in data and data["notes"] is not None:
        row.notes = str(data["notes"])
    if "tip_cents" in data and data["tip_cents"] is not None:
        row.tip_cents = max(0, int(data["tip_cents"]))
    loyalty_service.on_mobile_booking_status_change(db, row, prev_status)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("admin_mobile_patch_booking", "failed", request, booking_id=booking_id, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("admin", admin_id, "update_mobile_booking", request, booking_id=booking_id)
    action_log("admin_mobile_patch_booking", "success", request, booking_id=booking_id, latency_ms=round(monotonic_ms() - started, 2))
    return _booking_to_dict(row)

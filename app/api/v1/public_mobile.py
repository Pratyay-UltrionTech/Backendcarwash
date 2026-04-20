"""Public mobile service API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import (
    MobileBooking,
    MobileCatalogServiceItem,
    MobileDayTimePriceRule,
    MobileGlobalAddonItem,
    MobileLoyaltyProgram,
    MobilePromotion,
    MobileSlotSettings,
    MobileVehicleCatalogBlock,
)
from app.models.base import new_id
from app.schemas.mobile import MobileBookingCreate
from app.services import mobile_slot_service
from app.services.slot_service import add_minutes_to_hhmm
from app.services.mobile_addon_migration import ensure_mobile_global_addons_migrated
from app.services.jsonutil import dumps_json, loads_json_array, loads_json_object

router = APIRouter(prefix="/public/mobile", tags=["public-mobile"])


def _pin(pin: str) -> str:
    return "".join(ch for ch in str(pin) if ch.isdigit())


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
        "catalog_group_id": getattr(s, "catalog_group_id", None),
        "duration_minutes": int(getattr(s, "duration_minutes", 60) or 60),
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
        "services": [_service_to_dict(s) for s in b.services if s.active],
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


def _booking_to_dict(b: MobileBooking) -> dict[str, Any]:
    from app.services.duration_slots import snap_duration_to_base_slots

    s0, s1 = mobile_slot_service.booking_span_minutes(b.start_time, b.end_time)
    duration_minutes = snap_duration_to_base_slots(s1 - s0)
    return {
        "id": b.id,
        "city_pin_code": b.city_pin_code,
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "selected_addon_ids": loads_json_array(b.selected_addon_ids_json),
        "duration_minutes": duration_minutes,
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "status": b.status,
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "created_at": b.created_at.isoformat() if b.created_at else None,
    }


@router.get("/day-time-rules")
def list_public_mobile_day_time_rules(db: DbSession, request: Request) -> list[dict[str, Any]]:
    """Public list of mobile day/time pricing rules (for USER home offers, no PIN required)."""
    started = monotonic_ms()
    rows = db.query(MobileDayTimePriceRule).order_by(MobileDayTimePriceRule.created_at.desc()).all()
    out = [_day_rule_to_dict(r) for r in rows]
    action_log(
        "public_mobile_list_day_time_rules",
        "success",
        request,
        row_count=len(out),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return out


@router.get("/serviceability/{pin_code}")
def check_serviceability(pin_code: str, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    pin = _pin(pin_code)
    if not pin:
        raise HTTPException(status_code=400, detail={"detail": "Invalid pin code", "code": "invalid_pin_code"})
    manager, available_count = mobile_slot_service.manager_for_service_pin(db, pin)
    if not manager:
        out = {"serviceable": False, "city_pin_code": pin, "available_drivers": 0}
        action_log("public_mobile_serviceability", "success", request, city_pin_code=pin, serviceable=False, latency_ms=round(monotonic_ms() - started, 2))
        return out
    out = {
        "serviceable": available_count > 0,
        "city_pin_code": manager.city_pin_code,
        "available_drivers": available_count,
        "requested_pin_code": pin,
    }
    action_log("public_mobile_serviceability", "success", request, city_pin_code=pin, serviceable=out["serviceable"], available_drivers=out["available_drivers"], latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/snapshot")
def mobile_snapshot(
    db: DbSession,
    request: Request,
    pin_code: str = Query(..., description="Service pin code"),
) -> dict[str, Any]:
    started = monotonic_ms()
    pin = _pin(pin_code)
    if not pin:
        raise HTTPException(status_code=400, detail={"detail": "Invalid pin code", "code": "invalid_pin_code"})
    manager, available_count = mobile_slot_service.manager_for_service_pin(db, pin)
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Service not available in this pin code", "code": "service_not_available"})
    ensure_mobile_global_addons_migrated(db)
    blocks = db.query(MobileVehicleCatalogBlock).order_by(MobileVehicleCatalogBlock.vehicle_type).all()
    global_addons = db.query(MobileGlobalAddonItem).order_by(MobileGlobalAddonItem.name.asc()).all()
    promos = db.query(MobilePromotion).all()
    day_rules = db.query(MobileDayTimePriceRule).all()
    loyalty = db.query(MobileLoyaltyProgram).order_by(MobileLoyaltyProgram.created_at.asc()).first()
    slot = db.query(MobileSlotSettings).filter(MobileSlotSettings.manager_id == manager.id).one_or_none()
    out = {
        "service_area": {
            "requested_pin_code": pin,
            "city_pin_code": manager.city_pin_code,
            "manager_id": manager.id,
            "available_drivers": available_count,
        },
        "vehicle_blocks": [_vehicle_block_to_dict(v) for v in blocks],
        "mobile_addons": [_global_addon_to_dict(a) for a in global_addons if a.active],
        "promotions": [_promotion_to_dict(p) for p in promos],
        "day_time_rules": [_day_rule_to_dict(r) for r in day_rules],
        "loyalty": (
            {
                "qualifying_service_count": loyalty.qualifying_service_count,
                "tiers": loads_json_array(loyalty.tiers_json),
            }
            if loyalty
            else {"qualifying_service_count": 10, "tiers": []}
        ),
        "slot_settings": (
            {
                "slot_duration_minutes": slot.slot_duration_minutes,
                "open_time": slot.open_time,
                "close_time": slot.close_time,
                "slot_window_active_by_key": loads_json_object(slot.slot_window_active_by_key_json),
                "slot_driver_open_by_window": loads_json_object(slot.slot_driver_open_by_window_json),
                "slot_day_states": loads_json_object(slot.slot_day_states_json),
            }
            if slot
            else None
        ),
    }
    action_log("public_mobile_snapshot", "success", request, city_pin_code=pin, latency_ms=round(monotonic_ms() - started, 2))
    return out


@router.get("/slots")
def list_public_mobile_slots(
    db: DbSession,
    request: Request,
    pin_code: str = Query(...),
    date: str = Query(..., description="ISO date YYYY-MM-DD"),
    duration_minutes: int | None = Query(
        default=None,
        ge=30,
        description="Total booking duration (service + add-ons). Defaults to one 30-minute base slot.",
    ),
) -> list[dict[str, Any]]:
    started = monotonic_ms()
    pin = _pin(pin_code)
    if not pin:
        raise HTTPException(status_code=400, detail={"detail": "Invalid pin code", "code": "invalid_pin_code"})
    manager, _ = mobile_slot_service.manager_for_service_pin(db, pin)
    if not manager:
        raise HTTPException(
            status_code=404,
            detail={"detail": "Service not available in this pin code", "code": "service_not_available"},
        )
    rows = mobile_slot_service.list_slot_availability(
        db, manager, date, booking_duration_minutes=duration_minutes
    )
    out = [
        {
            "startTime": r.start_time,
            "endTime": r.end_time,
            "label": f"{r.start_time} – {r.end_time} ({r.duration_minutes} min)",
            "capacity": r.capacity,
            "booked": r.booked,
            "available": r.available,
            "durationMinutes": r.duration_minutes,
            "slotsNeeded": r.slots_needed,
        }
        for r in rows
    ]
    action_log(
        "public_mobile_list_slots",
        "success",
        request,
        city_pin_code=pin,
        date=date,
        row_count=len(out),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return out


@router.post("/bookings")
def create_mobile_booking(body: MobileBookingCreate, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    pin = _pin(body.city_pin_code)
    if not pin:
        raise HTTPException(status_code=400, detail={"detail": "city_pin_code is required", "code": "validation_error"})
    manager, _ = mobile_slot_service.manager_for_service_pin(db, pin)
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Service not available in this pin code", "code": "service_not_available"})
    dur_m = mobile_slot_service.resolve_mobile_booking_duration_minutes(db, body.service_id, body.selected_addon_ids)
    end_time = body.end_time or add_minutes_to_hhmm(body.start_time, dur_m)
    assigned_driver_id = body.assigned_driver_id
    try:
        mobile_slot_service.assert_slot_available(db, manager, body.slot_date, body.start_time, end_time)
        if assigned_driver_id:
            mobile_slot_service.assert_driver_assignable(
                db,
                manager,
                body.slot_date,
                body.start_time,
                end_time,
                assigned_driver_id,
            )
        else:
            assigned_driver_id = mobile_slot_service.allocate_driver_for_interval(
                db, manager, body.slot_date, body.start_time, end_time
            )
            if assigned_driver_id is None:
                raise ValueError("slot_unavailable")
    except ValueError as e:
        code = str(e)
        if code == "slot_unavailable":
            raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
        if code == "driver_not_open":
            raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
        if code == "driver_busy":
            raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
        raise HTTPException(status_code=400, detail={"detail": "Invalid slot assignment", "code": "invalid_slot"})
    # Store manager territory key (manager.city_pin_code), not the customer's requested service pin.
    # All manager/washer queries filter on this field; service-area resolution is manager_for_service_pin(pin).
    row = MobileBooking(
        id=new_id(),
        manager_id=manager.id,
        city_pin_code=manager.city_pin_code,
        customer_name=body.customer_name,
        address=body.address,
        phone=body.phone,
        vehicle_summary=body.vehicle_summary,
        service_id=body.service_id,
        vehicle_type=body.vehicle_type,
        selected_addon_ids_json=dumps_json(body.selected_addon_ids),
        slot_date=body.slot_date,
        start_time=body.start_time,
        end_time=end_time,
        assigned_driver_id=assigned_driver_id,
        status="scheduled",
        source=body.source,
        notes=body.notes,
        tip_cents=max(0, int(body.tip_cents or 0)),
    )
    db.add(row)
    try:
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        action_log("public_mobile_create_booking", "failed", request, city_pin_code=pin, error_code="db_commit_failed")
        raise HTTPException(status_code=500, detail={"detail": "Database operation failed", "code": "db_error"})
    db.refresh(row)
    audit_log("customer_public", "anonymous", "create_mobile_booking", request, booking_id=row.id, city_pin_code=pin)
    action_log(
        "public_mobile_create_booking",
        "success",
        request,
        booking_id=row.id,
        city_pin_code=manager.city_pin_code,
        requested_pin=pin,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    out = _booking_to_dict(row)
    out["requested_service_pin"] = pin
    return out


@router.get("/bookings/{booking_id}")
def get_mobile_booking(booking_id: str, db: DbSession, request: Request) -> dict[str, Any]:
    started = monotonic_ms()
    row = db.query(MobileBooking).filter(MobileBooking.id == booking_id).one_or_none()
    if not row:
        action_log(
            "public_mobile_get_booking",
            "failed",
            request,
            booking_id=booking_id,
            error_code="booking_not_found",
            latency_ms=round(monotonic_ms() - started, 2),
        )
        raise HTTPException(
            status_code=404,
            detail={"detail": "Booking not found", "code": "booking_not_found"},
        )
    action_log(
        "public_mobile_get_booking",
        "success",
        request,
        booking_id=booking_id,
        city_pin_code=row.city_pin_code,
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return _booking_to_dict(row)

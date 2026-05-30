"""Public mobile service API."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import DbSession, OptionalCustomerAuth
from app.core.observability import action_log, audit_log, monotonic_ms
from app.models import (
    MobileBooking,
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
from app.models.base import new_id
from app.schemas.mobile import MobileBookingCreate
from app.services import mobile_slot_service
from app.services.booking_status import effective_status
from app.services.slot_service import add_minutes_to_hhmm
from app.services.mobile_addon_migration import ensure_mobile_global_addons_migrated
from app.services.jsonutil import dumps_json, loads_json_array, loads_json_object

router = APIRouter(prefix="/public/mobile", tags=["public-mobile"])


def _pin(pin: str) -> str:
    return "".join(ch for ch in str(pin) if ch.isdigit())


def _manager_all_pins(db: Any, manager: MobileServiceManager) -> set[str]:
    """Return all postcodes the manager serves (hub + every active driver's pins)."""
    pins: set[str] = {_pin(manager.city_pin_code)}
    drivers = (
        db.query(MobileServiceDriver)
        .filter(MobileServiceDriver.manager_id == manager.id, MobileServiceDriver.active.is_(True))
        .all()
    )
    for d in drivers:
        if d.service_pin_code:
            p = _pin(d.service_pin_code)
            if p:
                pins.add(p)
        for z in loads_json_array(d.serviceable_zip_codes_json or "[]"):
            p = _pin(str(z))
            if p:
                pins.add(p)
    return pins


def _address_contains_any_pin(address: str, pins: set[str]) -> bool:
    for p in pins:
        if p and re.search(rf"(^|[^0-9]){re.escape(p)}([^0-9]|$)", address):
            return True
    return False


def _service_to_dict(s: MobileCatalogServiceItem) -> dict[str, Any]:
    return {
        "id": s.id,
        "name": s.name,
        "price": s.price,
        "free_coffee_count": s.free_coffee_count,
        "eligible_for_loyalty_points": s.eligible_for_loyalty_points,
        "recommended": s.recommended,
        "description_points": loads_json_array(s.description_points),
        "excluded_points": loads_json_array(getattr(s, "excluded_points", "[]") or "[]"),
        "active": s.active,
        "catalog_group_id": getattr(s, "catalog_group_id", None),
        "category": getattr(s, "category", "Washing"),
        "duration_minutes": int(getattr(s, "duration_minutes", 60) or 60),
        "sequence": int(getattr(s, "sequence", 999) or 999),
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


def _mobile_block_has_bookable_service(block: MobileVehicleCatalogBlock) -> bool:
    return any(
        getattr(s, "active", True) is not False and getattr(s, "catalog_group_id", None)
        for s in block.services
    )


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
        "requested_zip_code": getattr(b, "requested_zip_code", "") or "",
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "customer_email": getattr(b, "customer_email", "") or "",
        "vehicle_summary": b.vehicle_summary,
        "service_id": b.service_id,
        "vehicle_type": b.vehicle_type,
        "vehicle_model": getattr(b, "vehicle_model", ""),
        "selected_addon_ids": loads_json_array(b.selected_addon_ids_json),
        "duration_minutes": duration_minutes,
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "assigned_driver_id": b.assigned_driver_id,
        "customer_id": str(b.customer_id) if getattr(b, "customer_id", None) else None,
        "status": effective_status(b.status, b.slot_date, b.end_time),
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


@router.get("/vehicle-blocks")
def list_public_mobile_vehicle_blocks(db: DbSession, request: Request) -> list[dict[str, Any]]:
    """Bookable mobile vehicle types/services for USER profile and catalog (no PIN required)."""
    started = monotonic_ms()
    ensure_mobile_global_addons_migrated(db)
    rows = db.query(MobileVehicleCatalogBlock).order_by(MobileVehicleCatalogBlock.vehicle_type).all()
    out = [_vehicle_block_to_dict(v) for v in rows if _mobile_block_has_bookable_service(v)]
    action_log(
        "public_mobile_list_vehicle_blocks",
        "success",
        request,
        row_count=len(out),
        latency_ms=round(monotonic_ms() - started, 2),
    )
    return out


@router.get("/promotions")
def list_public_mobile_promotions(db: DbSession, request: Request) -> list[dict[str, Any]]:
    """Public list of mobile promotions (for USER home offers and coupon picker)."""
    started = monotonic_ms()
    rows = db.query(MobilePromotion).order_by(MobilePromotion.created_at.desc()).all()
    out = [_promotion_to_dict(p) for p in rows]
    action_log(
        "public_mobile_list_promotions",
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
        "vehicle_blocks": [_vehicle_block_to_dict(v) for v in blocks if _mobile_block_has_bookable_service(v)],
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
        db, manager, date, booking_duration_minutes=duration_minutes,
        requested_pin=pin, strict_zip=True,
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
def create_mobile_booking(
    body: MobileBookingCreate, db: DbSession, request: Request, auth: OptionalCustomerAuth = None
) -> dict[str, Any]:
    started = monotonic_ms()

    # Validate that the slot is in the future
    now = datetime.now()
    today_iso = now.date().isoformat()
    if body.slot_date < today_iso:
        raise HTTPException(status_code=400, detail={"detail": "Cannot book a past date", "code": "past_date"})
    
    if body.slot_date == today_iso:
        from app.services.mobile_slot_service import _parse_hhmm
        start_m = _parse_hhmm(body.start_time)
        now_m = now.hour * 60 + now.minute
        if start_m < now_m:
            raise HTTPException(status_code=400, detail={"detail": "Cannot book a past time", "code": "past_time"})

    pin = _pin(body.city_pin_code)
    if not pin:
        raise HTTPException(status_code=400, detail={"detail": "city_pin_code is required", "code": "validation_error"})
    manager, _ = mobile_slot_service.manager_for_service_pin(db, pin)
    if not manager:
        raise HTTPException(status_code=404, detail={"detail": "Service not available in this pin code", "code": "service_not_available"})
    addr_stripped = (body.address or "").strip()
    if len(addr_stripped) < 10:
        raise HTTPException(
            status_code=400,
            detail={"detail": "Please enter a complete service address.", "code": "invalid_address"},
        )
    service_pins = _manager_all_pins(db, manager)
    if pin:
        service_pins.add(pin)
    if not _address_contains_any_pin(addr_stripped, service_pins):
        raise HTTPException(
            status_code=400,
            detail={
                "detail": "Service address must include a postcode covered by this mobile service.",
                "code": "address_pin_mismatch",
            },
        )
    dur_m = mobile_slot_service.resolve_mobile_booking_duration_minutes(db, body.service_id, body.selected_addon_ids)
    end_time = body.end_time or add_minutes_to_hhmm(body.start_time, dur_m)
    assigned_driver_id = body.assigned_driver_id
    try:
        mobile_slot_service.assert_slot_available(
            db, manager, body.slot_date, body.start_time, end_time,
            requested_pin=pin, strict_zip=True,
        )
        if assigned_driver_id:
            mobile_slot_service.assert_driver_assignable(
                db,
                manager,
                body.slot_date,
                body.start_time,
                end_time,
                assigned_driver_id,
                requested_pin=pin,
            )
        # For user-portal mobile bookings, keep assignment unassigned unless explicitly provided.
        # Mobile manager or eligible mobile driver will assign/accept later.
    except ValueError as e:
        code = str(e)
        if code == "slot_unavailable":
            raise HTTPException(status_code=409, detail={"detail": "Selected slot is not available", "code": "slot_unavailable"})
        if code == "driver_not_open":
            raise HTTPException(status_code=409, detail={"detail": "Driver is not available for this slot", "code": "driver_unavailable"})
        if code == "driver_busy":
            raise HTTPException(status_code=409, detail={"detail": "Driver is already assigned in this slot", "code": "driver_busy"})
        raise HTTPException(status_code=400, detail={"detail": "Invalid slot assignment", "code": "invalid_slot"})
    cust_email_raw = (body.customer_email or "").strip()
    if len(cust_email_raw) > 320:
        cust_email_raw = cust_email_raw[:320]
    # city_pin_code stores the manager's territory key (for manager/driver queries).
    # requested_zip_code stores what the customer searched — used for driver assignment.
    row = MobileBooking(
        id=new_id(),
        customer_id=str(auth["sub"]) if auth and auth.get("sub") else None,
        manager_id=manager.id,
        city_pin_code=manager.city_pin_code,
        requested_zip_code=pin,
        customer_name=body.customer_name,
        address=body.address,
        phone=body.phone,
        customer_email=cust_email_raw,
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
        vehicle_model=body.vehicle_model,
        registration_number=body.registration_number or "",
        promo_code=body.promo_code.strip().upper() if body.promo_code and body.promo_code.strip() else None,
        payment_method=body.payment_method or "later",
    )
    db.add(row)
    db.flush()
    from app.services.booking_pricing import mobile_booking_catalog_package_cents, resolve_charged_service_cents

    pkg = mobile_booking_catalog_package_cents(db, row)
    charged, promo_disc = resolve_charged_service_cents(pkg, body.service_charged_cents)
    row.service_charged_cents = charged
    row.promo_discount_cents = promo_disc

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
        consume_reward(db, body.loyalty_reward_id, str(auth["sub"]), row.id)

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
    from app.services.email_service import send_booking_confirmed_email, send_staff_booking_notification
    if auth and auth.get("email"):
        send_booking_confirmed_email(
            to_email=str(auth["email"]),
            name=row.customer_name or "",
            date=row.slot_date,
            start_time=row.start_time,
            service_summary=getattr(row, "vehicle_summary", "") or "",
            booking_id=row.id,
            customer_id=str(row.customer_id) if row.customer_id else None,
            phone=row.phone or None,
            end_time=row.end_time or None,
            channel="mobile",
            payment_method=getattr(row, "payment_method", None),
        )
    elif (getattr(row, "customer_email", None) or "").strip():
        send_booking_confirmed_email(
            to_email=str(row.customer_email).strip(),
            name=row.customer_name or "",
            date=row.slot_date,
            start_time=row.start_time,
            service_summary=getattr(row, "vehicle_summary", "") or "",
            booking_id=row.id,
            customer_id=str(row.customer_id) if row.customer_id else None,
            phone=row.phone or None,
            end_time=row.end_time or None,
            channel="mobile",
            payment_method=getattr(row, "payment_method", None),
        )
    send_staff_booking_notification(
        db,
        event="new_booking",
        booking_type="mobile",
        booking_id=row.id,
        customer_name=row.customer_name or "",
        phone=row.phone or "",
        vehicle_type=row.vehicle_type or "",
        vehicle_model=row.vehicle_model or "",
        registration_number=row.registration_number or "",
        service_summary=getattr(row, "vehicle_summary", "") or "",
        slot_date=row.slot_date,
        start_time=row.start_time,
        end_time=row.end_time,
        city_pin_code=str(row.city_pin_code),
        customer_id=str(row.customer_id) if row.customer_id else None,
        payment_method=getattr(row, "payment_method", None),
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

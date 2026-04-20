import json
from typing import Any

from app.models import (
    Branch,
    BranchBooking,
    BranchLoyalty,
    BranchManager,
    BranchSlotSettings,
    CatalogAddonItem,
    CatalogServiceItem,
    DayTimePriceRule,
    FreeCoffeeRule,
    Promotion,
    VehicleCatalogBlock,
    Washer,
)
from app.services.duration_slots import snap_duration_to_base_slots
from app.services.jsonutil import loads_json_array
from app.services import slot_service


def service_to_dict(s: CatalogServiceItem) -> dict[str, Any]:
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


def addon_to_dict(a: CatalogAddonItem) -> dict[str, Any]:
    return {
        "id": a.id,
        "name": a.name,
        "price": a.price,
        "description_points": loads_json_array(a.description_points),
        "active": a.active,
    }


def vehicle_block_to_dict(b: VehicleCatalogBlock) -> dict[str, Any]:
    return {
        "id": b.id,
        "branch_id": b.branch_id,
        "vehicle_type": b.vehicle_type,
        "services": [service_to_dict(s) for s in b.services],
        "addons": [addon_to_dict(a) for a in b.addons],
    }


def promo_to_dict(p: Promotion) -> dict[str, Any]:
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


def day_rule_to_dict(r: DayTimePriceRule) -> dict[str, Any]:
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


def free_coffee_to_dict(f: FreeCoffeeRule) -> dict[str, Any]:
    return {
        "id": f.id,
        "kind": f.kind,
        "service_name": f.service_name,
        "services_count": f.services_count,
        "notes": f.notes,
    }


def loyalty_to_dict(l: BranchLoyalty) -> dict[str, Any]:
    tiers = loads_json_array(l.tiers_json)
    return {
        "qualifying_service_count": l.qualifying_service_count,
        "tiers": tiers,
    }


def slot_settings_to_dict(s: BranchSlotSettings) -> dict[str, Any]:
    return {
        "manager_slot_duration_minutes": s.manager_slot_duration_minutes,
        "slot_bay_open_by_window": json.loads(s.slot_bay_open_by_window_json or "{}"),
        "slot_window_active_by_key": json.loads(s.slot_window_active_by_key_json or "{}"),
        "slot_day_states": json.loads(s.slot_day_states_json or "{}"),
    }


def booking_to_dict(b: BranchBooking) -> dict[str, Any]:
    created = b.created_at.isoformat() if getattr(b, "created_at", None) else None
    completed = b.completed_at.isoformat() if getattr(b, "completed_at", None) else None
    s0, s1 = slot_service.booking_span_minutes(b.start_time, b.end_time)
    duration_minutes = snap_duration_to_base_slots(s1 - s0)
    return {
        "id": b.id,
        "branch_id": b.branch_id,
        "customer_name": b.customer_name,
        "address": b.address,
        "phone": b.phone,
        "vehicle_type": b.vehicle_type,
        "service_summary": b.service_summary,
        "service_id": getattr(b, "service_id", None),
        "selected_addon_ids": loads_json_array(getattr(b, "selected_addon_ids_json", "[]") or "[]"),
        "duration_minutes": duration_minutes,
        "slot_date": b.slot_date,
        "start_time": b.start_time,
        "end_time": b.end_time,
        "bay_number": b.bay_number,
        "assigned_washer_id": b.assigned_washer_id,
        "status": b.status,
        "source": b.source,
        "notes": b.notes,
        "tip_cents": int(b.tip_cents or 0),
        "created_at": created,
        "completed_at": completed,
    }


def manager_to_dict(m: BranchManager) -> dict[str, Any]:
    return {
        "id": m.id,
        "branch_id": m.branch_id,
        "name": m.name,
        "address": m.address,
        "zip_code": m.zip_code,
        "email": m.email,
        "phone": m.phone,
        "doj": m.doj,
        "login_id": m.login_id,
        "active": m.active,
    }


def washer_to_dict(w: Washer) -> dict[str, Any]:
    return {
        "id": w.id,
        "branch_id": w.branch_id,
        "name": w.name,
        "address": w.address,
        "zip_code": w.zip_code,
        "email": w.email,
        "phone": w.phone,
        "doj": w.doj,
        "login_id": w.login_id,
        "assigned_bay": w.assigned_bay,
        "active": w.active,
    }


def branch_to_dict(b: Branch) -> dict[str, Any]:
    return {
        "id": b.id,
        "name": b.name,
        "location": b.location,
        "zip_code": b.zip_code,
        "bay_count": b.bay_count,
        "open_time": b.open_time,
        "close_time": b.close_time,
    }

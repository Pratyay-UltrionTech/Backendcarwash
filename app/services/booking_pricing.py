"""Customer service totals (excl. tips) for branch and mobile bookings.

Catalog service and add-on prices are treated as GST-inclusive; no extra tax is added.
When ``service_charged_cents`` is set on the booking row, that value is the amount the
customer agreed to pay for service + add-ons (after discounts), matching user portal /
manager checkout.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.booking import BranchBooking
from app.models.catalog import BranchAddonItem, CatalogAddonItem, CatalogServiceItem, VehicleCatalogBlock
from app.models.mobile import (
    MobileBooking,
    MobileCatalogAddonItem,
    MobileCatalogServiceItem,
    MobileGlobalAddonItem,
    MobileVehicleCatalogBlock,
)
from app.services.jsonutil import loads_json_array


def resolve_charged_service_cents(
    package_cents: int,
    service_charged_cents: int | None,
) -> tuple[int | None, int]:
    """Return (stored service_charged_cents, promo_discount_cents)."""
    if service_charged_cents is None:
        return None, 0
    pkg = max(0, int(package_cents))
    charged = max(0, min(int(service_charged_cents), pkg))
    return charged, pkg - charged


def branch_booking_catalog_package_cents(db: Session, booking: BranchBooking) -> int:
    """Catalog service + add-ons before discounts (cents)."""
    addon_ids = [str(x) for x in loads_json_array(booking.selected_addon_ids_json) if x]
    sub_d = 0.0
    if booking.service_id:
        row = (
            db.query(CatalogServiceItem)
            .join(VehicleCatalogBlock, CatalogServiceItem.vehicle_block_id == VehicleCatalogBlock.id)
            .filter(
                VehicleCatalogBlock.branch_id == booking.branch_id,
                CatalogServiceItem.id == booking.service_id,
            )
            .one_or_none()
        )
        if row:
            sub_d += float(row.price or 0)
    for aid in addon_ids:
        br_addon = (
            db.query(BranchAddonItem)
            .filter(BranchAddonItem.id == aid, BranchAddonItem.branch_id == booking.branch_id)
            .one_or_none()
        )
        if br_addon:
            sub_d += float(br_addon.price or 0)
            continue
        ca = (
            db.query(CatalogAddonItem)
            .join(VehicleCatalogBlock, CatalogAddonItem.vehicle_block_id == VehicleCatalogBlock.id)
            .filter(VehicleCatalogBlock.branch_id == booking.branch_id, CatalogAddonItem.id == aid)
            .one_or_none()
        )
        if ca:
            sub_d += float(ca.price or 0)
    return int(round(sub_d * 100))


def mobile_booking_catalog_package_cents(db: Session, booking: MobileBooking) -> int:
    addon_ids = [str(x) for x in loads_json_array(booking.selected_addon_ids_json) if x]
    sub_d = 0.0
    if booking.service_id:
        svc = db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id == booking.service_id).one_or_none()
        if svc:
            sub_d += float(svc.price or 0)

    if addon_ids:
        globals_found = (
            db.query(MobileGlobalAddonItem)
            .filter(MobileGlobalAddonItem.id.in_(addon_ids), MobileGlobalAddonItem.active.is_(True))
            .all()
        )
        used = {str(a.id) for a in globals_found}
        for a in globals_found:
            sub_d += float(a.price or 0)
        remaining = [a for a in addon_ids if a not in used]
        if remaining:
            block = (
                db.query(MobileVehicleCatalogBlock)
                .filter(MobileVehicleCatalogBlock.vehicle_type == booking.vehicle_type)
                .one_or_none()
            )
            if block:
                for aid in remaining:
                    row = (
                        db.query(MobileCatalogAddonItem)
                        .filter(
                            MobileCatalogAddonItem.id == aid,
                            MobileCatalogAddonItem.vehicle_block_id == block.id,
                        )
                        .one_or_none()
                    )
                    if row:
                        sub_d += float(row.price or 0)

    return int(round(sub_d * 100))


def _loyalty_service_discount_cents_branch(db: Session, booking: BranchBooking) -> int:
    """Legacy bookings: infer free primary service when a loyalty reward was redeemed."""
    if not booking.service_id:
        return 0
    from app.models.loyalty import LoyaltyReward

    reward = (
        db.query(LoyaltyReward)
        .filter(
            LoyaltyReward.redeemed_booking_id == booking.id,
            LoyaltyReward.channel == "branch",
            LoyaltyReward.status == "redeemed",
        )
        .one_or_none()
    )
    if not reward:
        return 0
    row = (
        db.query(CatalogServiceItem)
        .join(VehicleCatalogBlock, CatalogServiceItem.vehicle_block_id == VehicleCatalogBlock.id)
        .filter(
            VehicleCatalogBlock.branch_id == booking.branch_id,
            CatalogServiceItem.id == booking.service_id,
        )
        .one_or_none()
    )
    if row:
        return int(round(float(row.price or 0) * 100))
    return 0


def _loyalty_service_discount_cents_mobile(db: Session, booking: MobileBooking) -> int:
    if not booking.service_id:
        return 0
    from app.models.loyalty import LoyaltyReward

    reward = (
        db.query(LoyaltyReward)
        .filter(
            LoyaltyReward.redeemed_booking_id == booking.id,
            LoyaltyReward.channel == "mobile",
            LoyaltyReward.status == "redeemed",
        )
        .one_or_none()
    )
    if not reward:
        return 0
    svc = db.query(MobileCatalogServiceItem).filter(MobileCatalogServiceItem.id == booking.service_id).one_or_none()
    if svc:
        return int(round(float(svc.price or 0) * 100))
    return 0


def branch_booking_customer_service_total_cents(db: Session, booking: BranchBooking) -> int:
    """Amount customer pays for service + add-ons (excl. tip)."""
    stored = getattr(booking, "service_charged_cents", None)
    if stored is not None:
        return max(0, int(stored))
    package_cents = branch_booking_catalog_package_cents(db, booking)
    promo = int(getattr(booking, "promo_discount_cents", 0) or 0)
    total = max(0, package_cents - promo)
    total = max(0, total - _loyalty_service_discount_cents_branch(db, booking))
    return total


def mobile_booking_customer_service_total_cents(db: Session, booking: MobileBooking) -> int:
    stored = getattr(booking, "service_charged_cents", None)
    if stored is not None:
        return max(0, int(stored))
    package_cents = mobile_booking_catalog_package_cents(db, booking)
    promo = int(getattr(booking, "promo_discount_cents", 0) or 0)
    total = max(0, package_cents - promo)
    total = max(0, total - _loyalty_service_discount_cents_mobile(db, booking))
    return total

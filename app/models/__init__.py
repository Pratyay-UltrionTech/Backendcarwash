from app.models.base import Base, TimestampMixin  # Base must match database metadata
from app.models.admin_account import AdminAccount
from app.models.branch import Branch
from app.models.staff import BranchManager, Washer, WasherUnavailability, WasherLeaveRequest
from app.models.catalog import VehicleCatalogBlock, CatalogServiceItem, CatalogAddonItem, BranchAddonItem
from app.models.promotion import Promotion, DayTimePriceRule, FreeCoffeeRule
from app.models.loyalty import BranchLoyalty, LoyaltyLedgerEntry, LoyaltyReward
from app.models.slot_settings import BranchSlotSettings
from app.models.booking import BranchBooking
from app.models.customer import CustomerUser
from app.models.user_address import UserAddress
from app.models.mobile import (
    MobileBooking,
    MobileCatalogAddonItem,
    MobileCatalogServiceItem,
    MobileDayTimePriceRule,
    MobileDriverLeaveRequest,
    MobileGlobalAddonItem,
    MobileLoyaltyProgram,
    MobilePromotion,
    MobileServiceDriver,
    MobileServiceManager,
    MobileSlotSettings,
    MobileVehicleCatalogBlock,
)

__all__ = [
    "Base",
    "TimestampMixin",
    "AdminAccount",
    "Branch",
    "BranchManager",
    "Washer",
    "WasherUnavailability",
    "WasherLeaveRequest",
    "VehicleCatalogBlock",
    "CatalogServiceItem",
    "CatalogAddonItem",
    "BranchAddonItem",
    "Promotion",
    "DayTimePriceRule",
    "FreeCoffeeRule",
    "BranchLoyalty",
    "LoyaltyLedgerEntry",
    "LoyaltyReward",
    "BranchSlotSettings",
    "BranchBooking",
    "CustomerUser",
    "UserAddress",
    "MobileServiceManager",
    "MobileServiceDriver",
    "MobileDriverLeaveRequest",
    "MobileVehicleCatalogBlock",
    "MobileCatalogServiceItem",
    "MobileCatalogAddonItem",
    "MobileGlobalAddonItem",
    "MobilePromotion",
    "MobileDayTimePriceRule",
    "MobileLoyaltyProgram",
    "MobileSlotSettings",
    "MobileBooking",
]

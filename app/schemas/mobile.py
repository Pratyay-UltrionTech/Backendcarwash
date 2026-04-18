from pydantic import BaseModel, Field


class MobileManagerCreate(BaseModel):
    city_pin_code: str
    emp_name: str = ""
    address: str = ""
    zip_code: str = ""
    email: str = ""
    mobile: str = ""
    doj: str = ""
    login_id: str
    password: str
    active: bool = True


class MobileManagerUpdate(BaseModel):
    emp_name: str | None = None
    address: str | None = None
    zip_code: str | None = None
    email: str | None = None
    mobile: str | None = None
    doj: str | None = None
    login_id: str | None = None
    password: str | None = None
    active: bool | None = None


class MobileDriverCreate(BaseModel):
    city_pin_code: str
    service_pin_code: str
    emp_name: str = ""
    address: str = ""
    zip_code: str = ""
    serviceable_zip_codes: list[str] = Field(default_factory=list)
    email: str = ""
    mobile: str = ""
    doj: str = ""
    login_id: str
    password: str
    active: bool = True


class MobileDriverUpdate(BaseModel):
    service_pin_code: str | None = None
    emp_name: str | None = None
    address: str | None = None
    zip_code: str | None = None
    serviceable_zip_codes: list[str] | None = None
    email: str | None = None
    mobile: str | None = None
    doj: str | None = None
    login_id: str | None = None
    password: str | None = None
    active: bool | None = None


class MobileServiceItemIn(BaseModel):
    id: str | None = None
    name: str = ""
    price: float = 0
    free_coffee_count: int = 0
    eligible_for_loyalty_points: bool = True
    recommended: bool = False
    description_points: list[str] = Field(default_factory=list)
    active: bool = True


class MobileAddonItemIn(BaseModel):
    id: str | None = None
    name: str = ""
    price: float = 0
    description_points: list[str] = Field(default_factory=list)
    active: bool = True


class MobileVehicleBlockCreate(BaseModel):
    vehicle_type: str
    services: list[MobileServiceItemIn] = Field(default_factory=list)
    addons: list[MobileAddonItemIn] = Field(default_factory=list)


class MobileGlobalAddonsReplace(BaseModel):
    """Replace the full list of mobile-wide add-ons (not tied to a vehicle type)."""

    items: list[MobileAddonItemIn] = Field(default_factory=list)


class MobilePromoIn(BaseModel):
    id: str | None = None
    code_name: str = ""
    discount_type: str = "flat"
    discount_value: float = 0
    validity_start: str = ""
    validity_end: str = ""
    max_uses_per_customer: int = 1
    applicable_service_ids: list[str] = Field(default_factory=list)
    applicable_vehicle_types: list[str] = Field(default_factory=list)


class MobileDayTimeRuleIn(BaseModel):
    id: str | None = None
    title: str = ""
    description: str = ""
    discount_type: str = "flat"
    discount_value: float = 0
    applicable_service_ids: list[str] = Field(default_factory=list)
    applicable_vehicle_types: list[str] = Field(default_factory=list)
    applicable_days: list[str] = Field(default_factory=list)
    time_window_start: str = ""
    time_window_end: str = ""
    validity_start: str = ""
    validity_end: str = ""


class MobileLoyaltyTierIn(BaseModel):
    id: str
    min_spend_in_window: float
    max_spend_in_window: float | None = None
    reward_service_id: str


class MobileLoyaltyProgramIn(BaseModel):
    qualifying_service_count: int = Field(default=10, ge=1)
    tiers: list[MobileLoyaltyTierIn] = Field(default_factory=list)


class MobileSlotSettingsPatch(BaseModel):
    slot_duration_minutes: int | None = Field(default=None, ge=15)
    open_time: str | None = None
    close_time: str | None = None
    slot_window_active_by_key: dict[str, bool] | None = None
    slot_driver_open_by_window: dict[str, list[bool]] | None = None
    slot_day_states: dict[str, dict] | None = None


class MobileBookingCreate(BaseModel):
    city_pin_code: str = ""
    customer_name: str
    phone: str
    address: str
    vehicle_summary: str = ""
    service_id: str | None = None
    vehicle_type: str = ""
    selected_addon_ids: list[str] = Field(default_factory=list)
    slot_date: str
    start_time: str
    end_time: str
    source: str = "online"
    notes: str = ""
    tip_cents: int = 0
    assigned_driver_id: str | None = None


class MobileBookingUpdate(BaseModel):
    assigned_driver_id: str | None = None
    status: str | None = None
    notes: str | None = None
    tip_cents: int | None = None

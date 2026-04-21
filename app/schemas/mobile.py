from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator


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
    catalog_group_id: str | None = None
    duration_minutes: int = Field(default=60, ge=30)

    @field_validator("duration_minutes")
    @classmethod
    def _snap_duration(cls, v: int) -> int:
        from app.services.duration_slots import snap_duration_to_base_slots

        return snap_duration_to_base_slots(v)


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

    @field_validator("code_name")
    @classmethod
    def _code_name_required(cls, v: str) -> str:
        out = v.strip()
        if not out:
            raise ValueError("code_name is required")
        return out

    @field_validator("discount_value")
    @classmethod
    def _discount_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("discount_value must be >= 0")
        return v

    @model_validator(mode="after")
    def _validate_dates_and_discount(self):
        if not self.validity_start or not self.validity_end:
            raise ValueError("validity_start and validity_end are required")
        start = date.fromisoformat(self.validity_start)
        end = date.fromisoformat(self.validity_end)
        today = date.today()
        if start < today:
            raise ValueError("validity_start cannot be in the past")
        if end < start:
            raise ValueError("validity_end must be on or after validity_start")
        if self.discount_type == "percentage" and self.discount_value >= 100:
            raise ValueError("percentage discount_value must be less than 100")
        return self


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
    end_time: str | None = None
    source: str = "online"
    notes: str = ""
    tip_cents: int = 0
    assigned_driver_id: str | None = None
    booking_id: str | None = None


class MobileBookingUpdate(BaseModel):
    assigned_driver_id: str | None = None
    status: str | None = None
    notes: str | None = None
    tip_cents: int | None = None
    customer_name: str | None = None
    phone: str | None = None
    address: str | None = None
    vehicle_type: str | None = None
    vehicle_summary: str | None = None
    service_id: str | None = None
    selected_addon_ids: list[str] | None = None
    slot_date: str | None = None
    start_time: str | None = None
    end_time: str | None = None

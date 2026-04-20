from pydantic import BaseModel, Field, field_validator


class ServiceItemIn(BaseModel):
    id: str | None = None
    name: str = ""
    price: float = 0
    free_coffee_count: int = 0
    eligible_for_loyalty_points: bool = True
    recommended: bool = False
    description_points: list[str] = Field(default_factory=list)
    active: bool = True
    catalog_group_id: str | None = None
    duration_minutes: int = Field(default=60, ge=30, description="Snapped up to a multiple of 30 minutes")

    @field_validator("duration_minutes")
    @classmethod
    def _snap_duration(cls, v: int) -> int:
        from app.services.duration_slots import snap_duration_to_base_slots

        return snap_duration_to_base_slots(v)


class AddonItemIn(BaseModel):
    id: str | None = None
    name: str = ""
    price: float = 0
    description_points: list[str] = Field(default_factory=list)
    active: bool = True


class VehicleBlockCreate(BaseModel):
    vehicle_type: str
    services: list[ServiceItemIn] = Field(default_factory=list)
    addons: list[AddonItemIn] = Field(default_factory=list)


class VehicleBlockOut(BaseModel):
    id: str
    branch_id: str
    vehicle_type: str
    services: list[dict]
    addons: list[dict]


class PromoIn(BaseModel):
    id: str | None = None
    code_name: str = ""
    discount_type: str = "flat"
    discount_value: float = 0
    validity_start: str = ""
    validity_end: str = ""
    max_uses_per_customer: int = 1
    applicable_service_ids: list[str] = Field(default_factory=list)
    applicable_vehicle_types: list[str] = Field(default_factory=list)


class DayTimeRuleIn(BaseModel):
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


class FreeCoffeeRuleIn(BaseModel):
    id: str | None = None
    kind: str = "on_service"
    service_name: str | None = None
    services_count: int | None = None
    notes: str = ""


class LoyaltyTierIn(BaseModel):
    id: str
    min_spend_in_window: float
    max_spend_in_window: float | None = None
    reward_service_id: str


class LoyaltyProgramIn(BaseModel):
    qualifying_service_count: int = Field(default=10, ge=1)
    tiers: list[LoyaltyTierIn] = Field(default_factory=list)


class SlotSettingsPatch(BaseModel):
    manager_slot_duration_minutes: int | None = Field(default=None, ge=15)
    slot_bay_open_by_window: dict[str, list[bool]] | None = None
    slot_window_active_by_key: dict[str, bool] | None = None
    slot_day_states: dict[str, dict] | None = None

from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator


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

    @field_validator("title")
    @classmethod
    def _title_required(cls, v: str) -> str:
        out = v.strip()
        if not out:
            raise ValueError("title is required")
        return out

    @field_validator("discount_value")
    @classmethod
    def _rule_discount_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("discount_value must be >= 0")
        return v

    @model_validator(mode="after")
    def _validate_rule(self):
        if not self.validity_start or not self.validity_end:
            raise ValueError("validity_start and validity_end are required")
        start = date.fromisoformat(self.validity_start)
        end = date.fromisoformat(self.validity_end)
        if end < start:
            raise ValueError("validity_end must be on or after validity_start")
        if self.discount_type == "percentage" and self.discount_value > 100:
            raise ValueError("percentage discount_value cannot exceed 100")
        return self


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
    manager_slot_duration_minutes: int | None = Field(default=None, ge=15, le=480)
    slot_bay_open_by_window: dict[str, list[bool]] | None = None
    slot_window_active_by_key: dict[str, bool] | None = None
    slot_day_states: dict[str, dict] | None = None

    @field_validator("manager_slot_duration_minutes")
    @classmethod
    def _validate_slot_duration(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if v % 15 != 0:
            raise ValueError("manager_slot_duration_minutes must be in 15-minute increments")
        return v

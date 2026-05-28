from pydantic import BaseModel, Field, field_validator


class BookingCreate(BaseModel):
    customer_name: str
    phone: str
    # Guest / walk-in contact email (registered users use CustomerUser.email).
    customer_email: str = ""
    address: str = ""
    vehicle_type: str
    vehicle_model: str = ""
    registration_number: str = ""
    service_summary: str
    service_id: str | None = None
    selected_addon_ids: list[str] = Field(default_factory=list)
    slot_date: str
    start_time: str
    # If omitted, end is computed from ``service_id`` duration + ``selected_addon_ids`` (+30 min each).
    end_time: str | None = None
    source: str = "online"
    tip_cents: int = Field(default=0, ge=0, le=50_000)
    """Service + add-ons total in cents after discounts (excl. tip), as shown at checkout."""
    service_charged_cents: int | None = Field(default=None, ge=0, le=5_000_000)
    notes: str = ""
    manager_notes: str = ""
    bay_number: int | None = None
    assigned_washer_id: str | None = None
    """Optional client-generated id so the portal can sync without replacing booking keys."""
    booking_id: str | None = None
    customer_id: str | None = None
    loyalty_reward_id: str | None = None
    promo_code: str | None = None

    @field_validator("customer_name", "phone", "vehicle_type", "service_summary", "slot_date", "start_time")
    @classmethod
    def _required_non_blank(cls, v: str) -> str:
        out = v.strip()
        if not out:
            raise ValueError("field is required")
        return out


class BookingUpdate(BaseModel):
    status: str | None = None
    assigned_washer_id: str | None = None
    bay_number: int | None = None
    notes: str | None = None
    manager_notes: str | None = None
    customer_name: str | None = None
    phone: str | None = None
    address: str | None = None
    vehicle_type: str | None = None
    vehicle_model: str | None = None
    registration_number: str | None = None
    service_summary: str | None = None
    service_id: str | None = None
    selected_addon_ids: list[str] | None = None
    slot_date: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    tip_cents: int | None = Field(default=None, ge=0, le=50_000)

    @field_validator("customer_name", "phone", "vehicle_type", "service_summary", "slot_date", "start_time", "end_time")
    @classmethod
    def _optional_non_blank(cls, v: str | None) -> str | None:
        if v is None:
            return None
        out = v.strip()
        if not out:
            raise ValueError("field cannot be blank")
        return out


class BookingOut(BaseModel):
    id: str
    branch_id: str
    customer_name: str
    address: str
    phone: str
    customer_email: str = ""
    vehicle_type: str
    vehicle_model: str = ""
    registration_number: str = ""
    service_summary: str
    service_id: str | None = None
    selected_addon_ids: list[str] = Field(default_factory=list)
    duration_minutes: int = 0
    slot_date: str
    start_time: str
    end_time: str
    bay_number: int | None
    assigned_washer_id: str | None
    status: str
    source: str
    notes: str
    tip_cents: int = 0

    model_config = {"from_attributes": True}

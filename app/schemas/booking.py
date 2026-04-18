from pydantic import BaseModel, Field


class BookingCreate(BaseModel):
    customer_name: str
    phone: str
    address: str = ""
    vehicle_type: str
    service_summary: str
    service_id: str | None = None
    slot_date: str
    start_time: str
    end_time: str
    source: str = "online"
    tip_cents: int = Field(default=0, ge=0, le=50_000)


class BookingUpdate(BaseModel):
    status: str | None = None
    assigned_washer_id: str | None = None
    bay_number: int | None = None
    notes: str | None = None


class BookingOut(BaseModel):
    id: str
    branch_id: str
    customer_name: str
    address: str
    phone: str
    vehicle_type: str
    service_summary: str
    service_id: str | None = None
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

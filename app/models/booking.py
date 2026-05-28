from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class BranchBooking(Base, TimestampMixin):
    __tablename__ = "branch_bookings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    customer_id: Mapped[str | None] = mapped_column(String(36), index=True, nullable=True)
    customer_name: Mapped[str] = mapped_column(String(255), default="")
    address: Mapped[str] = mapped_column(Text, default="")
    phone: Mapped[str] = mapped_column(String(64), default="")
    customer_email: Mapped[str] = mapped_column(String(320), nullable=False, default="")
    vehicle_type: Mapped[str] = mapped_column(String(128), default="")
    vehicle_model: Mapped[str] = mapped_column(String(255), default="")
    registration_number: Mapped[str] = mapped_column(String(64), default="")
    service_summary: Mapped[str] = mapped_column(Text, default="")
    # Primary catalog service id when booked — used for loyalty eligibility and price.
    service_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    selected_addon_ids_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    slot_date: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    start_time: Mapped[str] = mapped_column(String(8), nullable=False)
    end_time: Mapped[str] = mapped_column(String(8), nullable=False)
    bay_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    assigned_washer_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="online")
    notes: Mapped[str] = mapped_column(Text, default="")
    tip_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payment_method: Mapped[str] = mapped_column(String(32), nullable=False, default="cash")
    promo_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    promo_discount_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Service + add-ons total charged at booking (after promos/offers/loyalty), excluding tip."""
    service_charged_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Audit tracking — set on every status change so all portals have a shared source of truth.
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_by: Mapped[str | None] = mapped_column(String(36), nullable=True)
    updated_by: Mapped[str | None] = mapped_column(String(36), nullable=True)
    updated_by_role: Mapped[str | None] = mapped_column(String(32), nullable=True)

    branch = relationship("Branch", back_populates="bookings")

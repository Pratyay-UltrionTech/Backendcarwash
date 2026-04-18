from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class BranchLoyalty(Base, TimestampMixin):
    __tablename__ = "branch_loyalty"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("branches.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    qualifying_service_count: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    tiers_json: Mapped[str] = mapped_column(Text, default="[]")

    branch = relationship("Branch", back_populates="loyalty")


class LoyaltyLedgerEntry(Base):
    """One row per completed booking that counted toward loyalty (eligible primary service only)."""

    __tablename__ = "loyalty_ledger_entries"
    __table_args__ = (UniqueConstraint("channel", "booking_id", name="uq_loyalty_ledger_channel_booking"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    channel: Mapped[str] = mapped_column(String(16), nullable=False, index=True)  # branch | mobile
    branch_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    city_pin_code: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    customer_phone_normalized: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    booking_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    service_id: Mapped[str] = mapped_column(String(36), nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

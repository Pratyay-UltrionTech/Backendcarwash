from sqlalchemy import Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class Promotion(Base, TimestampMixin):
    __tablename__ = "promotions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    code_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    discount_type: Mapped[str] = mapped_column(String(16), nullable=False, default="flat")
    discount_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    validity_start: Mapped[str] = mapped_column(String(32), default="")
    validity_end: Mapped[str] = mapped_column(String(32), default="")
    max_uses_per_customer: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    applicable_service_ids: Mapped[str] = mapped_column(Text, default="[]")
    applicable_vehicle_types: Mapped[str] = mapped_column(Text, default="[]")

    branch = relationship("Branch", back_populates="promotions")


class DayTimePriceRule(Base, TimestampMixin):
    __tablename__ = "day_time_price_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    discount_type: Mapped[str] = mapped_column(String(16), nullable=False, default="flat")
    discount_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    applicable_service_ids: Mapped[str] = mapped_column(Text, default="[]")
    applicable_vehicle_types: Mapped[str] = mapped_column(Text, default="[]")
    applicable_days: Mapped[str] = mapped_column(Text, default="[]")
    time_window_start: Mapped[str] = mapped_column(String(8), default="")
    time_window_end: Mapped[str] = mapped_column(String(8), default="")
    validity_start: Mapped[str] = mapped_column(String(32), default="")
    validity_end: Mapped[str] = mapped_column(String(32), default="")

    branch = relationship("Branch", back_populates="day_time_rules")


class FreeCoffeeRule(Base, TimestampMixin):
    __tablename__ = "free_coffee_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="on_service")
    service_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    services_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notes: Mapped[str] = mapped_column(Text, default="")

    branch = relationship("Branch", back_populates="free_coffee_rules")

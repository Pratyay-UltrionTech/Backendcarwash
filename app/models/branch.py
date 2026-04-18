from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class Branch(Base, TimestampMixin):
    __tablename__ = "branches"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    location: Mapped[str] = mapped_column(Text, nullable=False, default="")
    zip_code: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    bay_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    open_time: Mapped[str] = mapped_column(String(8), nullable=False, default="09:00")
    close_time: Mapped[str] = mapped_column(String(8), nullable=False, default="18:00")

    managers = relationship("BranchManager", back_populates="branch", cascade="all, delete-orphan")
    washers = relationship("Washer", back_populates="branch", cascade="all, delete-orphan")
    vehicle_blocks = relationship(
        "VehicleCatalogBlock", back_populates="branch", cascade="all, delete-orphan"
    )
    branch_addons = relationship("BranchAddonItem", back_populates="branch", cascade="all, delete-orphan")
    promotions = relationship("Promotion", back_populates="branch", cascade="all, delete-orphan")
    day_time_rules = relationship(
        "DayTimePriceRule", back_populates="branch", cascade="all, delete-orphan"
    )
    free_coffee_rules = relationship(
        "FreeCoffeeRule", back_populates="branch", cascade="all, delete-orphan"
    )
    loyalty = relationship(
        "BranchLoyalty", back_populates="branch", uselist=False, cascade="all, delete-orphan"
    )
    slot_settings = relationship(
        "BranchSlotSettings", back_populates="branch", uselist=False, cascade="all, delete-orphan"
    )
    bookings = relationship("BranchBooking", back_populates="branch", cascade="all, delete-orphan")

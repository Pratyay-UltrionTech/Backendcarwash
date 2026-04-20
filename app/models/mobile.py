from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class MobileServiceManager(Base, TimestampMixin):
    __tablename__ = "mobile_service_managers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    city_pin_code: Mapped[str] = mapped_column(String(16), unique=True, index=True, nullable=False)
    emp_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    address: Mapped[str] = mapped_column(Text, nullable=False, default="")
    zip_code: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    email: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    mobile: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    doj: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    login_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    drivers = relationship("MobileServiceDriver", back_populates="manager", cascade="all, delete-orphan")
    slot_settings = relationship(
        "MobileSlotSettings", back_populates="manager", uselist=False, cascade="all, delete-orphan"
    )
    bookings = relationship("MobileBooking", back_populates="manager", cascade="all, delete-orphan")


class MobileServiceDriver(Base, TimestampMixin):
    __tablename__ = "mobile_service_drivers"
    __table_args__ = (UniqueConstraint("city_pin_code", "login_id", name="uq_mobile_driver_city_login"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    manager_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("mobile_service_managers.id", ondelete="CASCADE"), index=True
    )
    city_pin_code: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    service_pin_code: Mapped[str] = mapped_column(String(16), index=True, nullable=False, default="")
    emp_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    address: Mapped[str] = mapped_column(Text, nullable=False, default="")
    zip_code: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    serviceable_zip_codes_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    email: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    mobile: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    doj: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    login_id: Mapped[str] = mapped_column(String(128), nullable=False, default="", index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    manager = relationship("MobileServiceManager", back_populates="drivers")
    bookings = relationship("MobileBooking", back_populates="driver")


class MobileVehicleCatalogBlock(Base, TimestampMixin):
    __tablename__ = "mobile_vehicle_catalog_blocks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    vehicle_type: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    services = relationship(
        "MobileCatalogServiceItem", back_populates="vehicle_block", cascade="all, delete-orphan"
    )
    addons = relationship(
        "MobileCatalogAddonItem", back_populates="vehicle_block", cascade="all, delete-orphan"
    )


class MobileCatalogServiceItem(Base, TimestampMixin):
    __tablename__ = "mobile_catalog_service_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    vehicle_block_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("mobile_vehicle_catalog_blocks.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    free_coffee_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    eligible_for_loyalty_points: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    recommended: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    description_points: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    catalog_group_id: Mapped[str | None] = mapped_column(String(36), nullable=True, default=None)
    duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)

    vehicle_block = relationship("MobileVehicleCatalogBlock", back_populates="services")


class MobileCatalogAddonItem(Base, TimestampMixin):
    __tablename__ = "mobile_catalog_addon_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    vehicle_block_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("mobile_vehicle_catalog_blocks.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    description_points: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    vehicle_block = relationship("MobileVehicleCatalogBlock", back_populates="addons")


class MobileGlobalAddonItem(Base, TimestampMixin):
    """Mobile add-ons shared across all vehicle types (like branch-wide branch_addon_items)."""

    __tablename__ = "mobile_global_addon_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    description_points: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class MobilePromotion(Base, TimestampMixin):
    __tablename__ = "mobile_promotions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    code_name: Mapped[str] = mapped_column(String(128), nullable=False, default="", unique=True, index=True)
    discount_type: Mapped[str] = mapped_column(String(32), nullable=False, default="flat")
    discount_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    validity_start: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    validity_end: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    max_uses_per_customer: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    applicable_service_ids: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    applicable_vehicle_types: Mapped[str] = mapped_column(Text, nullable=False, default="[]")


class MobileDayTimePriceRule(Base, TimestampMixin):
    __tablename__ = "mobile_day_time_price_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    discount_type: Mapped[str] = mapped_column(String(32), nullable=False, default="flat")
    discount_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    applicable_service_ids: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    applicable_vehicle_types: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    applicable_days: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    time_window_start: Mapped[str] = mapped_column(String(8), nullable=False, default="")
    time_window_end: Mapped[str] = mapped_column(String(8), nullable=False, default="")
    validity_start: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    validity_end: Mapped[str] = mapped_column(String(16), nullable=False, default="")


class MobileLoyaltyProgram(Base, TimestampMixin):
    __tablename__ = "mobile_loyalty_programs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    qualifying_service_count: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    tiers_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")


class MobileSlotSettings(Base, TimestampMixin):
    __tablename__ = "mobile_slot_settings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    manager_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("mobile_service_managers.id", ondelete="CASCADE"), unique=True, index=True
    )
    city_pin_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    slot_duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    open_time: Mapped[str] = mapped_column(String(8), nullable=False, default="08:00")
    close_time: Mapped[str] = mapped_column(String(8), nullable=False, default="18:00")
    slot_window_active_by_key_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    slot_driver_open_by_window_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    slot_day_states_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    manager = relationship("MobileServiceManager", back_populates="slot_settings")


class MobileBooking(Base, TimestampMixin):
    __tablename__ = "mobile_bookings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    manager_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("mobile_service_managers.id", ondelete="CASCADE"), index=True
    )
    city_pin_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    address: Mapped[str] = mapped_column(Text, nullable=False, default="")
    phone: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    vehicle_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    service_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    vehicle_type: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    selected_addon_ids_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    slot_date: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    start_time: Mapped[str] = mapped_column(String(8), nullable=False)
    end_time: Mapped[str] = mapped_column(String(8), nullable=False)
    assigned_driver_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("mobile_service_drivers.id", ondelete="SET NULL"), nullable=True, index=True
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="online")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
    tip_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    manager = relationship("MobileServiceManager", back_populates="bookings")
    driver = relationship("MobileServiceDriver", back_populates="bookings")

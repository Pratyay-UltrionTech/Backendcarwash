from sqlalchemy import Boolean, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class VehicleCatalogBlock(Base, TimestampMixin):
    __tablename__ = "vehicle_catalog_blocks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    vehicle_type: Mapped[str] = mapped_column(String(128), nullable=False)

    services = relationship(
        "CatalogServiceItem", back_populates="vehicle_block", cascade="all, delete-orphan"
    )
    addons = relationship("CatalogAddonItem", back_populates="vehicle_block", cascade="all, delete-orphan")
    branch = relationship("Branch", back_populates="vehicle_blocks")


class CatalogServiceItem(Base, TimestampMixin):
    __tablename__ = "catalog_service_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    vehicle_block_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("vehicle_catalog_blocks.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    free_coffee_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    eligible_for_loyalty_points: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    recommended: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    description_points: Mapped[str] = mapped_column(Text, default="[]")  # JSON array as text
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    vehicle_block = relationship("VehicleCatalogBlock", back_populates="services")


class CatalogAddonItem(Base, TimestampMixin):
    __tablename__ = "catalog_addon_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    vehicle_block_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("vehicle_catalog_blocks.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    description_points: Mapped[str] = mapped_column(Text, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    vehicle_block = relationship("VehicleCatalogBlock", back_populates="addons")


class BranchAddonItem(Base, TimestampMixin):
    __tablename__ = "branch_addon_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    description_points: Mapped[str] = mapped_column(Text, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    branch = relationship("Branch", back_populates="branch_addons")

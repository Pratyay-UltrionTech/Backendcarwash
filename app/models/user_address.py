from sqlalchemy import Boolean, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, new_id


class UserAddress(Base, TimestampMixin):
    """Saved address for a customer user account."""

    __tablename__ = "user_addresses"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    label: Mapped[str] = mapped_column(String(64), default="Home", nullable=False)
    street_address: Mapped[str] = mapped_column(Text, default="", nullable=False)
    suburb: Mapped[str] = mapped_column(String(128), default="", nullable=False)
    state: Mapped[str] = mapped_column(String(32), default="", nullable=False)
    postcode: Mapped[str] = mapped_column(String(10), default="", nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

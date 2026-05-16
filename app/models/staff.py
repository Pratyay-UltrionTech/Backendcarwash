from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class BranchManager(Base, TimestampMixin):
    __tablename__ = "branch_managers"
    __table_args__ = (UniqueConstraint("branch_id", "login_id", name="uq_manager_branch_login"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    address: Mapped[str] = mapped_column(Text, default="")
    zip_code: Mapped[str] = mapped_column(String(32), default="")
    email: Mapped[str] = mapped_column(String(255), default="")
    phone: Mapped[str] = mapped_column(String(64), default="")
    doj: Mapped[str] = mapped_column(String(32), default="")
    login_id: Mapped[str] = mapped_column(String(128), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    branch = relationship("Branch", back_populates="managers")


class Washer(Base, TimestampMixin):
    __tablename__ = "branch_washers"
    __table_args__ = (UniqueConstraint("branch_id", "login_id", name="uq_washer_branch_login"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    address: Mapped[str] = mapped_column(Text, default="")
    zip_code: Mapped[str] = mapped_column(String(32), default="")
    email: Mapped[str] = mapped_column(String(255), default="")
    phone: Mapped[str] = mapped_column(String(64), default="")
    doj: Mapped[str] = mapped_column(String(32), default="")
    login_id: Mapped[str] = mapped_column(String(128), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    assigned_bay: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    branch = relationship("Branch", back_populates="washers")
    unavailability = relationship("WasherUnavailability", back_populates="washer", cascade="all, delete-orphan")


class WasherUnavailability(Base, TimestampMixin):
    __tablename__ = "washer_unavailability"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    washer_id: Mapped[str] = mapped_column(String(36), ForeignKey("branch_washers.id", ondelete="CASCADE"), index=True)
    date: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    all_day: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    start_time: Mapped[str] = mapped_column(String(8), nullable=False, default="")
    end_time: Mapped[str] = mapped_column(String(8), nullable=False, default="")

    washer = relationship("Washer", back_populates="unavailability")


class WasherLeaveRequest(Base, TimestampMixin):
    __tablename__ = "washer_leave_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id", ondelete="CASCADE"), index=True)
    washer_id: Mapped[str] = mapped_column(String(36), ForeignKey("branch_washers.id", ondelete="CASCADE"), index=True)
    leave_date: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    leave_type: Mapped[str] = mapped_column(String(32), nullable=False, default="full_day")
    start_time: Mapped[str] = mapped_column(String(8), nullable=False, default="")
    end_time: Mapped[str] = mapped_column(String(8), nullable=False, default="")
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    reviewed_by_manager_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    reviewed_at: Mapped[str | None] = mapped_column(String(32), nullable=True)

    washer = relationship("Washer", foreign_keys=[washer_id])

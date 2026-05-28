from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, new_id


class BranchSlotSettings(Base, TimestampMixin):
    __tablename__ = "branch_slot_settings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    branch_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("branches.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    manager_slot_duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    slot_bay_open_by_window_json: Mapped[str] = mapped_column(Text, default="{}")
    slot_window_active_by_key_json: Mapped[str] = mapped_column(Text, default="{}")
    slot_day_states_json: Mapped[str] = mapped_column(Text, default="{}")

    branch = relationship("Branch", back_populates="slot_settings")

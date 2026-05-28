"""AdminAccount — DB-stored admin users (separate from the env-based super admin)."""
from __future__ import annotations

import uuid

from sqlalchemy import Boolean, Column, DateTime, String, func

from app.models.base import Base


class AdminAccount(Base):
    __tablename__ = "admin_accounts"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(320), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

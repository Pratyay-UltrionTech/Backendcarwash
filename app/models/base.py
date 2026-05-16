import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import string
import random



class Base(DeclarativeBase):
    pass


def new_id() -> str:
    return str(uuid.uuid4())


def new_customer_id() -> str:
    # 5 random alphanumeric characters
    chars = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return f"CUST_{chars}"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

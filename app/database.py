from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.models.base import Base

settings = get_settings()
# Shorter fail than OS default; sslmode for Azure is set on the URL in Settings when applicable.
_engine_kwargs: dict = {"pool_pre_ping": True, "echo": False}
if "database.azure.com" in settings.sqlalchemy_database_uri().lower():
    _engine_kwargs["connect_args"] = {"connect_timeout": 15}

engine = create_engine(settings.sqlalchemy_database_uri(), **_engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

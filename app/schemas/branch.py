import re

from pydantic import BaseModel, Field, field_validator

MAX_BAY_COUNT = 50
ZIP_RE = re.compile(r"^\d{5,6}$")


class BranchCreate(BaseModel):
    name: str
    location: str
    zip_code: str
    bay_count: int = Field(default=1, ge=1, le=MAX_BAY_COUNT)
    open_time: str = "09:00"
    close_time: str = "18:00"

    @field_validator("name", "location")
    @classmethod
    def _non_blank(cls, v: str) -> str:
        out = v.strip()
        if not out:
            raise ValueError("value is required")
        return out

    @field_validator("zip_code")
    @classmethod
    def _zip_format(cls, v: str) -> str:
        out = v.strip()
        if not ZIP_RE.fullmatch(out):
            raise ValueError("zip_code must be 5 or 6 digits")
        return out


class BranchUpdate(BaseModel):
    name: str | None = None
    location: str | None = None
    zip_code: str | None = None
    bay_count: int | None = Field(default=None, ge=1, le=MAX_BAY_COUNT)
    open_time: str | None = None
    close_time: str | None = None

    @field_validator("name", "location")
    @classmethod
    def _non_blank_optional(cls, v: str | None) -> str | None:
        if v is None:
            return None
        out = v.strip()
        if not out:
            raise ValueError("value cannot be blank")
        return out

    @field_validator("zip_code")
    @classmethod
    def _zip_format_optional(cls, v: str | None) -> str | None:
        if v is None:
            return None
        out = v.strip()
        if not ZIP_RE.fullmatch(out):
            raise ValueError("zip_code must be 5 or 6 digits")
        return out


class BranchOut(BaseModel):
    id: str
    name: str
    location: str
    zip_code: str
    bay_count: int
    open_time: str
    close_time: str

    model_config = {"from_attributes": True}

from pydantic import BaseModel, Field


class BranchCreate(BaseModel):
    name: str
    location: str = ""
    zip_code: str = ""
    bay_count: int = Field(default=1, ge=1)
    open_time: str = "09:00"
    close_time: str = "18:00"


class BranchUpdate(BaseModel):
    name: str | None = None
    location: str | None = None
    zip_code: str | None = None
    bay_count: int | None = Field(default=None, ge=1)
    open_time: str | None = None
    close_time: str | None = None


class BranchOut(BaseModel):
    id: str
    name: str
    location: str
    zip_code: str
    bay_count: int
    open_time: str
    close_time: str

    model_config = {"from_attributes": True}

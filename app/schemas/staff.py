from pydantic import BaseModel, Field


class ManagerCreate(BaseModel):
    name: str = ""
    address: str = ""
    zip_code: str = ""
    email: str = ""
    phone: str = ""
    doj: str = ""
    login_id: str
    password: str
    active: bool = True


class ManagerUpdate(BaseModel):
    name: str | None = None
    address: str | None = None
    zip_code: str | None = None
    email: str | None = None
    phone: str | None = None
    doj: str | None = None
    login_id: str | None = None
    password: str | None = None
    active: bool | None = None


class ManagerOut(BaseModel):
    id: str
    branch_id: str
    name: str
    address: str
    zip_code: str
    email: str
    phone: str
    doj: str
    login_id: str
    active: bool

    model_config = {"from_attributes": True}


class WasherCreate(BaseModel):
    name: str = ""
    address: str = ""
    zip_code: str = ""
    email: str = ""
    phone: str = ""
    doj: str = ""
    login_id: str
    password: str
    assigned_bay: int = Field(default=1, ge=1)
    active: bool = True


class WasherUpdate(BaseModel):
    name: str | None = None
    address: str | None = None
    zip_code: str | None = None
    email: str | None = None
    phone: str | None = None
    doj: str | None = None
    login_id: str | None = None
    password: str | None = None
    assigned_bay: int | None = Field(default=None, ge=1)
    active: bool | None = None


class WasherOut(BaseModel):
    id: str
    branch_id: str
    name: str
    address: str
    zip_code: str
    email: str
    phone: str
    doj: str
    login_id: str
    assigned_bay: int
    active: bool

    model_config = {"from_attributes": True}

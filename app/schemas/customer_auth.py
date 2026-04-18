from pydantic import BaseModel, EmailStr, Field

from app.schemas.auth import TokenResponse


class CustomerRegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)


class CustomerLoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1, max_length=128)


class CustomerVehicleIn(BaseModel):
    type: str = Field(..., min_length=1, max_length=64)
    number: str = Field(..., min_length=1, max_length=64)


class CustomerProfileUpdate(BaseModel):
    full_name: str = Field(..., min_length=1, max_length=255)
    phone: str = Field(..., min_length=1, max_length=64)
    address: str = Field(..., min_length=1, max_length=4000)
    vehicles: list[CustomerVehicleIn] = Field(default_factory=list)


class CustomerAuthResponse(TokenResponse):
    email: str
    profile_completed: bool
    full_name: str = ""
    phone: str = ""
    address: str = ""

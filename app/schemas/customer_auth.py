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
    number: str = Field(default="", min_length=0, max_length=64)
    model: str = Field(default="", max_length=255)
    last_used: str | None = Field(default=None)


class CustomerProfileUpdate(BaseModel):
    full_name: str = Field(..., min_length=1, max_length=255)
    phone: str = Field(..., min_length=1, max_length=64)
    # Address is managed via /customer/addresses; accept empty string for backward compat.
    address: str = Field(default="", max_length=4000)
    vehicles: list[CustomerVehicleIn] = Field(default_factory=list)
    email: EmailStr | None = Field(
        default=None,
        description="New sign-in email. Omit or null to keep the current email.",
    )
    # Signed token returned by POST /auth/customer/verify-email-change.
    # Required when `email` is set to a new address — replaces the fragile
    # in-memory OTP check so the flow works across multiple workers.
    email_change_token: str | None = Field(
        default=None,
        description="Short-lived JWT from /auth/customer/verify-email-change.",
    )


class CustomerAuthResponse(TokenResponse):
    member_id: str
    email: str
    profile_completed: bool
    full_name: str = ""
    phone: str = ""
    address: str = ""
    vehicles: list[CustomerVehicleIn] = []

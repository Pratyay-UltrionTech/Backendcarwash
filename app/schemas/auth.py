from pydantic import BaseModel, Field


class AdminLoginRequest(BaseModel):
    username: str
    password: str


class ManagerLoginRequest(BaseModel):
    branch_id: str
    login_id: str
    password: str


class WasherLoginRequest(BaseModel):
    branch_id: str
    login_id: str
    password: str


class MobileManagerLoginRequest(BaseModel):
    city_pin_code: str
    login_id: str
    password: str


class MobileWasherLoginRequest(BaseModel):
    """city_pin_code is optional; omit or leave empty to sign in with login_id + password only."""

    city_pin_code: str = ""
    login_id: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MobileWasherTokenResponse(TokenResponse):
    """Returned by mobile washer login so the client can store the manager city PIN without asking the user."""

    city_pin_code: str = ""

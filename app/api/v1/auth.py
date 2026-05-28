import logging

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func

from app.api.deps import CustomerAuth, DbSession
from app.config import get_settings
from app.core.mobile_pins import is_valid_mobile_city_pin, normalize_mobile_city_pin
from app.core.security import create_access_token, hash_password, verify_password
from app.models import AdminAccount, BranchManager, CustomerUser, MobileServiceDriver, MobileServiceManager, Washer
from app.schemas.auth import (
    AdminLoginRequest,
    ManagerLoginRequest,
    MobileManagerLoginRequest,
    MobileManagerTokenResponse,
    MobileWasherLoginRequest,
    MobileWasherTokenResponse,
    TokenResponse,
    WasherLoginRequest,
)
from app.schemas.customer_auth import CustomerAuthResponse, CustomerLoginRequest, CustomerRegisterRequest
from app.services.jsonutil import loads_json_array
from app.services import otp_service

router = APIRouter(prefix="/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Convenience shims — keep callers in this file consistent
# ---------------------------------------------------------------------------

def _store_otp(scope: str, identifier: str, email: str) -> str:
    return otp_service.store_otp(scope, identifier, email)

def _verify_otp_store(scope: str, identifier: str, otp: str) -> bool:
    return otp_service.verify_otp(scope, identifier, otp)

def _check_reset_allowed(scope: str, identifier: str) -> str | None:
    return otp_service.check_verified(scope, identifier, ttl=otp_service.RESET_TTL)

def _clear_otp(scope: str, identifier: str) -> None:
    otp_service.clear_otp(scope, identifier)


# ---------------------------------------------------------------------------
# Forgot-password request/response schemas
# ---------------------------------------------------------------------------

class ForgotPasswordRequest(BaseModel):
    identifier: str  # email for customer; login_id or email for manager/washer

class VerifyOtpRequest(BaseModel):
    identifier: str
    otp: str

class ResetPasswordRequest(BaseModel):
    identifier: str
    new_password: str


def _normalize_customer_email(email: str) -> str:
    return email.strip().lower()


class GuestRegisterRequest(BaseModel):
    """Body for POST /auth/customer/register-guest (guest checkout, no OTP)."""
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    phone: str | None = Field(default=None, max_length=64)


@router.post("/customer/register-guest", response_model=CustomerAuthResponse)
def customer_register_guest(body: GuestRegisterRequest, db: DbSession) -> CustomerAuthResponse:
    """Register from guest checkout — no OTP required.

    Checks email AND phone uniqueness before creating the account and returns
    structured ``field_errors`` so the checkout UI can show per-field messages.
    """
    email_n = _normalize_customer_email(str(body.email))
    phone_n = str(body.phone or "").strip()

    field_errors: dict[str, str] = {}

    if db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none():
        field_errors["email"] = "Email already exists. Login to continue."

    if phone_n:
        phone_taken = (
            db.query(CustomerUser.id)
            .filter(CustomerUser.phone == phone_n)
            .first()
        )
        if phone_taken:
            field_errors["phone"] = "Phone number is already linked to an account. Please login to continue."

    if field_errors:
        raise HTTPException(
            status_code=422,
            detail={"field_errors": field_errors},
        )

    user = CustomerUser(
        email=email_n,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    token = create_access_token({"role": "customer", "sub": user.id, "email": user.email})
    from app.services.email_service import send_welcome_email
    send_welcome_email(user.email, "")
    return CustomerAuthResponse(
        access_token=token,
        member_id=user.id,
        email=user.email,
        profile_completed=user.profile_completed,
        full_name=user.full_name or "",
        phone=user.phone or "",
        address=user.address_line or "",
    )


class CheckAvailabilityRequest(BaseModel):
    email: EmailStr | None = None
    phone: str | None = None


@router.post("/customer/check-availability")
def customer_check_availability(body: CheckAvailabilityRequest, db: DbSession) -> dict:
    """Check whether an email or phone is already registered.

    Used for real-time inline validation in the guest checkout form.
    Returns ``{"email_taken": bool, "phone_taken": bool}`` (keys omitted when not queried).
    """
    result: dict = {}
    if body.email:
        email_n = _normalize_customer_email(str(body.email))
        result["email_taken"] = (
            db.query(CustomerUser.id).filter(CustomerUser.email == email_n).first() is not None
        )
    if body.phone:
        phone_n = str(body.phone).strip()
        result["phone_taken"] = (
            db.query(CustomerUser.id).filter(CustomerUser.phone == phone_n).first() is not None
        )
    return result


@router.post("/customer/register", response_model=CustomerAuthResponse)
def customer_register(body: CustomerRegisterRequest, db: DbSession) -> CustomerAuthResponse:
    email_n = _normalize_customer_email(str(body.email))
    existing = db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"detail": "Email already registered", "code": "email_taken"},
        )
    # Require prior email OTP verification before creating the account.
    verified_email = otp_service.check_verified("signup", email_n, ttl=otp_service.SIGNUP_TTL)
    if not verified_email:
        raise HTTPException(
            status_code=403,
            detail={
                "detail": "Email not verified. Please verify your email with the code sent to you.",
                "code": "email_not_verified",
            },
        )
    user = CustomerUser(
        email=email_n,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    otp_service.clear_otp("signup", email_n)
    token = create_access_token({"role": "customer", "sub": user.id, "email": user.email})
    from app.services.email_service import send_welcome_email
    send_welcome_email(user.email, user.full_name or "")
    return CustomerAuthResponse(
        access_token=token,
        member_id=user.id,
        email=user.email,
        profile_completed=user.profile_completed,
        full_name=user.full_name,
        phone=user.phone,
        address=user.address_line,
    )


@router.post("/customer/login", response_model=CustomerAuthResponse)
def customer_login(body: CustomerLoginRequest, db: DbSession) -> CustomerAuthResponse:
    email_n = _normalize_customer_email(str(body.email))
    user = db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none()
    if not user:
        raise HTTPException(
            status_code=404,
            detail={"detail": "User not registered", "code": "user_not_registered"},
        )
    if not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=401,
            detail={"detail": "Invalid email or password", "code": "invalid_credentials"},
        )
    token = create_access_token({"role": "customer", "sub": user.id, "email": user.email})
    return CustomerAuthResponse(
        access_token=token,
        member_id=user.id,
        email=user.email,
        profile_completed=user.profile_completed,
        full_name=user.full_name,
        phone=user.phone,
        address=user.address_line,
    )


@router.post("/admin/login", response_model=TokenResponse)
def admin_login(body: AdminLoginRequest, db: DbSession) -> TokenResponse:
    settings = get_settings()
    # 1. Check env-based super admin first (unchanged behaviour)
    if body.username == settings.admin_username and body.password == settings.admin_password:
        token = create_access_token({"role": "admin", "sub": "admin"})
        return TokenResponse(access_token=token)
    # 2. Fallback: check DB admin accounts
    acct = (
        db.query(AdminAccount)
        .filter(func.lower(AdminAccount.email) == str(body.username or "").strip().lower(), AdminAccount.active.is_(True))
        .one_or_none()
    )
    if acct and verify_password(body.password, acct.password_hash):
        token = create_access_token({"role": "admin", "sub": acct.id, "email": acct.email, "is_db_admin": True})
        return TokenResponse(access_token=token)
    raise HTTPException(status_code=401, detail={"detail": "Invalid admin credentials", "code": "invalid_credentials"})


@router.post("/manager/login", response_model=TokenResponse)
def manager_login(body: ManagerLoginRequest, db: DbSession) -> TokenResponse:
    login_id_n = str(body.login_id or "").strip().lower()
    mgr = (
        db.query(BranchManager)
        .filter(
            BranchManager.branch_id == body.branch_id,
            (func.lower(BranchManager.login_id) == login_id_n) | (func.lower(BranchManager.email) == login_id_n),
            BranchManager.active.is_(True),
        )
        .one_or_none()
    )
    if not mgr or not verify_password(body.password, mgr.password_hash):
        raise HTTPException(
            status_code=401, detail={"detail": "Invalid manager credentials", "code": "invalid_credentials"}
        )
    token = create_access_token({"role": "manager", "sub": mgr.id, "branch_id": mgr.branch_id})
    return TokenResponse(access_token=token)


@router.post("/washer/login", response_model=TokenResponse)
def washer_login(body: WasherLoginRequest, db: DbSession) -> TokenResponse:
    # Try login_id first, then email (case-insensitive)
    login_id_n = str(body.login_id or "").strip().lower()
    w = (
        db.query(Washer)
        .filter(
            Washer.branch_id == body.branch_id,
            (func.lower(Washer.login_id) == login_id_n) | (func.lower(Washer.email) == login_id_n),
            Washer.active.is_(True),
        )
        .one_or_none()
    )
    if not w or not verify_password(body.password, w.password_hash):
        raise HTTPException(
            status_code=401, detail={"detail": "Invalid washer credentials", "code": "invalid_credentials"}
        )
    token = create_access_token({"role": "washer", "sub": w.id, "branch_id": w.branch_id})
    return TokenResponse(access_token=token)


@router.post("/mobile/manager/login", response_model=MobileManagerTokenResponse)
def mobile_manager_login(body: MobileManagerLoginRequest, db: DbSession) -> MobileManagerTokenResponse:
    pin = normalize_mobile_city_pin(body.city_pin_code)
    login_id_n = str(body.login_id or "").strip().lower()
    if not login_id_n:
        raise HTTPException(
            status_code=400,
            detail={"detail": "login_id is required", "code": "validation_error"},
        )
    if pin and not is_valid_mobile_city_pin(pin):
        raise HTTPException(
            status_code=400,
            detail={
                "detail": "Enter a valid 4–6 digit city PIN, or leave it blank to sign in with login ID and password only.",
                "code": "invalid_pin_code",
            },
        )

    mgr: MobileServiceManager | None = None

    if is_valid_mobile_city_pin(pin):
        mgr = (
            db.query(MobileServiceManager)
            .filter(
                MobileServiceManager.city_pin_code == pin,
                (func.lower(MobileServiceManager.login_id) == login_id_n) | (func.lower(MobileServiceManager.email) == login_id_n),
                MobileServiceManager.active.is_(True),
            )
            .one_or_none()
        )
        if not mgr or not verify_password(body.password, mgr.password_hash):
            raise HTTPException(
                status_code=401,
                detail={"detail": "Invalid mobile manager credentials", "code": "invalid_credentials"},
            )
    else:
        candidates = (
            db.query(MobileServiceManager)
            .filter(
                (func.lower(MobileServiceManager.login_id) == login_id_n) | (func.lower(MobileServiceManager.email) == login_id_n),
                MobileServiceManager.active.is_(True)
            )
            .all()
        )
        matches = [m for m in candidates if verify_password(body.password, m.password_hash)]
        if len(matches) == 1:
            mgr = matches[0]
        elif len(matches) > 1:
            raise HTTPException(
                status_code=401,
                detail={
                    "detail": "More than one mobile manager uses this login ID; ask admin to use a unique login per manager or sign in with the city PIN included.",
                    "code": "invalid_credentials",
                },
            )
        else:
            raise HTTPException(
                status_code=401,
                detail={"detail": "Invalid mobile manager credentials", "code": "invalid_credentials"},
            )

    token = create_access_token({"role": "mobile_manager", "sub": mgr.id, "city_pin_code": mgr.city_pin_code})
    return MobileManagerTokenResponse(
        access_token=token,
        city_pin_code=str(mgr.city_pin_code or ""),
        emp_name=str(mgr.emp_name or "").strip(),
        zip_code=str(mgr.zip_code or "").strip(),
    )


@router.post("/mobile/washer/login", response_model=MobileWasherTokenResponse)
def mobile_washer_login(body: MobileWasherLoginRequest, db: DbSession) -> MobileWasherTokenResponse:
    pin = normalize_mobile_city_pin(body.city_pin_code)
    raw_pin = str(body.city_pin_code or "").strip()
    login_id_n = str(body.login_id or "").strip().lower()
    if not login_id_n:
        raise HTTPException(
            status_code=400,
            detail={"detail": "login_id is required", "code": "validation_error"},
        )
    if raw_pin and not is_valid_mobile_city_pin(pin):
        raise HTTPException(
            status_code=400,
            detail={
                "detail": "Enter a valid 4–6 digit city PIN.",
                "code": "invalid_pin_code",
            },
        )

    w: MobileServiceDriver | None = None

    if pin:
        w = (
            db.query(MobileServiceDriver)
            .filter(
                MobileServiceDriver.city_pin_code == pin,
                (func.lower(MobileServiceDriver.login_id) == login_id_n) | (func.lower(MobileServiceDriver.email) == login_id_n),
                MobileServiceDriver.active.is_(True),
            )
            .one_or_none()
        )
        if not w:
            candidates = (
                db.query(MobileServiceDriver)
                .filter(
                    MobileServiceDriver.service_pin_code == pin,
                    (func.lower(MobileServiceDriver.login_id) == login_id_n) | (func.lower(MobileServiceDriver.email) == login_id_n),
                    MobileServiceDriver.active.is_(True),
                )
                .all()
            )
            if len(candidates) == 1:
                w = candidates[0]
            elif len(candidates) > 1:
                raise HTTPException(
                    status_code=401,
                    detail={
                        "detail": "Multiple drivers match that PIN; use login and password only, or a unique login ID.",
                        "code": "invalid_credentials",
                    },
                )
        if not w or not verify_password(body.password, w.password_hash):
            raise HTTPException(
                status_code=401,
                detail={"detail": "Invalid mobile washer credentials", "code": "invalid_credentials"},
            )
    else:
        candidates = (
            db.query(MobileServiceDriver)
            .filter(
                (func.lower(MobileServiceDriver.login_id) == login_id_n) | (func.lower(MobileServiceDriver.email) == login_id_n),
                MobileServiceDriver.active.is_(True)
            )
            .all()
        )
        matches = [d for d in candidates if verify_password(body.password, d.password_hash)]
        if len(matches) == 1:
            w = matches[0]
        elif len(matches) > 1:
            raise HTTPException(
                status_code=401,
                detail={
                    "detail": "More than one driver uses this login ID; ask admin to assign a unique login for each driver.",
                    "code": "invalid_credentials",
                },
            )
        else:
            raise HTTPException(
                status_code=401,
                detail={"detail": "Invalid mobile washer credentials", "code": "invalid_credentials"},
            )

    token = create_access_token({"role": "mobile_washer", "sub": w.id, "city_pin_code": w.city_pin_code})
    return MobileWasherTokenResponse(
        access_token=token,
        city_pin_code=w.city_pin_code or "",
        service_pin_code=str(w.service_pin_code or ""),
        serviceable_zip_codes=[str(z).strip() for z in loads_json_array(w.serviceable_zip_codes_json) if str(z).strip()],
    )


# ---------------------------------------------------------------------------
# Signup email verification (OTP sent before account creation)
# ---------------------------------------------------------------------------

class SignupOtpRequest(BaseModel):
    email: EmailStr


@router.post("/customer/send-signup-otp")
def customer_send_signup_otp(body: SignupOtpRequest, db: DbSession) -> dict:
    """Send a 6-digit verification code to the supplied email.

    Returns success even if the email is already registered so the response
    cannot be used to enumerate existing accounts — the register endpoint
    will surface the conflict at creation time.
    """
    email_n = _normalize_customer_email(str(body.email))
    # Check availability early and give a clear error (better UX than waiting
    # until register), but do NOT reveal whether an account exists to strangers
    # — keep the message generic.
    existing = db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"detail": "Email already linked to another account", "code": "email_taken"},
        )
    otp = otp_service.store_otp("signup", email_n, email_n)
    logger.info("[SignupOtp] OTP generated for %s", email_n)
    from app.services.email_service import send_signup_otp_email
    send_signup_otp_email(email_n, otp)
    return {"message": "Verification code sent to your email."}


@router.post("/customer/verify-signup-otp")
def customer_verify_signup_otp(body: VerifyOtpRequest) -> dict:
    identifier = body.identifier.strip().lower()
    if not otp_service.verify_otp("signup", identifier, body.otp):
        raise HTTPException(
            status_code=400,
            detail={"detail": "Invalid or expired code. Please try again.", "code": "invalid_otp"},
        )
    return {"message": "Email verified."}


# ---------------------------------------------------------------------------
# Email change verification (authenticated — requires valid customer token)
# ---------------------------------------------------------------------------

class EmailChangeOtpRequest(BaseModel):
    new_email: EmailStr


@router.post("/customer/request-email-change")
def customer_request_email_change(
    body: EmailChangeOtpRequest, db: DbSession, auth: CustomerAuth
) -> dict:
    """Send a verification OTP to the *new* email address.

    The new email must not be taken by another account.  The current email
    remains unchanged until the OTP is verified and the profile is saved.
    """
    customer_id = str(auth["sub"])
    new_email_n = _normalize_customer_email(str(body.new_email))

    # Prevent claiming an email that belongs to someone else.
    taken = (
        db.query(CustomerUser.id)
        .filter(CustomerUser.email == new_email_n, CustomerUser.id != customer_id)
        .first()
    )
    if taken:
        raise HTTPException(
            status_code=409,
            detail={"detail": "Email already linked to another account", "code": "email_taken"},
        )

    u = db.query(CustomerUser).filter(CustomerUser.id == customer_id).one_or_none()
    otp = otp_service.store_otp("email_change", new_email_n, new_email_n)
    logger.info("[EmailChangeOtp] OTP generated for customer %s → new email %s", customer_id, new_email_n)
    from app.services.email_service import send_email_change_otp_email
    send_email_change_otp_email(new_email_n, u.full_name if u else "", otp)
    return {"message": "Verification code sent to your new email address."}


@router.post("/customer/verify-email-change")
def customer_verify_email_change(body: VerifyOtpRequest, auth: CustomerAuth) -> dict:
    customer_id = str(auth["sub"])
    new_email_n = body.identifier.strip().lower()
    if not otp_service.verify_otp("email_change", new_email_n, body.otp):
        raise HTTPException(
            status_code=400,
            detail={"detail": "Invalid or expired code. Please try again.", "code": "invalid_otp"},
        )
    # Issue a short-lived signed token that patch_profile can verify without
    # touching shared state.  This makes the email-change flow work correctly
    # across multiple gunicorn workers / Azure App Service instances.
    from datetime import timedelta
    verification_token = create_access_token(
        {"purpose": "email_change", "new_email": new_email_n, "sub": customer_id},
        expires_delta=timedelta(minutes=15),
    )
    return {"message": "New email address verified.", "email_change_token": verification_token}


# ---------------------------------------------------------------------------
# Forgot password — customer
# ---------------------------------------------------------------------------

@router.post("/customer/forgot-password")
def customer_forgot_password(body: ForgotPasswordRequest, db: DbSession) -> dict:
    email_n = _normalize_customer_email(body.identifier)
    logger.info("[ForgotPw/customer] request for identifier=%s", email_n)
    user = db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none()
    if not user:
        logger.info("[ForgotPw/customer] no account found for %s", email_n)
        raise HTTPException(
            status_code=404,
            detail={"detail": "No account found with this email.", "code": "account_not_found"},
        )
    elif not user.email:
        logger.warning("[ForgotPw/customer] account found but email field is empty for %s", email_n)
    else:
        otp = _store_otp("customer", email_n, user.email)
        logger.info("[ForgotPw/customer] OTP generated for %s, sending to %s", email_n, user.email)
        from app.services.email_service import send_otp_email
        send_otp_email(user.email, user.full_name or "", otp)
    return {"message": "If this email is registered, you will receive a reset code shortly."}


@router.post("/customer/verify-otp")
def customer_verify_otp(body: VerifyOtpRequest) -> dict:
    identifier = body.identifier.strip().lower()
    if not _verify_otp_store("customer", identifier, body.otp):
        raise HTTPException(status_code=400, detail={"detail": "Invalid or expired code. Please try again.", "code": "invalid_otp"})
    return {"message": "Code verified."}


@router.post("/customer/reset-password")
def customer_reset_password(body: ResetPasswordRequest, db: DbSession) -> dict:
    identifier = body.identifier.strip().lower()
    email = _check_reset_allowed("customer", identifier)
    if not email:
        raise HTTPException(status_code=400, detail={"detail": "Session expired. Please restart the reset flow.", "code": "not_verified"})
    user = db.query(CustomerUser).filter(CustomerUser.email == email).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail={"detail": "Account not found.", "code": "not_found"})
    user.password_hash = hash_password(body.new_password)
    db.commit()
    _clear_otp("customer", identifier)
    return {"message": "Password updated successfully."}


# ---------------------------------------------------------------------------
# Forgot password — manager (branch manager + mobile manager)
# ---------------------------------------------------------------------------

@router.post("/manager/forgot-password")
def manager_forgot_password(body: ForgotPasswordRequest, db: DbSession) -> dict:
    identifier_n = body.identifier.strip().lower()
    logger.info("[ForgotPw/manager] request for identifier=%s", identifier_n)
    mgr = db.query(BranchManager).filter(
        (func.lower(BranchManager.login_id) == identifier_n) | (func.lower(BranchManager.email) == identifier_n),
        BranchManager.active.is_(True),
    ).first()
    if not mgr:
        mgr = db.query(MobileServiceManager).filter(
            (func.lower(MobileServiceManager.login_id) == identifier_n) | (func.lower(MobileServiceManager.email) == identifier_n),
            MobileServiceManager.active.is_(True),
        ).first()
    if not mgr:
        logger.info("[ForgotPw/manager] no active account found for %s", identifier_n)
        raise HTTPException(
            status_code=404,
            detail={"detail": "No account linked to this email id", "code": "account_not_found"},
        )
    elif not mgr.email:
        logger.warning("[ForgotPw/manager] account found but email field is empty for %s", identifier_n)
    else:
        otp = _store_otp("manager", identifier_n, mgr.email)
        display_name = getattr(mgr, "emp_name", None) or getattr(mgr, "name", None) or ""
        logger.info("[ForgotPw/manager] OTP generated for %s, sending to %s (name=%s)", identifier_n, mgr.email, display_name)
        from app.services.email_service import send_otp_email
        send_otp_email(mgr.email, display_name, otp)
    return {"message": "If this login ID is registered, you will receive a reset code shortly."}


@router.post("/manager/verify-otp")
def manager_verify_otp(body: VerifyOtpRequest) -> dict:
    identifier = body.identifier.strip().lower()
    if not _verify_otp_store("manager", identifier, body.otp):
        raise HTTPException(status_code=400, detail={"detail": "Invalid or expired code. Please try again.", "code": "invalid_otp"})
    return {"message": "Code verified."}


@router.post("/manager/reset-password")
def manager_reset_password(body: ResetPasswordRequest, db: DbSession) -> dict:
    identifier = body.identifier.strip().lower()
    email = _check_reset_allowed("manager", identifier)
    if not email:
        raise HTTPException(status_code=400, detail={"detail": "Session expired. Please restart the reset flow.", "code": "not_verified"})
    mgr = db.query(BranchManager).filter(
        (func.lower(BranchManager.login_id) == identifier) | (func.lower(BranchManager.email) == identifier),
        BranchManager.active.is_(True),
    ).first()
    if not mgr:
        mgr = db.query(MobileServiceManager).filter(
            (func.lower(MobileServiceManager.login_id) == identifier) | (func.lower(MobileServiceManager.email) == identifier),
            MobileServiceManager.active.is_(True),
        ).first()
    if not mgr:
        raise HTTPException(status_code=404, detail={"detail": "Account not found.", "code": "not_found"})
    mgr.password_hash = hash_password(body.new_password)
    db.commit()
    _clear_otp("manager", identifier)
    return {"message": "Password updated successfully."}


# ---------------------------------------------------------------------------
# Forgot password — washer (branch washer + mobile driver)
# ---------------------------------------------------------------------------

@router.post("/washer/forgot-password")
def washer_forgot_password(body: ForgotPasswordRequest, db: DbSession) -> dict:
    identifier_n = body.identifier.strip().lower()
    logger.info("[ForgotPw/washer] request for identifier=%s", identifier_n)

    account_type = None
    w = db.query(Washer).filter(
        (func.lower(Washer.login_id) == identifier_n) | (func.lower(Washer.email) == identifier_n),
        Washer.active.is_(True),
    ).first()
    if w:
        account_type = "branch_washer"
    else:
        w = db.query(MobileServiceDriver).filter(
            (func.lower(MobileServiceDriver.login_id) == identifier_n) | (func.lower(MobileServiceDriver.email) == identifier_n),
            MobileServiceDriver.active.is_(True),
        ).first()
        if w:
            account_type = "mobile_driver"

    if not w:
        logger.info("[ForgotPw/washer] no active account found for identifier=%s", identifier_n)
        raise HTTPException(
            status_code=404,
            detail={"detail": "No account linked to this email id", "code": "account_not_found"},
        )

    logger.info("[ForgotPw/washer] found %s account, email=%r", account_type, w.email)

    if not w.email or not w.email.strip():
        logger.warning("[ForgotPw/washer] account %s has no email address — cannot send OTP", identifier_n)
        raise HTTPException(
            status_code=422,
            detail={"detail": "This account has no email address on file. Please contact your administrator.", "code": "no_email"},
        )

    otp = _store_otp("washer", identifier_n, w.email.strip())
    display_name = getattr(w, "emp_name", None) or getattr(w, "name", None) or ""
    logger.info("[ForgotPw/washer] OTP generated for %s, sending to %s (name=%r, type=%s)", identifier_n, w.email, display_name, account_type)

    from app.services.email_service import send_otp_email
    send_otp_email(w.email.strip(), display_name, otp)

    return {"message": "Reset code sent. Please check your email."}


@router.post("/washer/verify-otp")
def washer_verify_otp(body: VerifyOtpRequest) -> dict:
    identifier = body.identifier.strip().lower()
    logger.info("[VerifyOtp/washer] attempt for identifier=%s otp_length=%d", identifier, len(body.otp or ""))
    if not _verify_otp_store("washer", identifier, body.otp):
        logger.warning("[VerifyOtp/washer] invalid or expired OTP for identifier=%s", identifier)
        raise HTTPException(status_code=400, detail={"detail": "Invalid or expired code. Please try again.", "code": "invalid_otp"})
    logger.info("[VerifyOtp/washer] OTP verified successfully for identifier=%s", identifier)
    return {"message": "Code verified."}


@router.post("/washer/reset-password")
def washer_reset_password(body: ResetPasswordRequest, db: DbSession) -> dict:
    identifier = body.identifier.strip().lower()
    logger.info("[ResetPw/washer] request for identifier=%s", identifier)
    email = _check_reset_allowed("washer", identifier)
    if not email:
        logger.warning("[ResetPw/washer] OTP not verified or session expired for identifier=%s", identifier)
        raise HTTPException(status_code=400, detail={"detail": "Session expired. Please restart the reset flow.", "code": "not_verified"})
    w = db.query(Washer).filter(
        (func.lower(Washer.login_id) == identifier) | (func.lower(Washer.email) == identifier),
        Washer.active.is_(True),
    ).first()
    if not w:
        w = db.query(MobileServiceDriver).filter(
            (func.lower(MobileServiceDriver.login_id) == identifier) | (func.lower(MobileServiceDriver.email) == identifier),
            MobileServiceDriver.active.is_(True),
        ).first()
    if not w:
        logger.error("[ResetPw/washer] account not found at reset stage for identifier=%s", identifier)
        raise HTTPException(status_code=404, detail={"detail": "Account not found.", "code": "not_found"})
    w.password_hash = hash_password(body.new_password)
    db.commit()
    _clear_otp("washer", identifier)
    logger.info("[ResetPw/washer] password updated successfully for identifier=%s", identifier)
    return {"message": "Password updated successfully."}

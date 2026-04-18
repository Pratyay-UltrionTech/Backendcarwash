from fastapi import APIRouter, HTTPException

from app.api.deps import DbSession
from app.config import get_settings
from app.core.mobile_pins import normalize_mobile_city_pin
from app.core.security import create_access_token, hash_password, verify_password
from app.models import BranchManager, CustomerUser, MobileServiceDriver, MobileServiceManager, Washer
from app.schemas.auth import (
    AdminLoginRequest,
    ManagerLoginRequest,
    MobileManagerLoginRequest,
    MobileWasherLoginRequest,
    MobileWasherTokenResponse,
    TokenResponse,
    WasherLoginRequest,
)
from app.schemas.customer_auth import CustomerAuthResponse, CustomerLoginRequest, CustomerRegisterRequest

router = APIRouter(prefix="/auth", tags=["auth"])


def _normalize_customer_email(email: str) -> str:
    return email.strip().lower()


@router.post("/customer/register", response_model=CustomerAuthResponse)
def customer_register(body: CustomerRegisterRequest, db: DbSession) -> CustomerAuthResponse:
    email_n = _normalize_customer_email(str(body.email))
    existing = db.query(CustomerUser).filter(CustomerUser.email == email_n).one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"detail": "Email already registered", "code": "email_taken"},
        )
    user = CustomerUser(
        email=email_n,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    token = create_access_token({"role": "customer", "sub": user.id, "email": user.email})
    return CustomerAuthResponse(
        access_token=token,
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
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=401,
            detail={"detail": "Invalid email or password", "code": "invalid_credentials"},
        )
    token = create_access_token({"role": "customer", "sub": user.id, "email": user.email})
    return CustomerAuthResponse(
        access_token=token,
        email=user.email,
        profile_completed=user.profile_completed,
        full_name=user.full_name,
        phone=user.phone,
        address=user.address_line,
    )


@router.post("/admin/login", response_model=TokenResponse)
def admin_login(body: AdminLoginRequest) -> TokenResponse:
    settings = get_settings()
    if body.username != settings.admin_username or body.password != settings.admin_password:
        raise HTTPException(status_code=401, detail={"detail": "Invalid admin credentials", "code": "invalid_credentials"})
    token = create_access_token({"role": "admin", "sub": "admin"})
    return TokenResponse(access_token=token)


@router.post("/manager/login", response_model=TokenResponse)
def manager_login(body: ManagerLoginRequest, db: DbSession) -> TokenResponse:
    mgr = (
        db.query(BranchManager)
        .filter(
            BranchManager.branch_id == body.branch_id,
            BranchManager.login_id == body.login_id,
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
    w = (
        db.query(Washer)
        .filter(
            Washer.branch_id == body.branch_id,
            Washer.login_id == body.login_id,
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


@router.post("/mobile/manager/login", response_model=TokenResponse)
def mobile_manager_login(body: MobileManagerLoginRequest, db: DbSession) -> TokenResponse:
    pin = normalize_mobile_city_pin(body.city_pin_code)
    login_id = str(body.login_id or "").strip()
    if len(pin) != 6:
        raise HTTPException(
            status_code=400,
            detail={"detail": "Enter a valid 6-digit city PIN", "code": "invalid_pin_code"},
        )
    mgr = (
        db.query(MobileServiceManager)
        .filter(
            MobileServiceManager.city_pin_code == pin,
            MobileServiceManager.login_id == login_id,
            MobileServiceManager.active.is_(True),
        )
        .one_or_none()
    )
    if not mgr or not verify_password(body.password, mgr.password_hash):
        raise HTTPException(
            status_code=401,
            detail={"detail": "Invalid mobile manager credentials", "code": "invalid_credentials"},
        )
    token = create_access_token({"role": "mobile_manager", "sub": mgr.id, "city_pin_code": mgr.city_pin_code})
    return TokenResponse(access_token=token)


@router.post("/mobile/washer/login", response_model=MobileWasherTokenResponse)
def mobile_washer_login(body: MobileWasherLoginRequest, db: DbSession) -> MobileWasherTokenResponse:
    pin = normalize_mobile_city_pin(body.city_pin_code)
    login_id = str(body.login_id or "").strip()
    if not login_id:
        raise HTTPException(
            status_code=400,
            detail={"detail": "login_id is required", "code": "validation_error"},
        )

    w: MobileServiceDriver | None = None

    if pin:
        w = (
            db.query(MobileServiceDriver)
            .filter(
                MobileServiceDriver.city_pin_code == pin,
                MobileServiceDriver.login_id == login_id,
                MobileServiceDriver.active.is_(True),
            )
            .one_or_none()
        )
        if not w:
            candidates = (
                db.query(MobileServiceDriver)
                .filter(
                    MobileServiceDriver.service_pin_code == pin,
                    MobileServiceDriver.login_id == login_id,
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
            .filter(MobileServiceDriver.login_id == login_id, MobileServiceDriver.active.is_(True))
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
    return MobileWasherTokenResponse(access_token=token, city_pin_code=w.city_pin_code or "")

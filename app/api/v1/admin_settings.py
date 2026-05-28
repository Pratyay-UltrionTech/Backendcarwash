"""Admin Settings — change email / password, add admins, overview.

All endpoints require a valid admin token (env or DB admin).
Mutating operations (change email/password/add admin) are blocked for the env
super-admin (sub == "admin") — they must use DB admin accounts for those.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr

from app.api.deps import AdminUser, DbSession
from app.core.security import hash_password, verify_password
from app.models import AdminAccount
from app.models.branch import Branch
from app.models.catalog import BranchAddonItem, CatalogAddonItem, CatalogServiceItem, VehicleCatalogBlock
from app.models.loyalty import BranchLoyalty
from app.models.mobile import (
    MobileGlobalAddonItem,
    MobileLoyaltyProgram,
    MobileServiceDriver,
    MobileServiceManager,
    MobileVehicleCatalogBlock,
    MobileCatalogServiceItem,
    MobilePromotion,
    MobileDayTimePriceRule,
)
from app.models.promotion import DayTimePriceRule, Promotion
from app.models.staff import BranchManager, Washer
from app.services import otp_service
from app.services.email_service import send_otp_email

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/settings", tags=["admin-settings"])


# ── helpers ──────────────────────────────────────────────────────────────────

def _is_env_admin(payload: dict) -> bool:
    return payload.get("sub") == "admin"


def _require_db_admin(payload: dict) -> None:
    if _is_env_admin(payload):
        raise HTTPException(
            status_code=403,
            detail="The environment admin account cannot use this feature. "
                   "Please use a DB admin account.",
        )


def _current_admin_id(payload: dict) -> str:
    return str(payload.get("sub", ""))


# ── schemas ───────────────────────────────────────────────────────────────────

class MeResponse(BaseModel):
    is_env_admin: bool
    email: Optional[str] = None
    id: Optional[str] = None


class RequestOtpBody(BaseModel):
    new_email: EmailStr


class VerifyOtpBody(BaseModel):
    new_email: EmailStr
    otp: str


class ChangePasswordBody(BaseModel):
    current_password: str
    new_password: str


class AddAdminRequestOtpBody(BaseModel):
    email: EmailStr
    password: str


class AddAdminConfirmBody(BaseModel):
    email: EmailStr
    otp: str


class AdminListItem(BaseModel):
    id: str
    email: str


# ── Forgot password (public — no auth required) ───────────────────────────────

class ForgotPasswordRequestBody(BaseModel):
    email: EmailStr


class ForgotPasswordResetBody(BaseModel):
    email: EmailStr
    otp: str
    new_password: str


@router.post("/forgot-password/request-otp", status_code=204)
def forgot_password_request_otp(body: ForgotPasswordRequestBody, db: DbSession):
    """Send a password-reset OTP to the given email if it belongs to an active DB admin."""
    acct = (
        db.query(AdminAccount)
        .filter(AdminAccount.email == body.email.lower(), AdminAccount.active.is_(True))
        .one_or_none()
    )
    if not acct:
        raise HTTPException(
            status_code=404,
            detail={"detail": "No account linked to this email id", "code": "account_not_found"},
        )
    otp = otp_service.store_otp("admin_forgot_pw", body.email.lower(), body.email)
    send_otp_email(body.email, "Admin", otp)


@router.post("/forgot-password/reset", status_code=204)
def forgot_password_reset(body: ForgotPasswordResetBody, db: DbSession):
    """Verify OTP and set the new password for the DB admin account."""
    if len(body.new_password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters.")
    email_key = body.email.lower()
    if not otp_service.verify_otp("admin_forgot_pw", email_key, body.otp):
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")
    acct = (
        db.query(AdminAccount)
        .filter(AdminAccount.email == email_key, AdminAccount.active.is_(True))
        .one_or_none()
    )
    if not acct:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")
    acct.password_hash = hash_password(body.new_password)
    db.commit()
    otp_service.clear_otp("admin_forgot_pw", email_key)


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.get("/me", response_model=MeResponse)
def settings_me(current_admin: AdminUser) -> MeResponse:
    """Return basic info about the currently logged-in admin."""
    env = _is_env_admin(current_admin)
    if env:
        return MeResponse(is_env_admin=True, email=None, id=None)
    return MeResponse(
        is_env_admin=False,
        email=current_admin.get("email"),
        id=current_admin.get("sub"),
    )


# ── Change email (DB admins only) ─────────────────────────────────────────────

@router.post("/change-email/request-otp", status_code=204)
def change_email_request_otp(body: RequestOtpBody, current_admin: AdminUser, db: DbSession):
    """Send OTP to the *new* email address to verify ownership."""
    _require_db_admin(current_admin)
    # Check the new email isn't already taken
    existing = (
        db.query(AdminAccount)
        .filter(AdminAccount.email == body.new_email.lower())
        .one_or_none()
    )
    if existing and existing.id != _current_admin_id(current_admin):
        raise HTTPException(status_code=409, detail="That email is already in use by another admin account.")
    otp = otp_service.store_otp("admin_email_change", _current_admin_id(current_admin), body.new_email)
    send_otp_email(body.new_email, "Admin", otp)


@router.post("/change-email/verify", status_code=204)
def change_email_verify(body: VerifyOtpBody, current_admin: AdminUser, db: DbSession):
    """Verify OTP and update the email on the DB admin account."""
    _require_db_admin(current_admin)
    admin_id = _current_admin_id(current_admin)
    if not otp_service.verify_otp("admin_email_change", admin_id, body.otp):
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")
    acct = db.query(AdminAccount).filter(AdminAccount.id == admin_id).one_or_none()
    if not acct:
        raise HTTPException(status_code=404, detail="Admin account not found.")
    acct.email = body.new_email.lower()
    db.commit()
    otp_service.clear_otp("admin_email_change", admin_id)


# ── Change password (DB admins only) ─────────────────────────────────────────

@router.post("/change-password", status_code=204)
def change_password(body: ChangePasswordBody, current_admin: AdminUser, db: DbSession):
    """Change password for the currently logged-in DB admin."""
    _require_db_admin(current_admin)
    if len(body.new_password) < 8:
        raise HTTPException(status_code=422, detail="New password must be at least 8 characters.")
    admin_id = _current_admin_id(current_admin)
    acct = db.query(AdminAccount).filter(AdminAccount.id == admin_id).one_or_none()
    if not acct:
        raise HTTPException(status_code=404, detail="Admin account not found.")
    if not verify_password(body.current_password, acct.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect.")
    acct.password_hash = hash_password(body.new_password)
    db.commit()


# ── Add admin (any logged-in admin can invite) ────────────────────────────────

@router.post("/add-admin/request-otp", status_code=204)
def add_admin_request_otp(body: AddAdminRequestOtpBody, current_admin: AdminUser, db: DbSession):
    """Send OTP to the new admin's email to verify they own it."""
    if len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters.")
    existing = (
        db.query(AdminAccount)
        .filter(AdminAccount.email == body.email.lower())
        .one_or_none()
    )
    if existing:
        raise HTTPException(status_code=409, detail="An admin with that email already exists.")
    # Store password temporarily as the OTP "identifier" scope payload; we'll re-validate on confirm
    # We use a composite key: email is both scope identifier and recipient
    otp = otp_service.store_otp("admin_invite", body.email.lower(), body.email)
    # Also stash the hashed password under a separate scope keyed the same way
    _pending_passwords[body.email.lower()] = hash_password(body.password)
    send_otp_email(body.email, "New Admin", otp)


# In-memory temp store for pending invite password hashes (cleared on confirm/expiry)
_pending_passwords: dict[str, str] = {}


@router.post("/add-admin/confirm", status_code=204)
def add_admin_confirm(body: AddAdminConfirmBody, current_admin: AdminUser, db: DbSession):
    """Verify OTP and create the new admin account."""
    email_key = body.email.lower()
    if not otp_service.verify_otp("admin_invite", email_key, body.otp):
        raise HTTPException(status_code=400, detail="Invalid or expired OTP.")
    pw_hash = _pending_passwords.pop(email_key, None)
    if not pw_hash:
        raise HTTPException(status_code=400, detail="No pending invite found for this email. Please start over.")
    existing = db.query(AdminAccount).filter(AdminAccount.email == email_key).one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="An admin with that email already exists.")
    acct = AdminAccount(email=email_key, password_hash=pw_hash)
    db.add(acct)
    db.commit()
    otp_service.clear_otp("admin_invite", email_key)


@router.get("/admins", response_model=list[AdminListItem])
def list_admins(current_admin: AdminUser, db: DbSession) -> list[AdminListItem]:
    """List all active DB admin accounts."""
    rows = db.query(AdminAccount).filter(AdminAccount.active.is_(True)).order_by(AdminAccount.created_at).all()
    return [AdminListItem(id=r.id, email=r.email) for r in rows]


class UpdateAdminEmailBody(BaseModel):
    email: EmailStr


@router.patch("/admins/{admin_id}/email", status_code=204)
def update_admin_email(admin_id: str, body: UpdateAdminEmailBody, current_admin: AdminUser, db: DbSession):
    """Update email of a DB admin account directly (no OTP — admin table action)."""
    acct = db.query(AdminAccount).filter(AdminAccount.id == admin_id, AdminAccount.active.is_(True)).one_or_none()
    if not acct:
        raise HTTPException(status_code=404, detail="Admin account not found.")
    conflict = db.query(AdminAccount).filter(
        AdminAccount.email == body.email.lower(), AdminAccount.id != admin_id
    ).one_or_none()
    if conflict:
        raise HTTPException(status_code=409, detail="That email is already in use by another admin account.")
    acct.email = body.email.lower()
    db.commit()


@router.delete("/admins/{admin_id}", status_code=204)
def delete_admin(admin_id: str, current_admin: AdminUser, db: DbSession):
    """Soft-delete a DB admin account so they can no longer log in."""
    # Prevent self-deletion
    if str(current_admin.get("sub", "")) == admin_id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account.")
    acct = db.query(AdminAccount).filter(AdminAccount.id == admin_id).one_or_none()
    if not acct:
        raise HTTPException(status_code=404, detail="Admin account not found.")
    acct.active = False
    db.commit()


# ── Overview ──────────────────────────────────────────────────────────────────

@router.get("/overview")
def settings_overview(current_admin: AdminUser, db: DbSession):
    """Return a compact overview of all branches + mobile ops for the overview panel."""

    # ── Branches ──────────────────────────────────────────────────────────────
    branches = db.query(Branch).order_by(Branch.name).all()
    branch_list = []
    for b in branches:
        managers = (
            db.query(BranchManager)
            .filter(BranchManager.branch_id == b.id, BranchManager.active.is_(True))
            .all()
        )
        washers = (
            db.query(Washer)
            .filter(Washer.branch_id == b.id, Washer.active.is_(True))
            .all()
        )
        promotions = (
            db.query(Promotion)
            .filter(Promotion.branch_id == b.id)
            .all()
        )
        dt_rules = (
            db.query(DayTimePriceRule)
            .filter(DayTimePriceRule.branch_id == b.id)
            .all()
        )
        # Vehicle blocks with services
        v_blocks = (
            db.query(VehicleCatalogBlock)
            .filter(VehicleCatalogBlock.branch_id == b.id)
            .all()
        )
        # Add-ons (branch-wide)
        branch_addons = (
            db.query(BranchAddonItem)
            .filter(BranchAddonItem.branch_id == b.id, BranchAddonItem.active.is_(True))
            .all()
        )
        # Per-vehicle add-ons via CatalogAddonItem
        catalog_addons = []
        for vb_id in [vb.id for vb in v_blocks]:
            catalog_addons += (
                db.query(CatalogAddonItem)
                .filter(CatalogAddonItem.vehicle_block_id == vb_id, CatalogAddonItem.active.is_(True))
                .all()
            )

        # Loyalty config
        loyalty = (
            db.query(BranchLoyalty)
            .filter(BranchLoyalty.branch_id == b.id)
            .one_or_none()
        )

        vehicle_data = []
        for vb in v_blocks:
            services = (
                db.query(CatalogServiceItem)
                .filter(CatalogServiceItem.vehicle_block_id == vb.id)
                .all()
            )
            vehicle_data.append({
                "vehicle_type": vb.vehicle_type,
                "services": [
                    {"name": s.name, "category": getattr(s, "category", None)}
                    for s in services
                ],
            })

        all_addon_names = list({a.name for a in branch_addons + catalog_addons})

        branch_list.append({
            "id": b.id,
            "name": b.name,
            "location": b.location,
            "managers": [{"id": m.id, "name": m.name, "login_id": m.login_id} for m in managers],
            "staff": [{"id": w.id, "name": w.name} for w in washers],
            "vehicles": vehicle_data,
            "promo_codes": [
                {"id": p.id, "code": p.code_name, "type": p.discount_type, "value": p.discount_value}
                for p in promotions
            ],
            "day_time_rules_count": len(dt_rules),
            "addons": all_addon_names,
            "loyalty_configured": loyalty is not None,
            "loyalty_qualifying_count": loyalty.qualifying_service_count if loyalty else None,
        })

    # ── Mobile ops ────────────────────────────────────────────────────────────
    mobile_managers = db.query(MobileServiceManager).filter(MobileServiceManager.active.is_(True)).all()
    mobile_drivers = db.query(MobileServiceDriver).filter(MobileServiceDriver.active.is_(True)).all()
    mobile_promos = db.query(MobilePromotion).all()
    mobile_dt_rules = db.query(MobileDayTimePriceRule).all()
    mobile_v_blocks = db.query(MobileVehicleCatalogBlock).all()
    mobile_global_addons = db.query(MobileGlobalAddonItem).filter(MobileGlobalAddonItem.active.is_(True)).all()
    mobile_loyalty = db.query(MobileLoyaltyProgram).first()

    mobile_vehicles = []
    for vb in mobile_v_blocks:
        services = (
            db.query(MobileCatalogServiceItem)
            .filter(MobileCatalogServiceItem.vehicle_block_id == vb.id)
            .all()
        )
        mobile_vehicles.append({
            "vehicle_type": vb.vehicle_type,
            "services": [{"name": s.name, "category": getattr(s, "category", None)} for s in services],
        })

    mobile_ops = {
        "managers": [{"id": m.id, "name": m.emp_name, "login_id": m.login_id} for m in mobile_managers],
        "staff": [{"id": d.id, "name": d.emp_name, "login_id": d.login_id} for d in mobile_drivers],
        "vehicles": mobile_vehicles,
        "promo_codes": [
            {"id": p.id, "code": p.code_name, "type": p.discount_type, "value": p.discount_value}
            for p in mobile_promos
        ],
        "day_time_rules_count": len(mobile_dt_rules),
        "addons": [a.name for a in mobile_global_addons],
        "loyalty_configured": mobile_loyalty is not None,
        "loyalty_qualifying_count": mobile_loyalty.qualifying_service_count if mobile_loyalty else None,
    }

    return {"branches": branch_list, "mobile_ops": mobile_ops}

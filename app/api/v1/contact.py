"""Public contact / free-quote inquiry endpoint — landing page form."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/public", tags=["contact"])

INQUIRY_RECIPIENT = "lumicarspa@gmail.com"


class ContactInquiryIn(BaseModel):
    name: str = ""
    phone: str = ""
    service: str = ""
    preferred_date: str = ""


@router.post("/contact-inquiry")
def submit_contact_inquiry(body: ContactInquiryIn) -> dict:
    """Receive a free-quote inquiry from the landing page and email it to the business owner."""
    from app.services.email_service import send_contact_inquiry_email

    send_contact_inquiry_email(
        to_email=INQUIRY_RECIPIENT,
        sender_name=body.name,
        sender_phone=body.phone,
        service_required=body.service,
        preferred_date=body.preferred_date,
    )
    return {"ok": True}

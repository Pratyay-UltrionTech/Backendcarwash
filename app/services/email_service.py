"""Azure Communication Services email notifications."""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


def _get_client():
    from azure.communication.email import EmailClient
    from app.config import get_settings

    s = get_settings()
    if not s.azure_comm_connection_string:
        return None, None
    try:
        client = EmailClient.from_connection_string(s.azure_comm_connection_string)
        return client, s.azure_comm_sender
    except Exception as exc:
        logger.error("Failed to initialise Azure email client: %s", exc)
        print(f"[EMAIL ERROR] Azure client init failed: {exc}", flush=True)
        return None, None


def _do_send(to_email: str, subject: str, plain: str, html: str, dev_otp: str | None = None) -> None:
    client, sender = _get_client()
    if not client:
        if dev_otp:
            msg = f"\n{'='*60}\nDEV OTP for {to_email}: {dev_otp}\n{'='*60}\n"
            logger.warning("Azure email not configured — DEV MODE OTP for %s: %s", to_email, dev_otp)
            print(msg, flush=True)
        else:
            logger.warning("Azure Communication email not configured; skipping email to %s", to_email)
        return

    logger.info("Sending email to %s (subject: %s)", to_email, subject)
    message = {
        "senderAddress": sender,
        "recipients": {"to": [{"address": to_email}]},
        "content": {"subject": subject, "plainText": plain, "html": html},
    }
    try:
        poller = client.begin_send(message)
        result = poller.result()
        logger.info("Email sent successfully to %s — status: %s", to_email, getattr(result, "status", "ok"))
        print(f"[EMAIL] Sent '{subject}' to {to_email}", flush=True)
    except Exception as exc:
        logger.exception("Failed to send email to %s (subject: %s): %s", to_email, subject, exc)
        print(f"[EMAIL ERROR] Failed to send '{subject}' to {to_email}: {exc}", flush=True)


def _send(to_email: str, subject: str, plain: str, html: str, dev_otp: str | None = None) -> None:
    """Fire-and-forget: send in a background thread so the request is never blocked."""
    t = threading.Thread(target=_do_send, args=(to_email, subject, plain, html, dev_otp), daemon=True)
    t.start()


def send_welcome_email(to_email: str, name: str) -> None:
    display = name.strip() if name and name.strip() else "Valued Customer"
    plain = (
        f"Welcome to CarWash App, {display}!\n\n"
        "Your account has been created successfully. "
        "You can now book car wash appointments any time through the portal.\n\n"
        "Thank you for joining us!"
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#2563eb;margin-bottom:8px">Welcome to CarWash App!</h2>
<p>Hi {display},</p>
<p>Your account has been created successfully. You can now book car wash appointments any time through the portal.</p>
<p style="margin-top:24px">Thank you for joining us — we look forward to keeping your car sparkling!</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Welcome to CarWash App!", plain, html)


def send_booking_confirmed_email(
    to_email: str,
    name: str,
    date: str,
    start_time: str,
    service_summary: str,
    booking_id: str,
) -> None:
    display = name.strip() if name and name.strip() else "Valued Customer"
    plain = (
        f"Hi {display},\n\n"
        "Your car wash booking has been confirmed!\n\n"
        f"  Date:       {date}\n"
        f"  Time:       {start_time}\n"
        f"  Service:    {service_summary}\n"
        f"  Booking ID: {booking_id}\n\n"
        "If you need to reschedule, please use the portal or contact your branch."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#16a34a;margin-bottom:8px">Booking Confirmed!</h2>
<p>Hi {display},</p>
<p>Your car wash booking has been confirmed. Here are your details:</p>
<table style="border-collapse:collapse;width:100%;margin:16px 0;font-size:15px">
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600;width:130px">Date</td><td style="padding:10px 12px;border-bottom:1px solid #f3f4f6">{date}</td></tr>
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600">Time</td><td style="padding:10px 12px;border-bottom:1px solid #f3f4f6">{start_time}</td></tr>
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600">Service</td><td style="padding:10px 12px;border-bottom:1px solid #f3f4f6">{service_summary}</td></tr>
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600">Booking ID</td><td style="padding:10px 12px">{booking_id}</td></tr>
</table>
<p>We look forward to seeing you!</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Your Car Wash Booking is Confirmed", plain, html)


def send_booking_rescheduled_email(
    to_email: str,
    name: str,
    new_date: str,
    new_start_time: str,
    booking_id: str,
) -> None:
    display = name.strip() if name and name.strip() else "Valued Customer"
    plain = (
        f"Hi {display},\n\n"
        "Your car wash appointment has been rescheduled.\n\n"
        f"  New Date:   {new_date}\n"
        f"  New Time:   {new_start_time}\n"
        f"  Booking ID: {booking_id}\n\n"
        "If you did not request this change, please contact your branch immediately."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#d97706;margin-bottom:8px">Appointment Rescheduled</h2>
<p>Hi {display},</p>
<p>Your car wash appointment has been moved to a new time:</p>
<table style="border-collapse:collapse;width:100%;margin:16px 0;font-size:15px">
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600;width:130px">New Date</td><td style="padding:10px 12px;border-bottom:1px solid #f3f4f6">{new_date}</td></tr>
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600">New Time</td><td style="padding:10px 12px;border-bottom:1px solid #f3f4f6">{new_start_time}</td></tr>
  <tr><td style="padding:10px 12px;background:#f9fafb;font-weight:600">Booking ID</td><td style="padding:10px 12px">{booking_id}</td></tr>
</table>
<p>If you did not request this change, please contact your branch immediately.</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Your Car Wash Appointment Has Been Rescheduled", plain, html)


def send_otp_email(to_email: str, name: str, otp: str) -> None:
    display = name.strip() if name and name.strip() else "there"
    plain = (
        f"Hi {display},\n\n"
        f"Your CarWash password reset code is: {otp}\n\n"
        "This code expires in 10 minutes. If you did not request a password reset, please ignore this email."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#4F46E5;margin-bottom:8px">Password Reset Code</h2>
<p>Hi {display},</p>
<p>Use the code below to reset your CarWash password. It expires in <strong>10 minutes</strong>.</p>
<div style="margin:28px 0;text-align:center">
  <span style="display:inline-block;background:#f3f4f6;border:2px dashed #d1d5db;border-radius:12px;padding:18px 40px;font-size:36px;font-weight:700;letter-spacing:10px;color:#1e293b">{otp}</span>
</div>
<p style="color:#6b7280;font-size:14px">If you did not request a password reset, you can safely ignore this email.</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Your CarWash Password Reset Code", plain, html, dev_otp=otp)


def lookup_customer_email(db, customer_id: str | None, phone: str | None) -> tuple[str | None, str | None]:
    """Return (email, full_name) for a customer, trying customer_id first then phone."""
    from app.models import CustomerUser

    if customer_id:
        u = db.query(CustomerUser).filter(CustomerUser.id == customer_id).one_or_none()
        if u and u.email:
            return u.email, u.full_name
    if phone:
        phone_n = "".join(c for c in str(phone) if c.isdigit() or c == "+")
        rows = db.query(CustomerUser).filter(CustomerUser.phone.isnot(None)).all()
        for u in rows:
            stored = "".join(c for c in str(u.phone or "") if c.isdigit() or c == "+")
            if stored and stored == phone_n:
                return u.email, u.full_name
    return None, None

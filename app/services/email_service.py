"""Azure Communication Services email notifications."""

from __future__ import annotations

import html
import logging
import threading
from datetime import datetime

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


def _escape(text: str) -> str:
    return html.escape(str(text or ""), quote=True)


def _format_email_date(date_str: str) -> str:
    raw = str(date_str or "").strip()
    if not raw:
        return "—"
    try:
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return f"{dt.strftime('%A')}, {dt.day} {dt.strftime('%B %Y')}"
    except ValueError:
        return raw


def _format_email_time(time_str: str) -> str:
    raw = str(time_str or "").strip()
    if not raw:
        return "—"
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(raw, fmt).time()
            return t.strftime("%I:%M %p").lstrip("0")
        except ValueError:
            continue
    return raw


def _display_first_name(name: str) -> str:
    parts = str(name or "").strip().split()
    return parts[0] if parts else "there"


def short_booking_ref(booking_id: str, customer_id: str | None = None, phone: str | None = None) -> str:
    """Return a human-readable booking reference like #A1B2C3-D4E5 (matches front-end logic)."""
    raw = str(booking_id or "").replace("-", "")
    hex6 = raw[-6:].upper() if len(raw) >= 6 else raw.upper().zfill(6)
    if customer_id:
        cid = str(customer_id).replace("-", "")
        suffix = cid[-4:].upper() if len(cid) >= 4 else cid.upper().zfill(4)
        return f"#{hex6}-{suffix}"
    if phone:
        digits = "".join(c for c in str(phone) if c.isdigit())
        if digits:
            num = int(digits[-9:]) if len(digits) >= 9 else int(digits)
            chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            n, parts = num, []
            while n:
                parts.append(chars[n % 36])
                n //= 36
            b36 = "".join(reversed(parts)) if parts else "0"
            suffix = b36[-4:].zfill(4)
            return f"#{hex6}-{suffix}"
    return f"#{hex6}"


def _compute_duration_label(start: str, end: str) -> str:
    """Return a human-readable duration string like '~45 minutes', or '' on failure."""
    try:
        s_obj = e_obj = None
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                s_obj = datetime.strptime(start.strip(), fmt)
                break
            except ValueError:
                pass
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                e_obj = datetime.strptime(end.strip(), fmt)
                break
            except ValueError:
                pass
        if s_obj and e_obj:
            mins = int((e_obj - s_obj).total_seconds() / 60)
            if mins > 0:
                return f"~{mins} minutes"
    except Exception:
        pass
    return ""


_LUMI_WEBSITE = "https://www.lumicarspa.com.au"
_LUMI_PORTAL  = "https://www.lumicarspa.com.au/#/home"
_LUMI_PHONE   = "(02) 9894 2838"


def send_booking_confirmed_email(
    to_email: str,
    name: str,
    date: str,
    start_time: str,
    service_summary: str,
    booking_id: str,
    customer_id: str | None = None,
    phone: str | None = None,
    end_time: str | None = None,
    channel: str = "branch",
) -> None:
    display = name.strip() if name and name.strip() else "Valued Customer"
    first_name = _display_first_name(display)
    ref = short_booking_ref(booking_id, customer_id, phone)
    date_display = _format_email_date(date)
    time_display = _format_email_time(start_time)
    service_display = service_summary.strip() or "—"
    duration_label = _compute_duration_label(start_time, end_time) if end_time else ""

    is_mobile = channel == "mobile"

    if is_mobile:
        what_to_expect = [
            "Please ensure you are available at the service address at the scheduled time",
            "Clear any access to your vehicle (driveway, gate) before the team arrives",
            "Remove any valuables from your vehicle before the service begins",
            "Our team will notify you when they are on their way and again when your car is ready",
        ]
    else:
        what_to_expect = [
            "Please arrive 5 minutes early so we can get you checked in smoothly",
            "Leave your windows fully closed and remove any valuables from your vehicle",
            "Our team will notify you as soon as your car is ready for pickup",
        ]

    duration_row_plain = f"  Est. Duration: {duration_label}\n" if duration_label else ""
    duration_row_html = (
        f'<tr>'
        f'<td style="padding:11px 18px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap;width:150px">Est. Duration</td>'
        f'<td style="padding:11px 18px;border-bottom:1px solid #e5e7eb">{_escape(duration_label)}</td>'
        f'</tr>'
    ) if duration_label else ""

    expect_plain = "\n".join(f"  •  {pt}" for pt in what_to_expect)
    expect_html  = "\n".join(
        f'<li style="margin-bottom:8px;color:#374151">{_escape(pt)}</li>'
        for pt in what_to_expect
    )

    plain = (
        f"Subject: Your Car Wash Booking is Confirmed – {ref}\n\n"
        f"Booking Confirmed!\n\n"
        f"Hi {first_name},\n\n"
        "Thanks for booking with us! Your appointment is locked in and we can't wait to "
        "give your car the treatment it deserves. Here's everything you need to know:\n\n"
        "────────────────────────────────────\n"
        "📋 Booking Summary\n"
        "────────────────────────────────────\n"
        f"  Date:         {date_display}\n"
        f"  Time:         {time_display}\n"
        f"  Service:      {service_display}\n"
        f"  Booking ID:   {ref}\n"
        f"{duration_row_plain}"
        "\n────────────────────────────────────\n"
        "📍 What to Expect\n"
        "────────────────────────────────────\n"
        f"{expect_plain}\n\n"
        "────────────────────────────────────\n"
        "If you need to make changes?\n"
        "────────────────────────────────────\n"
        "To reschedule or cancel, please contact us at least 24 hours before your appointment:\n\n"
        f"  🌐  {_LUMI_PORTAL}\n"
        f"  📞  {_LUMI_PHONE}\n\n"
        "We look forward to seeing you and your car!\n"
        "The Lumi Car Spa Team"
    )

    html = f"""
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#111;max-width:640px;margin:0 auto;padding:0;line-height:1.65;font-size:15px">

  <!-- Header -->
  <div style="background:#0c1d3a;padding:28px 32px 22px">
    <p style="color:#c9a84c;margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:2.5px;text-transform:uppercase">Lumi Car Spa</p>
    <p style="color:#ffffff;margin:0;font-size:24px;font-weight:700">Booking Confirmed! &#x1F389;</p>
  </div>

  <!-- Body -->
  <div style="padding:28px 32px 4px;background:#ffffff">

    <p style="margin:0 0 6px;font-size:15px">Hi <strong>{_escape(first_name)}</strong>,</p>
    <p style="margin:0 0 24px;color:#374151;font-size:15px">
      Thanks for booking with us! Your appointment is locked in and we can&rsquo;t wait to
      give your car the treatment it deserves. Here&rsquo;s everything you need to know:
    </p>

    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 20px">

    <!-- Booking Summary -->
    <p style="margin:0 0 12px;font-size:14px;font-weight:700;color:#0c1d3a;letter-spacing:.04em">&#x1F4CB;&nbsp; Booking Summary</p>
    <table style="border-collapse:collapse;width:100%;font-size:14px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;margin-bottom:24px">
      <tr>
        <td style="padding:11px 18px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap;width:150px">Date</td>
        <td style="padding:11px 18px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(date_display)}</td>
      </tr>
      <tr>
        <td style="padding:11px 18px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Time</td>
        <td style="padding:11px 18px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(time_display)}</td>
      </tr>
      <tr>
        <td style="padding:11px 18px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Service</td>
        <td style="padding:11px 18px;border-bottom:1px solid #e5e7eb">{_escape(service_display)}</td>
      </tr>
      <tr>
        <td style="padding:11px 18px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Booking ID</td>
        <td style="padding:11px 18px;border-bottom:1px solid #e5e7eb;font-family:monospace;font-size:13px;color:#0c1d3a">{_escape(ref)}</td>
      </tr>
      {duration_row_html}
    </table>

    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 20px">

    <!-- What to Expect -->
    <p style="margin:0 0 12px;font-size:14px;font-weight:700;color:#0c1d3a;letter-spacing:.04em">&#x1F4CD;&nbsp; What to Expect</p>
    <ul style="margin:0 0 24px;padding-left:22px;font-size:14px">
      {expect_html}
    </ul>

    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 20px">

    <!-- Changes -->
    <p style="margin:0 0 8px;font-size:14px;font-weight:700;color:#0c1d3a">If you need to make changes?</p>
    <p style="margin:0 0 12px;font-size:14px;color:#374151">
      To reschedule or cancel, please contact us at least <strong>24 hours</strong> before your appointment:
    </p>
    <ul style="margin:0 0 28px;padding-left:22px;font-size:14px;list-style:none;padding-left:0">
      <li style="margin-bottom:8px">&#x1F310;&nbsp; <a href="{_LUMI_PORTAL}" style="color:#0c1d3a;text-decoration:underline">{_LUMI_WEBSITE}</a></li>
      <li style="margin-bottom:8px">&#x1F4DE;&nbsp; {_escape(_LUMI_PHONE)}</li>
    </ul>

  </div>

  <!-- Footer -->
  <div style="background:#f9fafb;padding:22px 32px;border-top:1px solid #e5e7eb;text-align:center">
    <p style="margin:0 0 4px;font-size:14px;color:#374151;font-weight:600">We look forward to seeing you and your car!</p>
    <p style="margin:0 0 16px;font-size:14px;color:#374151">The Lumi Car Spa Team</p>
    <p style="color:#9ca3af;font-size:11px;margin:0">This is an automated message &mdash; please do not reply directly to this email.</p>
  </div>

</body></html>"""
    _send(to_email, f"Your Car Wash Booking is Confirmed – {ref}", plain, html)


def send_booking_rescheduled_email(
    to_email: str,
    name: str,
    new_date: str,
    new_start_time: str,
    booking_id: str,
    customer_id: str | None = None,
    phone: str | None = None,
    new_end_time: str | None = None,
    service_summary: str = "",
    channel: str = "branch",
) -> None:
    display = name.strip() if name and name.strip() else "Valued Customer"
    first_name = _display_first_name(display)
    ref = short_booking_ref(booking_id, customer_id, phone)
    date_display = _format_email_date(new_date)
    time_display = _format_email_time(new_start_time)
    service_display = service_summary.strip() or "—"
    duration_label = _compute_duration_label(new_start_time, new_end_time) if new_end_time else ""

    is_mobile = channel == "mobile"
    channel_note = "Mobile Service" if is_mobile else "Branch Service"

    duration_row_plain = f"  Est. Duration: {duration_label}\n" if duration_label else ""
    duration_row_html = (
        f'<tr><td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Est. Duration</td>'
        f'<td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(duration_label)}</td></tr>'
        if duration_label else ""
    )

    if is_mobile:
        what_next = [
            "Please ensure you are available at the service address at the new scheduled time",
            "Clear access to your vehicle before the team arrives",
            "Our team will notify you when they are on their way",
        ]
    else:
        what_next = [
            "Please arrive 5 minutes before your new appointment time",
            "Leave your windows fully closed and remove any valuables from your vehicle",
            "Our team will notify you as soon as your car is ready for pickup",
        ]

    what_next_plain = "\n".join(f"  • {pt}" for pt in what_next)
    what_next_html = "\n".join(
        f'<li style="margin-bottom:6px">{_escape(pt)}</li>' for pt in what_next
    )

    plain = (
        f"Your Appointment Has Been Rescheduled\n\n"
        f"Hi {first_name},\n\n"
        "Your car wash appointment has been moved to a new date and time. "
        "Here are your updated booking details:\n\n"
        "──────────────────────────────────\n"
        "📋 Updated Booking Details\n"
        "──────────────────────────────────\n"
        f"  New Date:   {date_display}\n"
        f"  New Time:   {time_display}\n"
        f"  Service:    {service_display}\n"
        f"  Booking ID: {ref}\n"
        f"{duration_row_plain}"
        "\n──────────────────────────────────\n"
        "📍 What's Next\n"
        "──────────────────────────────────\n"
        f"{what_next_plain}\n\n"
        "If you did not request this change, please contact us immediately.\n\n"
        "— Lumi Car Spa"
    )

    html = f"""
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#111;max-width:640px;margin:0 auto;padding:0;line-height:1.6;font-size:15px">
  <div style="background:#0c1d3a;padding:28px 32px 22px">
    <p style="color:#c9a84c;margin:0 0 4px;font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase">Lumi Car Spa &nbsp;·&nbsp; {_escape(channel_note)}</p>
    <p style="color:#fff;margin:0;font-size:22px;font-weight:700">Your Appointment Has Been Rescheduled 🔄</p>
  </div>
  <div style="padding:28px 32px 8px;background:#fff">
    <p style="margin:0 0 6px">Hi <strong>{_escape(first_name)}</strong>,</p>
    <p style="margin:0 0 24px;color:#374151">Your car wash appointment has been moved to a new date and time. Here are your updated booking details:</p>

    <p style="margin:0 0 10px;font-size:13px;font-weight:700;color:#0c1d3a;text-transform:uppercase;letter-spacing:.08em">📋 Updated Booking Details</p>
    <table style="border-collapse:collapse;width:100%;font-size:14px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;margin-bottom:24px">
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;width:140px;white-space:nowrap">New Date</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(date_display)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">New Time</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(time_display)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Service</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(service_display)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;white-space:nowrap">Booking ID</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-family:monospace;font-size:13px">{_escape(ref)}</td>
      </tr>
      {duration_row_html}
    </table>

    <p style="margin:0 0 10px;font-size:13px;font-weight:700;color:#0c1d3a;text-transform:uppercase;letter-spacing:.08em">📍 What's Next</p>
    <ul style="margin:0 0 28px;padding-left:20px;color:#374151;font-size:14px">
      {what_next_html}
    </ul>

    <div style="background:#fff7ed;border-left:4px solid #f97316;border-radius:4px;padding:14px 18px;margin-bottom:24px">
      <p style="color:#92400e;margin:0;font-size:13px">If you did not request this change, please contact us immediately.</p>
    </div>
  </div>
  <div style="background:#f9fafb;padding:18px 32px;border-top:1px solid #e5e7eb">
    <p style="color:#9ca3af;font-size:12px;margin:0;text-align:center">
      This is an automated message — please do not reply.
    </p>
  </div>
</body></html>"""
    _send(to_email, f"Your Appointment Has Been Rescheduled – {ref} | Lumi Car Spa", plain, html)


def _otp_block(otp: str) -> str:
    """Reusable HTML block that displays the OTP code prominently."""
    return (
        f'<div style="margin:28px 0;text-align:center">'
        f'<span style="display:inline-block;background:#f3f4f6;border:2px dashed #d1d5db;'
        f'border-radius:12px;padding:18px 40px;font-size:36px;font-weight:700;'
        f'letter-spacing:10px;color:#1e293b">{otp}</span></div>'
    )


def send_otp_email(to_email: str, name: str, otp: str) -> None:
    """Password-reset OTP — only used by the forgot-password flow."""
    display = name.strip() if name and name.strip() else "there"
    plain = (
        f"Hi {display},\n\n"
        f"Your CarWash password reset code is: {otp}\n\n"
        "This code expires in 10 minutes.\n"
        "If you did not request a password reset, please ignore this email."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#4F46E5;margin-bottom:8px">Password Reset Code</h2>
<p>Hi {display},</p>
<p>Use the code below to reset your CarWash password. It expires in <strong>10 minutes</strong>.</p>
{_otp_block(otp)}
<p style="color:#6b7280;font-size:14px">If you did not request a password reset, you can safely ignore this email.</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Your CarWash Password Reset Code", plain, html, dev_otp=otp)


def send_signup_otp_email(to_email: str, otp: str) -> None:
    """Account-creation OTP — sent during the signup email-verification step."""
    plain = (
        f"Welcome to CarWash!\n\n"
        f"Your verification code is: {otp}\n\n"
        "Use this code to verify your email address and complete account creation.\n"
        "The code expires in 10 minutes.\n\n"
        "If you did not create a CarWash account, you can safely ignore this email."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#0c1d3a;margin-bottom:8px">Verify Your CarWash Account</h2>
<p>Thanks for signing up! Enter the code below to verify your email address and complete your account creation.</p>
{_otp_block(otp)}
<p style="color:#6b7280;font-size:14px">This code expires in <strong>10 minutes</strong>.</p>
<p style="color:#6b7280;font-size:14px">If you did not sign up for CarWash, you can safely ignore this email.</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Verify Your CarWash Account", plain, html, dev_otp=otp)


def send_email_change_otp_email(to_email: str, name: str, otp: str) -> None:
    """Email-change OTP — sent to the *new* address to confirm ownership."""
    display = name.strip() if name and name.strip() else "there"
    plain = (
        f"Hi {display},\n\n"
        f"Your CarWash email change verification code is: {otp}\n\n"
        "Use this code to verify your new email address. It expires in 10 minutes.\n\n"
        "If you did not request an email address change, please ignore this email "
        "and your current email will remain unchanged."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;padding:24px">
<h2 style="color:#0c1d3a;margin-bottom:8px">Verify Your New Email Address</h2>
<p>Hi {display},</p>
<p>Use the code below to confirm your new email address for your CarWash account. It expires in <strong>10 minutes</strong>.</p>
{_otp_block(otp)}
<p style="color:#6b7280;font-size:14px">If you did not request this change, you can safely ignore this email — your current email address will remain unchanged.</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:32px 0">
<p style="color:#9ca3af;font-size:12px">This is an automated message, please do not reply.</p>
</body></html>"""
    _send(to_email, "Verify Your New CarWash Email Address", plain, html, dev_otp=otp)


def send_loyalty_reward_email(
    to_email: str,
    name: str,
    service_name: str,
    channel: str = "branch",
) -> None:
    """Dedicated loyalty reward unlock email — not a generic booking email."""
    display = name.strip() if name and name.strip() else "Valued Customer"
    channel_label = "at our branch" if channel == "branch" else "via our mobile service"
    channel_badge = "Branch Service" if channel == "branch" else "Mobile Service"
    plain = (
        f"Congratulations, {display}!\n\n"
        f"You've earned a FREE loyalty reward: {service_name}!\n\n"
        f"This reward is now available in your Coonara Hand Car Wash account.\n"
        f"On your next eligible booking {channel_label}, simply select '{service_name}' "
        f"and the price will automatically display as $0.00 — no coupon code needed.\n\n"
        "Your reward can only be redeemed once, so make it count!\n\n"
        "Thank you for being a loyal Coonara customer.\n\n"
        "— The Coonara Hand Car Wash Team"
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#1a1a1a;max-width:600px;margin:auto;padding:0">
  <div style="background:#0c1d3a;padding:28px 32px 20px">
    <h1 style="color:#c9a84c;margin:0;font-size:22px;letter-spacing:0.5px">Coonara Hand Car Wash</h1>
    <p style="color:#8fa8c8;margin:6px 0 0;font-size:13px">Loyalty Rewards</p>
  </div>
  <div style="padding:32px 32px 24px;background:#ffffff">
    <div style="text-align:center;margin-bottom:28px">
      <div style="font-size:48px;margin-bottom:8px">🏆</div>
      <h2 style="color:#0c1d3a;margin:0 0 6px;font-size:24px">Congratulations, {display}!</h2>
      <p style="color:#4b5563;margin:0;font-size:15px">You've unlocked a free loyalty reward</p>
    </div>
    <div style="background:#fefce8;border:2px solid #c9a84c;border-radius:12px;padding:20px 24px;text-align:center;margin-bottom:24px">
      <p style="color:#92400e;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin:0 0 8px">Your Free Reward</p>
      <p style="color:#0c1d3a;font-size:22px;font-weight:700;margin:0 0 6px">{service_name}</p>
      <span style="display:inline-block;background:#0c1d3a;color:#c9a84c;font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px;letter-spacing:0.5px">{channel_badge}</span>
    </div>
    <div style="background:#f0fdf4;border-left:4px solid #16a34a;border-radius:4px;padding:16px 20px;margin-bottom:24px">
      <p style="color:#15803d;font-weight:600;margin:0 0 6px;font-size:14px">How to redeem</p>
      <ol style="color:#166534;margin:0;padding-left:18px;font-size:14px;line-height:1.8">
        <li>Log in to your Coonara account</li>
        <li>Book a service {channel_label}</li>
        <li>Select <strong>{service_name}</strong> — the price will automatically show <strong>$0.00</strong></li>
        <li>Complete your booking — reward consumed!</li>
      </ol>
    </div>
    <p style="color:#6b7280;font-size:13px;line-height:1.6;margin:0">
      This is a <strong>one-time reward</strong>. After redemption, your loyalty counter resets and
      you'll start earning toward your next reward automatically.
    </p>
  </div>
  <div style="background:#f9fafb;padding:20px 32px;border-top:1px solid #e5e7eb">
    <p style="color:#9ca3af;font-size:12px;margin:0;text-align:center">
      This is an automated message from Coonara Hand Car Wash. Please do not reply.
    </p>
  </div>
</body></html>"""
    _send(to_email, "🏆 You've Earned a Free Reward! — Coonara Hand Car Wash", plain, html)


def send_contact_inquiry_email(
    to_email: str,
    sender_name: str,
    sender_phone: str,
    service_required: str,
    preferred_date: str,
) -> None:
    """Send a contact / free-quote inquiry from the landing page to the business owner."""
    name = sender_name.strip() or "—"
    phone = sender_phone.strip() or "—"
    service = service_required.strip() or "—"
    date = preferred_date.strip() or "—"

    plain = (
        "New Free Quote Inquiry — Lumi Car Spa\n"
        "=======================================\n\n"
        f"  Name:             {name}\n"
        f"  Phone:            {phone}\n"
        f"  Service Required: {service}\n"
        f"  Preferred Date:   {date}\n\n"
        "Please follow up with this customer at your earliest convenience."
    )
    html = f"""
<html><body style="font-family:Arial,sans-serif;color:#1a1a1a;max-width:600px;margin:auto;padding:0">
  <div style="background:#0c1d3a;padding:28px 32px 20px">
    <h1 style="color:#c9a84c;margin:0;font-size:22px;letter-spacing:0.5px">Lumi Car Spa</h1>
    <p style="color:#8fa8c8;margin:6px 0 0;font-size:13px">New Quote Inquiry</p>
  </div>
  <div style="padding:32px 32px 24px;background:#ffffff">
    <h2 style="color:#0c1d3a;margin:0 0 6px;font-size:20px">📋 New Free Quote Request</h2>
    <p style="color:#4b5563;margin:0 0 24px;font-size:14px">A customer has submitted a free quote inquiry from your website.</p>
    <table style="border-collapse:collapse;width:100%;font-size:15px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
      <tr>
        <td style="padding:12px 16px;background:#f9fafb;font-weight:600;width:160px;border-bottom:1px solid #e5e7eb">Name</td>
        <td style="padding:12px 16px;border-bottom:1px solid #e5e7eb">{name}</td>
      </tr>
      <tr>
        <td style="padding:12px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Phone</td>
        <td style="padding:12px 16px;border-bottom:1px solid #e5e7eb">{phone}</td>
      </tr>
      <tr>
        <td style="padding:12px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Service Required</td>
        <td style="padding:12px 16px;border-bottom:1px solid #e5e7eb">{service}</td>
      </tr>
      <tr>
        <td style="padding:12px 16px;background:#f9fafb;font-weight:600">Preferred Date</td>
        <td style="padding:12px 16px">{date}</td>
      </tr>
    </table>
    <div style="margin-top:24px;background:#eff6ff;border-left:4px solid #3b82f6;border-radius:4px;padding:14px 18px">
      <p style="color:#1e40af;margin:0;font-size:14px">Please follow up with this customer at your earliest convenience.</p>
    </div>
  </div>
  <div style="background:#f9fafb;padding:20px 32px;border-top:1px solid #e5e7eb">
    <p style="color:#9ca3af;font-size:12px;margin:0;text-align:center">
      This inquiry was submitted via the Lumi Car Spa website contact form.
    </p>
  </div>
</body></html>"""
    _send(to_email, "📋 New Quote Inquiry — Lumi Car Spa", plain, html)


def send_staff_booking_notification(
    db,
    *,
    event: str,                          # "new_booking" | "rescheduled"
    booking_type: str,                   # "branch" | "mobile"
    booking_id: str,
    customer_name: str,
    phone: str,
    vehicle_type: str,
    vehicle_model: str,
    registration_number: str,
    service_summary: str,
    slot_date: str,
    start_time: str,
    end_time: str,
    branch_id: str | None = None,
    city_pin_code: str | None = None,
    old_slot_date: str | None = None,
    old_start_time: str | None = None,
    customer_id: str | None = None,
) -> None:
    """
    Notify all DB admins and the relevant branch/mobile manager(s) when a booking
    is created or rescheduled.  The env-based super admin (no email in DB) is excluded.
    Emails are collected synchronously then sent in background threads.
    """
    from app.models.admin_account import AdminAccount
    from app.models.staff import BranchManager
    from app.models.mobile import MobileServiceManager
    from app.models.branch import Branch

    recipient_emails: list[str] = []

    # All active DB admins (env admin has no AdminAccount row — naturally excluded)
    try:
        admins = db.query(AdminAccount).filter(AdminAccount.active == True).all()  # noqa: E712
        for a in admins:
            if a.email and a.email.strip():
                recipient_emails.append(a.email.strip().lower())
    except Exception as exc:
        logger.warning("Staff notification: could not query admin accounts: %s", exc)

    # Relevant manager(s) based on booking type
    if booking_type == "branch" and branch_id:
        try:
            managers = (
                db.query(BranchManager)
                .filter(BranchManager.branch_id == branch_id, BranchManager.active == True)  # noqa: E712
                .all()
            )
            for m in managers:
                if m.email and m.email.strip():
                    recipient_emails.append(m.email.strip().lower())
        except Exception as exc:
            logger.warning("Staff notification: could not query branch managers: %s", exc)
    elif booking_type == "mobile" and city_pin_code:
        try:
            managers = (
                db.query(MobileServiceManager)
                .filter(MobileServiceManager.city_pin_code == city_pin_code, MobileServiceManager.active == True)  # noqa: E712
                .all()
            )
            for m in managers:
                if m.email and m.email.strip():
                    recipient_emails.append(m.email.strip().lower())
        except Exception as exc:
            logger.warning("Staff notification: could not query mobile managers: %s", exc)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_recipients: list[str] = []
    for e in recipient_emails:
        if e not in seen:
            seen.add(e)
            unique_recipients.append(e)

    if not unique_recipients:
        return

    # Resolve branch/area label for the email body
    location_label = ""
    if booking_type == "branch" and branch_id:
        try:
            b = db.query(Branch).filter(Branch.id == branch_id).one_or_none()
            if b:
                location_label = str(b.name or "").strip()
        except Exception:
            pass
    elif booking_type == "mobile" and city_pin_code:
        location_label = f"Mobile — {city_pin_code}"

    ref = short_booking_ref(booking_id, customer_id, phone or None)
    date_display = _format_email_date(slot_date)
    time_display = _format_email_time(start_time)
    end_display = _format_email_time(end_time)
    svc = (service_summary or "—").strip()
    veh = " ".join(filter(None, [vehicle_type, vehicle_model])).strip() or "—"
    reg = (registration_number or "—").strip()
    cust = (customer_name or "—").strip()
    ph = (phone or "—").strip()

    if event == "new_booking":
        subject = f"New Booking — {ref}"
        event_label = "New Booking"
        event_colour = "#16a34a"
        event_icon = ""
        change_block_plain = ""
        change_block_html = ""
    else:
        subject = f"Booking Rescheduled — {ref}"
        event_label = "Booking Rescheduled"
        event_colour = "#d97706"
        event_icon = ""
        old_date_d = _format_email_date(old_slot_date or "")
        old_time_d = _format_email_time(old_start_time or "")
        change_block_plain = (
            f"\n  Previous Date:  {old_date_d}\n"
            f"  Previous Time:  {old_time_d}\n"
        )
        change_block_html = f"""
  <tr>
    <td style="padding:10px 16px;background:#fef9c3;font-weight:600;border-bottom:1px solid #e5e7eb">Previous Date</td>
    <td style="padding:10px 16px;background:#fef9c3;border-bottom:1px solid #e5e7eb;color:#92400e">{_escape(old_date_d)}</td>
  </tr>
  <tr>
    <td style="padding:10px 16px;background:#fef9c3;font-weight:600;border-bottom:1px solid #e5e7eb">Previous Time</td>
    <td style="padding:10px 16px;background:#fef9c3;border-bottom:1px solid #e5e7eb;color:#92400e">{_escape(old_time_d)}</td>
  </tr>"""

    channel_label = "Branch" if booking_type == "branch" else "Mobile Service"

    plain = (
        f"{(event_icon + ' ') if event_icon else ''}{event_label} — {channel_label}\n"
        f"{'='*50}\n\n"
        f"  Booking Ref:    {ref}\n"
        f"  Location:       {location_label or channel_label}\n"
        f"{change_block_plain}"
        f"  Date:           {date_display}\n"
        f"  Time:           {time_display} – {end_display}\n"
        f"  Service:        {svc}\n\n"
        f"  Customer:       {cust}\n"
        f"  Phone:          {ph}\n"
        f"  Vehicle:        {veh}\n"
        f"  Registration:   {reg}\n\n"
        f"Please log in to the admin/manager portal to view full details."
    )

    html_body = f"""
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#111;max-width:640px;margin:0 auto;padding:0;line-height:1.5;font-size:15px">
  <div style="background:#0c1d3a;padding:24px 32px 18px">
    <p style="color:#c9a84c;margin:0;font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase">{_escape(channel_label)}</p>
    <p style="color:#fff;margin:6px 0 0;font-size:20px;font-weight:700">{_escape(event_label)}</p>
  </div>
  <div style="padding:28px 32px 8px;background:#fff">
    <p style="margin:0 0 6px;font-size:13px;color:#6b7280">Booking Reference</p>
    <p style="margin:0 0 24px;font-size:22px;font-weight:700;color:#0c1d3a">{_escape(ref)}</p>

    <p style="margin:0 0 10px;font-size:14px;font-weight:700;color:{event_colour};text-transform:uppercase;letter-spacing:.05em">Slot Details</p>
    <table style="border-collapse:collapse;width:100%;font-size:14px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;margin-bottom:24px">
      {change_block_html}
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;width:160px">Location</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(location_label or channel_label)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Date</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(date_display)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Time</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0c1d3a">{_escape(time_display)} – {_escape(end_display)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600">Service</td>
        <td style="padding:10px 16px">{_escape(svc)}</td>
      </tr>
    </table>

    <p style="margin:0 0 10px;font-size:14px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.05em">Customer Details</p>
    <table style="border-collapse:collapse;width:100%;font-size:14px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;margin-bottom:28px">
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb;width:160px">Name</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(cust)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Phone</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(ph)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600;border-bottom:1px solid #e5e7eb">Vehicle</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{_escape(veh)}</td>
      </tr>
      <tr>
        <td style="padding:10px 16px;background:#f9fafb;font-weight:600">Registration</td>
        <td style="padding:10px 16px">{_escape(reg)}</td>
      </tr>
    </table>
  </div>
  <div style="background:#f9fafb;padding:18px 32px;border-top:1px solid #e5e7eb">
    <p style="color:#9ca3af;font-size:12px;margin:0;text-align:center">
      This is an automated staff notification from the CarWash booking system. Please do not reply.
    </p>
  </div>
</body></html>"""

    for to_email in unique_recipients:
        _send(to_email, subject, plain, html_body)


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

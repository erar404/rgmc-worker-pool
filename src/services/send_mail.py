"""Developer alert emails for the worker pool (errors and successes)."""
import logging
import smtplib
import ssl
import threading
from datetime import datetime, timezone

import src.config as config

logger = logging.getLogger("send_mail")


def notify_error(title: str, detail: str, context: str = "") -> None:
    """Send a developer alert email for a worker error.

    Silently skips if DEVELOPER_EMAIL or SMTP credentials are not configured.
    Must be called from a daemon thread or directly — never blocks the Pub/Sub callback.
    """
    if not config.developer_email or not config.smtp_user or not config.smtp_password:
        return
    threading.Thread(
        target=_send,
        args=(title, detail, context),
        daemon=True,
    ).start() 


def notify_success(title: str, detail: str, context: str = "") -> None:
    """Send a developer notification email for a successful sync operation.

    Silently skips if DEVELOPER_EMAIL or SMTP credentials are not configured.
    Fires in a daemon thread — never blocks the Pub/Sub callback.
    """
    if not config.developer_email or not config.smtp_user or not config.smtp_password:
        return
    threading.Thread(
        target=_send_success,
        args=(title, detail, context),
        daemon=True,
    ).start()


def _send(title: str, detail: str, context: str) -> None:
    from email.message import EmailMessage

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[RGMC Worker Pool] {title}"

    detail_html = (
        detail.replace("&", "&amp;")
              .replace("<", "&lt;")
              .replace(">", "&gt;")
              .replace("\n", "<br>")
    )
    context_html = (
        context.replace("&", "&amp;")
               .replace("<", "&lt;")
               .replace(">", "&gt;")
    )

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,sans-serif;">
        <table align="center" width="100%" cellpadding="0" cellspacing="0"
               style="max-width:640px;background:#ffffff;margin-top:20px;border-radius:8px;overflow:hidden;">
            <tr>
                <td style="background-color:#c0392b;padding:20px;text-align:center;color:white;font-size:18px;font-weight:bold;">
                    RGMC Worker Pool — Error
                </td>
            </tr>
            <tr>
                <td style="padding:24px;color:#333333;font-size:14px;line-height:1.7;">
                    <table width="100%" cellpadding="6" cellspacing="0">
                        <tr><td style="color:#777;width:120px;">Timestamp</td><td><strong>{timestamp}</strong></td></tr>
                        <tr><td style="color:#777;">Title</td><td><strong>{title}</strong></td></tr>
                        <tr><td style="color:#777;">Context</td><td>{context_html or "—"}</td></tr>
                    </table>
                    <hr style="margin:20px 0;border:none;border-top:1px solid #eeeeee;">
                    <p style="margin:0 0 8px;color:#777;font-size:12px;text-transform:uppercase;letter-spacing:.05em;">Error Detail</p>
                    <pre style="background:#f8f8f8;padding:16px;border-radius:4px;overflow:auto;font-size:12px;
                                font-family:'Courier New',monospace;white-space:pre-wrap;word-break:break-all;">{detail_html}</pre>
                </td>
            </tr>
            <tr>
                <td style="background-color:#eeeeee;padding:14px;text-align:center;font-size:11px;color:#777777;">
                    &copy; {datetime.now(timezone.utc).year} RGMC Group IT Department &mdash; automated alert, do not reply.
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

    msg = EmailMessage()
    msg["From"] = config.smtp_user
    msg["To"] = config.developer_email
    msg["Subject"] = subject
    msg.set_content(f"[{timestamp}] {title}\n\nContext: {context or '—'}\n\n{detail}")
    msg.add_alternative(html_content, subtype="html")

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls(ctx)
            server.login(config.smtp_user, config.smtp_password)
            server.send_message(msg)
    except Exception as exc:
        logger.error(f"notify_error: failed to send alert email: {exc}")


def _send_success(title: str, detail: str, context: str) -> None:
    from email.message import EmailMessage

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[RGMC Worker Pool] {title}"

    detail_html = (
        detail.replace("&", "&amp;")
              .replace("<", "&lt;")
              .replace(">", "&gt;")
              .replace("\n", "<br>")
    )
    context_html = (
        context.replace("&", "&amp;")
               .replace("<", "&lt;")
               .replace(">", "&gt;")
    )

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,sans-serif;">
        <table align="center" width="100%" cellpadding="0" cellspacing="0"
               style="max-width:640px;background:#ffffff;margin-top:20px;border-radius:8px;overflow:hidden;">
            <tr>
                <td style="background-color:#27ae60;padding:20px;text-align:center;color:white;font-size:18px;font-weight:bold;">
                    RGMC Worker Pool — Success
                </td>
            </tr>
            <tr>
                <td style="padding:24px;color:#333333;font-size:14px;line-height:1.7;">
                    <table width="100%" cellpadding="6" cellspacing="0">
                        <tr><td style="color:#777;width:120px;">Timestamp</td><td><strong>{timestamp}</strong></td></tr>
                        <tr><td style="color:#777;">Title</td><td><strong>{title}</strong></td></tr>
                        <tr><td style="color:#777;">Context</td><td>{context_html or "—"}</td></tr>
                    </table>
                    <hr style="margin:20px 0;border:none;border-top:1px solid #eeeeee;">
                    <p style="margin:0 0 8px;color:#777;font-size:12px;text-transform:uppercase;letter-spacing:.05em;">Summary</p>
                    <pre style="background:#f8f8f8;padding:16px;border-radius:4px;overflow:auto;font-size:12px;
                                font-family:'Courier New',monospace;white-space:pre-wrap;word-break:break-all;">{detail_html}</pre>
                </td>
            </tr>
            <tr>
                <td style="background-color:#eeeeee;padding:14px;text-align:center;font-size:11px;color:#777777;">
                    &copy; {datetime.now(timezone.utc).year} RGMC Group IT Department &mdash; automated alert, do not reply.
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

    msg = EmailMessage()
    msg["From"] = config.smtp_user
    msg["To"] = config.developer_email
    msg["Subject"] = subject
    msg.set_content(f"[{timestamp}] {title}\n\nContext: {context or '—'}\n\n{detail}")
    msg.add_alternative(html_content, subtype="html")

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls(ctx)
            server.login(config.smtp_user, config.smtp_password)
            server.send_message(msg)
    except Exception as exc:
        logger.error(f"notify_success: failed to send alert email: {exc}")

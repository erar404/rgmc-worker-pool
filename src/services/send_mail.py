"""Developer alert emails for the worker pool (errors, successes, and warnings).

Every notification is tagged with a `module` (BigQuery Bridge, POUL SO Import, Order
Task, Sync Worker, ...) that drives a distinct header color + icon, independent of the
severity badge (success/error/warning) shown in the body — so an inbox full of these
can be told apart by WHICH PIPELINE sent them at a glance, and by WHAT HAPPENED once
opened, without the two dimensions fighting over the same header color. See _MODULES
below; rgmc-gcp-api's own src/services/send_mail.py uses the same palette/icons for the
modules both repos share (BigQuery Bridge) so an email looks the same regardless of
which service actually sent it.
"""
import logging
import smtplib
import ssl
import threading
from datetime import datetime, timezone
from email.message import EmailMessage

import src.config as config

logger = logging.getLogger("send_mail")

# module key -> (display label, header icon, header/accent color). Keys are passed as
# the `module=` argument to notify_error/notify_success/notify_warning; an unknown or
# omitted key falls back to _DEFAULT_MODULE, never raises.
_MODULES: dict[str, tuple[str, str, str]] = {
    "bigquery_bridge": ("BigQuery Bridge", "\U0001F309", "#4338ca"),      # bridge (indigo)
    "poul_so_import": ("POUL SO Import", "\U0001F4E6", "#0f766e"),       # package (teal)
    "order_task": ("Order Task", "\U0001F9FE", "#7c3aed"),               # receipt (violet)
    "sync_worker": ("Sync Worker", "\U0001F501", "#b45309"),             # repeat (amber)
}
_DEFAULT_MODULE: tuple[str, str, str] = ("Worker Pool", "⚙️", "#334155")  # gear (slate)

# severity -> (badge text, badge background, badge text color) — shown as a small pill
# in the body, separate from the module's own header color/icon above.
_SEVERITIES: dict[str, tuple[str, str, str]] = {
    "error": ("ERROR", "#fee2e2", "#991b1b"),
    "success": ("SUCCESS", "#dcfce7", "#166534"),
    "warning": ("NEEDS ATTENTION", "#fef3c7", "#92400e"),
}


def notify_error(
    title: str, detail: str, context: str = "", extra_recipients: list[str] | None = None, module: str = "",
) -> None:
    """Send a developer alert email for a worker error.

    module (e.g. "bigquery_bridge", "poul_so_import", "order_task", "sync_worker") picks
    this notification's header color/icon — see _MODULES. Left blank, the email still
    sends with a generic "Worker Pool" identity rather than failing.
    extra_recipients are added alongside DEVELOPER_EMAIL (e.g. the employee who
    triggered a manual reprocess, so they see the outcome too).
    Silently skips if DEVELOPER_EMAIL or SMTP credentials are not configured.
    Must be called from a daemon thread or directly — never blocks the Pub/Sub callback.
    """
    if not config.developer_email or not config.smtp_user or not config.smtp_password:
        return
    threading.Thread(
        target=_send_email,
        args=("error", module, title, detail, context, extra_recipients),
        daemon=True,
    ).start()


def notify_success(
    title: str, detail: str, context: str = "", extra_recipients: list[str] | None = None, module: str = "",
) -> None:
    """Send a developer notification email for a successful sync operation.

    module (e.g. "bigquery_bridge", "poul_so_import", "order_task", "sync_worker") picks
    this notification's header color/icon — see _MODULES. Left blank, the email still
    sends with a generic "Worker Pool" identity rather than failing.
    extra_recipients are added alongside DEVELOPER_EMAIL (e.g. the employee who
    triggered a manual reprocess, so they see the outcome too).
    Silently skips if DEVELOPER_EMAIL or SMTP credentials are not configured.
    Fires in a daemon thread — never blocks the Pub/Sub callback.
    """
    if not config.developer_email or not config.smtp_user or not config.smtp_password:
        return
    threading.Thread(
        target=_send_email,
        args=("success", module, title, detail, context, extra_recipients),
        daemon=True,
    ).start()


def notify_warning(
    title: str, detail: str, context: str = "", extra_recipients: list[str] | None = None, module: str = "",
) -> None:
    """Send a developer notification email for a partial/needs-attention outcome
    (e.g. an order was inserted into BC but some lines were left out).

    module (e.g. "bigquery_bridge", "poul_so_import", "order_task", "sync_worker") picks
    this notification's header color/icon — see _MODULES. Left blank, the email still
    sends with a generic "Worker Pool" identity rather than failing.
    extra_recipients are added alongside DEVELOPER_EMAIL (e.g. the employee who
    triggered a manual reprocess, so they see the outcome too).
    Silently skips if DEVELOPER_EMAIL or SMTP credentials are not configured.
    Fires in a daemon thread — never blocks the Pub/Sub callback.
    """
    if not config.developer_email or not config.smtp_user or not config.smtp_password:
        return
    threading.Thread(
        target=_send_email,
        args=("warning", module, title, detail, context, extra_recipients),
        daemon=True,
    ).start()


def _headers(extra_recipients: list[str] | None) -> tuple[str, str]:
    """Return (to, cc).

    When extra_recipients (e.g. the employee who triggered a manual reprocess/sync)
    are given, they become the To — the developer is always Cc'd instead of blended
    into To, so the employee sees themselves as the primary recipient and the
    developer as an observer, on every single notification a trigger can produce.
    With no extra_recipients (a purely internal/automatic notification — no one
    "triggered" this), DEVELOPER_EMAIL is the sole To, unchanged from before.
    """
    extras: list[str] = []
    seen_lower: set[str] = set()
    for addr in extra_recipients or []:
        addr = (addr or "").strip()
        if addr and addr.lower() not in seen_lower:
            seen_lower.add(addr.lower())
            extras.append(addr)
    if extras:
        return ", ".join(extras), config.developer_email
    return config.developer_email, ""


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _send_email(
    severity: str,
    module: str,
    title: str,
    detail: str,
    context: str,
    extra_recipients: list[str] | None = None,
) -> None:
    """Build and send one notification email. Shared by notify_error/success/warning —
    severity picks the body's status badge, module picks the header's color/icon/label
    (see the module docstring at the top of this file for why these are kept separate).
    """
    module_label, module_icon, module_color = _MODULES.get(module, _DEFAULT_MODULE)
    badge_text, badge_bg, badge_fg = _SEVERITIES.get(severity, _SEVERITIES["error"])

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[RGMC Worker Pool · {module_label}] {title}"

    detail_html = _escape(detail).replace("\n", "<br>")
    context_html = _escape(context)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,sans-serif;">
        <table align="center" width="100%" cellpadding="0" cellspacing="0"
               style="max-width:640px;background:#ffffff;margin-top:20px;border-radius:8px;overflow:hidden;
                      border-top:4px solid {module_color};">
            <tr>
                <td style="background-color:{module_color};padding:18px 20px;color:white;">
                    <span style="font-size:22px;vertical-align:middle;">{module_icon}</span>
                    <span style="font-size:17px;font-weight:bold;vertical-align:middle;margin-left:8px;">
                        RGMC Worker Pool &mdash; {module_label}
                    </span>
                </td>
            </tr>
            <tr>
                <td style="padding:24px;color:#333333;font-size:14px;line-height:1.7;">
                    <span style="display:inline-block;background:{badge_bg};color:{badge_fg};
                                 font-size:11px;font-weight:bold;letter-spacing:.05em;text-transform:uppercase;
                                 padding:4px 10px;border-radius:999px;margin-bottom:14px;">
                        {badge_text}
                    </span>
                    <table width="100%" cellpadding="6" cellspacing="0">
                        <tr><td style="color:#777;width:120px;">Timestamp</td><td><strong>{timestamp}</strong></td></tr>
                        <tr><td style="color:#777;">Title</td><td><strong>{_escape(title)}</strong></td></tr>
                        <tr><td style="color:#777;">Context</td><td>{context_html or "&mdash;"}</td></tr>
                    </table>
                    <hr style="margin:20px 0;border:none;border-top:1px solid #eeeeee;">
                    <p style="margin:0 0 8px;color:#777;font-size:12px;text-transform:uppercase;letter-spacing:.05em;">
                        {"Error Detail" if severity == "error" else "Summary"}
                    </p>
                    <pre style="background:#f8f8f8;padding:16px;border-radius:4px;overflow:auto;font-size:12px;
                                font-family:'Courier New',monospace;white-space:pre-wrap;word-break:break-all;
                                border-left:3px solid {module_color};">{detail_html}</pre>
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

    to_addr, cc_addr = _headers(extra_recipients)
    msg = EmailMessage()
    msg["From"] = config.smtp_user
    msg["To"] = to_addr
    if cc_addr:
        msg["Cc"] = cc_addr
    msg["Subject"] = subject
    msg.set_content(
        f"[{timestamp}] [{module_label} — {badge_text}] {title}\n\nContext: {context or '-'}\n\n{detail}"
    )
    msg.add_alternative(html_content, subtype="html", charset="utf-8")

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls(context=ctx)
            server.login(config.smtp_user, config.smtp_password)
            server.send_message(msg)
    except Exception as exc:
        logger.error(f"notify_{severity}: failed to send alert email: {exc}")

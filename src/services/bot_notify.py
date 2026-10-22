"""Compact Teams chat notifications via rgmc-it-bot — the chat counterpart to
send_mail.py's developer-alert emails. Same source data (so_import_worker.py's
_send_batch_notification/_send_unmatched_items_notification), posted to
rgmc-it-bot's webhook instead of/alongside SMTP, so a subscribed Teams channel
gets a compact card without needing to read an inbox. Fires in a daemon thread,
same as send_mail.py — never blocks the Pub/Sub callback, and any failure is
logged, never raised.
"""
import logging
import threading

import requests

import src.config as config

logger = logging.getLogger("bot_notify")


def _post(event: str, body: dict) -> None:
    if not config.RGMC_IT_BOT_URL:
        return
    try:
        resp = requests.post(
            f"{config.RGMC_IT_BOT_URL.rstrip('/')}/api/notify/{event}",
            json=body,
            headers={"x-api-key": config.RGMC_IT_BOT_API_KEY},
            timeout=15,
        )
        if resp.status_code >= 400:
            logger.error(f"bot_notify {event}: rgmc-it-bot returned {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        logger.error(f"bot_notify {event}: failed to reach rgmc-it-bot: {exc}")


def _order_result(r: dict) -> dict:
    return {
        "po_ref": r.get("po_ref", ""),
        "order_no": r.get("order_no", ""),
        "customer_name": r.get("customer_name", ""),
        "branch_name": r.get("branch_name", ""),
        "resolution_tier": r.get("resolution_tier"),
        "lines_created": r.get("lines_created", 0),
        "lines_skipped": r.get("lines_skipped", 0),
        "from_buffer": bool(r.get("from_buffer")),
        "unmatched_items": r.get("unmatched_items") or [],
    }


def notify_po_batch(
    successes: list[dict],
    errors: list[dict],
    company: str,
    src_company: str,
    label: str = "POUL SO Import",
) -> None:
    """Teams counterpart to so_import_worker.py's _send_batch_notification — same
    successes/errors, routed to channels subscribed via `subscribe po-all|po-bc|po-buffer`.
    """
    if not successes and not errors:
        return
    body = {
        "event": "po.batch",
        "label": label,
        "company": company,
        "src_company": src_company,
        "successes": [_order_result(r) for r in successes],
        "errors": [
            {"po_ref": e.get("po_ref", ""), "error": e.get("error", ""), "attempt_count": e.get("attempt_count")}
            for e in errors
        ],
    }
    threading.Thread(target=_post, args=("po-upload-batch", body), daemon=True).start()


def notify_po_unmatched(
    successes: list[dict],
    company: str,
    src_company: str,
    label: str = "POUL SO Import",
) -> None:
    """Teams counterpart to so_import_worker.py's _send_unmatched_items_notification."""
    flagged = [r for r in successes if r.get("unmatched_items")]
    if not flagged:
        return
    body = {
        "event": "po.unmatched",
        "label": label,
        "company": company,
        "src_company": src_company,
        "flagged": [_order_result(r) for r in flagged],
    }
    threading.Thread(target=_post, args=("po-upload-unmatched", body), daemon=True).start()

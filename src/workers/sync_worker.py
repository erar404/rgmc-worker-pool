"""Pub/Sub consumer for catalog and Firestore sync messages.

Subscribes to PUBSUB_SYNC_SUBSCRIPTION and dispatches based on the "type" field.

Message formats (JSON):

  Routine sync — all companies:
    { "type": "routine-sync", "on_date": "YYYY-MM-DD" }

  Single company item prices:
    { "type": "sync-item-prices", "company": "RGMC", "on_date": "YYYY-MM-DD", "page_size": 500 }

  Single company price list headers:
    { "type": "sync-price-list-headers", "company": "RGMC" }

  Single company price list items (omit price_list_code to sync all codes):
    { "type": "sync-price-list-items", "company": "RGMC", "price_list_code": "PLH001" }

  Connectivity test (bc-api → worker pool):
    { "type": "ping", "sent_at": "ISO8601", "sent_by": "bc-api", "note": "optional" }

Cloud Scheduler publishes { "type": "routine-sync" } to the rgmc-sync topic on a cron schedule.
The main API can publish any of the above to trigger targeted syncs or test connectivity.
"""
import datetime
import json
import logging

from google.cloud import pubsub_v1

from src import config
from src.services.bc_client import (
    fetch_price_list_headers,
    fetch_price_list_headers_with_lines,
    fetch_v3_catalog,
)
from src.services.gcs_catalog import save_catalog
from src.services.price_firestore_service import (
    sync_price_list_headers_to_firestore,
    sync_price_list_items_to_firestore,
    sync_prices_to_firestore,
)
from src.services.send_mail import notify_error, notify_success

logger = logging.getLogger("worker.sync")


def _sync_company(company: str, on_date: str, page_size: int = 500) -> None:
    """Sync price list headers then full item price catalog for a single company."""
    logger.info(f"[{company}] sync started — on_date={on_date!r}")

    try:
        headers_with_lines = fetch_price_list_headers_with_lines(company)
        headers = [{k: v for k, v in h.items() if k != "priceListLines"} for h in headers_with_lines]
        written = sync_price_list_headers_to_firestore(headers, company)
        logger.info(f"[{company}] {written} price list headers written")
        total_items = 0
        for header in headers_with_lines:
            code = header.get("code") or ""
            lines = header.get("priceListLines") or []
            if not (code and lines):
                continue
            written_items = sync_price_list_items_to_firestore(lines, company, code)
            total_items += written_items
            logger.info(f"[{company}] price list items [{code}]: {written_items} written")
        logger.info(f"[{company}] {total_items} price list items written total")
    except Exception as e:
        logger.error(f"[{company}] price list headers/items failed — {e}")

    try:
        records = fetch_v3_catalog(company, on_date)
        save_catalog(company, on_date, records)
        total = 0
        for i in range(0, len(records), page_size):
            chunk = records[i : i + page_size]
            total += sync_prices_to_firestore(chunk, company, on_date)
            logger.info(
                f"[{company}] item prices page {i // page_size + 1}: "
                f"{len(chunk)} records (offset={i})"
            )
        logger.info(f"[{company}] {total} item prices written total")
    except Exception as e:
        logger.error(f"[{company}] item prices failed — {e}")

    logger.info(f"[{company}] sync complete")


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"Sync message decode failed: {e} — acking to drop poison pill")
        message.ack()
        return

    msg_type: str = data.get("type", "routine-sync")
    on_date: str = data.get("on_date") or datetime.date.today().isoformat()
    page_size: int = min(int(data.get("page_size", 500)), 500)

    try:
        if msg_type == "routine-sync":
            companies = [
                c.strip()
                for c in (config.BC_COMPANIES or config.BC_COMPANY or "").split(",")
                if c.strip()
            ]
            for company in companies:
                _sync_company(company, on_date, page_size)
            notify_success(
                title="Routine Sync Complete",
                detail="\n".join(companies),
                context=f"on_date={on_date}",
            )

        elif msg_type == "sync-item-prices":
            company = data.get("company") or config.BC_COMPANY
            _sync_company(company, on_date, page_size)
            notify_success(
                title=f"Item Prices Sync Complete — {company}",
                detail=f"Company: {company}",
                context=f"on_date={on_date}",
            )

        elif msg_type == "sync-price-list-headers":
            company = data.get("company") or config.BC_COMPANY
            headers_with_lines = fetch_price_list_headers_with_lines(company)
            headers = [{k: v for k, v in h.items() if k != "priceListLines"} for h in headers_with_lines]
            written = sync_price_list_headers_to_firestore(headers, company)
            logger.info(f"[{company}] {written} price list headers written")
            notify_success(
                title=f"Price List Headers Sync Complete — {company}",
                detail=f"Company: {company}\n{written} headers written to Firestore",
            )

        elif msg_type == "sync-price-list-items":
            company = data.get("company") or config.BC_COMPANY
            price_list_code = data.get("price_list_code")
            headers_with_lines = fetch_price_list_headers_with_lines(company)
            total = 0
            for header in headers_with_lines:
                code = header.get("code") or ""
                lines = header.get("priceListLines") or []
                if not code:
                    continue
                if price_list_code and code != price_list_code:
                    continue
                written = sync_price_list_items_to_firestore(lines, company, code)
                total += written
                logger.info(f"[{company}] price list items [{code}]: {written} written")
            logger.info(f"[{company}] {total} price list items written total")
            notify_success(
                title=f"Price List Items Sync Complete — {company}",
                detail=f"Company: {company}\n{total} items written to Firestore",
                context=f"price_list_code={price_list_code or 'all'}",
            )

        elif msg_type == "ping":
            sent_at = data.get("sent_at", "unknown")
            sent_by = data.get("sent_by", "unknown")
            note = data.get("note", "")
            detail = f"Ping received from {sent_by}.\nSent at: {sent_at}"
            if note:
                detail += f"\nNote: {note}"
            notify_success(
                title="Worker Pool Ping ACK",
                detail=detail,
                context=f"subscription={config.PUBSUB_SYNC_SUBSCRIPTION}",
            )
            logger.info(f"Ping ACK — sent_by={sent_by!r} sent_at={sent_at!r}")

        else:
            logger.warning(f"Unknown sync message type: {msg_type!r} — acking to discard")

        message.ack()

    except Exception as e:
        logger.error(f"Sync message failed (type={msg_type!r}): {e}")
        notify_error(
            title=f"Sync Worker Error — {msg_type}",
            detail=str(e),
            context=f"on_date={on_date} page_size={page_size}",
        )
        message.nack()  # let Pub/Sub redeliver for transient BC/Firestore errors


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the sync Pub/Sub subscriber and return the future for lifecycle management."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_SYNC_SUBSCRIPTION,
    )
    # max_messages=2: sync jobs are heavy (BC + Firestore), limit concurrency
    flow_control = pubsub_v1.types.FlowControl(max_messages=2)
    future = subscriber.subscribe(
        subscription_path,
        callback=_process,
        flow_control=flow_control,
    )
    logger.info(f"Sync worker subscribed to {subscription_path}")
    return future

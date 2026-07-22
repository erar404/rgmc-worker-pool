"""Pub/Sub consumer for catalog and Firestore sync messages.

Subscribes to PUBSUB_SYNC_SUBSCRIPTION and dispatches based on the "type" field.

Message formats (JSON):

  Routine sync — all companies:
    { "type": "routine-sync", "on_date": "YYYY-MM-DD" }

  Single company item prices:
    { "type": "sync-item-prices", "company": "RGMC", "on_date": "YYYY-MM-DD", "page_size": 500 }

  Single company price list headers:
    { "type": "sync-price-list-headers", "company": "RGMC" }

Cloud Scheduler publishes { "type": "routine-sync" } to the rgmc-sync topic on a cron schedule.
The main API can publish any of the above to trigger targeted syncs.
"""
import datetime
import json
import logging

from google.cloud import pubsub_v1

from src import config
from src.services.bc_client import fetch_price_list_headers, fetch_v3_catalog
from src.services.gcs_catalog import save_catalog
from src.services.price_firestore_service import (
    sync_price_list_headers_to_firestore,
    sync_prices_to_firestore,
)
from src.services.send_mail import notify_error

logger = logging.getLogger("worker.sync")


def _sync_company(company: str, on_date: str, page_size: int = 500) -> None:
    """Sync price list headers then full item price catalog for a single company."""
    logger.info(f"[{company}] sync started — on_date={on_date!r}")

    try:
        headers = fetch_price_list_headers(company)
        written = sync_price_list_headers_to_firestore(headers, company)
        logger.info(f"[{company}] {written} price list headers written")
    except Exception as e:
        logger.error(f"[{company}] price list headers failed — {e}")

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

        elif msg_type == "sync-item-prices":
            company = data.get("company") or config.BC_COMPANY
            _sync_company(company, on_date, page_size)

        elif msg_type == "sync-price-list-headers":
            company = data.get("company") or config.BC_COMPANY
            headers = fetch_price_list_headers(company)
            written = sync_price_list_headers_to_firestore(headers, company)
            logger.info(f"[{company}] {written} price list headers written")

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

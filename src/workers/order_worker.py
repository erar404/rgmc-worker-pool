"""Pub/Sub consumer for order processing messages.

Subscribes to PUBSUB_ORDER_SUBSCRIPTION and processes each message as a BC order submission.

Message format (JSON):
  {
    "task_id": "<uuid>",
    "order_type": "sales" | "return",
    "api_version": "v1" | "v2",
    "company": "RGMC",
    "header": { ... BC order header fields ... },
    "lines": [ { ... BC order line fields ... }, ... ]
  }

On success: message.ack()
On permanent failure (bad data, BC rejects): message.ack() + update Firestore task as failed
On transient failure (network, 429, 503): message.nack() → Pub/Sub redelivers after ack deadline
"""
import json
import logging
import time

from google.cloud import pubsub_v1

from src import config
from src.services.bc_client import (
    v1_create_record,
    v1_delete_record,
    v2_create_record,
    v2_delete_record,
)
from src.services.send_mail import notify_error
from src.services.task_service import update_task

logger = logging.getLogger("worker.order")

_TRANSIENT_SIGNALS = ("429", "502", "503", "timeout", "ConnectionError", "ReadTimeout")


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"Order message decode failed: {e} — acking to drop poison pill")
        message.ack()
        return

    task_id: str = data.get("task_id", "unknown")
    order_type: str = data.get("order_type", "sales")
    api_version: str = data.get("api_version", "v1")
    header: dict = data.get("header", {})
    lines: list = data.get("lines", [])
    company: str = data.get("company") or config.BC_COMPANY

    _TABLE = "salesOrders" if order_type == "sales" else "salesReturnOrders"
    _LINES_TABLE = "salesOrderLines" if order_type == "sales" else "salesReturnOrderLines"
    _create = v2_create_record if api_version == "v2" else v1_create_record
    _delete = v2_delete_record if api_version == "v2" else v1_delete_record

    update_task(task_id, status="processing")

    try:
        http_status, resp_data = _create(_TABLE, header, company)
        if http_status not in (200, 201):
            raise ValueError(f"Order header failed: BC returned {http_status}: {resp_data}")

        order_id = resp_data.get("id")

        for i, line in enumerate(lines, start=1):
            for attempt in range(4):
                lh, ld = _create(f"{_TABLE}({order_id})/{_LINES_TABLE}", line, company)
                if lh in (200, 201):
                    break
                if lh == 409 and attempt < 3:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                try:
                    _delete(_TABLE, order_id, company)
                except Exception as del_err:
                    logger.error(f"Task {task_id}: rollback failed — {del_err}")
                raise ValueError(f"Line {i} failed (BC {lh}): {ld}. Order rolled back.")

        update_task(task_id, status="done", result=resp_data)
        logger.info(f"Task {task_id} done — order {resp_data.get('no') or order_id}")
        message.ack()

    except ValueError as e:
        # Permanent failure — don't retry, store the error
        update_task(task_id, status="failed", error=str(e))
        logger.error(f"Task {task_id} permanent failure: {e}")
        notify_error(
            title=f"Order Task Failed — {task_id}",
            detail=str(e),
            context=f"company={company} order_type={order_type}",
        )
        message.ack()

    except Exception as e:
        err_str = str(e)
        logger.error(f"Task {task_id} transient failure: {e}")
        if any(sig in err_str for sig in _TRANSIENT_SIGNALS):
            message.nack()  # Pub/Sub will redeliver
        else:
            update_task(task_id, status="failed", error=err_str)
            notify_error(
                title=f"Order Task Error — {task_id}",
                detail=err_str,
                context=f"company={company} order_type={order_type}",
            )
            message.ack()


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the order Pub/Sub subscriber and return the future for lifecycle management."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_ORDER_SUBSCRIPTION,
    )
    # max_messages=5 limits concurrent order processing to respect BC's semaphore
    flow_control = pubsub_v1.types.FlowControl(max_messages=5)
    future = subscriber.subscribe(
        subscription_path,
        callback=_process,
        flow_control=flow_control,
    )
    logger.info(f"Order worker subscribed to {subscription_path}")
    return future

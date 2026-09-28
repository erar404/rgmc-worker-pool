"""Firestore buffer for failed POUL SO import orders.

Collection name: so_buffer_{env}  (e.g. so_buffer_production, so_buffer_staging)
Document ID   : poRefNumber (slugified) — idempotent; re-saves overwrite stale entries.

Lifecycle:
  save_failed_order()    → called when _create_order() raises for a new or retried order.
                           Increments attempt_count each time. Once MAX_ATTEMPTS is
                           exceeded the document is deleted and the caller gets False,
                           signalling that the order should be treated as a final failure.
  get_buffered_orders()  → called at the start of each batch to pull pending retries.
  delete_buffered_order()→ called after a buffered order is successfully created in BC.

All functions swallow their own exceptions and log a warning so a Firestore outage
never breaks the main SO import flow.
"""
import logging
from datetime import datetime, timezone

from src import config

logger = logging.getLogger("so_buffer")

_COLLECTION = f"so_buffer_{config.GCP_ENV.lower()}"
MAX_ATTEMPTS = 5

_db = None


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore  # noqa: PLC0415 — lazy import avoids startup cost
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def _doc_id(header: dict) -> str:
    return str(header.get("poRefNumber", "unknown")).replace("/", "_").replace(".", "_")


def save_failed_order(
    header: dict,
    lines: list,
    company: str,
    src_company: str,
    error: str,
    so_number: str | None = None,
) -> bool:
    """Upsert a failed/partial order into the buffer.

    `lines` here means "still outstanding" — for a fresh failure that's every line;
    for an order whose header already exists in BC (so_number set) it's only the
    lines that still haven't been added, so a later retry can resume adding just
    those instead of recreating the header.

    so_number: the BC sales order Document No. once the header has been created.
    Carried forward from the existing doc when not explicitly passed, so a save
    triggered by an unrelated later failure (e.g. a transient BC error looking up
    the order to resume it) never silently drops the linkage to the header.

    Returns True if saved, False if MAX_ATTEMPTS exceeded (order permanently dropped).
    """
    try:
        doc_id = _doc_id(header)
        doc_ref = _client().collection(_COLLECTION).document(doc_id)
        existing_data = doc_ref.get().to_dict() or {}
        attempt_count = existing_data.get("attempt_count", 0) + 1
        resolved_so_number = so_number if so_number is not None else existing_data.get("so_number")

        if attempt_count > MAX_ATTEMPTS:
            logger.warning(
                f"so_buffer: {doc_id!r} exceeded {MAX_ATTEMPTS} attempts — "
                f"removing from buffer (company={company!r}, so_number={resolved_so_number!r})"
            )
            doc_ref.delete()
            return False

        payload = {
            "header": header,
            "lines": lines,
            "company": company,
            "src_company": src_company,
            "last_error": error,
            "failed_at": datetime.now(timezone.utc),
            "attempt_count": attempt_count,
        }
        if resolved_so_number:
            payload["so_number"] = resolved_so_number
        doc_ref.set(payload)
        logger.info(
            f"so_buffer: saved {doc_id!r} attempt {attempt_count}/{MAX_ATTEMPTS} "
            f"to {_COLLECTION!r} (so_number={resolved_so_number!r}, {len(lines)} line(s) outstanding)"
        )
        return True
    except Exception as exc:
        logger.warning(f"so_buffer: save_failed_order failed for {_doc_id(header)!r}: {exc}")
        return False


def get_buffered_orders(company: str) -> list[tuple[str, dict]]:
    """Return [(doc_id, data)] for all buffered orders for the given BC company."""
    try:
        docs = (
            _client()
            .collection(_COLLECTION)
            .where("company", "==", company)
            .stream()
        )
        result = [(doc.id, doc.to_dict()) for doc in docs]
        if result:
            logger.info(
                f"so_buffer: {len(result)} pending order(s) found in {_COLLECTION!r} "
                f"(company={company!r})"
            )
        return result
    except Exception as exc:
        logger.warning(f"so_buffer: get_buffered_orders failed — skipping buffer: {exc}")
        return []


def delete_buffered_order(doc_id: str) -> None:
    """Remove a successfully created order from the buffer."""
    try:
        _client().collection(_COLLECTION).document(doc_id).delete()
        logger.info(f"so_buffer: cleared {doc_id!r} from {_COLLECTION!r}")
    except Exception as exc:
        logger.warning(f"so_buffer: delete_buffered_order failed for {doc_id!r}: {exc}")

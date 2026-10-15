"""Firestore-backed queue of pending BC Sales Order Ship-to Code fixes, saved from the
/reconcile page's Missing Ship-To tab (rgmc-bc-api's so_buffer_service.queue_shipto_fix
owns writing the "queued" doc) and applied here in batch when a human triggers Apply
Ship-to Fixes — the same two-step shape as the SKU/branch/customer override +
Reprocess Buffer flow: saving a fix is instant and never touches BC, applying it is a
separate, explicit, batched step (see so_import_worker.py's _run_shipto_fixes_batch).

Collection name: so_shipto_fixes_{env} — same project/env-slug convention as
so_buffer_overrides.py. Unlike that module (read-only; rgmc-bc-api owns every write),
this worker also writes back the outcome of each attempt (applied/error), since
applying is this worker's own job.
"""
import logging
from datetime import datetime, timezone

from src import config

logger = logging.getLogger("shipto_fixes")

_COLLECTION = f"so_shipto_fixes_{config.GCP_ENV.lower()}"

_db = None


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore  # noqa: PLC0415 — lazy import avoids startup cost
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def fetch_pending(companies: list[str] | None = None) -> list[dict]:
    """Every not-yet-applied fix, as {"id": doc_id, **doc_data} — optionally narrowed to
    `companies`. Filtered in Python rather than a Firestore "in" query since this
    collection is expected to stay small (tens of docs, same as so_buffer_{env})."""
    try:
        docs = _client().collection(_COLLECTION).where("applied", "==", False).stream()
        out = [{"id": doc.id, **(doc.to_dict() or {})} for doc in docs]
        if companies:
            wanted = set(companies)
            out = [f for f in out if f.get("company") in wanted]
        return out
    except Exception as exc:
        logger.warning(f"shipto_fixes: fetch_pending failed: {exc}")
        return []


def mark_applied(fix_id: str) -> None:
    try:
        _client().collection(_COLLECTION).document(fix_id).set({
            "applied": True, "applied_at": datetime.now(timezone.utc), "error": None,
        }, merge=True)
    except Exception as exc:
        logger.warning(f"shipto_fixes: mark_applied({fix_id!r}) failed: {exc}")


def mark_failed(fix_id: str, error: str) -> None:
    """Left in the queue (applied stays False) so it's still visible/retryable on the
    next Apply run — a fix is never silently dropped just because one attempt failed."""
    try:
        _client().collection(_COLLECTION).document(fix_id).set({
            "applied": False, "last_attempt_at": datetime.now(timezone.utc), "error": error,
        }, merge=True)
    except Exception as exc:
        logger.warning(f"shipto_fixes: mark_failed({fix_id!r}) failed: {exc}")

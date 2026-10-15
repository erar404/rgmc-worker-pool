"""Firestore history log for Ship-to Code fix attempts (Apply Ship-To Fixes runs).

Collection name: so_shipto_fix_history_{env}  (e.g. so_shipto_fix_history_production)
Document ID   : auto-generated — append-only, one record per fix per Apply attempt,
                mirroring so_buffer_history.py's pattern exactly. so_shipto_fixes_{env}
                itself only ever holds the current pending/last-attempted state for a
                given order_id (and the doc is reused — never deleted — across retries),
                so this is the only place a ship-to link change is still visible after
                the fact: which PO/order got which ship-to, when, by whom, and whether
                it actually landed in BC. Lets a human search by PO ref or order number
                on /reconcile to find and correct a wrong link after the fact.

Written once per fix by so_import_worker.py's _run_shipto_fixes_batch, for every
outcome (applied, skipped, failed) — not just successes — so a search for one PO shows
its whole attempt history, not just the one time it happened to succeed.

All functions swallow their own exceptions and log a warning so a Firestore outage
never breaks the actual Apply run.
"""
import logging
from datetime import datetime, timezone

from src import config

logger = logging.getLogger("shipto_fix_history")

_COLLECTION = f"so_shipto_fix_history_{config.GCP_ENV.lower()}"

_db = None


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore  # noqa: PLC0415 — lazy import avoids startup cost
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def record_attempt(
    fix: dict,
    outcome: str,
    run_id: str | None = None,
    detail: str | None = None,
) -> None:
    """Append one history record for a queued ship-to fix this Apply run attempted.

    fix is the so_shipto_fixes_{env} doc (company, order_id, order_no, po_ref,
    ship_to_code, ship_to_name, resolved_by, source — see rgmc-bc-api's
    so_buffer_service.queue_shipto_fix for the exact shape).

    outcome is one of "applied" (BC PATCH succeeded), "skipped" (no longer eligible —
    see detail for why), or "failed" (BC rejected the PATCH, or the request errored).
    """
    try:
        payload = {
            "po_ref": fix.get("po_ref") or "unknown",
            "order_id": fix.get("order_id"),
            "order_no": fix.get("order_no") or "",
            "company": fix.get("company"),
            "ship_to_code": fix.get("ship_to_code"),
            "ship_to_name": fix.get("ship_to_name") or "",
            "source": fix.get("source") or "",
            "resolved_by": fix.get("resolved_by") or "",
            "outcome": outcome,
            "run_id": run_id,
            "attempted_at": datetime.now(timezone.utc),
        }
        if detail:
            payload["detail"] = detail
        _client().collection(_COLLECTION).add(payload)
        logger.info(
            f"shipto_fix_history: recorded {payload['po_ref']!r} ({payload['order_no']!r}) "
            f"outcome={outcome!r} (company={payload['company']!r}, run_id={run_id!r})"
        )
    except Exception as exc:
        logger.warning(f"shipto_fix_history: record_attempt failed for {fix.get('po_ref', 'unknown')!r}: {exc}")

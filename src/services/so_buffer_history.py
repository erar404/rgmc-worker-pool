"""Firestore history log for buffer-reconciliation (reprocess-buffer) attempts.

Collection name: so_buffer_history_{env}  (e.g. so_buffer_history_production)
Document ID   : auto-generated — append-only, one record per PO per reprocess-buffer
                run, mirroring rgmc-bc-api's so_buffer_reference_{env} override-history
                log. Never updated or deleted. so_buffer_{env} itself only ever holds
                current state (and is deleted once a PO is fully resolved), so this is
                the only place a PO's reconciliation is still visible after the fact —
                who triggered the reprocess, when, and what the PO's header/lines
                looked like at that attempt.

Written once per PO by so_import_worker.py's _run_batch, only when called for a
"poul-so-reprocess-buffer" message — never for normal inbound-PO batches, which have
no triggering user (notify) to record.

All functions swallow their own exceptions and log a warning so a Firestore outage
never breaks the actual reprocess run.
"""
import logging
from datetime import datetime, timezone

from src import config

logger = logging.getLogger("so_buffer_history")

_COLLECTION = f"so_buffer_history_{config.GCP_ENV.lower()}"

_db = None


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore  # noqa: PLC0415 — lazy import avoids startup cost
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def record_reconciliation(
    header: dict,
    lines: list,
    company: str,
    outcome: str,
    notify: dict | None,
    run_id: str | None = None,
    so_number: str | None = None,
    detail: str | None = None,
) -> None:
    """Append one history record for a PO processed by a reprocess-buffer run.

    outcome is one of "resolved" (fully created/completed, buffer doc cleared),
    "still_buffered" (header exists but some lines remain unresolved, re-buffered),
    or "failed" (errored, re-buffered for another attempt).

    notify is the employee who triggered this reprocess-buffer run from /reconcile —
    {"name", "company", "department", "email"} — recorded verbatim so the history
    answers "who reconciled this PO and when", not just "what changed".
    """
    try:
        payload = {
            "po_ref": header.get("poRefNumber", "unknown"),
            "header": header,
            "lines": lines,
            "company": company,
            "outcome": outcome,
            "so_number": so_number,
            "run_id": run_id,
            "triggered_by": notify or None,
            "triggered_at": datetime.now(timezone.utc),
        }
        if detail:
            payload["detail"] = detail
        _client().collection(_COLLECTION).add(payload)
        logger.info(
            f"so_buffer_history: recorded {payload['po_ref']!r} outcome={outcome!r} "
            f"(company={company!r}, run_id={run_id!r})"
        )
    except Exception as exc:
        logger.warning(
            f"so_buffer_history: record_reconciliation failed for "
            f"{header.get('poRefNumber', 'unknown')!r}: {exc}"
        )

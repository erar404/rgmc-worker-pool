"""Lightweight Firestore history of each BigQuery-bridge handoff (the BigQuery ->
MSSQL insert step in bigquery_bridge.py's __insert_dataframes). Only PO ref number
lists per target MSSQL table ("collection") are stored, never full header/detail
rows — BigQuery and MSSQL already hold the full data, so a PO ref is all that's
needed to look either back up. Exists so /trigger can show "what ran, when, how
many POs, did it succeed" without re-querying BigQuery or MSSQL on every page load.

Same lazy-singleton-client pattern as so_buffer_history.py — each service file in
this repo keeps its own Firestore client rather than sharing one.
"""
import logging
from datetime import datetime, timezone

import src.config as config

logger = logging.getLogger("bridge_handoff_history")

_db = None
_COLLECTION = f"bridge_handoff_history_{config.GCP_ENV.lower()}"


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def record_handoff(
    group_code: str,
    method: str,
    status: str,
    tables: dict[str, dict[str, list[str]]],
    duplicate_po_refs: dict[str, list[str]] | None = None,
    error: str | None = None,
) -> None:
    """Record one handoff attempt. tables maps a target MSSQL table name (e.g.
    "CustomerPOULBQ", "CustomerPOULDetailBQ") to a {filename: [po_ref_numbers]}
    grouping of the PO refs actually inserted for it this run — grouped by the
    source document (BigQuery's fileName column) so the history reads as "these POs
    came from this upload" instead of one flat, unordered list. A table whose rows
    have no fileName available (e.g. detail tables that don't carry it, or a
    group_code whose source columns were never renamed) groups everything under the
    single key "(unknown file)" rather than failing.

    status is "success" or "error" — a duplicate-only run (nothing new to insert) is
    still "success", just with empty po_ref lists; duplicate_po_refs/error are extra
    context, not replacements for a full record, kept minimal on purpose.

    Never raises — a Firestore write failure here must not break the real bridge
    run it's only recording.
    """
    now = datetime.now(timezone.utc)
    payload: dict = {
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "timestamp": now,
        "group_code": group_code,
        "method": method,
        "status": status,
        "tables": {
            name: {
                "by_file": by_file,
                "count": sum(len(refs) for refs in by_file.values()),
            }
            for name, by_file in tables.items()
        },
    }
    if duplicate_po_refs:
        payload["duplicate_po_refs"] = {
            name: refs for name, refs in duplicate_po_refs.items() if refs
        }
    if error:
        payload["error"] = error
    try:
        _client().collection(_COLLECTION).add(payload)
    except Exception as e:
        logger.warning(f"record_handoff: failed to write history ({group_code}/{method}): {e}")

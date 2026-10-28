"""Firestore history of each BigQuery-bridge handoff (the BigQuery -> MSSQL insert
step in bigquery_bridge.py's __insert_dataframes), one document per PO ref, so both
halves of a PO's journey -- did it land in MSSQL, did the BC Sales Order that follows
actually get created -- show up side by side. Only PO ref numbers are stored, never
full header/detail rows (BigQuery and MSSQL already hold those).

rgmc-bc-api's record_manual_handoff writes into this same collection for the Upload
Dashboard's manual "Retrigger Handoff (MSSQL)" button, so a manual per-PO retrigger
shows up in the same history as an automated bridge run.

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
    mssql_status: str,
    po_refs_by_file: dict[str, list[str]],
    error: str | None = None,
    trigger_results: dict[str, bool] | None = None,
) -> None:
    """Record one document per PO ref handed off by the BigQuery bridge this run.
    mssql_status is "inserted" (freshly written to CustomerPOULBQ/CustomerRABQ this
    run) or "error" (the insert attempt raised for every po_ref still pending at that
    point). A po_ref already in MSSQL from an earlier run (a duplicate-key skip) is
    never re-recorded here — re-writing one every time BigQuery re-serves an
    already-ingested row (see handoff.md's known upstream-watermark limitation) would
    spam the history forever with nothing new to report.

    trigger_results ({po_ref: bool}, from BigqueryBridge.__verify_trigger_promotion)
    is the staging table's own AFTER INSERT trigger's ACTUAL result — did this po_ref
    really land in CustomerPOUL/CustomerRA, not just CustomerPOULBQ/CustomerRABQ — set
    into trigger_status: "promoted" / "not_promoted" (the insert into the staging
    table succeeded but the trigger never created the row — a real problem worth
    surfacing, distinct from mssql_status) / "unknown" (no result available — an
    "error" mssql_status, where the trigger never had a chance to fire, or the
    verification query itself failed).

    bc_status starts "pending" for an "inserted" po_ref — so_import_worker.py's
    update_bc_status patches it once the SO-import attempt that follows resolves one
    way or another — or "not_attempted" for an "error" po_ref, since a failed MSSQL
    insert means __trigger_so_import never ran for it this round.

    Never raises — a Firestore write failure here must not break the real bridge run
    it's only recording.
    """
    now = datetime.now(timezone.utc)
    bc_status = "pending" if mssql_status == "inserted" else "not_attempted"
    try:
        db = _client()
        collection = db.collection(_COLLECTION)
        batch = db.batch()
        for file_name, po_refs in po_refs_by_file.items():
            for po_ref in po_refs:
                promoted = (trigger_results or {}).get(po_ref)
                trigger_status = "promoted" if promoted is True else "not_promoted" if promoted is False else "unknown"
                batch.set(collection.document(), {
                    "po_ref_number": po_ref,
                    "file_name": file_name,
                    "group_code": group_code,
                    "method": method,
                    "mssql_status": mssql_status,
                    "mssql_error": error,
                    "trigger_status": trigger_status,
                    "bc_status": bc_status,
                    "bc_order_no": None,
                    "bc_error": None,
                    "bc_updated_at": None,
                    "date": now.strftime("%Y-%m-%d"),
                    "time": now.strftime("%H:%M:%S"),
                    "timestamp": now,
                })
        batch.commit()
    except Exception as e:
        logger.warning(f"record_handoff: failed to write history ({group_code}/{method}): {e}")


def update_bc_status(
    po_ref_number: str,
    bc_status: str,
    so_number: str | None = None,
    error: str | None = None,
) -> None:
    """Patches the bc_status (+ bc_order_no/bc_error) of whichever handoff record(s)
    for this po_ref are still "pending", once the SO-import attempt that followed its
    MSSQL insert has resolved. bc_status is "created" (header + all lines landed),
    "buffered" (header created, lines still incomplete), or "error" (the attempt
    raised, or BC locked the order against further changes).

    Matches on po_ref_number + bc_status=="pending" rather than a stored doc id,
    since callers (_run_batch, for both fresh inbound orders and buffered retries)
    only ever have the po_ref — normally at most one doc is ever "pending" for a
    given po_ref (see record_handoff's duplicate-skip above). A po_ref with no
    matching pending record (e.g. one that was never bridge-handed-off, just added
    straight to the buffer) is a silent no-op, not an error.

    Never raises — a Firestore write failure here must not break the real SO-import
    run it's only recording.
    """
    try:
        from google.cloud.firestore_v1 import FieldFilter
        db = _client()
        docs = list(
            db.collection(_COLLECTION)
            .where(filter=FieldFilter("po_ref_number", "==", po_ref_number))
            .where(filter=FieldFilter("bc_status", "==", "pending"))
            .stream()
        )
        if not docs:
            return
        batch = db.batch()
        for doc in docs:
            batch.update(doc.reference, {
                "bc_status": bc_status,
                "bc_order_no": so_number,
                "bc_error": error,
                "bc_updated_at": datetime.now(timezone.utc),
            })
        batch.commit()
    except Exception as e:
        logger.warning(f"update_bc_status: failed to update history for po_ref={po_ref_number!r}: {e}")

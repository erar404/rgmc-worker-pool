"""Validates session-history records (written by rgmc-bc-api's /session-history
endpoint) against Business Central, patching an in_bc flag onto each Firestore doc.

Collection: session_history_{env} — same collection and document-id scheme
({companyCode}_{sessionId}) as rgmc-bc-api's session_history_service.py. This service
only ever patches inBc/inBcStatus/inBcCheckedAt (and, when recovered, a stale/missing
salesOrderSeries/returnOrderSeries) via set(merge=True) — rgmc-bc-api remains the sole
writer of every other field on these documents.

Moved here from rgmc-bc-api because the full collection (~20k docs and growing) takes
too long to validate inside a single synchronous HTTP request — this runs instead as a
Pub/Sub-triggered background job (see sync_worker.py, message type
"verify-session-history"), with its own BC request budget (bc_client._bc_semaphore)
fully decoupled from bc-api's live-traffic semaphore, which was the actual bottleneck
in the original implementation.

verify_record()'s algorithm is kept in sync by hand with rgmc-bc-api's
session_history_service.verify_record — the two repos don't share a Python package
(see bc_client.py's own docstring: "a focused subset of the main API's
bc_functions.py"), so this is a deliberate, documented duplication, not an oversight.
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from google.cloud import firestore

from src.config import GCP_ENV, GCP_PROJECT_ID
from src.services.bc_client import v2_list_records_by_filter
from src.services.price_firestore_service import _BatchWriter

logger = logging.getLogger("session_history_verify_service")

_db: firestore.Client | None = None
_VERIFY_WORKERS = 16


def _firestore() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=GCP_PROJECT_ID)
    return _db


def _collection_name() -> str:
    env = (GCP_ENV or "staging").lower().replace(" ", "_")
    return f"session_history_{env}"


def _odata_escape(value: str) -> str:
    return value.replace("'", "''")


def _lookup_by_number(table: str, company_code: str, number: Optional[str]) -> Optional[dict]:
    """Exact lookup of a BC document by its own document number. None if not found."""
    if not number:
        return None
    records = v2_list_records_by_filter(table, f"number eq '{_odata_escape(number)}'", company_code)
    return records[0] if records else None


def _lookup_candidates_by_customer_date(
    table: str, company_code: str, posting_date: Optional[str], customer_number: Optional[str]
) -> list:
    """Exact customer+date match — the same fallback the webapp History page's manual
    'Fetch BC Order Number' button uses, but never auto-picks among multiple candidates
    (see the Thread C history-mismatch bug, caused by exactly that)."""
    if not posting_date or not customer_number:
        return []
    odata_filter = f"postingDate eq {posting_date} and sellToCustomerNo eq '{_odata_escape(customer_number)}'"
    return v2_list_records_by_filter(table, odata_filter, company_code)


def _verify_side(
    table: str, company_code: str, series: Optional[str], posting_date: Optional[str], customer_number: Optional[str]
) -> tuple[str, Optional[str]]:
    """Returns (status, recovered_number) for one side (sales or returns) of a session.

    status is one of 'confirmed' | 'not_found' | 'needs_review'.
    """
    if series and _lookup_by_number(table, company_code, series):
        return "confirmed", None

    candidates = _lookup_candidates_by_customer_date(table, company_code, posting_date, customer_number)
    if len(candidates) == 1:
        return "confirmed", candidates[0].get("number")
    if len(candidates) > 1:
        return "needs_review", None
    return "not_found", None


def verify_record(record: dict) -> dict:
    """Determine whether a session history record's order(s) really exist in BC.

    Returns a patch dict (inBc, inBcStatus, inBcCheckedAt, and any recovered series
    numbers) meant to be merged back onto the Firestore doc — never a full overwrite.
    """
    if record.get("noSales"):
        return {"inBc": True, "inBcStatus": "not_applicable", "inBcCheckedAt": time.time()}

    company_code = (record.get("companyCode") or "").strip().upper()
    if not company_code or company_code == "UNKNOWN":
        return {"inBc": None, "inBcStatus": "unknown_company", "inBcCheckedAt": time.time()}

    posting_date = record.get("postingDate")
    customer_number = record.get("customerNumber")
    sales_lines = record.get("salesOrders") or []
    return_lines = record.get("returnOrders") or []

    statuses: list[str] = []
    patch: dict = {}

    if sales_lines:
        side_status, recovered = _verify_side(
            "salesOrders", company_code, record.get("salesOrderSeries"), posting_date, customer_number,
        )
        statuses.append(side_status)
        if recovered:
            patch["salesOrderSeries"] = recovered

    if return_lines:
        side_status, recovered = _verify_side(
            "salesReturnOrders", company_code, record.get("returnOrderSeries"), posting_date, customer_number,
        )
        statuses.append(side_status)
        if recovered:
            patch["returnOrderSeries"] = recovered

    if not statuses:
        overall = "not_applicable"
    elif "not_found" in statuses:
        overall = "not_found"
    elif "needs_review" in statuses:
        overall = "needs_review"
    else:
        overall = "confirmed"

    patch["inBcStatus"] = overall
    patch["inBc"] = {"confirmed": True, "not_found": False, "not_applicable": True}.get(overall)
    patch["inBcCheckedAt"] = time.time()
    return patch


def backfill_validate(force: bool = False, limit: Optional[int] = None) -> dict:
    """Validate stored session history records against BC and patch their in_bc flag
    in place. Scans the whole collection (all companies) since this is a maintenance
    pass, not a user-scoped read.

    By default, only records that have never been checked (no inBcStatus) or were left
    `pending` are processed — already-`confirmed`/`not_found`/`needs_review`/
    `not_applicable`/`unknown_company` records are skipped, so repeated runs converge
    incrementally. Pass force=True to re-verify everything regardless of current status.

    limit caps how many records this run processes, for working through a large
    backlog in bounded chunks instead of one very long run.
    """
    db = _firestore()
    collection = _collection_name()
    all_docs = list(db.collection(collection).stream())

    if force:
        pending_docs = all_docs
    else:
        pending_docs = [d for d in all_docs if d.to_dict().get("inBcStatus") in (None, "pending")]
    skipped = len(all_docs) - len(pending_docs)

    if limit is not None:
        pending_docs = pending_docs[:limit]

    def _evaluate(doc) -> tuple[str, dict]:
        record = doc.to_dict()
        try:
            patch = verify_record(record)
        except Exception as e:
            logger.warning(f"verify_record failed for {doc.id}: {e}")
            patch = {"inBcStatus": "pending"}
        return doc.id, patch

    counts: dict[str, int] = {}

    with _BatchWriter(db) as writer:
        with ThreadPoolExecutor(max_workers=_VERIFY_WORKERS) as executor:
            futures = [executor.submit(_evaluate, doc) for doc in pending_docs]
            for future in as_completed(futures):
                doc_id, patch = future.result()
                label = patch.get("inBcStatus", "pending")
                counts[label] = counts.get(label, 0) + 1
                writer.set(db.collection(collection).document(doc_id), patch, merge=True)

    logger.info(
        f"session_history backfill_validate: {counts} "
        f"(total={len(all_docs)}, skipped_already_verified={skipped}, processed={len(pending_docs)})"
    )
    return {
        "total": len(all_docs),
        "skipped_already_verified": skipped,
        "processed": len(pending_docs),
        "counts": counts,
    }

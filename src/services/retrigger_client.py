"""HTTP client for rgmc-gcp-api's BigQuery-backed retrigger endpoints, used by
so_import_worker.py's "poul-so-retrigger-file" handler.

Moved off sbic-manual-trigger-page's/rgmc-gcp-api's own request path (confirmed live,
2026-10-29: resolving + re-fetching every PO under a large uploaded file can take
several minutes against int_document_ai/int_document_ai_detail's current size — well
past any reasonable HTTP request deadline). This worker has no such deadline, so these
calls simply use a generous timeout and let the retrigger run however long it genuinely
takes; the single-PO calls (cleanup-staging, insert-from-bigquery, the promotion check)
are already fast and keep their original timeouts.
"""
import logging

import requests

from src import config

logger = logging.getLogger("retrigger_client")

_session = requests.Session()
_BQ_LOOKUP_TIMEOUT = 600  # no HTTP request deadline to race against in this worker


def resolve_po_refs_by_file(file_name: str) -> list[str]:
    """Every po_ref_number BigQuery's int_document_ai has under this exact file_name."""
    resp = _session.get(f"{config.GCP_API_BASE}/bigquery_routes/by_table/value", params={
        "table_name": "int_document_ai", "where_column": "file_name", "where_value": file_name,
    }, timeout=_BQ_LOOKUP_TIMEOUT)
    resp.raise_for_status()
    rows = resp.json().get("data") or []
    return sorted({r.get("po_ref_number") for r in rows if r.get("po_ref_number")})


def fetch_headers_and_details(po_refs: list[str]) -> tuple[list[dict], list[dict]]:
    """Current int_document_ai/int_document_ai_detail rows for every po_ref — headers
    re-fetched fresh by po_ref_number rather than reused from the file-scoped lookup
    above, since BigQuery can re-stamp a PO's file_name under a dbt rematerialization
    while this call is still running (the same quirk sbic-manual-trigger-page's own
    batch route already works around)."""
    resp = _session.get(f"{config.GCP_API_BASE}/bigquery_routes/document-ai/search", params={
        "po_ref_number": ",".join(po_refs), "limit": 1000,
    }, timeout=_BQ_LOOKUP_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    return body.get("data") or [], body.get("detail_data") or []


def cleanup_staging(po_ref: str) -> dict:
    """Deletes po_ref's rows from CustomerPOULBQ/CustomerPOULDetailBQ (staging only) so
    the follow-up insert gets a genuinely fresh INSERT for the trigger to react to."""
    resp = _session.post(f"{config.GCP_API_BASE}/customerpoul/cleanup-staging", params={
        "po_ref_number": po_ref,
    }, timeout=60)
    resp.raise_for_status()
    return resp.json()


def insert_from_bigquery(header: dict, details: list, po_ref: str) -> tuple[int, dict]:
    """Re-inserts one PO's header/detail rows — groupcode=retrigger-{po_ref} gives this
    call its own rate-limit bucket (see rgmc-gcp-api's own rate_limit dependency),
    same as sbic-manual-trigger-page's equivalent call. Returns (status_code, body);
    body is always a dict even on a non-2xx/non-JSON response."""
    resp = _session.post(
        f"{config.GCP_API_BASE}/customerpoul/insert-from-bigquery",
        params={"groupcode": f"retrigger-{po_ref}"},
        json={"headers": [header], "details": details},
        timeout=90,
    )
    try:
        body = resp.json()
    except Exception:
        body = {"error": resp.text}
    return resp.status_code, body


def check_promoted(po_ref: str) -> str:
    """"promoted" / "not_promoted" / "unknown" — CustomerPOULBQ's own AFTER INSERT
    trigger's ACTUAL result for this po_ref (did it really land in CustomerPOUL)."""
    try:
        resp = _session.get(f"{config.GCP_API_BASE}/sbic/by_table/value", params={
            "table_name": "CustomerPOUL", "where_column": "poRefNumber", "where_value": po_ref,
        }, timeout=30)
        if resp.status_code == 200:
            return "promoted" if (resp.json().get("data") or []) else "not_promoted"
    except Exception as e:
        logger.warning(f"retrigger_client: promotion check failed for po_ref={po_ref!r}: {e}")
    return "unknown"

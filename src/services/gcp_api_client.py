"""Read-only HTTP client for rgmc-gcp-api's Cloud SQL endpoints.

Worker pool has no direct MSSQL connection of its own — gcp-api owns Cloud SQL
access and exposes it over HTTP. Used by two so_import_worker.py handlers:
  - poul-so-sync-from-cloudsql — re-derives a PO's header/lines from CustomerPOUL /
    CustomerPOULDetail for an order that's already been inserted into BC but may be
    missing lines (see bigquery_bridge.py's header/detail arrival-timing race).
  - poul-so-backfill-from-cloudsql — creates a fresh BC order for any CustomerPOUL
    row (optionally date-ranged) that was never inserted into BC at all.
"""
import logging

import requests

from src import config

logger = logging.getLogger("gcp_api_client")

_session = requests.Session()


def fetch_customerpoul_by_create_by(
    create_by: str,
    limit: int = 1000,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """GET /customerpoul?create_by=... — every header row inserted via that path.

    date_from/date_to (YYYY-MM-DD, both optional) scope to CustomerPOUL.poDate — used
    by the Cloud SQL backfill handler so a run can be bounded to a specific window
    instead of pulling every createBy='trigger' row ever inserted.
    """
    params: dict = {"create_by": create_by, "limit": limit}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    try:
        resp = _session.get(f"{config.GCP_API_BASE}/customerpoul", params=params, timeout=60)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"gcp_api_client: fetch_customerpoul_by_create_by({create_by!r}) failed: {e}")
        return []


def fetch_customerpouldetailbq(po_ref: str) -> list[dict]:
    """GET /customerpouldetail/bq/{po_ref} — CustomerPOULDetailBQ rows, field-for-field
    identical to what the BigQuery bridge itself used to build a fresh order's `lines`
    (customerSKUCode, customerSKUDesc, poQty, poQtyPcs, unitOfMeasurement, unitPrice,
    ...) — unlike plain CustomerPOULDetail, which is missing several of those columns.
    """
    try:
        resp = _session.get(f"{config.GCP_API_BASE}/customerpouldetail/bq/{po_ref}", timeout=30)
        if resp.status_code != 200:
            return []
        return resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"gcp_api_client: fetch_customerpouldetailbq({po_ref!r}) failed: {e}")
        return []

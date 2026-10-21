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
    company_id: int | None = None,
) -> list[dict]:
    """GET /customerpoul?create_by=... — every header row inserted via that path.

    date_from/date_to (YYYY-MM-DD, both optional) scope to CustomerPOUL.createDate (when
    the row was inserted into CustomerPOUL, not poDate, the original PO date from the
    source ERP) — used by the Cloud SQL backfill handler so a run can be bounded to a
    specific window instead of pulling every createBy='trigger' row ever inserted.

    company_id (optional) scopes to CustomerPOUL.companyId on sbic_prod (SBIC=6,
    MTC=12) — a clean numeric key, unlike companyName free text.
    """
    params: dict = {"create_by": create_by, "limit": limit}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    if company_id is not None:
        params["company_id"] = company_id
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


# Batch size for the chunked lookups below — comfortably under /customerpoul and
# /sbic/customerbranches' own limit=1000 cap, chunked so one slow/failed chunk doesn't
# lose the whole batch's results.
_BATCH_CHUNK_SIZE = 500


def fetch_customerpoul_by_refs(po_refs: list[str]) -> dict[str, dict]:
    """poRefNumber -> latest CustomerPOUL header row, for many refs in a handful of
    GET /customerpoul calls instead of one per PO — used to recover customerBranchId
    for orders sourced from the BigQuery/Pub/Sub pipeline, whose header never carries
    that FK (only customerBranchName/customerBranchLookUpCode — see mappings_bridge.py,
    which never maps customer_branch_id at all). The list endpoint orders by createDate
    DESC, so for a ref with more than one row (a re-PO), the first one seen per ref is
    the latest — same "first wins" convention used elsewhere in this ecosystem.

    limit is requested at the route's own max (1000) regardless of chunk size, not
    len(chunk) — a ref with multiple historical rows (re-POs) would otherwise let a
    `TOP {limit}` cutoff silently drop some refs from a full chunk entirely.
    """
    unique_refs = sorted({r for r in po_refs if r})
    if not unique_refs:
        return {}
    chunks = [unique_refs[i:i + _BATCH_CHUNK_SIZE] for i in range(0, len(unique_refs), _BATCH_CHUNK_SIZE)]
    by_ref: dict[str, dict] = {}
    for chunk in chunks:
        try:
            resp = _session.get(
                f"{config.GCP_API_BASE}/customerpoul",
                params={"po_ref_number": ",".join(chunk), "limit": 1000},
                timeout=60,
            )
            resp.raise_for_status()
            for row in resp.json().get("data", []):
                ref = row.get("poRefNumber")
                if ref and ref not in by_ref:
                    by_ref[ref] = row
        except Exception as e:
            logger.warning(f"gcp_api_client: fetch_customerpoul_by_refs chunk failed: {e}")
    return by_ref


def fetch_customer_branches(branch_ids: list[int]) -> dict[int, dict]:
    """customerBranchId -> CustomerBranch row (customerBranchCode, lookUpCode, ...), for
    many IDs in a handful of GET /sbic/customerbranches calls instead of one per order.
    """
    unique_ids = sorted({b for b in branch_ids if b is not None})
    if not unique_ids:
        return {}
    chunks = [unique_ids[i:i + _BATCH_CHUNK_SIZE] for i in range(0, len(unique_ids), _BATCH_CHUNK_SIZE)]
    by_id: dict[int, dict] = {}
    for chunk in chunks:
        try:
            resp = _session.get(
                f"{config.GCP_API_BASE}/sbic/customerbranches",
                params={"customer_branch_id": ",".join(str(b) for b in chunk), "limit": len(chunk)},
                timeout=60,
            )
            resp.raise_for_status()
            for row in resp.json().get("data", []):
                branch_id = row.get("customerBranchId")
                if branch_id is not None:
                    by_id[branch_id] = row
        except Exception as e:
            logger.warning(f"gcp_api_client: fetch_customer_branches chunk failed: {e}")
    return by_id

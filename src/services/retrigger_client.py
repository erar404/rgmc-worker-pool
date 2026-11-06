"""Client for rgmc-gcp-api's retrigger endpoints, used by so_import_worker.py's
"poul-so-retrigger-file" handler — plus direct BigQuery reads for the two lookups that
used to go through rgmc-gcp-api's HTTP layer.

The single-PO calls (cleanup-staging, insert-from-bigquery, the promotion check) stay
as HTTP calls to rgmc-gcp-api, since they're genuinely fast MSSQL writes (already well
under Cloud Run's own request ceiling) and MSSQL access is owned by that service.

resolve_po_refs_by_file/fetch_headers_and_details read BigQuery DIRECTLY instead
(confirmed live 2026-11-06: hopping through rgmc-gcp-api's GET /bigquery_routes/*
endpoints stacked TWO timeout ceilings — this worker's own 600s client timeout, which
never got the chance to matter, underneath Cloud Run's ~300s request-timeout, which
fired first and turned a slow-but-eventually-successful BigQuery read into a hard 504.
That uncaught 504 then nacked the Pub/Sub message, causing an infinite redelivery loop
hammering the same already-overloaded view every ~2 minutes for 33+ minutes before the
subscription's dead-letter policy finally discarded it.) This worker already has full
BigQuery credentials/config and no HTTP request deadline of its own — bigquery_bridge.py
in this same repo already reads these exact views the same way. Removing the HTTP hop
leaves BigQuery's own latency and the Pub/Sub ack deadline (600s, confirmed safe for
this use) as the only remaining limits.
"""
import logging

import pandas as pd
import requests

from src import config
from src.services.bq_read import read_gbq_with_timeout

logger = logging.getLogger("retrigger_client")

_session = requests.Session()
_BQ_READ_TIMEOUT = 300  # matches bigquery_bridge.py's own stopgap for this same slow view


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _json_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Converts every datetime64 column to an ISO string (NaT -> None) in place.

    BigQuery TIMESTAMP/DATETIME/DATE columns come back from read_gbq_with_timeout as
    pandas.Timestamp, not plain Python datetimes -- .to_dict(orient="records") keeps
    them as Timestamp objects, which requests' json= parameter (plain stdlib
    json.dumps, no datetime support) cannot serialize. Confirmed live 2026-11-06:
    every retrigger-file insert failed with "Object of type Timestamp is not JSON
    serializable" once this module started reading BigQuery directly here instead of
    going through rgmc-gcp-api's FastAPI layer, which serializes datetimes for free.
    """
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda v: v.isoformat() if pd.notnull(v) else None)
    return df


def resolve_po_refs_by_file(file_name: str) -> list[str]:
    """Every po_ref_number BigQuery's int_document_ai has under this exact file_name —
    read directly, see module docstring for why this no longer goes through
    rgmc-gcp-api's HTTP layer."""
    query = (
        f"SELECT DISTINCT po_ref_number FROM `{config.BIGQUERY_DATASET_ID}.int_document_ai` "
        f"WHERE file_name = '{_escape_sql_literal(file_name)}'"
    )
    df = read_gbq_with_timeout(
        query, project_id=config.BIGQUERY_PROJECT_ID, dialect="standard", timeout=_BQ_READ_TIMEOUT,
    )
    return sorted({r for r in df["po_ref_number"].dropna().tolist() if r})


def fetch_headers_and_details(po_refs: list[str]) -> tuple[list[dict], list[dict]]:
    """Current int_document_ai/int_document_ai_detail rows for every po_ref — headers
    re-fetched fresh by po_ref_number rather than reused from the file-scoped lookup
    above, since BigQuery can re-stamp a PO's file_name under a dbt rematerialization
    while this call is still running (the same quirk sbic-manual-trigger-page's own
    batch route already works around). Read directly, see module docstring for why."""
    if not po_refs:
        return [], []
    refs_sql = ", ".join(f"'{_escape_sql_literal(r)}'" for r in po_refs)
    header_query = (
        f"SELECT * FROM `{config.BIGQUERY_DATASET_ID}.int_document_ai` "
        f"WHERE po_ref_number IN ({refs_sql}) ORDER BY created_at DESC"
    )
    detail_query = (
        f"SELECT * FROM `{config.BIGQUERY_DATASET_ID}.int_document_ai_detail` "
        f"WHERE po_ref_number IN ({refs_sql})"
    )
    headers_df = read_gbq_with_timeout(
        header_query, project_id=config.BIGQUERY_PROJECT_ID, dialect="standard", timeout=_BQ_READ_TIMEOUT,
    )
    details_df = read_gbq_with_timeout(
        detail_query, project_id=config.BIGQUERY_PROJECT_ID, dialect="standard", timeout=_BQ_READ_TIMEOUT,
    )
    headers_df, details_df = _json_safe(headers_df), _json_safe(details_df)
    return headers_df.to_dict(orient="records"), details_df.to_dict(orient="records")


def merge_duplicate_headers(headers: list[dict], details: list[dict]) -> dict[str, dict]:
    """Collapses duplicate int_document_ai header rows sharing the same po_ref_number
    down to one per ref, picking the best candidate instead of a plain
    `{h["po_ref_number"]: h for h in headers}` dict comprehension, which silently keeps
    whichever duplicate happens to iterate last.

    headers/details are exactly what fetch_headers_and_details returns above (snake_case
    BigQuery column names; headers ordered created_at DESC per bigquery_routes.py's own
    query) — a plain dict comprehension over a DESC-ordered list keeps the LAST-iterated,
    i.e. the OLDEST duplicate, not the best one. Confirmed live 2026-11-05/06: the same
    PO can land in int_document_ai more than once under a different company_name (e.g. a
    "SUNCOAST BRANDS" ghost-tag row with zero matching detail lines, alongside the real
    row with real lines) — picking the older row here would silently re-insert the PO
    under the wrong header on every retrigger.

    Tie-break, most-preferred first: company_name resolves to a BC-postable company
    (contains SUNCOAST/SBIC — same keyword check so_import_worker.py's own
    __trigger_so_import mask uses), has >=1 matching row in details, has a non-blank
    company_name at all (even if not BC-routable — keeps this tier instead of dropping
    it when BC-routability was added), latest created_at.

    BC-routability deliberately outranks "has items": confirmed live 2026-11-06 that a
    po_ref's duplicates can split exactly this way — a BC-routable "SUNCOAST BRANDS"
    header with zero of its own matching detail rows, alongside a "MANILA TASTE
    CURATORS VENTURES" header (MTC has no BC company configured, so it can never reach
    BC at all) that DOES have the real detail rows. Picking the BC-routable header here
    is safe even though it looks item-less: `_run_retrigger_file` matches each po_ref's
    detail rows from the full `details` list independently of which header this
    function returns, so the real lines still get re-inserted regardless — this only
    decides which company_name attribution the resulting MSSQL row gets.
    """
    detail_refs = {d.get("po_ref_number") for d in details if d.get("po_ref_number")}

    def _score(h: dict):
        company_upper = (h.get("company_name") or "").upper()
        is_bc_routable = ("SUNCOAST" in company_upper) or ("SBIC" in company_upper)
        has_items = h.get("po_ref_number") in detail_refs
        has_company = bool(company_upper.strip())
        return (is_bc_routable, has_items, has_company, h.get("created_at") or "")

    best_by_ref: dict[str, dict] = {}
    for h in headers:
        ref = h.get("po_ref_number")
        if not ref:
            continue
        current = best_by_ref.get(ref)
        if current is None or _score(h) > _score(current):
            best_by_ref[ref] = h
    return best_by_ref


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

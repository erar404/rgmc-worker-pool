"""Read-only access to the manual reconciliation overrides saved from the /reconcile
page (rgmc-bc-api's so_buffer_service.py owns writing them).

Collection name: so_buffer_overrides_{env} — same project and env-slug convention as
so_buffer.py's so_buffer_{env}, read directly via Firestore rather than an HTTP call to
rgmc-bc-api, since worker-pool already owns a Firestore client for the buffer itself.

Needed because apply_resolution_to_buffer (rgmc-bc-api) only ever patches the
resolved/resolvedShipTo/resolvedCustomer fields onto a buffer doc that already exists
at the moment a human saves the link on /reconcile — a PO whose doc wasn't there yet
(a fresh inbound order sharing an already-resolved key, or a doc later recreated from
source) never carries that patch and would keep failing the exact same way forever,
even though the link is saved. Re-deriving each link fresh here, by raw SKU/branch/
customer text instead of relying on that one-time patch, closes that gap for every
caller: _sync_order_from_cloudsql (lines, orders whose header's already in BC) and
_create_order (lines + branch/customer, new headers).
"""
import logging

from src import config

logger = logging.getLogger("so_buffer_overrides")

_COLLECTION = f"so_buffer_overrides_{config.GCP_ENV.lower()}"

_db = None


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore  # noqa: PLC0415 — lazy import avoids startup cost
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def fetch_sku_item_overrides() -> dict[str, dict]:
    """Return {SKU_OR_DESC_KEY_UPPER: {"itemNo": ..., "description": ...}} for every
    saved "sku" override — overrides aren't scoped per BC company, so one fetch covers
    every company in a sync pass. Swallows its own exceptions (a Firestore hiccup here
    should fall back to ref_map-only matching, not block the sync pass).
    """
    try:
        docs = _client().collection(_COLLECTION).where("type", "==", "sku").stream()
        out: dict[str, dict] = {}
        for doc in docs:
            data = doc.to_dict() or {}
            key = (data.get("key") or "").strip().upper()
            resolved = data.get("resolved") or {}
            if key and resolved.get("itemNo"):
                out[key] = resolved
        return out
    except Exception as exc:
        logger.warning(f"so_buffer_overrides: fetch_sku_item_overrides failed — continuing without overrides: {exc}")
        return {}


def fetch_branch_overrides() -> dict[str, dict]:
    """Return {BRANCH_NAME_UPPER: {"customerNo": ..., "shipToCode": ..., "name": ...}}
    for every saved "branch" override.

    Same rationale as fetch_sku_item_overrides, for the same underlying gap:
    apply_resolution_to_buffer only patches header.resolvedShipTo onto a buffer doc
    that already existed at the moment a human resolved it on /reconcile. A PO whose
    buffer doc didn't exist yet then — a fresh inbound order sharing an
    already-resolved branch, or a buffer doc later recreated from source — never
    carries that patch and would keep failing ship-to resolution forever even though
    the link is saved. Re-deriving the same link fresh, by raw branch name, covers
    that gap the same way fetch_sku_item_overrides already does for SKUs.
    """
    try:
        docs = _client().collection(_COLLECTION).where("type", "==", "branch").stream()
        out: dict[str, dict] = {}
        for doc in docs:
            data = doc.to_dict() or {}
            key = (data.get("key") or "").strip().upper()
            resolved = data.get("resolved") or {}
            if key and resolved.get("customerNo"):
                out[key] = resolved
        return out
    except Exception as exc:
        logger.warning(f"so_buffer_overrides: fetch_branch_overrides failed — continuing without overrides: {exc}")
        return {}


def fetch_customer_overrides() -> dict[str, dict]:
    """Return {CUSTOMER_NAME_UPPER: {"customerNo": ..., "displayName": ...}} for every
    saved "customer" override. Same rationale as fetch_branch_overrides.
    """
    try:
        docs = _client().collection(_COLLECTION).where("type", "==", "customer").stream()
        out: dict[str, dict] = {}
        for doc in docs:
            data = doc.to_dict() or {}
            key = (data.get("key") or "").strip().upper()
            resolved = data.get("resolved") or {}
            if key and resolved.get("customerNo"):
                out[key] = resolved
        return out
    except Exception as exc:
        logger.warning(f"so_buffer_overrides: fetch_customer_overrides failed — continuing without overrides: {exc}")
        return {}


_INACTIVE_SKUS_COLLECTION = f"so_buffer_inactive_skus_{config.GCP_ENV.lower()}"


def fetch_inactive_skus() -> set[str]:
    """Return the set of raw SKU codes (or descriptions, for a blank-SKU group) a human
    has marked inactive on /reconcile's Inactive Items tab (rgmc-bc-api's
    mark_sku_inactive) — e.g. a discontinued item that will never get a real BC link.

    Before this existed, marking a SKU inactive only ever changed what /reconcile
    displayed (it drops the group from the Items (SKU) tab and its resolved/total
    counts) — the worker itself had no idea and kept retrying the exact same
    unresolvable line forever, so an order could show "All links resolved" (every
    *visible* SKU group was 0/0, i.e. none left to resolve) while silently stuck in
    the buffer on a line marked inactive weeks earlier. _resolve_valid_lines now reads
    this set and drops those lines outright (never retried, never re-buffered) so
    marking a SKU inactive actually does what its name implies.
    """
    try:
        docs = _client().collection(_INACTIVE_SKUS_COLLECTION).stream()
        return {(doc.to_dict() or {}).get("key", "").strip().upper() for doc in docs} - {""}
    except Exception as exc:
        logger.warning(f"so_buffer_overrides: fetch_inactive_skus failed — continuing without exclusions: {exc}")
        return set()

"""Read-only access to the manual reconciliation overrides saved from the /reconcile
page (rgmc-bc-api's so_buffer_service.py owns writing them).

Collection name: so_buffer_overrides_{env} — same project and env-slug convention as
so_buffer.py's so_buffer_{env}, read directly via Firestore rather than an HTTP call to
rgmc-bc-api, since worker-pool already owns a Firestore client for the buffer itself.

Needed specifically for _sync_order_from_cloudsql (so_import_worker.py): that path
re-derives a PO's lines fresh from Cloud SQL for orders whose Firestore buffer doc is
already gone, so it never sees the line.resolvedItem field apply_resolution_to_buffer
only patches onto a *buffer* doc. Without this, a SKU link saved on /reconcile after an
order's header was already created in BC would never actually get its lines inserted.
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

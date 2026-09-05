"""Cloud Storage persistence for the v3 item price catalog.

Blob layout: {GCP_ENV}/{COMPANY}/catalog.json
  e.g.  Production/RGMC/catalog.json
        Staging/RGMC/catalog.json

All public functions are non-fatal — any GCS error is logged and swallowed so
the BC fetch path continues normally when GCS is unavailable.
"""
import json
import logging
import time

from src.config import GCS_CATALOG_BUCKET, GCP_ENV

logger = logging.getLogger("gcs_catalog")

_client = None


def _gcs():
    global _client
    if _client is None:
        from google.cloud import storage
        _client = storage.Client()
    return _client


def _blob_path(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}/catalog.json"


def save_catalog(company_name: str, on_date: str, records: list) -> None:
    """Persist the catalog to GCS after a full BC fetch."""
    if not GCS_CATALOG_BUCKET:
        logger.warning("GCS_CATALOG_BUCKET not configured — skipping catalog save")
        return
    try:
        payload = json.dumps({"records": records, "on_date": on_date, "saved_at": time.time()})
        _gcs().bucket(GCS_CATALOG_BUCKET).blob(_blob_path(company_name)).upload_from_string(
            payload, content_type="application/json"
        )
        logger.info(f"GCS catalog saved: {len(records)} records (company={company_name!r}, date={on_date!r})")
    except Exception as e:
        logger.warning(f"GCS catalog save failed (company={company_name!r}): {e}")


# ---------------------------------------------------------------------------
# Price List Items (used by BC API's override cache)
# ---------------------------------------------------------------------------

def _pl_items_blob_path(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}/price_list_items.json"


def save_pl_items(company_name: str, items: list) -> None:
    """Persist all price list items for a company to GCS.

    The BC API reads this blob to serve price overrides from memory instead of
    issuing thousands of per-batch Firestore queries on every item-price request.
    Only called on full syncs — incremental syncs leave the existing blob intact.
    """
    if not GCS_CATALOG_BUCKET:
        return
    try:
        payload = json.dumps({"items": items, "saved_at": time.time()})
        _gcs().bucket(GCS_CATALOG_BUCKET).blob(_pl_items_blob_path(company_name)).upload_from_string(
            payload, content_type="application/json"
        )
        logger.info(f"GCS price list items saved: {len(items)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS price list items save failed (company={company_name!r}): {e}")


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

def _customers_blob_path(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}/customers.json"


def save_customers(company_name: str, customers: list) -> None:
    """Persist all customers for a company to GCS."""
    if not GCS_CATALOG_BUCKET:
        return
    try:
        payload = json.dumps({"customers": customers, "saved_at": time.time()})
        _gcs().bucket(GCS_CATALOG_BUCKET).blob(_customers_blob_path(company_name)).upload_from_string(
            payload, content_type="application/json"
        )
        logger.info(f"GCS customers saved: {len(customers)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS customers save failed (company={company_name!r}): {e}")


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

def _contacts_blob_path(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}/contacts.json"


def save_contacts(company_name: str, contacts: list) -> None:
    """Persist all contacts for a company to GCS."""
    if not GCS_CATALOG_BUCKET:
        return
    try:
        payload = json.dumps({"contacts": contacts, "saved_at": time.time()})
        _gcs().bucket(GCS_CATALOG_BUCKET).blob(_contacts_blob_path(company_name)).upload_from_string(
            payload, content_type="application/json"
        )
        logger.info(f"GCS contacts saved: {len(contacts)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS contacts save failed (company={company_name!r}): {e}")


# ---------------------------------------------------------------------------
# Item categories
# ---------------------------------------------------------------------------

def _item_categories_blob_path(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}/item_categories.json"


def save_item_categories(company_name: str, categories: list) -> None:
    """Persist all item categories for a company to GCS."""
    if not GCS_CATALOG_BUCKET:
        return
    try:
        payload = json.dumps({"item_categories": categories, "saved_at": time.time()})
        _gcs().bucket(GCS_CATALOG_BUCKET).blob(_item_categories_blob_path(company_name)).upload_from_string(
            payload, content_type="application/json"
        )
        logger.info(f"GCS item categories saved: {len(categories)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS item categories save failed (company={company_name!r}): {e}")

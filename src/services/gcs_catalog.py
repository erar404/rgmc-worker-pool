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

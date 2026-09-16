"""Cloud Storage persistence for everything the BC API serves without touching BC.

Blob layout under {GCP_ENV}/{COMPANY}/:
  catalog.json                 — full item price catalog (all families), prices overlaid
  families/_index.json         — {"families": {code: count}, "on_date", "overlay_on_date", "saved_at"}
  families/{FAMILY}.json       — slim, non-blocked records for one family, prices overlaid
  prices.json                  — {productNo: [unitPriceIncVAT, priceListCode]} for change detection
  price_overrides.json         — compact per-price-list line index for historical-date overlays
  price_list_headers.json      — all price list headers
  customers.json / contacts.json / item_categories.json

Every blob is uploaded gzip-encoded (Content-Encoding: gzip); the storage client
transparently decompresses on download, so readers are unchanged.

All public functions are non-fatal — GCS errors are logged and swallowed.
"""
import gzip
import io
import json
import logging
import time
from typing import Iterable

from src.config import GCS_CATALOG_BUCKET, GCP_ENV

logger = logging.getLogger("gcs_catalog")

_client = None

NO_FAMILY = "_NOFAMILY"

# Fields the consignment app reads from a catalog record; everything else is dropped
# from the family blobs (the full catalog keeps the raw BC record).
FAMILY_RECORD_FIELDS = (
    "id", "productNo", "description", "unitPriceIncVAT", "unitPrice", "familyCode",
    "itemId", "itemCategoryCode", "baseUnitOfMeasure", "priceListCode",
    "lastModifiedDateTime", "priceChangedAt",
    # Raw BC values kept so incremental rebuilds re-apply the overlay from scratch
    # instead of compounding on top of a previous overlay.
    "bcUnitPriceIncVAT", "bcPriceListCode",
)


def _gcs():
    global _client
    if _client is None:
        from google.cloud import storage
        _client = storage.Client()
    return _client


def _prefix(company_name: str) -> str:
    return f"{(GCP_ENV or 'Staging').strip()}/{company_name.upper()}"


def _upload_json(path: str, payload: dict) -> None:
    body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"), compresslevel=6)
    blob = _gcs().bucket(GCS_CATALOG_BUCKET).blob(path)
    blob.content_encoding = "gzip"
    blob.cache_control = "no-cache"
    blob.upload_from_string(body, content_type="application/json")


def _download_json(path: str) -> dict | None:
    blob = _gcs().bucket(GCS_CATALOG_BUCKET).blob(path)
    if not blob.exists(timeout=10):
        return None
    return json.loads(blob.download_as_bytes(timeout=120))


def family_blob_name(family_code: str) -> str:
    return family_code or NO_FAMILY


def slim_record(rec: dict) -> dict:
    return {k: rec[k] for k in FAMILY_RECORD_FIELDS if rec.get(k) not in (None, "")}


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def load_family_index(company_name: str) -> dict | None:
    if not GCS_CATALOG_BUCKET:
        return None
    try:
        return _download_json(f"{_prefix(company_name)}/families/_index.json")
    except Exception as e:
        logger.warning(f"GCS family index load failed (company={company_name!r}): {e}")
        return None


def load_family_records(company_name: str, family: str) -> list:
    if not GCS_CATALOG_BUCKET:
        return []
    try:
        data = _download_json(f"{_prefix(company_name)}/families/{family}.json") or {}
        return data.get("data") or data.get("records") or []
    except Exception as e:
        logger.warning(f"GCS family blob load failed (company={company_name!r}, family={family!r}): {e}")
        return []


def load_prices_index(company_name: str) -> dict:
    """productNo -> [unitPriceIncVAT, priceListCode] as of the previous sync (empty on first run)."""
    if not GCS_CATALOG_BUCKET:
        return {}
    try:
        data = _download_json(f"{_prefix(company_name)}/prices.json")
        return (data or {}).get("prices", {})
    except Exception as e:
        logger.warning(f"GCS prices index load failed (company={company_name!r}): {e}")
        return {}


def save_family_blob(company_name: str, family: str, on_date: str, overlay_on_date: str, records: list) -> None:
    """Write one family's records in the exact shape the BC API returns for
    GET /bc/custom/v3/item-prices?family_code=X&on_date=<overlay_on_date>.

    Matching the response shape lets the API hand the stored gzip bytes straight to the
    client for that (very common) request without parsing or re-serialising anything.
    """
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(
            f"{_prefix(company_name)}/families/{family}.json",
            {
                "data": records,
                "total": len(records),
                "onDate": on_date,
                "activePriceLists": [],
                "priceOverridesApplied": 0,
                "source": "gcs_family",
                "skip": 0,
                "limit": 0,
                "overlay_on_date": overlay_on_date,
                "saved_at": time.time(),
            },
        )
    except Exception as e:
        logger.warning(f"GCS family blob save failed (company={company_name!r}, family={family!r}): {e}")


def save_search_index(company_name: str, entries: list) -> None:
    """[[productNo, description_lower, familyBlobName], ...] for every non-blocked item.

    Lets the API resolve barcode/substring searches and product_nos batches to the
    right family blobs without ever loading the full catalog.
    """
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(f"{_prefix(company_name)}/search_index.json", {"items": entries, "saved_at": time.time()})
        logger.info(f"GCS search index saved: {len(entries)} items (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS search index save failed (company={company_name!r}): {e}")


def save_family_index(company_name: str, on_date: str, overlay_on_date: str, families: dict[str, int]) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(
            f"{_prefix(company_name)}/families/_index.json",
            {"families": families, "on_date": on_date, "overlay_on_date": overlay_on_date, "saved_at": time.time()},
        )
        logger.info(f"GCS family index saved: {len(families)} families (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS family index save failed (company={company_name!r}): {e}")


def save_prices_index(company_name: str, prices: dict) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(f"{_prefix(company_name)}/prices.json", {"prices": prices, "saved_at": time.time()})
    except Exception as e:
        logger.warning(f"GCS prices index save failed (company={company_name!r}): {e}")


def save_catalog_streaming(
    company_name: str,
    on_date: str,
    overlay_on_date: str,
    family_iter: Iterable[tuple[str, list]],
) -> int:
    """Write catalog.json by streaming family record lists one at a time.

    The full RGMC catalog is ~100k records; materialising the JSON string for all of
    them at once needs several hundred MB. Encoding record-by-record straight into a
    gzip buffer keeps peak memory at the compressed size (a few MB).
    """
    if not GCS_CATALOG_BUCKET:
        logger.warning("GCS_CATALOG_BUCKET not configured — skipping catalog save")
        return 0
    total = 0
    try:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
            gz.write(b'{"records":[')
            first = True
            for _family, records in family_iter:
                for rec in records:
                    if not first:
                        gz.write(b",")
                    first = False
                    gz.write(json.dumps(rec, separators=(",", ":")).encode("utf-8"))
                    total += 1
            tail = {"on_date": on_date, "overlay_on_date": overlay_on_date, "saved_at": time.time()}
            gz.write(b"]," + json.dumps(tail, separators=(",", ":"))[1:].encode("utf-8"))
        blob = _gcs().bucket(GCS_CATALOG_BUCKET).blob(f"{_prefix(company_name)}/catalog.json")
        blob.content_encoding = "gzip"
        blob.cache_control = "no-cache"
        blob.upload_from_string(buf.getvalue(), content_type="application/json")
        logger.info(
            f"GCS catalog saved: {total} records, {buf.tell() / 1e6:.1f} MB gzipped "
            f"(company={company_name!r}, date={on_date!r})"
        )
    except Exception as e:
        logger.warning(f"GCS catalog save failed (company={company_name!r}): {e}")
    return total


# ---------------------------------------------------------------------------
# Price lists
# ---------------------------------------------------------------------------

def save_pl_headers(company_name: str, headers: list) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(f"{_prefix(company_name)}/price_list_headers.json", {"headers": headers, "saved_at": time.time()})
        logger.info(f"GCS price list headers saved: {len(headers)} (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS price list headers save failed (company={company_name!r}): {e}")


def save_price_overrides(company_name: str, on_date: str, codes: dict[str, dict]) -> None:
    """Compact index {code: {assetNo: [incl, excl, startingDate]}} for active Sale lists.

    The BC API reads this only when a client asks for a posting date different from the
    one the catalog was overlaid for.
    """
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(
            f"{_prefix(company_name)}/price_overrides.json",
            {"codes": codes, "on_date": on_date, "saved_at": time.time()},
        )
        n_lines = sum(len(v) for v in codes.values())
        logger.info(f"GCS price overrides saved: {len(codes)} lists, {n_lines} lines (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS price overrides save failed (company={company_name!r}): {e}")


# ---------------------------------------------------------------------------
# Supporting datasets
# ---------------------------------------------------------------------------

def save_customers(company_name: str, customers: list) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(f"{_prefix(company_name)}/customers.json", {"customers": customers, "saved_at": time.time()})
        logger.info(f"GCS customers saved: {len(customers)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS customers save failed (company={company_name!r}): {e}")


def save_contacts(company_name: str, contacts: list) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(f"{_prefix(company_name)}/contacts.json", {"contacts": contacts, "saved_at": time.time()})
        logger.info(f"GCS contacts saved: {len(contacts)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS contacts save failed (company={company_name!r}): {e}")


def save_item_categories(company_name: str, categories: list) -> None:
    if not GCS_CATALOG_BUCKET:
        return
    try:
        _upload_json(
            f"{_prefix(company_name)}/item_categories.json",
            {"item_categories": categories, "saved_at": time.time()},
        )
        logger.info(f"GCS item categories saved: {len(categories)} records (company={company_name!r})")
    except Exception as e:
        logger.warning(f"GCS item categories save failed (company={company_name!r}): {e}")

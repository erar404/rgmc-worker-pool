"""Firestore persistence for the v3 item price catalog and price list headers.

Collection naming:
  item_prices_{env}           e.g. item_prices_production
  price_list_headers_{env}    e.g. price_list_headers_production

Document IDs:
  item_prices          → {company}_{productNo}
  price_list_headers   → {company}_{code}
"""
import logging
import time

from google.cloud import firestore

from src.config import GCP_ENV, GCP_PROJECT_ID

logger = logging.getLogger("price_firestore_service")

_db: firestore.Client | None = None
_BATCH_SIZE = 500


def _firestore() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=GCP_PROJECT_ID)
    return _db


def _env_slug() -> str:
    return (GCP_ENV or "staging").lower().replace(" ", "_")


def _prices_collection() -> str:
    return f"item_prices_{_env_slug()}"


def _headers_collection() -> str:
    return f"price_list_headers_{_env_slug()}"


def get_prices_from_firestore(
    company: str,
    family_code: str | None = None,
    product_no: str | None = None,
    product_nos: list | None = None,
    price_list_code: str | None = None,
    include_blocked: bool = False,
) -> list:
    """Return item prices from Firestore for the given company and current GCP_ENV.

    All filters are applied in Python after a single company-scoped query — avoids
    composite index requirements. Returns [] when the collection is empty or filters
    match nothing.
    """
    collection = _prices_collection()
    db = _firestore()
    docs = db.collection(collection).where("company", "==", company).stream()
    nos_set = set(product_nos) if product_nos else None
    results = []
    for doc in docs:
        data = doc.to_dict()
        if not include_blocked and data.get("blocked"):
            continue
        if family_code and data.get("familyCode") != family_code:
            continue
        if product_no and data.get("productNo") != product_no:
            continue
        if nos_set is not None and data.get("productNo") not in nos_set:
            continue
        if price_list_code and data.get("priceListCode") != price_list_code:
            continue
        results.append(data)
    return results


def get_price_list_headers_from_firestore(
    company: str,
    status: str | None = None,
    item_family_code: str | None = None,
    price_type: str | None = None,
) -> list:
    """Return price list headers from Firestore for the given company and current GCP_ENV.

    Filters are applied in Python after a single company-scoped query.
    """
    collection = _headers_collection()
    db = _firestore()
    docs = db.collection(collection).where("company", "==", company).stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        if status and data.get("status") != status:
            continue
        if item_family_code and data.get("itemFamilyCode") != item_family_code:
            continue
        if price_type and data.get("priceType") != price_type:
            continue
        results.append(data)
    return results


def sync_prices_to_firestore(records: list, company: str, on_date: str) -> int:
    """Upsert item price records into Firestore. Returns the count of records written."""
    collection = _prices_collection()
    db = _firestore()
    synced_at = time.time()
    written = 0
    batch = db.batch()
    count_in_batch = 0

    for record in records:
        product_no = record.get("productNo") or ""
        if not product_no:
            continue
        ref = db.collection(collection).document(f"{company}_{product_no}")
        batch.set(ref, {
            **record,
            "company": company,
            "onDate": on_date,
            "syncedAt": synced_at,
            "env": GCP_ENV,
        })
        count_in_batch += 1
        written += 1
        if count_in_batch >= _BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batch.commit()

    logger.info(f"Synced {written} item prices → {collection!r} (company={company!r}, onDate={on_date!r})")
    return written


def sync_price_list_headers_to_firestore(records: list, company: str) -> int:
    """Upsert price list header records into Firestore. Returns the count written."""
    collection = _headers_collection()
    db = _firestore()
    synced_at = time.time()
    written = 0
    batch = db.batch()
    count_in_batch = 0

    for record in records:
        code = record.get("code") or ""
        if not code:
            continue
        ref = db.collection(collection).document(f"{company}_{code}")
        batch.set(ref, {
            **record,
            "company": company,
            "syncedAt": synced_at,
            "env": GCP_ENV,
        })
        count_in_batch += 1
        written += 1
        if count_in_batch >= _BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batch.commit()

    logger.info(f"Synced {written} price list headers → {collection!r} (company={company!r})")
    return written

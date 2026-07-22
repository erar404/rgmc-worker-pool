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

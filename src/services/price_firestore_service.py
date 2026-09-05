"""Firestore persistence for the v3 item price catalog, price list headers, and price list items.

Collection naming:
  item_prices_{env}           e.g. item_prices_production
  price_list_headers_{env}    e.g. price_list_headers_production
  price_list_items_{env}      e.g. price_list_items_production

Document IDs:
  item_prices          → {company}_{productNo}
  price_list_headers   → {company}_{code}
  price_list_items     → {company}_{priceListCode}_{lineNo}  (fallback: _{id})
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def _items_collection() -> str:
    return f"price_list_items_{_env_slug()}"


def _state_collection() -> str:
    return f"sync_state_{_env_slug()}"


def get_sync_state(company: str, collection_type: str) -> str | None:
    """Return the UTC ISO timestamp of the last successful sync for (company, collection_type), or None."""
    db = _firestore()
    doc = db.collection(_state_collection()).document(f"{company}_{collection_type}").get()
    if not doc.exists:
        return None
    return doc.to_dict().get("lastSyncAt")


def set_sync_state(company: str, collection_type: str, timestamp: str) -> None:
    """Record the last successful sync timestamp for (company, collection_type)."""
    db = _firestore()
    db.collection(_state_collection()).document(f"{company}_{collection_type}").set({
        "company": company,
        "collectionType": collection_type,
        "lastSyncAt": timestamp,
        "env": GCP_ENV,
    })


def prices_exist_in_firestore(company: str) -> bool:
    """Return True if at least one item price record exists for this company."""
    db = _firestore()
    docs = db.collection(_prices_collection()).where("company", "==", company).limit(1).stream()
    return any(True for _ in docs)


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


_COMMIT_WORKERS = 8


def sync_prices_to_firestore(records: list, company: str, on_date: str) -> int:
    """Upsert item price records into Firestore. Returns the count of records written."""
    collection = _prices_collection()
    db = _firestore()
    synced_at = time.time()
    written = 0
    batches: list = []
    batch = db.batch()
    count_in_batch = 0

    for record in records:
        product_no = record.get("productNo") or ""
        if not product_no:
            continue
        ref = db.collection(collection).document(f"{company}_{product_no}")
        doc_data = {
            **record,
            "company": company,
            "onDate": on_date,
            "syncedAt": synced_at,
            "env": GCP_ENV,
        }
        # familyCode is a computed temp-buffer field that BC may omit from incremental
        # (lastModifiedDateTime-filtered) responses. Don't force-write "" — use merge=True
        # so any value previously set by backfill_family_codes is preserved.
        if not doc_data.get("familyCode"):
            doc_data.pop("familyCode", None)
        batch.set(ref, doc_data, merge=True)
        count_in_batch += 1
        written += 1
        if count_in_batch >= _BATCH_SIZE:
            batches.append(batch)
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batches.append(batch)

    if batches:
        with ThreadPoolExecutor(max_workers=min(len(batches), _COMMIT_WORKERS)) as ex:
            for f in as_completed([ex.submit(b.commit) for b in batches]):
                f.result()

    logger.info(f"Synced {written} item prices → {collection!r} (company={company!r}, onDate={on_date!r})")
    return written


def backfill_family_codes(records: list, company: str) -> dict:
    """Patch the familyCode field on existing Firestore item price documents.

    Uses set(merge=True) so only familyCode is touched on existing docs.
    Company name is uppercased to match the convention used by sync_prices_to_firestore
    (which receives company from BC_COMPANY env var, always uppercase).
    Skips records where BC returned no familyCode so we never overwrite a valid
    existing value with an empty string.
    Returns {"patched": int, "skipped_missing_product_no": int, "skipped_no_family_code": int}.
    """
    collection = _prices_collection()
    db = _firestore()
    doc_company = company.upper()
    patched = 0
    skipped_no_pno = 0
    skipped_no_fc = 0
    batches: list = []
    batch = db.batch()
    count_in_batch = 0

    for record in records:
        product_no = record.get("productNo") or ""
        if not product_no:
            skipped_no_pno += 1
            continue
        family_code = record.get("familyCode") or ""
        if not family_code:
            skipped_no_fc += 1
            continue
        ref = db.collection(collection).document(f"{doc_company}_{product_no}")
        batch.set(ref, {"familyCode": family_code}, merge=True)
        count_in_batch += 1
        patched += 1
        if count_in_batch >= _BATCH_SIZE:
            batches.append(batch)
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batches.append(batch)

    if batches:
        with ThreadPoolExecutor(max_workers=min(len(batches), _COMMIT_WORKERS)) as ex:
            for f in as_completed([ex.submit(b.commit) for b in batches]):
                f.result()

    logger.info(
        f"Backfilled familyCode on {patched} documents in {collection!r} "
        f"(company={company!r}, skipped_no_pno={skipped_no_pno}, skipped_no_fc={skipped_no_fc})"
    )
    return {"patched": patched, "skipped_missing_product_no": skipped_no_pno, "skipped_no_family_code": skipped_no_fc}


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


def sync_price_list_items_to_firestore(lines: list, company: str, price_list_code: str) -> int:
    """Upsert price list line items into Firestore. Returns count written.

    Document ID: {company}_{priceListCode}_{lineNo}, falling back to the line's
    id (SystemId) if lineNo is absent.
    """
    collection = _items_collection()
    db = _firestore()
    synced_at = time.time()
    written = 0
    batches: list = []
    batch = db.batch()
    count_in_batch = 0

    for line in lines:
        line_no = line.get("lineNo")
        line_id = line.get("id") or line.get("systemId") or ""
        key = str(line_no) if line_no is not None else line_id
        if not key:
            continue
        ref = db.collection(collection).document(f"{company}_{price_list_code}_{key}")
        batch.set(ref, {
            **line,
            "company": company,
            "priceListCode": price_list_code,
            "syncedAt": synced_at,
            "env": GCP_ENV,
        })
        count_in_batch += 1
        written += 1
        if count_in_batch >= _BATCH_SIZE:
            batches.append(batch)
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batches.append(batch)

    if batches:
        with ThreadPoolExecutor(max_workers=min(len(batches), _COMMIT_WORKERS)) as ex:
            for f in as_completed([ex.submit(b.commit) for b in batches]):
                f.result()

    logger.info(
        f"Synced {written} price list items → {collection!r} "
        f"(company={company!r}, priceListCode={price_list_code!r})"
    )
    return written


def _ile_collection() -> str:
    return f"item_ledger_entries_{_env_slug()}"


def ile_exists_in_firestore(company: str) -> bool:
    """Return True if at least one item ledger entry exists for this company."""
    db = _firestore()
    docs = db.collection(_ile_collection()).where("company", "==", company).limit(1).stream()
    return any(True for _ in docs)


def sync_item_ledger_entries_to_firestore(records: list, company: str) -> int:
    """Upsert item ledger entry records into Firestore. Returns count written.

    Document ID: {company}_{entryNo} — entryNo is the integer primary key of
    Item Ledger Entry (table 32), unique within a company.
    """
    collection = _ile_collection()
    db = _firestore()
    synced_at = time.time()
    written = 0
    batches: list = []
    batch = db.batch()
    count_in_batch = 0

    for record in records:
        entry_no = record.get("entryNo")
        if entry_no is None:
            continue
        ref = db.collection(collection).document(f"{company}_{entry_no}")
        batch.set(ref, {
            **record,
            "company": company,
            "syncedAt": synced_at,
            "env": GCP_ENV,
        })
        count_in_batch += 1
        written += 1
        if count_in_batch >= _BATCH_SIZE:
            batches.append(batch)
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batches.append(batch)

    if batches:
        with ThreadPoolExecutor(max_workers=min(len(batches), _COMMIT_WORKERS)) as ex:
            for f in as_completed([ex.submit(b.commit) for b in batches]):
                f.result()

    logger.info(
        f"Synced {written} item ledger entries → {collection!r} (company={company!r})"
    )
    return written


def backfill_ile_columns_in_firestore(records: list, company: str, fields: set[str]) -> int:
    """Patch specific fields on existing ILE Firestore documents.

    Uses set-with-merge so only the named fields are touched; documents that
    don't exist yet are created with just those fields.
    Returns count of documents patched.
    """
    collection = _ile_collection()
    db = _firestore()
    batch = db.batch()
    count_in_batch = 0
    patched = 0

    for record in records:
        entry_no = record.get("entryNo")
        if entry_no is None:
            continue
        patch = {f: record[f] for f in fields if f in record}
        if not patch:
            continue
        ref = db.collection(collection).document(f"{company}_{entry_no}")
        batch.set(ref, patch, merge=True)
        count_in_batch += 1
        patched += 1
        if count_in_batch >= _BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count_in_batch = 0

    if count_in_batch > 0:
        batch.commit()

    logger.info(f"Backfilled {patched} ILE documents in {collection!r} (company={company!r})")
    return patched


def get_price_list_items_from_firestore(
    company: str,
    price_list_code: str | None = None,
) -> list:
    """Return price list items from Firestore for the given company and current GCP_ENV."""
    collection = _items_collection()
    db = _firestore()
    docs = db.collection(collection).where("company", "==", company).stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        if price_list_code and data.get("priceListCode") != price_list_code:
            continue
        results.append(data)
    return results

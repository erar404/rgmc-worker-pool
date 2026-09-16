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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

from google.cloud import firestore

from src.config import GCP_ENV, GCP_PROJECT_ID
from src.services.price_overlay import BestPriceAccumulator

logger = logging.getLogger("price_firestore_service")

_db: firestore.Client | None = None
_BATCH_SIZE = 500
_COMMIT_WORKERS = 8


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


def _ile_collection() -> str:
    return f"item_ledger_entries_{_env_slug()}"


class _BatchWriter:
    """Commit 500-document batches in the background with a bounded number in flight.

    Peak memory is (max_in_flight + 1) batches regardless of how many documents are
    written — the previous implementation built every batch up front, which for a full
    catalog meant a second copy of the whole dataset in protobuf form.
    """

    def __init__(self, db: firestore.Client, max_in_flight: int = _COMMIT_WORKERS):
        self._db = db
        self._batch = db.batch()
        self._pending = 0
        self.written = 0
        self._executor = ThreadPoolExecutor(max_workers=max_in_flight)
        self._slots = threading.Semaphore(max_in_flight)
        self._futures: list = []

    def set(self, ref, data: dict, merge: bool = False) -> None:
        self._batch.set(ref, data, merge=merge)
        self._pending += 1
        self.written += 1
        if self._pending >= _BATCH_SIZE:
            self._flush()

    def _commit(self, batch) -> None:
        try:
            batch.commit()
        finally:
            self._slots.release()

    def _flush(self) -> None:
        if self._pending == 0:
            return
        batch, self._batch, self._pending = self._batch, self._db.batch(), 0
        self._slots.acquire()
        self._futures.append(self._executor.submit(self._commit, batch))
        still_running = []
        for f in self._futures:
            if f.done():
                f.result()
            else:
                still_running.append(f)
        self._futures = still_running

    def close(self) -> None:
        try:
            self._flush()
            for f in self._futures:
                f.result()
        finally:
            self._executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.close()
        else:
            self._executor.shutdown(wait=False)


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
    """Return item prices from Firestore; filters applied in Python after a company-scoped query."""
    db = _firestore()
    docs = db.collection(_prices_collection()).where("company", "==", company).stream()
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
    db = _firestore()
    docs = db.collection(_headers_collection()).where("company", "==", company).stream()
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


def sync_prices_to_firestore(records: Iterable[dict], company: str, on_date: str) -> int:
    """Upsert item price records into Firestore. Returns the count written."""
    collection = _prices_collection()
    db = _firestore()
    synced_at = time.time()
    with _BatchWriter(db) as writer:
        for record in records:
            product_no = record.get("productNo") or ""
            if not product_no:
                continue
            doc_data = {
                **record,
                "company": company,
                "onDate": on_date,
                "syncedAt": synced_at,
                "env": GCP_ENV,
            }
            # familyCode is a computed temp-buffer field that BC may omit from incremental
            # responses. Don't force-write "" — merge so a backfilled value persists.
            if not doc_data.get("familyCode"):
                doc_data.pop("familyCode", None)
            writer.set(db.collection(collection).document(f"{company}_{product_no}"), doc_data, merge=True)
    logger.info(f"Synced {writer.written} item prices → {collection!r} (company={company!r}, onDate={on_date!r})")
    return writer.written


def backfill_family_codes(records: Iterable[dict], company: str) -> dict:
    """Patch familyCode on existing item price documents (merge). Skips records without one."""
    collection = _prices_collection()
    db = _firestore()
    doc_company = company.upper()
    skipped_no_pno = 0
    skipped_no_fc = 0
    with _BatchWriter(db) as writer:
        for record in records:
            product_no = record.get("productNo") or ""
            if not product_no:
                skipped_no_pno += 1
                continue
            family_code = record.get("familyCode") or ""
            if not family_code:
                skipped_no_fc += 1
                continue
            writer.set(
                db.collection(collection).document(f"{doc_company}_{product_no}"),
                {"familyCode": family_code},
                merge=True,
            )
    logger.info(
        f"Backfilled familyCode on {writer.written} documents in {collection!r} "
        f"(company={company!r}, skipped_no_pno={skipped_no_pno}, skipped_no_fc={skipped_no_fc})"
    )
    return {
        "patched": writer.written,
        "skipped_missing_product_no": skipped_no_pno,
        "skipped_no_family_code": skipped_no_fc,
    }


def sync_price_list_headers_to_firestore(records: Iterable[dict], company: str) -> int:
    collection = _headers_collection()
    db = _firestore()
    synced_at = time.time()
    with _BatchWriter(db) as writer:
        for record in records:
            code = record.get("code") or ""
            if not code:
                continue
            doc = {k: v for k, v in record.items() if k != "priceListLines"}
            writer.set(
                db.collection(collection).document(f"{company}_{code}"),
                {**doc, "company": company, "syncedAt": synced_at, "env": GCP_ENV},
            )
    logger.info(f"Synced {writer.written} price list headers → {collection!r} (company={company!r})")
    return writer.written


def sync_price_list_items_to_firestore(lines: Iterable[dict], company: str, price_list_code: str) -> int:
    """Upsert price list lines. Document ID: {company}_{priceListCode}_{lineNo|id}."""
    collection = _items_collection()
    db = _firestore()
    synced_at = time.time()
    with _BatchWriter(db) as writer:
        for line in lines:
            line_no = line.get("lineNo")
            line_id = line.get("id") or line.get("systemId") or ""
            key = str(line_no) if line_no is not None else line_id
            if not key:
                continue
            writer.set(
                db.collection(collection).document(f"{company}_{price_list_code}_{key}"),
                {**line, "company": company, "priceListCode": price_list_code, "syncedAt": synced_at, "env": GCP_ENV},
            )
    logger.info(
        f"Synced {writer.written} price list items → {collection!r} "
        f"(company={company!r}, priceListCode={price_list_code!r})"
    )
    return writer.written


def ile_exists_in_firestore(company: str) -> bool:
    db = _firestore()
    docs = db.collection(_ile_collection()).where("company", "==", company).limit(1).stream()
    return any(True for _ in docs)


def sync_item_ledger_entries_to_firestore(records: Iterable[dict], company: str) -> int:
    """Upsert item ledger entries. Document ID: {company}_{entryNo}."""
    collection = _ile_collection()
    db = _firestore()
    synced_at = time.time()
    with _BatchWriter(db) as writer:
        for record in records:
            entry_no = record.get("entryNo")
            if entry_no is None:
                continue
            writer.set(
                db.collection(collection).document(f"{company}_{entry_no}"),
                {**record, "company": company, "syncedAt": synced_at, "env": GCP_ENV},
            )
    logger.info(f"Synced {writer.written} item ledger entries → {collection!r} (company={company!r})")
    return writer.written


def backfill_ile_columns_in_firestore(records: Iterable[dict], company: str, fields: set[str]) -> int:
    """Patch specific fields on existing ILE documents (merge). Returns count patched."""
    collection = _ile_collection()
    db = _firestore()
    with _BatchWriter(db) as writer:
        for record in records:
            entry_no = record.get("entryNo")
            if entry_no is None:
                continue
            patch = {f: record[f] for f in fields if f in record}
            if not patch:
                continue
            writer.set(db.collection(collection).document(f"{company}_{entry_no}"), patch, merge=True)
    logger.info(f"Backfilled {writer.written} ILE documents in {collection!r} (company={company!r})")
    return writer.written


def apply_best_prices_to_firestore(best: dict[str, dict], company: str) -> int:
    """Patch unitPriceIncVAT + priceListCode on item_prices_{env} from a best-price map (merge)."""
    if not best:
        return 0
    collection = _prices_collection()
    db = _firestore()
    doc_company = company.upper()
    with _BatchWriter(db) as writer:
        for product_no, price_data in best.items():
            writer.set(
                db.collection(collection).document(f"{doc_company}_{product_no}"),
                {"unitPriceIncVAT": price_data["unitPriceIncVAT"], "priceListCode": price_data["priceListCode"]},
                merge=True,
            )
    return writer.written


def backfill_item_prices_per_price_list(
    headers_with_lines: Iterable[dict],
    company: str,
    on_date: str,
    price_list_code: str | None = None,
) -> dict:
    """Patch unitPriceIncVAT and priceListCode on item_prices_{env} from price list lines.

    Consumes headers lazily so an iterator that fetches one header's lines at a time
    keeps memory bounded. Latest line startingDate <= on_date wins; IC lists are skipped.
    """
    acc = BestPriceAccumulator(on_date, price_list_code)
    for header in headers_with_lines:
        acc.add_header(header, header.get("priceListLines") or [])
    if not acc.best:
        logger.info(
            f"backfill_item_prices_per_price_list: no eligible prices found "
            f"(company={company!r}, price_list_code={price_list_code!r})"
        )
        return {"patched": 0, "skipped_no_product_no": 0, "skipped_no_price": 0}
    patched = apply_best_prices_to_firestore(acc.best, company)
    logger.info(
        f"backfill_item_prices_per_price_list: {patched} products patched "
        f"(company={company!r}, on_date={on_date!r}, price_list_code={price_list_code!r})"
    )
    return {"patched": patched, "skipped_no_product_no": 0, "skipped_no_price": 0}


def get_price_list_items_from_firestore(company: str, price_list_code: str | None = None) -> list:
    db = _firestore()
    docs = db.collection(_items_collection()).where("company", "==", company).stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        if price_list_code and data.get("priceListCode") != price_list_code:
            continue
        results.append(data)
    return results

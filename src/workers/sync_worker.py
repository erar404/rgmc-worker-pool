"""Pub/Sub consumer for catalog and Firestore sync messages.

Subscribes to PUBSUB_SYNC_SUBSCRIPTION and dispatches based on the "type" field.

Message formats (JSON):

  Routine sync — all companies (price lists + item prices + item ledger entries + GCS blobs):
    { "type": "routine-sync", "on_date": "YYYY-MM-DD" }

  Single company (same stages as routine-sync, one company):
    { "type": "sync-item-prices", "company": "RGMC", "on_date": "YYYY-MM-DD", "page_size": 500 }

  Single company price list headers only:
    { "type": "sync-price-list-headers", "company": "RGMC" }

  Single company price list items (omit price_list_code to sync all codes):
    { "type": "sync-price-list-items", "company": "RGMC", "price_list_code": "PLH001" }

  Single company item ledger entries only (Firestore):
    { "type": "sync-item-ledger-entries", "company": "RGMC", "since_date": "YYYY-MM-DD" }

  Item ledger entries → BigQuery — DISABLED 2026-09-22, superseded by Airbyte's own
  itemLedgerEntries stream (bc_custom_connector.yaml), which writes to the same
  bc_*_raw.itemLedgerEntries tables directly from BC. See bigquery_ile_service.py and
  the commented-out "bq-sync-ile" branch below for the code kept for reference/rollback.
    { "type": "bq-sync-ile", "company": "RGMC", "since_date": "YYYY-MM-DD" }
    Omit since_date for a full sync. "ALL" expands to all configured companies.

  Patch familyCode field on existing Firestore item price documents:
    { "type": "backfill-family-codes", "company": "RGMC", "on_date": "YYYY-MM-DD" }
    Omit company (or pass "ALL") to process all companies in BC_COMPANIES.

  Patch unitPriceIncVAT + priceListCode on item_prices_{env} from price list lines:
    { "type": "backfill-item-prices", "company": "RGMC", "on_date": "YYYY-MM-DD", "price_list_code": "PLH001" }
    Omit price_list_code to process all non-IC codes. Omit company (or pass "ALL") for all companies.

  Patch target ILE columns on existing Firestore docs (BQ half disabled 2026-09-22,
  see the bq-sync-ile note above — Airbyte now owns BigQuery for itemLedgerEntries):
    { "type": "backfill-ile-columns", "company": "RGMC", "since_date": "YYYY-MM-DD" }

  Connectivity test (bc-api → worker pool):
    { "type": "ping", "sent_at": "ISO8601", "sent_by": "bc-api", "note": "optional" }

Cloud Scheduler publishes { "type": "routine-sync" } to the rgmc-sync topic on a cron schedule.

What a company sync produces
----------------------------
Firestore (unchanged shape): price_list_headers, price_list_items, item_prices (raw BC prices,
then patched by the best-price backfill), item_ledger_entries, sync_state.

GCS (read by the BC API on every catalog request — see gcs_catalog.py for the layout):
families/{FAMILY}.json blobs with the active-price-list overlay already applied for
on_date, a streamed catalog.json, prices.json for change detection, price_overrides.json
for historical-date lookups, price_list_headers.json, customers/contacts/item_categories.

Sync state is persisted to sync_state_{env}, keyed by {company}_{collection_type}.
Incremental syncs use the stored lastSyncAt as a lastModifiedDateTime filter.
"""
import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import pubsub_v1

from src import config
from src.services.bc_client import (
    fetch_contacts,
    fetch_customers,
    fetch_item_categories,
    fetch_item_ledger_entries,
    fetch_price_list_headers,
    fetch_price_list_lines_for_code,
    fetch_v3_catalog,
    get_all_company_names,
    iter_price_list_headers_with_lines,
)
from src.services.gcs_catalog import (
    family_blob_name,
    load_family_index,
    load_family_records,
    load_prices_index,
    save_catalog_streaming,
    save_contacts,
    save_customers,
    save_family_blob,
    save_family_index,
    save_item_categories,
    save_pl_headers,
    save_price_overrides,
    save_prices_index,
    save_search_index,
    slim_record,
)
from src.services.price_firestore_service import (
    apply_best_prices_to_firestore,
    backfill_family_codes,
    backfill_ile_columns_in_firestore,
    backfill_item_prices_per_price_list,
    get_sync_state,
    ile_exists_in_firestore,
    prices_exist_in_firestore,
    set_sync_state,
    sync_item_ledger_entries_to_firestore,
    sync_price_list_headers_to_firestore,
    sync_price_list_items_to_firestore,
    sync_prices_to_firestore,
)
from src.services.price_overlay import (
    BestPriceAccumulator,
    OverrideAccumulator,
    active_price_list_codes,
    compact_index_lines,
    is_ic_code,
)
# bigquery_ile_service is disabled 2026-09-22 — Airbyte's itemLedgerEntries stream now
# writes directly to bc_*_raw.itemLedgerEntries from BC, superseding this worker-pool
# path. Kept importable for reference/rollback; not called anywhere below.
# from src.services.bigquery_ile_service import (
#     backfill_ile_columns_in_bigquery,
#     ensure_table,
#     get_max_last_modified,
#     upsert_ile_to_bigquery,
# )
from src.services.send_mail import notify_error, notify_success

logger = logging.getLogger("worker.sync")


def _now_utc() -> str:
    """Return current UTC time as 'YYYY-MM-DDTHH:MM:SSZ' — used as the sync state timestamp."""
    return datetime.datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_companies(company: str) -> list[str]:
    """Expand company to a list of real BC company names ("ALL" → BC_COMPANIES or BC's list)."""
    if company.upper() != "ALL":
        return [company]
    env_companies = [
        c.strip()
        for c in (config.BC_COMPANIES or config.BC_COMPANY or "").split(",")
        if c.strip() and c.strip().upper() != "ALL"
    ]
    if env_companies:
        return env_companies
    logger.info("BC_COMPANIES not set or contains only 'ALL' — fetching company list from BC")
    return get_all_company_names()


_ILE_PAGE_SIZE = 5000


def _sync_item_ledger_entries(company: str, since_date: str | None = None) -> int:
    """Fetch all item ledger entries for one company (with limit/offset paging) and write to Firestore."""
    sync_start = _now_utc()

    if since_date is not None and not ile_exists_in_firestore(company):
        logger.info(
            f"[{company}] ILE collection empty despite stored sync state "
            f"(since={since_date!r}) — forcing full fetch"
        )
        since_date = None

    total = 0
    offset = 0
    while True:
        records = fetch_item_ledger_entries(company, since_date=since_date, limit=_ILE_PAGE_SIZE, offset=offset)
        if not records:
            break
        written = sync_item_ledger_entries_to_firestore(records, company)
        total += written
        logger.info(
            f"[{company}] ILE page offset={offset}: {len(records)} fetched, {written} written "
            f"(since={since_date!r})"
        )
        if len(records) < _ILE_PAGE_SIZE:
            break
        offset += _ILE_PAGE_SIZE

    if total > 0:
        set_sync_state(company, "item_ledger_entries", sync_start)
    logger.info(f"[{company}] ILE sync complete: {total} records written (since={since_date!r})")
    return total


_BQ_ILE_PAGE_SIZE = 5000

_ILE_BACKFILL_COLUMNS: set[str] = {
    "quantity", "entryType", "itemNo", "sourceType", "description",
    "entryNo", "postingDate", "documentType", "sourceNo", "documentNo",
    "costAmountActual", "salesAmountActual", "lastModifiedDateTime",
}


def _backfill_ile_columns(company: str, since_date: str | None = None) -> dict:
    """Re-fetch ILE from BC and patch target columns in Firestore.

    BigQuery patching is disabled 2026-09-22 (see the "bigquery_ile_service is disabled"
    note near the top imports) — Airbyte's itemLedgerEntries stream owns BQ now. This
    still patches Firestore, which the pricing app/GCS catalog path still reads.
    """
    total_fs = 0
    total_bq = 0  # kept as 0 — BigQuery patching disabled, see docstring
    offset = 0
    while True:
        records = fetch_item_ledger_entries(company, since_date=since_date, limit=_ILE_PAGE_SIZE, offset=offset)
        if not records:
            break
        fs_patched = backfill_ile_columns_in_firestore(records, company, _ILE_BACKFILL_COLUMNS)
        total_fs += fs_patched
        logger.info(
            f"[{company}] backfill offset={offset}: {len(records)} from BC, "
            f"{fs_patched} Firestore patched (BQ patching disabled)"
        )
        if len(records) < _ILE_PAGE_SIZE:
            break
        offset += _ILE_PAGE_SIZE
    logger.info(f"[{company}] backfill-ile-columns complete: {total_fs} FS patched (BQ patching disabled)")
    return {"firestore": total_fs, "bq": total_bq}


# _sync_ile_to_bigquery is disabled 2026-09-22 — see the "bigquery_ile_service is
# disabled" note near the top imports. Kept for reference/rollback; not called anywhere.
# def _sync_ile_to_bigquery(company: str, since_date: str | None = None) -> tuple[int, list[str]]:
#     """Fetch all ILE records for one company from BC and stream them into BigQuery."""
#     cols_added = ensure_table(company)
#
#     if since_date is None:
#         since_date = get_max_last_modified(company)
#         logger.info(f"[{company}] BQ watermark from table: {since_date!r}")
#
#     total = 0
#     offset = 0
#     while True:
#         records = fetch_item_ledger_entries(
#             company,
#             since_date=since_date,
#             limit=_BQ_ILE_PAGE_SIZE,
#             offset=offset,
#         )
#         if not records:
#             break
#         inserted = upsert_ile_to_bigquery(records, company)
#         total += inserted
#         logger.info(
#             f"[{company}] BQ ILE page offset={offset}: {len(records)} fetched, "
#             f"{inserted} inserted (since={since_date!r})"
#         )
#         if len(records) < _BQ_ILE_PAGE_SIZE:
#             break
#         offset += _BQ_ILE_PAGE_SIZE
#
#     logger.info(f"[{company}] BQ ILE sync complete: {total} rows inserted (since={since_date!r})")
#     return total, cols_added


# ---------------------------------------------------------------------------
# Company sync stages
# ---------------------------------------------------------------------------

def _stage_price_lists(company: str, on_date: str, sync_start: str) -> tuple[dict, dict]:
    """Sync price lists and compute the two price maps.

    Returns (overlay, best):
      overlay — API-semantics map baked into the GCS catalog for on_date
      best    — worker-semantics map patched into Firestore item_prices

    Headers are always fetched in full (small). Lines are fetched one header at a time,
    only for headers that changed since the last sync (→ Firestore) or that are active
    Sale lists on on_date (→ overlay). Each header's lines are dropped before the next
    fetch, so peak memory is one price list.
    """
    since = get_sync_state(company, "price_list_headers")
    all_headers = fetch_price_list_headers(company)
    logger.info(f"[{company}] {len(all_headers)} price list headers from BC (since={since!r})")

    changed = {
        h.get("code") for h in all_headers
        if h.get("code") and (since is None or (h.get("lastModifiedDateTime") or "") > since)
    }
    active = active_price_list_codes(all_headers, on_date)
    need_lines = changed | set(active)

    sync_price_list_headers_to_firestore(all_headers, company)
    save_pl_headers(company, [{**h, "company": company} for h in all_headers])

    overlay = OverrideAccumulator(active)
    best = BestPriceAccumulator(on_date)
    index: dict[str, dict] = {}
    items_written = 0

    for header in all_headers:
        code = header.get("code") or ""
        if code not in need_lines:
            continue
        lines = fetch_price_list_lines_for_code(company, code)
        if code in changed:
            n = sync_price_list_items_to_firestore(lines, company, code)
            items_written += n
            logger.info(f"[{company}] price list items [{code}]: {n} written")
        if code in active:
            overlay.add_lines(code, lines)
            index[code] = compact_index_lines(lines)
        best.add_header(header, lines)
        del lines

    save_price_overrides(company, on_date, index)
    if all_headers:
        set_sync_state(company, "price_list_headers", sync_start)

    logger.info(
        f"[{company}] price lists: {len(changed)} changed, {len(active)} active on {on_date}, "
        f"{items_written} items written, overlay covers {len(overlay.as_map())} products"
    )
    return overlay.as_map(), best.best


def _assemble_catalog(
    company: str,
    on_date: str,
    sync_start: str,
    records: list,
    full: bool,
    overlay: dict,
) -> dict[str, int]:
    """Rebuild the GCS family blobs, prices index and streamed catalog.json.

    full=True  — records is the whole catalog; families are grouped from it.
    full=False — records are the items changed since the last sync; each existing family
                 blob is loaded, merged, re-overlaid and saved one family at a time.

    A record whose overlaid price or priceListCode differs from the previous sync gets
    priceChangedAt=sync_start so the app's modified_since delta sync picks it up — BC's
    lastModifiedDateTime does not move when only a price list changes.
    """
    prev_prices = load_prices_index(company)
    new_prices: dict[str, list] = {}
    search_entries: list = []

    def finalize(raw: list, fam: str) -> list:
        out = []
        for rec in raw:
            if rec.get("blocked") is True:
                continue
            pno = rec.get("productNo")
            if not pno:
                continue
            search_entries.append([pno, (rec.get("description") or "").lower(), fam])
            base_price = rec.get("bcUnitPriceIncVAT", rec.get("unitPriceIncVAT"))
            base_plc = rec.get("bcPriceListCode", rec.get("priceListCode"))
            rec = {**rec, "unitPriceIncVAT": base_price, "priceListCode": base_plc,
                   "bcUnitPriceIncVAT": base_price, "bcPriceListCode": base_plc}
            ov = overlay.get(pno)
            if ov:
                rec.update(ov)
            price, plc = rec.get("unitPriceIncVAT"), rec.get("priceListCode")
            prev = prev_prices.get(pno)
            if prev is not None and (prev[0] != price or prev[1] != plc):
                rec["priceChangedAt"] = sync_start
            new_prices[pno] = [price, plc]
            out.append(slim_record(rec))
        return out

    families: dict[str, int] = {}
    slim_by_family: dict[str, list] = {}

    if full:
        groups: dict[str, list] = {}
        for rec in records:
            groups.setdefault(family_blob_name(rec.get("familyCode") or ""), []).append(rec)
        records.clear()
        for fam in list(groups):
            slim = finalize(groups.pop(fam), fam)
            families[fam] = len(slim)
            slim_by_family[fam] = slim
            save_family_blob(company, fam, on_date, on_date, slim)
    else:
        index = load_family_index(company) or {}
        existing = set((index.get("families") or {}).keys())
        changed_by_family: dict[str, list] = {}
        new_family_of: dict[str, str] = {}
        for rec in records:
            fam = family_blob_name(rec.get("familyCode") or "")
            changed_by_family.setdefault(fam, []).append(rec)
            if rec.get("productNo"):
                new_family_of[rec["productNo"]] = fam
        for fam in sorted(existing | set(changed_by_family)):
            by_pno: dict[str, dict] = {}
            if fam in existing:
                for r in load_family_records(company, fam):
                    pno = r.get("productNo")
                    # Drop items that moved to another family in this delta.
                    if pno and new_family_of.get(pno, fam) == fam:
                        by_pno[pno] = r
            for r in changed_by_family.get(fam, []):
                by_pno[r["productNo"]] = r
            slim = finalize(list(by_pno.values()), fam)
            families[fam] = len(slim)
            slim_by_family[fam] = slim
            save_family_blob(company, fam, on_date, on_date, slim)

    save_catalog_streaming(company, on_date, on_date, ((fam, slim_by_family[fam]) for fam in slim_by_family))
    save_family_index(company, on_date, on_date, families)
    save_prices_index(company, new_prices)
    save_search_index(company, search_entries)
    return families


def _sync_company(company: str, on_date: str, page_size: int = 500) -> None:
    """Sync all datasets for a single company.

    Stages 1 (price lists), 2 (v3 catalog fetch + Firestore), 3 (item ledger entries) and
    4 (customers/contacts/categories → GCS) run in parallel. Stage 5 then bakes the price
    overlay from stage 1 into the catalog from stage 2 and rebuilds the GCS blobs, and
    stage 6 patches Firestore item_prices with the best-price map.
    """
    logger.info(f"[{company}] sync started — on_date={on_date!r}")

    # Snapshot time before any BC fetch so records modified during the sync window
    # are picked up on the next run (no gap, small overlap is fine).
    sync_start = _now_utc()

    price_maps: dict = {"overlay": {}, "best": {}}
    catalog: dict = {"records": None, "full": False}

    def _stage_price_lists_wrapped() -> None:
        try:
            overlay, best = _stage_price_lists(company, on_date, sync_start)
            price_maps["overlay"], price_maps["best"] = overlay, best
        except Exception as e:
            logger.error(f"[{company}] price list headers/items failed — {e}")

    def _stage_v3_catalog() -> None:
        try:
            since_prices = get_sync_state(company, "item_prices")
            # The collection being empty despite a stored state means the previous run's
            # writes were lost — force a full fetch so it actually gets populated.
            if since_prices is not None and not prices_exist_in_firestore(company):
                logger.info(
                    f"[{company}] item_prices collection empty despite stored sync state "
                    f"(since={since_prices!r}) — forcing full fetch"
                )
                since_prices = None
            # First run after the GCS layout change has no family index yet — a full
            # fetch is the only way to build it.
            if since_prices is not None and load_family_index(company) is None:
                logger.info(f"[{company}] no GCS family index yet — forcing full catalog fetch")
                since_prices = None
            records = fetch_v3_catalog(company, on_date, since=since_prices)
            total = sync_prices_to_firestore(records, company, on_date)
            if records:
                set_sync_state(company, "item_prices", sync_start)
            catalog["records"] = records
            catalog["full"] = since_prices is None
            logger.info(f"[{company}] {total} item prices written total (since={since_prices!r})")
        except Exception as e:
            logger.error(f"[{company}] item prices failed — {e}")

    def _stage_ile() -> None:
        try:
            since_ile = get_sync_state(company, "item_ledger_entries")
            since_date_ile = since_ile[:10] if since_ile else None
            _sync_item_ledger_entries(company, since_date=since_date_ile)
        except Exception as e:
            logger.error(f"[{company}] item ledger entries failed — {e}")

    def _stage_supporting_datasets() -> None:
        def _sync_customers() -> None:
            try:
                customers = fetch_customers(company)
                save_customers(company, customers)
            except Exception as e:
                logger.error(f"[{company}] customers GCS sync failed — {e}")

        def _sync_contacts() -> None:
            try:
                contacts = fetch_contacts(company)
                save_contacts(company, contacts)
            except Exception as e:
                logger.error(f"[{company}] contacts GCS sync failed — {e}")

        def _sync_item_categories() -> None:
            try:
                categories = fetch_item_categories(company)
                save_item_categories(company, categories)
            except Exception as e:
                logger.error(f"[{company}] item categories GCS sync failed — {e}")

        with ThreadPoolExecutor(max_workers=3) as ex:
            for f in as_completed([
                ex.submit(_sync_customers),
                ex.submit(_sync_contacts),
                ex.submit(_sync_item_categories),
            ]):
                f.result()

    with ThreadPoolExecutor(max_workers=4) as executor:
        for f in as_completed([
            executor.submit(_stage_price_lists_wrapped),
            executor.submit(_stage_v3_catalog),
            executor.submit(_stage_ile),
            executor.submit(_stage_supporting_datasets),
        ]):
            f.result()

    # Stage 5: bake the overlay into the catalog and rebuild the GCS blobs.
    records = catalog["records"]
    if records is not None and (records or catalog["full"] is False):
        try:
            families = _assemble_catalog(company, on_date, sync_start, records, catalog["full"], price_maps["overlay"])
            logger.info(f"[{company}] GCS catalog rebuilt: {sum(families.values())} records in {len(families)} families")
        except Exception as e:
            logger.error(f"[{company}] GCS catalog assembly failed — {e}")
    elif records is not None:
        logger.warning(f"[{company}] full catalog fetch returned 0 records — GCS blobs left unchanged")

    # Stage 6: patch Firestore item_prices with the best price per product.
    if price_maps["best"]:
        try:
            patched = apply_best_prices_to_firestore(price_maps["best"], company)
            logger.info(f"[{company}] item price backfill from price lists: {patched} patched")
        except Exception as e:
            logger.error(f"[{company}] item price backfill from price lists failed — {e}")

    logger.info(f"[{company}] sync complete")


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"Sync message decode failed: {e} — acking to drop poison pill")
        message.ack()
        return

    msg_type: str = data.get("type", "routine-sync")
    on_date: str = data.get("on_date") or datetime.date.today().isoformat()
    page_size: int = min(int(data.get("page_size", 500)), 500)

    try:
        if msg_type == "routine-sync":
            raw = [c.strip() for c in (config.BC_COMPANIES or config.BC_COMPANY or "").split(",") if c.strip()]
            companies = [c for c in raw if c.upper() != "ALL"]
            if not companies:
                companies = get_all_company_names()
            for company in companies:
                _sync_company(company, on_date, page_size)
            notify_success(
                title="Routine Sync Complete",
                detail="\n".join(companies),
                context=f"on_date={on_date}",
            )

        elif msg_type == "sync-item-prices":
            company = data.get("company") or config.BC_COMPANY
            _sync_company(company, on_date, page_size)
            notify_success(
                title=f"Item Prices Sync Complete — {company}",
                detail=f"Company: {company}",
                context=f"on_date={on_date}",
            )

        elif msg_type == "sync-price-list-headers":
            company = data.get("company") or config.BC_COMPANY
            sync_start = _now_utc()
            headers = fetch_price_list_headers(company)
            written = sync_price_list_headers_to_firestore(headers, company)
            save_pl_headers(company, [{**h, "company": company} for h in headers])
            if headers:
                set_sync_state(company, "price_list_headers", sync_start)
            logger.info(f"[{company}] {written} price list headers written")
            notify_success(
                title=f"Price List Headers Sync Complete — {company}",
                detail=f"Company: {company}\n{written} headers written to Firestore",
                context="full",
            )

        elif msg_type == "sync-price-list-items":
            company = data.get("company") or config.BC_COMPANY
            price_list_code = data.get("price_list_code")
            companies = _get_companies(company)
            total = 0
            for c in companies:
                sync_start = _now_utc()
                since_headers = get_sync_state(c, "price_list_headers")
                headers = fetch_price_list_headers(c)
                if price_list_code:
                    codes = [price_list_code]
                else:
                    codes = [
                        h.get("code") for h in headers
                        if h.get("code") and (since_headers is None or (h.get("lastModifiedDateTime") or "") > since_headers)
                    ]
                company_total = 0
                for code in codes:
                    lines = fetch_price_list_lines_for_code(c, code)
                    written = sync_price_list_items_to_firestore(lines, c, code)
                    company_total += written
                    logger.info(f"[{c}] price list items [{code}]: {written} written")
                    del lines
                if headers and not price_list_code:
                    set_sync_state(c, "price_list_headers", sync_start)
                logger.info(f"[{c}] {company_total} price list items written total")
                total += company_total
            notify_success(
                title=f"Price List Items Sync Complete — {company}",
                detail=f"Companies: {', '.join(companies)}\n{total} items written to Firestore",
                context=f"price_list_code={price_list_code or 'all'}",
            )

        elif msg_type == "sync-item-ledger-entries":
            company = data.get("company") or "ALL"
            since_date = data.get("since_date")
            companies = _get_companies(company)
            total = 0
            for c in companies:
                total += _sync_item_ledger_entries(c, since_date=since_date)
            notify_success(
                title=f"Item Ledger Entries Sync Complete — {company}",
                detail=f"Companies: {', '.join(companies)}\n{total} records written to Firestore",
                context=f"since_date={since_date or 'full'}",
            )

        elif msg_type == "backfill-family-codes":
            company = data.get("company") or "ALL"
            companies = _get_companies(company)
            for c in companies:
                logger.info(f"[{c}] backfill-family-codes: fetching v3 catalog from BC (on_date={on_date!r})")
                records = fetch_v3_catalog(c, on_date=on_date)
                logger.info(f"[{c}] backfill-family-codes: {len(records)} records from BC — patching Firestore familyCode")
                if not records:
                    logger.warning(f"[{c}] backfill-family-codes: BC returned 0 records — nothing to patch")
                    notify_success(
                        title=f"Family Code Backfill Skipped — {c}",
                        detail=f"Company: {c}\nBC returned 0 records for on_date={on_date} — Firestore unchanged.",
                        context=f"on_date={on_date}",
                    )
                else:
                    result = backfill_family_codes(records, c)
                    logger.info(
                        f"[{c}] backfill-family-codes complete: {result['patched']} patched, "
                        f"{result['skipped_missing_product_no']} skipped (no productNo), "
                        f"{result.get('skipped_no_family_code', 0)} skipped (no familyCode from BC)"
                    )
                    notify_success(
                        title=f"Family Code Backfill Complete — {c}",
                        detail=(
                            f"Company: {c}\n"
                            f"{result['patched']} documents patched\n"
                            f"{result['skipped_missing_product_no']} skipped (no productNo)\n"
                            f"{result.get('skipped_no_family_code', 0)} skipped (BC returned no familyCode)"
                        ),
                        context=f"on_date={on_date} bc_records={len(records)}",
                    )

        elif msg_type == "backfill-item-prices":
            company = data.get("company") or "ALL"
            price_list_code = data.get("price_list_code")
            companies = _get_companies(company)
            for c in companies:
                headers = fetch_price_list_headers(c)
                codes = [
                    h.get("code") for h in headers
                    if h.get("code") and not is_ic_code(h["code"])
                    and (not price_list_code or h["code"] == price_list_code)
                ]
                logger.info(
                    f"[{c}] backfill-item-prices: {len(codes)} price lists to read "
                    f"(price_list_code={price_list_code!r})"
                )
                result = backfill_item_prices_per_price_list(
                    iter_price_list_headers_with_lines(c, codes), c, on_date, price_list_code
                )
                logger.info(
                    f"[{c}] backfill-item-prices complete: {result['patched']} patched "
                    f"(price_list_code={price_list_code or 'all'})"
                )
                notify_success(
                    title=f"Item Prices Backfill Complete — {c}",
                    detail=(
                        f"Company: {c}\n"
                        f"{result['patched']} items patched with price list prices\n"
                        f"Price list: {price_list_code or 'all active non-IC codes'}"
                    ),
                    context=f"on_date={on_date} price_list_code={price_list_code or 'all'}",
                )

        # bq-sync-ile is disabled 2026-09-22 — Airbyte's own itemLedgerEntries stream
        # (bc_custom_connector.yaml) now writes bc_*_raw.itemLedgerEntries directly from
        # BC, making this worker-pool path redundant (and a dual-write risk if both ran).
        # Original handler body kept commented for reference/rollback.
        elif msg_type == "bq-sync-ile":
            logger.warning(
                "bq-sync-ile received but is disabled — Airbyte's itemLedgerEntries "
                "stream now owns this sync. Message acked without action."
            )
            notify_success(
                title="BQ ILE Sync Skipped (disabled)",
                detail=(
                    "bq-sync-ile is disabled as of 2026-09-22 — Airbyte's itemLedgerEntries "
                    "stream now syncs BC item ledger entries straight to bc_*_raw.itemLedgerEntries. "
                    "No BigQuery write was performed by the worker pool for this message."
                ),
                context=f"company={data.get('company') or 'ALL'} since_date={data.get('since_date')}",
            )
        # elif msg_type == "bq-sync-ile":
        #     company = data.get("company") or "ALL"
        #     since_date = data.get("since_date")
        #     triggered_at = data.get("triggered_at", "")
        #     companies = _get_companies(company)
        #     inserted_by_company: dict[str, int] = {}
        #     cols_added_by_company: dict[str, list[str]] = {}
        #     errors_by_company: dict[str, str] = {}
        #     for c in companies:
        #         try:
        #             rows, cols = _sync_ile_to_bigquery(c, since_date=since_date)
        #             inserted_by_company[c] = rows
        #             if cols:
        #                 cols_added_by_company[c] = cols
        #         except Exception as exc:
        #             logger.error(f"[{c}] BQ ILE sync failed: {exc}")
        #             errors_by_company[c] = str(exc)
        #
        #     context = (
        #         f"since_date={since_date or 'auto (BQ watermark)'}"
        #         + (f" | triggered_at={triggered_at}" if triggered_at else "")
        #     )
        #
        #     def _company_line(c: str) -> str:
        #         rows = inserted_by_company.get(c, 0)
        #         line = f"{c}: {rows} rows inserted"
        #         if c in cols_added_by_company:
        #             names = ", ".join(cols_added_by_company[c])
        #             line += f" | {len(cols_added_by_company[c])} column(s) added to table ({names})"
        #         return line
        #
        #     if errors_by_company:
        #         error_lines = [f"{c}: {err}" for c, err in errors_by_company.items()]
        #         success_lines = [_company_line(c) for c in inserted_by_company]
        #         detail = "\n".join(
        #             (["=== FAILED ==="] + error_lines)
        #             + (["\n=== SUCCEEDED ==="] + success_lines if success_lines else [])
        #         )
        #         notify_error(
        #             title=f"BQ ILE Sync {'Partial Failure' if inserted_by_company else 'Failed'} — {company}",
        #             detail=detail,
        #             context=context,
        #         )
        #     else:
        #         total = sum(inserted_by_company.values())
        #         lines = [_company_line(c) for c in inserted_by_company]
        #         notify_success(
        #             title=f"BQ ILE Sync Complete — {company}",
        #             detail=f"Total rows inserted: {total}\n\n" + "\n".join(lines),
        #             context=context,
        #         )

        elif msg_type == "backfill-ile-columns":
            company = data.get("company") or "ALL"
            since_date = data.get("since_date")
            companies = _get_companies(company)
            results: dict[str, dict] = {}
            errors: dict[str, str] = {}
            for c in companies:
                try:
                    results[c] = _backfill_ile_columns(c, since_date=since_date)
                except Exception as exc:
                    logger.error(f"[{c}] backfill-ile-columns failed: {exc}")
                    errors[c] = str(exc)
            total_fs = sum(r["firestore"] for r in results.values())
            total_bq = sum(r["bq"] for r in results.values())
            detail_lines = [
                f"{c}: {r['firestore']} Firestore patched, {r['bq']} BQ rows upserted"
                for c, r in results.items()
            ]
            if errors:
                error_lines = [f"{c}: {err}" for c, err in errors.items()]
                notify_error(
                    title=f"ILE Column Backfill {'Partial Failure' if results else 'Failed'} — {company}",
                    detail="\n".join(
                        ["=== FAILED ==="] + error_lines
                        + (["\n=== SUCCEEDED ==="] + detail_lines if detail_lines else [])
                    ),
                    context=f"since_date={since_date or 'full'}",
                )
            else:
                notify_success(
                    title=f"ILE Column Backfill Complete — {company}",
                    detail=f"Total: {total_fs} Firestore docs patched, {total_bq} BQ rows upserted\n\n" + "\n".join(detail_lines),
                    context=f"since_date={since_date or 'full'} columns={sorted(_ILE_BACKFILL_COLUMNS)}",
                )

        elif msg_type == "ping":
            sent_at = data.get("sent_at", "unknown")
            sent_by = data.get("sent_by", "unknown")
            note = data.get("note", "")
            detail = f"Ping received from {sent_by}.\nSent at: {sent_at}"
            if note:
                detail += f"\nNote: {note}"
            notify_success(
                title="Worker Pool Ping ACK",
                detail=detail,
                context=f"subscription={config.PUBSUB_SYNC_SUBSCRIPTION}",
            )
            logger.info(f"Ping ACK — sent_by={sent_by!r} sent_at={sent_at!r}")

        else:
            logger.warning(f"Unknown sync message type: {msg_type!r} — acking to discard")

        message.ack()

    except Exception as e:
        logger.error(f"Sync message failed (type={msg_type!r}): {e}")
        notify_error(
            title=f"Sync Worker Error — {msg_type}",
            detail=str(e),
            context=f"on_date={on_date} page_size={page_size}",
        )
        message.nack()  # let Pub/Sub redeliver for transient BC/Firestore errors


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the sync Pub/Sub subscriber and return the future for lifecycle management."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_SYNC_SUBSCRIPTION,
    )
    # max_messages=1: sync jobs hold large BC payloads in memory; no concurrent syncs
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    future = subscriber.subscribe(
        subscription_path,
        callback=_process,
        flow_control=flow_control,
    )
    logger.info(f"Sync worker subscribed to {subscription_path}")
    return future

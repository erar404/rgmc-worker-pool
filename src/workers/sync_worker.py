"""Pub/Sub consumer for catalog and Firestore sync messages.

Subscribes to PUBSUB_SYNC_SUBSCRIPTION and dispatches based on the "type" field.

Message formats (JSON):

  Routine sync — all companies (price list headers + item prices + item ledger entries):
    { "type": "routine-sync", "on_date": "YYYY-MM-DD" }

  Single company item prices (also syncs price list headers + item ledger entries):
    { "type": "sync-item-prices", "company": "RGMC", "on_date": "YYYY-MM-DD", "page_size": 500 }

  Single company price list headers only:
    { "type": "sync-price-list-headers", "company": "RGMC" }

  Single company price list items (omit price_list_code to sync all codes):
    { "type": "sync-price-list-items", "company": "RGMC", "price_list_code": "PLH001" }

  Single company item ledger entries only (Firestore):
    { "type": "sync-item-ledger-entries", "company": "RGMC", "since_date": "YYYY-MM-DD" }

  Item ledger entries → BigQuery (uses BC pagination endpoint directly):
    { "type": "bq-sync-ile", "company": "RGMC", "since_date": "YYYY-MM-DD" }
    Omit since_date for a full sync. "ALL" expands to all configured companies.

  Connectivity test (bc-api → worker pool):
    { "type": "ping", "sent_at": "ISO8601", "sent_by": "bc-api", "note": "optional" }

Cloud Scheduler publishes { "type": "routine-sync" } to the rgmc-sync topic on a cron schedule.
The main API can publish any of the above to trigger targeted syncs or test connectivity.

Sync state is persisted to sync_state_{env} Firestore collection, keyed by
{company}_{collection_type} (e.g. "RGMC_item_ledger_entries"). Incremental syncs
use the stored lastSyncAt timestamp as a modifiedFrom filter. Missing collection →
forced full fetch even when a state record exists.
"""
import datetime
import json
import logging

from google.cloud import pubsub_v1

from src import config
from src.services.bc_client import (
    fetch_item_ledger_entries,
    fetch_price_list_headers,
    fetch_price_list_headers_with_lines,
    fetch_v3_catalog,
    get_all_company_names,
)
from src.services.gcs_catalog import save_catalog
from src.services.price_firestore_service import (
    get_sync_state,
    ile_exists_in_firestore,
    prices_exist_in_firestore,
    set_sync_state,
    sync_item_ledger_entries_to_firestore,
    sync_price_list_headers_to_firestore,
    sync_price_list_items_to_firestore,
    sync_prices_to_firestore,
)
from src.services.bigquery_ile_service import get_max_last_modified, upsert_ile_to_bigquery
from src.services.send_mail import notify_error, notify_success

logger = logging.getLogger("worker.sync")


def _now_utc() -> str:
    """Return current UTC time as 'YYYY-MM-DDTHH:MM:SSZ' — used as the sync state timestamp."""
    return datetime.datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_companies(company: str) -> list[str]:
    """Expand company to a list of real BC company names.

    If company is "ALL" (or empty), returns companies from BC_COMPANIES env var
    (comma-separated), filtering out "ALL" entries. If BC_COMPANIES is not set or
    still resolves to nothing, falls back to fetching the full list from BC.
    """
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
    """Fetch all item ledger entries for one company (with limit/offset paging) and write to Firestore.

    since_date (YYYY-MM-DD): only fetch records modified on or after this date.
    Omit for a full fetch. Returns total records written.
    """
    sync_start = _now_utc()

    # If we have a stored sync timestamp but the collection is empty, force a full re-fetch.
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


def _sync_ile_to_bigquery(company: str, since_date: str | None = None) -> int:
    """Fetch all ILE records for one company from BC and stream them into BigQuery.

    When since_date is None, queries the BQ table for MAX(lastModifiedDateTime) of
    the company's existing rows and uses that date as the incremental watermark.
    Pass since_date explicitly (YYYY-MM-DD) to override, or pass "" to force a full fetch.
    Returns total rows inserted into BigQuery.
    """
    if since_date is None:
        since_date = get_max_last_modified(company)
        logger.info(f"[{company}] BQ watermark from table: {since_date!r}")

    total = 0
    offset = 0
    while True:
        records = fetch_item_ledger_entries(
            company,
            since_date=since_date,
            limit=_BQ_ILE_PAGE_SIZE,
            offset=offset,
        )
        if not records:
            break
        inserted = upsert_ile_to_bigquery(records, company)
        total += inserted
        logger.info(
            f"[{company}] BQ ILE page offset={offset}: {len(records)} fetched, "
            f"{inserted} inserted (since={since_date!r})"
        )
        if len(records) < _BQ_ILE_PAGE_SIZE:
            break
        offset += _BQ_ILE_PAGE_SIZE

    logger.info(f"[{company}] BQ ILE sync complete: {total} rows inserted (since={since_date!r})")
    return total


def _sync_company(company: str, on_date: str, page_size: int = 500) -> None:
    """Sync price list headers then item price catalog for a single company.

    Uses lastModifiedDateTime-based incremental sync: only records modified since the
    last successful sync are fetched from BC and written to Firestore. First run (no
    stored sync state) falls back to a full fetch.
    """
    logger.info(f"[{company}] sync started — on_date={on_date!r}")

    # Snapshot time before any BC fetch so records modified during the sync window
    # are picked up on the next run (no gap, small overlap is fine).
    sync_start = _now_utc()

    try:
        since_headers = get_sync_state(company, "price_list_headers")
        headers_with_lines = fetch_price_list_headers_with_lines(company, since=since_headers)
        headers = [{k: v for k, v in h.items() if k != "priceListLines"} for h in headers_with_lines]
        written = sync_price_list_headers_to_firestore(headers, company)
        logger.info(
            f"[{company}] {written} price list headers written "
            f"(since={since_headers!r})"
        )
        total_items = 0
        for header in headers_with_lines:
            code = header.get("code") or ""
            lines = header.get("priceListLines") or []
            if not (code and lines):
                continue
            written_items = sync_price_list_items_to_firestore(lines, company, code)
            total_items += written_items
            logger.info(f"[{company}] price list items [{code}]: {written_items} written")
        if headers_with_lines:
            set_sync_state(company, "price_list_headers", sync_start)
        logger.info(f"[{company}] {total_items} price list items written total")
    except Exception as e:
        logger.error(f"[{company}] price list headers/items failed — {e}")

    try:
        since_prices = get_sync_state(company, "item_prices")
        # If the collection is empty despite a stored sync timestamp, the previous sync
        # completed (setting the state) but Firestore writes failed or were lost.
        # Force a full fetch so we actually populate the collection.
        if since_prices is not None and not prices_exist_in_firestore(company):
            logger.info(
                f"[{company}] item_prices collection empty despite stored sync state "
                f"(since={since_prices!r}) — forcing full fetch"
            )
            since_prices = None
        records = fetch_v3_catalog(company, on_date, since=since_prices)
        # Only save a full GCS snapshot on full-fetch runs; incremental results are partial.
        if since_prices is None:
            save_catalog(company, on_date, records)
        total = 0
        for i in range(0, len(records), page_size):
            chunk = records[i : i + page_size]
            total += sync_prices_to_firestore(chunk, company, on_date)
            logger.info(
                f"[{company}] item prices page {i // page_size + 1}: "
                f"{len(chunk)} records (offset={i})"
            )
        if records:
            set_sync_state(company, "item_prices", sync_start)
        logger.info(
            f"[{company}] {total} item prices written total "
            f"(since={since_prices!r})"
        )
    except Exception as e:
        logger.error(f"[{company}] item prices failed — {e}")

    try:
        since_ile = get_sync_state(company, "item_ledger_entries")
        # Convert stored UTC datetime string to date string for BC's modifiedFrom filter.
        since_date_ile = since_ile[:10] if since_ile else None
        _sync_item_ledger_entries(company, since_date=since_date_ile)
    except Exception as e:
        logger.error(f"[{company}] item ledger entries failed — {e}")

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
            # Split BC_COMPANIES/BC_COMPANY; filter out "ALL" sentinels; fall back to BC API.
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
            since_headers = get_sync_state(company, "price_list_headers")
            headers_with_lines = fetch_price_list_headers_with_lines(company, since=since_headers)
            headers = [{k: v for k, v in h.items() if k != "priceListLines"} for h in headers_with_lines]
            written = sync_price_list_headers_to_firestore(headers, company)
            if headers_with_lines:
                set_sync_state(company, "price_list_headers", sync_start)
            logger.info(f"[{company}] {written} price list headers written (since={since_headers!r})")
            notify_success(
                title=f"Price List Headers Sync Complete — {company}",
                detail=f"Company: {company}\n{written} headers written to Firestore",
                context=f"since={since_headers or 'full'}",
            )

        elif msg_type == "sync-price-list-items":
            company = data.get("company") or config.BC_COMPANY
            price_list_code = data.get("price_list_code")
            sync_start = _now_utc()
            since_headers = get_sync_state(company, "price_list_headers")
            # Use since only when syncing all codes; targeted single-code sync is always full.
            effective_since = since_headers if not price_list_code else None
            headers_with_lines = fetch_price_list_headers_with_lines(company, since=effective_since)
            total = 0
            for header in headers_with_lines:
                code = header.get("code") or ""
                lines = header.get("priceListLines") or []
                if not code:
                    continue
                if price_list_code and code != price_list_code:
                    continue
                written = sync_price_list_items_to_firestore(lines, company, code)
                total += written
                logger.info(f"[{company}] price list items [{code}]: {written} written")
            if headers_with_lines and not price_list_code:
                set_sync_state(company, "price_list_headers", sync_start)
            logger.info(f"[{company}] {total} price list items written total")
            notify_success(
                title=f"Price List Items Sync Complete — {company}",
                detail=f"Company: {company}\n{total} items written to Firestore",
                context=f"price_list_code={price_list_code or 'all'} since={effective_since or 'full'}",
            )

        elif msg_type == "sync-item-ledger-entries":
            company = data.get("company") or "ALL"
            since_date = data.get("since_date")  # YYYY-MM-DD, optional
            companies = _get_companies(company)
            total = 0
            for c in companies:
                total += _sync_item_ledger_entries(c, since_date=since_date)
            notify_success(
                title=f"Item Ledger Entries Sync Complete — {company}",
                detail=f"Companies: {', '.join(companies)}\n{total} records written to Firestore",
                context=f"since_date={since_date or 'full'}",
            )

        elif msg_type == "bq-sync-ile":
            company = data.get("company") or "ALL"
            # since_date (YYYY-MM-DD) overrides the BQ watermark; omit for auto-incremental.
            # Pass "" to force a full re-fetch regardless of existing BQ data.
            since_date = data.get("since_date")
            triggered_at = data.get("triggered_at", "")
            companies = _get_companies(company)
            inserted_by_company: dict[str, int] = {}
            errors_by_company: dict[str, str] = {}
            for c in companies:
                try:
                    inserted_by_company[c] = _sync_ile_to_bigquery(c, since_date=since_date)
                except Exception as exc:
                    logger.error(f"[{c}] BQ ILE sync failed: {exc}")
                    errors_by_company[c] = str(exc)

            context = (
                f"since_date={since_date or 'auto (BQ watermark)'}"
                + (f" | triggered_at={triggered_at}" if triggered_at else "")
            )

            if errors_by_company:
                error_lines = [f"{c}: {err}" for c, err in errors_by_company.items()]
                success_lines = [f"{c}: {n} rows inserted" for c, n in inserted_by_company.items()]
                detail = "\n".join(
                    (["=== FAILED ==="] + error_lines)
                    + (["\n=== SUCCEEDED ==="] + success_lines if success_lines else [])
                )
                notify_error(
                    title=f"BQ ILE Sync {'Partial Failure' if inserted_by_company else 'Failed'} — {company}",
                    detail=detail,
                    context=context,
                )
            else:
                total = sum(inserted_by_company.values())
                lines = [f"{c}: {n} rows inserted" for c, n in inserted_by_company.items()]
                notify_success(
                    title=f"BQ ILE Sync Complete — {company}",
                    detail=f"Total rows inserted: {total}\n\n" + "\n".join(lines),
                    context=context,
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

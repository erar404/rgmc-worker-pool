"""BigQuery writer for Item Ledger Entry records fetched from Business Central.

Table: {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.item_ledger_entries
  - Partitioned by postingDate (DAY)
  - Clustered by company, entryType, locationCode

Writes use streaming inserts with row_id = '{company}_{entryNo}' for best-effort
deduplication within the streaming buffer (~1 minute window). Callers should treat
the table as append-only and use DISTINCT / QUALIFY in queries, or run a periodic
dedup job, if strict uniqueness is required.

Fields excluded from BC records before writing:
  limit, offset, modifiedFrom, modifiedTo, modifiedMonth, modifiedYear,
  modifiedAsOfDate  — AL pagination filter artifacts, not real BC data
  env, syncedAt     — Firestore metadata, replaced with bq_synced_at
  @odata.etag       — OData internal field
"""
import logging
import time

from google.cloud import bigquery

from src import config

logger = logging.getLogger("bigquery_ile_service")

_client: bigquery.Client | None = None
_TABLE_NAME = "item_ledger_entries"

_EXCLUDE_FIELDS = {
    "limit", "offset", "modifiedFrom", "modifiedTo",
    "modifiedMonth", "modifiedYear", "modifiedAsOfDate",
    "env", "syncedAt", "@odata.etag",
}

_DATE_FIELDS = {"postingDate", "documentDate", "warrantyDate", "expirationDate", "lastInvoiceDate"}
_TIMESTAMP_FIELDS = {"lastModifiedDateTime", "biTimestamp"}
_NULL_DATE = "0001-01-01"
_NULL_TIMESTAMP_PREFIX = "0001-01-01"

_SCHEMA = [
    bigquery.SchemaField("entryNo",                    "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("postingDate",                "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("documentDate",               "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("itemNo",                     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("description",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("locationCode",               "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("quantity",                   "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("remainingQuantity",          "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("invoicedQuantity",           "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("qtyPerUnitOfMeasure",        "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("shippedQtyNotReturned",      "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("unitOfMeasureCode",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("entryType",                  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("documentType",               "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("documentNo",                 "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("externalDocumentNo",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("orderNo",                    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("orderType",                  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("orderLineNo",                "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("documentLineNo",             "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("open",                       "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("positive",                   "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("correction",                 "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("completelyInvoiced",         "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("nonstock",                   "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("dropShipment",               "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("outOfStockSubstitution",     "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("assembleToOrder",            "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("jobPurchase",                "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("appliedEntryToAdjust",       "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("derivedFromBlanketOrder",    "BOOLEAN",   mode="NULLABLE"),
    bigquery.SchemaField("variantCode",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("serialNo",                   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("lotNo",                      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("itemCategoryCode",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("itemTracking",               "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("globalDimension1Code",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("globalDimension2Code",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("dimensionSetId",             "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("sourceType",                 "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("sourceNo",                   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("jobNo",                      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("jobTaskNo",                  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("noSeries",                   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("purchasingCode",             "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("returnReasonCode",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("countryRegionCode",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("transactionType",            "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("transactionSpecification",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("entryExitPoint",             "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("intrastatArea",              "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("transportMethod",            "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("prodOrderCompLineNo",        "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("appliesToEntry",             "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("originallyOrderedNo",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("originallyOrderedVariantCode", "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("lastModifiedDateTime",       "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("lastInvoiceDate",            "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("warrantyDate",               "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("expirationDate",             "DATE",      mode="NULLABLE"),
    # TableExt 50456 fields
    bigquery.SchemaField("transferType",               "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("batchNo",                    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("offerNo",                    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("promotionNo",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("statementNo",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("biTimestamp",                "TIMESTAMP", mode="NULLABLE"),
    # Identity / metadata
    bigquery.SchemaField("id",                         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("company",                    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("companyName",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("bq_synced_at",               "TIMESTAMP", mode="NULLABLE"),
]


def _bq() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=config.BIGQUERY_PROJECT_ID)
    return _client


def _table_id() -> str:
    return f"{config.BIGQUERY_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.{_TABLE_NAME}"


def ensure_table() -> None:
    """Create the ILE BigQuery table if it does not already exist."""
    client = _bq()
    tid = _table_id()
    try:
        client.get_table(tid)
        return
    except Exception:
        pass
    table = bigquery.Table(tid, schema=_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="postingDate",
    )
    table.clustering_fields = ["company", "entryType", "locationCode"]
    client.create_table(table, exists_ok=True)
    logger.info(f"Created BigQuery table {tid}")


def _clean(record: dict, bq_synced_at: str) -> dict:
    """Return a copy of record with pagination artifacts removed and types coerced for BQ."""
    row = {k: v for k, v in record.items() if k not in _EXCLUDE_FIELDS}
    # Null out BC's default "not set" date / timestamp sentinel values
    for field in _DATE_FIELDS:
        if row.get(field) == _NULL_DATE:
            row[field] = None
    for field in _TIMESTAMP_FIELDS:
        val = row.get(field) or ""
        if val.startswith(_NULL_TIMESTAMP_PREFIX):
            row[field] = None
    row["bq_synced_at"] = bq_synced_at
    return row


def upsert_ile_to_bigquery(records: list[dict], company: str) -> int:
    """Stream ILE records into BigQuery. Returns the count of rows inserted.

    row_id is set to '{company}_{entryNo}' for best-effort deduplication within
    the streaming buffer. The table is append-only; use QUALIFY ROW_NUMBER() or
    a scheduled dedup query for strict uniqueness over historical data.
    """
    if not records:
        return 0
    if not config.BIGQUERY_PROJECT_ID or not config.BIGQUERY_DATASET_ID:
        logger.warning("BIGQUERY_PROJECT_ID or BIGQUERY_DATASET_ID not configured — skipping BQ write")
        return 0

    ensure_table()
    client = _bq()
    tid = _table_id()
    bq_synced_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    rows = [_clean(r, bq_synced_at) for r in records]
    row_ids = [f"{company}_{r.get('entryNo', i)}" for i, r in enumerate(rows)]

    errors = client.insert_rows_json(tid, rows, row_ids=row_ids)
    if errors:
        logger.error(f"[{company}] BigQuery streaming insert errors (first 3): {errors[:3]}")
        return 0
    return len(rows)

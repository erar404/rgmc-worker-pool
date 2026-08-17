"""BigQuery writer for Item Ledger Entry records fetched from Business Central.

Table: {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.itemLedgerEntries
  - Partitioned by postingDate (DAY)
  - Clustered by company, entryType, locationCode

Writes use a load-to-staging + MERGE pattern keyed on (company, entryNo).
Each call loads rows into a temporary staging table, runs a MERGE into the main
table (UPDATE on match, INSERT on new), then drops the staging table. This
guarantees true upsert semantics with no duplicate rows regardless of how many
times the same entryNo is fetched from BC.

BC's modifiedFrom filter is a Date type — it always re-fetches the entire
watermark day. MERGE handles the resulting overlap without producing duplicates.

Fields excluded from BC records before writing:
  limit, offset, modifiedFrom, modifiedTo, modifiedMonth, modifiedYear,
  modifiedAsOfDate  — AL pagination filter artifacts, not real BC data
  env, syncedAt     — Firestore metadata, replaced with bq_synced_at
  @odata.etag       — OData internal field
"""
import logging
import time
import uuid

from google.cloud import bigquery

from src import config

logger = logging.getLogger("bigquery_ile_service")

_client: bigquery.Client | None = None
_TABLE_NAME = "itemLedgerEntries"

_DATASET_MAP: dict[str, str] = {
    "USGI":  "bc_usgi_raw",
    "CGI":   "bc_covent_raw",
    "KW1":   "bc_keywest_raw",
    "LGAP":  "bc_lgap_raw",
    "RGMC":  "bc_richfield_raw",
    "SBIC":  "bc_sbic_raw",
}

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
    # Airbyte compatibility columns
    bigquery.SchemaField("_airbyte_raw_id",            "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("_airbyte_extracted_at",      "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_airbyte_meta",              "JSON",      mode="NULLABLE"),
    bigquery.SchemaField("_airbyte_generation_id",     "INTEGER",   mode="NULLABLE"),
    # Legacy Airbyte / CDC compatibility columns (null-filled; not sourced from BC ILE)
    bigquery.SchemaField("ab_id",                      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("_ab_cdc_lsn",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("_ab_cdc_cursor",             "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("_ab_cdc_deleted_at",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("_ab_cdc_updated_at",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("_ab_cdc_event_serial_no",    "STRING",    mode="NULLABLE"),
    # BC fields absent from ILE (Table 32) — always null
    bigquery.SchemaField("Product_Group_Code",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("Cross_Reference_No_",        "STRING",    mode="NULLABLE"),
]


def _bq() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=config.BIGQUERY_PROJECT_ID)
    return _client


def _dataset_id(company: str) -> str | None:
    """Return the BigQuery dataset ID for a given company code, or None if unmapped."""
    return _DATASET_MAP.get(company)


def _table_id(company: str) -> str:
    return f"{config.BIGQUERY_PROJECT_ID}.{_dataset_id(company)}.{_TABLE_NAME}"


def ensure_table(company: str) -> list[str]:
    """Create the ILE BigQuery table for the given company if it does not already exist.

    If the table already exists, any columns present in _SCHEMA but missing from the
    current table schema are added via update_table (e.g. new Airbyte compat columns).

    Returns the names of columns added to an existing table (empty list when the table
    was just created or already had all columns).
    """
    client = _bq()
    tid = _table_id(company)
    from google.api_core.exceptions import NotFound
    try:
        table = client.get_table(tid)
        existing_names = {f.name for f in table.schema}
        new_fields = [f for f in _SCHEMA if f.name not in existing_names]
        if new_fields:
            table.schema = list(table.schema) + new_fields
            client.update_table(table, ["schema"])
            added = [f.name for f in new_fields]
            logger.info(f"[{company}] Added {len(added)} column(s) to {tid}: {added}")
            return added
        return []
    except NotFound:
        pass  # Table does not exist yet — fall through to create it
    table = bigquery.Table(tid, schema=_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="postingDate",
    )
    table.clustering_fields = ["company", "entryType", "locationCode"]
    client.create_table(table, exists_ok=True)
    logger.info(f"Created BigQuery table {tid}")
    return []


def get_max_last_modified(company: str) -> str | None:
    """Return the date (YYYY-MM-DD) of the most recent lastModifiedDateTime in the BQ table
    for the given company, or None if the table is empty, doesn't exist, or is unmapped.

    Used by the sync worker to determine the incremental fetch window from BC.
    """
    if not config.BIGQUERY_PROJECT_ID or not _dataset_id(company):
        return None
    try:
        tid = _table_id(company)
        query = (
            f"SELECT MAX(lastModifiedDateTime) AS max_ts "
            f"FROM `{tid}` "
            f"WHERE company = @company"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("company", "STRING", company)]
        )
        rows = list(_bq().query(query, job_config=job_config).result())
        if rows and rows[0].max_ts is not None:
            # max_ts is a datetime object; return just the date portion for BC's modifiedFrom filter
            return rows[0].max_ts.date().isoformat()
        return None
    except Exception as exc:
        logger.warning(f"[{company}] could not query BQ max lastModifiedDateTime: {exc}")
        return None


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
    airbyte_id = str(uuid.uuid4())
    row["_airbyte_raw_id"] = airbyte_id
    row["_airbyte_extracted_at"] = bq_synced_at
    row["_airbyte_meta"] = {"changes": [], "sync_id": 0}
    row["_airbyte_generation_id"] = 0
    # Legacy Airbyte / CDC columns — always null for full-refresh BC syncs
    row["ab_id"] = airbyte_id
    row["_ab_cdc_lsn"] = None
    row["_ab_cdc_cursor"] = None
    row["_ab_cdc_deleted_at"] = None
    row["_ab_cdc_updated_at"] = None
    row["_ab_cdc_event_serial_no"] = None
    # BC ILE (Table 32) fields not exposed by AL page — always null
    row["Product_Group_Code"] = None
    row["Cross_Reference_No_"] = None
    return row


def _merge_sql(
    target: str,
    staging: str,
    staging_schema: list[bigquery.SchemaField],
    target_schema: list[bigquery.SchemaField],
) -> str:
    """Return the MERGE DML that upserts staging rows into the target table.

    Adds explicit CASTs when a staging column's type differs from the target's
    (e.g. DATE→STRING for legacy tables created before the DATE schema was adopted).
    """
    target_types = {f.name: f.field_type for f in target_schema}
    staging_type_map = {f.name: f.field_type for f in staging_schema}

    def src_expr(field: str) -> str:
        t_type = target_types.get(field)
        s_type = staging_type_map.get(field)
        if t_type and s_type and t_type != s_type:
            return f"CAST(S.`{field}` AS {t_type})"
        return f"S.`{field}`"

    all_fields = [f.name for f in staging_schema]
    update_fields = [f for f in all_fields if f != "_airbyte_raw_id"]
    set_clause = ",\n        ".join(f"T.`{f}` = {src_expr(f)}" for f in update_fields)
    col_list = ", ".join(f"`{f}`" for f in all_fields)
    val_list = ", ".join(src_expr(f) for f in all_fields)
    return f"""
MERGE `{target}` AS T
USING `{staging}` AS S
ON T.company = S.company AND T.entryNo = S.entryNo
WHEN MATCHED THEN UPDATE SET
        {set_clause}
WHEN NOT MATCHED THEN INSERT ({col_list})
VALUES ({val_list})
"""


def upsert_ile_to_bigquery(records: list[dict], company: str) -> int:
    """Upsert ILE records into BigQuery via load-to-staging + MERGE.

    Keyed on (company, entryNo). Existing rows are updated; new rows are
    inserted. The staging table is deleted in a finally block regardless of
    outcome. Returns the count of source rows processed (not BQ merge rows).
    """
    if not records:
        return 0
    if not config.BIGQUERY_PROJECT_ID:
        logger.warning("BIGQUERY_PROJECT_ID not configured — skipping BQ write")
        return 0
    dataset = _dataset_id(company)
    if not dataset:
        logger.warning(f"[{company}] no BigQuery dataset mapped — skipping BQ write")
        return 0

    client = _bq()
    tid = _table_id(company)
    bq_synced_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    rows = [_clean(r, bq_synced_at) for r in records if r.get("entryNo") is not None]
    if not rows:
        logger.warning(f"[{company}] all records missing entryNo — skipping BQ write")
        return 0

    stg_id = f"{config.BIGQUERY_PROJECT_ID}.{dataset}.itemLedgerEntries_stg_{uuid.uuid4().hex[:12]}"
    stg_ref = bigquery.TableReference.from_string(stg_id)

    try:
        job_config = bigquery.LoadJobConfig(
            schema=_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = client.load_table_from_json(rows, stg_ref, job_config=job_config)
        load_job.result()
        if load_job.errors:
            logger.error(f"[{company}] staging load errors: {load_job.errors[:3]}")
            return 0

        target_table = client.get_table(tid)
        sql = _merge_sql(tid, stg_id, _SCHEMA, list(target_table.schema))
        merge_job = client.query(sql)
        merge_job.result()
        if merge_job.errors:
            logger.error(f"[{company}] MERGE errors: {merge_job.errors[:3]}")
            return 0

        logger.info(f"[{company}] MERGE complete — {len(rows)} source rows processed into {tid}")
        return len(rows)
    finally:
        try:
            client.delete_table(stg_ref, not_found_ok=True)
        except Exception as exc:
            logger.warning(f"[{company}] could not delete staging table {stg_id}: {exc}")

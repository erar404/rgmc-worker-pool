"""BigQuery to MSSQL Bridge — ported from rgmc-gcp-api's src/routers/bigquery_bridge.py
for src/workers/bigquery_bridge_worker.py.

This used to run inline inside rgmc-gcp-api's own HTTP routes (/handoff/runbridge/,
/customerpoul/runbridge/[/onlinesalespo/], /customerra/runbridge/), tying up a Cloud
Run request/instance for however long the BigQuery read + MSSQL write + SO-import
publish took. Those routes now just publish a {"group_code", "method"} message (see
rgmc-gcp-api's src/services/bridge_trigger.py) and this worker does the real work,
pulled one at a time (flow_control=FlowControl(max_messages=1) in
bigquery_bridge_worker.py) instead of racing on MSSQL under concurrent HTTP requests.

Kept logic-identical to the source other than:
  - config.* names uppercased to match this repo's convention (BIGQUERY_PROJECT_ID etc).
  - send_mail.send_mail(..., category=) calls replaced with this repo's own
    notify_error/notify_success (src/services/send_mail.py) — same two outcomes
    (error vs. everything-else), different email-sending API.
  - DbConn() takes no db_name — this worker only ever touches MSSQL_DATABASE
    (sbic_prod), unlike rgmc-gcp-api's DbConn, which serves several internal databases.
  - insert_selected_records (the BigQuery Lookup tab's manual, caller-scoped insert)
    is NOT ported — that one stays inline in rgmc-gcp-api, since it's a small,
    explicitly-picked set of rows, not the heavy "everything new since the last run"
    pull this module exists to get off that service's request path.

If rgmc-gcp-api's own copy of this file changes (new column, new group_code, a bug
fix), mirror the change here too — these are two independent copies, not a shared
package, same as other business logic already duplicated across these repos (e.g.
the SUNCOAST/SBIC company-keyword map in CustomerPOUL.py and so_import_worker.py).
"""
import json
import math
import re
import time
from datetime import timedelta, datetime

import pandas
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import src.mappings_bridge as mappings
from src import config
from src.db.dbconn import DbConn
from src.services.bq_read import read_gbq_with_timeout
from src.services.send_mail import notify_error, notify_success


class BigqueryBridge(object):
    def __init__(self, logger, method="manual", group_code='customerpoul'):
        self.__dbconn = DbConn(logger)
        self.__mssql_engine = self.__dbconn.main()
        self.__logger = logger
        self.__last_run_timestamp = None
        self.__last_run_timestamp_detail = None
        self.__table_version = config.BQ_TABLE_VERSION
        self.__method = method
        self.__log_body = "BigQuery Bridge Execution Log ({} Run): {}".format(self.__method, datetime.now().isoformat())
        self.__group_code = group_code
        self.__dup_header_keys: list[str] = []
        self.__dup_detail_keys: list[str] = []
        self.__dataset_id = "{}.{}".format(config.BIGQUERY_PROJECT_ID,
            mappings.bigquery_dataset_mappings[self.__table_version].get(group_code, 'sbic_int'))

    def __log(self, message, level="info"):
        if level == "info":
            self.__logger.info(message)
        elif level == "error":
            self.__logger.error(message)
        elif level == "warning":
            self.__logger.warning(message)
        self.__log_body += "\n{}: {}".format(level.upper(), message)

    def __log_section(self, content: str) -> None:
        """Append a pre-formatted block verbatim to both Cloud Run logs and the email body."""
        self.__logger.info(content)
        self.__log_body += f"\n{content}"

    def __notify(self, category: str) -> None:
        title = f"BigQuery Bridge {'Error' if category == 'ERROR' else 'Complete'} — {self.__group_code}"
        context = f"method={self.__method}"
        if category == "ERROR":
            notify_error(module="bigquery_bridge", title=title, detail=self.__log_body, context=context)
        else:
            notify_success(module="bigquery_bridge", title=title, detail=self.__log_body, context=context)

    def __get_last_run_timestamp(self):
        """Get the last run timestamp from MSSQL table."""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                with self.__mssql_engine.connect() as connection:
                    if self.__group_code == 'customerpoul':
                        result = connection.execute(
                            text("SELECT MAX(createdAt) FROM CustomerPOULBQ")
                        )
                        self.__last_run_timestamp = result.scalar()
                        if self.__last_run_timestamp:
                            self.__last_run_timestamp = self.__last_run_timestamp + timedelta(seconds=1)  # Add 1 second to avoid fetching the last record again
                        self.__log(f"Last run timestamp (in UTC): {self.__last_run_timestamp}", level="info")

                        result = connection.execute(
                            text("SELECT MAX(createdAt) FROM CustomerPOULDetailBQ")
                        )
                        self.__last_run_timestamp_detail = result.scalar()
                        if self.__last_run_timestamp_detail:
                            self.__last_run_timestamp_detail = self.__last_run_timestamp_detail + timedelta(seconds=1)
                    elif self.__group_code == 'customerra':
                        result = connection.execute(
                            text("SELECT MAX(createdAt) FROM CustomerRABQ")
                        )
                        self.__last_run_timestamp = result.scalar()

                        if self.__last_run_timestamp:
                            self.__last_run_timestamp = self.__last_run_timestamp + timedelta(seconds=1)
                        self.__log(f"Last run timestamp for CustomerRA (in UTC): {self.__last_run_timestamp}", level="info")

                        result = connection.execute(
                            text("SELECT MAX(createdAt) FROM customerRADetailBQ")
                        )
                        self.__last_run_timestamp_detail = result.scalar()

                        if self.__last_run_timestamp_detail:
                            self.__last_run_timestamp_detail = self.__last_run_timestamp_detail + timedelta(seconds=1)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    self.__log(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...", level="warning")
                    time.sleep(retry_delay)
                    self.__mssql_engine = self.__dbconn.get_mssql_engine()
                else:
                    self.__log(f"Error fetching last run timestamp: {e}", level="error")
                    self.__notify("ERROR")
                    return {"status": "error", "message": str(e)}

    def __get_bigquery_data(self, table_name):
        """Get data from BigQuery based on the last run timestamp."""
        try:
            if table_name == 'DocumentAIBQ':
                query = "SELECT * FROM `{}.int_document_ai`".format(config.BIGQUERY_DATASET_ID)

                if self.__last_run_timestamp:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp}'"

                df = read_gbq_with_timeout(
                    query,
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dialect='standard',
                )
                self.__log(f"Fetched {len(df)} records from BigQuery.", level="info")
                return df
            elif table_name == 'DocumentAIDetailBQ':
                query = "SELECT * FROM `{}.int_document_ai_detail`".format(config.BIGQUERY_DATASET_ID)

                if self.__last_run_timestamp_detail:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp_detail}'"

                df = read_gbq_with_timeout(
                    query,
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dialect='standard',
                )
                self.__log(f"Fetched {len(df)} records from BigQuery.", level="info")
                return df

            elif table_name == 'document_ai_ra':
                query = "SELECT * FROM `{}.marts_document_ai_ra`".format(self.__dataset_id)

                if self.__last_run_timestamp:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp}'"

                df = read_gbq_with_timeout(
                    query,
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dialect='standard',
                )
                self.__log(f"Fetched {len(df)} records from BigQuery.", level="info")
                return df
            elif table_name == 'document_ai_ra_detail':
                query = "SELECT * FROM `{}.marts_document_ai_ra_detail`".format(self.__dataset_id)

                if self.__last_run_timestamp_detail:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp_detail}'"

                df = read_gbq_with_timeout(
                    query,
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dialect='standard',
                )
                self.__log(f"Fetched {len(df)} records from BigQuery.", level="info")
                return df
            elif table_name == 'int_online_sales_data':
                query = "SELECT * FROM `{}.int_online_sales_data`".format(self.__dataset_id)

                if self.__last_run_timestamp:
                    query += f" WHERE createdAt  > '{self.__last_run_timestamp}'"

                df = read_gbq_with_timeout(
                    query,
                    project_id=config.BIGQUERY_PROJECT_ID,
                    dialect='standard',
                )
                self.__log(f"Fetched {len(df)} records from BigQuery.", level="info")
                return df
        except Exception as e:
            self.__log(f"Error fetching data from BigQuery: {e}", level="error")
            self.__notify("ERROR")
            return {"status": "error", "message": str(e)}

    def __rename_columns(self, table_name, df):
        """Rename columns based on the mapping."""
        table_name = table_name.lower()
        column_mapping = mappings.table_mappings[self.__table_version].get(table_name, {})
        ignore_cols = mappings.ignore_columns.get(self.__table_version, {}).get(table_name, [])
        column_defaults = mappings.column_defaults.get(self.__table_version, {}).get(table_name, {})
        requirements_cols = mappings.required_columns.get(self.__table_version, {}).get(table_name, [])
        date_cols = mappings.date_columns.get(self.__table_version, {}).get(table_name, [])
        money_cols = mappings.money_columns.get(self.__table_version, {}).get(table_name, [])

        if not column_mapping:
            self.__log(f"No column mapping found for table: {table_name}", level="warning")
            return df

        if ignore_cols:
            df.drop(columns=ignore_cols, inplace=True)
            self.__log(f"Removed Columns for {table_name}", level="info")

        if requirements_cols:
            len_before = len(df)
            self.__log('Removing Duplicates for table: {}'.format(table_name), level="info")
            df.drop_duplicates(requirements_cols, keep='first', inplace=True)
            self.__log('Removed {} duplicate rows for table: {}'.format(len_before - len(df), table_name), level="info")

        for key, val in column_defaults.items():
            df[key] = df[key].mask(df[key].isnull(), val)

        df['customer_name'] = df['customer_name'].astype(str).str.encode('utf-8')

        for money_col in money_cols:
            if money_col in df.columns:
                df[money_col] = pandas.to_numeric(df[money_col], errors='coerce').fillna(0)

        for date_col in date_cols:
            if date_col in df.columns:
                df[date_col] = pandas.to_datetime(df[date_col],
                                                  errors="coerce",
                                                  format='mixed',
                                                  yearfirst=True,
                                                  dayfirst=True,
                                                  utc=True)
                df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")

        return df.rename(columns=column_mapping)

    def __trigger_so_import(self, header_table: pandas.DataFrame, detail_table: pandas.DataFrame):
        """Publish one Pub/Sub message per POUL order whose companyName contains SUNCOAST or
        SBIC — the only companies this pipeline is allowed to insert into Business Central.
        Any other company (e.g. Manila Taste Curators / MTC, which has no BC company code
        configured) is deliberately excluded here, not just left unmapped downstream.

        Also holds back (does not publish) any SUNCOAST/SBIC order whose poRefNumber has
        zero matching rows in this run's detail_table — see rgmc-gcp-api's handoff.md for
        the "Held back (0 line items extracted yet)" incident this guards against,
        including the automated retry (customerpoul-backfill-held-back Cloud Scheduler
        job) that now picks these up once their lines actually land in BigQuery.
        """
        sep = "-" * 55

        try:
            from google.cloud import pubsub_v1
        except ImportError:
            self.__log_section(
                f"\n{sep}\nSO IMPORT TRIGGER\n{sep}\n"
                f"  Status : SKIPPED — google-cloud-pubsub not installed\n{sep}"
            )
            return

        topic = config.PUBSUB_POUL_SO_TOPIC
        if not topic:
            self.__log_section(
                f"\n{sep}\nSO IMPORT TRIGGER\n{sep}\n"
                f"  Status : SKIPPED — PUBSUB_POUL_SO_TOPIC not configured\n{sep}"
            )
            return

        company_upper = header_table['companyName'].astype(str).str.upper()
        mask = company_upper.str.contains('SUNCOAST|SBIC', na=False)
        filtered = header_table[mask]
        excluded = header_table[~mask]

        if filtered.empty:
            excluded_summary = (
                ", ".join(sorted(excluded['companyName'].astype(str).unique()))
                if not excluded.empty else "(none)"
            )
            self.__log_section(
                f"\n{sep}\nSO IMPORT TRIGGER\n{sep}\n"
                f"  Status   : SKIPPED — no SUNCOAST / SBIC orders in batch\n"
                f"  Excluded : {len(excluded)} order(s) from non-SBIC compan(ies): {excluded_summary}\n{sep}"
            )
            return

        def _serializable(val):
            if isinstance(val, bytes):
                return val.decode('utf-8', errors='replace')
            if isinstance(val, float) and math.isnan(val):
                return None
            if hasattr(val, 'isoformat'):
                return val.isoformat()
            return val

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.BIGQUERY_PROJECT_ID, topic)

        orders = []
        published_refs: list[tuple[str, str]] = []
        held_back_refs: list[tuple[str, str]] = []
        for _, hrow in filtered.iterrows():
            header_dict = {k: _serializable(v) for k, v in hrow.to_dict().items()}
            po_ref = str(hrow.get('poRefNumber', '(unknown)'))
            company_name = str(hrow.get('companyName', ''))

            detail_mask = detail_table['poRefNumber'] == hrow.get('poRefNumber', '')

            lines = [
                {k: _serializable(v) for k, v in row.items()}
                for row in detail_table[detail_mask].to_dict(orient='records')
            ]

            if not lines:
                held_back_refs.append((po_ref, company_name))
                continue

            orders.append({"header": header_dict, "lines": lines})
            published_refs.append((po_ref, company_name))

        trigger_lines = [f"\n{sep}", "SO IMPORT TRIGGER", sep]

        if not excluded.empty:
            excluded_summary = ", ".join(sorted(excluded['companyName'].astype(str).unique()))
            trigger_lines.append(f"  Excluded (non-SBIC/Suncoast) : {len(excluded)} order(s) — {excluded_summary}")

        if held_back_refs:
            trigger_lines.append(
                f"  Held back (0 line items extracted yet) : {len(held_back_refs)} order(s) — "
                f"NOT inserted into BC; re-run once CustomerPOULDetail has lines for these refs"
            )
            for po_ref, co in held_back_refs:
                trigger_lines.append(f"    - {po_ref}  [{co}]")

        if not orders:
            trigger_lines.append(f"  Status : SKIPPED — every SBIC/Suncoast order in this batch was held back")
            trigger_lines.append(sep)
            self.__log_section("\n".join(trigger_lines))
            return

        # Publish ONE batch message containing all orders
        payload = {"type": "poul-so-import-batch", "orders": orders}
        publisher.publish(topic_path, json.dumps(payload).encode('utf-8'))

        trigger_lines += [
            f"  Status : TRIGGERED",
            f"  Topic  : {topic}",
            f"  Sent   : 1 batch message ({len(published_refs)} order(s))",
        ]
        for po_ref, co in published_refs:
            trigger_lines.append(f"    - {po_ref}  [{co}]")
        trigger_lines.append(sep)
        self.__log_section("\n".join(trigger_lines))

    def __extract_duplicate_key(self, error_message: str):
        """
        Extracts the duplicate key value from a SQL IntegrityError message.

        Returns:
            tuple: (key1, key2, ...) if found
            None: if no duplicate key found
        """
        pattern = r"The duplicate key value is \((.*?)\)"
        match = re.search(pattern, error_message)

        if not match:
            return None

        values = [v.strip() for v in match.group(1).split(",")]

        return values

    def __build_summary(
        self,
        header_table: pandas.DataFrame,
        detail_table: pandas.DataFrame,
        main_key: str,
        header_tbl_name: str,
        detail_tbl_name: str,
    ) -> str:
        sep = "-" * 55
        lines = [f"\n{sep}", "INSERT SUMMARY", sep]

        h_keys = (
            header_table[main_key].astype(str).tolist()
            if main_key in header_table.columns else []
        )
        lines.append(f"  Inserted  {header_tbl_name:<30}: {len(h_keys)}")
        for k in h_keys:
            lines.append(f"            - {k}")

        d_unique = (
            detail_table[main_key].astype(str).unique().tolist()
            if main_key in detail_table.columns else []
        )
        lines.append(f"  Inserted  {detail_tbl_name:<30}: {len(detail_table)}  ({len(d_unique)} unique PO refs)")

        lines += [f"\n{sep}", "DUPLICATES SKIPPED", sep]
        lines.append(f"  {header_tbl_name:<40}: {len(self.__dup_header_keys)}")
        for k in self.__dup_header_keys:
            lines.append(f"    - {k}")
        lines.append(f"  {detail_tbl_name:<40}: {len(self.__dup_detail_keys)}")
        for k in self.__dup_detail_keys:
            lines.append(f"    - {k}")

        lines.append(sep)
        return "\n".join(lines)

    def __format_onlinesalespo_data(self, df):
        """Format Online Sales PO data to match MSSQL schema."""
        detail_dict = df.to_dict(orient='records')
        porefnumber_list = []
        header_list = []
        detail_list = []
        lineindex = 0

        for record in detail_dict:
            porefnumber = record.get('poRefNumber')
            if porefnumber and porefnumber not in porefnumber_list:
                temp_header_dict = {
                    'po_ref_number': record.get('poRefNumber'),
                    'customer_name': record.get('customerName'),
                    'customer_branch_name': record.get('customerBranchName', ''),
                    'company_name': record.get('companyName', ''),
                    'created_at': record.get('createdAt', '{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])),
                    'po_date': record.get('poDate', '1900-01-01'),
                    'delivery_date': record.get('deliveryDate', '1900-01-01'),
                    'cancellation_date': record.get('cancellationDate', '1900-01-01'),
                    'total_gross_amount': record.get('totalGrossAmount', 0),
                    'total_discount': record.get('totalDiscount', 0),
                    'total_discount_percent': record.get('totalDiscountPercent', 0),
                    'file_name': record.get('fileName', ''),
                    'total_net_amount': record.get('totalNetAmount', 0),
                    'total_quantity': record.get('totalQuantity', 0),
                    'remark': record.get('remark'),
                    'poStatus': record.get('poStatus', 'NEW'),
                    'poRefNumberPrimary': record.get('poRefNumberPrimary', ''),
                    'poRefNumberCount': record.get('poRefNumberCount', ''),
                    'customerSKUCode': '',
                    'customerSKUDesc': '',
                    'poQty': record.get('poQty', 0),
                    'poQtyPcs': record.get('poQtyPcs', 0),
                    'unitPrice': record.get('unitPrice', 0),
                    'unitPricePcs': record.get('unitPricePcs', 0),
                    'netPrice': record.get('netPrice', 0),
                }
                porefnumber_list.append(porefnumber)
                header_list.append(temp_header_dict)
                lineindex = 0

            temp_detail_dict = {
                'po_ref_number': record.get('poRefNumber'),
                'customer_name': record.get('customerName'),
                'customer_branch_name': record.get('customerBranchName', ''),
                'customer_sku_code': record.get('customerSKUCode'),
                'customer_sku_desc': record.get('customerSKUDesc'),
                'created_at': record.get('createdAt', '{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])),
                'po_date': record.get('poDate', '1900-01-01'),
                'delivery_date': record.get('deliveryDate', '1900-01-01'),
                'po_qty': record.get('poQty', 0),
                'unit_price': record.get('unitPrice', 0),
                'unit_amount': record.get('unitAmount', 0),
                'net_price': record.get('netPrice', 0),
                'line_index': lineindex,
                'unit_price_pcs': record.get('unitPricePcs', 0),
                'net_price_pcs': record.get('netPricePcs', 0),
                'po_qty_pcs': record.get('poQtyPcs', 0),
                'unit_of_measurement': record.get('unitOfMeasurement', ''),
                'discount_percent': record.get('discountPercent', 0),
                'file_name': record.get('fileName', ''),
                'page_index': record.get('pageIndex', 0),
                'netAmount': record.get('netAmount', 0),
            }
            detail_list.append(temp_detail_dict)
            lineindex += 1

        header_df = pandas.DataFrame(header_list)
        detail_df = pandas.DataFrame(detail_list)

        return header_df, detail_df

    def main(self):
        """Bigquery Bridge Main Function."""
        self.__log("BigQuery to MSSQL Bridge initialized.", level="info")
        self.__get_last_run_timestamp()
        self.__log(f"Last run timestamp obtained: {self.__last_run_timestamp}", level="info")

        self.__log("Fetching data from BigQuery...", level="info")
        variable_mapping = mappings.variable_mappings[self.__table_version].get(self.__group_code, {})
        bq_header_table_name = variable_mapping.get('bq_header_name', 'DocumentAIBQ')
        bq_detail_table_name = variable_mapping.get('bq_detail_name', 'DocumentAIDetailBQ')

        mssql_header_table_name = variable_mapping.get('mssql_header_name', 'CustomerPOULBQ')
        mssql_detail_table_name = variable_mapping.get('mssql_detail_name', 'CustomerPOULDetailBQ')
        main_key = variable_mapping.get('main_key', 'poRefNumber')

        if self.__group_code in ('customerpoul', 'customerra'):
            header_table = self.__get_bigquery_data(bq_header_table_name)
            detail_table = self.__get_bigquery_data(bq_detail_table_name)

        elif self.__group_code == 'onlinesalespo':
            self.__log("Processing data from online sales bigquery table...", level="info")
            bq_table = self.__get_bigquery_data('int_online_sales_data')
            header_table, detail_table = self.__format_onlinesalespo_data(bq_table)

        header_empty = not isinstance(header_table, pandas.DataFrame) or header_table.empty
        detail_empty = not isinstance(detail_table, pandas.DataFrame) or detail_table.empty
        if header_empty and detail_empty:
            self.__log("No data fetched from BigQuery. Exiting process.", level="info")
            return {"status": "success", "message": "No new data to process."}

        self.__log("Renaming columns to match MSSQL schema...", level="info")
        header_table = self.__rename_columns(mssql_header_table_name.lower(), header_table)
        detail_table = self.__rename_columns(mssql_detail_table_name.lower(), detail_table)

        return self.__insert_dataframes(
            header_table, detail_table, mssql_header_table_name, mssql_detail_table_name,
            main_key, trigger_so_import=(self.__group_code == 'customerpoul'),
        )

    def __insert_dataframes(self, header_table, detail_table, mssql_header_table_name,
                             mssql_detail_table_name, main_key, trigger_so_import=True):
        """Insert header_table/detail_table into their MSSQL staging tables — detail
        FIRST, header SECOND (an AFTER INSERT trigger on the header table promotes
        matching rows into CustomerPOUL/CustomerPOULDetail, so the detail rows it joins
        against must already exist by the time the header insert fires it). Retries
        once per duplicate-key IntegrityError, dropping just the offending row and
        trying again, same as main()'s original inline loop.
        """
        continue_execution = True
        while continue_execution:
            try:
                bq_inserted = False
                self.__log("Attempting to insert data into MSSQL. Total Records - {}: {}, {}: {}".format(mssql_header_table_name,
                                                                                                         len(header_table),
                                                                                                         mssql_detail_table_name,
                                                                                                         len(detail_table)),
                                                                                                         level="info")
                with self.__mssql_engine.connect() as connection:
                    uldetail_bq = detail_table.to_sql(
                        name=mssql_detail_table_name,
                        con=connection,
                        if_exists='append',
                        chunksize=50,
                        index=False
                    )
                    bq_inserted = True
                    ul_bq = header_table.to_sql(
                        name=mssql_header_table_name,
                        con=connection,
                        if_exists='append',
                        schema='dbo',
                        index=False,
                        chunksize=50,
                        method='multi'
                    )
                continue_execution = False
                self.__log("Data inserted successfully into MSSQL.", level="info")
                if trigger_so_import:
                    self.__trigger_so_import(header_table, detail_table)
                self.__log("Inserted Records - {}: {}, {}: {}".format(mssql_header_table_name,
                                                                      ul_bq,
                                                                      mssql_detail_table_name,
                                                                      uldetail_bq),
                                                                      level="info")
            except IntegrityError as ie:
                duplicate_keys = self.__extract_duplicate_key(str(ie))
                if duplicate_keys:
                    if not bq_inserted:
                        requirements_cols = mappings.required_columns[self.__table_version].get(mssql_detail_table_name.lower(), [])
                        self.__log(f"Duplicate entries found in {mssql_detail_table_name} ({requirements_cols}): {duplicate_keys}. Skipping insertion for these records.", level="warning")
                        self.__dup_detail_keys.append(str(duplicate_keys[0]))
                        detail_table = detail_table[detail_table[main_key] != duplicate_keys[0]]
                    else:
                        requirements_cols = mappings.required_columns[self.__table_version].get(mssql_header_table_name.lower(), [])
                        self.__log(f"Duplicate entries found in {mssql_header_table_name} ({requirements_cols}): {duplicate_keys}. Skipping insertion for these records.", level="warning")
                        self.__dup_header_keys.append(str(duplicate_keys[0]))
                        header_table = header_table[header_table[main_key] != duplicate_keys[0]]
                else:
                    self.__log(f"IntegrityError encountered: {ie}", level="error")
                    self.__notify("ERROR")
                    return {"status": "error", "message": str(ie)}
            except Exception as e:
                self.__log(f"Error inserting data into MSSQL: {e}", level="error")
                self.__notify("ERROR")
                return {"status": "error", "message": str(e)}

        self.__log_section(self.__build_summary(
            header_table, detail_table, main_key,
            mssql_header_table_name, mssql_detail_table_name,
        ))

        total_inserted = len(header_table) + len(detail_table)
        if total_inserted > 0:
            self.__notify("INFO")
        else:
            self.__log("No new entries inserted into target tables. Skipping email notification.", level="info")
        return {
            "status": "success",
            "message": "Data transfer from BigQuery to MSSQL completed successfully.",
            "details": {"header_records": len(header_table),
                        "detail_records": len(detail_table)}}

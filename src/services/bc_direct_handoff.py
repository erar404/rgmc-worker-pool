"""Direct BigQuery -> CustomerPOUL/CustomerPOULDetail -> BC handoff, independent of
CustomerPOULBQ and its tr_CustomerPOBQ trigger.

Why this exists: tr_CustomerPOBQ (the AFTER INSERT trigger on CustomerPOULBQ that
promotes staged rows into CustomerPOUL/CustomerPOULDetail) re-scans the ENTIRE
poStatus='NEW' backlog on every firing, not just the rows the triggering INSERT just
added. Confirmed live (2026-11-12): a single bad row anywhere in that backlog can roll
back the whole firing statement, silently discarding brand-new header rows for
unrelated POs with zero error anywhere in this app's own logs. Two full upload batches
(125 POs, then another 72) were lost to this before it was caught, both times requiring
a manual SQL rescue.

run_direct_bc_handoff is that rescue, productized: called from bigquery_bridge.py's own
main() with the SAME header_table/detail_table it already fetched from BigQuery and
renamed to MSSQL column names -- right after that cleanup, before __insert_dataframes
hands them to CustomerPOULBQ (and therefore before tr_CustomerPOBQ can ever fire on
them). It resolves the exact same reference lookups
(Customer/CustomerBranch/CustomerBranchWarehouse/CustomerSKU/SKU) tr_CustomerPOBQ itself
computes, and inserts straight into CustomerPOUL/CustomerPOULDetail. CustomerPOULBQ is
never touched by this path, so the trigger never fires and can never roll this back.

This runs as one more step inside the EXISTING BigQuery-bridge Pub/Sub flow (same
PUBSUB_BRIDGE_SUBSCRIPTION / rgmc-bridge-run-sub topic, same BigqueryBridge.main() call,
same worker) -- not a new topic, subscription, or worker. A failure here is caught and
logged by the caller; it must never block __insert_dataframes's own (separate, already
working for most POs) CustomerPOULBQ path from still running.

Deliberately mirrors tr_CustomerPOBQ's own resolution rules exactly (same lookup
priority: numeric vs non-numeric branch code, Puregold's SKU-code special case, the
fixed-branch overrides for Landmark/Lee Plaza/Alturas/online marketplaces) -- see
sql/fix_tr_customerpobq_trigger.sql for the trigger's own (not yet deployed) version of
the same logic, kept in sync by hand since neither side is a shared library.

Which companies this promotes AND which of those are actually sent to BC is entirely
config.BC_HANDOFF_COMPANY_MAP -- see that mapping's own comment. A company absent from
the map resolves to companyId=-1 (same sentinel tr_CustomerPOBQ itself defaults to) and
is never sent to BC, same as an unrecognized company always was.
"""
import logging
from datetime import date, timedelta

import pandas
from sqlalchemy import text

from src import config

logger = logging.getLogger("worker.bigquery_bridge")

_CREATE_BY = "bc_direct_handoff"
_NULL_DATE = date(1900, 1, 1)
_NULL_DATE_ALT = date(1900, 1, 2)  # BigQuery has served both sentinels historically
_PUREGOLD_CUSTOMER_ID = 670

# Same fixed branch overrides tr_CustomerPOBQ hardcodes -- keep these two lists in sync.
_FIXED_BRANCH_BY_CUSTOMER_ID = {
    16: 547,     # Landmark Trinoma
    540: 2449,   # Landmark Alabang
    541: 2450,   # Landmark Nuvali
    981: 3985,   # Landmark Manila Bay
    130: 389,    # Lee Plaza Super Plaza
    132: 391,    # Lee Plaza City Central
    211: 772,    # Lee Plaza Hypermart
    792: 3437,   # Alturas / Plaza Marcela
    73: 186,     # Shoppers Mart
    224: 1911,   # Lazada
    841: 3570,   # Shopee
    1025: 4129,  # TikTok
}


def _resolve_company(company_name: str) -> tuple[int, bool]:
    """(companyId, bc_valid) for one header's companyName, via
    config.BC_HANDOFF_COMPANY_MAP's substring-keyword match. No match -> (-1, False),
    same "unresolved" sentinel tr_CustomerPOBQ itself defaults companyId to."""
    company_upper = (company_name or "").upper()
    for keyword, entry in config.BC_HANDOFF_COMPANY_MAP.items():
        if keyword in company_upper:
            return entry["company_id"], entry["bc_valid"]
    return -1, False


def is_bc_valid_company(company_name: str) -> bool:
    """True when company_name resolves (via config.BC_HANDOFF_COMPANY_MAP) to a
    company this module already sends to BC directly. Exported for
    bigquery_bridge.py's own main() to exclude these rows from the CustomerPOULBQ/
    tr_CustomerPOBQ path entirely -- once run_direct_bc_handoff has fully handled a
    company (promoted + sent to BC), there's no reason for the old, trigger-dependent
    path to touch the same rows too, and keeping them out shrinks that trigger's own
    table-wide rescan to just the companies (e.g. MTC) that still rely on it."""
    return _resolve_company(company_name)[1]


def _resolve_null_date(raw_date) -> date:
    """Same substitution tr_CustomerPOBQ's own poDate CASE performs: the
    1900-01-01/1900-01-02 sentinel (BigQuery's "no date given" convention) becomes
    today's date, never a literal date in 1900 -- a literal 1900 date was exactly what
    broke Business Central's Sales Order number-series lookup for 3 POs in the first
    manual rescue of this bug."""
    if not raw_date:
        return date.today()
    try:
        parsed = date.fromisoformat(str(raw_date)[:10])
    except ValueError:
        return date.today()
    if parsed in (_NULL_DATE, _NULL_DATE_ALT):
        return date.today()
    return parsed


def _existing_customer_po_refs(connection, po_refs: list[str]) -> set[tuple[int, str]]:
    """(customerId, poRefNumber) pairs already in CustomerPOUL, regardless of which
    path put them there (this one, or the trigger) -- the dedup check, so this running
    on every bridge cycle (or overlapping with the trigger's own path) is always safe."""
    if not po_refs:
        return set()
    placeholders = ", ".join(f":v{i}" for i in range(len(po_refs)))
    params = {f"v{i}": ref for i, ref in enumerate(po_refs)}
    rows = connection.execute(
        text(f"SELECT customerId, poRefNumber FROM CustomerPOUL WHERE poRefNumber IN ({placeholders})"),
        params,
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def _resolve_header(connection, h: dict) -> dict:
    """companyId/customerId/customerBranchId/warehouseId for one header, exactly
    mirroring tr_CustomerPOBQ's own SELECT INTO #tmpPO + the three UPDATE #tmpPO passes
    (customer, branch, warehouse). Unresolved values default to -1, same sentinel the
    trigger itself uses -- never NULL (CustomerPOUL has NOT NULL constraints on these
    columns)."""
    company_id, bc_valid = _resolve_company(h.get("companyName"))

    customer_name = (h.get("customerName") or "").strip().upper()
    branch_name = (h.get("customerBranchName") or "").strip()

    is_numeric_branch = branch_name[:1].isdigit()
    if is_numeric_branch:
        digits = ""
        for ch in branch_name:
            if ch.isdigit():
                digits += ch
            elif digits:
                break
        branch_lookup_code = digits.upper()
    else:
        branch_lookup_code = branch_name.upper()

    customer_row = connection.execute(
        text(
            "SELECT TOP 1 customerId FROM Customer "
            "WHERE REPLACE(:name, ' ', '') LIKE CONCAT('%', REPLACE(lookUpCode, ' ', ''), '%') "
            "AND lookUpCode <> '' AND isActive = 1"
        ),
        {"name": customer_name},
    ).fetchone()
    customer_id = customer_row[0] if customer_row else -1

    branch_id = -1
    if customer_id != -1 and branch_lookup_code:
        if is_numeric_branch:
            branch_row = connection.execute(
                text(
                    "SELECT TOP 1 customerBranchId FROM CustomerBranch "
                    "WHERE customerId = :cid AND lookUpCode = :code AND lookUpCode <> ''"
                ),
                {"cid": customer_id, "code": branch_lookup_code},
            ).fetchone()
        else:
            branch_row = connection.execute(
                text(
                    "SELECT TOP 1 customerBranchId FROM CustomerBranch "
                    "WHERE customerId = :cid AND lookUpCode <> '' "
                    "AND REPLACE(:code, ' ', '') LIKE CONCAT('%', REPLACE(lookUpCode, ' ', ''), '%')"
                ),
                {"cid": customer_id, "code": branch_lookup_code},
            ).fetchone()
        branch_id = branch_row[0] if branch_row else -1
    if customer_id in _FIXED_BRANCH_BY_CUSTOMER_ID:
        branch_id = _FIXED_BRANCH_BY_CUSTOMER_ID[customer_id]

    warehouse_id = -1
    if branch_id != -1:
        wh_row = connection.execute(
            text(
                "SELECT TOP 1 warehouseId FROM CustomerBranchWarehouse "
                "WHERE companyId = :cid AND customerBranchId = :bid AND isPrimary = 1"
            ),
            {"cid": company_id, "bid": branch_id},
        ).fetchone()
        warehouse_id = wh_row[0] if wh_row else -1

    return {
        **h,
        "companyId": company_id,
        "bcValid": bc_valid,
        "customerId": customer_id,
        "customerBranchId": branch_id,
        "warehouseId": warehouse_id,
        "customerBranchLookUpCode": branch_lookup_code,
        "poDate": _resolve_null_date(h.get("poDate")),
        "deliveryDate": _resolve_null_date(h.get("deliveryDate")) if h.get("deliveryDate") else None,
    }


def _resolve_detail_sku(connection, customer_id: int, sku_code: str) -> tuple[int, int]:
    """skuId/productId for one (customerId, customerSKUCode) -- same CustomerSKU -> SKU
    chain tr_CustomerPOBQ's own two UPDATE #tmpPO passes perform, including the
    Puregold-specific zero-padded-code match."""
    sku_code = (sku_code or "").strip().upper()
    if not sku_code:
        return -1, -1
    if customer_id == _PUREGOLD_CUSTOMER_ID:
        row = connection.execute(
            text(
                "SELECT TOP 1 skuId FROM CustomerSKU WHERE customerId = :cid "
                "AND REPLACE(RIGHT(SPACE(20) + :code, 20), ' ', '0') "
                "= REPLACE(RIGHT(SPACE(20) + customerSKUCode, 20), ' ', '0')"
            ),
            {"cid": customer_id, "code": sku_code},
        ).fetchone()
    else:
        row = connection.execute(
            text("SELECT TOP 1 skuId FROM CustomerSKU WHERE customerId = :cid AND customerSKUCode = :code"),
            {"cid": customer_id, "code": sku_code},
        ).fetchone()
    if not row:
        return -1, -1
    sku_id = row[0]
    product_row = connection.execute(
        text("SELECT TOP 1 productId FROM SKU WHERE skuId = :sid"), {"sid": sku_id},
    ).fetchone()
    return sku_id, (product_row[0] if product_row else -1)


def _insert(connection, headers: list[dict], details_by_ref: dict[str, list[dict]]) -> list[str]:
    """Inserts resolved headers/details directly into CustomerPOUL/CustomerPOULDetail --
    CustomerPOULBQ is never touched, so tr_CustomerPOBQ never fires for this data.
    Returns the po_refs actually inserted this run."""
    inserted_refs = []
    for h in headers:
        po_ref = h["poRefNumber"].strip().upper()
        remark = (h.get("remark") or "").strip().upper()
        connection.execute(
            text(
                "INSERT INTO CustomerPOUL("
                "companyId, companyName, warehouseId, customerId, customerName, "
                "poRefNumber, poDate, deliveryDate, cancellationDate, "
                "customerBranchId, customerBranchName, customerBranchLookUpCode, "
                "remark, createBy, updateBy) "
                "VALUES (:companyId, :companyName, :warehouseId, :customerId, :customerName, "
                ":poRefNumber, :poDate, :deliveryDate, :cancellationDate, "
                ":customerBranchId, :customerBranchName, :customerBranchLookUpCode, "
                ":remark, :createBy, :updateBy)"
            ),
            {
                "companyId": h["companyId"],
                "companyName": (h.get("companyName") or "").strip().upper(),
                "warehouseId": h["warehouseId"],
                "customerId": h["customerId"],
                "customerName": (h.get("customerName") or "").strip().upper(),
                "poRefNumber": po_ref,
                "poDate": h["poDate"],
                "deliveryDate": h["deliveryDate"] or h["poDate"],
                "cancellationDate": h["poDate"] + timedelta(days=1),
                "customerBranchId": h["customerBranchId"],
                "customerBranchName": (h.get("customerBranchName") or "").strip().upper(),
                "customerBranchLookUpCode": h["customerBranchLookUpCode"],
                "remark": remark,
                "createBy": _CREATE_BY,
                "updateBy": _CREATE_BY,
            },
        )

        for d in details_by_ref.get(po_ref, []):
            connection.execute(
                text(
                    "INSERT INTO CustomerPOULDetail("
                    "customerId, poRefNumber, productId, skuId, customerSKUCode, customerSKUDesc, "
                    "poQty, unitPrice, discountPercent, netPrice) "
                    "VALUES (:customerId, :poRefNumber, :productId, :skuId, :skuCode, :skuDesc, "
                    ":qty, :price, :discount, :net)"
                ),
                {
                    "customerId": h["customerId"],
                    "poRefNumber": po_ref,
                    "productId": d["productId"],
                    "skuId": d["skuId"],
                    "skuCode": (d.get("customerSKUCode") or "").strip().upper(),
                    "skuDesc": (d.get("customerSKUDesc") or "").strip().upper(),
                    "qty": d["poQty"] or d.get("poQtyPcs") or 0,
                    "price": d["unitPrice"] or d.get("unitPricePcs") or 0,
                    "discount": d.get("discountPercent") or 0,
                    "net": d["netPrice"] or d.get("unitPricePcs") or 0,
                },
            )
            # CustomerPOULDetailBQ too (detail-only, no matching CustomerPOULBQ header
            # row -- tr_CustomerPOBQ fires on CustomerPOULBQ INSERTs only, never on this
            # table, so this can never trigger the promotion logic this module exists to
            # avoid). Required because _run_backfill_from_cloudsql (called below to
            # actually create the BC order) fetches its line items via
            # gcp_api_client.fetch_customerpouldetailbq -- i.e. from THIS table, not
            # CustomerPOULDetail above. Confirmed live 2026-11-13: every BC order this
            # module had sent so far has 0 lines, because this table was never
            # populated. Same raw (unresolved) fields the trigger's own staging insert
            # uses -- already present on d/h since both dataframes are post-
            # __rename_columns by the time bigquery_bridge.py hands them to this module.
            connection.execute(
                text(
                    "INSERT INTO CustomerPOULDetailBQ("
                    "poRefNumber, customerName, customerBranchName, poDate, deliveryDate, "
                    "lineIndex, customerSKUCode, customerSKUDesc, poQty, poQtyPcs, "
                    "unitOfMeasurement, unitPrice, unitPricePcs, unitAmount, netAmount, "
                    "netPrice, netPricePcs, discountPercent, createdAt) "
                    "VALUES (:poRefNumber, :customerName, :customerBranchName, :poDate, :deliveryDate, "
                    ":lineIndex, :skuCode, :skuDesc, :poQty, :poQtyPcs, "
                    ":uom, :unitPrice, :unitPricePcs, :unitAmount, :netAmount, "
                    ":netPrice, :netPricePcs, :discountPercent, :createdAt)"
                ),
                {
                    "poRefNumber": po_ref,
                    "customerName": (h.get("customerName") or "").strip().upper(),
                    "customerBranchName": (h.get("customerBranchName") or "").strip().upper(),
                    "poDate": h["poDate"],
                    "deliveryDate": h["deliveryDate"] or h["poDate"],
                    "lineIndex": d.get("lineIndex") or 0,
                    "skuCode": (d.get("customerSKUCode") or "").strip().upper(),
                    "skuDesc": (d.get("customerSKUDesc") or "").strip().upper(),
                    "poQty": d.get("poQty") or 0,
                    "poQtyPcs": d.get("poQtyPcs") or 0,
                    "uom": d.get("unitOfMeasurement") or "",
                    "unitPrice": d.get("unitPrice") or 0,
                    "unitPricePcs": d.get("unitPricePcs") or 0,
                    "unitAmount": d.get("unitAmount") or 0,
                    "netAmount": d.get("netAmount") or 0,
                    "netPrice": d.get("netPrice") or 0,
                    "netPricePcs": d.get("netPricePcs") or 0,
                    "discountPercent": d.get("discountPercent") or 0,
                    "createdAt": d.get("createdAt") or h["poDate"],
                },
            )
        inserted_refs.append(po_ref)
    return inserted_refs


def run_direct_bc_handoff(engine, header_table: pandas.DataFrame, detail_table: pandas.DataFrame) -> dict:
    """Entry point called from bigquery_bridge.py's main(), right after it fetches and
    renames header_table/detail_table from BigQuery and before __insert_dataframes
    hands them to CustomerPOULBQ. Resolves and inserts directly into
    CustomerPOUL/CustomerPOULDetail for every header whose company resolves via
    config.BC_HANDOFF_COMPANY_MAP (any company, not just bc_valid ones -- see that
    map's own comment), then hands the bc_valid subset to BC via
    so_import_worker._run_backfill_from_cloudsql.

    engine: the SAME SQLAlchemy engine bigquery_bridge.py already holds open
    (self.__mssql_engine) -- no second MSSQL connection is created.

    Returns {"status", "message", "inserted": [po_ref, ...], "sent_to_bc": [po_ref, ...]}.
    Never raises for an expected (empty-batch) case; the caller is responsible for
    catching anything unexpected so this can never block __insert_dataframes's own
    CustomerPOULBQ path.
    """
    if header_table.empty:
        return {"status": "success", "message": "No headers to process.", "inserted": [], "sent_to_bc": []}

    headers = header_table.to_dict("records")
    details_by_ref: dict[str, list[dict]] = {}
    for d in detail_table.to_dict("records"):
        details_by_ref.setdefault(str(d.get("poRefNumber") or "").strip().upper(), []).append(d)

    with engine.begin() as connection:
        po_refs = [str(h.get("poRefNumber") or "").strip().upper() for h in headers]
        headers = [_resolve_header(connection, h) for h in headers]

        existing = _existing_customer_po_refs(connection, po_refs)
        headers = [h for h in headers if (h["customerId"], h["poRefNumber"].strip().upper()) not in existing]
        if not headers:
            return {"status": "success", "message": "Already in CustomerPOUL.", "inserted": [], "sent_to_bc": []}

        for h in headers:
            po_ref = h["poRefNumber"].strip().upper()
            for d in details_by_ref.get(po_ref, []):
                sku_id, product_id = _resolve_detail_sku(connection, h["customerId"], d.get("customerSKUCode"))
                d["skuId"] = sku_id
                d["productId"] = product_id

        inserted_refs = _insert(connection, headers, details_by_ref)

    bc_valid_refs = [
        h["poRefNumber"].strip().upper() for h in headers
        if h["bcValid"] and details_by_ref.get(h["poRefNumber"].strip().upper())
    ]

    sent_to_bc: list[str] = []
    if bc_valid_refs:
        # Imported here, not at module level: so_import_worker.py is a worker module,
        # and this keeps bc_direct_handoff.py importable (e.g. for a unit test) without
        # pulling in that module's own BC-client/Firestore setup unless this path
        # actually has something to send.
        from src.workers.so_import_worker import _run_backfill_from_cloudsql

        _run_backfill_from_cloudsql(["SBIC"], create_by=_CREATE_BY, po_ref_numbers=bc_valid_refs)
        sent_to_bc = bc_valid_refs

    return {
        "status": "success",
        "message": f"Inserted {len(inserted_refs)} PO(s) into CustomerPOUL, sent {len(sent_to_bc)} to BC.",
        "inserted": inserted_refs,
        "sent_to_bc": sent_to_bc,
    }

"""Direct MSSQL (sbic_prod) access for resolving customer/branch identity straight from
BigQuery-bridge raw data (customerName / customerBranchName) — replicates sbic_prod's own
tr_CustomerPOBQ trigger's Customer -> CustomerBranch resolution (see
RGMC_DB/sbic_prod_db/03_Table_Trigger/table_trigger.sql) without depending on CustomerPOUL,
which only carries this same resolution because that trigger populates it after an insert
into CustomerPOULBQ. Used by so_import_worker.py's _run_batch so the main Pub/Sub SO-import
path can resolve a branch's customerBranchCode/lookUpCode directly, instead of a
CustomerPOUL-by-poRefNumber round trip through rgmc-gcp-api just to recover a
customerBranchId that's then immediately thrown away (see _create_order's structured-match
tier, which only ever reads customerBranchCode/lookUpCode, never the numeric ID itself).

Only the customerId -> customerBranchId chain is replicated here — the trigger's other
three joins (CustomerBranchWarehouse -> warehouseId, CustomerSKU -> skuId, SKU ->
productId) are confirmed dead weight for this pipeline (nothing in any of rgmc-gcp-api,
rgmc-worker-pool, rgmc-bc-api, or sbic-manual-trigger-page reads warehouseId/skuId/
productId from a CustomerPOUL(Detail) row) and are deliberately not ported. fetch_customer_skus
below is provided anyway, pre-scoped by customerId, purely so a future feature needing
CustomerSKU data doesn't need its own separate round trip on top of this module's existing
Customer/CustomerBranch fetch — not currently called from anywhere.

Uses the same DbConn (Cloud SQL Python Connector + pytds, wrapped as a SQLAlchemy Core
engine) src/services/bigquery_bridge.py already uses — no ORM; sbic_prod's own tables have
no declarative models anywhere in this ecosystem, and this module's queries are simple
enough that Core's text() is clearer than standing up models for it.
"""
import logging
import re

from sqlalchemy import text

from src.db.dbconn import DbConn

logger = logging.getLogger("sbic_db")

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = DbConn(logger).main()
    return _engine


def _normalize(s: str | None) -> str:
    return (s or "").strip().upper().replace(" ", "")


# The trigger has one more hardcoded special-customer ID beyond the branch overrides
# below: Puregold (customerId 670) gets a zero-padded, right-aligned 20-char SKU-code
# comparison instead of CustomerSKU's normal exact-match ("chamonix" padding — see the
# trigger's SKU section). Noted here for completeness/documentation only — it's a SKU-
# matching rule, not a branch one, and this module doesn't port CustomerSKU resolution
# at all (so_import_worker.py resolves items exclusively via BC's own Item Reference
# table, never CustomerSKU — see sbic_db.py's own module docstring), so there is nothing
# for this constant to actually do here. It exists purely so a future reader porting
# more of the trigger knows this special case was seen and deliberately left out.
PUREGOLD_CUSTOMER_ID = 670

# customerId -> fixed customerBranchId, ported verbatim from tr_CustomerPOBQ's final CASE
# statement (RGMC_DB/sbic_prod_db/03_Table_Trigger/table_trigger.sql) for customers whose
# branch can't be reliably derived from customerBranchName text alone — a retail chain
# with one BC branch per physical store that doesn't follow the usual "<code> - <name>"
# pattern, or an online marketplace with no real "branch" concept at all. Wins over the
# normal lookup below whenever the customer is one of these, exactly like the trigger.
# Keep this in sync with the SQL trigger by hand if it's ever updated there. Every one of
# these 12 pairs was re-verified character-by-character against the live trigger source —
# this is the complete set; there are no others.
_FIXED_BRANCH_BY_CUSTOMER_ID: dict[int, int] = {
    16: 547,     # Landmark Trinoma
    540: 2449,   # Landmark Alabang
    541: 2450,   # Landmark Nuvali
    981: 3985,   # Landmark Manila Bay
    130: 389,    # Lee Plaza Super Plaza
    132: 391,    # Lee Plaza City Central
    211: 772,    # Lee Plaza Hypermart
    792: 3437,   # Alturas Plaza Marcela
    224: 1911,   # Lazada
    841: 3570,   # Shopee
    1025: 4129,  # TikTok
    73: 186,     # Shoppers Mart
}


def fetch_active_customers() -> list[dict]:
    """customerId, customerCode, lookUpCode for every active Customer row with a
    lookUpCode set — the exact set tr_CustomerPOBQ itself scans for its
    `customerName LIKE '%' + lookUpCode + '%'` match. customerCode is carried along too
    (not used for matching — lookUpCode is) but it's the same prefix
    CustomerBranch.customerBranchCode is built from (e.g. "WA001" in "WA001-Q001-SU"),
    selected here alongside lookUpCode so a caller has both without a second query."""
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT customerId, customerCode, lookUpCode FROM Customer "
            "WHERE isActive = 1 AND lookUpCode <> ''"
        )).mappings().all()
    return [dict(r) for r in rows]


def fetch_customer_branches(customer_ids: list[int] | None = None) -> list[dict]:
    """customerBranchId, customerId, lookUpCode, customerBranchCode for every
    CustomerBranch row with a lookUpCode set — the exact set tr_CustomerPOBQ scans for
    its numeric-exact / non-numeric-substring match, scoped by customerId.
    customer_ids (optional) narrows to just the customers resolved for this batch,
    avoiding pulling every branch in sbic_prod when only a handful of customers are
    actually in play this run."""
    engine = _get_engine()
    conditions = ["lookUpCode <> ''"]
    params: dict = {}
    if customer_ids:
        id_params = {f"cid{i}": cid for i, cid in enumerate(customer_ids)}
        conditions.append(f"customerId IN ({', '.join(':' + k for k in id_params)})")
        params.update(id_params)
    query = (
        "SELECT customerBranchId, customerId, lookUpCode, customerBranchCode "
        f"FROM CustomerBranch WHERE {' AND '.join(conditions)}"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    return [dict(r) for r in rows]


def fetch_customer_skus(customer_ids: list[int]) -> list[dict]:
    """customerId, customerSKUCode, skuId for the given customers' CustomerSKU rows —
    the same table tr_CustomerPOBQ joins to resolve CustomerPOULDetail.skuId. Not
    currently consumed by any item-resolution logic (so_import_worker.py resolves items
    exclusively via BC's own Item Reference table — see _build_ref_map in
    so_import_worker.py) — provided here, pre-scoped by customerId, so a future feature
    needing it doesn't require a new round trip on top of this module's existing
    Customer/CustomerBranch resolution.
    """
    if not customer_ids:
        return []
    engine = _get_engine()
    id_params = {f"cid{i}": cid for i, cid in enumerate(customer_ids)}
    query = (
        "SELECT customerId, customerSKUCode, skuId FROM CustomerSKU "
        f"WHERE customerId IN ({', '.join(':' + k for k in id_params)})"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(query), id_params).mappings().all()
    return [dict(r) for r in rows]


def _branch_lookup_code(branch_name: str | None) -> tuple[str, bool]:
    """Mirror tr_CustomerPOBQ's customerBranchLookUpCode derivation exactly: if the
    branch name starts with a digit, the lookup code is the leading run of digits (e.g.
    "3035 - METRO SUPERMARKET..." -> "3035"); otherwise it's the whole trimmed/
    uppercased name. Returns (lookup_code, is_numeric)."""
    stripped = (branch_name or "").strip()
    if stripped[:1].isdigit():
        m = re.match(r"\d+", stripped)
        return (m.group(0) if m else stripped), True
    return stripped.upper(), False


def resolve_customer_id(customer_name: str, customers: list[dict]) -> int | None:
    """The Customer whose lookUpCode appears as a substring of customer_name (spaces
    stripped from both sides before comparing) — replicates tr_CustomerPOBQ's own
    `customerName LIKE '%' + lookUpCode + '%'` match exactly. customers is the full
    pre-fetched list (fetch_active_customers()) — matching happens in Python rather than
    SQL, same batch-prefetch-then-match-in-memory pattern this codebase already uses for
    BC ship-to addresses and item references. Returns None when nothing matches, same as
    the trigger silently leaving customerId at its -1 placeholder."""
    norm_customer_name = _normalize(customer_name)
    if not norm_customer_name:
        return None
    for c in customers:
        lookup = _normalize(c.get("lookUpCode"))
        if lookup and lookup in norm_customer_name:
            return c.get("customerId")
    return None


def resolve_branch(customer_id: int, branch_name: str, branches: list[dict]) -> dict:
    """Resolve one branch's customerBranchId/customerBranchCode/lookUpCode, scoped to an
    already-resolved customerId (see resolve_customer_id), replicating tr_CustomerPOBQ's
    own matching rules exactly:
      1. An EXACT match on lookUpCode when the branch's derived lookup code (see
         _branch_lookup_code) is numeric, else a SUBSTRING match (the CustomerBranch
         row's lookUpCode must appear within the branch's derived code).
      2. The fixed customerId -> customerBranchId overrides
         (_FIXED_BRANCH_BY_CUSTOMER_ID) win over step 1 whenever the customer is one of
         them — same as the trigger's own final CASE statement.

    branches is the full pre-fetched list (fetch_customer_branches()), ideally already
    scoped to the customer_ids seen in this batch.

    Returns {"customerBranchId": None, "customerBranchCode": "", "lookUpCode": ""} (or
    partially filled) when nothing matches — a best-effort result, never raises, same as
    the trigger silently leaving customerBranchId at its -1 placeholder.
    """
    branch_lookup_code, is_numeric = _branch_lookup_code(branch_name)
    norm_branch_lookup = _normalize(branch_lookup_code)

    branch_id = None
    branch_code = ""
    matched_lookup = ""
    for b in branches:
        if b.get("customerId") != customer_id:
            continue
        b_lookup = _normalize(b.get("lookUpCode"))
        if not b_lookup:
            continue
        matched = (b_lookup == norm_branch_lookup) if is_numeric else (b_lookup in norm_branch_lookup)
        if matched:
            branch_id = b.get("customerBranchId")
            branch_code = b.get("customerBranchCode") or ""
            matched_lookup = b.get("lookUpCode") or ""
            break

    fixed_branch_id = _FIXED_BRANCH_BY_CUSTOMER_ID.get(customer_id)
    if fixed_branch_id is not None:
        branch_id = fixed_branch_id
        fixed_row = next((b for b in branches if b.get("customerBranchId") == fixed_branch_id), None)
        if fixed_row:
            branch_code = fixed_row.get("customerBranchCode") or branch_code
            matched_lookup = fixed_row.get("lookUpCode") or matched_lookup

    return {"customerBranchId": branch_id, "customerBranchCode": branch_code, "lookUpCode": matched_lookup}

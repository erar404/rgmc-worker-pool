"""Pub/Sub consumer for POUL Sales Order import messages.

Subscribes to PUBSUB_POUL_SO_SUBSCRIPTION. Each message is published by the GCP
BigQuery bridge after it successfully inserts customerpoul data for companies whose
name contains "SUNCOAST" or "SBIC".

Message format (JSON) — batch (current):
  {
    "type": "poul-so-import-batch",
    "orders": [
      { "header": { ...CustomerPOULBQ row fields... },
        "lines":  [ { ...CustomerPOULDetailBQ row fields... }, ... ] },
      ...
    ]
  }

Legacy single-order format is also accepted for backward compatibility:
  { "type": "poul-so-import", "header": {...}, "lines": [...] }

A third message type triggers a buffer-only retry pass, with no fresh orders — published
by gcp-api's POST /customerpoul/reprocess-buffer (manual "reprocess now" trigger):
  { "type": "poul-so-reprocess-buffer", "companies": ["SBIC", "MTC"] }
  "companies" is optional; omitted means every company in _ALL_BC_COMPANIES.

A fourth message type backfills lines onto orders whose BC header already exists but
whose Firestore buffer doc is gone (already deleted after a header-only success) —
published by gcp-api's POST /customerpoul/sync-inserted-orders:
  { "type": "poul-so-sync-from-cloudsql", "companies": [...], "create_by": "trigger" }
  For every CustomerPOUL row matching create_by (default "trigger" — the BigQuery
  bridge's automated inserts), finds the BC order by externalDocumentNo == poRefNumber,
  and adds whichever CustomerPOULDetailBQ lines aren't already on it. Any line that
  still can't be resolved (no BC item match, or BC rejects it) gets buffered with the
  order's so_number, same as a normal reprocess-buffer entry.

A fifth message type is the exact opposite skip condition of the fourth: it creates a
FRESH BC order (header + lines) for a CustomerPOUL row that was never inserted into BC
at all, skipping any row whose externalDocumentNo already matches an existing BC order
— published by gcp-api's POST /customerpoul/backfill-from-cloudsql:
  { "type": "poul-so-backfill-from-cloudsql", "companies": [...], "create_by": "trigger",
    "date_from": "2026-01-01", "date_to": "2026-01-31" }
  date_from/date_to (both optional) scope the CustomerPOUL rows considered to a
  createDate range (when the row was inserted into CustomerPOUL, not poDate, the
  original PO date from the source ERP). Any row that can't be fully resolved (no
  ship-to/customer/item match, or BC rejects it) is buffered via so_buffer, exactly
  like a normal inbound batch.

Processing per batch message:
  1. Resolve BC company from the first order's companyName.
  2. Pre-fetch all BC ship-to addresses and item references ONCE for the batch.
  3. For each order: look up customer via ship-to address (by customerBranchLookUpCode code
     first, then customerBranchName), create SO header + lines.
  4. Send one consolidated email covering all orders in the batch.

On transient failure during pre-fetch: message.nack() for Pub/Sub redelivery.
On per-order permanent failure: logged + included in the batch email, processing continues.
"""
import json
import logging
import re
import time
from difflib import SequenceMatcher

from google.cloud import pubsub_v1

from src import config
from src.services import bc_client, gcp_api_client, so_buffer, so_buffer_history, so_buffer_overrides, reprocess_status
from src.services.send_mail import notify_error, notify_success, notify_warning

logger = logging.getLogger("worker.so_import")

_TRANSIENT_SIGNALS = ("429", "502", "503", "timeout", "ConnectionError", "ReadTimeout")

# Maps source companyName keywords (upper-case, substring match) → BC company name.
# Checked in order; first match wins. Falls back to POUL_SO_BC_COMPANY / BC_COMPANY.
_COMPANY_MAP: list[tuple[str, str]] = [
    ("SUNCOAST", "SBIC"),
    ("SBIC", "SBIC"),
]

# BC company code -> CustomerPOUL.companyId on sbic_prod (MSSQL) — a clean numeric key,
# unlike companyName (free text, and _COMPANY_MAP above has no MTC entry at all). Used
# by the Cloud SQL backfill to scope its CustomerPOUL query to exactly the company
# selected on the /reconcile page's dropdown, instead of relying on companyName keyword
# matching after the fact.
_BC_COMPANY_ID_MAP: dict[str, int] = {"SBIC": 6, "MTC": 12}

# UOM rules: if the source unit_of_measurement contains any of these substrings
# (case-insensitive), treat the line as Piece/Pcs and use poQtyPcs / unitPricePcs.
_PCS_KEYWORDS = ("pcs", "piece", "pc/s")

# Minimum SequenceMatcher ratio (0–1) for a fuzzy ship-to name match to be accepted.
_SHIP_TO_FUZZY_THRESHOLD = 0.5


def _normalize_name(s: str) -> str:
    """Uppercase, strip punctuation, collapse whitespace — for fuzzy comparison."""
    return " ".join(re.sub(r"[^A-Z0-9\s]", "", (s or "").upper()).split())


def _fuzzy_ship_to_lookup(
    branch_name: str,
    ship_to_by_name: dict[str, tuple[str, str]],
) -> tuple[str, str] | None:
    """Return (customer_no, ship_to_code) for the BC ship-to address whose normalized
    name has the highest SequenceMatcher similarity to branch_name, provided the ratio
    is at or above _SHIP_TO_FUZZY_THRESHOLD.  Returns None when no match qualifies.
    """
    norm_query = _normalize_name(branch_name)
    if not norm_query or not ship_to_by_name:
        return None

    best_ratio = 0.0
    best_key: str | None = None
    best_match: tuple[str, str] | None = None

    for norm_bc_name, match_data in ship_to_by_name.items():
        ratio = SequenceMatcher(None, norm_query, norm_bc_name).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_key = norm_bc_name
            best_match = match_data

    if best_ratio >= _SHIP_TO_FUZZY_THRESHOLD and best_match is not None:
        logger.info(
            f"Fuzzy ship-to match: {branch_name!r} → {best_key!r} "
            f"(ratio={best_ratio:.2f})"
        )
        return best_match

    logger.warning(
        f"Fuzzy ship-to: no match above threshold {_SHIP_TO_FUZZY_THRESHOLD} "
        f"for {branch_name!r} (best={best_key!r} ratio={best_ratio:.2f})"
    )
    return None


def _resolve_bc_company(src_company_name: str) -> str:
    upper = (src_company_name or "").upper()
    for keyword, bc_company in _COMPANY_MAP:
        if keyword in upper:
            return bc_company
    return config.POUL_SO_BC_COMPANY or config.BC_COMPANY


def _is_pcs(uom_raw: str) -> bool:
    lower = (uom_raw or "").lower().strip()
    return any(kw in lower for kw in _PCS_KEYWORDS)


def _uom_code(uom_raw: str) -> str:
    return "PCS" if _is_pcs(uom_raw) else "CS"


def _safe_float(val) -> float:
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _resolve_line_qty(line: dict) -> float:
    """The real order quantity for one line, picking the UOM-implied field first
    (poQtyPcs for a PCS unit, else poQty) and falling back to the other field when
    that one is non-positive.

    Needed because CustomerPOULDetailBQ (Cloud SQL) can carry the real quantity in
    the "wrong" field relative to its own unitOfMeasurement label — confirmed live
    2026-10-02 against CustomerPOULDetail (the clean, non-Document-AI table for the
    same PO): CustomerPOULDetailBQ had poQty=0/poQtyPcs=288/uom="Cases" for a line
    whose CustomerPOULDetail counterpart has the unambiguous poQty=288 (no uom/pcs
    split at all). Traced as far as this repo can see: the swap is already present in
    BigQuery's int_document_ai_detail (Document AI's OCR extraction, built by a dbt
    model outside any of these repos), not introduced by rgmc-gcp-api's bridge, whose
    own column rename is a straightforward 1:1 po_qty->poQty / po_qty_pcs->poQtyPcs.
    Rejecting the line outright in that case would silently drop an otherwise fully-
    and correctly-linked line forever — this still trusts the UOM label for which
    unit code/price to submit (see _build_line_payload), just not which physical
    field the quantity landed in.
    """
    uom_raw = line.get("unitOfMeasurement") or ""
    pcs = _is_pcs(uom_raw)
    primary = _safe_float(line.get("poQtyPcs") if pcs else line.get("poQty"))
    if primary > 0:
        return primary
    return _safe_float(line.get("poQty") if pcs else line.get("poQtyPcs"))


_SUBMITTED_BY = "SBIC AI Uploading"


def _build_header_payload(
    header: dict, customer_no: str, ship_to_code: str, location_code: str
) -> dict:
    payload: dict = {
        "sellToCustomerNo": customer_no,
        "externalDocumentNo": (header.get("poRefNumber") or "")[:35],
        "orderDate": header.get("poDate") or "",
        "postingDate": header.get("poDate") or "",
        "shipmentDate": header.get("deliveryDate") or "",
        "dueDate": header.get("cancellationDate") or "",
        "postingDescription": (header.get("remark") or "")[:100],
        "submittedBy": _SUBMITTED_BY,
    }
    if ship_to_code:
        payload["shipToCode"] = ship_to_code
    if location_code:
        payload["locationCode"] = location_code
    # Drop empty strings so BC uses its own defaults for omitted fields
    return {k: v for k, v in payload.items() if v}


def _build_line_payload(line: dict, item_no: str, location_code: str, line_no: int) -> dict:
    uom_raw: str = line.get("unitOfMeasurement") or ""
    pcs = _is_pcs(uom_raw)

    qty = _resolve_line_qty(line)
    unit_price = _safe_float(line.get("unitPricePcs") if pcs else line.get("unitPrice"))
    uom_code = _uom_code(uom_raw)

    # postingGroup used to be hardcoded here as "TRADE" — BC now rejects that as
    # "Control 'postingGroup' is read-only" on every line insert (confirmed live,
    # 2026-10-01). BC derives it on its own from the item/customer posting setup once
    # "number" is set, so it's no longer sent at all.
    #
    # lineNo (Rec."Line No." on RGMCSalesOrderLinesAPIv2, made Editable=true this
    # session) is now assigned explicitly by the caller rather than left to BC's own
    # auto-numbering — _create_order computes it deterministically (existing max + i *
    # 10000), matching standard BC line-number spacing without relying on BC's
    # auto-increment to behave correctly across the retry-on-409 loop below.
    payload: dict = {
        "lineType": "Item",
        "number": item_no,
        "lineNo": line_no,
        "unitOfMeasureCode": uom_code,
        "quantity": qty,
        "unitPrice": unit_price,
        "shipmentDate": line.get("deliveryDate") or "",
    }
    if location_code:
        payload["locationCode"] = location_code
    return {k: v for k, v in payload.items() if v != "" and v is not None}


_NO_SKU_KEY = "(no SKU code, no description)".upper()


def _resolve_valid_lines(
    po_ref: str, lines: list, ref_map: dict[str, str], inactive_skus: set[str] | None = None
) -> tuple[list[tuple[dict, str]], list[dict], list[dict]]:
    """Pre-validate lines against ref_map without touching BC.

    Returns (resolved, skipped, dropped_inactive):
      - resolved: [(line, item_no), ...] for lines with a known SKU, a matching item
        reference, and a positive quantity.
      - skipped: [{"sku", "description", "reason", "line"}, ...] for every line left
        out that might still become resolvable later — reason is "no_bc_match" (empty
        SKU or no item reference found in BC) or "non_positive_qty". "line" is the raw
        original line dict, kept so it can be re-buffered and retried (e.g. once a
        reconcile-page override resolves its SKU) instead of lost.
      - dropped_inactive: [{"sku", "description", "line"}, ...] for every line whose raw
        SKU (or description, if blank) matches a key saved on /reconcile's Inactive
        Items tab. These are deliberately excluded from both resolved AND skipped —
        never retried, never re-buffered, never counted toward unmatched_items —
        because marking a SKU inactive is a human's explicit "never resolve this"
        decision. Before this, that mark only ever changed what /reconcile displayed
        (dropped the group from the Items (SKU) tab's resolved/total counts); the
        worker itself had no idea and kept retrying the exact same unresolvable line
        forever, so an order could show "All links resolved — Items 0/0" while
        silently stuck in the buffer for weeks on a line marked inactive long ago.
    """
    resolved: list[tuple[dict, str]] = []
    skipped: list[dict] = []
    dropped_inactive: list[dict] = []
    inactive_skus = inactive_skus or set()
    for i, line in enumerate(lines, start=1):
        sku_raw: str = (line.get("customerSKUCode") or "").strip()
        sku: str = sku_raw.upper()
        desc: str = (line.get("customerSKUDesc") or "").strip()

        inactive_key = sku or (desc.upper() or _NO_SKU_KEY)
        if inactive_key in inactive_skus:
            logger.info(
                f"PO {po_ref} line {i}: {sku_raw or desc!r} is marked inactive on /reconcile — "
                f"dropping from this order, will never be retried"
            )
            dropped_inactive.append({"sku": sku_raw, "description": desc, "line": line})
            continue

        # A reconcile-page SKU link always wins over ref_map lookup — resolvedItem is
        # the denormalized copy rgmc-bc-api's apply_resolution_to_buffer writes onto this
        # exact line when a human resolves its SKU group on /reconcile. Previously written
        # but never read back here, so a resolved SKU link never actually changed the retry.
        resolved_item: dict = line.get("resolvedItem") or {}
        item_no: str | None = resolved_item.get("itemNo")
        if item_no:
            logger.info(f"PO {po_ref} line {i}: using reconcile-page SKU link → item={item_no!r}")
        elif not sku:
            logger.warning(f"PO {po_ref} line {i}: empty SKU — invalid")
            skipped.append({"sku": "", "description": desc, "reason": "no_bc_match", "line": line})
            continue
        else:
            item_no = ref_map.get(sku)
            if not item_no:
                logger.warning(
                    f"PO {po_ref} line {i}: SKU {sku!r} not found in item references — invalid"
                )
                skipped.append({"sku": sku_raw, "description": desc, "reason": "no_bc_match", "line": line})
                continue

        qty = _resolve_line_qty(line)
        if qty <= 0:
            logger.warning(f"PO {po_ref} line {i}: zero quantity — invalid")
            skipped.append({"sku": sku_raw, "description": desc, "reason": "non_positive_qty", "line": line})
            continue

        resolved.append((line, item_no))
    return resolved, skipped, dropped_inactive


def _apply_sku_overrides(lines: list, sku_overrides: dict[str, dict]) -> None:
    """Patch resolvedItem onto any line whose SKU (or description, if the SKU is
    blank) matches a saved SKU override — in place.

    Mirrors rgmc-bc-api's apply_resolution_to_buffer matching rule exactly (SKU code,
    or description when blank, case-insensitive) so a reconcile-page SKU link applies
    here too — needed because these lines came fresh from Cloud SQL
    (_sync_order_from_cloudsql), not from a buffer doc apply_resolution_to_buffer
    would already have patched.
    """
    if not sku_overrides:
        return
    for line in lines:
        sku = (line.get("customerSKUCode") or "").strip()
        match_key = (sku or (line.get("customerSKUDesc") or "").strip() or "(no SKU code, no description)").upper()
        resolved = sku_overrides.get(match_key)
        if resolved:
            line["resolvedItem"] = resolved


_ORDER_NOT_OPEN_RE = re.compile(r"Status must be equal to 'Open'.*?Current value is '([^']+)'")


def _order_status_lock_reason(bc_error_body) -> str | None:
    """If a line-create rejection is BC's "Status must be equal to 'Open'" guard, the
    order's header was changed (e.g. Released, Invoiced) in Business Central sometime
    after it was created here — every other line on this same order will be rejected
    identically, no matter how well-matched the item is, and no number of retries will
    ever change that; only a human changing the order's status back in BC can. Returns
    the BC-reported current status (e.g. "Released"), or None if this isn't that error.
    """
    try:
        message = (bc_error_body or {}).get("error", {}).get("message", "")
    except AttributeError:
        return None
    match = _ORDER_NOT_OPEN_RE.search(message)
    return match.group(1) if match else None


_INVALID_TABLE_RELATION_RE = re.compile(
    r"field (.+?) of table .+? contains a value \(([^)]+)\) that cannot be found in the related table \(([^)]+)\)"
)


def _describe_invalid_table_relation(bc_error_body, resolution_source: str) -> str | None:
    """If a sales-header-create rejection is BC's Internal_InvalidTableRelation (a
    field's value — most often Ship-to Code — doesn't exist in its related table FOR
    THE CUSTOMER THIS HEADER USES), returns a clear explanation naming the bad value,
    where it was resolved from, and where to fix it. Returns None if this isn't that
    error, so the caller falls back to BC's raw error body.

    This is almost always a stale/incorrect saved override, not a transient BC issue —
    e.g. a branch override on /reconcile whose shipToCode actually belongs to a
    DIFFERENT customer than its own customerNo (so "the ship-to exists in BC" is true,
    just not for this sell-to customer, which is exactly what BC's "related table"
    check is enforcing). Retrying changes nothing; only correcting the override does —
    the very next retry then succeeds automatically, since overrides are re-read fresh
    on every pass (no other buffer/history action is taken here, unlike
    _order_status_lock_reason's case — there's no reason to stop retrying something
    that self-heals the moment the override is fixed).
    """
    try:
        err = (bc_error_body or {}).get("error", {})
    except AttributeError:
        return None
    if err.get("code") != "Internal_InvalidTableRelation":
        return None
    match = _INVALID_TABLE_RELATION_RE.search(err.get("message", ""))
    if not match:
        return None
    field, bad_value, related_table = match.groups()
    return (
        f"{field} {bad_value!r} (from {resolution_source}) does not exist in {related_table!r} "
        f"for this order's customer in BC. This is a bad/stale saved override, not a transient "
        f"failure — it will keep failing every retry until the override is corrected (or deleted) "
        f"on /reconcile's Overrides tab; the next retry then succeeds automatically."
    )


def _describe_line_rejection(bc_error_body, item_no: str, resolution_source: str) -> str | None:
    """If a sales-LINE-create rejection is one BC reports deterministically for this
    exact item/value every time (not a transient BC issue), returns a clear,
    actionable message naming the item, where it was resolved from, and what's wrong.
    Returns None if this isn't a known-permanent rejection, so the caller falls back to
    BC's raw error body (same contract as _describe_invalid_table_relation).

    Covers two cases seen in practice:
      - Internal_InvalidTableRelation: some field on the line (most often Unit of
        Measure Code) has a value not configured for this item in BC — either a bad
        SKU override/source value, or a genuine gap in the item's BC setup.
      - Application_DialogException on the item number field: the resolved item_no
        (from a reconcile-page SKU override or a placeholder value) is not an actual
        item in BC at all.
    Neither self-heals on its own; retrying changes nothing until a human fixes the
    override on /reconcile or the item's setup in BC.
    """
    try:
        err = (bc_error_body or {}).get("error", {})
    except AttributeError:
        return None
    code = err.get("code")
    message = err.get("message", "")
    if code == "Internal_InvalidTableRelation":
        match = _INVALID_TABLE_RELATION_RE.search(message)
        if not match:
            return None
        field, bad_value, related_table = match.groups()
        return (
            f"item {item_no!r} (from {resolution_source}): {field} {bad_value!r} does not exist in "
            f"{related_table!r} in BC — not a transient failure; needs fixing on /reconcile's "
            f"Overrides tab or in this item's BC setup before this line can ever go through."
        )
    if code == "Application_DialogException" and err.get("target") == "number":
        return (
            f"item {item_no!r} (from {resolution_source}) is not a valid/existing item in BC — "
            f"not a transient failure; almost certainly a bad or placeholder SKU override on "
            f"/reconcile's Overrides tab that will keep failing every retry until corrected."
        )
    return None


def _create_order(
    header: dict,
    lines: list,
    company: str,
    ship_to_by_code: dict[str, tuple[str, str]],
    ship_to_by_lookup_code: dict[str, tuple[str, str]],
    ship_to_by_name: dict[str, tuple[str, str]],
    ref_map: dict[str, str],
    location_code: str,
    branch_overrides: dict[str, dict],
    customer_overrides: dict[str, dict],
    inactive_skus: set[str] | None = None,
    existing_so_number: str | None = None,
) -> dict:
    """Create (or resume) one BC Sales Order and return a result summary dict.

    ship_to_by_code / ship_to_by_lookup_code / ship_to_by_name map upper-cased branch
    code / lookup code / name → (customer_no, ship_to_code). ref_map is pre-fetched by
    the caller for the whole batch.

    branch_overrides / customer_overrides: so_buffer_overrides.fetch_branch_overrides()
    / fetch_customer_overrides() — every saved reconcile-page branch/customer link,
    keyed by raw upper-cased customerBranchName/customerName text, fetched fresh by
    the caller for the whole batch (see this function's own "Customer lookup via
    Ship-to Address" section for why this is needed alongside header.resolvedShipTo).

    existing_so_number: when set, this PO's header was already created on a previous
    pass (recorded as so_buffer's `so_number`) — skip ship-to resolution and header
    creation entirely, look the order up by its number, and only attempt to add
    whichever lines are still outstanding. This is what lets a buffered order whose
    header succeeded but left some lines unmatched get *those specific lines* added
    later (e.g. once a reconcile-page override resolves the SKU) instead of either
    re-creating a duplicate header or losing the unresolved lines outright.

    When left unset (every fresh inbound order, and any buffered order whose doc was
    never stamped with a so_number), a fresh externalDocumentNo lookup runs before
    deciding to create a new header — the same defensive check
    _backfill_from_cloudsql/_sync_order_from_cloudsql already make — so an order
    created through some other path for this PO ref (a manual BC entry, backfill, the
    BigQuery-lookup manual-buffer-insert feature) is resumed instead of duplicated.

    Raises ValueError on permanent failures (bad data, BC rejects — e.g. no ship-to
    match, the header/lookup itself failing). The header is always created (or found)
    once a ship-to match is found / so_number resolves, regardless of how many lines
    resolve — an order is no longer all-or-nothing; a PO with zero resolvable lines
    still gets its header (and PO ref number) into BC, just with no lines attached.
    """
    po_ref: str = header.get("poRefNumber", "unknown")
    base_line_no = 0

    if not existing_so_number:
        # Defensive fresh check — the caller may not already know about an order
        # created through some OTHER path for this exact PO ref (a manual BC entry,
        # the backfill-from-cloudsql path, the manual-trigger page's BigQuery-lookup
        # buffer-insert feature, or simply a buffer doc that was never stamped with
        # so_number). Without this, a fresh inbound order — or ANY buffered order with
        # no recorded so_number, which _run_batch always treats as existing_so_number
        # None — would sail straight into creating a DUPLICATE Sales Order under the
        # same externalDocumentNo. Mirrors the identical check _backfill_from_cloudsql
        # (skips entirely) and _sync_order_from_cloudsql (backfills only missing
        # lines) already make before acting on a PO — this makes _create_order itself
        # just as safe regardless of which caller reaches it. Best-effort: a failed
        # check here falls back to the normal create-new-header path rather than
        # blocking the whole order on an unrelated BC outage.
        try:
            found_existing = bc_client.v2_find_sales_order_by_external_doc_no(po_ref, company)
        except Exception as e:
            found_existing = None
            logger.warning(f"PO {po_ref}: existing-order check failed ({e}) — proceeding as if none exists")
        if found_existing:
            existing_so_number = found_existing.get("number")
            logger.info(
                f"PO {po_ref}: found existing BC order {existing_so_number!r} via externalDocumentNo "
                f"check — resuming it instead of creating a new header"
            )

    existing_item_nos: set[str] = set()
    if existing_so_number:
        # Header already exists in BC from a previous pass — resume it instead of
        # resolving ship-to / creating a second header for the same PO.
        order_no = existing_so_number
        found = bc_client.v2_find_sales_order_by_number(order_no, company)
        if not found:
            raise ValueError(
                f"Could not find existing BC sales order {order_no!r} for PO {po_ref!r} "
                f"(company={company!r}) — buffering for retry"
            )
        order_id: str = found.get("id", "")
        # New lines must continue past whatever's already on the order — base_line_no
        # stays 0 (first new line gets 10000) only for a brand-new header below.
        existing_lines = bc_client.v2_list_sales_order_lines(order_id, company)
        base_line_no = max((l.get("lineNo") or 0 for l in existing_lines), default=0)
        # Same existing_item_nos check _sync_order_from_cloudsql already makes before
        # backfilling a line — resuming an order (whether via a recorded so_number or
        # the fresh externalDocumentNo fallback above) must not re-POST a line whose
        # item is already on it. Covers a raw SKU resolved via an exact ref_map match
        # AND one resolved via a saved reconcile-page override identically — both are
        # just (line, item_no) pairs by the time the line-creation loop below runs, so
        # one check here covers every line source this function is ever called with
        # (CustomerPOULDetail, int_document_ai_detail, or anywhere else).
        existing_item_nos = {l.get("number") for l in existing_lines if l.get("number")}
    else:
        # ── Customer lookup via Ship-to Address ───────────────────────────────
        # A reconcile-page link always wins over automatic matching. Two independent
        # sources feed resolved_ship_to/resolved_customer:
        #   1. branch_overrides / customer_overrides — the saved override, fetched
        #      fresh from so_buffer_overrides_{env} by the caller for this whole pass
        #      (so_buffer_overrides.fetch_branch_overrides/fetch_customer_overrides).
        #   2. header.resolvedShipTo / header.resolvedCustomer — denormalized copies
        #      rgmc-bc-api's apply_resolution_to_buffer writes directly onto a buffer
        #      doc at the moment a human resolves it on /reconcile.
        # #1 takes priority over #2 — a buffer doc's denormalized field is a one-time
        # patch applied only to the buffer_ids known at save time, so if a user later
        # re-links the same branch/customer text to a different value (the UI's
        # "Change…" action) while some buffer doc with the OLD patched value was missed
        # by that save (e.g. page wasn't refreshed, or the doc didn't exist/was
        # recreated after the first link), the doc keeps the stale value forever and
        # #2-first would silently keep routing to the wrong customer/ship-to on every
        # retry, with no error. #1 is always the current truth for that branch/customer
        # text (fetch_branch_overrides/fetch_customer_overrides re-read it fresh on
        # every pass — same self-healing pattern _apply_sku_overrides already uses for
        # SKU links), so it must win whenever it has a value. #2 only still matters as
        # a fallback for a buffer doc whose branch/customer text no longer has ANY
        # saved override (e.g. the override was deleted outright via /reconcile's
        # "unlink", but the doc was already patched beforehand).
        # resolvedShipTo (customerNo + shipToCode) takes priority since it pins the
        # exact ship-to; resolvedCustomer alone (no branch link saved) still lets the
        # order in with just a customer, no ship-to code.
        branch_key = (header.get("customerBranchName") or "").strip().upper()
        customer_key = (header.get("customerName") or "").strip().upper()
        resolved_ship_to: dict = branch_overrides.get(branch_key) or header.get("resolvedShipTo") or {}
        resolved_customer: dict = customer_overrides.get(customer_key) or header.get("resolvedCustomer") or {}

        if resolved_ship_to.get("customerNo"):
            customer_no = resolved_ship_to["customerNo"]
            ship_to_code = resolved_ship_to.get("shipToCode", "")
            resolution_source = (
                f"the saved branch override for {header.get('customerBranchName')!r} on /reconcile's "
                f"Overrides tab (customerNo={customer_no!r}, shipToCode={ship_to_code!r})"
            )
            logger.info(f"PO {po_ref}: using reconcile-page branch link → customer={customer_no!r} shipTo={ship_to_code!r}")
        elif resolved_customer.get("customerNo"):
            customer_no = resolved_customer["customerNo"]
            ship_to_code = ""
            resolution_source = (
                f"the saved customer override for {header.get('customerName')!r} on /reconcile's "
                f"Overrides tab (customerNo={customer_no!r})"
            )
            logger.info(f"PO {po_ref}: using reconcile-page customer link → customer={customer_no!r} (no ship-to)")
        else:
            branch_code: str = (header.get("customerBranchLookUpCode") or "").strip().upper()
            branch_name: str = (header.get("customerBranchName") or "").strip()

            # 1. Exact match on BC's own native ship-to code (fastest, most reliable where it
            #    happens to align — BC's code and SBIC's branch_code are usually different
            #    coding schemes entirely, e.g. "DS001-C001" vs "2001").
            # 2. Exact match on the ship-to's lookupCode field (tableextension 50458) — this
            #    is purpose-built to hold SBIC's own CustomerBranch.lookUpCode value, so once
            #    populated this is the reliable match for branch_code, not #1.
            # 3. Fuzzy name match (SequenceMatcher on normalized strings) — last resort.
            ship_to_match = (
                ship_to_by_code.get(branch_code)
                or ship_to_by_lookup_code.get(branch_code)
                or _fuzzy_ship_to_lookup(branch_name, ship_to_by_name)
            )
            if not ship_to_match:
                raise ValueError(
                    f"No ship-to address in BC for branch code={branch_code!r} / "
                    f"name={branch_name!r} (company={company!r})"
                )
            customer_no, ship_to_code = ship_to_match
            resolution_source = f"automatic ship-to matching for branch {branch_name!r} (no saved override)"

        header_payload = _build_header_payload(header, customer_no, ship_to_code, location_code)
        h_status, h_data = bc_client.v2_create_record("salesOrders", header_payload, company)
        if h_status not in (200, 201):
            clearer = _describe_invalid_table_relation(h_data, resolution_source)
            detail = f"— {clearer}" if clearer else f"(BC {h_status}): {h_data}"
            raise ValueError(f"SO header create failed for PO {po_ref!r} {detail}")
        order_id = h_data.get("id", "")
        order_no = h_data.get("number", order_id)

    # ── Pre-validate lines BEFORE touching BC ──────────────────────────────────
    # No longer all-or-nothing: an order with zero resolvable lines still gets its
    # header created in BC (so the PO ref number is visible there), just with no
    # lines — the invalid lines are reported as skipped rather than blocking the
    # whole PO from ever reaching BC.
    resolved_lines, skipped_lines, dropped_inactive = _resolve_valid_lines(po_ref, lines, ref_map, inactive_skus)
    unmatched_items: list[dict] = [s for s in skipped_lines if s["reason"] == "no_bc_match"]
    if not resolved_lines:
        logger.warning(
            f"PO {po_ref!r}: all {len(lines)} line(s) invalid (missing SKU/item "
            f"reference or non-positive quantity) — creating header with no lines"
        )

    # ── Create Sales Order lines ──────────────────────────────────────────────
    # remaining_lines accumulates every line still not in BC after this pass — pre-
    # validation skips plus any that BC itself rejected — so the caller can re-buffer
    # exactly those (and only those) for a future retry.
    lines_created = 0
    lines_skipped = len(skipped_lines)
    lines_already_present = 0
    remaining_lines: list[dict] = [s["line"] for s in skipped_lines]
    order_locked_reason: str | None = None
    line_issues: list[str] = []
    next_line_no = base_line_no
    for line, item_no in resolved_lines:
        if item_no in existing_item_nos:
            # Already on the order from an earlier pass (or from whatever created it
            # before this run even found it via the fresh externalDocumentNo check) —
            # POSTing it again would create a duplicate line, not update one. Line
            # numbering intentionally only advances for lines actually submitted below,
            # same as _sync_order_from_cloudsql's equivalent loop.
            lines_already_present += 1
            continue
        if order_locked_reason:
            # Already confirmed this order's own status blocks every further line —
            # no BC call can possibly succeed for the rest, so just record them as not
            # added instead of repeating the same rejection line by line.
            remaining_lines.append(line)
            continue
        next_line_no += 10000
        line_payload = _build_line_payload(line, item_no, location_code, next_line_no)
        created = False
        for attempt in range(4):
            lh, ld = bc_client.v2_create_record(
                f"salesOrders({order_id})/salesOrderLines", line_payload, company
            )
            if lh in (200, 201):
                lines_created += 1
                created = True
                break
            if lh == 409 and attempt < 3:
                time.sleep(0.5 * (attempt + 1))
                continue
            logger.error(
                f"PO {po_ref} item={item_no!r} failed after {attempt + 1} attempt(s) "
                f"(BC {lh}): {ld}"
            )
            lines_skipped += 1
            lock_reason = _order_status_lock_reason(ld)
            if lock_reason:
                order_locked_reason = lock_reason
                logger.warning(
                    f"PO {po_ref} → BC {order_no!r}: order status is {lock_reason!r} (not 'Open') — "
                    f"no further lines will be attempted; see order_locked_reason in the caller"
                )
            else:
                resolution_source = (
                    "reconcile-page SKU link"
                    if (line.get("resolvedItem") or {}).get("itemNo") == item_no
                    else "automatic item-reference matching"
                )
                clearer = _describe_line_rejection(ld, item_no, resolution_source)
                if clearer:
                    logger.error(f"PO {po_ref} line rejected by BC — {clearer}")
                    line_issues.append(clearer)
            break
        if not created:
            remaining_lines.append(line)

    attempted_lines = len(resolved_lines) - lines_already_present
    if lines_created == 0 and attempted_lines > 0 and not order_locked_reason:
        # All ATTEMPTED lines were nonetheless rejected by BC (e.g. bad posting group)
        # — distinct from lines_already_present, which were never attempted at all, not
        # rejected. No longer rolled back — the header (and PO ref number) stays in BC
        # with no lines, same as the zero-resolvable-lines case above.
        logger.warning(
            f"PO {po_ref} → BC {order_no!r}: 0/{attempted_lines} lines created "
            f"(all rejected by BC) — header kept with no lines"
        )

    logger.info(
        f"POUL SO {'resumed' if existing_so_number else 'created'} — PO {po_ref!r} → BC {order_no!r} "
        f"(lines_created={lines_created} lines_already_present={lines_already_present} "
        f"lines_skipped={lines_skipped} remaining={len(remaining_lines)})"
    )
    return {
        "po_ref": po_ref,
        "order_no": order_no,
        "so_number": order_no,
        "remaining_lines": remaining_lines,
        "lines_already_present": lines_already_present,
        "customer_name": header.get("customerName", ""),
        "branch_name": header.get("customerBranchName", ""),
        "po_date": header.get("poDate", ""),
        "delivery_date": header.get("deliveryDate", ""),
        "lines_created": lines_created,
        "lines_skipped": lines_skipped,
        "unmatched_items": unmatched_items,
        "order_locked_reason": order_locked_reason,
        "dropped_inactive_count": len(dropped_inactive),
        "line_issues": line_issues,
    }


def _describe_unresolved_line(line: dict) -> str:
    resolved = line.get("resolvedItem") or {}
    item_no = resolved.get("itemNo")
    sku = line.get("customerSKUCode") or line.get("customerSKUDesc") or "(no SKU)"
    return f"{item_no} (SKU {sku})" if item_no else f"SKU {sku} (no BC item match)"


def _handle_order_locked(
    header: dict,
    lines: list,
    remaining_lines: list,
    company: str,
    so_number: str,
    lock_reason: str,
    buf_id: str | None,
    notify: dict | None,
    run_id: str | None,
) -> None:
    """A PO whose BC order can never accept more lines automatically (see
    _order_status_lock_reason — its status was changed to something other than 'Open'
    in BC after the header was created here, e.g. Released or Invoiced). Retrying this
    forever is pointless — only a human reopening the order in BC can ever fix it — so
    this removes the PO from the buffer for good (rather than endlessly re-buffering
    it, the previous behavior) and records the WHOLE order (every line, not just the
    unresolved ones) to so_buffer_history_{env} so there's a durable record of exactly
    which items never made it in. A human who later reopens the order in BC re-triggers
    this PO themselves via Sync/Backfill from Cloud SQL on /reconcile — same as every
    other buffer resolution path, deliberately never automatic.
    """
    po_ref = header.get("poRefNumber", "unknown")
    unresolved_desc = ", ".join(_describe_unresolved_line(line) for line in remaining_lines) or "(none)"
    detail = (
        f"BC order {so_number!r}'s status is {lock_reason!r} (not 'Open') — further lines cannot be "
        f"added automatically. Removed from the buffer; {len(remaining_lines)} item(s) never added to BC: "
        f"{unresolved_desc}. Reopen the order in BC, then use Sync/Backfill from Cloud SQL on /reconcile "
        f"to add them."
    )
    logger.warning(f"PO {po_ref!r} → BC {so_number!r}: {detail}")
    so_buffer_history.record_reconciliation(
        header, lines, company, "blocked_in_bc", notify,
        run_id=run_id, so_number=so_number, detail=detail,
    )
    if buf_id:
        so_buffer.delete_buffered_order(buf_id)


def _buffer_status_note(attempt_count: int | None) -> str:
    """Email-detail suffix for one failed order, from save_failed_order's return value.

    None means the failure never reached save_failed_order at all (e.g. backfill's
    existence check erroring out before an order was ever buffered) — distinct from
    0, which means the Firestore write itself failed (a real infra problem). Past
    so_buffer.MAX_ATTEMPTS the order is still kept (see save_failed_order's docstring
    — it's never dropped just for repeated failures), so this only changes the
    wording into a nudge to go resolve it on /reconcile, never the underlying data.
    """
    if attempt_count is None:
        return "  [not buffered — never attempted]"
    if attempt_count <= 0:
        return "  [NOT buffered — Firestore write failed, will not auto-retry]"
    if attempt_count > so_buffer.MAX_ATTEMPTS:
        return f"  [buffered for retry — failed {attempt_count}x, needs a manual link on /reconcile]"
    return "  [buffered for retry]"


def _send_batch_notification(
    successes: list[dict],
    errors: list[dict],
    company: str,
    src_company: str,
    label: str = "POUL SO Import",
    notify: dict | None = None,
) -> None:
    """Send one consolidated email covering all orders in the batch.

    label distinguishes a manual reprocess-buffer trigger ("POUL SO Reprocess-Buffer")
    from a normal inbound-PO batch ("POUL SO Import") in the email subject/title, so the
    two are easy to tell apart in an inbox. notify (optional) is the employee who
    triggered a manual reprocess — also CC'd on this email when set.
    """
    extra_recipients = [notify["email"]] if notify and notify.get("email") else None
    context = f"company={company} src_company={src_company}"
    if notify:
        context += (
            f" requested_by={notify.get('name', '')} <{notify.get('email', '')}> "
            f"({notify.get('department', '')}, {notify.get('company', '')})"
        )

    if successes:
        count = len(successes)
        total_created = sum(r["lines_created"] for r in successes)
        total_skipped = sum(r["lines_skipped"] for r in successes)
        order_lines = []
        for r in successes:
            customer_str = r["customer_name"]
            if r["branch_name"]:
                customer_str += f" — {r['branch_name']}"
            tag = "  [retried from buffer]" if r.get("from_buffer") else ""
            order_lines.append(
                f"  PO Ref        : {r['po_ref']}{tag}\n"
                f"  BC Order No.  : {r['order_no']}\n"
                f"  Customer      : {customer_str}\n"
                f"  PO Date       : {r['po_date']}\n"
                f"  Delivery Date : {r['delivery_date']}\n"
                f"  Lines Created : {r['lines_created']}  |  Lines Skipped : {r['lines_skipped']}"
            )
        detail = (
            f"Orders Created  : {count}\n"
            f"Total Lines In  : {total_created}\n"
            f"Total Skipped   : {total_skipped}\n"
            "\n\n"
            + "\n\n".join(order_lines)
        )
        if errors:
            detail += f"\n\nFailed Orders   : {len(errors)}\n"
            for e in errors:
                detail += f"  - {e['po_ref']}: {e['error']}{_buffer_status_note(e.get('attempt_count'))}\n"

        title = f"{label} — {count} order{'s' if count != 1 else ''} created"
        if errors:
            title += f", {len(errors)} failed"
        notify_success(title=title, detail=detail, context=context, extra_recipients=extra_recipients)

    elif errors:
        detail = "\n".join(
            f"  - {e['po_ref']}: {e['error']}{_buffer_status_note(e.get('attempt_count'))}"
            for e in errors
        )
        notify_error(
            title=f"{label} Failed — {len(errors)} order{'s' if len(errors) != 1 else ''}",
            detail=detail,
            context=context,
            extra_recipients=extra_recipients,
        )


def _send_unmatched_items_notification(
    successes: list[dict],
    company: str,
    src_company: str,
    label: str = "POUL SO Import",
    notify: dict | None = None,
) -> None:
    """Flag orders that were inserted into BC but left some lines out because their
    SKU had no matching item reference in BC (empty SKU or an unrecognized code —
    i.e. no data to match against), separate from the general success/failure email
    so these are easy to spot and reconcile (e.g. via the /reconcile page's SKU
    linking) even when the PO itself "succeeded". notify (optional) is the employee
    who triggered a manual reprocess — also CC'd on this email when set.
    """
    flagged = [r for r in successes if r.get("unmatched_items")]
    if not flagged:
        return

    extra_recipients = [notify["email"]] if notify and notify.get("email") else None
    context = f"company={company} src_company={src_company}"
    if notify:
        context += (
            f" requested_by={notify.get('name', '')} <{notify.get('email', '')}> "
            f"({notify.get('department', '')}, {notify.get('company', '')})"
        )
    total_unmatched = sum(len(r["unmatched_items"]) for r in flagged)
    order_lines = []
    for r in flagged:
        customer_str = r["customer_name"]
        if r["branch_name"]:
            customer_str += f" — {r['branch_name']}"
        item_lines = "\n".join(
            f"      - {(it['sku'] or '(blank SKU)')}"
            + (f" — {it['description']}" if it["description"] else "")
            for it in r["unmatched_items"]
        )
        order_lines.append(
            f"  PO Ref        : {r['po_ref']}\n"
            f"  BC Order No.  : {r['order_no']}\n"
            f"  Customer      : {customer_str}\n"
            f"  Unmatched ({len(r['unmatched_items'])}):\n{item_lines}"
        )
    detail = (
        f"Orders Inserted With Unmatched Items : {len(flagged)}\n"
        f"Total Unmatched Items                : {total_unmatched}\n"
        "\n\n"
        + "\n\n".join(order_lines)
    )
    count = len(flagged)
    notify_warning(
        title=f"{label} — {count} order{'s' if count != 1 else ''} inserted with unmatched items",
        detail=detail,
        context=context,
        extra_recipients=extra_recipients,
    )


# Distinct BC company codes _resolve_bc_company can produce — used when a
# poul-so-reprocess-buffer message doesn't name specific companies, so every
# company's buffer gets a retry pass.
_ALL_BC_COMPANIES: list[str] = sorted({bc_company for _, bc_company in _COMPANY_MAP})


_EMPTY_SUMMARY = {"orders_created": 0, "orders_failed": 0, "lines_created": 0, "lines_skipped": 0, "unmatched_items": 0}


def _sync_order_from_cloudsql(
    poul_header: dict,
    company: str,
    src_company: str,
    ref_map: dict[str, str],
    sku_overrides: dict[str, dict] | None = None,
    inactive_skus: set[str] | None = None,
) -> dict | None:
    """Backfill missing lines onto one BC sales order from Cloud SQL, using
    externalDocumentNo == poRefNumber to find it (not so_number/buffer state — this
    path is specifically for orders whose Firestore buffer doc is already gone).

    sku_overrides (from so_buffer_overrides.fetch_sku_item_overrides): a reconcile-page
    SKU link saved AFTER this order's header was already created in BC would otherwise
    never reach it — these lines come fresh from Cloud SQL, not a buffer doc
    apply_resolution_to_buffer could have already patched line.resolvedItem onto.

    Returns None if no BC order was ever created for this PO (nothing to sync — not
    an error, just out of scope for this pass). Otherwise returns a summary dict.
    Any line that can't be resolved against a BC item, or that BC itself rejects, is
    buffered via so_buffer with the order's so_number — the same mechanism a normal
    reprocess-buffer entry uses, so it shows up on the /reconcile page and a later
    pass resumes adding it via _create_order's existing_so_number path.
    """
    po_ref: str = poul_header.get("poRefNumber", "unknown")
    found = bc_client.v2_find_sales_order_by_external_doc_no(po_ref, company)
    if not found:
        return None
    order_id: str = found.get("id", "")
    order_no: str = found.get("number", order_id)

    existing_lines = bc_client.v2_list_sales_order_lines(order_id, company)
    existing_item_nos = {l.get("number") for l in existing_lines if l.get("number")}
    next_line_no = max((l.get("lineNo") or 0 for l in existing_lines), default=0)

    detail_rows = gcp_api_client.fetch_customerpouldetailbq(po_ref)
    _apply_sku_overrides(detail_rows, sku_overrides or {})
    resolved_lines, skipped_lines, dropped_inactive = _resolve_valid_lines(po_ref, detail_rows, ref_map, inactive_skus)

    lines_created = 0
    already_present = 0
    lines_skipped = len(skipped_lines)
    remaining_lines: list[dict] = [s["line"] for s in skipped_lines]
    order_locked_reason: str | None = None
    line_issues: list[str] = []

    for line, item_no in resolved_lines:
        if item_no in existing_item_nos:
            already_present += 1
            continue
        if order_locked_reason:
            remaining_lines.append(line)
            continue
        next_line_no += 10000
        line_payload = _build_line_payload(line, item_no, config.POUL_SO_DEFAULT_LOCATION, next_line_no)
        created = False
        for attempt in range(4):
            lh, ld = bc_client.v2_create_record(
                f"salesOrders({order_id})/salesOrderLines", line_payload, company
            )
            if lh in (200, 201):
                lines_created += 1
                created = True
                break
            if lh == 409 and attempt < 3:
                time.sleep(0.5 * (attempt + 1))
                continue
            logger.error(
                f"Cloud SQL sync — PO {po_ref} item={item_no!r} failed after "
                f"{attempt + 1} attempt(s) (BC {lh}): {ld}"
            )
            lines_skipped += 1
            lock_reason = _order_status_lock_reason(ld)
            if lock_reason:
                order_locked_reason = lock_reason
                logger.warning(
                    f"Cloud SQL sync — PO {po_ref!r} → BC {order_no!r}: order status is "
                    f"{lock_reason!r} (not 'Open') — no further lines will be attempted"
                )
            else:
                resolution_source = (
                    "reconcile-page SKU link"
                    if (line.get("resolvedItem") or {}).get("itemNo") == item_no
                    else "automatic item-reference matching"
                )
                clearer = _describe_line_rejection(ld, item_no, resolution_source)
                if clearer:
                    logger.error(f"Cloud SQL sync — PO {po_ref!r} line rejected by BC — {clearer}")
                    line_issues.append(clearer)
            break
        if not created:
            remaining_lines.append(line)

    buf_id = so_buffer._doc_id(poul_header)
    if order_locked_reason:
        # Same reasoning as _create_order's equivalent branch — this order's own BC
        # status blocks every further line, so there's no point re-buffering it (which
        # save_failed_order would otherwise do here, since this path runs specifically
        # for orders whose buffer doc is already gone — it would recreate one that can
        # never succeed). Record the whole order to history instead.
        _handle_order_locked(
            poul_header, detail_rows, remaining_lines, company,
            order_no, order_locked_reason, buf_id, None, None,
        )
    elif remaining_lines:
        dropped_note = (
            f" ({len(dropped_inactive)} line(s) excluded — marked inactive on /reconcile, never retried)"
            if dropped_inactive else ""
        )
        issues_note = f" — {'; '.join(line_issues)}" if line_issues else ""
        so_buffer.save_failed_order(
            poul_header, remaining_lines, company, src_company,
            f"Cloud SQL sync — order {order_no!r} still has {len(remaining_lines)} "
            f"unresolved line(s) (no BC item match or rejected by BC){dropped_note}{issues_note}",
            so_number=order_no,
        )
    elif so_buffer.get_buffered_order(buf_id):
        # Every line is now confirmed present in BC, and this exact PO is STILL
        # sitting in the buffer — this path never consults buffer state (it finds the
        # order by externalDocumentNo, not by buf_id, per this function's docstring),
        # so a PO that only got fully resolved via a Sync-from-Cloud-SQL run (as
        # opposed to Reprocess Buffer, whose own success path already cleans up —
        # see _run_batch) would otherwise sit in so_buffer forever with no code path
        # ever revisiting it, even though /reconcile's Buffer tab would show it as
        # fully linked. Clear it and move the record to history so it shows up there
        # instead, same as every other resolved-buffer-entry path.
        so_buffer.delete_buffered_order(buf_id)
        so_buffer_history.record_reconciliation(
            poul_header, detail_rows, company, "resolved", None,
            so_number=order_no,
            detail="Resolved via Sync from Cloud SQL — all lines confirmed present in BC.",
        )

    logger.info(
        f"POUL SO synced from Cloud SQL — PO {po_ref!r} → BC {order_no!r} "
        f"(added={lines_created} already_present={already_present} "
        f"skipped={lines_skipped})"
    )
    return {
        "po_ref": po_ref,
        "order_no": order_no,
        "lines_created": lines_created,
        "already_present": already_present,
        "lines_skipped": lines_skipped,
        "remaining_lines": len(remaining_lines),
        "dropped_inactive_count": len(dropped_inactive),
    }


def _run_sync_from_cloudsql(
    companies: list[str],
    create_by: str,
    notify: dict | None = None,
    po_ref_numbers: list[str] | None = None,
) -> tuple[bool, dict]:
    """Handle a poul-so-sync-from-cloudsql message: for every CustomerPOUL row with
    the given create_by, find its BC order (by externalDocumentNo) and backfill
    whatever lines are missing from Cloud SQL's CustomerPOULDetailBQ.

    po_ref_numbers, when given, narrows the candidate set to exactly those PO refs —
    e.g. the manual-trigger page's BigQuery Lookup "Quick Align" action, which only
    wants this safe, already-existing-order-only backfill run for the one PO it's
    looking at, not every create_by='trigger' row company-wide. Everything else about
    this function (never creates a header, only backfills missing lines on an order
    that already exists) is unchanged.

    A PO is reported as exactly one of:
      - synced     — checked successfully (lines added and/or already present).
      - not_found  — no BC order exists for this PO's externalDocumentNo. Genuine.
      - errored    — the check itself failed (e.g. BC returned 503 after exhausting
        bc_client's own retries) and was never actually resolved either way. Kept
        strictly separate from not_found — conflating the two previously made a
        transient BC outage during a manual sync (2026-10-01) look identical to "these
        1000 POs have no BC order yet", which isn't true and isn't actionable the same
        way. Each PO gets one immediate retry before landing here, since the kind of
        blip that causes this is often already gone a few seconds later.

    Always returns (True, summary) — a per-company pre-fetch failure is logged and
    that company is skipped rather than failing the whole run, since this is an
    on-demand maintenance pass, not something Pub/Sub needs to retry redelivering.
    """
    extra_recipients = [notify["email"]] if notify and notify.get("email") else None
    requested_by = (
        f" requested_by={notify.get('name', '')} <{notify.get('email', '')}> "
        f"({notify.get('department', '')}, {notify.get('company', '')})"
        if notify else ""
    )

    headers = gcp_api_client.fetch_customerpoul_by_create_by(create_by)
    if po_ref_numbers:
        wanted_refs = set(po_ref_numbers)
        headers = [h for h in headers if h.get("poRefNumber") in wanted_refs]
    synced: list[dict] = []
    not_found: list[str] = []
    errored: list[dict] = []

    # Overrides aren't scoped per BC company (so_buffer_service.save_override keys
    # purely by raw SKU/branch/customer text), so one fetch covers every company below.
    sku_overrides = so_buffer_overrides.fetch_sku_item_overrides()
    inactive_skus = so_buffer_overrides.fetch_inactive_skus()

    for company in companies:
        try:
            item_refs = bc_client.fetch_item_references(company)
        except Exception as e:
            logger.error(f"POUL SO sync: pre-fetch item references failed (company={company!r}): {e}")
            continue
        ref_map: dict[str, str] = _build_ref_map(item_refs)

        company_headers = [h for h in headers if _resolve_bc_company(h.get("companyName", "")) == company]
        retry_headers: list[dict] = []
        for h in company_headers:
            po_ref = h.get("poRefNumber", "unknown")
            try:
                result = _sync_order_from_cloudsql(h, company, f"cloudsql-sync:{company}", ref_map, sku_overrides, inactive_skus)
                if result is None:
                    not_found.append(po_ref)
                else:
                    synced.append(result)
            except Exception as e:
                logger.warning(f"Cloud SQL sync: PO {po_ref!r} errored, will retry once: {e}")
                retry_headers.append(h)

        # One retry per PO that raised above — gives a transient BC blip (the kind
        # that caused this fix) a second chance before it's reported as a real error,
        # since the rest of this company's pass has often given BC time to recover.
        for h in retry_headers:
            po_ref = h.get("poRefNumber", "unknown")
            try:
                result = _sync_order_from_cloudsql(h, company, f"cloudsql-sync:{company}", ref_map, sku_overrides, inactive_skus)
                if result is None:
                    not_found.append(po_ref)
                else:
                    synced.append(result)
            except Exception as e:
                logger.error(f"Cloud SQL sync failed for PO {po_ref!r} (after retry): {e}")
                errored.append({"po_ref": po_ref, "error": str(e)})

    context = f"companies={companies} create_by={create_by!r} po_ref_numbers={po_ref_numbers or 'all'}{requested_by}"
    if synced or not_found or errored:
        detail_lines = [
            f"  PO Ref {r['po_ref']:<20} → BC {r['order_no']}: "
            f"+{r['lines_created']} added, {r['already_present']} already present, "
            f"{r['lines_skipped']} skipped"
            for r in synced
        ]
        if errored:
            detail_lines.append("")
            detail_lines.append(f"Could not be checked due to an error (NOT necessarily missing from BC — retry this sync) — {len(errored)} PO(s):")
            detail_lines.extend(f"  - {e['po_ref']}: {e['error']}" for e in errored)
        if not_found:
            detail_lines.append("")
            detail_lines.append(f"No matching BC order found for {len(not_found)} PO(s):")
            detail_lines.extend(f"  - {ref}" for ref in not_found)
        title = f"POUL SO Cloud SQL Sync — {len(synced)} order(s) checked"
        if not_found:
            title += f", {len(not_found)} not found in BC"
        if errored:
            title += f", {len(errored)} errored"
        notify_success(title=title, detail="\n".join(detail_lines), context=context, extra_recipients=extra_recipients)
    else:
        notify_success(
            title="POUL SO Cloud SQL Sync — nothing to sync",
            detail=f"No CustomerPOUL rows found with create_by={create_by!r}.",
            context=context,
            extra_recipients=extra_recipients,
        )

    summary = {
        "orders_synced": len(synced),
        "orders_not_found": len(not_found),
        "orders_errored": len(errored),
        "lines_created": sum(r["lines_created"] for r in synced),
        "lines_skipped": sum(r["lines_skipped"] for r in synced),
    }
    return True, summary


def _run_backfill_from_cloudsql(
    companies: list[str],
    create_by: str,
    date_from: str | None = None,
    date_to: str | None = None,
    notify: dict | None = None,
) -> tuple[bool, dict]:
    """Handle a poul-so-backfill-from-cloudsql message: for every CustomerPOUL row
    matching create_by (and, if given, within [date_from, date_to] on createDate —
    when the row was inserted into CustomerPOUL, not poDate), create
    a FRESH BC sales order (header + lines) from CustomerPOUL/CustomerPOULDetailBQ —
    unless a BC order already exists for that PO's externalDocumentNo, in which case
    it's skipped untouched. This is the exact opposite skip condition from
    _sync_order_from_cloudsql, which only acts when the order already exists.

    Reuses _create_order directly (existing_so_number=None, same as a fresh inbound
    PO) so every already-fixed resolution path applies here too: a reconcile-page
    branch/customer/SKU link wins over automatic matching, a partially-resolved order
    still gets its header created, and anything left over is buffered via so_buffer —
    same Firestore mechanism every other import path uses, so it shows up on
    /reconcile for manual reconciliation instead of silently failing.

    Always returns (True, summary) — a per-company pre-fetch failure is logged and
    that company is skipped rather than failing the whole run, since this is an
    on-demand maintenance pass, not something Pub/Sub needs to retry redelivering.
    """
    extra_recipients = [notify["email"]] if notify and notify.get("email") else None
    requested_by = (
        f" requested_by={notify.get('name', '')} <{notify.get('email', '')}> "
        f"({notify.get('department', '')}, {notify.get('company', '')})"
        if notify else ""
    )

    location_code: str = config.POUL_SO_DEFAULT_LOCATION

    # Overrides aren't scoped per BC company (so_buffer_service.save_override keys
    # purely by raw SKU/branch/customer text), so one fetch covers every company below.
    sku_overrides = so_buffer_overrides.fetch_sku_item_overrides()
    inactive_skus = so_buffer_overrides.fetch_inactive_skus()
    branch_overrides = so_buffer_overrides.fetch_branch_overrides()
    customer_overrides = so_buffer_overrides.fetch_customer_overrides()

    successes: list[dict] = []
    errors: list[dict] = []
    skipped_existing: list[str] = []
    total_headers_considered = 0

    for company in companies:
        try:
            ship_tos = bc_client.fetch_ship_to_addresses(company)
            item_refs = bc_client.fetch_item_references(company)
        except Exception as e:
            logger.error(f"POUL SO backfill: pre-fetch failed (company={company!r}): {e}")
            continue
        ship_to_by_code, ship_to_by_lookup_code, ship_to_by_name = _build_ship_to_maps(ship_tos)
        ref_map: dict[str, str] = _build_ref_map(item_refs)

        # Scope the CustomerPOUL query itself to this company's companyId (SBIC=6,
        # MTC=12 on sbic_prod) rather than fetching every createBy row once and
        # filtering by companyName keyword after the fact — companyId is a clean,
        # unambiguous key (and _COMPANY_MAP above has no MTC entry at all, so the
        # keyword approach would silently misroute or drop MTC rows here).
        company_id = _BC_COMPANY_ID_MAP.get(company)
        if company_id is None:
            logger.warning(f"POUL SO backfill: no companyId mapping for BC company {company!r} — skipping")
            continue
        company_headers = gcp_api_client.fetch_customerpoul_by_create_by(
            create_by, date_from=date_from, date_to=date_to, company_id=company_id,
        )
        total_headers_considered += len(company_headers)
        for h in company_headers:
            po_ref = h.get("poRefNumber", "unknown")
            try:
                if bc_client.v2_find_sales_order_by_external_doc_no(po_ref, company):
                    skipped_existing.append(po_ref)
                    continue
            except Exception as e:
                logger.error(f"POUL SO backfill: existence check failed for PO {po_ref!r}: {e}")
                errors.append({"po_ref": po_ref, "error": str(e)})  # never buffered — the check itself failed
                continue

            lines = gcp_api_client.fetch_customerpouldetailbq(po_ref)
            _apply_sku_overrides(lines, sku_overrides)
            try:
                result = _create_order(
                    h, lines, company,
                    ship_to_by_code, ship_to_by_lookup_code, ship_to_by_name,
                    ref_map, location_code, branch_overrides, customer_overrides,
                    inactive_skus=inactive_skus, existing_so_number=None,
                )
                result["from_buffer"] = False
                successes.append(result)
                if result.get("order_locked_reason"):
                    # Freshly-created header got Released/Invoiced/etc. in BC before
                    # its lines could all be added — no retry will ever succeed; never
                    # buffer it, just record the whole order to history (buf_id is
                    # None here, backfill never reads from the buffer in the first
                    # place, so there's nothing to delete).
                    _handle_order_locked(
                        h, lines, result["remaining_lines"], company,
                        result["so_number"], result["order_locked_reason"],
                        None, notify, None,
                    )
                elif result["remaining_lines"]:
                    dropped_note = (
                        f" ({result['dropped_inactive_count']} line(s) excluded — marked inactive on /reconcile, never retried)"
                        if result.get("dropped_inactive_count") else ""
                    )
                    issues_note = (
                        f" — {'; '.join(result['line_issues'])}" if result.get("line_issues") else ""
                    )
                    so_buffer.save_failed_order(
                        h, result["remaining_lines"], company, f"cloudsql-backfill:{company}",
                        f"Backfill — header {result['so_number']!r} created — "
                        f"{len(result['remaining_lines'])} line(s) still unresolved "
                        f"(no BC item match or rejected by BC){dropped_note}{issues_note}",
                        so_number=result["so_number"],
                    )
            except Exception as e:
                err_str = str(e)
                if isinstance(e, ValueError):
                    logger.error(f"POUL SO backfill permanent failure — PO {po_ref!r}: {e}")
                else:
                    logger.error(f"POUL SO backfill error — PO {po_ref!r}: {e}")
                attempt_count = so_buffer.save_failed_order(h, lines, company, f"cloudsql-backfill:{company}", err_str)
                errors.append({"po_ref": po_ref, "error": err_str, "attempt_count": attempt_count})

    _send_batch_notification(
        successes, errors, "/".join(companies), f"cloudsql-backfill:{create_by}",
        label="POUL SO Cloud SQL Backfill", notify=notify,
    )
    _send_unmatched_items_notification(
        successes, "/".join(companies), f"cloudsql-backfill:{create_by}",
        label="POUL SO Cloud SQL Backfill", notify=notify,
    )
    if skipped_existing:
        notify_success(
            title=f"POUL SO Cloud SQL Backfill — {len(skipped_existing)} PO(s) skipped (already in BC)",
            detail="\n".join(f"  - {ref}" for ref in skipped_existing),
            context=f"companies={companies} create_by={create_by!r} date_from={date_from!r} date_to={date_to!r}{requested_by}",
            extra_recipients=extra_recipients,
        )
    if not total_headers_considered:
        notify_success(
            title="POUL SO Cloud SQL Backfill — nothing to backfill",
            detail=f"No CustomerPOUL rows found with create_by={create_by!r} "
                   f"date_from={date_from!r} date_to={date_to!r}.",
            context=f"companies={companies}{requested_by}",
            extra_recipients=extra_recipients,
        )

    summary = {
        "orders_created": len(successes),
        "orders_failed": len(errors),
        "orders_skipped_existing": len(skipped_existing),
        "lines_created": sum(r["lines_created"] for r in successes),
        "lines_skipped": sum(r["lines_skipped"] for r in successes),
        "unmatched_items": sum(len(r.get("unmatched_items") or []) for r in successes),
    }
    return True, summary


def _build_ship_to_maps(ship_tos: list) -> tuple[dict, dict, dict]:
    """ship_to_by_code / ship_to_by_lookup_code / ship_to_by_name, each mapping an
    upper-cased key to (customer_no, ship_to_code) — shared by every handler that
    resolves a PO's customer branch against BC's ship-to addresses.

    ship_to_by_code: exact upper-cased BC ship-to code.
    ship_to_by_lookup_code: exact upper-cased lookupCode (tableextension 50458 — SBIC's
      own CustomerBranch.lookUpCode, manually populated in BC).
    ship_to_by_name: normalized BC name, used for fuzzy match.
    """
    ship_to_by_code: dict[str, tuple[str, str]] = {}
    ship_to_by_lookup_code: dict[str, tuple[str, str]] = {}
    ship_to_by_name: dict[str, tuple[str, str]] = {}
    for st in ship_tos:
        cust_no = st.get("customerNumber") or ""
        st_code = (st.get("code") or "").strip()
        st_lookup_code = (st.get("lookupCode") or "").strip()
        st_name = (st.get("name") or "").strip()
        if not cust_no or not st_code:
            continue
        ship_to_by_code[st_code.upper()] = (cust_no, st_code)
        if st_lookup_code:
            ship_to_by_lookup_code[st_lookup_code.upper()] = (cust_no, st_code)
        if st_name:
            norm_name = _normalize_name(st_name)
            if norm_name:
                ship_to_by_name[norm_name] = (cust_no, st_code)
    return ship_to_by_code, ship_to_by_lookup_code, ship_to_by_name


def _build_ref_map(item_refs: list) -> dict[str, str]:
    """Upper-cased referenceNo → itemNo, shared by every handler that resolves a
    line's customerSKUCode against BC's item references."""
    return {
        r.get("referenceNo", "").upper(): r["itemNo"]
        for r in item_refs
        if r.get("itemNo") and r.get("referenceNo")
    }


def _run_batch(
    company: str,
    src_company: str,
    fresh_orders: list[dict],
    label: str = "POUL SO Import",
    notify: dict | None = None,
    record_history: bool = False,
    run_id: str | None = None,
) -> tuple[bool, dict]:
    """Process fresh_orders plus anything buffered for `company`.

    Used both by normal batch delivery (fresh_orders from the inbound message) and by
    a poul-so-reprocess-buffer trigger (fresh_orders=[], buffer-only retry pass).

    label is used verbatim in every email this call can send (pre-fetch failure, empty
    buffer, and the final batch result), so a manual reprocess trigger's outcome is
    always visible by mail and clearly distinguishable from a normal inbound-PO import.

    notify is the employee who triggered a manual reprocess from the reconcile page
    (None for normal inbound-PO batches) — {"name", "company", "department", "email"}.
    When set, every email this call sends is also delivered to notify["email"].

    record_history (True only for the poul-so-reprocess-buffer caller) appends one
    so_buffer_history_{env} record per PO processed here — header, lines, outcome,
    `notify`, and the current timestamp — so a PO's buffer reconciliation is still
    visible after the fact even once its so_buffer_{env} doc is deleted. Never set for
    normal inbound-PO batches, which have no triggering user to record. run_id (the
    same id reprocess_status tracks this run under) is carried onto each record so
    every PO reconciled by one reprocess-buffer run can be correlated back to it.

    Returns (ok, summary). ok is False only on a transient pre-fetch failure (caller
    should nack and redeliver); True otherwise (caller should ack — including when
    there was simply nothing to do). summary feeds reprocess_status's run tracking —
    it's _EMPTY_SUMMARY when ok is False, since nothing was actually processed yet.
    """
    extra_recipients = [notify["email"]] if notify and notify.get("email") else None
    requested_by = (
        f" requested_by={notify.get('name', '')} <{notify.get('email', '')}> "
        f"({notify.get('department', '')}, {notify.get('company', '')})"
        if notify else ""
    )
    # ── Pre-fetch shared BC data once for the whole batch ─────────────────────
    try:
        ship_tos = bc_client.fetch_ship_to_addresses(company)
        item_refs = bc_client.fetch_item_references(company)
    except Exception as e:
        err_str = str(e)
        logger.error(f"POUL SO pre-fetch failed (company={company!r}): {e}")
        if any(sig in err_str for sig in _TRANSIENT_SIGNALS):
            return False, dict(_EMPTY_SUMMARY)
        notify_error(
            title=f"{label} Error — BC pre-fetch failed",
            detail=err_str,
            context=f"company={company} src_company={src_company}{requested_by}",
            extra_recipients=extra_recipients,
        )
        return True, dict(_EMPTY_SUMMARY)

    ship_to_by_code, ship_to_by_lookup_code, ship_to_by_name = _build_ship_to_maps(ship_tos)
    ref_map: dict[str, str] = _build_ref_map(item_refs)
    location_code: str = config.POUL_SO_DEFAULT_LOCATION

    # ── Merge fresh orders with any previously buffered (failed) orders ───────
    # Each item: {"header": ..., "lines": ..., "_buf_id": str|None, "so_number": str|None}
    # so_number carries forward a header already created in BC on a previous pass
    # (see _create_order's existing_so_number) — always None for fresh orders.
    all_orders: list[dict] = [
        {"header": o.get("header", {}), "lines": o.get("lines", []), "_buf_id": None, "so_number": None}
        for o in fresh_orders
    ]
    for buf_doc_id, buf_data in so_buffer.get_buffered_orders(company):
        all_orders.append({
            "header": buf_data.get("header", {}),
            "lines": buf_data.get("lines", []),
            "_buf_id": buf_doc_id,
            "so_number": buf_data.get("so_number"),
        })

    # A buffered order's lines normally already carry resolvedItem (apply_resolution_to_buffer
    # patches it on save), but a fresh inbound order never does, and a buffer doc saved before
    # its SKU was linked only gets patched if the override is re-saved against its exact
    # buffer_id. Re-applying every current SKU/branch/customer override here, by raw text
    # rather than relying on that prior patch, covers both gaps — a linked item/branch/
    # customer takes effect whether the order is still being linked up or has already
    # reached BC once (branch/customer are read inside _create_order, below).
    sku_overrides = so_buffer_overrides.fetch_sku_item_overrides()
    inactive_skus = so_buffer_overrides.fetch_inactive_skus()
    branch_overrides = so_buffer_overrides.fetch_branch_overrides()
    customer_overrides = so_buffer_overrides.fetch_customer_overrides()
    for order_item in all_orders:
        _apply_sku_overrides(order_item["lines"], sku_overrides)

    if not all_orders:
        logger.info(f"POUL SO: nothing to do for company={company!r} (no fresh orders, empty buffer)")
        notify_success(
            title=f"{label} — {company}: nothing to retry",
            detail=f"Firestore buffer for company={company!r} is empty; no orders were reprocessed.",
            context=f"company={company} src_company={src_company}{requested_by}",
            extra_recipients=extra_recipients,
        )
        return True, dict(_EMPTY_SUMMARY)

    # ── Process every order (fresh + buffered) ─────────────────────────────────
    successes: list[dict] = []
    errors: list[dict] = []

    for order_item in all_orders:
        buf_id: str | None = order_item["_buf_id"]
        header: dict = order_item["header"]
        lines: list = order_item["lines"]
        so_number: str | None = order_item.get("so_number")
        po_ref: str = header.get("poRefNumber", "unknown")
        try:
            result = _create_order(
                header, lines, company,
                ship_to_by_code, ship_to_by_lookup_code, ship_to_by_name,
                ref_map, location_code, branch_overrides, customer_overrides,
                inactive_skus=inactive_skus, existing_so_number=so_number,
            )
            result["from_buffer"] = buf_id is not None
            successes.append(result)
            if result.get("order_locked_reason"):
                # The order's own BC status (Released, Invoiced, ...) blocks any
                # further lines — no retry will ever succeed. Remove it from the
                # buffer for good and record the whole order to history instead of
                # re-buffering it forever (see _handle_order_locked).
                _handle_order_locked(
                    header, lines, result["remaining_lines"], company,
                    result["so_number"], result["order_locked_reason"],
                    buf_id, notify, run_id,
                )
                continue
            dropped_note = (
                f" ({result['dropped_inactive_count']} line(s) excluded — marked inactive on /reconcile, never retried)"
                if result.get("dropped_inactive_count") else ""
            )
            if result["remaining_lines"]:
                # Header exists in BC but some lines still aren't — keep (or create)
                # the buffer doc with just those lines and the so_number, so a future
                # pass resumes this exact order instead of re-creating its header or
                # losing the unresolved lines. Applies whether this order came from
                # the buffer or was a fresh inbound PO that partially succeeded.
                issues_note = (
                    f" — {'; '.join(result['line_issues'])}" if result.get("line_issues") else ""
                )
                so_buffer.save_failed_order(
                    header, result["remaining_lines"], company, src_company,
                    f"Header {result['so_number']!r} created — "
                    f"{len(result['remaining_lines'])} line(s) still unresolved "
                    f"(no BC item match or rejected by BC){dropped_note}{issues_note}",
                    so_number=result["so_number"],
                )
                outcome = "still_buffered"
            else:
                if buf_id:
                    so_buffer.delete_buffered_order(buf_id)
                outcome = "resolved"
            if record_history:
                so_buffer_history.record_reconciliation(
                    header, lines, company, outcome, notify,
                    run_id=run_id, so_number=result.get("so_number"),
                    detail=f"Resolved{dropped_note}" if outcome == "resolved" and dropped_note else None,
                )
        except Exception as e:
            err_str = str(e)
            if isinstance(e, ValueError):
                logger.error(f"POUL SO permanent failure — PO {po_ref!r}: {e}")
            else:
                logger.error(f"POUL SO error — PO {po_ref!r}: {e}")
            attempt_count = so_buffer.save_failed_order(
                header, lines, company, src_company, err_str, so_number=so_number
            )
            errors.append({"po_ref": po_ref, "error": err_str, "attempt_count": attempt_count})
            if record_history:
                so_buffer_history.record_reconciliation(
                    header, lines, company, "failed", notify,
                    run_id=run_id, so_number=so_number, detail=err_str,
                )

    # ── Send one consolidated email, plus a separate flag for unmatched items ──
    _send_batch_notification(successes, errors, company, src_company, label=label, notify=notify)
    _send_unmatched_items_notification(successes, company, src_company, label=label, notify=notify)
    summary = {
        "orders_created": len(successes),
        "orders_failed": len(errors),
        "lines_created": sum(r["lines_created"] for r in successes),
        "lines_skipped": sum(r["lines_skipped"] for r in successes),
        "unmatched_items": sum(len(r.get("unmatched_items") or []) for r in successes),
    }
    return True, summary


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    # ── Parse ─────────────────────────────────────────────────────────────────
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"POUL SO message decode failed: {e} — dropping poison pill")
        message.ack()
        return

    msg_type = data.get("type", "")

    if msg_type == "poul-so-reprocess-buffer":
        # Manual trigger (published by gcp-api) — buffer-only retry pass, no fresh orders.
        # "notify" (optional) is the employee who triggered this from the reconcile page —
        # {"name", "company", "department", "email"} — CC'd on the result emails below.
        # "run_id" (optional) is what the /reconcile page polls (via rgmc-bc-api reading
        # Firestore reprocess_runs_{env}) to show ongoing/done/error instead of only
        # inferring progress from watching the buffered-order count.
        companies: list[str] = data.get("companies") or _ALL_BC_COMPANIES
        notify: dict | None = data.get("notify") or None
        run_id: str | None = data.get("run_id") or None
        logger.info(
            f"POUL SO: reprocess-buffer triggered for companies={companies} "
            f"notify={notify} run_id={run_id!r}"
        )
        reprocess_status.start_run(run_id, companies=companies, notify=notify)

        all_ok = True
        run_summary = dict(_EMPTY_SUMMARY)
        try:
            for company in companies:
                ok, summary = _run_batch(
                    company,
                    src_company=f"manual-reprocess:{company}",
                    fresh_orders=[],
                    label="POUL SO Reprocess-Buffer",
                    notify=notify,
                    record_history=True,
                    run_id=run_id,
                )
                if not ok:
                    all_ok = False
                else:
                    for k in run_summary:
                        run_summary[k] += summary.get(k, 0)
        except Exception as exc:
            logger.error(f"POUL SO: reprocess-buffer run {run_id!r} crashed: {exc}")
            reprocess_status.finish_run(run_id, status="error", summary=run_summary, error=str(exc))
            message.nack()
            return

        if all_ok:
            reprocess_status.finish_run(run_id, status="done", summary=run_summary)
            message.ack()
        else:
            # Transient pre-fetch failure — Pub/Sub will redeliver this same run_id, so
            # leave the Firestore doc at "processing" rather than finalizing it here.
            message.nack()
        return

    if msg_type == "poul-so-sync-from-cloudsql":
        # Manual trigger (published by gcp-api) — backfills lines onto BC orders whose
        # Firestore buffer doc is already gone, using Cloud SQL as the source of truth.
        companies = data.get("companies") or _ALL_BC_COMPANIES
        create_by = data.get("create_by") or "trigger"
        po_ref_numbers = data.get("po_ref_numbers") or None
        notify = data.get("notify") or None
        run_id = data.get("run_id") or None
        logger.info(
            f"POUL SO: Cloud SQL sync triggered for companies={companies} "
            f"create_by={create_by!r} po_ref_numbers={po_ref_numbers or 'all'} notify={notify} run_id={run_id!r}"
        )
        reprocess_status.start_run(run_id, companies=companies, notify=notify)
        try:
            ok, summary = _run_sync_from_cloudsql(companies, create_by, notify=notify, po_ref_numbers=po_ref_numbers)
        except Exception as exc:
            logger.error(f"POUL SO: Cloud SQL sync run {run_id!r} crashed: {exc}")
            reprocess_status.finish_run(run_id, status="error", summary={}, error=str(exc))
            message.nack()
            return
        if ok:
            reprocess_status.finish_run(run_id, status="done", summary=summary)
            message.ack()
        else:
            message.nack()
        return

    if msg_type == "poul-so-backfill-from-cloudsql":
        # Manual trigger (published by gcp-api) — creates fresh BC orders for
        # CustomerPOUL rows never inserted into BC at all, optionally date-ranged.
        companies = data.get("companies") or _ALL_BC_COMPANIES
        create_by = data.get("create_by") or "trigger"
        date_from = data.get("date_from") or None
        date_to = data.get("date_to") or None
        notify = data.get("notify") or None
        run_id = data.get("run_id") or None
        logger.info(
            f"POUL SO: Cloud SQL backfill triggered for companies={companies} "
            f"create_by={create_by!r} date_from={date_from!r} date_to={date_to!r} "
            f"notify={notify} run_id={run_id!r}"
        )
        reprocess_status.start_run(run_id, companies=companies, notify=notify)
        try:
            ok, summary = _run_backfill_from_cloudsql(companies, create_by, date_from, date_to, notify=notify)
        except Exception as exc:
            logger.error(f"POUL SO: Cloud SQL backfill run {run_id!r} crashed: {exc}")
            reprocess_status.finish_run(run_id, status="error", summary={}, error=str(exc))
            message.nack()
            return
        if ok:
            reprocess_status.finish_run(run_id, status="done", summary=summary)
            message.ack()
        else:
            message.nack()
        return

    if msg_type == "poul-so-import-batch":
        orders: list[dict] = data.get("orders", [])
    elif msg_type == "poul-so-import" and "header" in data:
        # Legacy single-order format — wrap for uniform handling
        orders = [{"header": data["header"], "lines": data.get("lines", [])}]
    else:
        logger.warning(f"POUL SO worker: unexpected message type {msg_type!r} — dropping")
        message.ack()
        return

    if not orders:
        logger.warning("POUL SO worker: empty orders list — dropping")
        message.ack()
        return

    # ── Resolve BC company from first order ───────────────────────────────────
    first_header: dict = orders[0].get("header", {})
    src_company: str = first_header.get("companyName", "")
    company: str = _resolve_bc_company(src_company)

    ok, _ = _run_batch(company, src_company, orders)
    if ok:
        message.ack()
    else:
        message.nack()


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the POUL SO import Pub/Sub subscriber and return the future."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_POUL_SO_SUBSCRIPTION,
    )
    # max_messages=1: each message is now a full batch; no need for concurrency here
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    future = subscriber.subscribe(
        subscription_path,
        callback=_process,
        flow_control=flow_control,
    )
    logger.info(f"POUL SO import worker subscribed to {subscription_path}")
    return future

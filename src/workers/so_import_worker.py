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
from src.services import bc_client, gcp_api_client, so_buffer, so_buffer_overrides, reprocess_status
from src.services.send_mail import notify_error, notify_success, notify_warning

logger = logging.getLogger("worker.so_import")

_TRANSIENT_SIGNALS = ("429", "502", "503", "timeout", "ConnectionError", "ReadTimeout")

# Maps source companyName keywords (upper-case, substring match) → BC company name.
# Checked in order; first match wins. Falls back to POUL_SO_BC_COMPANY / BC_COMPANY.
_COMPANY_MAP: list[tuple[str, str]] = [
    ("SUNCOAST", "SBIC"),
    ("SBIC", "SBIC"),
]

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
    }
    if ship_to_code:
        payload["shipToCode"] = ship_to_code
    if location_code:
        payload["locationCode"] = location_code
    # Drop empty strings so BC uses its own defaults for omitted fields
    return {k: v for k, v in payload.items() if v}


def _build_line_payload(line: dict, item_no: str, location_code: str) -> dict:
    uom_raw: str = line.get("unitOfMeasurement") or ""
    pcs = _is_pcs(uom_raw)

    qty = _safe_float(line.get("poQtyPcs") if pcs else line.get("poQty"))
    unit_price = _safe_float(line.get("unitPricePcs") if pcs else line.get("unitPrice"))
    uom_code = _uom_code(uom_raw)

    payload: dict = {
        "lineType": "Item",
        "number": item_no,
        "unitOfMeasureCode": uom_code,
        "quantity": qty,
        "unitPrice": unit_price,
        "postingGroup": "TRADE",
        "shipmentDate": line.get("deliveryDate") or "",
    }
    if location_code:
        payload["locationCode"] = location_code
    return {k: v for k, v in payload.items() if v != "" and v is not None}


def _resolve_valid_lines(
    po_ref: str, lines: list, ref_map: dict[str, str]
) -> tuple[list[tuple[dict, str]], list[dict]]:
    """Pre-validate lines against ref_map without touching BC.

    Returns (resolved, skipped) where resolved is [(line, item_no), ...] for lines
    with a known SKU, a matching item reference, and a positive quantity, and
    skipped is [{"sku", "description", "reason", "line"}, ...] for every line left
    out — reason is one of "no_bc_match" (empty SKU or no item reference found in
    BC — i.e. no data to match against) or "non_positive_qty". "line" is the raw
    original line dict, kept so an unresolved line can be re-buffered and retried
    later (e.g. once a reconcile-page override resolves its SKU) instead of lost.
    """
    resolved: list[tuple[dict, str]] = []
    skipped: list[dict] = []
    for i, line in enumerate(lines, start=1):
        sku_raw: str = (line.get("customerSKUCode") or "").strip()
        sku: str = sku_raw.upper()
        desc: str = (line.get("customerSKUDesc") or "").strip()

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

        uom_raw = line.get("unitOfMeasurement") or ""
        pcs = _is_pcs(uom_raw)
        qty = _safe_float(line.get("poQtyPcs") if pcs else line.get("poQty"))
        if qty <= 0:
            logger.warning(f"PO {po_ref} line {i}: zero quantity — invalid")
            skipped.append({"sku": sku_raw, "description": desc, "reason": "non_positive_qty", "line": line})
            continue

        resolved.append((line, item_no))
    return resolved, skipped


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


def _create_order(
    header: dict,
    lines: list,
    company: str,
    ship_to_by_code: dict[str, tuple[str, str]],
    ship_to_by_lookup_code: dict[str, tuple[str, str]],
    ship_to_by_name: dict[str, tuple[str, str]],
    ref_map: dict[str, str],
    location_code: str,
    existing_so_number: str | None = None,
) -> dict:
    """Create (or resume) one BC Sales Order and return a result summary dict.

    ship_to_by_code / ship_to_by_lookup_code / ship_to_by_name map upper-cased branch
    code / lookup code / name → (customer_no, ship_to_code). ref_map is pre-fetched by
    the caller for the whole batch.

    existing_so_number: when set, this PO's header was already created on a previous
    pass (recorded as so_buffer's `so_number`) — skip ship-to resolution and header
    creation entirely, look the order up by its number, and only attempt to add
    whichever lines are still outstanding. This is what lets a buffered order whose
    header succeeded but left some lines unmatched get *those specific lines* added
    later (e.g. once a reconcile-page override resolves the SKU) instead of either
    re-creating a duplicate header or losing the unresolved lines outright.

    Raises ValueError on permanent failures (bad data, BC rejects — e.g. no ship-to
    match, the header/lookup itself failing). The header is always created (or found)
    once a ship-to match is found / so_number resolves, regardless of how many lines
    resolve — an order is no longer all-or-nothing; a PO with zero resolvable lines
    still gets its header (and PO ref number) into BC, just with no lines attached.
    """
    po_ref: str = header.get("poRefNumber", "unknown")

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
    else:
        # ── Customer lookup via Ship-to Address ───────────────────────────────
        # A reconcile-page link always wins over automatic matching: header.resolvedShipTo
        # (saved against the "branch" override type) and header.resolvedCustomer (saved
        # against "customer") are denormalized copies rgmc-bc-api's apply_resolution_to_buffer
        # writes directly onto this buffer doc when a human resolves the branch/customer
        # group on /reconcile — previously written but never read back here, so a resolved
        # link never actually changed what got retried. resolvedShipTo (customerNo +
        # shipToCode) takes priority since it pins the exact ship-to; resolvedCustomer alone
        # (no branch link saved) still lets the order in with just a customer, no ship-to code.
        resolved_ship_to: dict = header.get("resolvedShipTo") or {}
        resolved_customer: dict = header.get("resolvedCustomer") or {}

        if resolved_ship_to.get("customerNo"):
            customer_no = resolved_ship_to["customerNo"]
            ship_to_code = resolved_ship_to.get("shipToCode", "")
            logger.info(f"PO {po_ref}: using reconcile-page branch link → customer={customer_no!r} shipTo={ship_to_code!r}")
        elif resolved_customer.get("customerNo"):
            customer_no = resolved_customer["customerNo"]
            ship_to_code = ""
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

        header_payload = _build_header_payload(header, customer_no, ship_to_code, location_code)
        h_status, h_data = bc_client.v2_create_record("salesOrders", header_payload, company)
        if h_status not in (200, 201):
            raise ValueError(
                f"SO header create failed for PO {po_ref!r} (BC {h_status}): {h_data}"
            )
        order_id = h_data.get("id", "")
        order_no = h_data.get("number", order_id)

    # ── Pre-validate lines BEFORE touching BC ──────────────────────────────────
    # No longer all-or-nothing: an order with zero resolvable lines still gets its
    # header created in BC (so the PO ref number is visible there), just with no
    # lines — the invalid lines are reported as skipped rather than blocking the
    # whole PO from ever reaching BC.
    resolved_lines, skipped_lines = _resolve_valid_lines(po_ref, lines, ref_map)
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
    remaining_lines: list[dict] = [s["line"] for s in skipped_lines]
    for i, (line, item_no) in enumerate(resolved_lines, start=1):
        line_payload = _build_line_payload(line, item_no, location_code)
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
                f"PO {po_ref} line {i} (item={item_no!r}) failed after {attempt + 1} attempt(s) "
                f"(BC {lh}): {ld}"
            )
            lines_skipped += 1
            break
        if not created:
            remaining_lines.append(line)

    if lines_created == 0 and resolved_lines:
        # All pre-validated lines were nonetheless rejected by BC (e.g. bad posting
        # group). No longer rolled back — the header (and PO ref number) stays in
        # BC with no lines, same as the zero-resolvable-lines case above.
        logger.warning(
            f"PO {po_ref} → BC {order_no!r}: 0/{len(resolved_lines)} lines created "
            f"(all rejected by BC) — header kept with no lines"
        )

    logger.info(
        f"POUL SO {'resumed' if existing_so_number else 'created'} — PO {po_ref!r} → BC {order_no!r} "
        f"(lines_created={lines_created} lines_skipped={lines_skipped} remaining={len(remaining_lines)})"
    )
    return {
        "po_ref": po_ref,
        "order_no": order_no,
        "so_number": order_no,
        "remaining_lines": remaining_lines,
        "customer_name": header.get("customerName", ""),
        "branch_name": header.get("customerBranchName", ""),
        "po_date": header.get("poDate", ""),
        "delivery_date": header.get("deliveryDate", ""),
        "lines_created": lines_created,
        "lines_skipped": lines_skipped,
        "unmatched_items": unmatched_items,
    }


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
                buffered_note = "  [buffered for retry]" if e.get("buffered") else "  [dropped — max retries exceeded]"
                detail += f"  - {e['po_ref']}: {e['error']}{buffered_note}\n"

        title = f"{label} — {count} order{'s' if count != 1 else ''} created"
        if errors:
            title += f", {len(errors)} failed"
        notify_success(title=title, detail=detail, context=context, extra_recipients=extra_recipients)

    elif errors:
        detail = "\n".join(
            f"  - {e['po_ref']}: {e['error']} "
            f"({'buffered for retry' if e.get('buffered') else 'dropped — max retries exceeded'})"
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

    detail_rows = gcp_api_client.fetch_customerpouldetailbq(po_ref)
    _apply_sku_overrides(detail_rows, sku_overrides or {})
    resolved_lines, skipped_lines = _resolve_valid_lines(po_ref, detail_rows, ref_map)

    lines_created = 0
    already_present = 0
    lines_skipped = len(skipped_lines)
    remaining_lines: list[dict] = [s["line"] for s in skipped_lines]

    for line, item_no in resolved_lines:
        if item_no in existing_item_nos:
            already_present += 1
            continue
        line_payload = _build_line_payload(line, item_no, config.POUL_SO_DEFAULT_LOCATION)
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
            break
        if not created:
            remaining_lines.append(line)

    if remaining_lines:
        so_buffer.save_failed_order(
            poul_header, remaining_lines, company, src_company,
            f"Cloud SQL sync — order {order_no!r} still has {len(remaining_lines)} "
            f"unresolved line(s) (no BC item match or rejected by BC)",
            so_number=order_no,
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
    }


def _run_sync_from_cloudsql(
    companies: list[str],
    create_by: str,
    notify: dict | None = None,
) -> tuple[bool, dict]:
    """Handle a poul-so-sync-from-cloudsql message: for every CustomerPOUL row with
    the given create_by, find its BC order (by externalDocumentNo) and backfill
    whatever lines are missing from Cloud SQL's CustomerPOULDetailBQ.

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
    synced: list[dict] = []
    not_found: list[str] = []

    # Overrides aren't scoped per BC company (so_buffer_service.save_override keys
    # purely by raw SKU/branch/customer text), so one fetch covers every company below.
    sku_overrides = so_buffer_overrides.fetch_sku_item_overrides()

    for company in companies:
        try:
            item_refs = bc_client.fetch_item_references(company)
        except Exception as e:
            logger.error(f"POUL SO sync: pre-fetch item references failed (company={company!r}): {e}")
            continue
        ref_map: dict[str, str] = {
            r.get("referenceNo", "").upper(): r["itemNo"]
            for r in item_refs
            if r.get("itemNo") and r.get("referenceNo")
        }

        company_headers = [h for h in headers if _resolve_bc_company(h.get("companyName", "")) == company]
        for h in company_headers:
            po_ref = h.get("poRefNumber", "unknown")
            try:
                result = _sync_order_from_cloudsql(h, company, f"cloudsql-sync:{company}", ref_map, sku_overrides)
                if result is None:
                    not_found.append(po_ref)
                else:
                    synced.append(result)
            except Exception as e:
                logger.error(f"Cloud SQL sync failed for PO {po_ref!r}: {e}")
                not_found.append(po_ref)

    context = f"companies={companies} create_by={create_by!r}{requested_by}"
    if synced or not_found:
        detail_lines = [
            f"  PO Ref {r['po_ref']:<20} → BC {r['order_no']}: "
            f"+{r['lines_created']} added, {r['already_present']} already present, "
            f"{r['lines_skipped']} skipped"
            for r in synced
        ]
        if not_found:
            detail_lines.append("")
            detail_lines.append(f"No matching BC order found for {len(not_found)} PO(s):")
            detail_lines.extend(f"  - {ref}" for ref in not_found)
        title = f"POUL SO Cloud SQL Sync — {len(synced)} order(s) checked"
        if not_found:
            title += f", {len(not_found)} not found in BC"
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
        "lines_created": sum(r["lines_created"] for r in synced),
        "lines_skipped": sum(r["lines_skipped"] for r in synced),
    }
    return True, summary


def _run_batch(
    company: str,
    src_company: str,
    fresh_orders: list[dict],
    label: str = "POUL SO Import",
    notify: dict | None = None,
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

    # Build ship-to lookup maps
    # ship_to_by_code: exact upper-cased BC ship-to code → (customer_no, ship_to_code)
    # ship_to_by_lookup_code: exact upper-cased lookupCode (tableextension 50458 — SBIC's
    #   own CustomerBranch.lookUpCode, manually populated in BC) → (customer_no, ship_to_code)
    # ship_to_by_name: normalized BC name → (customer_no, ship_to_code), used for fuzzy match
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

    ref_map: dict[str, str] = {
        r.get("referenceNo", "").upper(): r["itemNo"]
        for r in item_refs
        if r.get("itemNo") and r.get("referenceNo")
    }

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
    # buffer_id. Re-applying every current SKU override here, by raw SKU/description text
    # rather than relying on that prior patch, covers both gaps — a linked item's line gets
    # inserted whether the order is still being linked up or has already reached BC once.
    sku_overrides = so_buffer_overrides.fetch_sku_item_overrides()
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
                ref_map, location_code,
                existing_so_number=so_number,
            )
            result["from_buffer"] = buf_id is not None
            successes.append(result)
            if result["remaining_lines"]:
                # Header exists in BC but some lines still aren't — keep (or create)
                # the buffer doc with just those lines and the so_number, so a future
                # pass resumes this exact order instead of re-creating its header or
                # losing the unresolved lines. Applies whether this order came from
                # the buffer or was a fresh inbound PO that partially succeeded.
                so_buffer.save_failed_order(
                    header, result["remaining_lines"], company, src_company,
                    f"Header {result['so_number']!r} created — "
                    f"{len(result['remaining_lines'])} line(s) still unresolved "
                    f"(no BC item match or rejected by BC)",
                    so_number=result["so_number"],
                )
            elif buf_id:
                so_buffer.delete_buffered_order(buf_id)
        except Exception as e:
            err_str = str(e)
            if isinstance(e, ValueError):
                logger.error(f"POUL SO permanent failure — PO {po_ref!r}: {e}")
            else:
                logger.error(f"POUL SO error — PO {po_ref!r}: {e}")
            still_buffered = so_buffer.save_failed_order(
                header, lines, company, src_company, err_str, so_number=so_number
            )
            errors.append({"po_ref": po_ref, "error": err_str, "buffered": still_buffered})

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
        notify = data.get("notify") or None
        run_id = data.get("run_id") or None
        logger.info(
            f"POUL SO: Cloud SQL sync triggered for companies={companies} "
            f"create_by={create_by!r} notify={notify} run_id={run_id!r}"
        )
        reprocess_status.start_run(run_id, companies=companies, notify=notify)
        try:
            ok, summary = _run_sync_from_cloudsql(companies, create_by, notify=notify)
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

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
from src.services import bc_client, so_buffer
from src.services.send_mail import notify_error, notify_success

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
) -> tuple[list[tuple[dict, str]], int]:
    """Pre-validate lines against ref_map without touching BC.

    Returns (resolved, invalid_count) where resolved is [(line, item_no), ...] for
    lines with a known SKU, a matching item reference, and a positive quantity.
    Used up front so an order with zero resolvable lines never gets a header
    created in BC — see _create_order.
    """
    resolved: list[tuple[dict, str]] = []
    invalid = 0
    for i, line in enumerate(lines, start=1):
        sku: str = (line.get("customerSKUCode") or "").strip().upper()
        if not sku:
            logger.warning(f"PO {po_ref} line {i}: empty SKU — invalid")
            invalid += 1
            continue

        item_no: str | None = ref_map.get(sku)
        if not item_no:
            logger.warning(
                f"PO {po_ref} line {i}: SKU {sku!r} not found in item references — invalid"
            )
            invalid += 1
            continue

        uom_raw = line.get("unitOfMeasurement") or ""
        pcs = _is_pcs(uom_raw)
        qty = _safe_float(line.get("poQtyPcs") if pcs else line.get("poQty"))
        if qty <= 0:
            logger.warning(f"PO {po_ref} line {i}: zero quantity — invalid")
            invalid += 1
            continue

        resolved.append((line, item_no))
    return resolved, invalid


def _create_order(
    header: dict,
    lines: list,
    company: str,
    ship_to_by_code: dict[str, tuple[str, str]],
    ship_to_by_name: dict[str, tuple[str, str]],
    ref_map: dict[str, str],
    location_code: str,
) -> dict:
    """Create one BC Sales Order (header + lines) and return a result summary dict.

    ship_to_by_code / ship_to_by_name map upper-cased branch code / name → (customer_no, ship_to_code).
    ref_map is pre-fetched by the caller for the whole batch.
    Raises ValueError on permanent failures (bad data, BC rejects). Lines are fully
    pre-validated before the header is created, so an order with zero resolvable
    lines never leaves an empty SO sitting in BC — the whole order (header + lines)
    is buffered for retry instead.
    """
    po_ref: str = header.get("poRefNumber", "unknown")

    # ── Customer lookup via Ship-to Address ───────────────────────────────────
    branch_code: str = (header.get("customerBranchLookUpCode") or "").strip().upper()
    branch_name: str = (header.get("customerBranchName") or "").strip()

    # 1. Exact code match (fastest, most reliable)
    # 2. Fuzzy name match (SequenceMatcher on normalized strings)
    ship_to_match = (
        ship_to_by_code.get(branch_code)
        or _fuzzy_ship_to_lookup(branch_name, ship_to_by_name)
    )
    if not ship_to_match:
        raise ValueError(
            f"No ship-to address in BC for branch code={branch_code!r} / "
            f"name={branch_name!r} (company={company!r})"
        )
    customer_no, ship_to_code = ship_to_match

    # ── Pre-validate lines BEFORE touching BC ──────────────────────────────────
    # If nothing is resolvable, don't create a header at all — buffer header+lines
    # together so the whole order retries once the data issue (missing item
    # reference, bad qty, etc.) is fixed, instead of leaving an empty SO in BC.
    resolved_lines, invalid_count = _resolve_valid_lines(po_ref, lines, ref_map)
    if not resolved_lines:
        raise ValueError(
            f"No valid sales order lines for PO {po_ref!r} — all {len(lines)} line(s) "
            f"invalid (missing SKU/item reference or non-positive quantity); "
            f"buffering header + lines for retry"
        )

    # ── Create Sales Order header ─────────────────────────────────────────────
    header_payload = _build_header_payload(header, customer_no, ship_to_code, location_code)
    h_status, h_data = bc_client.v2_create_record("salesOrders", header_payload, company)
    if h_status not in (200, 201):
        raise ValueError(
            f"SO header create failed for PO {po_ref!r} (BC {h_status}): {h_data}"
        )

    order_id: str = h_data.get("id", "")
    order_no: str = h_data.get("number", order_id)

    # ── Create Sales Order lines ──────────────────────────────────────────────
    lines_created = 0
    lines_skipped = invalid_count
    for i, (line, item_no) in enumerate(resolved_lines, start=1):
        line_payload = _build_line_payload(line, item_no, location_code)
        for attempt in range(4):
            lh, ld = bc_client.v2_create_record(
                f"salesOrders({order_id})/salesOrderLines", line_payload, company
            )
            if lh in (200, 201):
                lines_created += 1
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

    if lines_created == 0:
        # All pre-validated lines were nonetheless rejected by BC (e.g. bad posting
        # group) — don't leave an empty SO sitting in BC. Delete the header
        # (best-effort) and buffer the whole order for retry instead.
        del_status = bc_client.v2_delete_record("salesOrders", order_id, company)
        logger.warning(
            f"PO {po_ref} → BC {order_no!r}: 0/{len(resolved_lines)} lines created — "
            f"deleting empty header (delete status {del_status}) and buffering for retry"
        )
        raise ValueError(
            f"All {len(resolved_lines)} line(s) rejected by BC for PO {po_ref!r} "
            f"(BC order {order_no!r} created then deleted); buffering header + lines for retry"
        )

    logger.info(
        f"POUL SO created — PO {po_ref!r} → BC {order_no!r} "
        f"(lines_created={lines_created} lines_skipped={lines_skipped})"
    )
    return {
        "po_ref": po_ref,
        "order_no": order_no,
        "customer_name": header.get("customerName", ""),
        "branch_name": header.get("customerBranchName", ""),
        "po_date": header.get("poDate", ""),
        "delivery_date": header.get("deliveryDate", ""),
        "lines_created": lines_created,
        "lines_skipped": lines_skipped,
    }


def _send_batch_notification(
    successes: list[dict],
    errors: list[dict],
    company: str,
    src_company: str,
) -> None:
    """Send one consolidated email covering all orders in the batch."""
    context = f"company={company} src_company={src_company}"

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

        title = f"POUL SO Import — {count} order{'s' if count != 1 else ''} created"
        if errors:
            title += f", {len(errors)} failed"
        notify_success(title=title, detail=detail, context=context)

    elif errors:
        detail = "\n".join(
            f"  - {e['po_ref']}: {e['error']} "
            f"({'buffered for retry' if e.get('buffered') else 'dropped — max retries exceeded'})"
            for e in errors
        )
        notify_error(
            title=f"POUL SO Import Failed — {len(errors)} order{'s' if len(errors) != 1 else ''}",
            detail=detail,
            context=context,
        )


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    # ── Parse ─────────────────────────────────────────────────────────────────
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"POUL SO message decode failed: {e} — dropping poison pill")
        message.ack()
        return

    msg_type = data.get("type", "")
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

    # ── Pre-fetch shared BC data once for the whole batch ─────────────────────
    try:
        ship_tos = bc_client.fetch_ship_to_addresses(company)
        item_refs = bc_client.fetch_item_references(company)
    except Exception as e:
        err_str = str(e)
        logger.error(f"POUL SO pre-fetch failed (company={company!r}): {e}")
        if any(sig in err_str for sig in _TRANSIENT_SIGNALS):
            message.nack()
            return
        notify_error(
            title="POUL SO Import Error — BC pre-fetch failed",
            detail=err_str,
            context=f"company={company} src_company={src_company}",
        )
        message.ack()
        return

    # Build ship-to lookup maps
    # ship_to_by_code: exact upper-cased code → (customer_no, ship_to_code)
    # ship_to_by_name: normalized BC name → (customer_no, ship_to_code), used for fuzzy match
    ship_to_by_code: dict[str, tuple[str, str]] = {}
    ship_to_by_name: dict[str, tuple[str, str]] = {}
    for st in ship_tos:
        cust_no = st.get("customerNumber") or ""
        st_code = (st.get("code") or "").strip()
        st_name = (st.get("name") or "").strip()
        if not cust_no or not st_code:
            continue
        ship_to_by_code[st_code.upper()] = (cust_no, st_code)
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

    # ── Merge new orders with any previously buffered (failed) orders ─────────
    # Each item: {"header": ..., "lines": ..., "_buf_id": str|None}
    all_orders: list[dict] = [
        {"header": o.get("header", {}), "lines": o.get("lines", []), "_buf_id": None}
        for o in orders
    ]
    for buf_doc_id, buf_data in so_buffer.get_buffered_orders(company):
        all_orders.append({
            "header": buf_data.get("header", {}),
            "lines": buf_data.get("lines", []),
            "_buf_id": buf_doc_id,
        })

    # ── Process every order (new + buffered) ──────────────────────────────────
    successes: list[dict] = []
    errors: list[dict] = []

    for order_item in all_orders:
        buf_id: str | None = order_item["_buf_id"]
        header: dict = order_item["header"]
        lines: list = order_item["lines"]
        po_ref: str = header.get("poRefNumber", "unknown")
        try:
            result = _create_order(header, lines, company, ship_to_by_code, ship_to_by_name, ref_map, location_code)
            result["from_buffer"] = buf_id is not None
            successes.append(result)
            if buf_id:
                so_buffer.delete_buffered_order(buf_id)
        except Exception as e:
            err_str = str(e)
            if isinstance(e, ValueError):
                logger.error(f"POUL SO permanent failure — PO {po_ref!r}: {e}")
            else:
                logger.error(f"POUL SO error — PO {po_ref!r}: {e}")
            still_buffered = so_buffer.save_failed_order(
                header, lines, company, src_company, err_str
            )
            errors.append({"po_ref": po_ref, "error": err_str, "buffered": still_buffered})

    # ── Send one consolidated email and ack ───────────────────────────────────
    _send_batch_notification(successes, errors, company, src_company)
    message.ack()


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

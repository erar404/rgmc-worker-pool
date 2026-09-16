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
  2. Pre-fetch all BC customers and item references ONCE for the batch.
  3. For each order: look up customer, create SO header, create SO lines.
  4. Send one consolidated email covering all orders in the batch.

On transient failure during pre-fetch: message.nack() for Pub/Sub redelivery.
On per-order permanent failure: logged + included in the batch email, processing continues.
"""
import json
import logging
import time

from google.cloud import pubsub_v1

from src import config
from src.services import bc_client
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


def _build_header_payload(header: dict, customer_no: str, location_code: str) -> dict:
    payload: dict = {
        "sellToCustomerNo": customer_no,
        "externalDocumentNo": (header.get("poRefNumber") or "")[:35],
        "orderDate": header.get("poDate") or "",
        "postingDate": header.get("poDate") or "",
        "shipmentDate": header.get("deliveryDate") or "",
        "dueDate": header.get("cancellationDate") or "",
        "postingDescription": (header.get("remark") or "")[:100],
    }
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


def _create_order(
    header: dict,
    lines: list,
    company: str,
    customer_map: dict[str, str],
    ref_map: dict[str, str],
    location_code: str,
) -> dict:
    """Create one BC Sales Order (header + lines) and return a result summary dict.

    customer_map and ref_map are pre-fetched by the caller for the whole batch.
    Raises ValueError on permanent failures (bad data, BC rejects).
    """
    po_ref: str = header.get("poRefNumber", "unknown")

    # ── Customer lookup from pre-fetched map ─────────────────────────────────
    customer_name_raw: str = header.get("customerName") or ""
    customer_no: str = customer_map.get(customer_name_raw.upper(), "")
    if not customer_no:
        raise ValueError(
            f"Customer not found in BC (company={company!r}): {customer_name_raw!r}"
        )

    # ── Create Sales Order header ─────────────────────────────────────────────
    header_payload = _build_header_payload(header, customer_no, location_code)
    h_status, h_data = bc_client.v2_create_record("salesOrders", header_payload, company)
    if h_status not in (200, 201):
        raise ValueError(
            f"SO header create failed for PO {po_ref!r} (BC {h_status}): {h_data}"
        )

    order_id: str = h_data.get("id", "")
    order_no: str = h_data.get("number", order_id)

    # ── Create Sales Order lines ──────────────────────────────────────────────
    lines_created = 0
    lines_skipped = 0
    for i, line in enumerate(lines, start=1):
        sku: str = (line.get("customerSKUCode") or "").strip().upper()
        if not sku:
            logger.warning(f"PO {po_ref} line {i}: empty SKU — skipping")
            lines_skipped += 1
            continue

        item_no: str | None = ref_map.get(sku)
        if not item_no:
            logger.warning(
                f"PO {po_ref} line {i}: SKU {sku!r} not found in item references — skipping"
            )
            lines_skipped += 1
            continue

        uom_raw = line.get("unitOfMeasurement") or ""
        pcs = _is_pcs(uom_raw)
        qty = _safe_float(line.get("poQtyPcs") if pcs else line.get("poQty"))
        if qty <= 0:
            logger.warning(f"PO {po_ref} line {i}: zero quantity — skipping")
            lines_skipped += 1
            continue

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
            order_lines.append(
                f"  PO Ref        : {r['po_ref']}\n"
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
                detail += f"  - {e['po_ref']}: {e['error']}\n"

        title = f"POUL SO Import — {count} order{'s' if count != 1 else ''} created"
        if errors:
            title += f", {len(errors)} failed"
        notify_success(title=title, detail=detail, context=context)

    elif errors:
        detail = "\n".join(f"  - {e['po_ref']}: {e['error']}" for e in errors)
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
        customers = bc_client.fetch_customers(company)
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

    customer_map: dict[str, str] = {
        c.get("name", "").upper(): c.get("customerNo", "")
        for c in customers
        if c.get("customerNo")
    }
    ref_map: dict[str, str] = {
        r.get("referenceNo", "").upper(): r["itemNo"]
        for r in item_refs
        if r.get("itemNo") and r.get("referenceNo")
    }

    location_code: str = config.POUL_SO_DEFAULT_LOCATION

    # ── Process each order in the batch ──────────────────────────────────────
    successes: list[dict] = []
    errors: list[dict] = []

    for order in orders:
        header: dict = order.get("header", {})
        lines: list = order.get("lines", [])
        po_ref: str = header.get("poRefNumber", "unknown")
        try:
            result = _create_order(header, lines, company, customer_map, ref_map, location_code)
            successes.append(result)
        except ValueError as e:
            logger.error(f"POUL SO permanent failure — PO {po_ref!r}: {e}")
            errors.append({"po_ref": po_ref, "error": str(e)})
        except Exception as e:
            logger.error(f"POUL SO error — PO {po_ref!r}: {e}")
            errors.append({"po_ref": po_ref, "error": str(e)})

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

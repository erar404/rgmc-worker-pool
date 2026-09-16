"""Pub/Sub consumer for POUL Sales Order import messages.

Subscribes to PUBSUB_POUL_SO_SUBSCRIPTION. Each message is published by the GCP
BigQuery bridge after it successfully inserts customerpoul data for companies whose
name contains "SUNCOAST" or "SBIC".

Message format (JSON):
  {
    "type": "poul-so-import",
    "header": { ...CustomerPOULBQ row fields... },
    "lines":  [ { ...CustomerPOULDetailBQ row fields... }, ... ]
  }

Processing per message:
  1. Look up the BC customer by name (customerName from header).
  2. Build a reference map: customerSKUCode → BC item number (via item references).
  3. Create a BC Sales Order header via the RGMC Sales Order API v2.
  4. For each line: resolve item, map UOM, create BC Sales Order line.

On success: message.ack()
On permanent failure (bad data, BC rejects): message.ack() + error email
On transient failure (429/5xx/network): message.nack() for Pub/Sub redelivery
"""
import json
import logging
import threading
import time

from google.cloud import pubsub_v1

from src import config
from src.services import bc_client
from src.services.send_mail import notify_error, notify_success

logger = logging.getLogger("worker.so_import")

_TRANSIENT_SIGNALS = ("429", "502", "503", "timeout", "ConnectionError", "ReadTimeout")

# UOM rules: if the source unit_of_measurement contains any of these substrings
# (case-insensitive), treat the line as Piece/Pcs and use poQtyPcs / unitPricePcs.
# Maps source companyName keywords (upper-case, substring match) → BC company name.
# Checked in order; first match wins. Falls back to POUL_SO_BC_COMPANY / BC_COMPANY.
_COMPANY_MAP: list[tuple[str, str]] = [
    ("SUNCOAST", "SBIC"),
    ("SBIC", "SBIC"),
]

_PCS_KEYWORDS = ("pcs", "piece", "pc/s")

# ---------------------------------------------------------------------------
# Batch accumulator — consolidates per-PO successes into one email per trigger
# ---------------------------------------------------------------------------

class _BatchAccumulator:
    """Debounces individual SO success results into one consolidated email.

    A bridge trigger publishes N messages in a tight burst. Each message is
    processed independently, but we want one summary email per trigger. We
    collect results and start a countdown timer; each new result resets it.
    When 60 s pass with no new results the batch is flushed as one email.
    """

    FLUSH_DELAY = 60  # seconds of silence after the last result

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._successes: list[dict] = []
        self._timer: threading.Timer | None = None

    def add(
        self,
        *,
        po_ref: str,
        order_no: str,
        customer_name: str,
        branch_name: str,
        po_date: str,
        delivery_date: str,
        lines_created: int,
        lines_skipped: int,
        company: str,
        src_company: str,
    ) -> None:
        with self._lock:
            self._successes.append(
                dict(
                    po_ref=po_ref,
                    order_no=order_no,
                    customer_name=customer_name,
                    branch_name=branch_name,
                    po_date=po_date,
                    delivery_date=delivery_date,
                    lines_created=lines_created,
                    lines_skipped=lines_skipped,
                    company=company,
                    src_company=src_company,
                )
            )
            self._reschedule()

    def _reschedule(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        self._timer = threading.Timer(self.FLUSH_DELAY, self._flush)
        self._timer.daemon = True
        self._timer.start()

    def _flush(self) -> None:
        with self._lock:
            batch = self._successes[:]
            self._successes.clear()
            self._timer = None
        if not batch:
            return
        count = len(batch)
        total_created = sum(r["lines_created"] for r in batch)
        total_skipped = sum(r["lines_skipped"] for r in batch)
        order_lines = []
        for r in batch:
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
        src = batch[0]["src_company"]
        company = batch[0]["company"]
        notify_success(
            title=f"POUL SO Import — {count} order{'s' if count != 1 else ''} created",
            detail=detail,
            context=f"company={company} src_company={src}",
        )


_batch = _BatchAccumulator()


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


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"POUL SO message decode failed: {e} — dropping poison pill")
        message.ack()
        return

    if data.get("type") != "poul-so-import":
        logger.warning(f"POUL SO worker: unexpected message type {data.get('type')!r} — dropping")
        message.ack()
        return

    header: dict = data.get("header", {})
    lines: list = data.get("lines", [])
    company_name_src: str = header.get("companyName", "")
    company: str = _resolve_bc_company(company_name_src)
    po_ref: str = header.get("poRefNumber", "unknown")

    try:
        # ── Customer lookup ──────────────────────────────────────────────────
        customer_name_raw: str = header.get("customerName") or ""
        customers = bc_client.fetch_customers(company)
        customer = next(
            (c for c in customers if c.get("name", "").upper() == customer_name_raw.upper()),
            None,
        )
        if not customer:
            raise ValueError(
                f"Customer not found in BC (company={company!r}): {customer_name_raw!r}"
            )
        customer_no: str = customer["customerNo"]

        # ── Item reference lookup ─────────────────────────────────────────────
        item_refs = bc_client.fetch_item_references(company)
        # Build map: referenceNo (upper) → itemNo
        ref_map: dict[str, str] = {
            r.get("referenceNo", "").upper(): r["itemNo"]
            for r in item_refs
            if r.get("itemNo") and r.get("referenceNo")
        }

        # ── Location code ─────────────────────────────────────────────────────
        location_code: str = config.POUL_SO_DEFAULT_LOCATION

        # ── Ship-To Address lookup (informational — resolved but not yet set) ─
        # Fetched here so it can be used for location resolution in future if needed.
        # ship_to_list = bc_client.fetch_ship_to_addresses(company, customer_no)

        # ── Create Sales Order header ─────────────────────────────────────────
        header_payload = _build_header_payload(header, customer_no, location_code)
        h_status, h_data = bc_client.v2_create_record("salesOrders", header_payload, company)
        if h_status not in (200, 201):
            raise ValueError(
                f"SO header create failed for PO {po_ref!r} (BC {h_status}): {h_data}"
            )

        order_id: str = h_data.get("id", "")
        order_no: str = h_data.get("number", order_id)

        # ── Create Sales Order lines ──────────────────────────────────────────
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
            f"POUL SO import done — PO {po_ref!r} → BC order {order_no!r} "
            f"(company={company!r} src={company_name_src!r} "
            f"lines_created={lines_created} lines_skipped={lines_skipped})"
        )
        _batch.add(
            po_ref=po_ref,
            order_no=order_no,
            customer_name=header.get("customerName", ""),
            branch_name=header.get("customerBranchName", ""),
            po_date=header.get("poDate", ""),
            delivery_date=header.get("deliveryDate", ""),
            lines_created=lines_created,
            lines_skipped=lines_skipped,
            company=company,
            src_company=company_name_src,
        )
        message.ack()

    except ValueError as e:
        logger.error(f"POUL SO import permanent failure — PO {po_ref!r}: {e}")
        notify_error(
            title=f"POUL SO Import Failed — {po_ref}",
            detail=str(e),
            context=f"company={company} src_company={company_name_src}",
        )
        message.ack()

    except Exception as e:
        err_str = str(e)
        logger.error(f"POUL SO import error — PO {po_ref!r}: {e}")
        if any(sig in err_str for sig in _TRANSIENT_SIGNALS):
            message.nack()
        else:
            notify_error(
                title=f"POUL SO Import Error — {po_ref}",
                detail=err_str,
                context=f"company={company} src_company={company_name_src}",
            )
            message.ack()


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the POUL SO import Pub/Sub subscriber and return the future."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_POUL_SO_SUBSCRIPTION,
    )
    # max_messages=3: each message triggers BC API calls; keep concurrency modest
    flow_control = pubsub_v1.types.FlowControl(max_messages=3)
    future = subscriber.subscribe(
        subscription_path,
        callback=_process,
        flow_control=flow_control,
    )
    logger.info(f"POUL SO import worker subscribed to {subscription_path}")
    return future

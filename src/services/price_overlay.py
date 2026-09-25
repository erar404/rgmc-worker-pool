"""Price-list overlay semantics — must stay identical to the BC API's request-time logic.

The BC API used to recompute "which price list is active on date X and what does it
say for product Y" on every catalog request by scanning the full price_list_items blob.
The worker now computes the same answer once per sync and bakes it into the GCS catalog
blobs, so the API only has to serve them.

Two accumulators exist because two consumers have historically used different rules:

* OverrideAccumulator — the API's rule (header must be Active + Sale + within its date
  window + non-IC; latest line startingDate wins, header priority breaks ties). This is
  what gets written into the catalog/family blobs.
* BestPriceAccumulator — the worker's Firestore backfill rule (any non-IC header, line
  startingDate <= on_date, latest wins). Kept unchanged for item_prices_{env}.
"""
from typing import Iterable

BC_NULL_DATE = "0001-01-01"

# Fields kept per price list line in the compact override index served to the API.
INDEX_FIELDS = ("unitPriceIncVAT", "unitPrice", "startingDate")


def is_ic_code(code: str) -> bool:
    code_upper = (code or "").upper()
    parts = code_upper.split("_")
    return code_upper.startswith("IC") or (len(parts) >= 2 and parts[1].startswith("IC"))


def _norm_date(value: str | None) -> str:
    d = (value or "").strip()[:10]
    return "" if d == BC_NULL_DATE else d


def active_price_list_codes(headers: Iterable[dict], on_date: str) -> list[str]:
    """Codes of Sale price lists active on on_date, most recently started first."""
    pairs: list[tuple[str, str]] = []
    for h in headers:
        if h.get("status") != "Active" or h.get("priceType") != "Sale":
            continue
        starting = _norm_date(h.get("startingDate"))
        ending = _norm_date(h.get("endingDate"))
        if starting and starting > on_date:
            continue
        if ending and ending < on_date:
            continue
        code = h.get("code")
        if code and not is_ic_code(code):
            pairs.append((code, starting))
    pairs.sort(key=lambda x: x[1], reverse=True)
    return [code for code, _ in pairs]


def _line_prices(line: dict) -> tuple[float | None, float | None]:
    incl = line.get("unitPriceIncVAT") or line.get("unitPrice") or line.get("unitAmount")
    if incl is None:
        return None, None
    excl = line.get("unitPrice") or line.get("unitAmount") or incl
    return incl, excl


class OverrideAccumulator:
    """API-semantics override map: assetNo -> {unitPrice, unitPriceIncVAT, priceListCode}."""

    def __init__(self, active_codes: list[str]):
        self._priority = {code: i for i, code in enumerate(active_codes)}
        self._best: dict[str, tuple[str, int, float, float, str]] = {}

    def add_lines(self, code: str, lines: Iterable[dict]) -> None:
        priority = self._priority.get(code)
        if priority is None:
            return
        for line in lines:
            if line.get("assetType", "Item") != "Item":
                continue
            asset_no = line.get("assetNo") or ""
            if not asset_no:
                continue
            incl, excl = _line_prices(line)
            if incl is None:
                continue
            line_date = _norm_date(line.get("startingDate"))
            current = self._best.get(asset_no)
            if (
                current is None
                or line_date > current[0]
                or (line_date == current[0] and priority < current[1])
            ):
                self._best[asset_no] = (line_date, priority, incl, excl, code)

    def as_map(self) -> dict[str, dict]:
        return {
            asset_no: {"unitPrice": excl, "unitPriceIncVAT": incl, "priceListCode": code}
            for asset_no, (_, _, incl, excl, code) in self._best.items()
        }


def compact_index_lines(lines: Iterable[dict]) -> dict[str, list]:
    """assetNo -> [unitPriceIncVAT, unitPrice, startingDate] for one price list."""
    out: dict[str, list] = {}
    for line in lines:
        if line.get("assetType", "Item") != "Item":
            continue
        asset_no = line.get("assetNo") or ""
        if not asset_no:
            continue
        incl, excl = _line_prices(line)
        if incl is None:
            continue
        out[asset_no] = [incl, excl, _norm_date(line.get("startingDate"))]
    return out


class BestPriceAccumulator:
    """Worker Firestore-backfill semantics (unchanged from backfill_item_prices_per_price_list)."""

    def __init__(self, on_date: str, price_list_code: str | None = None):
        self._on_date = on_date
        self._only_code = price_list_code
        self.best: dict[str, dict] = {}

    def add_header(self, header: dict, lines: Iterable[dict]) -> None:
        code = (header.get("code") or "").strip()
        if not code or is_ic_code(code):
            return
        if self._only_code and code != self._only_code:
            return
        for line in lines:
            asset_no = (line.get("assetNo") or "").strip().upper()
            if not asset_no:
                continue
            unit_price = line.get("unitPriceIncVAT") or line.get("unitPrice") or line.get("unitAmount")
            if unit_price is None:
                continue
            line_date = _norm_date(line.get("startingDate"))
            if line_date and line_date > self._on_date:
                continue
            current = self.best.get(asset_no)
            if current is None or line_date > current["startingDate"]:
                self.best[asset_no] = {
                    "unitPriceIncVAT": unit_price,
                    "priceListCode": code,
                    "startingDate": line_date,
                }

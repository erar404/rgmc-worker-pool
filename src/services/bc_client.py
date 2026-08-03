"""Business Central HTTP client for the worker pool.

Focused subset of the main API's bc_functions.py — contains only what background
workers need: auth, company lookup, paginated fetching, v3 catalog parallel fetch,
v2 price list headers, and order CRUD (v1 + v2).

No in-memory list caches or stale-while-revalidate logic — workers always fetch fresh.
"""
import datetime
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from src.config import (
    BC_AUTH_URL,
    BC_CLIENT_ID,
    BC_CLIENT_SECRET,
    BC_ENVIRONMENT,
    BC_SCOPE,
    BC_TENANT_ID,
)

logger = logging.getLogger("bc_client")

_BC_BASE = "https://api.businesscentral.dynamics.com/v2.0"
_RGMC_CUSTOM_API_V1 = "api/rgmc/rgmccustom/v1.0"
_RGMC_CUSTOM_API_V2 = "api/rgmc/rgmccustom/v2.0"
_RGMC_CUSTOM_API_V3 = "api/rgmc/rgmccustom/v3.0"

# Shared HTTP session with connection pooling
_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# BC auth token (cached for up to 59 min)
_token_lock = threading.Lock()
_token_cache: dict = {"token": None, "expires_at": 0.0}

# Company GUID cache (TTL = 10 min)
_company_id_cache: dict = {}
_companies_lock = threading.Lock()
_companies_cache: dict = {"value": None, "expires_at": 0.0}
_COMPANIES_TTL = 600

# Semaphore: one below BC's 5-concurrent-request cap.
# Workers are background-only so 3 slots are sufficient; keeps head-room for the main API.
_bc_semaphore = threading.Semaphore(3)
_active_bc_requests: int = 0
_active_bc_lock = threading.Lock()

# v3 field selection — matches the main API exactly
_V3_SELECT_FIELDS = (
    "productNo,description,unitPriceIncVAT,unitPrice,familyCode,"
    "itemId,itemCategoryCode,baseUnitOfMeasure,priceListCode,"
    "lastModifiedDateTime,blocked"
)
_V3_PREFER_HEADER = {"Prefer": "odata.maxpagesize=5000"}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token(force_refresh: bool = False) -> str:
    with _token_lock:
        if not force_refresh and time.time() < _token_cache["expires_at"] - 60:
            return _token_cache["token"]
        resp = _session.post(
            BC_AUTH_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": BC_CLIENT_ID,
                "client_secret": BC_CLIENT_SECRET,
                "scope": BC_SCOPE,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["token"] = data["access_token"]
        _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
        return _token_cache["token"]


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {get_access_token()}", "Accept": "application/json"}


# ---------------------------------------------------------------------------
# HTTP primitives
# ---------------------------------------------------------------------------

def _retry_after(response: requests.Response, fallback: int) -> int:
    raw = response.headers.get("Retry-After")
    try:
        return int(raw) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _bc_request(method: str, url: str, max_retries: int = 3, **kwargs) -> requests.Response:
    """Gate a BC HTTP call through _bc_semaphore with 429/5xx retry.

    Semaphore is held only while the HTTP request is in flight — released before each
    sleep so other workers are not starved during rate-limit hold-offs.
    """
    global _active_bc_requests
    kwargs.setdefault("timeout", 90)
    response = None
    for attempt in range(max_retries + 1):
        with _bc_semaphore:
            with _active_bc_lock:
                _active_bc_requests += 1
            try:
                response = getattr(_session, method)(url, **kwargs)
            finally:
                with _active_bc_lock:
                    _active_bc_requests -= 1
        if response.status_code not in (401, 429, 502, 503):
            return response
        if attempt == max_retries:
            break
        if response.status_code == 401:
            logger.warning(f"BC 401 (attempt {attempt + 1}). Refreshing token.")
            new_token = get_access_token(force_refresh=True)
            if "headers" in kwargs:
                kwargs["headers"] = {**kwargs["headers"], "Authorization": f"Bearer {new_token}"}
        elif response.status_code == 429:
            wait = _retry_after(response, min(2 ** attempt, 30))
            logger.warning(f"BC 429 (attempt {attempt + 1}). Waiting {wait}s.")
            time.sleep(wait)
        else:
            wait = min(2 ** attempt, 16)
            logger.warning(f"BC {response.status_code} (attempt {attempt + 1}). Waiting {wait}s.")
            time.sleep(wait)
    return response


def _fetch_all_pages(url: str, max_retries: int = 6, extra_headers: dict | None = None) -> list:
    """Follow OData @odata.nextLink pages and return the combined value list.

    409 (temp-buffer cursor invalidated by a concurrent request) restarts pagination
    from page 1 up to 3 times before giving up.
    """
    global _active_bc_requests
    _MAX_RESTARTS = 3
    all_records: list = []
    next_url = url
    restarts = 0
    while next_url:
        response = None
        should_restart = False
        for attempt in range(max_retries + 1):
            headers = {**_auth_headers(), **(extra_headers or {})}
            with _bc_semaphore:
                with _active_bc_lock:
                    _active_bc_requests += 1
                try:
                    response = _session.get(next_url, headers=headers, timeout=120)
                finally:
                    with _active_bc_lock:
                        _active_bc_requests -= 1
            if response.status_code == 409:
                if restarts < _MAX_RESTARTS:
                    restarts += 1
                    logger.warning(f"BC 409 conflict — restarting pagination ({restarts}/{_MAX_RESTARTS}): {url}")
                    all_records.clear()
                    next_url = url
                    time.sleep(1)
                    should_restart = True
                break
            if response.status_code not in (401, 429, 502, 503):
                break
            if attempt == max_retries:
                break
            if response.status_code == 401:
                logger.warning(f"BC 401 on GET (attempt {attempt + 1}). Refreshing token.")
                get_access_token(force_refresh=True)
            elif response.status_code == 429:
                wait = _retry_after(response, min(2 ** attempt, 60))
                logger.warning(f"BC 429 — waiting {wait}s (attempt {attempt + 1}).")
                time.sleep(wait)
            else:
                wait = min(2 ** attempt, 16)
                logger.warning(f"BC {response.status_code} — waiting {wait}s (attempt {attempt + 1}).")
                time.sleep(wait)
        if should_restart:
            continue
        response.raise_for_status()
        data = response.json()
        all_records.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
    return all_records


def _safe_json(response: requests.Response) -> Any:
    if not response.content:
        return {}
    try:
        return response.json()
    except Exception:
        return response.text


# ---------------------------------------------------------------------------
# Company lookup
# ---------------------------------------------------------------------------

def _fetch_companies() -> list:
    now = time.time()
    if _companies_cache["value"] is not None and now < _companies_cache["expires_at"]:
        return _companies_cache["value"]
    with _companies_lock:
        if _companies_cache["value"] is not None and time.time() < _companies_cache["expires_at"]:
            return _companies_cache["value"]
        url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/api/v2.0/companies"
        response = _bc_request("get", url, headers=_auth_headers())
        response.raise_for_status()
        companies = response.json().get("value", [])
        for c in companies:
            _company_id_cache[c.get("name", "").upper()] = c["id"]
        _companies_cache["value"] = companies
        _companies_cache["expires_at"] = time.time() + _COMPANIES_TTL
        return companies


def get_company_id(company_name: str) -> str:
    name = company_name.upper()
    if name in _company_id_cache:
        return _company_id_cache[name]
    _fetch_companies()
    if name in _company_id_cache:
        return _company_id_cache[name]
    raise ValueError(f"Company '{name}' not found in Business Central")


# ---------------------------------------------------------------------------
# v3 Item Price Catalog (Pag50318)
# ---------------------------------------------------------------------------

def _build_v3_url(
    company_id: str,
    on_date: str | None,
    odata_filter: str | None = None,
    since: str | None = None,
) -> str:
    filters = []
    if on_date:
        filters.append(f"onDate eq {on_date}")
    if since:
        filters.append(f"lastModifiedDateTime gt {since}")
    if odata_filter:
        filters.append(odata_filter)
    params = []
    if filters:
        params.append(f"$filter={' and '.join(filters)}")
    params.append(f"$select={_V3_SELECT_FIELDS}")
    url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V3}/companies({company_id})/itemPrices"
    return url + "?" + "&".join(params)


def _fetch_v3_catalog_for_date(
    company_id: str, company_name: str, effective_date: str, since: str | None = None
) -> list:
    """Run the 4-range parallel fetch for a specific effective_date."""
    ranges = [("", "G"), ("G", "M"), ("M", "S"), ("S", "")]

    def _fetch_range(low: str, high: str) -> list:
        parts = []
        if low:
            parts.append(f"productNo ge '{low}'")
        if high:
            parts.append(f"productNo lt '{high}'")
        odata_filter = " and ".join(parts) if parts else None
        url = _build_v3_url(company_id, effective_date, odata_filter, since=since)
        return _fetch_all_pages(url, extra_headers=_V3_PREFER_HEADER)

    logger.info(
        f"v3 catalog parallel fetch (4 ranges) — company={company_name!r} "
        f"on_date={effective_date!r} since={since!r}"
    )
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(_fetch_range, low, high) for low, high in ranges]
        results = [f.result() for f in as_completed(futures)]

    seen: set = set()
    merged: list = []
    for chunk in results:
        for record in chunk:
            key = record.get("productNo") or record.get("id")
            if key not in seen:
                seen.add(key)
                merged.append(record)

    logger.info(f"v3 catalog fetch complete ({effective_date!r}): {len(merged)} records for {company_name!r}")
    return merged


def _probe_v3_date(company_id: str, on_date: str) -> str | None:
    """Light probe: fetch up to 50 records for on_date and check if any are non-blocked.

    Returns 'active' (has non-blocked records), 'blocked' (all blocked), or None
    (BC returned an error or no records for this date — skip it).

    Uses a single cheap request instead of the full 4-range parallel fetch so that
    the fallback loop can cheaply scan many candidate dates without memory pressure.
    """
    url = (
        f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V3}"
        f"/companies({company_id})/itemPrices"
        f"?$filter=onDate eq {on_date}&$select=productNo,blocked&$top=50"
    )
    resp = _bc_request("get", url, headers=_auth_headers(), timeout=30)
    if not resp.ok:
        return None
    records = resp.json().get("value", [])
    if not records:
        return None
    return "active" if any(not r.get("blocked") for r in records) else "blocked"


def fetch_v3_catalog(company_name: str, on_date: str | None = None, since: str | None = None) -> list:
    """Fetch the v3 item price catalog using four parallel productNo range requests.

    Splits the alphabet into four ranges (A-G, G-M, M-S, S-Z) so BC runs four
    independent OData cursors simultaneously. Results are merged and de-duplicated on
    productNo.

    For full fetches (since=None): probes each candidate date with a minimal single
    request before committing to the full 4-range parallel fetch. Probe errors (4xx)
    skip the date; full-fetch errors also skip and try the next date. This handles
    expired price list end dates and transient BC range errors (e.g. 400 on M-S range
    for a specific date) without aborting the entire fallback loop.

    For incremental fetches (since set): fetches only records modified after that
    timestamp on the given date — no date fallback (partial delta, not a full re-sync).
    """
    company_id = get_company_id(company_name)
    start_date = datetime.date.fromisoformat(on_date) if on_date else datetime.date.today()

    if since is not None:
        # Incremental: just fetch the delta for the given date, no fallback needed
        return _fetch_v3_catalog_for_date(company_id, company_name, start_date.isoformat(), since=since)

    # Full fetch: probe each date cheaply before committing to the full 4-range parallel fetch
    for days_back in range(31):
        effective_date = (start_date - datetime.timedelta(days=days_back)).isoformat()

        probe = _probe_v3_date(company_id, effective_date)
        if probe is None:
            logger.warning(f"v3 catalog: date {effective_date!r} probe failed for {company_name!r} — skipping")
            continue
        if probe != "active":
            continue  # all-blocked on this date, try the next one silently

        try:
            records = _fetch_v3_catalog_for_date(company_id, company_name, effective_date)
        except Exception as e:
            logger.warning(
                f"v3 catalog: full fetch for {effective_date!r} failed ({e}) "
                f"for {company_name!r} — skipping"
            )
            continue

        if any(not r.get("blocked") for r in records):
            if days_back > 0:
                logger.info(
                    f"v3 catalog: no active prices on {start_date.isoformat()!r}; "
                    f"using {effective_date!r} ({days_back}d back) — {len(records)} records for {company_name!r}"
                )
            return records
        # Probe said active but full fetch had no non-blocked (probe sample was all-blocked by chance)
        logger.warning(
            f"v3 catalog: probe said active but full fetch all-blocked on {effective_date!r} "
            f"for {company_name!r} — trying earlier date"
        )

    logger.warning(
        f"v3 catalog fetch: no active prices in 30-day window for {company_name!r} "
        f"(checked {start_date.isoformat()!r} to {(start_date - datetime.timedelta(days=30)).isoformat()!r})"
    )
    return []


# ---------------------------------------------------------------------------
# v2 Price List Headers (Pag50320)
# ---------------------------------------------------------------------------

def fetch_price_list_headers(company_name: str, odata_filter: str | None = None) -> list:
    """Fetch all priceListHeaders from the v2.0 RGMC custom API (Pag50320).

    Small dataset (< 100 records) — always fetches fresh, no cache.
    """
    company_id = get_company_id(company_name)
    url = (
        f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V2}"
        f"/companies({company_id})/priceListHeaders"
    )
    if odata_filter:
        url += f"?$filter={odata_filter}"
    return _fetch_all_pages(url)


def fetch_price_list_headers_with_lines(company_name: str, since: str | None = None) -> list:
    """Fetch priceListHeaders with embedded priceListLines via OData $expand.

    Pass since (UTC ISO string) to fetch only headers modified after that timestamp.
    Lines are embedded in their parent header — so a header change brings all its
    current lines. If only a line changes without touching its header, pass since=None
    for a full sync to pick it up.
    """
    company_id = get_company_id(company_name)
    base = (
        f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V2}"
        f"/companies({company_id})/priceListHeaders"
    )
    if since:
        url = f"{base}?$expand=priceListLines&$filter=lastModifiedDateTime gt {since}"
    else:
        url = f"{base}?$expand=priceListLines"
    logger.info(f"fetch_price_list_headers_with_lines — company={company_name!r} since={since!r}")
    return _fetch_all_pages(url)


# ---------------------------------------------------------------------------
# Order CRUD — v1 (RGMC custom v1.0) and v2 (RGMC custom v2.0)
# ---------------------------------------------------------------------------

def v1_create_record(table_endpoint: str, payload: dict, company_name: str):
    company_id = get_company_id(company_name)
    url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V1}/companies({company_id})/{table_endpoint}"
    headers = {**_auth_headers(), "Content-Type": "application/json"}
    resp = _bc_request("post", url, json=payload, headers=headers)
    return resp.status_code, _safe_json(resp)


def v1_delete_record(table_endpoint: str, record_id: str, company_name: str):
    company_id = get_company_id(company_name)
    url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V1}/companies({company_id})/{table_endpoint}({record_id})"
    resp = _bc_request("delete", url, headers=_auth_headers())
    return resp.status_code


def v2_create_record(table_endpoint: str, payload: dict, company_name: str):
    company_id = get_company_id(company_name)
    url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V2}/companies({company_id})/{table_endpoint}"
    headers = {**_auth_headers(), "Content-Type": "application/json"}
    resp = _bc_request("post", url, json=payload, headers=headers)
    return resp.status_code, _safe_json(resp)


def v2_delete_record(table_endpoint: str, record_id: str, company_name: str):
    company_id = get_company_id(company_name)
    url = f"{_BC_BASE}/{BC_TENANT_ID}/{BC_ENVIRONMENT}/{_RGMC_CUSTOM_API_V2}/companies({company_id})/{table_endpoint}({record_id})"
    resp = _bc_request("delete", url, headers=_auth_headers())
    return resp.status_code

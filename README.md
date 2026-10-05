<div align="center">

# <span style="color:#A07320">RGMC Worker Pool</span>

<span style="color:#666">Background job runner for Business Central sync, order submission, and POUL sales-order import — no public endpoints, just Pub/Sub and a heartbeat.</span>

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Cloud Run](https://img.shields.io/badge/Cloud%20Run-Worker%20Pool-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/run)
[![Pub/Sub](https://img.shields.io/badge/Pub%2FSub-messaging-669DF6?logo=googlecloud&logoColor=white)](https://cloud.google.com/pubsub)
[![Firestore](https://img.shields.io/badge/Firestore-state-FFA000?logo=firebase&logoColor=white)](https://cloud.google.com/firestore)
[![BigQuery](https://img.shields.io/badge/BigQuery-analytics-4285F4?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)

</div>

---

## <span style="color:#A07320">📑 Table of Contents</span>

- [Overview](#overview-)
- [Tech Stack](#tech-stack-)
- [Features](#features-)
- [Message Types / "Routes"](#message-types--routes-)
- [Project Structure](#project-structure-)
- [Setup & Installation](#setup--installation-)
- [Environment Variables](#environment-variables-)
- [Running the App](#running-the-app-)
- [Building for Production](#building-for-production-)
- [Cloud Run Worker Pool Deployment](#cloud-run-worker-pool-deployment-)
- [API Endpoints Consumed](#api-endpoints-consumed-)
- [Data & Caching Strategy](#data--caching-strategy-)
- [Authentication Flow](#authentication-flow-)
- [Core Data Flow / Lifecycle](#core-data-flow--lifecycle-)
- [Brand / Design Tokens](#brand--design-tokens-)
- [License](#license-)

---

## <span style="color:#A07320">🧭 Overview</span>

**rgmc-worker-pool** is a Python background-processing service for RGMC Group's Microsoft Dynamics 365 Business Central (BC) integration. It runs as a **Google Cloud Run Worker Pool** (not a web service) and does three jobs:

1. **Order submission to BC** — consumes sales/return order tasks and creates them in Business Central via the RGMC custom API.
2. **Catalog & pricing sync** — pulls item prices, price lists, item ledger entries, customers, contacts, and item categories from BC and republishes them to Firestore and Cloud Storage for the main `rgmc-bc-api` service to serve.
3. **POUL Sales Order import** — ingests Suncoast Brands International Corp. (SBIC) purchase orders sourced from `CustomerPOUL`/`CustomerPOULDetail` (via BigQuery/Cloud SQL) and creates the corresponding BC sales orders, with a Firestore-backed retry buffer for anything that can't be resolved automatically.

Key design decisions:

- 💡 **No public HTTP surface.** The only inbound HTTP is a tiny health-check server for Cloud Run liveness/readiness probes. All real work arrives over **Google Cloud Pub/Sub**.
- **Pull, don't push.** Three independent `StreamingPullFuture` subscribers run concurrently in one process; each has its own flow-control concurrency limit tuned to the kind of work it does.
- **Fail loudly, retry safely.** Transient errors (`429`, `502`, `503`, timeouts) `nack()` so Pub/Sub redelivers; permanent errors `ack()` and are recorded (Firestore task status, the `so_buffer` retry collection, or an email alert) so nothing retries forever on bad data.
- **Firestore as the source of truth for sync state and buffering** — incremental syncs use a stored `lastSyncAt` watermark; failed POUL orders are buffered by PO reference and retried idempotently.

---

## <span style="color:#A07320">🧱 Tech Stack</span>

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.12 (slim image) |
| Compute | Google Cloud Run **Worker Pools** | — |
| Messaging | Google Cloud Pub/Sub (`google-cloud-pubsub`) | 2.21.1 |
| State / buffering | Google Cloud Firestore (`google-cloud-firestore`) | 2.20.1 |
| Blob storage | Google Cloud Storage (`google-cloud-storage`) | 2.19.0 |
| Analytics (legacy/disabled path) | Google BigQuery (`google-cloud-bigquery`) | 3.27.0 |
| HTTP client | `requests` | 2.32.3 |
| ERP | Microsoft Dynamics 365 Business Central (OAuth2 client-credentials) | RGMC custom API v1/v2/v3 |
| CI/CD | Google Cloud Build → Artifact Registry → Cloud Run Worker Pool | — |
| Container | Docker (`python:3.12-slim`) | — |

---

## <span style="color:#A07320">✨ Features</span>

### <span style="color:#2a9d8f">📦 Order Submission (`order_worker`)</span>

- Consumes `PUBSUB_ORDER_SUBSCRIPTION` with `max_messages=5` concurrency.
- Creates a BC sales order or sales return order header, then its lines, via v1 or v2 RGMC custom API depending on `api_version`.
- Retries a `409` on a line insert up to 4 times with linear backoff; if a line still fails, the whole order header is rolled back (deleted) and the task is marked `failed`.
- Writes live task status (`processing` / `done` / `failed`) to Firestore (`order_tasks_{env}`) so a caller can poll progress.
- Sends a developer email alert on permanent failure.

### <span style="color:#2a9d8f">🔄 Catalog & Price Sync (`sync_worker`)</span>

- Consumes `PUBSUB_SYNC_SUBSCRIPTION` with `max_messages=1` (large in-memory payloads, no concurrent syncs).
- Dispatches on a `type` field to one of ten message handlers (see [Message Types](#message-types--routes-) below).
- A full company sync runs 4 stages **in parallel** via `ThreadPoolExecutor`: price lists, v3 item-price catalog, item ledger entries, and supporting datasets (customers/contacts/item categories) — then bakes the price overlay into the GCS catalog blobs and patches Firestore with best-effective prices.
- Full item-price fetches are split into 27 parallel `productNo` letter-range requests (A–Z plus non-alpha) to avoid Business Central's OData temp-buffer `409` on large result sets; incremental deltas use a single `lastModifiedDateTime`-filtered request.
- Sends a success/failure developer email per sync run.

### <span style="color:#2a9d8f">🧾 POUL Sales Order Import (`so_import_worker`)</span>

- Consumes `PUBSUB_POUL_SO_SUBSCRIPTION` with `max_messages=1`.
- Handles 5 message shapes: fresh batch import, legacy single-order import, manual buffer reprocess, Cloud SQL line-backfill for already-created orders, and Cloud SQL header+line backfill for orders never inserted into BC.
- Resolves the customer's ship-to address by **BC ship-to code → SBIC lookup code → fuzzy name match** (`SequenceMatcher`, threshold 0.5), with manual reconcile-page overrides taking priority over all three.
- Resolves each line's item by SKU against BC item references, with the same override precedence.
- Handles a known source-data quirk where `poQty`/`poQtyPcs` can be swapped relative to the line's stated unit of measure (documented live in `_resolve_line_qty`).
- An order with zero resolvable lines or BC-rejected lines **still gets its header created** (no more all-or-nothing rollback) — unresolved lines are buffered in Firestore (`so_buffer_{env}`) for a future retry instead of being dropped.
- Sends one consolidated batch email per run, plus a separate "unmatched items" warning email when any order was inserted with skipped lines.
- Tracks manual reprocess/backfill runs in Firestore (`reprocess_runs_{env}`) so the `/reconcile` page (served by `rgmc-bc-api`) can poll run status.

### <span style="color:#2a9d8f">💓 Health Checks</span>

- Minimal stdlib `http.server` on `$PORT` (default `8080`) — `GET /`, `/healthz`, `/healthcheck` return `{"status": "ok", "service": "rgmc-worker-pool", "env": ..., "revision": ...}`.
- No other routes are exposed; 404 otherwise.

---

## <span style="color:#A07320">📨 Message Types / "Routes"</span>

This service has no HTTP routes — its "API" is the shape of the JSON payload on each Pub/Sub subscription.

```text
PUBSUB_ORDER_SUBSCRIPTION (order_worker)
  { "task_id", "order_type": "sales"|"return", "api_version": "v1"|"v2",
    "company", "header": {...}, "lines": [...] }
    → creates one BC sales/return order

PUBSUB_SYNC_SUBSCRIPTION (sync_worker)
  { "type": "routine-sync", "on_date"? }                         → full sync, all BC_COMPANIES
  { "type": "sync-item-prices", "company", "on_date"?, "page_size"? }
  { "type": "sync-price-list-headers", "company" }
  { "type": "sync-price-list-items", "company", "price_list_code"? }
  { "type": "sync-item-ledger-entries", "company", "since_date"? }
  { "type": "backfill-family-codes", "company"?, "on_date"? }
  { "type": "backfill-item-prices", "company"?, "on_date"?, "price_list_code"? }
  { "type": "backfill-ile-columns", "company"?, "since_date"? }
  { "type": "bq-sync-ile", ... }    → DISABLED 2026-09-22 (Airbyte owns this now); acked as a no-op
  { "type": "ping", "sent_at", "sent_by", "note"? }               → connectivity test

PUBSUB_POUL_SO_SUBSCRIPTION (so_import_worker)
  { "type": "poul-so-import-batch", "orders": [{ "header", "lines" }, ...] }
  { "type": "poul-so-import", "header", "lines" }                 → legacy single-order
  { "type": "poul-so-reprocess-buffer", "companies"?, "notify"?, "run_id"? }
  { "type": "poul-so-sync-from-cloudsql", "companies"?, "create_by"?, "run_id"? }
  { "type": "poul-so-backfill-from-cloudsql", "companies"?, "create_by"?, "date_from"?, "date_to"?, "run_id"? }
```

> 📌 All messages are published by `rgmc-bc-api` (the main API service) or by Cloud Scheduler (`routine-sync` on a cron). This worker pool never publishes to these topics itself — only to Firestore/GCS/BC/email.

---

## <span style="color:#A07320">🗂️ Project Structure</span>

```text
rgmc-worker-pool/
├── src/
│   ├── main.py                      # Entry point — starts health server + 3 Pub/Sub subscribers, handles SIGTERM/SIGINT
│   ├── config.py                    # All environment variables, resolved once at import time
│   ├── health.py                    # Minimal stdlib HTTP server for Cloud Run liveness/readiness probes
│   ├── logger.py                    # Shared stdout logger configuration
│   ├── workers/
│   │   ├── order_worker.py          # Pub/Sub consumer — BC sales/return order submission
│   │   ├── sync_worker.py           # Pub/Sub consumer — catalog/price/ILE sync dispatch (10 message types)
│   │   └── so_import_worker.py      # Pub/Sub consumer — POUL SO import, reprocess, Cloud SQL sync/backfill
│   └── services/
│       ├── bc_client.py             # Business Central HTTP client — OAuth2, pagination, v1/v2/v3 RGMC APIs
│       ├── gcp_api_client.py        # Read-only HTTP client for rgmc-gcp-api's Cloud SQL endpoints
│       ├── gcs_catalog.py           # Cloud Storage persistence — catalog/family blobs, price overrides, supporting datasets
│       ├── price_firestore_service.py  # Firestore persistence — item prices, price lists, ILE, sync state
│       ├── price_overlay.py         # Price-list overlay math shared with the main API (Override + BestPrice accumulators)
│       ├── bigquery_ile_service.py  # BigQuery ILE writer (load-to-staging + MERGE) — DISABLED, kept for rollback
│       ├── task_service.py          # Firestore task-status store for async order processing
│       ├── send_mail.py             # SMTP developer alert emails (error / success / warning templates)
│       ├── so_buffer.py             # Firestore retry buffer for failed POUL SO import orders
│       ├── so_buffer_overrides.py   # Read-only access to /reconcile page's manual SKU/branch/customer links
│       └── reprocess_status.py      # Firestore run-status tracking for manual reprocess/backfill triggers
├── Dockerfile                       # python:3.12-slim, no web framework — runs `python -m src.main`
├── cloudbuild.yaml                  # Cloud Build pipeline: build → push → deploy to Cloud Run Worker Pool
├── deploy.sh                        # Manual deploy script (production or --staging)
├── requirements.txt                 # 5 pinned GCP + requests dependencies
├── .env.example                     # Template for local environment variables
├── DEPLOY.md                        # Deployment runbook and infra notes
└── worker-pool-setup.md             # Setup notes (gitignored — contains project IDs/infra details)
```

---

## <span style="color:#A07320">⚙️ Setup & Installation</span>

### <span style="color:#555">Prerequisites</span>

- Python 3.12+
- A Google Cloud project with Pub/Sub, Firestore, Cloud Storage (and optionally BigQuery) enabled
- Business Central app registration with OAuth2 client-credentials access to the RGMC custom APIs
- `gcloud` CLI (for deployment)

### <span style="color:#555">Clone & Install</span>

```bash
git clone <repository-url>
cd rgmc-worker-pool
pip install -r requirements.txt
```

### <span style="color:#555">Configure environment</span>

```bash
cp .env.example .env
# then edit .env with real BC and GCP credentials
```

---

## <span style="color:#A07320">🔐 Environment Variables</span>

| Variable | File | Purpose |
|---|---|---|
| `BC_CLIENT_ID` | `.env` | Business Central app registration client ID |
| `BC_CLIENT_SECRET` | `.env` | Business Central app registration client secret |
| `BC_TENANT_ID` | `.env` | Azure AD tenant ID hosting the BC environment |
| `BC_SCOPE` | `.env` | OAuth2 scope — default `https://api.businesscentral.dynamics.com/.default` |
| `BC_ENVIRONMENT` | `.env` | BC environment name, e.g. `Production` or `UAT` |
| `BC_COMPANY` | `.env` | Default/fallback BC company code (e.g. `RGMC`) |
| `BC_COMPANIES` | `.env` | Comma-separated company list for `routine-sync`/`"ALL"` expansion |
| `GCP_PROJECT_ID` | `.env` | GCP project ID — used for Pub/Sub, Firestore clients |
| `GCP_ENV` | `.env` | `Production` or `Staging` — controls Firestore/GCS collection and path suffixes |
| `BIGQUERY_PROJECT_ID` | `.env` | GCP project ID for the (disabled) BigQuery ILE writer |
| `GCS_CATALOG_BUCKET` | `.env` | Cloud Storage bucket name for the catalog/family blobs |
| `PUBSUB_ORDER_SUBSCRIPTION` | `.env` | Subscription the order worker pulls from (default `rgmc-orders-worker-sub`) |
| `PUBSUB_SYNC_SUBSCRIPTION` | `.env` | Subscription the sync worker pulls from (default `rgmc-sync-worker-sub`) |
| `PUBSUB_POUL_SO_SUBSCRIPTION` | `src/config.py` default | Subscription the POUL SO worker pulls from (default `rgmc-poul-so-import-sub`) |
| `PUBSUB_ORDER_TOPIC` | `.env` | Order topic name, published to by `rgmc-bc-api` (default `rgmc-orders`) |
| `PUBSUB_SYNC_TOPIC` | `.env` | Sync topic name, published to by `rgmc-bc-api`/Cloud Scheduler (default `rgmc-sync`) |
| `POUL_SO_BC_COMPANY` | `src/config.py` default | Fallback BC company for POUL SO import when `_COMPANY_MAP` doesn't match |
| `POUL_SO_DEFAULT_LOCATION` | `src/config.py` default | Default BC location code applied to POUL SO headers/lines |
| `GCP_API_BASE` | `src/config.py` default | Base URL of `rgmc-gcp-api`, used for read-only Cloud SQL lookups |
| `DEVELOPER_EMAIL` | `.env` | Recipient for error/success/warning alert emails (blank disables email) |
| `SMTP_HOST` | `.env` | SMTP server host (default `smtp.gmail.com`) |
| `SMTP_PORT` | `.env` | SMTP server port (default `587`) |
| `SMTP_USER` | `.env` | SMTP auth username / From address |
| `SMTP_PASSWORD` | `.env` | SMTP auth password / app password |
| `API_TAG_VERSION` | Cloud Build / Cloud Run | Deployed version tag, injected by `cloudbuild.yaml` |
| `K_REVISION` | Cloud Run (automatic) | Cloud Run revision ID, used in health check and startup log |
| `PORT` | Cloud Run (automatic) | Port the health check server listens on (default `8080`) |

---

## <span style="color:#A07320">▶️ Running the App</span>

```bash
# Local run — requires .env populated with real BC/GCP credentials
# and Application Default Credentials for Google Cloud:
gcloud auth application-default login

python -m src.main
```

What happens on start:

1. Logs `Worker pool starting — env=... revision=... version=...`.
2. Starts the health HTTP server on `$PORT` (default `8080`) in a daemon thread.
3. Starts three Pub/Sub `StreamingPullFuture` subscribers: `order_worker`, `sync_worker`, `so_import_worker`.
4. Registers `SIGTERM`/`SIGINT` handlers that cancel all three futures for a clean shutdown.
5. Blocks on `stop.wait()` until a shutdown signal arrives.

```bash
# Manually publish a test message (requires gcloud + Pub/Sub permissions)
gcloud pubsub topics publish rgmc-sync --message='{"type":"ping","sent_at":"2026-10-05T00:00:00Z","sent_by":"manual-test"}'
```

---

## <span style="color:#A07320">🏗️ Building for Production</span>

```bash
# Build the image locally
docker build -t rgmc-worker-pool .

# Run it locally (mount GCP credentials, pass env vars)
docker run --rm -p 8080:8080 --env-file .env rgmc-worker-pool
```

The image is `python:3.12-slim` based, installs `requirements.txt`, copies only `src/`, and runs `python -m src.main` — there is no build step or static output directory since this is a long-running background process, not a compiled frontend.

---

## <span style="color:#A07320">☁️ Cloud Run Worker Pool Deployment</span>

Deployment target is a **Cloud Run Worker Pool** (`gcloud beta run worker-pools deploy`), not a standard Cloud Run service — it has no ingress URL.

### <span style="color:#555">Automated (Cloud Build)</span>

`cloudbuild.yaml` runs on push to `main` (or on a git tag, which sets `API_TAG_VERSION`):

```bash
# 1. docker build, tagged with $SHORT_SHA, latest, and $TAG_NAME (if present)
# 2. docker push --all-tags to Artifact Registry
# 3. gcloud beta run worker-pools deploy rgmc-worker-pool \
#      --image=<AR image>:$SHORT_SHA --region=... --service-account=...
```

Required Cloud Build trigger substitutions: `_REGION`, `_AR_REPO`, `_SA`.

### <span style="color:#555">Manual (`deploy.sh`)</span>

```bash
./deploy.sh              # deploy production worker pool: rgmc-worker-pool
./deploy.sh --staging    # deploy staging worker pool:    rgmc-worker-pool-staging
```

The script auto-resolves the Git commit SHA and tag/branch, prompts for confirmation, then submits a Cloud Build job against `cloudbuild.yaml`. Pushing to the `staging` branch auto-selects the staging worker pool even without `--staging`.

See `DEPLOY.md` for the full deployment runbook and `worker-pool-setup.md` (gitignored) for project-specific infra details.

---

## <span style="color:#A07320">🔌 API Endpoints Consumed</span>

This service calls out to two external HTTP APIs — it exposes none of its own beyond the health check.

### <span style="color:#555">Business Central (RGMC custom API — `bc_client.py`)</span>

| Method | Path | Description |
|---|---|---|
| POST | `{tenant}/{env}/api/v2.0/companies` (token) | OAuth2 client-credentials token request |
| GET | `.../api/v2.0/companies` | List all BC companies (cached 10 min) |
| GET | `.../rgmccustom/v3.0/companies({id})/itemPrices` | v3 item price catalog (27-range parallel fetch, or incremental) |
| GET | `.../rgmccustom/v2.0/companies({id})/itemLedgerEntries` | Item ledger entries (limit/offset paged) |
| GET | `.../rgmccustom/v2.0/companies({id})/priceListHeaders` | Price list headers (optionally `$expand=priceListLines`) |
| GET | `.../rgmccustom/v2.0/companies({id})/customers` | Full customer list |
| GET | `.../rgmccustom/v2.0/companies({id})/contacts` | Full contact list |
| GET | `.../api/v2.0/companies({id})/itemCategories` | Item categories (standard BC API) |
| GET | `.../api/v2.0/companies({id})/locations` | Locations (standard BC API) |
| GET | `.../rgmccustom/v2.0/companies({id})/shipToAddresses` | Ship-to addresses (custom Pag50350) |
| GET | `.../rgmccustom/v2.0/companies({id})/itemReferences` | Item/SKU cross-references (custom Pag50349) |
| POST | `.../rgmccustom/v1.0\|v2.0/companies({id})/{table}` | Create a record (orders, order lines) |
| GET | `.../rgmccustom/v2.0/companies({id})/salesOrders?$filter=...` | Find an existing sales order by number or external document no. |
| GET | `.../rgmccustom/v2.0/companies({id})/salesOrders({id})/salesOrderLines` | List an order's existing lines |
| DELETE | `.../rgmccustom/v1.0\|v2.0/companies({id})/{table}({id})` | Delete a record (order rollback) |

### <span style="color:#555">rgmc-gcp-api (`gcp_api_client.py` — read-only Cloud SQL bridge)</span>

| Method | Path | Description |
|---|---|---|
| GET | `/customerpoul` | `CustomerPOUL` rows by `create_by`, optional `date_from`/`date_to`/`company_id` |
| GET | `/customerpouldetail/bq/{po_ref}` | `CustomerPOULDetailBQ` line rows for one PO reference |

**Example POST payload (BC sales order header, v2):**

```json
{
  "sellToCustomerNo": "C00123",
  "externalDocumentNo": "PO-2026-00456",
  "orderDate": "2026-10-01",
  "postingDate": "2026-10-01",
  "shipmentDate": "2026-10-05",
  "shipToCode": "SHIP01",
  "locationCode": "WH01",
  "submittedBy": "SBIC AI Uploading"
}
```

---

## <span style="color:#A07320">💾 Data & Caching Strategy</span>

### <span style="color:#555">Firestore collections (all suffixed `_{env}`, e.g. `_production` / `_staging`)</span>

| Collection | Document ID | Purpose |
|---|---|---|
| `order_tasks_{env}` | `task_id` | Live status of an async order submission (`processing`/`done`/`failed`) |
| `item_prices_{env}` | `{company}_{productNo}` | Synced BC item prices, patched with best-effective price-list pricing |
| `price_list_headers_{env}` | `{company}_{code}` | Synced BC price list headers |
| `price_list_items_{env}` | `{company}_{priceListCode}_{lineNo}` | Synced BC price list lines |
| `item_ledger_entries_{env}` | `{company}_{entryNo}` | Synced BC item ledger entries |
| `sync_state_{env}` | `{company}_{collectionType}` | Watermark (`lastSyncAt`) for incremental sync per company/dataset |
| `so_buffer_{env}` | slugified `poRefNumber` | Retry buffer for POUL orders with unresolved lines/headers |
| `so_buffer_overrides_{env}` | (read-only) | Manual SKU/branch/customer links saved from the `/reconcile` page |
| `reprocess_runs_{env}` | `run_id` | Status of a manual reprocess/sync/backfill run, for `/reconcile` polling |

### <span style="color:#555">Cloud Storage layout (`{GCS_CATALOG_BUCKET}/{GCP_ENV}/{COMPANY}/`)</span>

| Blob | Refresh trigger | Purpose |
|---|---|---|
| `catalog.json` | Every sync | Full item price catalog, streamed to bound memory (~100k records) |
| `families/_index.json` | Every sync | Family code → record count index |
| `families/{FAMILY}.json` | Every sync (incremental per family when possible) | Slim, non-blocked records for one family, prices overlaid |
| `prices.json` | Every sync | `productNo → [price, priceListCode]`, used to detect and flag price changes |
| `price_overrides.json` | Every sync | Compact per-price-list line index, for historical-date requests |
| `price_list_headers.json` | Every sync | All price list headers |
| `customers.json` / `contacts.json` / `item_categories.json` | Every sync | Supporting datasets served by `rgmc-bc-api` |

All blobs are uploaded gzip-encoded; downstream readers get transparent decompression. Every write function is non-fatal — a GCS error is logged and swallowed rather than failing the sync.

### <span style="color:#555">BigQuery (disabled path)</span>

`bigquery_ile_service.py` implements a load-to-staging + `MERGE` upsert for item ledger entries, keyed on `(company, entryNo)`. **Disabled since 2026-09-22** — Airbyte's own `itemLedgerEntries` stream now writes directly to `bc_*_raw.itemLedgerEntries`, making this worker-pool path redundant. Code is kept importable for reference/rollback only; nothing calls it.

---

## <span style="color:#A07320">🔑 Authentication Flow</span>

1. The worker needs a Business Central access token before any API call. `bc_client.get_access_token()` checks an in-memory cache (`_token_cache`) first.
2. If the cached token is missing or expires within 60 seconds, it POSTs an OAuth2 **client-credentials** grant to `BC_AUTH_URL` (`https://login.microsoftonline.com/{BC_TENANT_ID}/oauth2/v2.0/token`) using `BC_CLIENT_ID` / `BC_CLIENT_SECRET` / `BC_SCOPE`.
3. The returned `access_token` and `expires_in` are cached under a thread lock so concurrent requests across all three workers share one token.
4. Every BC HTTP call goes through `_bc_request()` or `_fetch_all_pages()`, which gate access through a 4-slot semaphore (`_bc_semaphore`) to respect BC's own rate limits, and retry on `401` (refreshing the token), `429` (honoring `Retry-After`), and `502`/`503` (exponential backoff).
5. A `409` during OData pagination (BC's temp-buffer cursor invalidated by a concurrent request) restarts that fetch from page 1, up to 3 times.
6. The worker pool itself has **no inbound authentication** — it is not reachable except by Pub/Sub delivering to its subscriptions and Cloud Run's own internal health-check probes. There is no end-user login anywhere in this service.
7. GCP service access (Pub/Sub, Firestore, Cloud Storage, BigQuery) uses the Cloud Run Worker Pool's attached service account (`rgmc-worker-pool@{project}.iam.gserviceaccount.com`) via Application Default Credentials — no keys are embedded in the image.

---

## <span style="color:#A07320">🔁 Core Data Flow / Lifecycle</span>

### <span style="color:#555">1. Routine catalog sync (Cloud Scheduler → sync_worker)</span>

```text
Cloud Scheduler (cron)
      │  publishes {"type": "routine-sync"} → rgmc-sync topic
      ▼
sync_worker._process()
      │  expands BC_COMPANIES ("ALL" → BC's own company list)
      ▼
  for each company → _sync_company(company, on_date)
      │
      ├─ Stage 1  _stage_price_lists         ─┐
      ├─ Stage 2  _stage_v3_catalog           │  run concurrently via
      ├─ Stage 3  _stage_ile                  │  ThreadPoolExecutor(max_workers=4)
      └─ Stage 4  _stage_supporting_datasets ─┘
      │
      ▼
  Stage 5  _assemble_catalog()  → bakes Stage 1's price overlay into Stage 2's
            records → rebuilds GCS family blobs, catalog.json, prices.json, search_index.json
      │
      ▼
  Stage 6  apply_best_prices_to_firestore() → patches item_prices_{env} with
            the best-effective price per product
      │
      ▼
  notify_success() email + message.ack()
```

### <span style="color:#555">2. BC order submission (rgmc-bc-api → order_worker)</span>

```text
rgmc-bc-api
      │  publishes {task_id, order_type, header, lines} → rgmc-orders topic
      ▼
order_worker._process()
      │  update_task(task_id, status="processing")
      ▼
  v1/v2_create_record("salesOrders", header)  → order_id
      │
      ▼
  for each line: v1/v2_create_record(.../salesOrderLines)
      │  on 409: retry up to 4x
      │  on other failure: delete the header (full rollback), raise
      ▼
  update_task(task_id, status="done", result) + message.ack()
      (permanent failure → status="failed" + notify_error email, still ack'd)
```

### <span style="color:#555">3. POUL Sales Order import (BigQuery bridge → so_import_worker)</span>

```text
rgmc-gcp-api's BigQuery bridge
      │  inserts CustomerPOUL/CustomerPOULDetailBQ rows, then
      │  publishes {"type": "poul-so-import-batch", "orders": [...]} → POUL SO topic
      ▼
so_import_worker._run_batch()
      │  pre-fetches ship-to addresses + item references ONCE for the whole batch
      │  merges fresh orders with anything already buffered for this company
      ▼
  for each order → _create_order()
      │  1. resolve customer via reconcile override → ship-to code → lookup code → fuzzy name
      │  2. create (or resume) the BC sales order header — created even with 0 resolvable lines
      │  3. pre-validate lines against item references; resolve quantity/UOM quirks
      │  4. create each resolvable line (retry on 409)
      │  5. unresolved/rejected lines → so_buffer.save_failed_order() for later retry
      ▼
  one consolidated batch email (successes + failures)
  + a separate "unmatched items" warning email when applicable
      ▼
  message.ack()  (or nack() only on a transient pre-fetch failure)
```

---

## <span style="color:#A07320">🎨 Brand / Design Tokens</span>

This is a headless background service with no UI of its own. The colors below are used only in this README and in the HTML email templates sent by `send_mail.py`.

| Token | Hex | Use |
|---|---|---|
| Alert — error | `#c0392b` | Email header bar for `notify_error` |
| Alert — success | `#27ae60` | Email header bar for `notify_success` |
| Alert — warning | `#e67e22` | Email header bar for `notify_warning` |
| Email body text | `#333333` | Default email body copy |
| Email meta label | `#777777` | Timestamp/title/context labels and footer |
| Email panel background | `#f8f8f8` | Detail/summary `<pre>` block background |
| Email page background | `#f4f4f4` | Outer email wrapper background |

---

## <span style="color:#A07320">📄 License</span>

Private and proprietary — internal RGMC Group software. Not licensed for external use, distribution, or redistribution.

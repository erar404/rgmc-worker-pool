import os

__version__ = os.getenv("API_TAG_VERSION", "0.1.0")

# BC OAuth2
BC_CLIENT_ID = os.getenv("BC_CLIENT_ID", "")
BC_CLIENT_SECRET = os.getenv("BC_CLIENT_SECRET", "")
BC_TENANT_ID = os.getenv("BC_TENANT_ID", "")
BC_SCOPE = os.getenv("BC_SCOPE", "https://api.businesscentral.dynamics.com/.default")
BC_AUTH_URL = os.getenv(
    "BC_AUTH_URL",
    f"https://login.microsoftonline.com/{os.getenv('BC_TENANT_ID', '')}/oauth2/v2.0/token",
)
BC_ENVIRONMENT = os.getenv("BC_ENVIRONMENT", "UAT")
BC_COMPANY = os.getenv("BC_COMPANY", "RGMC")
BC_COMPANIES = os.getenv("BC_COMPANIES", "")

# GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_ENV = os.getenv("GCP_ENV", "Staging")      # "Production" or "Staging"

# BigQuery
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "")
# Used only by the ported BigQuery bridge (src/services/bigquery_bridge.py) for its
# customerpoul header/detail read -- the RA/onlinesalespo paths resolve their own
# dataset from BIGQUERY_DATASET_MAPPINGS-equivalent values in src/mappings_bridge.py.
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "sbic_int")
BQ_TABLE_VERSION = os.getenv("BQ_TABLE_VERSION", "v1")
BQ_READ_TIMEOUT_SECONDS = float(os.getenv("BQ_READ_TIMEOUT_SECONDS", "60"))

# MSSQL (Cloud SQL for SQL Server), via the Cloud SQL Python Connector + pytds --
# used only by the ported BigQuery bridge (src/services/bigquery_bridge.py /
# src/db/dbconn.py) to write CustomerPOULBQ/CustomerRABQ staging tables directly, the
# same tables rgmc-gcp-api's own copy of this code writes to. Empty-string defaults
# only -- never put a real value here; it's set as a Cloud Run env var instead.
MSSQL_INSTANCE = os.getenv("MSSQL_INSTANCE", "")
MSSQL_USER = os.getenv("MSSQL_USER", "")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "sbic_prod")

# Cloud Storage
GCS_CATALOG_BUCKET = os.getenv("GCS_CATALOG_BUCKET", "")

# Pub/Sub subscriptions (pull — worker pool consumes these)
PUBSUB_ORDER_SUBSCRIPTION = os.getenv("PUBSUB_ORDER_SUBSCRIPTION", "rgmc-orders-worker-sub")
PUBSUB_SYNC_SUBSCRIPTION = os.getenv("PUBSUB_SYNC_SUBSCRIPTION", "rgmc-sync-worker-sub")
PUBSUB_POUL_SO_SUBSCRIPTION = os.getenv("PUBSUB_POUL_SO_SUBSCRIPTION", "rgmc-poul-so-import-sub")
# BigQuery-bridge run requests, published by rgmc-gcp-api's src/services/bridge_trigger.py
# instead of running BigqueryBridge.main() inline on that service's own request path --
# see src/workers/bigquery_bridge_worker.py and rgmc-gcp-api's handoff.md ("Move
# bigquery_bridge heavy work to worker pool").
PUBSUB_BRIDGE_SUBSCRIPTION = os.getenv("PUBSUB_BRIDGE_SUBSCRIPTION", "rgmc-bridge-run-sub")

# Pub/Sub topics (for publishing; used by the main API and Cloud Scheduler)
PUBSUB_ORDER_TOPIC = os.getenv("PUBSUB_ORDER_TOPIC", "rgmc-orders")
PUBSUB_SYNC_TOPIC = os.getenv("PUBSUB_SYNC_TOPIC", "rgmc-sync")
# Published by src/services/bigquery_bridge.py (ported from rgmc-gcp-api) after it
# inserts new customerpoul data for a SUNCOAST/SBIC company -- so_import_worker.py
# already subscribes to this same topic via PUBSUB_POUL_SO_SUBSCRIPTION above.
PUBSUB_POUL_SO_TOPIC = os.getenv("PUBSUB_POUL_SO_TOPIC", "rgmc-poul-so-import")

# POUL Sales Order import
POUL_SO_BC_COMPANY = os.getenv("POUL_SO_BC_COMPANY", "")
POUL_SO_DEFAULT_LOCATION = os.getenv("POUL_SO_DEFAULT_LOCATION", "")

# rgmc-gcp-api — read-only Cloud SQL access (CustomerPOUL/CustomerPOULDetail), used by
# the poul-so-sync-from-cloudsql handler to re-derive lines for an order already
# inserted in BC. Worker pool has no direct MSSQL client; it reads via gcp-api's HTTP API.
GCP_API_BASE = os.getenv("GCP_API_BASE", "https://rgmc-gcp-api-935246372408.asia-southeast1.run.app")

revision_code = os.environ.get("K_REVISION", "00001")

# Error notification email (leave blank to disable)
developer_email = os.getenv("DEVELOPER_EMAIL", "")
smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
smtp_port = int(os.getenv("SMTP_PORT", "587"))
smtp_user = os.getenv("SMTP_USER", "")
smtp_password = os.getenv("SMTP_PASSWORD", "")

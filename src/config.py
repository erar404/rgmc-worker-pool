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

# Cloud Storage
GCS_CATALOG_BUCKET = os.getenv("GCS_CATALOG_BUCKET", "")

# Pub/Sub subscriptions (pull — worker pool consumes these)
PUBSUB_ORDER_SUBSCRIPTION = os.getenv("PUBSUB_ORDER_SUBSCRIPTION", "rgmc-orders-worker-sub")
PUBSUB_SYNC_SUBSCRIPTION = os.getenv("PUBSUB_SYNC_SUBSCRIPTION", "rgmc-sync-worker-sub")

# Pub/Sub topics (for publishing; used by the main API and Cloud Scheduler)
PUBSUB_ORDER_TOPIC = os.getenv("PUBSUB_ORDER_TOPIC", "rgmc-orders")
PUBSUB_SYNC_TOPIC = os.getenv("PUBSUB_SYNC_TOPIC", "rgmc-sync")

revision_code = os.environ.get("K_REVISION", "00001")

# Error notification email (leave blank to disable)
developer_email = os.getenv("DEVELOPER_EMAIL", "")
smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
smtp_port = int(os.getenv("SMTP_PORT", "587"))
smtp_user = os.getenv("SMTP_USER", "")
smtp_password = os.getenv("SMTP_PASSWORD", "")

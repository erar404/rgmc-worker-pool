# RGMC BC Worker Pool — Deployment Guide

## Architecture

```
Cloud Scheduler ──publish──▶ rgmc-sync topic ──▶ rgmc-sync-worker-sub
Main API        ──publish──▶ rgmc-orders topic ──▶ rgmc-orders-worker-sub
                                                         │
                                              Cloud Run Worker Pool
                                              (Pub/Sub pull consumer)
                                                         │
                                              ┌──────────┼──────────┐
                                         BC Orders   GCS Catalog  Firestore
```

The worker pool **pulls** messages from two Pub/Sub subscriptions and processes them
in parallel subscriber threads. No inbound HTTP. A health server on port 8080 responds
to Cloud Run liveness probes only.

---

## Part 1 — Artifact Registry Setup (one time)

### 1.1 Enable Required APIs

```bash
PROJECT="your-project-id"

gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  firestore.googleapis.com \
  storage.googleapis.com \
  --project=$PROJECT
```

### 1.2 Create Artifact Registry Repository

```bash
PROJECT="your-project-id"
REGION="us-central1"

gcloud artifacts repositories create rgmc-docker \
  --repository-format=docker \
  --location=$REGION \
  --description="RGMC Docker images" \
  --project=$PROJECT
```

### 1.3 Create Service Account

```bash
PROJECT="your-project-id"
SA="rgmc-worker-pool@$PROJECT.iam.gserviceaccount.com"

gcloud iam service-accounts create rgmc-worker-pool \
  --display-name="RGMC BC Worker Pool" \
  --project=$PROJECT

# Firestore read/write
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/datastore.user"

# GCS catalog bucket read/write
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/storage.objectAdmin"

# Pub/Sub subscriber (pull messages)
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/pubsub.subscriber"

# Pub/Sub viewer (list subscriptions/topics)
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/pubsub.viewer"
```

---

## Part 2 — Pub/Sub Setup (one time)

### 2.1 Create Topics

```bash
PROJECT="your-project-id"

# Order submission messages (published by the main API)
gcloud pubsub topics create rgmc-orders --project=$PROJECT

# Sync trigger messages (published by Cloud Scheduler and/or the main API)
gcloud pubsub topics create rgmc-sync --project=$PROJECT
```

### 2.2 Create Pull Subscriptions

```bash
PROJECT="your-project-id"

# Order subscription — ack deadline 300s (BC order can take up to 5 min with retries)
gcloud pubsub subscriptions create rgmc-orders-worker-sub \
  --topic=rgmc-orders \
  --ack-deadline=300 \
  --message-retention-duration=7d \
  --expiration-period=never \
  --project=$PROJECT

# Sync subscription — ack deadline 1800s (full catalog sync across companies can be long)
gcloud pubsub subscriptions create rgmc-sync-worker-sub \
  --topic=rgmc-sync \
  --ack-deadline=1800 \
  --message-retention-duration=7d \
  --expiration-period=never \
  --project=$PROJECT
```

### 2.3 Grant the Main API SA Pub/Sub Publisher Role

The main API needs to publish order messages to the `rgmc-orders` topic:

```bash
PROJECT="your-project-id"
MAIN_API_SA="rgmc-bc-api@$PROJECT.iam.gserviceaccount.com"   # adjust to your main API SA

gcloud pubsub topics add-iam-policy-binding rgmc-orders \
  --member="serviceAccount:$MAIN_API_SA" \
  --role="roles/pubsub.publisher" \
  --project=$PROJECT

gcloud pubsub topics add-iam-policy-binding rgmc-sync \
  --member="serviceAccount:$MAIN_API_SA" \
  --role="roles/pubsub.publisher" \
  --project=$PROJECT
```

---

## Part 3 — Build & Push the Docker Image

### Option A — Cloud Build (no local Docker required)

From the `rgmc-worker-pool` directory:

```bash
PROJECT="your-project-id"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/rgmc-docker/rgmc-worker-pool"
VERSION="1.0.0"   # or use git tag

gcloud builds submit . \
  --tag=$IMAGE:$VERSION \
  --project=$PROJECT
```

Cloud Build also pushes a `:latest` tag automatically.

### Option B — Local Docker

```bash
PROJECT="your-project-id"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/rgmc-docker/rgmc-worker-pool"
VERSION="1.0.0"

gcloud auth configure-docker $REGION-docker.pkg.dev --project=$PROJECT

docker build \
  -t $IMAGE:$VERSION \
  -t $IMAGE:latest \
  .

docker push --all-tags $IMAGE
```

### Option C — Cloud Build CI/CD trigger (automated)

The included `cloudbuild.yaml` builds, pushes, and deploys automatically.

Set up a Cloud Build trigger:

```bash
PROJECT="your-project-id"
REGION="us-central1"

gcloud builds triggers create github \
  --name=rgmc-worker-pool-deploy \
  --repo-name=rgmc-worker-pool \
  --repo-owner=YOUR_GITHUB_ORG \
  --branch-pattern='^main$' \
  --build-config=cloudbuild.yaml \
  --substitutions=_REGION=$REGION \
  --region=$REGION \
  --project=$PROJECT
```

After setup, every push to `main` builds and deploys automatically.
Tag a commit (`git tag v1.0.0 && git push --tags`) to set the `API_TAG_VERSION` env var.

---

## Part 4 — Deploy to Cloud Run Worker Pool

```bash
PROJECT="your-project-id"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/rgmc-docker/rgmc-worker-pool:latest"
SA="rgmc-worker-pool@$PROJECT.iam.gserviceaccount.com"

gcloud run worker-pools deploy rgmc-worker-pool \
  --image=$IMAGE \
  --region=$REGION \
  --service-account=$SA \
  --min-instances=1 \
  --max-instances=5 \
  --set-env-vars="^|^BC_CLIENT_ID=YOUR_BC_CLIENT_ID\
|BC_CLIENT_SECRET=YOUR_BC_CLIENT_SECRET\
|BC_TENANT_ID=YOUR_BC_TENANT_ID\
|BC_SCOPE=https://api.businesscentral.dynamics.com/.default\
|BC_ENVIRONMENT=Production\
|BC_COMPANY=RGMC\
|BC_COMPANIES=RGMC,CGI\
|GCP_PROJECT_ID=$PROJECT\
|GCP_ENV=Production\
|GCS_CATALOG_BUCKET=rgmc-bc-catalog-$PROJECT\
|PUBSUB_ORDER_SUBSCRIPTION=rgmc-orders-worker-sub\
|PUBSUB_SYNC_SUBSCRIPTION=rgmc-sync-worker-sub\
|PUBSUB_ORDER_TOPIC=rgmc-orders\
|PUBSUB_SYNC_TOPIC=rgmc-sync\
|DEVELOPER_EMAIL=it.arellanoerwin@gmail.com\
|SMTP_HOST=smtp.gmail.com\
|SMTP_PORT=587\
|SMTP_USER=YOUR_GMAIL_ADDRESS\
|SMTP_PASSWORD=YOUR_GMAIL_APP_PASSWORD" \
  --project=$PROJECT
```

> Worker Pools do not have a public URL. There is no `--allow-unauthenticated` flag.
> The health server on port 8080 is used for Cloud Run's internal liveness probe only.

---

## Part 5 — Cloud Scheduler (Routine Sync)

Cloud Scheduler publishes directly to the `rgmc-sync` Pub/Sub topic. No HTTP target needed.

```bash
PROJECT="your-project-id"
SA="rgmc-worker-pool@$PROJECT.iam.gserviceaccount.com"

# Publish role for the Scheduler SA on the sync topic
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format="value(projectNumber)")
SCHEDULER_SA="service-$PROJECT_NUMBER@gcp-sa-cloudscheduler.iam.gserviceaccount.com"

gcloud pubsub topics add-iam-policy-binding rgmc-sync \
  --member="serviceAccount:$SCHEDULER_SA" \
  --role="roles/pubsub.publisher" \
  --project=$PROJECT

# Routine sync — all companies, nightly at 2 AM Manila time
gcloud scheduler jobs create pubsub rgmc-routine-sync \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --time-zone="Asia/Manila" \
  --topic=rgmc-sync \
  --message-body='{"type": "routine-sync"}' \
  --project=$PROJECT
```

To trigger a one-off sync manually:

```bash
# Full routine sync (all BC_COMPANIES)
gcloud pubsub topics publish rgmc-sync \
  --message='{"type": "routine-sync"}' \
  --project=$PROJECT

# Single company item prices
gcloud pubsub topics publish rgmc-sync \
  --message='{"type": "sync-item-prices", "company": "RGMC"}' \
  --project=$PROJECT

# Single company price list headers
gcloud pubsub topics publish rgmc-sync \
  --message='{"type": "sync-price-list-headers", "company": "RGMC"}' \
  --project=$PROJECT
```

---

## Part 6 — Update the Main API to Publish to Pub/Sub

Replace the Cloud Tasks order-enqueue call in the main API with a Pub/Sub publish:

```python
from google.cloud import pubsub_v1
import json, uuid

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(config.GCP_PROJECT_ID, "rgmc-orders")

task_id = str(uuid.uuid4())
payload = {
    "task_id": task_id,
    "order_type": order_type,
    "api_version": api_version,
    "company": company,
    "header": header,
    "lines": lines,
}
publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
```

Add `google-cloud-pubsub` to the main API's `requirements.txt`.

---

## Part 7 — Verify the Deployment

```bash
PROJECT="your-project-id"

# Check worker pool status
gcloud run worker-pools describe rgmc-worker-pool \
  --region=us-central1 \
  --project=$PROJECT

# Watch Cloud Logging for subscriber output
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="rgmc-worker-pool"' \
  --limit=50 \
  --format="table(timestamp,textPayload)" \
  --project=$PROJECT

# Publish a test sync message and watch logs
gcloud pubsub topics publish rgmc-sync \
  --message='{"type": "sync-price-list-headers", "company": "RGMC"}' \
  --project=$PROJECT

# Check Pub/Sub subscription backlog (should drain to 0 after processing)
gcloud pubsub subscriptions describe rgmc-sync-worker-sub --project=$PROJECT
```

---

## Environment Variables Reference

| Variable | Description |
|---|---|
| `BC_CLIENT_ID` | Azure AD app client ID |
| `BC_CLIENT_SECRET` | Azure AD app client secret |
| `BC_TENANT_ID` | Azure AD tenant ID |
| `BC_SCOPE` | BC OAuth2 scope |
| `BC_ENVIRONMENT` | BC environment (`Production` / `UAT`) |
| `BC_COMPANY` | Default BC company |
| `BC_COMPANIES` | Comma-separated list for routine sync |
| `GCP_PROJECT_ID` | GCP project ID |
| `GCP_ENV` | Firestore collection suffix (`Production` / `Staging`) |
| `GCS_CATALOG_BUCKET` | GCS bucket for catalog JSON |
| `PUBSUB_ORDER_SUBSCRIPTION` | Pull subscription for order messages |
| `PUBSUB_SYNC_SUBSCRIPTION` | Pull subscription for sync messages |
| `PUBSUB_ORDER_TOPIC` | Topic name for order messages (published by main API) |
| `PUBSUB_SYNC_TOPIC` | Topic name for sync messages (published by Cloud Scheduler) |
| `DEVELOPER_EMAIL` | Error alert recipient (blank to disable) |
| `SMTP_HOST` | SMTP server |
| `SMTP_PORT` | SMTP port |
| `SMTP_USER` | Gmail sender address |
| `SMTP_PASSWORD` | Gmail App Password |

---

## Firestore Collections

| Collection | Document ID pattern |
|---|---|
| `item_prices_Production` | `{COMPANY}_{productNo}` |
| `item_prices_Staging` | `{COMPANY}_{productNo}` |
| `price_list_headers_Production` | `{COMPANY}_{code}` |
| `price_list_headers_Staging` | `{COMPANY}_{code}` |
| `tasks` | `{task_id}` — order task status written by order worker |

---

## Redeployment

```bash
PROJECT="your-project-id"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/rgmc-docker/rgmc-worker-pool"
VERSION="1.1.0"

gcloud builds submit . --tag=$IMAGE:$VERSION --project=$PROJECT

gcloud run worker-pools deploy rgmc-worker-pool \
  --image=$IMAGE:$VERSION \
  --region=$REGION \
  --project=$PROJECT
```

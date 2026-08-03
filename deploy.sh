#!/usr/bin/env bash
# Manual Cloud Build deploy for rgmc-worker-pool.
# Usage:
#   ./deploy.sh              # deploy production (rgmc-worker-pool)
#   ./deploy.sh --staging    # deploy staging   (rgmc-worker-pool-staging)
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT="durable-woods-465907-n1"
REGION="asia-southeast1"

# ── Resolve git context ───────────────────────────────────────────────────────
SHORT_SHA=$(git rev-parse --short HEAD)
TAG_NAME=$(git tag --points-at HEAD 2>/dev/null | tail -1)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
TAG_NAME="${TAG_NAME:-$BRANCH}"

# ── Parse args ────────────────────────────────────────────────────────────────
WORKER_POOL="rgmc-worker-pool"
for arg in "$@"; do
  if [[ "$arg" == "--staging" ]]; then
    WORKER_POOL="rgmc-worker-pool-staging"
  fi
done

# Auto-select staging when on the staging branch
if [[ "$BRANCH" == "staging" && "$WORKER_POOL" == "rgmc-worker-pool" ]]; then
  WORKER_POOL="rgmc-worker-pool-staging"
fi

# ── Confirm ───────────────────────────────────────────────────────────────────
echo ""
echo "  Project:     $PROJECT"
echo "  Region:      $REGION"
echo "  Worker Pool: $WORKER_POOL"
echo "  Commit:      $SHORT_SHA"
echo "  Tag/branch:  $TAG_NAME"
echo ""
read -rp "Deploy? [y/N] " confirm
if [[ "${confirm,,}" != "y" ]]; then
  echo "Aborted."
  exit 0
fi

# ── Submit Cloud Build ────────────────────────────────────────────────────────
AR_IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/rgmc-docker/rgmc-worker-pool"
SA="rgmc-worker-pool@${PROJECT}.iam.gserviceaccount.com"

echo ""
echo "Submitting Cloud Build..."
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --substitutions="SHORT_SHA=${SHORT_SHA},TAG_NAME=${TAG_NAME},_REGION=${REGION},_WORKER_POOL=${WORKER_POOL},_AR_IMAGE=${AR_IMAGE},_SA=${SA}" \
  --project="${PROJECT}"

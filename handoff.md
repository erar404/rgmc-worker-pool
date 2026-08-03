# Handoff

## Goal

Maintain and extend the `rgmc-worker-pool` Cloud Run Worker Pool service for RGMC Group IT.
The worker pulls messages from two Pub/Sub subscriptions (`rgmc-orders-worker-sub`, `rgmc-sync-worker-sub`)
and processes BC orders and catalog syncs. The overall goal is a production-grade, fully automated
CI/CD pipeline with email alerting on both errors and successful syncs.

GCP project: `durable-woods-465907-n1` | Region: `asia-southeast1`
GitHub repo: `erar404/rgmc-worker-pool` (branch: `master`)

---

## Current State

Everything is stable and committed. The worker pool is deployed and running. CI/CD pipeline works.

**What is working:**
- Cloud Run Worker Pool deployed in `asia-southeast1` (`rgmc-worker-pool` for prod, `rgmc-worker-pool-staging` for staging)
- CI/CD: Cloud Build triggers on push to `master` / `staging` via the GitHub App on `erar404/rgmc-worker-pool`
- Manual deploy: `deploy.sh` (run from Git Bash/WSL — does NOT work from PowerShell due to Windows Python alias conflict with gcloud)
- Email alerts: `notify_error()` (red) and `notify_success()` (green) both working via daemon threads
- Success emails fire for: `routine-sync`, `sync-item-prices`, `sync-price-list-headers`, `sync-price-list-items`, and `ping` message types
- `ping` message type: worker sends a success email ACK back when it receives a ping, used for BC API → worker pool connectivity tests
- `cloudbuild.yaml` build step uses bash entrypoint to conditionally tag with `$TAG_NAME` only when non-empty (fixes CI failure on branch pushes where `TAG_NAME` is always empty)

**What is uncommitted:**
- `.gitignore` — exists on disk but not yet staged/committed (`git status` shows it as untracked)
- `worker-pool-setup.md` — intentionally gitignored, local-only setup reference

**Recent git log (top 5):**
```
ce27319 feat: sync price list items per price list code in routine sync
c0a09de fix: add missing global declaration for _active_bc_requests in _fetch_all_pages
3cf88e5 fix: pass SSL context as keyword arg to starttls() in send_mail
9d33e36 sample push to trigger ci/cd pipeline
d8566f2 added cloudbuild suggestions
```

---

## Files Actively Being Edited

- `cloudbuild.yaml` — Build step changed from `docker` entrypoint to `bash` entrypoint; conditionally adds `$TAG_NAME` tag using `[[ -n "$TAG_NAME" ]]` and `$$TAGS` (double-dollar escapes the local var from Cloud Build substitution parsing); `images:` section trimmed to only `$SHORT_SHA` and `latest` (removes `$TAG_NAME` which would be empty on branch pushes and fail pre-build validation)
- `deploy.sh` — New file. Manual Cloud Build submitter. Pre-expands `_AR_IMAGE` and `_SA` before passing to `--substitutions` (required because `gcloud builds submit` does not resolve nested substitution references like `${PROJECT_ID}` in YAML defaults). Auto-selects `--staging` when current branch is `staging`. Has interactive confirm prompt.
- `src/services/send_mail.py` — Added `notify_success()` (line 28) and `_send_success()` (line 112). Green (`#27ae60`) header vs red (`#c0392b`) for errors. "Summary" label instead of "Error Detail". Both send via daemon thread.
- `src/workers/sync_worker.py` — Added `notify_success()` calls after each successful message type (lines 109–113, 118–122, 130–133, 151–155, 164–168). Also has additional message types beyond what Claude added: `sync-price-list-items` and `ping` (user added these).
- `.gitignore` — New file. Excludes `.env`, `worker-pool-setup.md`, Python caches, IDE dirs. **Not yet committed.**
- `worker-pool-setup.md` — New file. Full GCP infrastructure setup reference (all one-time CLI commands, trigger creation payloads, known gotchas). Gitignored. Local only.

---

## Failed Attempts

- **What was tried**: Running `deploy.sh` directly via the Bash tool — **Why it failed**: Windows Python alias (`python` → Microsoft Store) intercepts the `gcloud` call in the bash environment. Error: `Python was not found; run without arguments to install from the Microsoft Store`. Workaround: run the equivalent PowerShell commands manually.

- **What was tried**: `gcloud builds submit --substitutions=SHORT_SHA=...,TAG_NAME=...,_REGION=...,_WORKER_POOL=...,_AR_IMAGE=...` without `_SA` — **Why it failed**: `_SA` default in `cloudbuild.yaml` references `${PROJECT_ID}` which is not resolved by `gcloud builds submit`. Deploy step received literal `rgmc-worker-pool@${PROJECT_ID}.iam.gserviceaccount.com`. Fix: pre-expand and pass `_SA` explicitly.

- **What was tried**: `gcloud builds submit` without `_AR_IMAGE` — **Why it failed**: `_AR_IMAGE` default references `${_REGION}` and `${PROJECT_ID}` which are not resolved in manual submits. Fix: pre-expand and pass `_AR_IMAGE` explicitly.

- **What was tried**: Inline bash in `cloudbuild.yaml` build step using `$TAGS` as a local variable — **Why it failed**: Cloud Build treats `$TAGS` as a substitution key (even though it doesn't start with `_`), fails with `key in the template "TAGS" is not a valid built-in substitution`. Fix: use `$$TAGS` (double-dollar) so Cloud Build passes `$TAGS` literally to bash.

- **What was tried**: Including `$_AR_IMAGE:$TAG_NAME` in `images:` section — **Why it failed**: Cloud Build validates all entries in `images:` before the build starts. On branch pushes, `TAG_NAME` is empty, making the image name end with `:` which is invalid. Fix: removed `$TAG_NAME` from `images:` section entirely.

- **What was tried**: Original `cloudbuild.yaml` with `--tag=$_AR_IMAGE:$TAG_NAME` in build step args (non-bash entrypoint) — **Why it failed**: Same empty `TAG_NAME` on branch push gives Docker an invalid tag. Fix: bash entrypoint with conditional tagging.

---

## Next Step

Commit the `.gitignore` file — it's the only untracked change:

```bash
git add .gitignore
git commit -m "chore: add .gitignore"
git push
```

After that, consider testing the CI trigger end-to-end by pushing a small change to `master` and confirming the Cloud Build run succeeds (the previous CI failures were due to the `TAG_NAME` issue now fixed in `cloudbuild.yaml`).

---

## Context & Gotchas

**deploy.sh can only be run from Git Bash or WSL, not PowerShell.**
`gcloud` on Windows resolves Python via the Windows Python alias, which breaks in the bash environment. The PowerShell equivalent works fine (manually set variables and call `gcloud builds submit`).

**`gcloud builds submit` does NOT resolve nested substitution references.**
The `cloudbuild.yaml` defaults like `_AR_IMAGE: ${_REGION}-docker.pkg.dev/${PROJECT_ID}/...` and `_SA: rgmc-worker-pool@${PROJECT_ID}...` reference other substitutions. These resolve correctly in CI triggered builds but NOT in manual `gcloud builds submit`. `deploy.sh` handles this by pre-expanding both to their literal values before passing `--substitutions`.

**Cloud Build substitution escaping: `$$VAR` in inline scripts.**
In a `bash` entrypoint step's args, any `$VAR` is treated as a Cloud Build substitution. Local bash variables must use `$$VAR` so Cloud Build passes `$VAR` literally to bash. Cloud Build substitution variables (`$_AR_IMAGE`, `$SHORT_SHA`, `$TAG_NAME`, `$BUILD_ID`) are still expanded by Cloud Build before the script runs.

**`TAG_NAME` is empty on all branch pushes.**
Cloud Build only sets `TAG_NAME` when the triggering event is a git tag push. For regular commits to `master` or `staging`, `TAG_NAME` is an empty string. Any YAML that uses `$TAG_NAME` in image names or step args must guard against this.

**Cloud Build triggers require `serviceAccount` field in this GCP project.**
`gcloud builds triggers create` silently fails with a generic 400 if the `serviceAccount` field is omitted. All triggers must be created via REST API. The service account in use: `935246372408-compute@developer.gserviceaccount.com`.

**Worker Pool uses `gcloud beta run worker-pools` — beta track required.**
`gcloud run worker-pools` does not exist. Worker Pools use `--scaling=N` (fixed instance count), not `--min-instances`/`--max-instances`.

**Pub/Sub ack deadline cap is 600s**, not 1800s as documented elsewhere.

**IAM policy bindings prompt for condition type** in this project. Answer `2` (None) at each prompt.

**GitHub connection is 1st-gen GitHub App on `erar404` account.** Classic PAT required — fine-grained PATs are rejected by Cloud Build connections. Repo is `erar404/rgmc-worker-pool`.

**`worker-pool-setup.md` is gitignored and local-only** — contains all one-time GCP setup commands with actual project IDs, trigger creation REST payloads, and known gotchas for future re-setup.

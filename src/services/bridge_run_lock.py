"""Per-group_code lock gating the BigQuery bridge's AUTOMATED (Eventarc-triggered,
method="triggered") runs only -- see bigquery_bridge_worker.py. A manual "Run Bridge
Now" trigger always bypasses this; only the automated path is gated, since that's the
one a burst of upstream BigQuery events can re-fire repeatedly within minutes of each
other -- confirmed live 2026-10-28: a string of back-to-back triggered runs for
group_code=customerpoul, one of which hit a transient BigQuery fetch error and raised
a hard-error alert for every run in the burst.

Collection: bridge_run_lock_{env}, one document per group_code.
"""
import logging
from datetime import datetime, timedelta, timezone

import src.config as config

logger = logging.getLogger("bridge_run_lock")

_db = None
_COLLECTION = f"bridge_run_lock_{config.GCP_ENV.lower()}"
COOLDOWN = timedelta(minutes=5)


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def try_start(group_code: str) -> bool:
    """True (and marks group_code "running") if no automated run is currently in
    progress for it and the last one didn't start within COOLDOWN (5 minutes) --
    False, with no state change, if either is true, meaning the caller should skip
    this trigger entirely rather than run again.

    Best-effort and fails OPEN: a Firestore read/write failure here must never
    permanently block a real bridge run, so it's treated as "no lock" and returns
    True on any error.
    """
    try:
        doc_ref = _client().collection(_COLLECTION).document(group_code)
        data = doc_ref.get().to_dict() or {}
        now = datetime.now(timezone.utc)
        if data.get("status") == "running":
            logger.info(f"bridge_run_lock: {group_code!r} already running — skipping this trigger")
            return False
        started_at = data.get("started_at")
        if started_at and (now - started_at) < COOLDOWN:
            logger.info(f"bridge_run_lock: {group_code!r} last started {now - started_at} ago (< {COOLDOWN}) — skipping this trigger")
            return False
        doc_ref.set({"status": "running", "started_at": now}, merge=True)
        return True
    except Exception as e:
        logger.warning(f"bridge_run_lock: check failed for {group_code!r} ({e}) — proceeding as if clear")
        return True


def finish(group_code: str) -> None:
    """Marks group_code idle again once its run completes, success or failure --
    started_at is left in place so the cooldown above still applies to the next
    trigger. Never raises."""
    try:
        _client().collection(_COLLECTION).document(group_code).set(
            {"status": "idle", "finished_at": datetime.now(timezone.utc)}, merge=True,
        )
    except Exception as e:
        logger.warning(f"bridge_run_lock: failed to clear lock for {group_code!r}: {e}")

"""Append-only log of every BigQuery-bridge run (automated or manual), one doc per
run — written by bigquery_bridge_worker.py right after each run finishes.

Distinct from bridge_run_lock_{env} (current state only, one doc per group_code,
overwritten on every run): this is what lets the Monitoring page's Trigger Status
modal show "previous triggers and their status" as a history, not just whether one
is running right now.

Collection: bridge_run_history_{env}, one doc per run (auto-ID, append-only).
"""
import logging
from datetime import datetime

import src.config as config

logger = logging.getLogger("bridge_run_history")

_db = None
_COLLECTION = f"bridge_run_history_{config.GCP_ENV.lower()}"


def _client():
    global _db
    if _db is None:
        from google.cloud import firestore
        _db = firestore.Client(project=config.GCP_PROJECT_ID)
    return _db


def record_run(
    group_code: str,
    method: str,
    status: str,
    summary: dict | None,
    error: str | None,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Best-effort — a Firestore write failure here must never fail the actual bridge
    run, so any error is logged and swallowed, same convention as bridge_run_lock.py.
    """
    try:
        _client().collection(_COLLECTION).add({
            "group_code": group_code,
            "method": method,
            "status": status,
            "summary": summary or {},
            "error": error,
            "started_at": started_at,
            "finished_at": finished_at,
        })
    except Exception as e:
        logger.warning(f"bridge_run_history: failed to record run for {group_code!r}: {e}")

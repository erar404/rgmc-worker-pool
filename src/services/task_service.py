"""Firestore task-status store for async order processing."""
import logging

from google.cloud import firestore

from src.config import GCP_ENV, GCP_PROJECT_ID

logger = logging.getLogger("task_service")

_db: firestore.Client | None = None


def _collection() -> str:
    env = (GCP_ENV or "staging").lower().replace(" ", "_")
    return f"order_tasks_{env}"


def _firestore() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=GCP_PROJECT_ID)
    return _db


def get_task(task_id: str) -> dict | None:
    doc = _firestore().collection(_collection()).document(task_id).get()
    return doc.to_dict() if doc.exists else None


def update_task(task_id: str, **fields):
    _firestore().collection(_collection()).document(task_id).set(fields, merge=True)

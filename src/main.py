"""RGMC BC Worker Pool — Cloud Run Worker Pool entry point.

Pulls messages from two Pub/Sub subscriptions:
  • PUBSUB_ORDER_SUBSCRIPTION  →  order submission to Business Central
  • PUBSUB_SYNC_SUBSCRIPTION   →  Firestore catalog/price sync

A lightweight HTTP server on PORT handles Cloud Run liveness/readiness probes.
No FastAPI. No public endpoints. No inbound HTTP beyond health checks.
"""
import os
import signal
import threading

from src import config
from src.logger import logger
from src.health import start as start_health_server
from src.workers import order_worker, sync_worker


def main() -> None:
    logger.info(
        f"Worker pool starting — "
        f"env={config.GCP_ENV} revision={config.revision_code} version={config.__version__}"
    )

    port = int(os.getenv("PORT", "8080"))
    start_health_server(port)

    order_future = order_worker.start()
    sync_future = sync_worker.start()

    stop = threading.Event()

    def _shutdown(signum, frame) -> None:
        logger.info("Worker pool shutting down — cancelling Pub/Sub subscriptions")
        order_future.cancel()
        sync_future.cancel()
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Worker pool ready")
    stop.wait()
    logger.info("Worker pool stopped")


if __name__ == "__main__":
    main()

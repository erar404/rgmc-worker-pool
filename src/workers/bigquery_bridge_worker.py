"""Pub/Sub consumer for BigQuery-bridge run requests.

Subscribes to PUBSUB_BRIDGE_SUBSCRIPTION. Each message is published by rgmc-gcp-api
(src/services/bridge_trigger.py) instead of running BigqueryBridge.main() inline on
that service's own request path -- covers BOTH the automated Eventarc-triggered path
(/handoff/runbridge/, group_code defaults to "customerpoul") and the three manual
"Run Bridge Now" buttons on sbic-manual-trigger-page's /trigger page
(/customerpoul/runbridge/[/onlinesalespo/], /customerra/runbridge/). See rgmc-gcp-api's
handoff.md ("Move bigquery_bridge heavy work to worker pool") for the full rationale.

Message format (JSON):
  { "group_code": "customerpoul" | "customerra" | "onlinesalespo", "method": "manual" | "triggered" }

flow_control=FlowControl(max_messages=1) processes one bridge run at a time across
ALL group_codes -- genuine mutual exclusion (not the approximate time-based rate
limiter rgmc-gcp-api's /handoff/runbridge/ still keeps as defense-in-depth against a
misbehaving trigger spamming publishes), so two bursty events for the same or
different group_code can never race on MSSQL concurrently the way this project's
"8 concurrent /handoff/runbridge/ calls" incident did.

bridge_run_lock additionally gates method="triggered" messages specifically: even
serialized one-at-a-time, a string of back-to-back automated runs for the same
group_code (upstream BigQuery events trickling in over several minutes) still meant
repeated re-processing and a notification for each -- see bridge_run_lock.py.
"""
import json
import logging

from google.cloud import pubsub_v1

from src import config
from src.logger import logger as app_logger
from src.services import bridge_run_lock
from src.services.bigquery_bridge import BigqueryBridge
from src.services.send_mail import notify_error

logger = logging.getLogger("worker.bigquery_bridge")


def _process(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        logger.error(f"Bridge-run message decode failed: {e} — acking to drop poison pill")
        notify_error(module="bigquery_bridge",
            title="BigQuery Bridge — message decode failed (dropped)",
            detail=str(e),
            context=f"raw_data={message.data!r}"[:2000],
        )
        message.ack()
        return

    group_code = data.get("group_code", "customerpoul")
    method = data.get("method", "manual")

    # Automated (Eventarc-triggered) runs only — a manual "Run Bridge Now" always goes
    # through. Blocks a new triggered run while one for this group_code is still in
    # progress, and for COOLDOWN (5 min) after the last one started — see
    # bridge_run_lock.py for why. The debounce-collapsed message this skips is dropped
    # outright (ack), not redelivered: the next genuinely new upstream event schedules
    # its own later run anyway, and BigQuery's watermark means nothing is lost by
    # waiting for it.
    if method == "triggered" and not bridge_run_lock.try_start(group_code):
        message.ack()
        return

    try:
        bridge = BigqueryBridge(app_logger, method=method, group_code=group_code)
        result = bridge.main()
        logger.info(f"Bridge run complete (group_code={group_code!r}, method={method!r}): {result}")
        message.ack()
    except Exception as e:
        logger.error(f"Bridge run failed (group_code={group_code!r}, method={method!r}): {e}")
        # BigqueryBridge.main() already self-alerts (notify_error/success(module=
        # "bigquery_bridge")) for errors it anticipates (see bigquery_bridge.py's
        # __notify) — but anything raised before that internal handling kicks in
        # (e.g. the constructor's DB connection, or an unanticipated step in main())
        # previously reached only this log line, never an email. This is a safety
        # net, not a replacement — it may duplicate an alert main() already sent for
        # errors it does handle internally; a missed alert is worse than a double one.
        notify_error(module="bigquery_bridge",
            title=f"BigQuery Bridge — hard error (group_code={group_code!r}, method={method!r})",
            detail=str(e),
            context=f"group_code={group_code} method={method}",
        )
        message.nack()  # let Pub/Sub redeliver for a transient BigQuery/MSSQL error
    finally:
        if method == "triggered":
            bridge_run_lock.finish(group_code)


def start() -> pubsub_v1.subscriber.futures.StreamingPullFuture:
    """Start the BigQuery-bridge Pub/Sub subscriber and return the future for
    lifecycle management."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_BRIDGE_SUBSCRIPTION,
    )
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    future = subscriber.subscribe(subscription_path, callback=_process, flow_control=flow_control)
    logger.info(f"BigQuery bridge worker subscribed to {subscription_path}")
    return future

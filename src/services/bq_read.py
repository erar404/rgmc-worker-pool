"""BigQuery reads with an enforced wall-clock timeout.

Ported from rgmc-gcp-api's src/services/bq_read.py for the BigQuery bridge worker (see
src/services/bigquery_bridge.py). pandas_gbq.read_gbq has no timeout parameter of its
own, and google-api-core's default retry policy for transient errors has a 600-second
deadline baked in -- without this, a flaky BigQuery connection could tie up this
worker's single message slot (flow_control=FlowControl(max_messages=1)) for up to 10
minutes.

read_gbq_with_timeout runs the read in a worker thread and bounds how long the calling
request waits via Future.result(timeout=...), regardless of what pandas_gbq/
google-api-core are doing internally. This is a wall-clock cutoff on the CALLING code's
wait, not a cancellation of the underlying HTTP call -- the background read may keep
running briefly after this raises, but the caller fails fast and predictably instead.
"""
import concurrent.futures

import pandas_gbq

from src import config

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="bq-read")


def read_gbq_with_timeout(*args, timeout: float = None, **kwargs):
    """Same call shape as pandas_gbq.read_gbq, but raises TimeoutError instead of
    blocking past `timeout` seconds (defaults to config.BQ_READ_TIMEOUT_SECONDS)."""
    timeout = config.BQ_READ_TIMEOUT_SECONDS if timeout is None else timeout
    future = _executor.submit(pandas_gbq.read_gbq, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"BigQuery read timed out after {timeout:.0f}s") from None

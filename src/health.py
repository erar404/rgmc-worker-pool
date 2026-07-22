"""Minimal HTTP health server for Cloud Run Worker Pool liveness probes."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from src import config
from src.logger import logger


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/healthz", "/healthcheck"):
            body = json.dumps({
                "status": "ok",
                "service": "rgmc-worker-pool",
                "env": config.GCP_ENV,
                "revision": config.revision_code,
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass  # suppress per-request access logs


def start(port: int = 8080) -> None:
    server = HTTPServer(("", port), _Handler)
    threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="health-server",
    ).start()
    logger.info(f"Health server listening on port {port}")

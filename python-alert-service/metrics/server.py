"""Lightweight Prometheus-compatible metrics server."""

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger("alert_service")


class MetricsCollector:
    """Thread-safe pipeline metrics using a lock."""

    def __init__(self, pipeline_mode: str, worker_count: int):
        self.pipeline_mode = pipeline_mode
        self.worker_count = worker_count
        self._lock = threading.Lock()
        self._events_received = 0
        self._events_processed = 0
        self._alerts_triggered = 0
        self._processing_micros_sum = 0
        self._processing_micros_count = 0

    def inc_received(self):
        with self._lock:
            self._events_received += 1

    def inc_processed(self):
        with self._lock:
            self._events_processed += 1

    def inc_triggered(self):
        with self._lock:
            self._alerts_triggered += 1

    def record_processing_duration(self, start: float):
        """Record elapsed time since start (time.monotonic() value) in microseconds."""
        microseconds = (time.monotonic() - start) * 1_000_000
        with self._lock:
            self._processing_micros_sum += microseconds
            self._processing_micros_count += 1

    def snapshot(self) -> str:
        """Return metrics in Prometheus text exposition format."""
        with self._lock:
            received = self._events_received
            processed = self._events_processed
            triggered = self._alerts_triggered
            dur_sum = self._processing_micros_sum
            dur_count = self._processing_micros_count

        avg_microseconds = dur_sum / dur_count if dur_count > 0 else 0.0

        lines = [
            "# HELP events_received_total Total sensor.updated events consumed from RabbitMQ.",
            "# TYPE events_received_total counter",
            f"events_received_total {received}",
            "",
            "# HELP events_processed_total Total events that completed evaluation.",
            "# TYPE events_processed_total counter",
            f"events_processed_total {processed}",
            "",
            "# HELP alerts_triggered_total Total alerts triggered by threshold crossings.",
            "# TYPE alerts_triggered_total counter",
            f"alerts_triggered_total {triggered}",
            "",
            "# HELP event_processing_avg_microseconds Average event processing duration in microseconds.",
            "# TYPE event_processing_avg_microseconds gauge",
            f"event_processing_avg_microseconds {avg_microseconds:.1f}",
            "",
            "# HELP pipeline_info Pipeline configuration.",
            "# TYPE pipeline_info gauge",
            f'pipeline_info{{mode="{self.pipeline_mode}",worker_count="{self.worker_count}"}} 1',
        ]
        return "\n".join(lines) + "\n"


# Module-level collector; set from main.py before serve() is called.
collector: MetricsCollector | None = None


def _make_handler():
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics" and collector is not None:
                body = collector.snapshot().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            # Silence per-request access logs.
            pass

    return MetricsHandler


def serve(addr: str = ":9090"):
    """Start the metrics HTTP server in a daemon thread."""
    host, _, port_str = addr.rpartition(":")
    host = host or "0.0.0.0"
    port = int(port_str)

    server = HTTPServer((host, port), _make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Metrics server starting", extra={"addr": addr})

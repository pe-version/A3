"""Pipeline metrics — thread-safe counters served as Prometheus text on :9090."""

from metrics.server import MetricsCollector, serve

__all__ = ["MetricsCollector", "serve"]

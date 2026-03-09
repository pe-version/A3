"""Logging middleware with correlation ID support."""

import json
import logging
import sys
import time
import uuid
from contextvars import ContextVar
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from config import get_settings

# Context variable for correlation ID - accessible throughout the request lifecycle
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Get the current correlation ID."""
    return correlation_id_var.get("")


class CorrelationIdFilter(logging.Filter):
    """Logging filter that adds correlation_id to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get("-")
        return True


def setup_logging() -> logging.Logger:
    """Configure structured JSON logging."""
    settings = get_settings()

    logger = logging.getLogger("sensor_service")
    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler(sys.stdout)

    if settings.log_format.lower() == "json":
        # JSON formatter
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                log_obj = {
                    "timestamp": self.formatTime(record, self.datefmt),
                    "level": record.levelname,
                    "correlation_id": getattr(record, "correlation_id", "-"),
                    "message": record.getMessage(),
                }
                # Add extra fields if present
                if hasattr(record, "method"):
                    log_obj["method"] = record.method
                if hasattr(record, "path"):
                    log_obj["path"] = record.path
                if hasattr(record, "status"):
                    log_obj["status"] = record.status
                if hasattr(record, "duration_ms"):
                    log_obj["duration_ms"] = record.duration_ms
                return json.dumps(log_obj)

        handler.setFormatter(JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S"))
    else:
        # Plain text formatter
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - [%(correlation_id)s] - %(message)s"
            )
        )

    handler.addFilter(CorrelationIdFilter())
    logger.addHandler(handler)

    return logger


# Initialize logger
logger = setup_logging()


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware that adds correlation IDs and logs requests."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get or generate correlation ID
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        correlation_id_var.set(correlation_id)

        # Log request start
        start_time = time.time()

        # Process request
        response = await call_next(request)

        # Calculate duration
        duration_ms = round((time.time() - start_time) * 1000, 2)

        # Log request completion
        logger.info(
            "Request completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": duration_ms,
            },
        )

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        return response

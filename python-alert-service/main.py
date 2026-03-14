"""
IoT Alert Service - Python (FastAPI)

A RESTful API for managing alert rules and triggered alerts,
with RabbitMQ consumer for sensor update events and circuit
breaker for resilient inter-service communication.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from clients.sensor_client import SensorClient
from config import get_settings
from database import init_database
from messaging.consumer import AlertConsumer
from metrics import server as metrics_server
from metrics.server import MetricsCollector, serve as serve_metrics
from middleware.logging import LoggingMiddleware
from routers import alerts_router, health_router, rules_router
from services.alert_evaluator import AlertEvaluator


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database, sensor client, and RabbitMQ consumer on startup."""
    settings = get_settings()

    # Initialize database schema and seed data
    init_database()

    # Start metrics server on :9090
    metrics_server.collector = MetricsCollector(settings.pipeline_mode, settings.worker_count)
    serve_metrics(":9090")

    # Create sensor client with circuit breaker
    app.state.sensor_client = SensorClient(
        base_url=settings.sensor_service_url,
        api_token=settings.api_token,
        cb_fail_max=settings.cb_fail_max,
        cb_reset_timeout=settings.cb_reset_timeout,
    )

    # Start RabbitMQ consumer for sensor events
    evaluator = AlertEvaluator(settings.database_path)
    worker_count = settings.worker_count if settings.pipeline_mode == "async" else 0
    consumer = AlertConsumer(settings.rabbitmq_url, evaluator.evaluate, worker_count=worker_count)
    consumer.start()

    yield


app = FastAPI(
    title="IoT Alert Service",
    description="Alert rules and triggered alerts for IoT sensor monitoring",
    version="1.0.0",
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(LoggingMiddleware)

# Include routers
app.include_router(health_router)
app.include_router(rules_router)
app.include_router(alerts_router)

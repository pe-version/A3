"""
IoT Sensor Service - Python (FastAPI)

A RESTful API for managing IoT sensor devices with SQLite persistence
and Bearer token authentication.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import get_settings
from database import init_database
from messaging.publisher import EventPublisher
from middleware.logging import LoggingMiddleware
from routers import health_router, sensors_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and RabbitMQ publisher on startup."""
    init_database()

    # Create and connect event publisher for sensor update events
    settings = get_settings()
    publisher = EventPublisher(settings.rabbitmq_url)
    publisher.connect()
    app.state.publisher = publisher

    yield


app = FastAPI(
    title="IoT Sensor Service",
    description="RESTful API for managing IoT sensor devices in a smart home ecosystem",
    version="1.0.0",
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(LoggingMiddleware)

# Include routers
app.include_router(health_router)
app.include_router(sensors_router)

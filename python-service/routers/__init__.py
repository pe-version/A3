"""API routers for the sensor service."""

from routers.health import router as health_router
from routers.sensors import router as sensors_router

__all__ = ["health_router", "sensors_router"]

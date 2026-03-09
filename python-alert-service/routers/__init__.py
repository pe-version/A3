"""API routers for the alert service."""

from routers.alerts import router as alerts_router
from routers.health import router as health_router
from routers.rules import router as rules_router

__all__ = ["health_router", "rules_router", "alerts_router"]

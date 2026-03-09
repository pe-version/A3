"""Health check endpoint."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """
    Health check endpoint.

    Returns the service status and identifier.
    Unauthenticated by design for load balancer/orchestrator probes.
    """
    return {"status": "ok", "service": "python"}

"""Triggered alert endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status

from middleware.auth import verify_token
from models.triggered_alert import TriggeredAlert, TriggeredAlertList, TriggeredAlertUpdate
from repositories.triggered_alert_repository import (
    TriggeredAlertRepository,
    get_triggered_alert_repository,
)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=TriggeredAlertList)
def list_alerts(
    repo: TriggeredAlertRepository = Depends(get_triggered_alert_repository),
    _: str = Depends(verify_token),
):
    """List all triggered alerts (most recent first)."""
    alerts = repo.get_all()
    return TriggeredAlertList(alerts=alerts, count=len(alerts))


@router.get("/{alert_id}", response_model=TriggeredAlert)
def get_alert(
    alert_id: str,
    repo: TriggeredAlertRepository = Depends(get_triggered_alert_repository),
    _: str = Depends(verify_token),
):
    """Get a triggered alert by ID."""
    alert = repo.get_by_id(alert_id)
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No alert with id '{alert_id}'",
        )
    return alert


@router.put("/{alert_id}", response_model=TriggeredAlert)
def update_alert(
    alert_id: str,
    updates: TriggeredAlertUpdate,
    repo: TriggeredAlertRepository = Depends(get_triggered_alert_repository),
    _: str = Depends(verify_token),
):
    """Update a triggered alert status (acknowledge or resolve)."""
    updated = repo.update_status(alert_id, updates.status)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No alert with id '{alert_id}'",
        )
    return updated

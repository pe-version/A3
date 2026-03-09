"""Alert rule CRUD endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from middleware.auth import verify_token
from models.alert_rule import (
    AlertRule,
    AlertRuleCreate,
    AlertRuleList,
    AlertRuleResponse,
    AlertRuleUpdate,
)
from repositories.alert_rule_repository import AlertRuleRepository, get_alert_rule_repository

router = APIRouter(prefix="/rules", tags=["rules"])


@router.get("", response_model=AlertRuleList)
def list_rules(
    repo: AlertRuleRepository = Depends(get_alert_rule_repository),
    _: str = Depends(verify_token),
):
    """List all alert rules."""
    rules = repo.get_all()
    return AlertRuleList(rules=rules, count=len(rules))


@router.get("/{rule_id}", response_model=AlertRule)
def get_rule(
    rule_id: str,
    repo: AlertRuleRepository = Depends(get_alert_rule_repository),
    _: str = Depends(verify_token),
):
    """Get an alert rule by ID."""
    rule = repo.get_by_id(rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No rule with id '{rule_id}'",
        )
    return rule


@router.post("", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
def create_rule(
    rule: AlertRuleCreate,
    request: Request,
    repo: AlertRuleRepository = Depends(get_alert_rule_repository),
    _: str = Depends(verify_token),
):
    """Create a new alert rule.

    Validates the sensor_id by making a sync REST call to the sensor
    service. If the sensor service is unavailable (circuit breaker open),
    the rule is created anyway with a warning in the response.
    """
    sensor_client = request.app.state.sensor_client

    sensor_data, validated = sensor_client.get_sensor(rule.sensor_id)

    if validated and sensor_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sensor '{rule.sensor_id}' not found in sensor service",
        )

    created = repo.create(rule)

    response = AlertRuleResponse(**created.model_dump())
    if not validated:
        response.warning = "Sensor service unavailable; sensor_id not validated"

    return response


@router.put("/{rule_id}", response_model=AlertRule)
def update_rule(
    rule_id: str,
    updates: AlertRuleUpdate,
    repo: AlertRuleRepository = Depends(get_alert_rule_repository),
    _: str = Depends(verify_token),
):
    """Update an existing alert rule."""
    updated = repo.update(rule_id, updates)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No rule with id '{rule_id}'",
        )
    return updated


@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_rule(
    rule_id: str,
    repo: AlertRuleRepository = Depends(get_alert_rule_repository),
    _: str = Depends(verify_token),
):
    """Delete an alert rule."""
    deleted = repo.delete(rule_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No rule with id '{rule_id}'",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)

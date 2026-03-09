"""Pydantic models for triggered alert data."""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class AlertStatus(str, Enum):
    """Valid triggered alert statuses."""

    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class TriggeredAlert(BaseModel):
    """Complete triggered alert model."""

    id: str = Field(..., description="Unique alert identifier")
    rule_id: str = Field(..., description="ID of the rule that triggered this alert")
    sensor_id: str = Field(..., description="ID of the sensor that triggered this alert")
    sensor_value: float = Field(..., description="Sensor value at time of trigger")
    threshold: float = Field(..., description="Threshold that was crossed")
    message: str = Field(..., description="Human-readable alert message")
    status: AlertStatus = Field(..., description="Alert status")
    created_at: str = Field(..., description="When the alert was triggered")
    resolved_at: Optional[str] = Field(None, description="When the alert was resolved (None until resolved)")

    class Config:
        from_attributes = True


class TriggeredAlertUpdate(BaseModel):
    """Model for updating a triggered alert status."""

    status: AlertStatus = Field(..., description="New alert status")


class TriggeredAlertList(BaseModel):
    """Response model for list of triggered alerts."""

    alerts: list[TriggeredAlert]
    count: int

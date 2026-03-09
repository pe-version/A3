"""Pydantic models for alert rule data."""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Operator(str, Enum):
    """Valid comparison operators for alert thresholds."""

    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    EQ = "eq"


class RuleStatus(str, Enum):
    """Valid alert rule statuses."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class AlertRuleBase(BaseModel):
    """Base alert rule model with common fields."""

    sensor_id: str = Field(..., min_length=1, description="ID of the sensor to monitor")
    metric: str = Field(default="value", min_length=1, max_length=50, description="Metric to evaluate")
    operator: Operator = Field(..., description="Comparison operator")
    threshold: float = Field(..., description="Threshold value")
    name: str = Field(..., min_length=1, max_length=200, description="Rule display name")
    status: RuleStatus = Field(default=RuleStatus.ACTIVE, description="Rule status")


class AlertRuleCreate(AlertRuleBase):
    """Model for creating a new alert rule."""

    pass


class AlertRuleUpdate(BaseModel):
    """Model for updating an existing alert rule. All fields optional."""

    sensor_id: Optional[str] = Field(None, min_length=1)
    metric: Optional[str] = Field(None, min_length=1, max_length=50)
    operator: Optional[Operator] = None
    threshold: Optional[float] = None
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    status: Optional[RuleStatus] = None


class AlertRule(AlertRuleBase):
    """Complete alert rule model with ID and timestamps.

    Both timestamps are always populated: updated_at is set to
    created_at on initial creation, matching the sensor service pattern.
    """

    id: str = Field(..., description="Unique rule identifier")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp (equals created_at on creation)")

    class Config:
        from_attributes = True


class AlertRuleResponse(BaseModel):
    """Response model for alert rule creation (may include warning)."""

    id: str
    sensor_id: str
    metric: str
    operator: Operator
    threshold: float
    name: str
    status: RuleStatus
    created_at: str
    updated_at: str
    warning: Optional[str] = None

    class Config:
        from_attributes = True


class AlertRuleList(BaseModel):
    """Response model for list of alert rules."""

    rules: list[AlertRule]
    count: int

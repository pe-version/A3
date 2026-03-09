"""Pydantic models for sensor data."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SensorType(str, Enum):
    """Valid sensor types."""

    TEMPERATURE = "temperature"
    MOTION = "motion"
    HUMIDITY = "humidity"
    LIGHT = "light"
    AIR_QUALITY = "air_quality"
    CO2 = "co2"
    CONTACT = "contact"
    PRESSURE = "pressure"


class SensorUnit(str, Enum):
    """Valid sensor units of measurement."""

    FAHRENHEIT = "fahrenheit"
    CELSIUS = "celsius"
    BOOLEAN = "boolean"
    PERCENT = "percent"
    AQI = "aqi"
    PPM = "ppm"
    LUX = "lux"
    PASCAL = "pascal"


class SensorStatus(str, Enum):
    """Valid sensor statuses."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class SensorBase(BaseModel):
    """Base sensor model with common fields."""

    name: str = Field(..., min_length=1, max_length=100, description="Sensor display name")
    type: SensorType = Field(..., description="Type of sensor")
    location: str = Field(..., min_length=1, max_length=100, description="Physical location")
    value: float = Field(..., description="Current sensor value")
    unit: SensorUnit = Field(..., description="Unit of measurement")
    status: SensorStatus = Field(..., description="Operational status")


class SensorCreate(SensorBase):
    """Model for creating a new sensor."""

    pass


class SensorUpdate(BaseModel):
    """Model for updating an existing sensor. All fields optional."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    type: Optional[SensorType] = None
    location: Optional[str] = Field(None, min_length=1, max_length=100)
    value: Optional[float] = None
    unit: Optional[SensorUnit] = None
    status: Optional[SensorStatus] = None


class Sensor(SensorBase):
    """Complete sensor model with ID and timestamps."""

    id: str = Field(..., description="Unique sensor identifier")
    last_reading: str = Field(..., description="ISO 8601 timestamp of last reading")
    created_at: Optional[str] = Field(None, description="Creation timestamp")
    updated_at: Optional[str] = Field(None, description="Last update timestamp")

    class Config:
        from_attributes = True


class SensorList(BaseModel):
    """Response model for list of sensors."""

    sensors: list[Sensor]
    count: int

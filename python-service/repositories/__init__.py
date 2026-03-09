"""Repository layer for data access."""

from repositories.sensor_repository import SensorRepository, get_sensor_repository

__all__ = ["SensorRepository", "get_sensor_repository"]

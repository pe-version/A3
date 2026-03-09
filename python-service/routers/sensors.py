"""Sensor CRUD endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from middleware.auth import verify_token
from models.sensor import Sensor, SensorCreate, SensorList, SensorUpdate
from repositories.sensor_repository import SensorRepository, get_sensor_repository

router = APIRouter(prefix="/sensors", tags=["sensors"])


@router.get("", response_model=SensorList)
def list_sensors(
    repo: SensorRepository = Depends(get_sensor_repository),
    _: str = Depends(verify_token),
):
    """
    List all sensors.

    Returns all sensors in the database with a count.
    """
    sensors = repo.get_all()
    return SensorList(sensors=sensors, count=len(sensors))


@router.get("/{sensor_id}", response_model=Sensor)
def get_sensor(
    sensor_id: str,
    repo: SensorRepository = Depends(get_sensor_repository),
    _: str = Depends(verify_token),
):
    """
    Get a sensor by ID.

    Returns the sensor details or 404 if not found.
    """
    sensor = repo.get_by_id(sensor_id)
    if not sensor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No sensor with id '{sensor_id}'",
        )
    return sensor


@router.post("", response_model=Sensor, status_code=status.HTTP_201_CREATED)
def create_sensor(
    sensor: SensorCreate,
    repo: SensorRepository = Depends(get_sensor_repository),
    _: str = Depends(verify_token),
):
    """
    Create a new sensor.

    Generates a unique ID and sets timestamps automatically.
    """
    created = repo.create(sensor)
    return created


@router.put("/{sensor_id}", response_model=Sensor)
def update_sensor(
    sensor_id: str,
    updates: SensorUpdate,
    request: Request,
    repo: SensorRepository = Depends(get_sensor_repository),
    _: str = Depends(verify_token),
):
    """
    Update an existing sensor.

    Only the provided fields will be updated.
    Publishes a sensor.updated event to RabbitMQ on success.
    Returns 404 if the sensor doesn't exist.
    """
    updated = repo.update(sensor_id, updates)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No sensor with id '{sensor_id}'",
        )

    # Publish sensor.updated event to RabbitMQ
    publisher = request.app.state.publisher
    publisher.publish_sensor_updated(
        sensor_id=updated.id,
        value=updated.value,
        sensor_type=updated.type.value,
        unit=updated.unit.value,
    )

    return updated


@router.delete("/{sensor_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_sensor(
    sensor_id: str,
    repo: SensorRepository = Depends(get_sensor_repository),
    _: str = Depends(verify_token),
):
    """
    Delete a sensor.

    Returns 204 on success, 404 if not found.
    """
    deleted = repo.delete(sensor_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No sensor with id '{sensor_id}'",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)

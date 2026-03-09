"""Repository for sensor data access."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends

from database import get_db_connection
from models.sensor import Sensor, SensorCreate, SensorUpdate


class SensorRepository:
    """Data access layer for sensors using SQLite."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def get_all(self) -> list[Sensor]:
        """Retrieve all sensors."""
        cursor = self.db.execute(
            "SELECT id, name, type, location, value, unit, status, last_reading, created_at, updated_at FROM sensors ORDER BY id"
        )
        rows = cursor.fetchall()
        return [self._row_to_sensor(row) for row in rows]

    def get_by_id(self, sensor_id: str) -> Optional[Sensor]:
        """Retrieve a sensor by ID."""
        cursor = self.db.execute(
            "SELECT id, name, type, location, value, unit, status, last_reading, created_at, updated_at FROM sensors WHERE id = ?",
            (sensor_id,),
        )
        row = cursor.fetchone()
        return self._row_to_sensor(row) if row else None

    def create(self, sensor: SensorCreate) -> Sensor:
        """Create a new sensor."""
        now = datetime.now(timezone.utc).isoformat()
        value = self._convert_value_for_storage(sensor.value)

        # Generate new ID and insert atomically to prevent duplicate IDs under concurrency.
        with self.db:
            cursor = self.db.execute("SELECT MAX(CAST(SUBSTR(id, 8) AS INTEGER)) FROM sensors WHERE id LIKE 'sensor-%'")
            max_num = cursor.fetchone()[0] or 0
            new_id = f"sensor-{max_num + 1:03d}"
            self.db.execute(
                """
                INSERT INTO sensors (id, name, type, location, value, unit, status, last_reading, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_id,
                    sensor.name,
                    sensor.type.value,
                    sensor.location,
                    value,
                    sensor.unit,
                    sensor.status.value,
                    now,
                    now,
                    now,
                ),
            )

        return self.get_by_id(new_id)

    def update(self, sensor_id: str, updates: SensorUpdate) -> Optional[Sensor]:
        """Update an existing sensor."""
        existing = self.get_by_id(sensor_id)
        if not existing:
            return None

        # Build update query dynamically based on provided fields
        update_fields = []
        values = []

        update_data = updates.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            if value is not None:
                if field == "type" or field == "status":
                    value = value.value if hasattr(value, "value") else value
                elif field == "value":
                    value = self._convert_value_for_storage(value)
                update_fields.append(f"{field} = ?")
                values.append(value)

        if not update_fields:
            return existing

        now = datetime.now(timezone.utc).isoformat()
        update_fields.append("updated_at = ?")
        update_fields.append("last_reading = ?")
        values.extend([now, now, sensor_id])

        query = f"UPDATE sensors SET {', '.join(update_fields)} WHERE id = ?"
        self.db.execute(query, values)
        self.db.commit()

        return self.get_by_id(sensor_id)

    def delete(self, sensor_id: str) -> bool:
        """Delete a sensor by ID."""
        cursor = self.db.execute("DELETE FROM sensors WHERE id = ?", (sensor_id,))
        self.db.commit()
        return cursor.rowcount > 0

    def _row_to_sensor(self, row: sqlite3.Row) -> Sensor:
        """Convert a database row to a Sensor model."""
        return Sensor(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            location=row["location"],
            value=row["value"],
            unit=row["unit"],
            status=row["status"],
            last_reading=row["last_reading"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _convert_value_for_storage(self, value) -> float:
        """Convert sensor value to float for SQLite storage."""
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        return float(value)


def get_sensor_repository(
    db: sqlite3.Connection = Depends(get_db_connection),
) -> SensorRepository:
    """Dependency that provides a SensorRepository instance."""
    return SensorRepository(db)

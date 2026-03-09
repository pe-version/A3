"""Repository for triggered alert data access."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends

from database import get_db_connection
from models.triggered_alert import AlertStatus, TriggeredAlert


class TriggeredAlertRepository:
    """Data access layer for triggered alerts using SQLite."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def get_all(self) -> list[TriggeredAlert]:
        """Retrieve all triggered alerts."""
        cursor = self.db.execute(
            "SELECT id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at, resolved_at "
            "FROM triggered_alerts ORDER BY created_at DESC"
        )
        rows = cursor.fetchall()
        return [self._row_to_alert(row) for row in rows]

    def get_by_id(self, alert_id: str) -> Optional[TriggeredAlert]:
        """Retrieve a triggered alert by ID."""
        cursor = self.db.execute(
            "SELECT id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at, resolved_at "
            "FROM triggered_alerts WHERE id = ?",
            (alert_id,),
        )
        row = cursor.fetchone()
        return self._row_to_alert(row) if row else None

    def create(
        self,
        rule_id: str,
        sensor_id: str,
        sensor_value: float,
        threshold: float,
        message: str,
    ) -> TriggeredAlert:
        """Create a new triggered alert."""
        cursor = self.db.execute(
            "SELECT MAX(CAST(SUBSTR(id, 7) AS INTEGER)) FROM triggered_alerts WHERE id LIKE 'alert-%'"
        )
        max_num = cursor.fetchone()[0] or 0
        new_id = f"alert-{max_num + 1:03d}"

        now = datetime.now(timezone.utc).isoformat()

        self.db.execute(
            """
            INSERT INTO triggered_alerts (id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (new_id, rule_id, sensor_id, sensor_value, threshold, message, "open", now),
        )
        self.db.commit()

        return self.get_by_id(new_id)

    def update_status(self, alert_id: str, status: AlertStatus) -> Optional[TriggeredAlert]:
        """Update the status of a triggered alert."""
        existing = self.get_by_id(alert_id)
        if not existing:
            return None

        resolved_at = None
        if status == AlertStatus.RESOLVED:
            resolved_at = datetime.now(timezone.utc).isoformat()

        self.db.execute(
            "UPDATE triggered_alerts SET status = ?, resolved_at = ? WHERE id = ?",
            (status.value, resolved_at, alert_id),
        )
        self.db.commit()

        return self.get_by_id(alert_id)

    def _row_to_alert(self, row: sqlite3.Row) -> TriggeredAlert:
        """Convert a database row to a TriggeredAlert model."""
        return TriggeredAlert(
            id=row["id"],
            rule_id=row["rule_id"],
            sensor_id=row["sensor_id"],
            sensor_value=row["sensor_value"],
            threshold=row["threshold"],
            message=row["message"],
            status=row["status"],
            created_at=row["created_at"],
            resolved_at=row["resolved_at"],
        )


def get_triggered_alert_repository(
    db: sqlite3.Connection = Depends(get_db_connection),
) -> TriggeredAlertRepository:
    """Dependency that provides a TriggeredAlertRepository instance."""
    return TriggeredAlertRepository(db)

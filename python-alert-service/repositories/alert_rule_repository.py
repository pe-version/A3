"""Repository for alert rule data access."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends

from database import get_db_connection
from models.alert_rule import AlertRule, AlertRuleCreate, AlertRuleUpdate


class AlertRuleRepository:
    """Data access layer for alert rules using SQLite."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def get_all(self) -> list[AlertRule]:
        """Retrieve all alert rules."""
        cursor = self.db.execute(
            "SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at "
            "FROM alert_rules ORDER BY id"
        )
        rows = cursor.fetchall()
        return [self._row_to_rule(row) for row in rows]

    def get_by_id(self, rule_id: str) -> Optional[AlertRule]:
        """Retrieve an alert rule by ID."""
        cursor = self.db.execute(
            "SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at "
            "FROM alert_rules WHERE id = ?",
            (rule_id,),
        )
        row = cursor.fetchone()
        return self._row_to_rule(row) if row else None

    def get_active_rules_for_sensor(self, sensor_id: str) -> list[AlertRule]:
        """Retrieve all active rules for a given sensor."""
        cursor = self.db.execute(
            "SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at "
            "FROM alert_rules WHERE sensor_id = ? AND status = 'active' ORDER BY id",
            (sensor_id,),
        )
        rows = cursor.fetchall()
        return [self._row_to_rule(row) for row in rows]

    def create(self, rule: AlertRuleCreate) -> AlertRule:
        """Create a new alert rule."""
        cursor = self.db.execute(
            "SELECT MAX(CAST(SUBSTR(id, 6) AS INTEGER)) FROM alert_rules WHERE id LIKE 'rule-%'"
        )
        max_num = cursor.fetchone()[0] or 0
        new_id = f"rule-{max_num + 1:03d}"

        now = datetime.now(timezone.utc).isoformat()

        self.db.execute(
            """
            INSERT INTO alert_rules (id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id,
                rule.sensor_id,
                rule.metric,
                rule.operator.value,
                rule.threshold,
                rule.name,
                rule.status.value,
                now,
                now,
            ),
        )
        self.db.commit()

        return self.get_by_id(new_id)

    def update(self, rule_id: str, updates: AlertRuleUpdate) -> Optional[AlertRule]:
        """Update an existing alert rule."""
        existing = self.get_by_id(rule_id)
        if not existing:
            return None

        update_fields = []
        values = []

        update_data = updates.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            if value is not None:
                if field in ("operator", "status"):
                    value = value.value if hasattr(value, "value") else value
                update_fields.append(f"{field} = ?")
                values.append(value)

        if not update_fields:
            return existing

        now = datetime.now(timezone.utc).isoformat()
        update_fields.append("updated_at = ?")
        values.extend([now, rule_id])

        query = f"UPDATE alert_rules SET {', '.join(update_fields)} WHERE id = ?"
        self.db.execute(query, values)
        self.db.commit()

        return self.get_by_id(rule_id)

    def delete(self, rule_id: str) -> bool:
        """Delete an alert rule by ID."""
        cursor = self.db.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
        self.db.commit()
        return cursor.rowcount > 0

    def _row_to_rule(self, row: sqlite3.Row) -> AlertRule:
        """Convert a database row to an AlertRule model."""
        return AlertRule(
            id=row["id"],
            sensor_id=row["sensor_id"],
            metric=row["metric"],
            operator=row["operator"],
            threshold=row["threshold"],
            name=row["name"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


def get_alert_rule_repository(
    db: sqlite3.Connection = Depends(get_db_connection),
) -> AlertRuleRepository:
    """Dependency that provides an AlertRuleRepository instance."""
    return AlertRuleRepository(db)

"""Alert evaluation service — processes sensor events against rules."""

import logging
import sqlite3

from repositories.alert_rule_repository import AlertRuleRepository
from repositories.triggered_alert_repository import TriggeredAlertRepository

logger = logging.getLogger("alert_service")

OPERATORS = {
    "gt": lambda v, t: v > t,
    "lt": lambda v, t: v < t,
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
    "eq": lambda v, t: v == t,
}


class AlertEvaluator:
    """Evaluates sensor update events against active alert rules.

    Opens its own database connection since it runs in the consumer
    thread, separate from the FastAPI request lifecycle.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def evaluate(self, event: dict) -> None:
        """Evaluate a sensor.updated event against active rules.

        Args:
            event: Dict with keys: sensor_id, value, type, unit, timestamp.
        """
        sensor_id = event.get("sensor_id")
        sensor_value = event.get("value")

        if sensor_id is None or sensor_value is None:
            logger.warning("Received incomplete sensor event: %s", event)
            return

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rule_repo = AlertRuleRepository(conn)
            alert_repo = TriggeredAlertRepository(conn)

            active_rules = rule_repo.get_active_rules_for_sensor(sensor_id)

            for rule in active_rules:
                op_func = OPERATORS.get(rule.operator)
                if op_func and op_func(sensor_value, rule.threshold):
                    message = (
                        f"Sensor {sensor_id} value {sensor_value} "
                        f"{rule.operator} threshold {rule.threshold} "
                        f"(rule: {rule.name})"
                    )
                    alert = alert_repo.create(
                        rule_id=rule.id,
                        sensor_id=sensor_id,
                        sensor_value=sensor_value,
                        threshold=rule.threshold,
                        message=message,
                    )
                    logger.info(
                        "Alert triggered: %s",
                        message,
                        extra={"alert_id": alert.id, "rule_id": rule.id},
                    )
        finally:
            conn.close()

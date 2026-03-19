"""Alert evaluation service — processes sensor events against rules."""

import logging
import time
import uuid
from datetime import datetime, timezone

import aiosqlite

from metrics import server as metrics_server

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

    Uses aiosqlite for non-blocking database access so evaluation
    can run in the asyncio event loop alongside the aio-pika consumer.
    Each call to evaluate() opens its own connection so that concurrent
    async workers do not share a single connection.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def _open_db(self) -> aiosqlite.Connection:
        """Open a new database connection with WAL mode and busy timeout."""
        db = await aiosqlite.connect(self.db_path)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        db.row_factory = aiosqlite.Row
        return db

    async def evaluate(self, event: dict) -> None:
        """Evaluate a sensor.updated event against active rules.

        Args:
            event: Dict with keys: sensor_id, value, type, unit, timestamp.
        """
        start = time.monotonic()
        sensor_id = event.get("sensor_id")
        sensor_value = event.get("value")
        trace_id = event.get("trace_id")

        if sensor_id is None or sensor_value is None:
            logger.warning("Received incomplete sensor event: %s", event)
            return

        db = await self._open_db()
        try:
            cursor = await db.execute(
                "SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at "
                "FROM alert_rules WHERE sensor_id = ? AND status = 'active' ORDER BY id",
                (sensor_id,),
            )
            rules = await cursor.fetchall()

            for rule in rules:
                op_func = OPERATORS.get(rule["operator"])
                if op_func and op_func(sensor_value, rule["threshold"]):
                    await self._trigger_alert(db, rule, sensor_id, sensor_value, trace_id)
        finally:
            await db.close()
            if metrics_server.collector is not None:
                metrics_server.collector.record_processing_duration(start)
                metrics_server.collector.inc_processed()

    async def _trigger_alert(
        self, db: aiosqlite.Connection, rule, sensor_id: str, sensor_value: float, trace_id: str | None
    ) -> None:
        """Create a triggered alert for a rule whose threshold was crossed."""
        message = (
            f"Sensor {sensor_id} value {sensor_value} "
            f"{rule['operator']} threshold {rule['threshold']} "
            f"(rule: {rule['name']})"
        )

        new_id = f"alert-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).isoformat()

        await db.execute(
            "INSERT INTO triggered_alerts (id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (new_id, rule["id"], sensor_id, sensor_value, rule["threshold"], message, "open", now),
        )
        await db.commit()

        if metrics_server.collector is not None:
            metrics_server.collector.inc_triggered()
        logger.info(
            "Alert triggered: %s",
            message,
            extra={"alert_id": new_id, "rule_id": rule["id"], "trace_id": trace_id},
        )

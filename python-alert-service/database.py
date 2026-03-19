"""SQLite database connection and initialization."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from config import get_settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT 'value',
    operator TEXT NOT NULL CHECK(operator IN ('gt', 'lt', 'gte', 'lte', 'eq')),
    threshold REAL NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS triggered_alerts (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_value REAL NOT NULL,
    threshold REAL NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open', 'acknowledged', 'resolved')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT,
    FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_sensor_id ON alert_rules(sensor_id);
CREATE INDEX IF NOT EXISTS idx_alert_rules_status ON alert_rules(status);
CREATE INDEX IF NOT EXISTS idx_triggered_alerts_rule_id ON triggered_alerts(rule_id);
CREATE INDEX IF NOT EXISTS idx_triggered_alerts_status ON triggered_alerts(status);
"""


def init_database() -> None:
    """Initialize database schema and seed data if needed."""
    settings = get_settings()
    db_path = Path(settings.database_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(settings.database_path, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.executescript(SCHEMA)
        conn.commit()

        cursor = conn.execute("SELECT COUNT(*) FROM alert_rules")
        count = cursor.fetchone()[0]

        if count == 0:
            seed_from_json(conn, settings.seed_data_path)
    finally:
        conn.close()


def seed_from_json(conn: sqlite3.Connection, json_path: str) -> None:
    """Seed database from JSON file."""
    path = Path(json_path)
    if not path.exists():
        return

    with open(path) as f:
        rules = json.load(f)

    now = datetime.now(timezone.utc).isoformat()

    for rule in rules:
        conn.execute(
            """
            INSERT INTO alert_rules (id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule["id"],
                rule["sensor_id"],
                rule["metric"],
                rule["operator"],
                rule["threshold"],
                rule["name"],
                rule["status"],
                now,
                now,
            ),
        )

    conn.commit()


def get_db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Yield a database connection for the request lifecycle."""
    settings = get_settings()
    conn = sqlite3.connect(settings.database_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

"""SQLite database connection and initialization."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from config import get_settings

# SQL schema for sensors table
SCHEMA = """
CREATE TABLE IF NOT EXISTS sensors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('temperature', 'motion', 'humidity', 'light', 'air_quality', 'co2', 'contact', 'pressure')),
    location TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive', 'error')),
    last_reading TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sensors_type ON sensors(type);
CREATE INDEX IF NOT EXISTS idx_sensors_location ON sensors(location);
CREATE INDEX IF NOT EXISTS idx_sensors_status ON sensors(status);
"""


def init_database() -> None:
    """Initialize database schema and seed data if needed."""
    settings = get_settings()
    db_path = Path(settings.database_path)

    # Ensure directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(settings.database_path)
    try:
        # Create schema
        conn.executescript(SCHEMA)
        conn.commit()

        # Check if we need to seed data
        cursor = conn.execute("SELECT COUNT(*) FROM sensors")
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
        sensors = json.load(f)

    now = datetime.now(timezone.utc).isoformat()

    for sensor in sensors:
        conn.execute(
            """
            INSERT INTO sensors (id, name, type, location, value, unit, status, last_reading, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sensor["id"],
                sensor["name"],
                sensor["type"],
                sensor["location"],
                sensor["value"],
                sensor["unit"],
                sensor["status"],
                sensor["last_reading"],
                now,
                now,
            ),
        )

    conn.commit()


def get_db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Yield a database connection for the request lifecycle."""
    settings = get_settings()
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

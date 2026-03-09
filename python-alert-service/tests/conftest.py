"""Pytest fixtures for alert service tests."""

import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

TEST_TOKEN = "test-secret-token"

# Set env vars before any app module is imported
os.environ["API_TOKEN"] = TEST_TOKEN
os.environ["RABBITMQ_URL"] = "amqp://localhost/"
os.environ["SENSOR_SERVICE_URL"] = "http://localhost:0"

_APP_MODULES = (
    "main", "config", "database",
    "clients", "clients.sensor_client",
    "messaging", "messaging.consumer",
    "repositories", "repositories.alert_rule_repository",
    "repositories.triggered_alert_repository",
    "routers", "routers.rules", "routers.alerts", "routers.health",
    "services", "services.alert_evaluator",
    "middleware", "middleware.auth", "middleware.logging",
    "models", "models.alert_rule", "models.triggered_alert",
)


def _flush_modules():
    for mod in _APP_MODULES:
        sys.modules.pop(mod, None)


def _make_fixture(sensor_result: tuple):
    @pytest.fixture(scope="function")
    def _fixture(temp_db, request):
        os.environ["DATABASE_PATH"] = temp_db
        os.environ["SEED_DATA_PATH"] = "/nonexistent/path.json"
        _flush_modules()

        mock_sensor_client = MagicMock()
        mock_sensor_client.get_sensor.return_value = sensor_result
        mock_consumer = MagicMock()
        mock_consumer.start = MagicMock()

        with patch("clients.sensor_client.SensorClient", return_value=mock_sensor_client), \
             patch("messaging.consumer.AlertConsumer", return_value=mock_consumer):
            import main  # noqa: PLC0415
            with TestClient(main.app) as tc:
                yield tc

    return _fixture


@pytest.fixture(scope="function")
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture(scope="function")
def client(temp_db):
    """Sensor service unavailable — fallback path."""
    os.environ["DATABASE_PATH"] = temp_db
    os.environ["SEED_DATA_PATH"] = "/nonexistent/path.json"
    _flush_modules()

    mock_sensor_client = MagicMock()
    mock_sensor_client.get_sensor.return_value = (None, False)
    mock_consumer = MagicMock()
    mock_consumer.start = MagicMock()

    with patch("clients.sensor_client.SensorClient", return_value=mock_sensor_client), \
         patch("messaging.consumer.AlertConsumer", return_value=mock_consumer):
        import main  # noqa: PLC0415
        with TestClient(main.app) as tc:
            yield tc


@pytest.fixture(scope="function")
def client_sensor_found(temp_db):
    """Sensor service returns a valid sensor."""
    os.environ["DATABASE_PATH"] = temp_db
    os.environ["SEED_DATA_PATH"] = "/nonexistent/path.json"
    _flush_modules()

    mock_sensor_client = MagicMock()
    mock_sensor_client.get_sensor.return_value = ({"id": "sensor-001"}, True)
    mock_consumer = MagicMock()
    mock_consumer.start = MagicMock()

    with patch("clients.sensor_client.SensorClient", return_value=mock_sensor_client), \
         patch("messaging.consumer.AlertConsumer", return_value=mock_consumer):
        import main  # noqa: PLC0415
        with TestClient(main.app) as tc:
            yield tc


@pytest.fixture(scope="function")
def client_sensor_not_found(temp_db):
    """Sensor service confirms sensor does not exist (404)."""
    os.environ["DATABASE_PATH"] = temp_db
    os.environ["SEED_DATA_PATH"] = "/nonexistent/path.json"
    _flush_modules()

    mock_sensor_client = MagicMock()
    mock_sensor_client.get_sensor.return_value = (None, True)
    mock_consumer = MagicMock()
    mock_consumer.start = MagicMock()

    with patch("clients.sensor_client.SensorClient", return_value=mock_sensor_client), \
         patch("messaging.consumer.AlertConsumer", return_value=mock_consumer):
        import main  # noqa: PLC0415
        with TestClient(main.app) as tc:
            yield tc


@pytest.fixture
def auth_headers():
    return {"Authorization": f"Bearer {TEST_TOKEN}"}

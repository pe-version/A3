"""Pytest fixtures for sensor service tests."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Set test environment variables before importing app
TEST_TOKEN = "test-secret-token"
os.environ["API_TOKEN"] = TEST_TOKEN
os.environ["RABBITMQ_URL"] = "amqp://localhost/"  # won't be used (mocked)


@pytest.fixture(scope="function")
def temp_db():
    """Create a temporary database file for each test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    os.environ["DATABASE_PATH"] = db_path
    os.environ["SEED_DATA_PATH"] = "/nonexistent/path.json"  # Don't seed
    yield db_path
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture(scope="function")
def client(temp_db):
    """Create a test client with a fresh database. RabbitMQ is mocked."""
    # Import app after setting environment variables
    from main import app

    # Clear any cached settings
    from config import get_settings
    get_settings.cache_clear()

    mock_publisher = MagicMock()
    mock_publisher.publish_sensor_updated = MagicMock()

    with patch("main.EventPublisher", return_value=mock_publisher):
        with TestClient(app) as test_client:
            yield test_client


@pytest.fixture
def auth_headers():
    """Return authorization headers with the test token."""
    return {"Authorization": f"Bearer {TEST_TOKEN}"}

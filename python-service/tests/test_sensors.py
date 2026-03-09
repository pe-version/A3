"""Integration tests for sensor CRUD operations."""

import pytest


class TestAuthentication:
    """Tests for authentication middleware."""

    def test_unauthorized_without_token(self, client):
        """Requests without token should return 401."""
        response = client.get("/sensors")
        assert response.status_code == 401

    def test_unauthorized_with_invalid_token(self, client):
        """Requests with invalid token should return 401."""
        response = client.get(
            "/sensors",
            headers={"Authorization": "Bearer invalid-token"},
        )
        assert response.status_code == 401

    def test_unauthorized_malformed_header(self, client):
        """Requests with malformed auth header (no Bearer prefix) should return 401."""
        response = client.get(
            "/sensors",
            headers={"Authorization": "Token some-token"},
        )
        assert response.status_code == 401

    def test_unauthorized_post(self, client):
        """POST without token should return 401."""
        response = client.post("/sensors", json={"name": "test"})
        assert response.status_code == 401

    def test_unauthorized_put(self, client):
        """PUT without token should return 401."""
        response = client.put("/sensors/sensor-001", json={"value": 1})
        assert response.status_code == 401

    def test_unauthorized_delete(self, client):
        """DELETE without token should return 401."""
        response = client.delete("/sensors/sensor-001")
        assert response.status_code == 401

    def test_unauthorized_response_body(self, client):
        """401 response should include detail field."""
        response = client.get("/sensors")
        data = response.json()
        assert "detail" in data

    def test_authorized_with_valid_token(self, client, auth_headers):
        """Requests with valid token should succeed."""
        response = client.get("/sensors", headers=auth_headers)
        assert response.status_code == 200


class TestHealthEndpoint:
    """Tests for the health endpoint."""

    def test_health_returns_ok_without_auth(self, client):
        """Health endpoint should return ok status without authentication."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["service"] == "python"


class TestSensorCRUD:
    """Tests for sensor CRUD operations."""

    def test_list_sensors_empty(self, client, auth_headers):
        """Empty database should return empty list."""
        response = client.get("/sensors", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["sensors"] == []
        assert data["count"] == 0

    def test_create_sensor(self, client, auth_headers):
        """Creating a sensor should return 201 with the created sensor."""
        new_sensor = {
            "name": "Test Sensor",
            "type": "temperature",
            "location": "test_room",
            "value": 72.5,
            "unit": "fahrenheit",
            "status": "active",
        }
        response = client.post("/sensors", json=new_sensor, headers=auth_headers)
        assert response.status_code == 201
        data = response.json()
        assert "id" in data
        assert data["name"] == "Test Sensor"
        assert data["type"] == "temperature"
        assert data["value"] == 72.5

    def test_create_and_fetch_sensor(self, client, auth_headers):
        """Created sensor should be retrievable by ID."""
        # Create
        new_sensor = {
            "name": "Fetch Test Sensor",
            "type": "humidity",
            "location": "bathroom",
            "value": 65.0,
            "unit": "percent",
            "status": "active",
        }
        create_response = client.post("/sensors", json=new_sensor, headers=auth_headers)
        assert create_response.status_code == 201
        created = create_response.json()
        sensor_id = created["id"]

        # Fetch
        get_response = client.get(f"/sensors/{sensor_id}", headers=auth_headers)
        assert get_response.status_code == 200
        fetched = get_response.json()
        assert fetched["id"] == sensor_id
        assert fetched["name"] == "Fetch Test Sensor"
        assert fetched["value"] == 65.0

    def test_update_sensor(self, client, auth_headers):
        """Updating a sensor should modify only specified fields."""
        # Create
        new_sensor = {
            "name": "Update Test",
            "type": "temperature",
            "location": "kitchen",
            "value": 70.0,
            "unit": "fahrenheit",
            "status": "active",
        }
        create_response = client.post("/sensors", json=new_sensor, headers=auth_headers)
        sensor_id = create_response.json()["id"]

        # Update
        update_data = {"value": 75.5, "status": "inactive"}
        update_response = client.put(
            f"/sensors/{sensor_id}", json=update_data, headers=auth_headers
        )
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated["value"] == 75.5
        assert updated["status"] == "inactive"
        assert updated["name"] == "Update Test"  # Unchanged

    def test_delete_sensor(self, client, auth_headers):
        """Deleting a sensor should remove it from the database."""
        # Create
        new_sensor = {
            "name": "Delete Test",
            "type": "motion",
            "location": "hallway",
            "value": 0,
            "unit": "boolean",
            "status": "active",
        }
        create_response = client.post("/sensors", json=new_sensor, headers=auth_headers)
        sensor_id = create_response.json()["id"]

        # Delete
        delete_response = client.delete(f"/sensors/{sensor_id}", headers=auth_headers)
        assert delete_response.status_code == 204

        # Verify deleted
        get_response = client.get(f"/sensors/{sensor_id}", headers=auth_headers)
        assert get_response.status_code == 404

    def test_get_nonexistent_sensor(self, client, auth_headers):
        """Getting a nonexistent sensor should return 404."""
        response = client.get("/sensors/nonexistent-id", headers=auth_headers)
        assert response.status_code == 404

    def test_delete_nonexistent_sensor(self, client, auth_headers):
        """Deleting a nonexistent sensor should return 404."""
        response = client.delete("/sensors/nonexistent-id", headers=auth_headers)
        assert response.status_code == 404

    def test_create_sensor_validation(self, client, auth_headers):
        """Creating a sensor with invalid data should return 422."""
        invalid_sensor = {
            "name": "Invalid",
            "type": "invalid_type",  # Not a valid type
            "location": "test",
            "value": 0,
            "unit": "test",
            "status": "active",
        }
        response = client.post("/sensors", json=invalid_sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_list_sensors_after_create(self, client, auth_headers):
        """List should include created sensors."""
        # Create multiple sensors
        for i in range(3):
            sensor = {
                "name": f"Sensor {i}",
                "type": "temperature",
                "location": f"room_{i}",
                "value": 70.0 + i,
                "unit": "fahrenheit",
                "status": "active",
            }
            client.post("/sensors", json=sensor, headers=auth_headers)

        # List
        response = client.get("/sensors", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        assert len(data["sensors"]) == 3

    def test_get_nonexistent_response_body(self, client, auth_headers):
        """404 response should include detail field with sensor ID."""
        response = client.get("/sensors/sensor-999", headers=auth_headers)
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert "sensor-999" in data["detail"]

    def test_update_nonexistent_sensor(self, client, auth_headers):
        """Updating a nonexistent sensor should return 404."""
        response = client.put(
            "/sensors/nonexistent-id",
            json={"value": 99.0},
            headers=auth_headers,
        )
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data


class TestBadPayloads:
    """Tests for invalid request bodies."""

    def test_create_empty_body(self, client, auth_headers):
        """Creating a sensor with empty body should return 422."""
        response = client.post("/sensors", json={}, headers=auth_headers)
        assert response.status_code == 422

    def test_create_missing_required_fields(self, client, auth_headers):
        """Creating a sensor with only some fields should return 422."""
        response = client.post(
            "/sensors",
            json={"name": "Partial Sensor"},
            headers=auth_headers,
        )
        assert response.status_code == 422

    def test_create_invalid_type_enum(self, client, auth_headers):
        """Creating a sensor with invalid type enum should return 422."""
        sensor = {
            "name": "Bad Type",
            "type": "invalid_type",
            "location": "test",
            "value": 0,
            "unit": "fahrenheit",
            "status": "active",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_create_invalid_unit_enum(self, client, auth_headers):
        """Creating a sensor with invalid unit enum should return 422."""
        sensor = {
            "name": "Bad Unit",
            "type": "temperature",
            "location": "test",
            "value": 0,
            "unit": "invalid_unit",
            "status": "active",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_create_invalid_status_enum(self, client, auth_headers):
        """Creating a sensor with invalid status enum should return 422."""
        sensor = {
            "name": "Bad Status",
            "type": "temperature",
            "location": "test",
            "value": 0,
            "unit": "fahrenheit",
            "status": "broken",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_create_wrong_value_type(self, client, auth_headers):
        """Creating a sensor with string value should return 422."""
        sensor = {
            "name": "Bad Value",
            "type": "temperature",
            "location": "test",
            "value": "not_a_number",
            "unit": "fahrenheit",
            "status": "active",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_create_empty_name(self, client, auth_headers):
        """Creating a sensor with empty name should return 422."""
        sensor = {
            "name": "",
            "type": "temperature",
            "location": "test",
            "value": 0,
            "unit": "fahrenheit",
            "status": "active",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_create_empty_location(self, client, auth_headers):
        """Creating a sensor with empty location should return 422."""
        sensor = {
            "name": "No Location",
            "type": "temperature",
            "location": "",
            "value": 0,
            "unit": "fahrenheit",
            "status": "active",
        }
        response = client.post("/sensors", json=sensor, headers=auth_headers)
        assert response.status_code == 422

    def test_update_invalid_status_enum(self, client, auth_headers):
        """Updating a sensor with invalid status should return 422."""
        # Create valid sensor first
        sensor = {
            "name": "Update Target",
            "type": "temperature",
            "location": "test",
            "value": 70.0,
            "unit": "fahrenheit",
            "status": "active",
        }
        create_resp = client.post("/sensors", json=sensor, headers=auth_headers)
        sensor_id = create_resp.json()["id"]

        # Update with invalid status
        response = client.put(
            f"/sensors/{sensor_id}",
            json={"status": "broken"},
            headers=auth_headers,
        )
        assert response.status_code == 422

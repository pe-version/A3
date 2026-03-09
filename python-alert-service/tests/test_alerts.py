"""Tests for the Python alert service."""

import pytest


TEST_TOKEN = "test-secret-token"

VALID_RULE = {
    "sensor_id": "sensor-001",
    "name": "High Temperature",
    "operator": "gt",
    "threshold": 80.0,
}


# ─── Authentication ────────────────────────────────────────────────────────────

class TestAuthentication:
    def test_unauthorized_without_token(self, client):
        resp = client.get("/rules")
        assert resp.status_code == 401

    def test_unauthorized_with_invalid_token(self, client):
        resp = client.get("/rules", headers={"Authorization": "Bearer wrong-token"})
        assert resp.status_code == 401

    def test_unauthorized_malformed_header(self, client):
        resp = client.get("/rules", headers={"Authorization": "NotBearer token"})
        assert resp.status_code == 401

    def test_unauthorized_response_body(self, client):
        resp = client.get("/rules")
        assert resp.status_code == 401
        assert "detail" in resp.json()

    def test_authorized_with_valid_token(self, client, auth_headers):
        resp = client.get("/rules", headers=auth_headers)
        assert resp.status_code == 200


# ─── Health ────────────────────────────────────────────────────────────────────

class TestHealth:
    def test_health_no_auth_required(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ─── Alert Rules CRUD ──────────────────────────────────────────────────────────

class TestAlertRules:
    def test_list_rules_empty(self, client, auth_headers):
        resp = client.get("/rules", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["rules"] == []

    def test_create_rule_with_valid_sensor(self, client_sensor_found, auth_headers):
        resp = client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        assert resp.status_code == 201
        data = resp.json()
        assert data["sensor_id"] == "sensor-001"
        assert data["operator"] == "gt"
        assert data["threshold"] == 80.0
        assert data["status"] == "active"
        assert "warning" not in data or data["warning"] is None
        assert "id" in data

    def test_create_rule_sensor_not_found_returns_404(self, client_sensor_not_found, auth_headers):
        resp = client_sensor_not_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        assert resp.status_code == 404

    def test_create_rule_sensor_unavailable_allows_with_warning(self, client, auth_headers):
        """When sensor service is down, rule is created with a warning (fallback behavior)."""
        resp = client.post("/rules", json=VALID_RULE, headers=auth_headers)
        assert resp.status_code == 201
        data = resp.json()
        assert "warning" in data
        assert data["warning"] is not None
        assert "sensor_id" in data["warning"] or "unavailable" in data["warning"]

    def test_create_rule_invalid_operator(self, client_sensor_found, auth_headers):
        rule = {**VALID_RULE, "operator": "invalid"}
        resp = client_sensor_found.post("/rules", json=rule, headers=auth_headers)
        assert resp.status_code == 422

    def test_create_rule_missing_required_fields(self, client_sensor_found, auth_headers):
        resp = client_sensor_found.post("/rules", json={"sensor_id": "sensor-001"}, headers=auth_headers)
        assert resp.status_code == 422

    def test_get_nonexistent_rule(self, client, auth_headers):
        resp = client.get("/rules/rule-999", headers=auth_headers)
        assert resp.status_code == 404

    def test_create_and_fetch_rule(self, client_sensor_found, auth_headers):
        create = client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        assert create.status_code == 201
        rule_id = create.json()["id"]

        fetch = client_sensor_found.get(f"/rules/{rule_id}", headers=auth_headers)
        assert fetch.status_code == 200
        assert fetch.json()["id"] == rule_id

    def test_update_rule(self, client_sensor_found, auth_headers):
        create = client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        rule_id = create.json()["id"]

        resp = client_sensor_found.put(
            f"/rules/{rule_id}",
            json={"status": "inactive"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "inactive"

    def test_update_rule_invalid_status(self, client_sensor_found, auth_headers):
        create = client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        rule_id = create.json()["id"]

        resp = client_sensor_found.put(
            f"/rules/{rule_id}",
            json={"status": "deleted"},
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_update_nonexistent_rule(self, client, auth_headers):
        resp = client.put("/rules/rule-999", json={"status": "inactive"}, headers=auth_headers)
        assert resp.status_code == 404

    def test_delete_rule(self, client_sensor_found, auth_headers):
        create = client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        rule_id = create.json()["id"]

        delete = client_sensor_found.delete(f"/rules/{rule_id}", headers=auth_headers)
        assert delete.status_code == 204

        fetch = client_sensor_found.get(f"/rules/{rule_id}", headers=auth_headers)
        assert fetch.status_code == 404

    def test_delete_nonexistent_rule(self, client, auth_headers):
        resp = client.delete("/rules/rule-999", headers=auth_headers)
        assert resp.status_code == 404

    def test_list_rules_after_create(self, client_sensor_found, auth_headers):
        client_sensor_found.post("/rules", json=VALID_RULE, headers=auth_headers)
        client_sensor_found.post(
            "/rules",
            json={**VALID_RULE, "name": "Low Temp", "operator": "lt", "threshold": 10.0},
            headers=auth_headers,
        )
        resp = client_sensor_found.get("/rules", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["count"] == 2


# ─── Triggered Alerts ─────────────────────────────────────────────────────────

class TestTriggeredAlerts:
    def test_list_alerts_empty(self, client, auth_headers):
        resp = client.get("/alerts", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["alerts"] == []

    def test_get_nonexistent_alert(self, client, auth_headers):
        resp = client.get("/alerts/alert-999", headers=auth_headers)
        assert resp.status_code == 404

    def test_update_nonexistent_alert(self, client, auth_headers):
        resp = client.put("/alerts/alert-999", json={"status": "resolved"}, headers=auth_headers)
        assert resp.status_code == 404

    def test_update_alert_invalid_status(self, client, auth_headers):
        resp = client.put("/alerts/alert-999", json={"status": "deleted"}, headers=auth_headers)
        assert resp.status_code == 422

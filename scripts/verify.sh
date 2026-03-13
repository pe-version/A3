#!/usr/bin/env bash
# verify.sh — end-to-end smoke test for the A2 IoT microservices stack.
#
# Assumes all services are already running (docker compose up -d).
# Reads API_TOKEN from environment; falls back to the default dev token.
#
# Usage:
#   ./scripts/verify.sh
#   API_TOKEN=mysecret ./scripts/verify.sh

set -euo pipefail

TOKEN="${API_TOKEN:-your-secret-token}"

PY_SENSOR="http://localhost:8000"
GO_SENSOR="http://localhost:8080"
GO_ALERT="http://localhost:8081"
PY_ALERT="http://localhost:8002"

PASS=0
FAIL=0

# ── helpers ────────────────────────────────────────────────────────────────────

green() { printf '\033[0;32m✔ %s\033[0m\n' "$*"; }
red()   { printf '\033[0;31m✘ %s\033[0m\n' "$*"; }

pass() { green "$1"; PASS=$(( PASS + 1 )); }
fail() { red "$1";   FAIL=$(( FAIL + 1 )); }

auth_header() { echo "Authorization: Bearer ${TOKEN}"; }

check() {
  local label="$1" url="$2" expected_status="$3"
  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" -H "$(auth_header)" "$url")
  if [[ "$status" == "$expected_status" ]]; then
    pass "$label (HTTP $status)"
  else
    fail "$label — expected $expected_status, got $status"
  fi
}

# ── 1. health checks ───────────────────────────────────────────────────────────

echo ""
echo "=== 1. Health checks ==="
check "Python sensor service /health"  "$PY_SENSOR/health"  200
check "Go sensor service /health"      "$GO_SENSOR/health"  200
check "Go alert service /health"       "$GO_ALERT/health"   200
check "Python alert service /health"   "$PY_ALERT/health"   200

# ── 2. sensor reads ────────────────────────────────────────────────────────────

echo ""
echo "=== 2. Sensor reads ==="
check "Python sensor service GET /sensors"       "$PY_SENSOR/sensors"        200
check "Go sensor service GET /sensors"           "$GO_SENSOR/sensors"        200
check "Python sensor service GET /sensors/sensor-001" "$PY_SENSOR/sensors/sensor-001" 200
check "Go sensor service GET /sensors/sensor-001"     "$GO_SENSOR/sensors/sensor-001" 200

# ── 3. alert rule reads ────────────────────────────────────────────────────────

echo ""
echo "=== 3. Alert rule reads ==="
check "Go alert service GET /rules"     "$GO_ALERT/rules"   200
check "Python alert service GET /rules" "$PY_ALERT/rules"   200

# ── 4. alert rule create (exercises sync CB validation path) ───────────────────

echo ""
echo "=== 4. Alert rule creation (sync sensor validation) ==="

create_rule_go() {
  curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$GO_ALERT/rules" \
    -H "$(auth_header)" \
    -H "Content-Type: application/json" \
    -d '{"sensor_id":"sensor-001","metric":"value","operator":"gt","threshold":90.0,"name":"Verify Smoke Test Rule Go"}'
}

create_rule_py() {
  curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$PY_ALERT/rules" \
    -H "$(auth_header)" \
    -H "Content-Type: application/json" \
    -d '{"sensor_id":"sensor-001","metric":"value","operator":"gt","threshold":90.0,"name":"Verify Smoke Test Rule Python"}'
}

status=$(create_rule_go)
if [[ "$status" == "201" ]]; then
  pass "Go alert service POST /rules (HTTP 201)"
else
  fail "Go alert service POST /rules — expected 201, got $status"
fi

status=$(create_rule_py)
if [[ "$status" == "201" ]]; then
  pass "Python alert service POST /rules (HTTP 201)"
else
  fail "Python alert service POST /rules — expected 201, got $status"
fi

# ── 5. async pipeline: sensor update → triggered alert ────────────────────────
#
# Update sensor-001 to 85.0, crossing the seeded "gt 80.0" rule.
# Poll GET /alerts on both alert services for up to 10 seconds.
# Then restore sensor-001 to its original value.

echo ""
echo "=== 5. Async pipeline: sensor update → triggered alert ==="

TRIGGER_VALUE=85.0
RESTORE_VALUE=72.5
SENSOR_ID="sensor-001"
POLL_TIMEOUT=10
POLL_INTERVAL=1

update_sensor() {
  local base="$1"
  curl -s -o /dev/null -w "%{http_code}" \
    -X PUT "$base/sensors/$SENSOR_ID" \
    -H "$(auth_header)" \
    -H "Content-Type: application/json" \
    -d "{\"value\": $TRIGGER_VALUE}"
}

count_alerts_for_sensor() {
  local base="$1"
  local body count
  body=$(curl -s -H "$(auth_header)" "$base/alerts")
  count=$(echo "$body" | grep -o "\"sensor_id\":\"$SENSOR_ID\"" | wc -l | tr -d ' \n') || true
  echo "${count:-0}"
}

# Snapshot alert counts before the update so we detect *new* alerts only.
alerts_before_go=$(count_alerts_for_sensor "$GO_ALERT")
alerts_before_py=$(count_alerts_for_sensor "$PY_ALERT")

# Trigger the sensor update on both sensor services.
status=$(update_sensor "$GO_SENSOR")
if [[ "$status" == "200" ]]; then
  pass "Go sensor service PUT /sensors/$SENSOR_ID to $TRIGGER_VALUE (HTTP 200)"
else
  fail "Go sensor service PUT /sensors/$SENSOR_ID — expected 200, got $status"
fi

status=$(update_sensor "$PY_SENSOR")
if [[ "$status" == "200" ]]; then
  pass "Python sensor service PUT /sensors/$SENSOR_ID to $TRIGGER_VALUE (HTTP 200)"
else
  fail "Python sensor service PUT /sensors/$SENSOR_ID — expected 200, got $status"
fi

# Poll for new triggered alerts.
poll_for_alert() {
  local label="$1" base="$2" before="$3"
  local elapsed=0
  while [[ "$elapsed" -lt "$POLL_TIMEOUT" ]]; do
    local after
    after=$(count_alerts_for_sensor "$base")
    if [[ "$after" -gt "$before" ]]; then
      pass "$label — triggered alert appeared after ${elapsed}s"
      return 0
    fi
    sleep "$POLL_INTERVAL"
    (( elapsed += POLL_INTERVAL ))
  done
  fail "$label — no triggered alert after ${POLL_TIMEOUT}s"
  return 1
}

poll_for_alert "Go alert service async pipeline"     "$GO_ALERT" "$alerts_before_go"
poll_for_alert "Python alert service async pipeline" "$PY_ALERT" "$alerts_before_py"

# Restore sensor value so repeated runs start clean.
update_sensor "$GO_SENSOR" > /dev/null
curl -s -o /dev/null \
  -X PUT "$GO_SENSOR/sensors/$SENSOR_ID" \
  -H "$(auth_header)" \
  -H "Content-Type: application/json" \
  -d "{\"value\": $RESTORE_VALUE}"

curl -s -o /dev/null \
  -X PUT "$PY_SENSOR/sensors/$SENSOR_ID" \
  -H "$(auth_header)" \
  -H "Content-Type: application/json" \
  -d "{\"value\": $RESTORE_VALUE}"

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
echo "================================================"
TOTAL=$(( PASS + FAIL ))
if [[ $FAIL -eq 0 ]]; then
  green "All $TOTAL checks passed"
  exit 0
else
  red "$FAIL/$TOTAL checks failed"
  exit 1
fi

#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Load Test Script — A3 Reactive/Async Pipeline Performance
#
# Fires sensor updates at the sensor service and measures:
#   • throughput  (events processed / wall-clock time)
#   • latency     (average processing time from metrics endpoint)
#   • CPU/memory  (docker stats snapshot)
#   • error rate  (non-2xx responses)
#
# Usage:
#   ./scripts/load_test.sh                 # default: Go stack, all sizes
#   ./scripts/load_test.sh --stack python   # Python stack
#   ./scripts/load_test.sh --size small     # single size
#   PIPELINE_MODE=async WORKER_COUNT=4 docker compose up -d --build
#   ./scripts/load_test.sh                 # re-run against async
# ──────────────────────────────────────────────────────────────
set -e

# ── Defaults ──────────────────────────────────────────────────
STACK="${STACK:-go}"          # "go" or "python"
SIZE=""                       # "" = run all sizes
TOKEN="${API_TOKEN:-your-secret-token}"
RESULTS_DIR="results"

# ── Parse flags ───────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack)  STACK="$2";  shift 2 ;;
    --size)   SIZE="$2";   shift 2 ;;
    *)        echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# ── Stack-specific ports ──────────────────────────────────────
if [[ "$STACK" == "go" ]]; then
  SENSOR_BASE="http://localhost:8080"
  ALERT_BASE="http://localhost:8081"
  METRICS_URL="http://localhost:9091/metrics"
  ALERT_CONTAINER="a3-go-alert-service-1"
elif [[ "$STACK" == "python" ]]; then
  SENSOR_BASE="http://localhost:8000"
  ALERT_BASE="http://localhost:8002"
  METRICS_URL="http://localhost:9092/metrics"
  ALERT_CONTAINER="a3-python-alert-service-1"
else
  echo "ERROR: --stack must be 'go' or 'python'"
  exit 1
fi

# ── Dataset sizes ─────────────────────────────────────────────
declare -A SIZES
SIZES[small]=50
SIZES[medium]=500
SIZES[large]=5000

# ── Helpers ───────────────────────────────────────────────────
auth_header() {
  echo "Authorization: Bearer $TOKEN"
}

get_metric() {
  # Extract a single metric value from Prometheus text output.
  # Usage: get_metric "events_processed_total"
  local name="$1"
  curl -s "$METRICS_URL" | grep "^${name} " | awk '{print $2}' || echo "0"
}

capture_docker_stats() {
  # Single-shot docker stats for the alert container.
  docker stats --no-stream --format '{{.CPUPerc}}\t{{.MemUsage}}' "$ALERT_CONTAINER" 2>/dev/null || echo "N/A\tN/A"
}

wait_for_processing() {
  # Wait until events_processed_total reaches the expected count.
  local expected="$1"
  local timeout=60
  local elapsed=0
  while [[ "$elapsed" -lt "$timeout" ]]; do
    local processed
    processed=$(get_metric "events_processed_total")
    if [[ "${processed%.*}" -ge "$expected" ]]; then
      return 0
    fi
    sleep 1
    elapsed=$(( elapsed + 1 ))
  done
  echo "WARNING: timed out waiting for processing to complete (got $processed, expected $expected)"
  return 0
}

run_load() {
  local size_name="$1"
  local count="${SIZES[$size_name]}"

  echo ""
  echo "═══════════════════════════════════════════════════"
  echo "  $STACK | $size_name ($count events)"
  echo "═══════════════════════════════════════════════════"

  # Snapshot metrics before
  local before_processed
  before_processed=$(get_metric "events_processed_total")
  before_processed="${before_processed:-0}"

  local expected_total
  expected_total=$(( ${before_processed%.*} + count ))

  # Snapshot CPU/memory before
  local stats_before
  stats_before=$(capture_docker_stats)

  # Fire sensor updates concurrently using xargs -P for parallelism.
  local concurrency="${CONCURRENCY:-10}"
  local error_file
  error_file=$(mktemp)

  local start_time
  start_time=$(date +%s%N)

  echo "  Sending $count PUT requests to $SENSOR_BASE/sensors/sensor-001 (concurrency=$concurrency) ..."

  # Export vars so subshells can access them.
  export LOAD_TOKEN="$TOKEN"
  export LOAD_SENSOR_BASE="$SENSOR_BASE"
  export LOAD_ERROR_FILE="$error_file"

  send_one() {
    local i="$1"
    local value=$(( 81 + (i % 19) ))
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
      -X PUT \
      -H "Authorization: Bearer $LOAD_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"value\": ${value}.0}" \
      "$LOAD_SENSOR_BASE/sensors/sensor-001")
    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
      # Append is atomic on POSIX for writes < PIPE_BUF (4096 bytes).
      echo 1 >> "$LOAD_ERROR_FILE"
    fi
  }
  export -f send_one

  seq 1 "$count" | xargs -P "$concurrency" -n1 bash -c 'send_one "$1"' _

  local errors=0
  if [[ -s "$error_file" ]]; then
    errors=$(wc -l < "$error_file")
  fi
  rm -f "$error_file"

  local end_time
  end_time=$(date +%s%N)
  local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
  local elapsed_sec
  elapsed_sec=$(echo "scale=2; $elapsed_ms / 1000" | bc)

  echo "  Sent $count requests in ${elapsed_sec}s (${errors} errors)"
  echo "  Waiting for pipeline to finish processing ..."

  # Wait for the alert pipeline to process all events
  wait_for_processing "$expected_total"

  # Snapshot metrics after
  local after_processed
  after_processed=$(get_metric "events_processed_total")
  local after_received
  after_received=$(get_metric "events_received_total")
  local after_triggered
  after_triggered=$(get_metric "alerts_triggered_total")
  local avg_microseconds
  avg_microseconds=$(get_metric "event_processing_avg_microseconds")

  # Snapshot CPU/memory after
  local stats_after
  stats_after=$(capture_docker_stats)
  local cpu_after
  cpu_after=$(echo "$stats_after" | cut -f1)
  local mem_after
  mem_after=$(echo "$stats_after" | cut -f2)

  # Compute events actually processed in this run
  local processed_this_run
  processed_this_run=$(( ${after_processed%.*} - ${before_processed%.*} ))

  # Compute throughput (events / second)
  local throughput
  if [[ "$elapsed_ms" -gt 0 ]]; then
    throughput=$(echo "scale=1; $processed_this_run * 1000 / $elapsed_ms" | bc)
  else
    throughput="N/A"
  fi

  # Compute error rate
  local error_rate
  if [[ "$count" -gt 0 ]]; then
    error_rate=$(echo "scale=1; $errors * 100 / $count" | bc)
  else
    error_rate="0.0"
  fi

  # Read pipeline mode from metrics
  local mode
  mode=$(curl -s "$METRICS_URL" | grep '^pipeline_info' | grep -o 'mode="[^"]*"' | cut -d'"' -f2)
  local workers
  workers=$(curl -s "$METRICS_URL" | grep '^pipeline_info' | grep -o 'worker_count="[^"]*"' | cut -d'"' -f2)

  echo ""
  echo "  ┌─────────────────────────────────────────────┐"
  echo "  │ Results: $STACK / $size_name / ${mode:-unknown} (workers: ${workers:-N/A})"
  echo "  ├─────────────────────────────────────────────┤"
  printf "  │ %-22s %20s │\n" "Events sent:"       "$count"
  printf "  │ %-22s %20s │\n" "Events processed:"  "$processed_this_run"
  printf "  │ %-22s %20s │\n" "Alerts triggered:"  "$after_triggered"
  printf "  │ %-22s %18s s │\n" "Wall clock:"       "$elapsed_sec"
  printf "  │ %-22s %16s /s │\n" "Throughput:"       "$throughput"
  printf "  │ %-22s %17s µs │\n" "Avg latency:"     "$avg_microseconds"
  printf "  │ %-22s %19s%% │\n" "Error rate:"       "$error_rate"
  printf "  │ %-22s %20s │\n" "CPU (peak):"        "$cpu_after"
  printf "  │ %-22s %20s │\n" "Memory:"            "$mem_after"
  echo "  └─────────────────────────────────────────────┘"

  # Append to CSV
  echo "$STACK,$size_name,$mode,$workers,$count,$processed_this_run,$elapsed_sec,$throughput,$avg_microseconds,$error_rate,$cpu_after,$mem_after" >> "$RESULTS_DIR/results.csv"
}

# ── Main ──────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"

# Write CSV header if file doesn't exist
if [[ ! -f "$RESULTS_DIR/results.csv" ]]; then
  echo "stack,size,mode,workers,events_sent,events_processed,wall_clock_s,throughput_per_s,avg_latency_us,error_rate_pct,cpu_peak,memory" > "$RESULTS_DIR/results.csv"
fi

echo "╔═══════════════════════════════════════════════════╗"
echo "║        A3 Pipeline Load Test                     ║"
echo "║  Stack: $STACK                                       ║"
echo "║  Metrics: $METRICS_URL             ║"
echo "╚═══════════════════════════════════════════════════╝"

# Verify services are reachable
echo ""
echo "Checking service health ..."
curl -sf -H "$(auth_header)" "$SENSOR_BASE/health" > /dev/null || { echo "ERROR: sensor service at $SENSOR_BASE not reachable"; exit 1; }
curl -sf -H "$(auth_header)" "$ALERT_BASE/health" > /dev/null || { echo "ERROR: alert service at $ALERT_BASE not reachable"; exit 1; }
curl -sf "$METRICS_URL" > /dev/null || { echo "ERROR: metrics endpoint at $METRICS_URL not reachable"; exit 1; }
echo "All services healthy."

if [[ -n "$SIZE" ]]; then
  run_load "$SIZE"
else
  for s in small medium large; do
    run_load "$s"
  done
fi

echo ""
echo "Results appended to $RESULTS_DIR/results.csv"
echo "Done."

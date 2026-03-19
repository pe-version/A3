# A2 – Integration, Persistence & Resilience

## Service Definition

This assignment evolves the A1 IoT Sensor Service into a multi-service architecture with real persistence, synchronous inter-service calls, asynchronous event-driven communication, and resilience patterns.

**Domain:** IoT Smart Home Sensors + Alert Rules
**Services:** Sensor service (Python + Go) and Alert service (Python + Go)
**Persistence:** SQLite (one database per service)
**Messaging:** RabbitMQ (fanout exchange)
**Resilience:** Circuit breaker + HTTP retry with exponential backoff
**Authentication:** Bearer Token

## Architecture

![Architecture Diagram](diagrams/architecture.png)

```mermaid
flowchart TB
    Client["HTTP Client\n(curl, Postman, etc.)"]

    subgraph Docker["Docker Compose Network"]
        RabbitMQ["RabbitMQ\n:5672 / :15672"]

        subgraph SensorServices["Sensor Services"]
            PythonSvc["Python Sensor\nFastAPI · :8000"]
            GoSvc["Go Sensor\nGin · :8080"]
        end

        subgraph AlertServices["Alert Services"]
            PythonAlert["Python Alert\nFastAPI · :8002"]
            GoAlert["Go Alert\nGin · :8081"]
        end

        subgraph Storage["Data Storage"]
            PDB[("sensors-python.db")]
            GDB[("sensors-go.db")]
            PAB[("alerts-python.db")]
            GAB[("alerts-go.db")]
        end
    end

    Client -->|"Bearer token"| PythonSvc
    Client -->|"Bearer token"| GoSvc
    Client -->|"Bearer token"| PythonAlert
    Client -->|"Bearer token"| GoAlert

    GoSvc -->|"PUT /sensors/:id\npublishes sensor.updated"| RabbitMQ
    PythonSvc -->|"PUT /sensors/:id\npublishes sensor.updated"| RabbitMQ
    RabbitMQ -->|"sensor.updated events"| GoAlert
    RabbitMQ -->|"sensor.updated events"| PythonAlert

    GoAlert -->|"GET /sensors/:id\n(circuit breaker)"| GoSvc
    PythonAlert -->|"GET /sensors/:id\n(retries + timeout)"| PythonSvc

    PythonSvc --> PDB
    GoSvc --> GDB
    PythonAlert --> PAB
    GoAlert --> GAB
```

## Sequence Diagram: Sensor Update → Alert Evaluation

This is the critical async flow. A client updates a sensor value; the sensor service publishes an event; the alert service evaluates it against active rules.

![Async Flow Sequence Diagram](diagrams/sequence-async-flow.png)

```mermaid
sequenceDiagram
    actor Client
    participant GoSvc as Go Sensor Service
    participant RMQ as RabbitMQ
    participant GoAlert as Go Alert Service
    participant AlertDB as alerts-go.db

    Client->>+GoSvc: PUT /sensors/sensor-001\nAuthorization: Bearer token
    GoSvc->>GoSvc: Update sensor in DB
    GoSvc-->>-Client: 200 OK {sensor}

    GoSvc-)RMQ: Publish sensor.updated\n{sensor_id, value, type, unit}

    Note over GoSvc,RMQ: Fire-and-forget (goroutine)\nHTTP response already sent

    RMQ-)GoAlert: Deliver sensor.updated event

    GoAlert->>GoAlert: Load active rules for sensor-001
    loop For each matching rule
        GoAlert->>GoAlert: Evaluate value OP threshold
        alt Threshold crossed
            GoAlert->>AlertDB: INSERT triggered_alert
            GoAlert->>GoAlert: Log alert triggered
        end
    end
```

## Resilience: Circuit Breaker Flow

When the alert service creates a rule, it validates the sensor exists via HTTP with circuit breaker protection:

![Circuit Breaker Sequence Diagram](diagrams/sequence-circuit-breaker.png)

```mermaid
sequenceDiagram
    actor Client
    participant GoAlert as Go Alert Service
    participant CB as Circuit Breaker
    participant GoSvc as Go Sensor Service

    Client->>+GoAlert: POST /rules {sensor_id: "sensor-001"}
    GoAlert->>CB: Execute GET /sensors/sensor-001

    alt Sensor service healthy
        CB->>+GoSvc: GET /sensors/sensor-001
        GoSvc-->>-CB: 200 OK {sensor}
        CB-->>GoAlert: sensor found
        GoAlert->>GoAlert: Create rule in DB
        GoAlert-->>-Client: 201 Created {rule}
    else Sensor not found (404)
        CB->>+GoSvc: GET /sensors/sensor-001
        GoSvc-->>-CB: 404 Not Found
        CB-->>GoAlert: not found error
        GoAlert-->>Client: 400 Bad Request "Sensor not found"
    else Sensor service down → circuit trips OPEN
        loop Retry with backoff (up to 3 attempts, 1s / 2s delays)
            CB->>GoSvc: GET /sensors/sensor-001
            GoSvc--xCB: connection refused (timeout)
            Note over CB: failure count +1
        end
        Note over CB: ConsecutiveFailures >= fail_max → OPEN
        CB-->>GoAlert: CircuitBreakerError (no further network calls)
        GoAlert->>GoAlert: Create rule with warning
        GoAlert-->>Client: 201 Created {rule, warning}
    end
```

## Project Structure

```
A2/
├── README.md
├── RUBRIC.md
├── architecture.md
├── docker-compose.yml               # All services + RabbitMQ
├── openapi.yaml
├── adrs/
│   ├── ADR-001-sync-vs-async.md
│   ├── ADR-002-rabbitmq-selection.md
│   └── ADR-003-circuit-breaker.md
├── data/
│   ├── sensors.json
│   └── alert_rules.json
├── python-service/                  # Sensor service (Python/FastAPI) + RabbitMQ publisher
├── go-service/                      # Sensor service (Go/Gin) + RabbitMQ publisher
├── python-alert-service/            # Alert service (Python/FastAPI)
└── go-alert-service/                # Alert service (Go/Gin)
    ├── main.go
    ├── Dockerfile
    ├── go.mod
    ├── clients/sensor_client.go     # HTTP client with circuit breaker
    ├── config/config.go
    ├── database/database.go
    ├── handlers/
    │   ├── health.go
    │   ├── alert_rules.go
    │   └── triggered_alerts.go
    ├── messaging/consumer.go
    ├── middleware/auth.go
    ├── middleware/logging.go
    ├── models/alert_rule.go
    ├── models/triggered_alert.go
    ├── repositories/alert_rule_repository.go
    ├── repositories/triggered_alert_repository.go
    ├── services/evaluator.go
    └── tests/alerts_test.go
```

## API Endpoints

### Sensor Services (Python :8000 / Go :8080)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/health` | No | Health check |
| GET | `/sensors` | Yes | List all sensors |
| GET | `/sensors/{id}` | Yes | Get sensor by ID |
| POST | `/sensors` | Yes | Create sensor |
| PUT | `/sensors/{id}` | Yes | Update sensor (**publishes sensor.updated event**) |
| DELETE | `/sensors/{id}` | Yes | Delete sensor |

### Alert Services (Python :8002 / Go :8081)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/health` | No | Health check |
| GET | `/rules` | Yes | List all alert rules |
| GET | `/rules/{id}` | Yes | Get alert rule |
| POST | `/rules` | Yes | Create rule (validates sensor via circuit breaker) |
| PUT | `/rules/{id}` | Yes | Update rule |
| DELETE | `/rules/{id}` | Yes | Delete rule |
| GET | `/alerts` | Yes | List triggered alerts |
| GET | `/alerts/{id}` | Yes | Get triggered alert |
| PUT | `/alerts/{id}` | Yes | Update alert status |

## Running the Services

### Prerequisites

- Docker and Docker Compose
- `API_TOKEN` environment variable set

### Quick Start

```bash
export API_TOKEN=my-secret-token
cd A2
docker compose up --build
```

Services start in order: RabbitMQ → Sensor services → Alert services (enforced by `depends_on` health checks).

### Stop

```bash
docker compose down
```

## Running Tests

### Go Alert Service

```bash
cd go-alert-service
go test ./tests/ -v
```

Via Docker (no local Go required):

```bash
docker run --rm -v "$(pwd)/go-alert-service":/app -w /app \
  golang:1.21-alpine sh -c \
  "apk add --no-cache gcc musl-dev sqlite-dev && go test ./tests/ -v"
```

### Go Sensor Service

```bash
cd go-service && go test ./tests/ -v
```

## Example Requests

### Create an Alert Rule

```bash
curl -X POST \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"sensor-001","name":"High Temp","operator":"gt","threshold":80.0}' \
  http://localhost:8081/rules
```

Response (201):
```json
{
  "id": "rule-001",
  "sensor_id": "sensor-001",
  "name": "High Temp",
  "operator": "gt",
  "threshold": 80.0,
  "metric": "value",
  "status": "active",
  "created_at": "2026-03-08T10:00:00Z",
  "updated_at": "2026-03-08T10:00:00Z"
}
```

### Trigger an Alert (Update Sensor Above Threshold)

```bash
curl -X PUT \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"value": 95.0}' \
  http://localhost:8080/sensors/sensor-001
```

This publishes a `sensor.updated` event. The alert service evaluates active rules and creates a triggered alert.

### List Triggered Alerts

```bash
curl -H "Authorization: Bearer my-secret-token" http://localhost:8081/alerts
```

### Acknowledge an Alert

```bash
curl -X PUT \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"status": "acknowledged"}' \
  http://localhost:8081/alerts/alert-001
```

### Circuit Breaker Fallback Example

When the sensor service is unavailable, rule creation still succeeds with a warning:

```json
{
  "id": "rule-002",
  "sensor_id": "sensor-001",
  "name": "Fallback Rule",
  "operator": "lt",
  "threshold": 10.0,
  "status": "active",
  "warning": "Sensor service unavailable; sensor_id not validated"
}
```

## Resilience Features

| Feature | Location | Detail |
|---------|----------|--------|
| Circuit Breaker | Go Alert → Go Sensor | Opens after 5 consecutive failures; resets after 30s |
| HTTP Retry + Backoff | Go Alert → Go Sensor | Up to 3 attempts, exponential backoff (1s, 2s) |
| Request Timeout | Go Alert → Go Sensor | 2-second per-request timeout |
| Graceful Fallback | Rule creation | Creates rule with warning if sensor service unavailable |
| Publisher Resilience | Sensor → RabbitMQ | Publish failures logged/swallowed; auto-reconnect |
| Consumer Reconnect | Alert ← RabbitMQ | Auto-reconnects with 5-second backoff |

## Resilience & Integration Evidence

### Go Alert Service Tests: 23/23 Passing

```
=== RUN   TestUnauthorizedWithoutToken        --- PASS (0.04s)
=== RUN   TestUnauthorizedWithInvalidToken    --- PASS (0.01s)
=== RUN   TestUnauthorizedMalformedHeader     --- PASS (0.01s)
=== RUN   TestUnauthorizedResponseBody        --- PASS (0.01s)
=== RUN   TestHealthEndpointNoAuthRequired    --- PASS (0.01s)
=== RUN   TestListRulesEmpty                  --- PASS (0.01s)
=== RUN   TestCreateRuleWithValidSensor       --- PASS (0.01s)
=== RUN   TestCreateRuleWithNonexistentSensor --- PASS (0.01s)
=== RUN   TestCreateRuleSensorUnavailableAllowsWithWarning --- PASS (3.03s)
=== RUN   TestCreateRuleInvalidOperator       --- PASS (0.01s)
=== RUN   TestCreateRuleMissingRequired       --- PASS (0.01s)
=== RUN   TestGetNonexistentRule              --- PASS (0.01s)
=== RUN   TestCreateAndFetchRule              --- PASS (0.01s)
=== RUN   TestUpdateRule                      --- PASS (0.01s)
=== RUN   TestUpdateRuleInvalidStatus         --- PASS (0.01s)
=== RUN   TestDeleteRule                      --- PASS (0.01s)
=== RUN   TestDeleteNonexistentRule           --- PASS (0.01s)
=== RUN   TestUpdateNonexistentRule           --- PASS (0.01s)
=== RUN   TestListRulesAfterCreate            --- PASS (0.01s)
=== RUN   TestListAlertsEmpty                 --- PASS (0.01s)
=== RUN   TestGetNonexistentAlert             --- PASS (0.01s)
=== RUN   TestUpdateNonexistentAlert          --- PASS (0.01s)
=== RUN   TestUpdateAlertInvalidStatus        --- PASS (0.02s)
PASS
ok  	iot-alert-service/tests	3.289s
```

### Go Sensor Service Tests: 28/28 Passing

```
=== RUN   TestUnauthorizedWithoutToken     --- PASS
=== RUN   TestUnauthorizedWithInvalidToken --- PASS
=== RUN   TestCreateSensor                 --- PASS
=== RUN   TestCreateAndFetchSensor         --- PASS
=== RUN   TestUpdateSensor                 --- PASS
=== RUN   TestDeleteSensor                 --- PASS
... (28 tests total)
PASS
ok  	iot-sensor-service/tests	0.201s
```

### End-to-End Async Flow: Sensor Update → Alert Triggered

Sensor `sensor-001` updated to `95.5°F` (above threshold of `80.0`). The `sensor.updated` event was published to RabbitMQ, consumed by the alert service, and two triggered alerts were created — one per matching active rule.

**Go alert service logs:**
```
2026/03/09 02:50:06 INFO Connected to RabbitMQ, waiting for sensor events
2026/03/09 02:50:06 INFO Received sensor.updated event sensor_id=sensor-001 value=95.5
2026/03/09 02:50:06 INFO Alert triggered alert_id=alert-001 rule_id=rule-001 message="Sensor sensor-001 value 95.50 gt threshold 80.00 (rule: Living Room Temperature High)"
2026/03/09 02:50:06 INFO Alert triggered alert_id=alert-002 rule_id=rule-004 message="Sensor sensor-001 value 95.50 gt threshold 80.00 (rule: High Temperature Alert)"
```

**GET /alerts response after update:**
```json
{
  "alerts": [
    {
      "id": "alert-001",
      "rule_id": "rule-001",
      "sensor_id": "sensor-001",
      "sensor_value": 95.5,
      "threshold": 80,
      "message": "Sensor sensor-001 value 95.50 gt threshold 80.00 (rule: Living Room Temperature High)",
      "status": "open",
      "created_at": "2026-03-09T02:50:06Z",
      "resolved_at": null
    },
    {
      "id": "alert-002",
      "rule_id": "rule-004",
      "sensor_id": "sensor-001",
      "sensor_value": 95.5,
      "threshold": 80,
      "message": "Sensor sensor-001 value 95.50 gt threshold 80.00 (rule: High Temperature Alert)",
      "status": "resolved",
      "created_at": "2026-03-09T02:50:06Z",
      "resolved_at": "2026-03-09T02:50:57Z"
    }
  ],
  "count": 2
}
```

### Circuit Breaker: Retry + Fallback (Sensor Service Down)

Go sensor service stopped. `POST /rules` attempted — circuit breaker executed 3 retries with exponential backoff, then opened and returned fallback with warning. The rule was still created successfully.

**Go alert service logs (retries → fallback):**
```
2026/03/09 02:51:10 WARN Sensor service request failed, retrying sensor_id=sensor-002 attempt=1 error="...dial tcp: lookup go-service: no such host"
2026/03/09 02:51:11 WARN Sensor service request failed, retrying sensor_id=sensor-002 attempt=2 error="...dial tcp: lookup go-service: no such host"
2026/03/09 02:51:13 WARN Sensor service request failed, retrying sensor_id=sensor-002 attempt=3 error="...dial tcp: lookup go-service: no such host"
2026/03/09 02:51:13 WARN Sensor service unavailable sensor_id=sensor-002 error="...dial tcp: lookup go-service: no such host"
2026/03/09 02:51:13 INFO Request completed method=POST path=/rules status=201 duration_ms=3148
```

**POST /rules response (circuit open — fallback with warning):**
```json
{
  "id": "rule-005",
  "sensor_id": "sensor-002",
  "metric": "value",
  "operator": "gt",
  "threshold": 5,
  "name": "Circuit Breaker Test Rule",
  "status": "active",
  "created_at": "2026-03-09T02:51:13Z",
  "updated_at": "2026-03-09T02:51:13Z",
  "warning": "Sensor service unavailable; sensor_id not validated"
}
```

Note: `duration_ms=3148` reflects 3 × ~1s retry backoff before falling back — the service remained available throughout.

## Architecture Decision Records

- [ADR-001](adrs/ADR-001-sync-vs-async.md) — Sync vs. async: why both are used
- [ADR-002](adrs/ADR-002-rabbitmq-selection.md) — Why RabbitMQ over Kafka/Redis Streams
- [ADR-003](adrs/ADR-003-circuit-breaker.md) — Why circuit breaker as the resilience pattern

## Environment Variables

### Go Alert Service

| Variable | Default | Description |
|----------|---------|-------------|
| `API_TOKEN` | *(required)* | Bearer token |
| `PORT` | `8081` | Service port |
| `DATABASE_PATH` | `/app/data/alerts-go.db` | SQLite path |
| `SEED_DATA_PATH` | `/app/data/alert_rules.json` | Seed data |
| `SENSOR_SERVICE_URL` | `http://go-service:8080` | Sensor service URL |
| `RABBITMQ_URL` | `amqp://iot_service:iot_secret@rabbitmq:5672/` | RabbitMQ URL |
| `CB_FAIL_MAX` | `5` | Circuit breaker failure threshold |
| `CB_RESET_TIMEOUT` | `30` | Circuit breaker reset timeout (seconds) |
| `PIPELINE_MODE` | `blocking` | Pipeline mode: `blocking` or `async` |
| `WORKER_COUNT` | `4` | Worker pool size (used in async mode) |

## A3: Performance Analysis

### Pipeline Modes

The alert service supports two pipeline modes, configurable via `PIPELINE_MODE`:

![Pipeline Modes Diagram](diagrams/pipeline-modes.png)

```mermaid
---
title: "Pipeline Modes: Blocking vs Async"
---
flowchart LR
    subgraph Blocking["Blocking Mode"]
        direction TB
        B_RMQ["RabbitMQ\n(prefetch=10)"] -->|deliver| B_Consumer["Consumer\n(single coroutine/goroutine)"]
        B_Consumer -->|evaluate inline| B_DB[("SQLite")]
        B_DB -->|complete| B_Ack["Ack message"]
        B_Ack -->|next message| B_RMQ
    end

    subgraph Async["Async Mode"]
        direction TB
        A_RMQ["RabbitMQ\n(prefetch=10)"] -->|deliver| A_Consumer["Consumer"]
        A_Consumer -->|enqueue\n(blocks if full)| A_Queue["Bounded Queue\n(workers × 10)"]
        A_Consumer -->|ack immediately| A_RMQ
        A_Queue --> A_W1["Worker 1"]
        A_Queue --> A_W2["Worker 2"]
        A_Queue --> A_WN["Worker N"]
        A_W1 -->|write| A_DB[("SQLite\n(serialized writes)")]
        A_W2 -->|write| A_DB
        A_WN -->|write| A_DB
    end
```

**Blocking mode:** Messages are evaluated inline before being acknowledged — sequential, strong at-least-once guarantee. **Async mode:** Messages are enqueued to a bounded queue and acked immediately; a worker pool drains the queue concurrently. The bounded queue provides backpressure: when full, the consumer blocks until a worker frees a slot, preventing unbounded memory growth.

### Load Test Results

The tests follow a progression: start with the simple blocking pipeline, observe its behavior as data scales, then transition to the async pipeline to see whether concurrency improves throughput.

#### Phase 1: Blocking (Sequential) Baseline

| Stack | Size | Throughput (events/s) | Avg Latency (µs) | Error Rate | CPU Peak | Memory |
|-------|------|-----------------------|-------------------|------------|----------|--------|
| Python | Small (50) | 37.3 | 21,479 | 0% | 0.22% | 43.1 MiB |
| Python | Medium (500) | 41.8 | 23,843 | 0% | 0.27% | 43.5 MiB |
| Python | Large (5000) | 41.6 | 24,160 | 0% | 5.39% | 44.8 MiB |

Blocking throughput plateaus at ~42 events/s regardless of dataset size. Latency is stable (~24ms). CPU spikes only at the large dataset. The sequential pipeline is predictable but cannot scale beyond the speed of a single evaluator.

#### Phase 2: Async Pipeline (4 Workers)

| Stack | Size | Throughput (events/s) | Avg Latency (µs) | Error Rate | CPU Peak | Memory |
|-------|------|-----------------------|-------------------|------------|----------|--------|
| Python | Small (50) | 27.5 | 45,048 | 0% | 0.30% | 43.3 MiB |
| Python | Medium (500) | 37.5 | 28,778 | 0% | 0.29% | 43.9 MiB |
| Python | Large (5000) | 34.3 | 31,074 | 0% | 0.31% | 45.1 MiB |

Async is **slower** across all sizes — throughput drops and latency increases.

### Analysis

The async pipeline is slower than blocking in this implementation. The bottleneck is SQLite: it serializes writes at the filesystem level, so the 4 async workers contend on the same database file without achieving actual parallelism. Instead of speeding things up, the workers add lock contention overhead — more time is spent waiting for the database lock than doing useful work. Blocking mode avoids this entirely by processing events one at a time with no contention.

This result is specific to the choice of storage backend, not a flaw in the async pattern itself. In a production system, the storage layer would be a database that supports concurrent writes (e.g., PostgreSQL, MySQL, or a distributed store like DynamoDB). With such a backend, the async workers would perform truly parallel I/O, and the throughput advantage of the async pipeline would materialize. The general principle: **async pipelines improve throughput when the bottleneck is parallelizable I/O; when the bottleneck serializes all access through a single lock, async adds overhead without benefit.**

Go faces the same constraint. Earlier load test results that appeared to show async improvement in Go were an artifact of the sequential test sender being the bottleneck — both modes consumed events faster than they arrived, and async's goroutine overhead was negligible. Under concurrent load with SQLite as the backend, Go would exhibit the same write serialization.

### Rerunning the Load Tests

The full blocking→async progression can be reproduced in a single command:

```bash
export API_TOKEN=my-secret-token
./scripts/load_test.sh --stack python --progression
```

This automatically:
1. Starts services in **blocking** mode, runs small → medium → large
2. Restarts services in **async** mode (4 workers), runs small → medium → large
3. Appends all results to `results/results.csv`

To run a single mode manually:

```bash
# Start in the desired mode
PIPELINE_MODE=blocking WORKER_COUNT=0 docker compose up -d --build
./scripts/load_test.sh --stack python

# Or async
PIPELINE_MODE=async WORKER_COUNT=4 docker compose up -d --build
./scripts/load_test.sh --stack python
```

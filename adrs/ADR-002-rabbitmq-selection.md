# ADR-002: Message Broker Selection — RabbitMQ

**Date:** 2026-03-08
**Status:** Accepted

## Context

The async flow requires a message broker to carry `sensor.updated` events from the sensor service to the alert service(s). Candidates evaluated:

| Broker | Model | Complexity | Use case fit |
|--------|-------|------------|-------------|
| **RabbitMQ** | Push (AMQP) | Low–Medium | Task queues, fanout, routing |
| **Kafka** | Pull (log) | High | High-throughput event streaming, replay |
| **Redis Streams** | Pull (log-lite) | Low | Simple pub/sub, cache-adjacent |

## Decision

Use **RabbitMQ** with a `fanout` exchange named `sensor_events`.

## Rationale

### RabbitMQ fits the workload

Sensor update events are discrete, low-to-moderate frequency notifications (not a continuous high-throughput stream). RabbitMQ's push model with durable queues matches this pattern well: events are delivered promptly, and multiple independent queues (`alert_service_go`, `alert_service_python`) each receive their own copy via the fanout exchange.

### Kafka is over-engineered for this scale

Kafka excels at very high throughput (millions of events/day) and long-term log retention for replay. For a smart home sensor system with a handful of devices, Kafka's operational overhead (ZooKeeper/KRaft, partition management, consumer group offsets) is not justified.

### Redis Streams is viable but less expressive

Redis Streams would work and is simpler operationally. However, RabbitMQ provides richer routing primitives (topic exchanges, dead-letter queues) that would be useful if the system grows to route events by sensor type or severity.

### Fanout exchange design

Using a fanout exchange decouples producers from consumers entirely. The sensor service publishes to the exchange without knowing which services consume the events. Each alert service binds its own durable queue to the exchange. Adding a new consumer requires zero changes to the sensor service.

## Consequences

- RabbitMQ must be deployed and healthy before the sensor service and alert service start (enforced via `depends_on: condition: service_healthy` in docker-compose).
- The sensor service must tolerate RabbitMQ unavailability — publish failures are logged and swallowed so HTTP responses are unaffected.
- RabbitMQ's management UI (port 15672) is exposed in docker-compose for observability during development.

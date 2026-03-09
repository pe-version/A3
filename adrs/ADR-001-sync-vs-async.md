# ADR-001: Synchronous vs. Asynchronous Communication

**Date:** 2026-03-08
**Status:** Accepted

## Context

The A2 architecture requires two services to collaborate: a sensor service that stores sensor readings, and an alert service that evaluates those readings against threshold rules. Two interaction patterns were available:

- **Synchronous (REST):** The alert service periodically polls or the sensor service calls the alert service inline during a sensor update.
- **Asynchronous (message queue):** The sensor service emits an event after each update; the alert service consumes it independently.

A third use case also emerged: when creating an alert rule, the alert service must verify that the referenced sensor actually exists. This is inherently synchronous — it is a validation check that must succeed or fail before the rule is persisted.

## Decision

Use **both** patterns, each where it fits best:

- **Asynchronous (RabbitMQ fanout)** for sensor update → alert evaluation. When a sensor's value changes, the sensor service publishes a `sensor.updated` event. The alert service consumes it and evaluates rules.
- **Synchronous (HTTP with circuit breaker)** for sensor existence validation at rule creation time.

## Rationale

### Why async for sensor update → alert evaluation

Sensor updates may occur at high frequency. Evaluating alerts synchronously inside the `PUT /sensors/:id` handler would add latency to every update and create tight coupling — the sensor service would need to know the alert service's address, and a slow or unavailable alert service would block sensor writes.

Using a fanout exchange means:
- The sensor service remains unaware of consumers; new services can subscribe without any code changes.
- Sensor write latency is unaffected by alert evaluation time.
- If the alert service is temporarily unavailable, RabbitMQ queues the events and they are processed upon recovery.
- Multiple alert services (Python and Go) can consume the same events independently.

### Why sync for sensor validation

Rule creation is a user-initiated action (not high-frequency). Validating that the sensor exists before persisting a rule provides an immediate, clear error response. This is qualitatively different from fire-and-forget event processing. The circuit breaker fallback preserves availability even when the sensor service is down — the rule is created with a warning rather than being rejected.

## Consequences

- Sensor updates gain resilience and decoupling at the cost of eventual consistency — alerts may be evaluated milliseconds after the sensor value is written.
- Alert rule creation requires the sensor service to be reachable (or tolerates its unavailability via circuit breaker fallback).
- RabbitMQ becomes a required infrastructure component.

# ADR-003: Resilience Pattern — Circuit Breaker for Inter-Service Calls

**Date:** 2026-03-08
**Status:** Accepted

## Context

The Go alert service makes synchronous HTTP calls to the Go sensor service when creating alert rules (to validate that a referenced sensor exists). If the sensor service is unavailable or slow, these calls will hang or fail. Without a resilience pattern, repeated failures would:

1. Degrade response times for alert rule creation (threads blocked waiting for timeouts)
2. Potentially cascade if many concurrent requests pile up

Three resilience patterns were considered:

| Pattern | What it does |
|---------|-------------|
| **Circuit Breaker** | Stops calling a failing service after N consecutive failures; tries again after a reset timeout |
| **Bulkhead** | Limits concurrent requests to a downstream service to prevent resource exhaustion |
| **Rate Limiter** | Caps requests per time window, protecting the downstream service |

## Decision

Implement a **circuit breaker** on the alert service's HTTP client for sensor service calls, using the `github.com/sony/gobreaker` library.

Additionally, the HTTP client uses:
- **Timeout:** 2-second per-request timeout
- **Retry with exponential backoff:** Up to 3 attempts before the circuit breaker registers a failure

## Rationale

### Circuit breaker is the best fit for this failure mode

The primary failure mode is the sensor service being temporarily unavailable (crash, restart, network partition). A circuit breaker directly addresses this: after 5 consecutive failures, the breaker opens and immediately returns an error without attempting the network call, preserving alert service responsiveness. After a 30-second reset timeout, it allows one probe request to test recovery.

Bulkhead would protect against resource exhaustion (too many concurrent callers) but doesn't help when the downstream service itself is down.

Rate limiting protects the sensor service from being overwhelmed, which is not the concern here — the alert service calls the sensor service infrequently (only at rule creation time).

### Fallback behavior preserves availability

A key design choice: when the circuit is open (sensor service unavailable), the alert service **still creates the rule** and returns a `warning` field in the response rather than rejecting the request. This means alert rules can be configured even during sensor service maintenance windows, at the cost of skipping sensor existence validation.

### `sony/gobreaker` is well-suited

The library is lightweight, well-maintained, and already used in the alert service's dependency tree. It provides the state machine, failure counting, and reset timeout with minimal configuration.

## Consequences

- Alert rules may reference sensor IDs that don't exist (if created while the circuit is open). This is an accepted trade-off documented via the `warning` field.
- The circuit breaker's state is in-process (not shared across alert service instances). In a multi-instance deployment, each instance maintains its own breaker state.
- The 2-second HTTP timeout and 3 retries mean the worst-case latency for a single create-rule call before the circuit opens is ~6 seconds (3 × 2s). This is acceptable for a low-frequency admin operation.

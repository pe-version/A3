"""Sensor service client with circuit breaker and retry logic."""

import logging
import time

import httpx
import pybreaker

logger = logging.getLogger("alert_service")


class _LogListener(pybreaker.CircuitBreakerListener):
    """Logs circuit breaker state transitions."""

    def state_change(self, cb, old_state, new_state):
        logger.warning(
            "Circuit breaker state changed name=%s from=%s to=%s",
            cb.name,
            str(old_state),
            str(new_state),
        )


class SensorClient:
    """HTTP client for the sensor service with resilience patterns.

    Uses a circuit breaker (pybreaker) to prevent cascade failures.
    Retry with exponential backoff is applied *inside* the circuit breaker
    call so the entire retry sequence counts as one failure toward fail_max.
    Falls back gracefully when the sensor service is unavailable.
    """

    def __init__(
        self,
        base_url: str,
        api_token: str,
        cb_fail_max: int = 5,
        cb_reset_timeout: int = 30,
    ):
        self.base_url = base_url
        self.api_token = api_token
        self.breaker = pybreaker.CircuitBreaker(
            fail_max=cb_fail_max,
            reset_timeout=cb_reset_timeout,
            name="sensor-service",
            listeners=[_LogListener()],
        )

    def _make_request_with_retry(self, sensor_id: str, max_retries: int = 3) -> dict | None:
        """Make HTTP request with retry and exponential backoff.

        Runs inside the circuit breaker call so the full retry sequence
        counts as one attempt toward fail_max. Backoff: 1s, 2s between attempts.
        Timeout per request: 2 seconds.
        """
        last_err: Exception | None = None
        for attempt in range(max_retries):
            backoff = 0
            if attempt > 0:
                backoff = 2 ** (attempt - 1)  # 1s, 2s
                time.sleep(backoff)
            try:
                response = httpx.get(
                    f"{self.base_url}/sensors/{sensor_id}",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    timeout=2.0,
                )
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_err = e
                logger.warning(
                    "Sensor service request failed, retrying sensor_id=%s attempt=%d max_retries=%d backoff_ms=%d error=%s",
                    sensor_id,
                    attempt + 1,
                    max_retries,
                    backoff * 1000,
                    str(e),
                )
        logger.warning(
            "All retries exhausted for sensor service sensor_id=%s max_retries=%d error=%s",
            sensor_id,
            max_retries,
            str(last_err),
        )
        raise last_err

    def get_sensor(self, sensor_id: str) -> tuple[dict | None, bool]:
        """Get sensor data via circuit breaker.

        Returns:
            A tuple of (sensor_data, is_validated) with three possible outcomes:
            - (sensor_data, True): Sensor found and validated successfully
            - (None, True): Sensor confirmed not found (404 from sensor service)
            - (None, False): Sensor service unavailable — fallback, not validated
        """
        try:
            result = self.breaker.call(self._make_request_with_retry, sensor_id)
            return result, True
        except pybreaker.CircuitBreakerError:
            logger.warning(
                "Circuit breaker is OPEN — sensor service unavailable sensor_id=%s",
                sensor_id,
            )
            return None, False
        except Exception as e:
            logger.warning(
                "Sensor service unavailable sensor_id=%s error=%s",
                sensor_id,
                str(e),
            )
            return None, False

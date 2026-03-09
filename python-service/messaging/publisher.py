"""RabbitMQ event publisher for sensor update events."""

import json
import logging
from datetime import datetime, timezone

import pika

logger = logging.getLogger("sensor_service")


class EventPublisher:
    """Publishes sensor events to RabbitMQ.

    Uses a fanout exchange so all bound queues (e.g. alert services)
    receive every event. Reconnects automatically on failure.
    """

    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self._connection = None
        self._channel = None

    def connect(self):
        """Establish connection to RabbitMQ and declare the exchange.

        Tolerates failure — logs a warning and leaves the connection as None
        so publish_sensor_updated can retry on the next call.
        """
        try:
            params = pika.URLParameters(self.rabbitmq_url)
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange="sensor_events", exchange_type="fanout", durable=True
            )
            logger.info("Connected to RabbitMQ for publishing")
        except Exception as e:
            logger.warning(
                "RabbitMQ not available at startup — will retry on first publish: %s", str(e)
            )
            self._connection = None
            self._channel = None

    def publish_sensor_updated(
        self, sensor_id: str, value: float, sensor_type: str, unit: str
    ) -> None:
        """Publish a sensor.updated event to the sensor_events exchange.

        Args:
            sensor_id: ID of the updated sensor.
            value: New sensor value.
            sensor_type: Type of sensor (temperature, humidity, etc.).
            unit: Unit of measurement.
        """
        event = {
            "event": "sensor.updated",
            "sensor_id": sensor_id,
            "value": value,
            "type": sensor_type,
            "unit": unit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            if not self._channel or self._channel.is_closed:
                self.connect()

            self._channel.basic_publish(
                exchange="sensor_events",
                routing_key="",
                body=json.dumps(event),
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            logger.info(
                "Published sensor.updated event",
                extra={"sensor_id": sensor_id, "value": value},
            )
        except Exception as e:
            logger.warning("Failed to publish event: %s", str(e))
            self._connection = None
            self._channel = None

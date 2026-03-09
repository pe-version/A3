"""RabbitMQ consumer for sensor update events."""

import json
import logging
import threading
import time

import pika

logger = logging.getLogger("alert_service")


class AlertConsumer:
    """Consumes sensor.updated events from RabbitMQ.

    Runs in a background daemon thread so it doesn't block the FastAPI
    event loop. Automatically reconnects on connection failure.
    """

    def __init__(self, rabbitmq_url: str, callback):
        """Initialize the consumer.

        Args:
            rabbitmq_url: AMQP connection URL.
            callback: Function to call with each sensor event dict.
        """
        self.rabbitmq_url = rabbitmq_url
        self.callback = callback
        self._thread = None

    def start(self):
        """Start consuming in a background daemon thread."""
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("Alert consumer started")

    def _consume_loop(self):
        """Reconnect loop — retries on connection failure."""
        while True:
            try:
                self._consume()
            except Exception as e:
                logger.error("Consumer connection lost: %s — reconnecting in 5s", str(e))
                time.sleep(5)

    def _consume(self):
        """Connect to RabbitMQ and consume sensor.updated events."""
        params = pika.URLParameters(self.rabbitmq_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(exchange="sensor_events", exchange_type="fanout", durable=True)
        channel.queue_declare(queue="alert_service_python", durable=True)
        channel.queue_bind(exchange="sensor_events", queue="alert_service_python")

        logger.info("Connected to RabbitMQ, waiting for sensor events")

        # auto_ack=False: ack only after successful processing so messages are
        # not lost if the evaluator crashes mid-flight.
        channel.basic_consume(
            queue="alert_service_python",
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        channel.start_consuming()

    def _on_message(self, ch, method, properties, body):
        """Handle incoming message."""
        try:
            event = json.loads(body)
            if event.get("event") == "sensor.updated":
                logger.info(
                    "Received sensor.updated event",
                    extra={"sensor_id": event.get("sensor_id"), "value": event.get("value")},
                )
                self.callback(event)
            # Ack after successful processing (including unknown event types).
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.warning("Received invalid JSON message")
            # Nack without requeue — malformed messages cannot be retried usefully.
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

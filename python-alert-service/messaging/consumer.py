"""RabbitMQ consumer for sensor update events."""

import json
import logging
import queue
import threading
import time

import pika

from metrics import server as metrics_server

logger = logging.getLogger("alert_service")


class AlertConsumer:
    """Consumes sensor.updated events from RabbitMQ.

    Blocking mode: each message is evaluated inline in the consumer thread
    before the next message is fetched (sequential, strong at-least-once).

    Async mode: messages are placed onto a thread-safe queue; a pool of
    worker threads drains it in parallel. The consumer acks immediately after
    enqueuing, so a crash between ack and evaluation drops the event — higher
    throughput at the cost of a weaker delivery guarantee.
    Backpressure: when the queue is full, put() blocks the consumer thread
    until a worker frees a slot.
    """

    def __init__(self, rabbitmq_url: str, callback, worker_count: int = 0):
        """Initialize the consumer.

        Args:
            rabbitmq_url: AMQP connection URL.
            callback: Function to call with each sensor event dict.
            worker_count: Number of worker threads for async mode.
                          0 means blocking mode (no worker pool).
        """
        self.rabbitmq_url = rabbitmq_url
        self.callback = callback
        self._thread = None

        if worker_count > 0:
            # Buffer = worker_count * 10 gives workers headroom without unbounded queuing.
            self._event_queue = queue.Queue(maxsize=worker_count * 10)
            self._mode = "async"
            for _ in range(worker_count):
                t = threading.Thread(target=self._worker, daemon=True)
                t.start()
        else:
            self._event_queue = None
            self._mode = "blocking"

    def _worker(self):
        """Pull events from the queue and evaluate them."""
        for event in iter(self._event_queue.get, None):
            self.callback(event)
            self._event_queue.task_done()

    def start(self):
        """Start consuming in a background daemon thread."""
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("Alert consumer started", extra={"mode": self._mode})

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
                if metrics_server.collector is not None:
                    metrics_server.collector.inc_received()
                logger.info(
                    "Received sensor.updated event",
                    extra={
                        "sensor_id": event.get("sensor_id"),
                        "value": event.get("value"),
                        "trace_id": event.get("trace_id"),
                    },
                )
                if self._event_queue is not None:
                    # Async: enqueue for worker pool; blocks if queue is full (backpressure).
                    self._event_queue.put(event)
                else:
                    # Blocking: evaluate inline before acking.
                    self.callback(event)
            # Ack after dispatch (including unknown event types).
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.warning("Received invalid JSON message")
            # Nack without requeue — malformed messages cannot be retried usefully.
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

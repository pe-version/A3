"""RabbitMQ consumer for sensor update events using aio-pika (asyncio)."""

import asyncio
import json
import logging

import aio_pika

from metrics import server as metrics_server

logger = logging.getLogger("alert_service")


class AlertConsumer:
    """Consumes sensor.updated events from RabbitMQ via aio-pika.

    Blocking mode (worker_count=0): each message is evaluated inline in the
    consumer coroutine before the next message is fetched — sequential,
    strong at-least-once.

    Async mode (worker_count>0): messages are placed onto an asyncio.Queue;
    a pool of worker tasks drains it concurrently. The consumer acks
    immediately after enqueuing, so a crash between ack and evaluation
    drops the event — higher throughput at the cost of a weaker delivery
    guarantee. Backpressure: when the queue is full, put() awaits until
    a worker frees a slot.
    """

    def __init__(self, rabbitmq_url: str, callback, worker_count: int = 0):
        """Initialize the consumer.

        Args:
            rabbitmq_url: AMQP connection URL.
            callback: Async callable to invoke with each sensor event dict.
            worker_count: Number of worker tasks for async mode.
                          0 means blocking mode (no worker pool).
        """
        self.rabbitmq_url = rabbitmq_url
        self.callback = callback
        self.worker_count = worker_count

        if worker_count > 0:
            # Buffer = worker_count * 10 gives workers headroom without unbounded queuing.
            self._event_queue: asyncio.Queue | None = asyncio.Queue(
                maxsize=worker_count * 10
            )
            self._mode = "async"
        else:
            self._event_queue = None
            self._mode = "blocking"

    def start(self):
        """Schedule the consumer loop as an asyncio task."""
        loop = asyncio.get_event_loop()
        loop.create_task(self._run_workers_and_consumer())
        logger.info("Alert consumer started", extra={"mode": self._mode})

    async def _run_workers_and_consumer(self):
        """Start worker tasks (if async) then enter the consume loop."""
        if self._event_queue is not None:
            for _ in range(self.worker_count):
                asyncio.create_task(self._worker())

        while True:
            try:
                await self._consume()
            except Exception as e:
                logger.error(
                    "Consumer connection lost: %s — reconnecting in 5s", str(e)
                )
                await asyncio.sleep(5)

    async def _worker(self):
        """Pull events from the queue and evaluate them."""
        while True:
            event = await self._event_queue.get()
            try:
                await self.callback(event)
            except Exception:
                logger.exception("Worker failed to evaluate event: %s", event)
            finally:
                self._event_queue.task_done()

    async def _consume(self):
        """Connect to RabbitMQ and consume sensor.updated events."""
        connection = await aio_pika.connect_robust(self.rabbitmq_url)
        async with connection:
            channel = await connection.channel()

            # Prefetch limits how many unacked messages RabbitMQ sends at once.
            await channel.set_qos(prefetch_count=10)

            exchange = await channel.declare_exchange(
                "sensor_events", aio_pika.ExchangeType.FANOUT, durable=True
            )
            queue = await channel.declare_queue(
                "alert_service_python", durable=True
            )
            await queue.bind(exchange)

            logger.info("Connected to RabbitMQ, waiting for sensor events")

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await self._on_message(message)

    async def _on_message(self, message: aio_pika.IncomingMessage):
        """Handle incoming message."""
        try:
            event = json.loads(message.body)
        except json.JSONDecodeError:
            logger.warning("Received invalid JSON message")
            await message.nack(requeue=False)
            return

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
                # Async: enqueue for worker pool; awaits if queue is full (backpressure).
                await self._event_queue.put(event)
            else:
                # Blocking: evaluate inline before acking.
                await self.callback(event)

        await message.ack()

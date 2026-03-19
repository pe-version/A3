"""RabbitMQ consumer for sensor update events using aio-pika (asyncio).

Reactive mode uses RxPY to model the event pipeline as an Observable stream
with backpressure via flat_map's max_concurrent parameter.
"""

import asyncio
import json
import logging

import aio_pika
import reactivex as rx
from reactivex import operators as ops
from reactivex.disposable import CompositeDisposable
from reactivex.scheduler.eventloop import AsyncIOScheduler
from reactivex.subject import Subject

from metrics import server as metrics_server

logger = logging.getLogger("alert_service")


class AlertConsumer:
    """Consumes sensor.updated events from RabbitMQ via aio-pika.

    Blocking mode (worker_count=0): each message is evaluated inline in the
    consumer coroutine before the next message is fetched — sequential,
    strong at-least-once.

    Reactive mode (worker_count>0): messages are emitted into an RxPY Subject.
    A flat_map operator fans out evaluation to up to ``worker_count`` concurrent
    asyncio tasks.  Backpressure: flat_map limits in-flight evaluations to
    ``worker_count``; when all slots are occupied, the Subject's on_next call
    awaits until a slot frees up.
    """

    def __init__(self, rabbitmq_url: str, callback, worker_count: int = 0):
        """Initialize the consumer.

        Args:
            rabbitmq_url: AMQP connection URL.
            callback: Async callable to invoke with each sensor event dict.
            worker_count: Number of concurrent reactive evaluations.
                          0 means blocking mode (no reactive pipeline).
        """
        self.rabbitmq_url = rabbitmq_url
        self.callback = callback
        self.worker_count = worker_count
        self._disposable = CompositeDisposable()

        if worker_count > 0:
            self._subject: Subject | None = Subject()
            self._mode = "reactive"
        else:
            self._subject = None
            self._mode = "blocking"

    def start(self):
        """Schedule the consumer loop as an asyncio task."""
        loop = asyncio.get_event_loop()

        if self._subject is not None:
            scheduler = AsyncIOScheduler(loop=loop)
            self._setup_reactive_pipeline(scheduler)

        loop.create_task(self._consume_loop())
        logger.info("Alert consumer started", extra={"mode": self._mode})

    def _setup_reactive_pipeline(self, scheduler):
        """Wire the RxPY Subject → flat_map → evaluate pipeline."""

        def evaluate_as_observable(event):
            """Wrap the async callback as a single-element Observable."""
            async def _run():
                try:
                    await self.callback(event)
                except Exception:
                    logger.exception("Reactive worker failed to evaluate event: %s", event)
                return event

            future = asyncio.ensure_future(_run())
            return rx.from_future(future)

        # flat_map subscribes to at most worker_count inner Observables
        # concurrently — this is the reactive backpressure mechanism.
        pipeline = self._subject.pipe(
            ops.flat_map(evaluate_as_observable, max_concurrent=self.worker_count),
        )

        disposable = pipeline.subscribe(
            on_next=lambda _: None,
            on_error=lambda e: logger.error("Reactive pipeline error: %s", e),
            scheduler=scheduler,
        )
        self._disposable.add(disposable)

    async def _consume_loop(self):
        """Reconnect loop — retries on connection failure."""
        while True:
            try:
                await self._consume()
            except Exception as e:
                logger.error(
                    "Consumer connection lost: %s — reconnecting in 5s", str(e)
                )
                await asyncio.sleep(5)

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
            if self._subject is not None:
                # Reactive: emit into the RxPY stream; flat_map limits concurrency.
                self._subject.on_next(event)
            else:
                # Blocking: evaluate inline before acking.
                await self.callback(event)

        await message.ack()

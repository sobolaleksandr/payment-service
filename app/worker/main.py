import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitMessage

from app.config import WorkerSettings
from app.db import create_engine, create_session_factory
from app.logging_config import setup_logging
from app.messaging.broker import create_broker
from app.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    RetryPolicy,
    declare_consumer_topology,
)
from app.schemas import PaymentCreatedEvent
from app.worker.gateway import EmulatedPaymentGateway
from app.worker.handler import PaymentEventHandler, RabbitFailurePublisher, read_attempt
from app.worker.processor import PaymentProcessor, SqlPaymentStore
from app.worker.webhook import WebhookNotifier


def create_app(settings: WorkerSettings | None = None) -> FastStream:
    settings = settings or WorkerSettings()
    setup_logging(settings.log_level)

    broker = create_broker(settings.rabbitmq_url)
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    policy = RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        base_delay_ms=round(settings.retry_base_delay * 1000),
    )
    processor = PaymentProcessor(
        SqlPaymentStore(create_session_factory(engine)),
        EmulatedPaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        WebhookNotifier(http_client),
    )
    handler = PaymentEventHandler(processor, RabbitFailurePublisher(broker), policy)

    @broker.subscriber(
        PAYMENTS_NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        channel=Channel(prefetch_count=settings.consumer_prefetch),
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def on_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await handler.handle(event, read_attempt(message.headers))

    app = FastStream(broker)

    @app.on_startup
    async def declare_topology() -> None:
        await broker.connect()
        await declare_consumer_topology(broker, policy)

    @app.after_shutdown
    async def release_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return app

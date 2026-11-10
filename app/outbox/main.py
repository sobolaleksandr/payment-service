import asyncio
import contextlib

from faststream import FastStream

from app.config import RelaySettings
from app.db import create_engine, create_session_factory
from app.logging_config import setup_logging
from app.messaging.broker import create_broker
from app.messaging.topology import PAYMENTS_EXCHANGE
from app.outbox.relay import OutboxRelay, RabbitOutboxPublisher


def create_app(settings: RelaySettings | None = None) -> FastStream:
    settings = settings or RelaySettings()
    setup_logging(settings.log_level)

    broker = create_broker(settings.rabbitmq_url)
    engine = create_engine(settings.database_url)
    relay = OutboxRelay(
        create_session_factory(engine),
        RabbitOutboxPublisher(broker),
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )
    tasks: list[asyncio.Task[None]] = []

    app = FastStream(broker)

    @app.after_startup
    async def start_relay() -> None:
        await broker.declare_exchange(PAYMENTS_EXCHANGE)
        tasks.append(asyncio.create_task(relay.run(), name="outbox-relay"))

    @app.on_shutdown
    async def stop_relay() -> None:
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await engine.dispose()

    return app

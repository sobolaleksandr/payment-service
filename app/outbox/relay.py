import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging.topology import PAYMENTS_EXCHANGE
from app.models import OutboxMessage
from app.repositories import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxPublisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None: ...


class RabbitOutboxPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        await self._broker.publish(
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.event_type,
            message_id=str(message.id),
            message_type=message.event_type,
            timestamp=message.created_at,
            persist=True,
            mandatory=True,
        )


@dataclass(frozen=True, slots=True)
class RelayBatchResult:
    published: int
    failed: int


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: OutboxPublisher,
        *,
        batch_size: int,
        poll_interval: float,
        max_backoff: float = 10.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_backoff = max_backoff
        self._sleep = sleep

    async def run(self) -> None:
        logger.info("Outbox relay started")
        failed_runs = 0
        while True:
            try:
                result = await self.relay_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                result = RelayBatchResult(published=0, failed=1)

            failed_runs = failed_runs + 1 if result.failed else 0
            if failed_runs:
                await self._sleep(min(self._poll_interval * 2**failed_runs, self._max_backoff))
            elif result.published < self._batch_size:
                await self._sleep(self._poll_interval)

    async def relay_batch(self) -> RelayBatchResult:
        async with self._session_factory() as session, session.begin():
            outbox = OutboxRepository(session)
            messages = await outbox.lock_unpublished(self._batch_size)
            if not messages:
                return RelayBatchResult(published=0, failed=0)

            outcomes = await asyncio.gather(
                *(self._publisher.publish(message) for message in messages),
                return_exceptions=True,
            )
            published = [
                message.id
                for message, outcome in zip(messages, outcomes, strict=True)
                if not isinstance(outcome, BaseException)
            ]
            if published:
                await outbox.mark_published(published)

        for message, outcome in zip(messages, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                logger.warning(
                    "Failed to publish outbox message %s (%s): %r",
                    message.id,
                    message.event_type,
                    outcome,
                )
        if published:
            logger.info("Published %d outbox message(s)", len(published))
        return RelayBatchResult(published=len(published), failed=len(messages) - len(published))

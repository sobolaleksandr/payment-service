import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Currency, OutboxMessage
from app.outbox.relay import OutboxRelay
from app.schemas import PaymentCreate
from app.services import PaymentService

pytestmark = pytest.mark.integration


class RecordingPublisher:
    def __init__(self, failing: set[uuid.UUID] | None = None, delay: float = 0) -> None:
        self.failing = failing or set()
        self.delay = delay
        self.published: list[uuid.UUID] = []

    async def publish(self, message: OutboxMessage) -> None:
        await asyncio.sleep(self.delay)
        if message.id in self.failing:
            raise ConnectionError("broker unavailable")
        self.published.append(message.id)


async def create_payments(factory: async_sessionmaker[AsyncSession], count: int) -> None:
    for number in range(count):
        async with factory() as session:
            await PaymentService(session).create(
                PaymentCreate(
                    amount=Decimal(100 + number),
                    currency=Currency.USD,
                    description=f"Order #{number}",
                    webhook_url="http://merchant.test/webhook",
                ),
                idempotency_key=str(uuid.uuid4()),
            )


async def outbox_state(factory: async_sessionmaker[AsyncSession]) -> dict[uuid.UUID, bool]:
    async with factory() as session:
        messages = (await session.scalars(select(OutboxMessage))).all()
    return {message.id: message.published_at is not None for message in messages}


def relay(
    factory: async_sessionmaker[AsyncSession], publisher: RecordingPublisher, batch_size: int = 10
) -> OutboxRelay:
    return OutboxRelay(factory, publisher, batch_size=batch_size, poll_interval=0.01)


async def test_relay_publishes_and_marks_messages(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await create_payments(session_factory, 3)
    publisher = RecordingPublisher()

    first = await relay(session_factory, publisher).relay_batch()
    second = await relay(session_factory, publisher).relay_batch()

    assert (first.published, first.failed) == (3, 0)
    assert (second.published, second.failed) == (0, 0)
    state = await outbox_state(session_factory)
    assert all(state.values())
    assert sorted(publisher.published) == sorted(state)


async def test_failed_publish_stays_in_outbox_until_next_run(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await create_payments(session_factory, 3)
    broken_id = next(iter(await outbox_state(session_factory)))

    failed_run = await relay(session_factory, RecordingPublisher({broken_id})).relay_batch()
    state = await outbox_state(session_factory)

    assert (failed_run.published, failed_run.failed) == (2, 1)
    assert state[broken_id] is False
    assert sum(state.values()) == 2

    healthy = RecordingPublisher()
    await relay(session_factory, healthy).relay_batch()

    assert healthy.published == [broken_id]
    assert all((await outbox_state(session_factory)).values())


async def test_parallel_relays_do_not_publish_the_same_message(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await create_payments(session_factory, 6)
    first, second = RecordingPublisher(delay=0.2), RecordingPublisher(delay=0.2)

    await asyncio.gather(
        relay(session_factory, first, batch_size=3).relay_batch(),
        relay(session_factory, second, batch_size=3).relay_batch(),
    )

    assert len(first.published) == 3
    assert len(second.published) == 3
    assert set(first.published).isdisjoint(second.published)
    assert all((await outbox_state(session_factory)).values())

import asyncio
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxMessage
from app.outbox.relay import OutboxRelay, RelayBatchResult


class NullPublisher:
    async def publish(self, message: OutboxMessage) -> None:
        return None


async def run_relay(results: list[RelayBatchResult | Exception]) -> list[float]:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    relay = OutboxRelay(
        cast(async_sessionmaker[AsyncSession], None),
        NullPublisher(),
        batch_size=10,
        poll_interval=0.5,
        max_backoff=4.0,
        sleep=sleep,
    )
    outcomes = iter(results)

    async def relay_batch() -> RelayBatchResult:
        outcome = next(outcomes, None)
        if outcome is None:
            raise asyncio.CancelledError
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    cast(Any, relay).relay_batch = relay_batch
    with pytest.raises(asyncio.CancelledError):
        await relay.run()
    return sleeps


async def test_relay_backs_off_exponentially_while_publishing_fails() -> None:
    failed = RelayBatchResult(published=0, failed=1)
    sleeps = await run_relay([failed, failed, ConnectionError(), failed, failed])
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 4.0]


async def test_relay_drains_full_batches_without_waiting() -> None:
    full, partial = RelayBatchResult(published=10, failed=0), RelayBatchResult(5, 0)
    failed = RelayBatchResult(published=3, failed=1)
    sleeps = await run_relay([full, full, partial, failed, partial])
    assert sleeps == [0.5, 1.0, 0.5]

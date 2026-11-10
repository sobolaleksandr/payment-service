from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from app.messaging.topology import RetryPolicy
from app.schemas import PaymentCreatedEvent
from app.worker.handler import PaymentEventHandler, read_attempt
from app.worker.processor import PaymentProcessor, UnknownPaymentError

POLICY = RetryPolicy(max_attempts=3, base_delay_ms=2000)


class StubProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def process(self, payment_id: UUID) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error


class RecordingFailures:
    def __init__(self) -> None:
        self.retries: list[tuple[int, int]] = []
        self.dead_letters: list[tuple[int, str]] = []

    async def retry(self, event: PaymentCreatedEvent, attempt: int, delay_ms: int) -> None:
        self.retries.append((attempt, delay_ms))

    async def dead_letter(self, event: PaymentCreatedEvent, attempts: int, error: str) -> None:
        self.dead_letters.append((attempts, error))


def make_handler(error: Exception | None) -> tuple[PaymentEventHandler, RecordingFailures]:
    failures = RecordingFailures()
    processor = cast(PaymentProcessor, StubProcessor(error))
    return PaymentEventHandler(processor, failures, POLICY), failures


def event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(event_id=uuid4(), payment_id=uuid4(), occurred_at=datetime.now(UTC))


async def test_success_is_acknowledged_without_retry() -> None:
    handler, failures = make_handler(None)
    await handler.handle(event(), attempt=1)
    assert failures.retries == []
    assert failures.dead_letters == []


@pytest.mark.parametrize(("attempt", "expected"), [(1, (2, 2000)), (2, (3, 4000))])
async def test_transient_error_is_retried_with_exponential_delay(
    attempt: int, expected: tuple[int, int]
) -> None:
    handler, failures = make_handler(ConnectionError("webhook is down"))
    await handler.handle(event(), attempt=attempt)
    assert failures.retries == [expected]
    assert failures.dead_letters == []


async def test_last_attempt_goes_to_dead_letter_queue() -> None:
    handler, failures = make_handler(ConnectionError("webhook is down"))
    await handler.handle(event(), attempt=3)
    assert failures.retries == []
    assert failures.dead_letters == [(3, "ConnectionError('webhook is down')")]


async def test_permanent_error_skips_retries() -> None:
    handler, failures = make_handler(UnknownPaymentError(uuid4()))
    await handler.handle(event(), attempt=1)
    assert failures.retries == []
    assert [attempts for attempts, _ in failures.dead_letters] == [1]


@pytest.mark.parametrize(
    ("headers", "expected"),
    [({}, 1), ({"x-attempt": 2}, 2), ({"x-attempt": "3"}, 3), ({"x-attempt": "x"}, 1)],
)
def test_read_attempt(headers: dict[str, Any], expected: int) -> None:
    assert read_attempt(headers) == expected


def test_retry_policy_delays() -> None:
    assert POLICY.delays() == [2000, 4000]
    assert RetryPolicy(max_attempts=1, base_delay_ms=2000).delays() == []
    assert POLICY.can_retry(2)
    assert not POLICY.can_retry(3)

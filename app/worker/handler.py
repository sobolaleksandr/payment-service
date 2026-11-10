import logging
from collections.abc import Mapping
from typing import Any, Protocol

from faststream.rabbit import RabbitBroker

from app.messaging.topology import (
    ATTEMPT_HEADER,
    DEAD_LETTER_EXCHANGE,
    ERROR_HEADER,
    RETRY_EXCHANGE,
    RetryPolicy,
    retry_queue,
)
from app.schemas import PAYMENT_CREATED_ROUTING_KEY, PaymentCreatedEvent
from app.worker.processor import PaymentProcessor, PermanentProcessingError

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000


class FailurePublisher(Protocol):
    async def retry(self, event: PaymentCreatedEvent, attempt: int, delay_ms: int) -> None: ...

    async def dead_letter(self, event: PaymentCreatedEvent, attempts: int, error: str) -> None: ...


class RabbitFailurePublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def retry(self, event: PaymentCreatedEvent, attempt: int, delay_ms: int) -> None:
        await self._broker.publish(
            event,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_queue(delay_ms).routing(),
            headers={ATTEMPT_HEADER: attempt},
            message_id=str(event.event_id),
            persist=True,
            mandatory=True,
        )

    async def dead_letter(self, event: PaymentCreatedEvent, attempts: int, error: str) -> None:
        await self._broker.publish(
            event,
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=PAYMENT_CREATED_ROUTING_KEY,
            headers={ATTEMPT_HEADER: attempts, ERROR_HEADER: error[:MAX_ERROR_LENGTH]},
            message_id=str(event.event_id),
            persist=True,
            mandatory=True,
        )


def read_attempt(headers: Mapping[str, Any]) -> int:
    try:
        return max(1, int(headers.get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


class PaymentEventHandler:
    def __init__(
        self,
        processor: PaymentProcessor,
        failures: FailurePublisher,
        policy: RetryPolicy,
    ) -> None:
        self._processor = processor
        self._failures = failures
        self._policy = policy

    async def handle(self, event: PaymentCreatedEvent, attempt: int) -> None:
        try:
            await self._processor.process(event.payment_id)
        except PermanentProcessingError as exc:
            logger.error("Payment %s cannot be processed: %s", event.payment_id, exc)
            await self._failures.dead_letter(event, attempt, repr(exc))
        except Exception as exc:
            if self._policy.can_retry(attempt):
                delay_ms = self._policy.delay_after(attempt)
                logger.warning(
                    "Attempt %d/%d for payment %s failed: %r; retrying in %d ms",
                    attempt,
                    self._policy.max_attempts,
                    event.payment_id,
                    exc,
                    delay_ms,
                )
                await self._failures.retry(event, attempt + 1, delay_ms)
            else:
                logger.error(
                    "Attempt %d/%d for payment %s failed: %r; moving to DLQ",
                    attempt,
                    self._policy.max_attempts,
                    event.payment_id,
                    exc,
                )
                await self._failures.dead_letter(event, attempt, repr(exc))

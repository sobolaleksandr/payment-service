import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, Payment
from app.repositories import OutboxRepository, PaymentRepository
from app.schemas import PAYMENT_CREATED_ROUTING_KEY, PaymentCreate, PaymentCreatedEvent


class IdempotencyKeyReusedError(Exception):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency key {idempotency_key!r} was already used with a different request body"
        )


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreatePaymentResult:
        fingerprint = data.fingerprint()
        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                id=uuid.uuid4(),
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                meta=data.metadata,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                webhook_url=str(data.webhook_url),
            )
            if payment is not None:
                self._outbox.add(_payment_created_message(payment))
                return CreatePaymentResult(payment=payment, created=True)

            existing = await self._payments.get_by_idempotency_key(idempotency_key)

        if existing is None:
            raise RuntimeError(f"Payment for idempotency key {idempotency_key!r} vanished")
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyKeyReusedError(idempotency_key)
        return CreatePaymentResult(payment=existing, created=False)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment


def _payment_created_message(payment: Payment) -> OutboxMessage:
    event = PaymentCreatedEvent(
        event_id=uuid.uuid4(),
        payment_id=payment.id,
        occurred_at=payment.created_at,
    )
    return OutboxMessage(
        id=event.event_id,
        aggregate_type="payment",
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED_ROUTING_KEY,
        payload=event.model_dump(mode="json"),
    )

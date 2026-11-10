import logging
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.repositories import PaymentRepository
from app.worker.gateway import PaymentGateway
from app.worker.webhook import Notifier

logger = logging.getLogger(__name__)


class PermanentProcessingError(Exception):
    pass


class UnknownPaymentError(PermanentProcessingError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} does not exist")


class PaymentStore(Protocol):
    async def get(self, payment_id: UUID) -> Payment | None: ...

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None: ...


class SqlPaymentStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await PaymentRepository(session).get(payment_id)

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        async with self._session_factory() as session, session.begin():
            return await PaymentRepository(session).complete(payment_id, status)


class PaymentProcessor:
    def __init__(self, store: PaymentStore, gateway: PaymentGateway, notifier: Notifier) -> None:
        self._store = store
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: UUID) -> Payment:
        payment = await self._load(payment_id)
        if payment.status == PaymentStatus.PENDING:
            payment = await self._charge(payment)
        else:
            logger.info(
                "Payment %s is already %s, skipping the gateway", payment.id, payment.status
            )

        await self._notifier.notify(payment)
        logger.info("Webhook for payment %s delivered to %s", payment.id, payment.webhook_url)
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        status = await self._gateway.charge(payment)
        completed = await self._store.complete(payment.id, status)
        if completed is None:
            logger.warning(
                "Payment %s was completed concurrently, using the stored status", payment.id
            )
            return await self._load(payment.id)
        logger.info("Payment %s processed: %s", completed.id, completed.status)
        return completed

    async def _load(self, payment_id: UUID) -> Payment:
        payment = await self._store.get(payment_id)
        if payment is None:
            raise UnknownPaymentError(payment_id)
        return payment

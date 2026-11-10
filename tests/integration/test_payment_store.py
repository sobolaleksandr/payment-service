import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Currency, PaymentStatus
from app.schemas import PaymentCreate
from app.services import PaymentService
from app.worker.processor import SqlPaymentStore

pytestmark = pytest.mark.integration


async def test_payment_is_completed_only_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        created = await PaymentService(session).create(
            PaymentCreate(
                amount=Decimal("10.00"),
                currency=Currency.EUR,
                description="Order",
                webhook_url="http://merchant.test/webhook",
            ),
            idempotency_key=str(uuid.uuid4()),
        )
    store = SqlPaymentStore(session_factory)

    first = await store.complete(created.payment.id, PaymentStatus.SUCCEEDED)
    second = await store.complete(created.payment.id, PaymentStatus.FAILED)
    stored = await store.get(created.payment.id)

    assert first is not None
    assert first.status == PaymentStatus.SUCCEEDED
    assert first.processed_at is not None
    assert second is None
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED

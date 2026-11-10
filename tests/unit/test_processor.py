from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.models import Payment, PaymentStatus
from app.worker.processor import PaymentProcessor, UnknownPaymentError
from tests.conftest import PaymentFactory


class FakeStore:
    def __init__(self, *payments: Payment) -> None:
        self.payments = {payment.id: payment for payment in payments}
        self.completed_concurrently: PaymentStatus | None = None

    async def get(self, payment_id: UUID) -> Payment | None:
        return self.payments.get(payment_id)

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        payment = self.payments[payment_id]
        if self.completed_concurrently is not None:
            payment.status = self.completed_concurrently
        if payment.status is not PaymentStatus.PENDING:
            return None
        payment.status = status
        payment.processed_at = datetime.now(UTC)
        return payment


class FakeGateway:
    def __init__(self, result: PaymentStatus) -> None:
        self.result = result
        self.calls: list[UUID] = []

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls.append(payment.id)
        return self.result


class FakeNotifier:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[UUID, PaymentStatus]] = []

    async def notify(self, payment: Payment) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append((payment.id, payment.status))


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_pending_payment_is_charged_stored_and_notified(
    make_payment: PaymentFactory, result: PaymentStatus
) -> None:
    payment = make_payment()
    store, gateway, notifier = FakeStore(payment), FakeGateway(result), FakeNotifier()

    processed = await PaymentProcessor(store, gateway, notifier).process(payment.id)

    assert processed.status is result
    assert processed.processed_at is not None
    assert gateway.calls == [payment.id]
    assert notifier.sent == [(payment.id, result)]


async def test_completed_payment_is_not_charged_again(make_payment: PaymentFactory) -> None:
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))
    store, gateway, notifier = (
        FakeStore(payment),
        FakeGateway(PaymentStatus.FAILED),
        FakeNotifier(),
    )

    await PaymentProcessor(store, gateway, notifier).process(payment.id)

    assert gateway.calls == []
    assert notifier.sent == [(payment.id, PaymentStatus.SUCCEEDED)]


async def test_retry_after_webhook_failure_only_resends_webhook(
    make_payment: PaymentFactory,
) -> None:
    payment = make_payment()
    store, gateway = FakeStore(payment), FakeGateway(PaymentStatus.SUCCEEDED)

    with pytest.raises(ConnectionError):
        await PaymentProcessor(store, gateway, FakeNotifier(ConnectionError())).process(payment.id)
    notifier = FakeNotifier()
    await PaymentProcessor(store, gateway, notifier).process(payment.id)

    assert gateway.calls == [payment.id]
    assert notifier.sent == [(payment.id, PaymentStatus.SUCCEEDED)]


async def test_concurrent_completion_wins(make_payment: PaymentFactory) -> None:
    payment = make_payment()
    store, notifier = FakeStore(payment), FakeNotifier()
    store.completed_concurrently = PaymentStatus.FAILED

    processed = await PaymentProcessor(
        store, FakeGateway(PaymentStatus.SUCCEEDED), notifier
    ).process(payment.id)

    assert processed.status is PaymentStatus.FAILED
    assert notifier.sent == [(payment.id, PaymentStatus.FAILED)]


async def test_unknown_payment_is_a_permanent_error() -> None:
    processor = PaymentProcessor(FakeStore(), FakeGateway(PaymentStatus.SUCCEEDED), FakeNotifier())
    with pytest.raises(UnknownPaymentError):
        await processor.process(uuid4())

import random
from collections import Counter

from app.models import PaymentStatus
from app.worker.gateway import EmulatedPaymentGateway
from tests.conftest import PaymentFactory


async def test_gateway_success_rate(make_payment: PaymentFactory) -> None:
    gateway = EmulatedPaymentGateway(
        min_delay=0, max_delay=0, success_rate=0.9, rng=random.Random(42)
    )
    payment = make_payment()

    results = Counter([await gateway.charge(payment) for _ in range(2000)])

    assert 0.87 < results[PaymentStatus.SUCCEEDED] / 2000 < 0.93
    assert results[PaymentStatus.SUCCEEDED] + results[PaymentStatus.FAILED] == 2000

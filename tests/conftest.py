import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.models import Currency, Payment, PaymentStatus


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.getenv("TEST_DATABASE_URL"):
        return
    skip = pytest.mark.skip(reason="set TEST_DATABASE_URL to run integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


PaymentFactory = Callable[..., Payment]


@pytest.fixture
def make_payment() -> PaymentFactory:
    def factory(**overrides: Any) -> Payment:
        values: dict[str, Any] = {
            "id": uuid.uuid4(),
            "amount": Decimal("1500.00"),
            "currency": Currency.RUB,
            "description": "Order #42",
            "meta": {"order_id": 42},
            "status": PaymentStatus.PENDING,
            "idempotency_key": str(uuid.uuid4()),
            "request_fingerprint": "0" * 64,
            "webhook_url": "http://merchant.test/webhook",
            "created_at": datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
            "processed_at": None,
        }
        values.update(overrides)
        return Payment(**values)

    return factory

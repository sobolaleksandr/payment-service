from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate


def body(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "1500.5",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://merchant.test/webhook",
    }
    data.update(overrides)
    return data


def test_amount_is_normalized_to_cents() -> None:
    assert PaymentCreate.model_validate(body()).amount == Decimal("1500.50")
    assert str(PaymentCreate.model_validate(body(amount=10)).amount) == "10.00"


@pytest.mark.parametrize("amount", ["0", "-1", "0.001", "1e30", "NaN"])
def test_invalid_amount_is_rejected(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(amount=amount))


@pytest.mark.parametrize(
    "overrides",
    [
        {"currency": "GBP"},
        {"description": ""},
        {"webhook_url": "not-a-url"},
        {"unexpected": True},
    ],
)
def test_invalid_body_is_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(**overrides))


def test_metadata_defaults_to_empty_object() -> None:
    data = body()
    del data["metadata"]
    assert PaymentCreate.model_validate(data).metadata == {}


def test_fingerprint_ignores_formatting_differences() -> None:
    first = PaymentCreate.model_validate(body(amount="1500.5", metadata={"a": 1, "b": 2}))
    second = PaymentCreate.model_validate(body(amount=1500.50, metadata={"b": 2, "a": 1}))
    assert first.fingerprint() == second.fingerprint()


def test_fingerprint_changes_with_content() -> None:
    first = PaymentCreate.model_validate(body())
    second = PaymentCreate.model_validate(body(description="Order #43"))
    assert first.fingerprint() != second.fingerprint()

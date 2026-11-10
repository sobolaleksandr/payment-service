import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxMessage, Payment

pytestmark = pytest.mark.integration

URL = "/api/v1/payments"


def body(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42, "customer": {"tier": "gold"}},
        "webhook_url": "http://merchant.test/webhook",
    }
    data.update(overrides)
    return data


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(factory: async_sessionmaker[AsyncSession], model: type[Any]) -> int:
    async with factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_payment_writes_payment_and_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(URL, json=body(), headers=key())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert "Idempotent-Replayed" not in response.headers

    async with session_factory() as session:
        [message] = (await session.scalars(select(OutboxMessage))).all()
    assert message.event_type == "payments.new"
    assert message.aggregate_id == uuid.UUID(data["payment_id"])
    assert message.payload["payment_id"] == data["payment_id"]
    assert message.payload["event_id"] == str(message.id)
    assert message.published_at is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    headers = key()
    created = (await client.post(URL, json=body(), headers=headers)).json()

    response = await client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42, "customer": {"tier": "gold"}},
        "status": "pending",
        "idempotency_key": headers["Idempotency-Key"],
        "webhook_url": "http://merchant.test/webhook",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_unknown_payment_returns_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_repeated_request_returns_same_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    first = await client.post(URL, json=body(), headers=headers)
    second = await client.post(URL, json=body(amount=1500.5), headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_key_reused_with_different_body_is_rejected(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    await client.post(URL, json=body(), headers=headers)

    response = await client.post(URL, json=body(amount="99.99"), headers=headers)

    assert response.status_code == 422
    assert await count(session_factory, Payment) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()

    responses = await asyncio.gather(
        *(client.post(URL, json=body(), headers=headers) for _ in range(10))
    )

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sum("Idempotent-Replayed" not in response.headers for response in responses) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_missing_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(URL, json=body())
    assert response.status_code == 422


async def test_invalid_body_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(URL, json=body(currency="GBP"), headers=key())
    assert response.status_code == 422

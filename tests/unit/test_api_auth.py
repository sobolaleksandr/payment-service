from collections.abc import AsyncIterator

import httpx
import pytest
from pydantic import SecretStr

from app.api.main import create_app
from app.config import ApiSettings

API_KEY = "unit-test-api-key"


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        ApiSettings(
            database_url="postgresql+asyncpg://nobody:nothing@127.0.0.1:1/none",
            api_key=SecretStr(API_KEY),
        )
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
async def test_payments_endpoints_require_api_key(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    create = await client.post(
        "/api/v1/payments", json={}, headers={**headers, "Idempotency-Key": "k"}
    )
    read = await client.get(
        "/api/v1/payments/6f1c1d3e-0000-4000-8000-000000000000", headers=headers
    )
    assert create.status_code == 401
    assert read.status_code == 401


async def test_health_is_public(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200

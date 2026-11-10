import json

import httpx
import pytest

from app.models import PaymentStatus
from app.worker.webhook import WebhookDeliveryError, WebhookNotifier
from tests.conftest import PaymentFactory


def notifier_with(status_code: int, requests: list[httpx.Request]) -> WebhookNotifier:
    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code)

    return WebhookNotifier(httpx.AsyncClient(transport=httpx.MockTransport(respond)))


async def test_webhook_carries_final_payment_state(make_payment: PaymentFactory) -> None:
    payment = make_payment(status=PaymentStatus.SUCCEEDED)
    requests: list[httpx.Request] = []

    await notifier_with(200, requests).notify(payment)

    [request] = requests
    body = json.loads(request.content)
    assert request.method == "POST"
    assert str(request.url) == payment.webhook_url
    assert request.headers["X-Payment-Id"] == str(payment.id)
    assert body["event"] == "payment.succeeded"
    assert body["payment"]["payment_id"] == str(payment.id)
    assert body["payment"]["amount"] == "1500.00"
    assert body["payment"]["status"] == "succeeded"
    assert body["payment"]["metadata"] == {"order_id": 42}


@pytest.mark.parametrize("status_code", [301, 404, 500, 503])
async def test_non_2xx_response_is_a_delivery_error(
    make_payment: PaymentFactory, status_code: int
) -> None:
    payment = make_payment(status=PaymentStatus.FAILED)
    with pytest.raises(WebhookDeliveryError) as error:
        await notifier_with(status_code, []).notify(payment)
    assert error.value.status_code == status_code

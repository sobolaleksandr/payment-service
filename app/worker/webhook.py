from typing import Literal, Protocol

import httpx

from app.models import Payment, PaymentStatus
from app.schemas import PaymentRead, WebhookPayload

WEBHOOK_EVENTS: dict[PaymentStatus, Literal["payment.succeeded", "payment.failed"]] = {
    PaymentStatus.SUCCEEDED: "payment.succeeded",
    PaymentStatus.FAILED: "payment.failed",
}


class WebhookDeliveryError(Exception):
    def __init__(self, url: str, status_code: int) -> None:
        super().__init__(f"Webhook {url} responded with HTTP {status_code}")
        self.url = url
        self.status_code = status_code


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


def build_webhook_payload(payment: Payment) -> WebhookPayload:
    return WebhookPayload(
        event=WEBHOOK_EVENTS[payment.status],
        payment=PaymentRead.model_validate(payment),
    )


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def notify(self, payment: Payment) -> None:
        payload = build_webhook_payload(payment)
        response = await self._client.post(
            payment.webhook_url,
            json=payload.model_dump(mode="json"),
            headers={"X-Payment-Id": str(payment.id)},
        )
        if not response.is_success:
            raise WebhookDeliveryError(payment.webhook_url, response.status_code)

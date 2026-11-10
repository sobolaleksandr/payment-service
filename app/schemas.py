import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, HttpUrl

from app.models import Currency, Payment, PaymentStatus

CENT = Decimal("0.01")
PAYMENT_CREATED_ROUTING_KEY = "payments.new"

Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"]),
    AfterValidator(lambda value: value.quantize(CENT)),
]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def of(cls, payment: Payment) -> "PaymentAccepted":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    event_id: UUID
    payment_id: UUID
    occurred_at: datetime


class WebhookPayload(BaseModel):
    event: Literal["payment.succeeded", "payment.failed"]
    payment: PaymentRead

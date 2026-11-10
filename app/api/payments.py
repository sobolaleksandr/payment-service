from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.dependencies import PaymentServiceDep, verify_api_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services import IdempotencyKeyReusedError, PaymentNotFoundError

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Client-generated unique key; repeating it returns the same payment",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Invalid body, or the Idempotency-Key was used with another body",
        },
    },
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKey,
    service: PaymentServiceDep,
    response: Response,
) -> PaymentAccepted:
    try:
        result = await service.create(payload, idempotency_key)
    except IdempotencyKeyReusedError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.of(result.payment)


@router.get("/{payment_id}", responses={status.HTTP_404_NOT_FOUND: {"description": "Not found"}})
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentRead:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return PaymentRead.model_validate(payment)

import logging
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import Body, FastAPI, Query, Response, status

from app.logging_config import setup_logging

logger = logging.getLogger("webhook-receiver")


def create_app() -> FastAPI:
    setup_logging("INFO")
    app = FastAPI(title="Webhook receiver (demo)")
    received: deque[dict[str, Any]] = deque(maxlen=500)
    attempts: Counter[str] = Counter()

    @app.post("/webhook")
    async def receive(
        payload: Annotated[dict[str, Any], Body()],
        response: Response,
        fail_times: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, Any]:
        payment_id = str(payload.get("payment", {}).get("payment_id"))
        attempts[payment_id] += 1
        attempt = attempts[payment_id]
        accepted = attempt > fail_times
        received.append(
            {
                "received_at": datetime.now(UTC).isoformat(),
                "attempt": attempt,
                "accepted": accepted,
                "payload": payload,
            }
        )
        logger.info(
            "Webhook %s for payment %s, attempt %d -> %s",
            payload.get("event"),
            payment_id,
            attempt,
            "200" if accepted else "500",
        )
        if not accepted:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"accepted": accepted, "attempt": attempt}

    @app.get("/webhook")
    async def history() -> list[dict[str, Any]]:
        return list(received)

    return app

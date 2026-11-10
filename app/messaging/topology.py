from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.schemas import PAYMENT_CREATED_ROUTING_KEY

ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-error"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENT_CREATED_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENT_CREATED_ROUTING_KEY,
    },
)

PAYMENTS_NEW_DLQ = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    routing_key=PAYMENT_CREATED_ROUTING_KEY,
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_delay_ms: int

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def delay_after(self, attempt: int) -> int:
        return int(self.base_delay_ms * 2 ** (attempt - 1))

    def delays(self) -> list[int]:
        return [self.delay_after(attempt) for attempt in range(1, self.max_attempts)]


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = f"{PAYMENTS_NEW_QUEUE.name}.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENT_CREATED_ROUTING_KEY,
        },
    )


async def declare_consumer_topology(broker: RabbitBroker, policy: RetryPolicy) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry = await broker.declare_exchange(RETRY_EXCHANGE)
    dead_letter = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    main = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await main.bind(payments, routing_key=PAYMENT_CREATED_ROUTING_KEY)

    dlq = await broker.declare_queue(PAYMENTS_NEW_DLQ)
    await dlq.bind(dead_letter, routing_key=PAYMENT_CREATED_ROUTING_KEY)

    for delay_ms in policy.delays():
        queue = retry_queue(delay_ms)
        declared = await broker.declare_queue(queue)
        await declared.bind(retry, routing_key=queue.routing())

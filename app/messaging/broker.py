from faststream.rabbit import Channel, RabbitBroker


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30.0,
    )

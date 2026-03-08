import aio_pika
from aio_pika import ExchangeType
import os

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@broker:5672/")

_connection = None
_channel = None
_exchange = None


async def init_rabbitmq():
    global _connection, _channel, _exchange

    _connection = await aio_pika.connect_robust(RABBITMQ_URL)

    _channel = await _connection.channel()

    _exchange = await _channel.declare_exchange(
        "normalized_events",
        ExchangeType.TOPIC,
        durable=True,
    )

    api_queue = await _channel.declare_queue(
        "api_service_queue",
        durable=True
    )

    automation_queue = await _channel.declare_queue(
        "automation_service_queue",
        durable=True
    )

    await api_queue.bind(_exchange, routing_key="normalized.api")
    await automation_queue.bind(_exchange, routing_key="normalized.automation")


async def publish_message(routing_key: str, body: bytes):
    await _exchange.publish(
        aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


async def publish_to_both_services(body: bytes):
    await publish_message("normalized.api", body)
    await publish_message("normalized.automation", body)


async def close_rabbitmq():
    global _connection
    if _connection is not None:
        await _connection.close()
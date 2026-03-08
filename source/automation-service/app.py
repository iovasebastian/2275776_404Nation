import asyncio
import json
import logging
import os
from typing import Any, Dict

import aio_pika
import httpx
from fastapi import FastAPI

from rules import create_session, list_rules_for_sensor, evaluate_rule

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@broker:5672/")
SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://simulator:8080")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////app/data/rules.db")

EXCHANGE_NAME = "normalized_events"
QUEUE_NAME = "automation_service_queue"
ROUTING_KEY = "normalized.automation"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("automation-service")

app = FastAPI(title="mars-automation-service")

SessionLocal = create_session(DATABASE_URL)
state: Dict[str, Any] = {
    "running": False,
    "latest": {},
    "task": None,
}


async def trigger_actuator(client: httpx.AsyncClient, actuator_name: str, action_state: str) -> None:
    url = f"{SIMULATOR_URL}/api/actuators/{actuator_name}"
    payload = {"state": action_state}
    resp = await client.post(url, json=payload, timeout=10)
    resp.raise_for_status()


async def process_event(payload: Dict[str, Any], client: httpx.AsyncClient) -> None:
    sensor_id = payload.get("sensor_id")
    measurements = payload.get("measurements", [])
    print(sensor_id, measurements)

    if not sensor_id or not measurements:
        logger.warning("Invalid payload received: %s", payload)
        return

    db = SessionLocal()
    try:
        rules = list_rules_for_sensor(db, str(sensor_id))

        for measurement in measurements:
            metric = measurement.get("metric")
            value = measurement.get("value")

            if metric is None or value is None:
                continue

            key = f"{sensor_id}:{metric}"
            state["latest"][key] = {
                "sensor_id": sensor_id,
                "metric": metric,
                "value": value,
                "occurred_at": payload.get("occurred_at"),
                "status": payload.get("status"),
                "metadata": payload.get("metadata", {}),
            }

            for rule in rules:
                print(rules)
                try:
                    if evaluate_rule(rule, float(value)):
                        try:
                            await trigger_actuator(client, rule.actuator_name, rule.action_state)
                            logger.info(
                                "Rule %s triggered actuator %s=%s for sensor=%s metric=%s value=%s",
                                rule.id,
                                rule.actuator_name,
                                rule.action_state,
                                sensor_id,
                                metric,
                                value,
                            )
                        except Exception as exc:
                            logger.warning("Failed actuator trigger for rule %s: %s", rule.id, exc)
                except Exception as exc:
                    logger.warning("Rule evaluation failed for rule %s: %s", getattr(rule, "id", "?"), exc)
    finally:
        db.close()


async def consume_loop() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True
    )

    queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True
    )

    await queue.bind(exchange, routing_key=ROUTING_KEY)

    logger.info("Consuming from exchange=%s queue=%s routing_key=%s", EXCHANGE_NAME, QUEUE_NAME, ROUTING_KEY)

    async with httpx.AsyncClient() as client:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                if not state["running"]:
                    break

                async with message.process(ignore_processed=True):
                    try:
                        payload = json.loads(message.body.decode("utf-8"))
                        await process_event(payload, client)
                    except Exception as exc:
                        logger.warning("Failed processing message: %s", exc)


@app.on_event("startup")
async def on_startup() -> None:
    state["running"] = True
    state["task"] = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    state["running"] = False
    task = state.get("task")
    if task:
        task.cancel()


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "running": state["running"],
        "cached_metrics": len(state["latest"]),
    }
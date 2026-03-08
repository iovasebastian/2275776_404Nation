import asyncio
import json
import logging
import os
from typing import Any, Dict

import aio_pika
import httpx
from fastapi import FastAPI

from rules import create_session, list_rules_for_sensor, evaluate_rule, get_all_rules

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@broker:5672/")
SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://simulator:8080")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////app/data/rules.db")
EXCHANGE_NAME = "mars.events"
QUEUE_NAME = "automation.events"

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
    sensor_name = payload.get("sensor_name")
    value = payload.get("value")
    if sensor_name is None or value is None:
        return

    key = f"{sensor_name}:{payload.get('metric', 'value')}"
    state["latest"][key] = payload

    db = SessionLocal()
    try:
        rules = list_rules_for_sensor(db, str(sensor_name))
        for rule in rules:
            if evaluate_rule(rule, float(value)):
                try:
                    await trigger_actuator(client, rule.actuator_name, rule.action_state)
                    logger.info("Rule %s triggered actuator %s=%s", rule.id, rule.actuator_name, rule.action_state)
                except Exception as exc:
                    logger.warning("Failed actuator trigger for rule %s: %s", rule.id, exc)
    finally:
        db.close()


async def consume_loop() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key="#")

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
    return {"status": "ok", "running": state["running"], "cached_sensors": len(state["latest"])}

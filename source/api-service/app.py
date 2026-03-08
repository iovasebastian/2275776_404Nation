import asyncio
import json
import os

import aio_pika
from fastapi import Body, FastAPI, WebSocket, WebSocketDisconnect
import httpx
from db import get_db
from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

RABBITMQ_URL = os.getenv("RABBITMQ_URL") or "amqp://guest:guest@broker:5672/"
EXCHANGE_NAME = "normalized_events"
QUEUE_NAME = "api_service_queue"
ROUTING_KEY = "normalized.api"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////app/data/rules.db")



app = FastAPI()

connected_clients: list[WebSocket] = []
latest_events: dict[str, dict] = {}
consumer_task: asyncio.Task | None = None
running = True

from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db

from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db


#ENDPOINTS
@app.get("/db-schema")
def db_schema(db: Session = Depends(get_db)):
    tables = db.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    )

    schema = {}

    for (table_name,) in tables:
        cols = db.execute(text(f"PRAGMA table_info('{table_name}')"))

        schema[table_name] = [
            {c[1] : c[2] for c in cols}
        ]

    return schema

@app.get("/latest")
async def get_latest():
    return list(latest_events.values())

@app.post("/new-rule")
async def new_rule(
    sensor_name: str = Body(...),
    operator: str = Body(...),
    threshold_value: float = Body(...),
    actuator_name: str = Body(...),
    action_state: str = Body(...),
    db: Session = Depends(get_db)
):
    db.execute(
        text("""
            INSERT INTO rules
            (sensor_name, operator, threshold_value, actuator_name, action_state)
            VALUES (:sensor_name, :operator, :threshold_value, :actuator_name, :action_state)
        """),
        {
            "sensor_name": sensor_name,
            "operator": operator,
            "threshold_value": threshold_value,
            "actuator_name": actuator_name,
            "action_state": action_state,
        }
    )

    db.commit()

    return {"status": "rule created"}

@app.put("/update-rule")
async def update_rule(rule, db: Session = Depends(get_db)):
    db.execute(
        text("""
            UPDATE rules
            SET sensor_name = :sensor_name,
                operator = :operator,
                threshold_value = :threshold_value,
                actuator_name = :actuator_name,
                action_state = :action_state
            WHERE id = :id
        """),
        {
            "id": rule.id,
            "sensor_name": rule.sensor_name,
            "operator": rule.operator,
            "threshold_value": rule.threshold_value,
            "actuator_name": rule.actuator_name,
            "action_state": rule.action_state,
        }
    )

    db.commit()

    return {"status": "rule updated"}

@app.delete("/delete-rule/{id}")
async def delete_rule(id: int, db: Session = Depends(get_db)):
    db.execute(
        text("DELETE FROM rules WHERE id = :id"),
        {"id": id}
    )

    db.commit()

    return {"status": "rule deleted"}

@app.post("/change-actuator/{actuator}/{state}")
async def change_actuator(actuator: str, state: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"http://simulator:8080/api/actuators/{actuator}",
            json={"state": state}
        )

    if response.status_code == 200:
        return {"message": f"Actuator {actuator} changed to {state}"}
    else:
        return {"error": "Failed to change actuator", "details": response.text}

async def broadcast(event: dict):
    dead_clients = []

    for ws in connected_clients:
        try:
            await ws.send_json(event)
        except Exception:
            dead_clients.append(ws)

    for ws in dead_clients:
        if ws in connected_clients:
            connected_clients.remove(ws)


async def consume_messages():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
    )

    await queue.bind(exchange, routing_key=ROUTING_KEY)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            if not running:
                break

            async with message.process():
                payload = json.loads(message.body.decode("utf-8"))

                sensor_id = payload.get("sensor_id")
                if sensor_id:
                    latest_events[sensor_id] = payload

                await broadcast(payload)


@app.on_event("startup")
async def startup():
    global consumer_task
    consumer_task = asyncio.create_task(consume_messages())


@app.on_event("shutdown")
async def shutdown():
    global running
    running = False

    if consumer_task:
        consumer_task.cancel()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)

    try:
        await websocket.send_json({
            "type": "snapshot",
            "data": list(latest_events.values())
        })

        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
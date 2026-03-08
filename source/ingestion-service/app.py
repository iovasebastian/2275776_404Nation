import asyncio
from contextlib import asynccontextmanager
import httpx
import os
import uvicorn
from fastapi import FastAPI
from fastapi import FastAPI
import json
import asyncio
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from pydantic import BaseModel
from datetime import datetime
from typing import Any
from uuid import uuid4
from broker import init_rabbitmq, publish_to_both_services, close_rabbitmq

SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://simulator:8080")
SENSOR_API_BASE_URL = f"{SIMULATOR_URL}/api/sensors"
POLL_INTERVAL_SECONDS = 5 # poll every 5 seconds

# Global state ----------------------------------------------------------------
sensor_list: list[str] = []
_poll_task: asyncio.Task | None = None
_http_client: httpx.AsyncClient | None = None
latest_sensor_data: dict[str, dict] = {} # I store the latest sensor data here
latest_events: dict[str, "NormalizedEvent"] = {}

WS_URLs = []

# Derive the WebSocket base from SIMULATOR_URL (replace http(s) with ws(s))
_ws_base = SIMULATOR_URL.replace("https://", "wss://").replace("http://", "ws://")

topic_list = ["mars/telemetry/solar_array", 
              "mars/telemetry/radiation",
              "mars/telemetry/life_support",
              "mars/telemetry/thermal_loop",
              "mars/telemetry/power_bus",
              "mars/telemetry/power_consumption",
              "mars/telemetry/airlock"
            ]
for topic in topic_list:
    WS_URLs.append(f"{_ws_base}/api/telemetry/ws?topic={topic}")

class Measurement(BaseModel):
    metric: str
    value: float
    unit: str | None = None

class NormalizedEvent(BaseModel):
    event_id: str
    source: str
    sensor_id: str
    occurred_at: datetime
    status: str | None = None
    measurements: list[Measurement]
    metadata: dict[str, Any] | None = None



def store_event(event: "NormalizedEvent") -> None:
    latest_events[event.sensor_id] = event

def print_all_events():
    print("\n===== ALL NORMALIZED EVENTS =====")
    for sensor_id, event in latest_events.items():
        print(f"\n[{sensor_id}]")
        print(event.model_dump_json(indent=4))

# Background polling -----------------------------------------------------------
async def poll_sensors() -> None:
    while True:
        for sensor_name in sensor_list:
            try:
                response = await _http_client.get(
                    f"{SENSOR_API_BASE_URL}/{sensor_name}"
                )
                response.raise_for_status()
                payload = response.json()
                normalized_event = normalize_sensor_event(payload)
                store_event(normalized_event)
                await publish_to_both_services(
                    normalized_event.model_dump_json().encode("utf-8")
                )
            except httpx.HTTPStatusError as exc:
                print(
                    f"[Sensor: {sensor_name}] HTTP error "
                    f"{exc.response.status_code}: {exc.response.text}"
                )
            except httpx.RequestError as exc:
                print(f"[Sensor: {sensor_name}] Request failed: {exc}")

        print("-" * 80 + "\n")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# Lifespan ---------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global sensor_list, _poll_task, _http_client

    # --- Startup ---
    _http_client = httpx.AsyncClient()

    await init_rabbitmq()

    for WS_URL in WS_URLs:
        asyncio.create_task(websocket_listener(WS_URL))

    try:
        print("Discovering sensors …")
        response = await _http_client.get(SENSOR_API_BASE_URL)
        response.raise_for_status()
        sensor_list = response.json().get("sensors", [])
        print(f"Discovered {len(sensor_list)} sensor(s): {sensor_list}")
    except httpx.RequestError as exc:
        print(f"Sensor discovery failed (request error): {exc}")
    except httpx.HTTPStatusError as exc:
        print(
            f"Sensor discovery failed (HTTP {exc.response.status_code}): "
            f"{exc.response.text}"
        )

    if sensor_list:
        _poll_task = asyncio.create_task(poll_sensors())

    yield

    # --- Shutdown ---
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass

    if _http_client is not None:
        await _http_client.aclose()

    await close_rabbitmq()

    print("Ingestion service shut down cleanly.")

    
# FastAPI app ------------------------------------------------------------------
app = FastAPI(
    title="Ingestion Service",
    version="0.1.0",
    lifespan=lifespan,
)



class Event(BaseModel): #every topic has topic and event_time
    name : str
    topic : str
    event_time: datetime

class PowerEvent(Event): #solar_array, power_bus, power_consumption
    subsystem : str
    power_kw : float
    voltage_v : float
    current_a : float
    cumulative_kwh : float

class MeasurementEvent(Event): #radiation, life_support
    source : dict[str,str]
    measurements: list
    status: str

class ThermalLoopEvent(Event): #thermal_loop
    loop: str
    temperature_c : float
    flow_l_min : float
    status : str

class AirlockEvent(Event): #airlock
    airlock_id : str
    cycles_per_hour : float
    last_state : str

EVENT_CLASS_MAP = {
    topic_list[0]: PowerEvent,
    topic_list[4]: PowerEvent,
    topic_list[5]: PowerEvent,

    topic_list[1]: MeasurementEvent,
    topic_list[2]: MeasurementEvent,

    topic_list[3]: ThermalLoopEvent,

    topic_list[6]: AirlockEvent,
}




def normalize_sensor_event(data: dict[str, Any]) -> NormalizedEvent:
    measurements: list[Measurement] = []

    if "measurements" in data:
        measurements = [
            Measurement(
                metric=m["metric"],
                value=m["value"],
                unit=m.get("unit")
            )
            for m in data["measurements"]
        ]
    elif "metric" in data and "value" in data:
        measurements = [
            Measurement(
                metric=data["metric"],
                value=data["value"],
                unit=data.get("unit")
            )
        ]
    elif "level_pct" in data and "level_liters" in data:
        measurements = [
            Measurement(metric="level_pct", value=data["level_pct"], unit="%"),
            Measurement(metric="level_liters", value=data["level_liters"], unit="L"),
        ]
    elif "pm1_ug_m3" in data:
        measurements = [
            Measurement(metric="pm1_ug_m3", value=data["pm1_ug_m3"], unit="ug/m3"),
            Measurement(metric="pm25_ug_m3", value=data["pm25_ug_m3"], unit="ug/m3"),
            Measurement(metric="pm10_ug_m3", value=data["pm10_ug_m3"], unit="ug/m3"),
        ]

    return NormalizedEvent(
        event_id=str(uuid4()),
        source="sensor-api",
        sensor_id=data["sensor_id"],
        occurred_at=data["captured_at"],
        status=data.get("status"),
        measurements=measurements,
        metadata={},
    )

def normalize_telemetry_event(event_object: Event) -> NormalizedEvent:
    data = event_object.model_dump()

    topic = data["topic"]
    sensor_id = data["name"]
    status = data.get("status")

    measurements: list[Measurement] = []
    metadata: dict[str, Any] = {"topic": topic}

    if topic in [
        "mars/telemetry/solar_array",
        "mars/telemetry/power_bus",
        "mars/telemetry/power_consumption",
    ]:
        measurements = [
            Measurement(metric="power_kw", value=data["power_kw"], unit="kW"),
            Measurement(metric="voltage_v", value=data["voltage_v"], unit="V"),
            Measurement(metric="current_a", value=data["current_a"], unit="A"),
            Measurement(metric="cumulative_kwh", value=data["cumulative_kwh"], unit="kWh"),
        ]
        metadata["subsystem"] = data["subsystem"]

    elif topic in [
        "mars/telemetry/radiation",
        "mars/telemetry/life_support",
    ]:
        measurements = [
            Measurement(
                metric=m["metric"],
                value=m["value"],
                unit=m.get("unit")
            )
            for m in data["measurements"]
        ]
        metadata["source_info"] = data["source"]

    elif topic == "mars/telemetry/thermal_loop":
        measurements = [
            Measurement(metric="temperature_c", value=data["temperature_c"], unit="C"),
            Measurement(metric="flow_l_min", value=data["flow_l_min"], unit="L/min"),
        ]
        metadata["loop"] = data["loop"]

    elif topic == "mars/telemetry/airlock":
        measurements = [
            Measurement(metric="cycles_per_hour", value=data["cycles_per_hour"], unit="cycles/hour"),
        ]
        metadata["airlock_id"] = data["airlock_id"]
        metadata["last_state"] = data["last_state"]

    return NormalizedEvent(
        event_id=str(uuid4()),
        source="telemetry-ws",
        sensor_id=sensor_id,
        occurred_at=data["event_time"],
        status=status,
        measurements=measurements,
        metadata=metadata,
    )

async def websocket_listener(WS_URL : str):
    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                print(f"Connected to WS, {WS_URL}")
                while True:
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=10)
                    data = json.loads(raw_message)
                    topic = data["topic"]
                    event_class = EVENT_CLASS_MAP.get(topic)
                    event_object = event_class(name=topic.split("/")[-1], **data)

                    normalized_event = normalize_telemetry_event(event_object)
                    store_event(normalized_event)
                    await publish_to_both_services(
                        normalized_event.model_dump_json().encode("utf-8")
                    )
        except asyncio.TimeoutError:
            print("No message received in 10 seconds")
        except ConnectionClosedOK:
            print(f"[{topic}] WebSocket closed normally")
            await asyncio.sleep(1)
        except Exception as e:
            print("Websocket error", e)
            await asyncio.sleep(5)

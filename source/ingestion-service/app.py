import asyncio
from contextlib import asynccontextmanager
import httpx
import uvicorn
from fastapi import FastAPI

SENSOR_API_BASE_URL = "http://localhost:8080/api/sensors"
POLL_INTERVAL_SECONDS = 5

# Global state ----------------------------------------------------------------
sensor_list: list[str] = []
_poll_task: asyncio.Task | None = None
_http_client: httpx.AsyncClient | None = None
latest_sensor_data: dict[str, dict] = {}


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
                print(f"[Sensor: {sensor_name}] {payload}\n")
                latest_sensor_data[sensor_name] = payload
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

    print("Ingestion service shut down cleanly.")


# FastAPI app ------------------------------------------------------------------
app = FastAPI(
    title="Ingestion Service",
    version="0.1.0",
    lifespan=lifespan,
)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)

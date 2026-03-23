import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from api.anomalies import router as anomalies_router
from api.router import router
from api.vessels import router as vessels_router
from config import settings
from database import Base, engine
from detection.engine import run_detection_engine
from ingestion.poller import run_poller

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    poller_task    = asyncio.create_task(run_poller())
    detection_task = asyncio.create_task(run_detection_engine())

    yield

    for task, name in [(poller_task, "AIS poller"), (detection_task, "detection engine")]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info("%s stopped", name)

    await engine.dispose()


app = FastAPI(
    title="Maritime Domain Awareness",
    description="Maritime situational awareness and anomaly detection system",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")
app.include_router(vessels_router, prefix="/api/v1")
app.include_router(anomalies_router, prefix="/api/v1")


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/static/index.html")


@app.get("/map", include_in_schema=False)
async def map_view():
    return RedirectResponse(url="/static/map.html")


app.mount("/static", StaticFiles(directory="frontend/static"), name="static")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )

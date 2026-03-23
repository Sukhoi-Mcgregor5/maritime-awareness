import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from api.router import router
from api.vessels import router as vessels_router
from config import settings
from database import Base, engine
from ingestion.poller import run_poller

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    poller_task = asyncio.create_task(run_poller())

    yield

    poller_task.cancel()
    try:
        await poller_task
    except asyncio.CancelledError:
        logger.info("AIS poller stopped")

    await engine.dispose()


app = FastAPI(
    title="Maritime Domain Awareness",
    description="Maritime situational awareness and anomaly detection system",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")
app.include_router(vessels_router, prefix="/api/v1")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )

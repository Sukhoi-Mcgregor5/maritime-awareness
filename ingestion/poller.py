"""
Background polling loop.

Fetches AIS vessel positions from AISHub every `ais_poll_interval` seconds
and upserts them into the vessels table.  The loop runs as an asyncio Task
started in FastAPI's lifespan context.
"""

import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert

from config import settings
from database import AsyncSessionLocal
from ingestion.client import AISHubClient, AISHubError, BoundingBox
from ingestion.normalizer import normalize
from ontology.models import Vessel

logger = logging.getLogger(__name__)

# Restrict polling to a broad area by default (whole world).
# Narrow this down to your area of interest to reduce payload size.
DEFAULT_BBOX = BoundingBox(lat_min=-90, lat_max=90, lon_min=-180, lon_max=180)


async def _upsert_vessels(records: list[dict]) -> int:
    """
    Upsert normalised vessel records into the database.

    On MMSI conflict: update all position fields and identity fields that
    are non-null in the incoming record. Physical dimensions (length, beam,
    etc.) are excluded from the update set so manually-entered values are
    never overwritten by the feed.

    Returns the number of rows processed.
    """
    if not records:
        return 0

    UPDATE_FIELDS = (
        "imo", "name", "call_sign", "flag", "vessel_type",
        "latitude", "longitude",
        "speed_over_ground", "course_over_ground", "heading",
        "nav_status", "position_timestamp",
    )

    async with AsyncSessionLocal() as session:
        stmt = (
            insert(Vessel)
            .values(records)
            .on_conflict_do_update(
                index_elements=["mmsi"],
                set_={col: insert(Vessel).excluded[col] for col in UPDATE_FIELDS},
            )
        )
        await session.execute(stmt)
        await session.commit()

    return len(records)


async def _poll_once(client: AISHubClient, bbox: BoundingBox | None) -> None:
    """Run a single fetch → normalise → upsert cycle."""
    try:
        raw_vessels = await client.fetch_vessels(bbox)
    except AISHubError as exc:
        logger.error("AISHub fetch failed: %s", exc)
        return
    except Exception as exc:
        logger.exception("Unexpected error fetching AIS data: %s", exc)
        return

    records = [r for raw in raw_vessels if (r := normalize(raw)) and r.get("mmsi")]

    if not records:
        logger.warning("No valid AIS records after normalisation")
        return

    try:
        count = await _upsert_vessels(records)
        logger.info("Upserted %d vessel records", count)
    except Exception as exc:
        logger.exception("DB upsert failed: %s", exc)


async def run_poller(bbox: BoundingBox | None = DEFAULT_BBOX) -> None:
    """
    Infinite polling loop.  Intended to be run as an asyncio Task.

    Call `asyncio.create_task(run_poller())` from FastAPI's lifespan and
    cancel the task on shutdown.
    """
    client = AISHubClient()
    interval = settings.ais_poll_interval
    logger.info("AIS poller started — interval %ds", interval)

    while True:
        await _poll_once(client, bbox)
        await asyncio.sleep(interval)

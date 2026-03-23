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
from ontology.models import Vessel, VesselTrack

logger = logging.getLogger(__name__)

# Restrict polling to a broad area by default (whole world).
# Narrow this down to your area of interest to reduce payload size.
DEFAULT_BBOX = BoundingBox(lat_min=-90, lat_max=90, lon_min=-180, lon_max=180)


_VESSEL_UPDATE_FIELDS = (
    "imo", "name", "call_sign", "flag", "vessel_type",
    "latitude", "longitude",
    "speed_over_ground", "course_over_ground", "heading",
    "nav_status", "position_timestamp",
)

_TRACK_FIELDS = (
    "mmsi", "latitude", "longitude",
    "speed_over_ground", "course_over_ground", "heading",
    "nav_status", "recorded_at",
)


def _to_track_record(r: dict) -> dict | None:
    """Extract a VesselTrack row from a normalised vessel record.

    Returns None when the record lacks a valid position or timestamp.
    """
    if r.get("latitude") is None or r.get("longitude") is None:
        return None
    recorded_at = r.get("position_timestamp")
    if recorded_at is None:
        return None
    return {
        "mmsi": r["mmsi"],
        "latitude": r["latitude"],
        "longitude": r["longitude"],
        "speed_over_ground": r.get("speed_over_ground"),
        "course_over_ground": r.get("course_over_ground"),
        "heading": r.get("heading"),
        "nav_status": r.get("nav_status"),
        "recorded_at": recorded_at,
    }


async def _upsert_vessels(records: list[dict]) -> int:
    """
    Upsert normalised vessel records and append track history in one transaction.

    Vessel upsert: overwrites position/identity fields; never touches physical
    dimensions set manually.

    Track insert: ON CONFLICT (mmsi, recorded_at) DO NOTHING — re-polling the
    same data is idempotent.

    Returns the number of vessel records processed.
    """
    if not records:
        return 0

    track_records = [t for r in records if (t := _to_track_record(r))]

    async with AsyncSessionLocal() as session:
        vessel_stmt = (
            insert(Vessel)
            .values(records)
            .on_conflict_do_update(
                index_elements=["mmsi"],
                set_={col: insert(Vessel).excluded[col] for col in _VESSEL_UPDATE_FIELDS},
            )
        )
        await session.execute(vessel_stmt)

        if track_records:
            track_stmt = (
                insert(VesselTrack)
                .values(track_records)
                .on_conflict_do_nothing(
                    index_elements=["mmsi", "recorded_at"],
                )
            )
            await session.execute(track_stmt)

        await session.commit()

    logger.debug("Inserted %d track points", len(track_records))
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

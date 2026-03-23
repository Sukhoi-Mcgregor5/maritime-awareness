"""
AIS ingestion task.

Consumes the AISStream.io WebSocket feed (push-based, no polling loop) and
flushes buffered records to the database every `ais_batch_interval` seconds
or whenever the buffer reaches `ais_batch_max` messages, whichever comes first.

Two message types are handled:
  PositionReport  → upsert vessel position + insert VesselTrack row
  ShipStaticData  → upsert vessel identity/static fields only
"""

import asyncio
import logging
import time

from sqlalchemy.dialects.postgresql import insert

from config import settings
from database import AsyncSessionLocal
from ingestion.client import AISStreamClient
from ingestion.normalizer import normalize_position, normalize_static
from ontology.models import Vessel, VesselTrack

logger = logging.getLogger(__name__)

# Fields updated from a PositionReport upsert
_POSITION_FIELDS = (
    "name", "vessel_type",
    "latitude", "longitude",
    "speed_over_ground", "course_over_ground", "heading",
    "nav_status", "position_timestamp",
)

# Fields updated from a ShipStaticData upsert
_STATIC_FIELDS = (
    "imo", "name", "call_sign", "vessel_type",
    "length", "beam", "draught",
)


def _to_track(r: dict) -> dict | None:
    if r.get("latitude") is None or r.get("longitude") is None:
        return None
    if (recorded_at := r.get("position_timestamp")) is None:
        return None
    return {
        "mmsi":               r["mmsi"],
        "latitude":           r["latitude"],
        "longitude":          r["longitude"],
        "speed_over_ground":  r.get("speed_over_ground"),
        "course_over_ground": r.get("course_over_ground"),
        "heading":            r.get("heading"),
        "nav_status":         r.get("nav_status"),
        "recorded_at":        recorded_at,
    }


async def _flush(positions: list[dict], statics: list[dict]) -> None:
    """Write buffered records to the database in a single transaction."""
    if not positions and not statics:
        return

    track_records = [t for r in positions if (t := _to_track(r))]

    async with AsyncSessionLocal() as session:
        if positions:
            pos_stmt = (
                insert(Vessel)
                .values(positions)
                .on_conflict_do_update(
                    index_elements=["mmsi"],
                    set_={col: insert(Vessel).excluded[col] for col in _POSITION_FIELDS},
                )
            )
            await session.execute(pos_stmt)

        if statics:
            static_stmt = (
                insert(Vessel)
                .values(statics)
                .on_conflict_do_update(
                    index_elements=["mmsi"],
                    set_={col: insert(Vessel).excluded[col] for col in _STATIC_FIELDS},
                )
            )
            await session.execute(static_stmt)

        if track_records:
            track_stmt = (
                insert(VesselTrack)
                .values(track_records)
                .on_conflict_do_nothing(index_elements=["mmsi", "recorded_at"])
            )
            await session.execute(track_stmt)

        await session.commit()

    logger.info(
        "Flushed — %d position, %d static, %d track records",
        len(positions), len(statics), len(track_records),
    )


async def run_poller(
    bounding_boxes: list[list[list[float]]] | None = None,
) -> None:
    """
    Consume the AISStream WebSocket feed and persist records to the database.

    Intended to be run as an asyncio Task started from FastAPI's lifespan.
    The client handles reconnection internally; this function runs until
    the task is cancelled.
    """
    client = AISStreamClient(bounding_boxes=bounding_boxes)

    batch_interval = settings.ais_batch_interval
    batch_max      = settings.ais_batch_max

    positions: list[dict] = []
    statics:   list[dict] = []
    last_flush = time.monotonic()

    logger.info("AIS ingestion started — flush every %ds or %d msgs", batch_interval, batch_max)

    async for msg in client.stream():
        msg_type = msg.get("MessageType")

        if msg_type == "PositionReport":
            if rec := normalize_position(msg):
                positions.append(rec)

        elif msg_type == "ShipStaticData":
            if rec := normalize_static(msg):
                statics.append(rec)

        total    = len(positions) + len(statics)
        elapsed  = time.monotonic() - last_flush
        if total >= batch_max or (total > 0 and elapsed >= batch_interval):
            try:
                await _flush(positions, statics)
            except Exception:
                logger.exception("DB flush failed")
            positions.clear()
            statics.clear()
            last_flush = time.monotonic()

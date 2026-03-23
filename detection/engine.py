"""
Detection engine — background task.

Runs all detectors on a schedule, then reconciles findings against the
anomalies table:
  - New finding with no active anomaly  → INSERT active anomaly
  - Existing active anomaly, still firing → UPDATE details (refresh context)
  - Existing active anomaly, no longer firing → resolve it (set resolved_at)
"""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import and_, select, update

from config import settings
from database import AsyncSessionLocal
from detection.detector import Finding, detect_dark_vessels, detect_loitering
from ontology.models import Anomaly, AnomalyStatus, AnomalyType

logger = logging.getLogger(__name__)


async def _reconcile(session, findings: list[Finding], anomaly_type: AnomalyType) -> None:
    """Open new anomalies, refresh active ones, resolve cleared ones."""
    firing_mmsis = {f.mmsi for f in findings}
    findings_by_mmsi = {f.mmsi: f for f in findings}

    # Fetch all currently active anomalies of this type
    active_rows = (
        await session.execute(
            select(Anomaly).where(
                and_(
                    Anomaly.anomaly_type == anomaly_type,
                    Anomaly.status == AnomalyStatus.active,
                )
            )
        )
    ).scalars().all()
    active_mmsis = {a.mmsi for a in active_rows}
    active_by_mmsi = {a.mmsi: a for a in active_rows}

    now = datetime.now(timezone.utc)

    # Open new anomalies
    new_mmsis = firing_mmsis - active_mmsis
    for mmsi in new_mmsis:
        session.add(Anomaly(
            mmsi=mmsi,
            anomaly_type=anomaly_type,
            status=AnomalyStatus.active,
            details=findings_by_mmsi[mmsi].details,
        ))
        logger.info("Opened %s anomaly for MMSI %s", anomaly_type.value, mmsi)

    # Refresh details on still-active anomalies
    for mmsi in firing_mmsis & active_mmsis:
        active_by_mmsi[mmsi].details = findings_by_mmsi[mmsi].details

    # Resolve anomalies that are no longer firing
    cleared_mmsis = active_mmsis - firing_mmsis
    if cleared_mmsis:
        await session.execute(
            update(Anomaly)
            .where(
                and_(
                    Anomaly.anomaly_type == anomaly_type,
                    Anomaly.status == AnomalyStatus.active,
                    Anomaly.mmsi.in_(cleared_mmsis),
                )
            )
            .values(status=AnomalyStatus.resolved, resolved_at=now)
        )
        for mmsi in cleared_mmsis:
            logger.info("Resolved %s anomaly for MMSI %s", anomaly_type.value, mmsi)


async def _run_once() -> None:
    async with AsyncSessionLocal() as session:
        dark = await detect_dark_vessels(
            session,
            silence_minutes=settings.dark_vessel_silence_minutes,
            lookback_hours=settings.dark_vessel_lookback_hours,
        )
        await _reconcile(session, dark, AnomalyType.dark_vessel)

        loitering = await detect_loitering(
            session,
            window_hours=settings.loitering_window_hours,
            min_duration_minutes=settings.loitering_min_duration_minutes,
            max_displacement_nm=settings.loitering_max_displacement_nm,
            max_avg_sog=settings.loitering_max_avg_sog,
        )
        await _reconcile(session, loitering, AnomalyType.loitering)

        await session.commit()

    total = len(dark) + len(loitering)
    logger.info(
        "Detection run complete — %d dark vessel(s), %d loitering",
        len(dark), len(loitering),
    )


async def run_detection_engine() -> None:
    """
    Periodic detection loop.  Runs every `detection_interval_seconds`.
    Intended to be started as an asyncio Task from FastAPI's lifespan.
    """
    interval = settings.detection_interval_seconds
    logger.info("Detection engine started — interval %ds", interval)

    # Initial delay so the DB has some data before the first run
    await asyncio.sleep(min(interval, 60))

    while True:
        try:
            await _run_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Detection run failed")
        await asyncio.sleep(interval)

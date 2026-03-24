"""
Anomaly detectors.

Each detector is a pure async function that queries the database and returns
a list of Finding objects.  No anomaly records are written here — that is the
engine's responsibility so it can handle open/resolve lifecycle cleanly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import atan2, cos, radians, sin, sqrt

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ontology.models import AnomalyType, NavigationStatus, Vessel, VesselTrack

logger = logging.getLogger(__name__)


@dataclass
class Finding:
    mmsi:         str
    anomaly_type: AnomalyType
    details:      dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R = 3440.065  # Earth radius in nautical miles
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# ---------------------------------------------------------------------------
# Dark vessel detector
# ---------------------------------------------------------------------------

async def detect_dark_vessels(
    session:             AsyncSession,
    silence_minutes:     int = 60,
    active_window_hours: int = 6,
    min_active_points:   int = 10,
) -> list[Finding]:
    """
    Identify vessels that were actively transmitting and have gone silent.

    A vessel is flagged only when ALL of the following are true:
      - It has >= min_active_points track records within the last active_window_hours
        (10+ points proves sustained active transmission, not a one-off ping)
      - Its most recent track record is older than silence_minutes ago (default 1 hour)
      - Its current nav_status is not 'moored' or 'at_anchor'
        (AIS-off while berthed or anchored is normal behaviour)

    Querying VesselTrack (not Vessel.position_timestamp) is intentional:
    the Vessel table holds stale upserted data for 22k+ historical vessels,
    which would produce false positives for every vessel we've ever seen.
    """
    now          = datetime.now(timezone.utc)
    active_since = now - timedelta(hours=active_window_hours)
    silent_since = now - timedelta(minutes=silence_minutes)

    stationary = {NavigationStatus.moored, NavigationStatus.at_anchor}

    # Aggregate recent track activity per vessel, joining vessels to filter stationary ones
    active_rows = (
        await session.execute(
            select(
                VesselTrack.mmsi,
                func.count(VesselTrack.id).label("point_count"),
                func.max(VesselTrack.recorded_at).label("last_seen"),
                Vessel.name,
                Vessel.latitude,
                Vessel.longitude,
                Vessel.nav_status,
            )
            .join(Vessel, Vessel.mmsi == VesselTrack.mmsi)
            .where(VesselTrack.recorded_at >= active_since)
            .where(Vessel.nav_status.notin_(stationary))
            .group_by(VesselTrack.mmsi, Vessel.name, Vessel.latitude, Vessel.longitude, Vessel.nav_status)
            .having(func.count(VesselTrack.id) >= min_active_points)
        )
    ).all()

    findings = []
    for row in active_rows:
        # Only flag if the last transmission crossed the silence threshold
        if row.last_seen >= silent_since:
            continue

        silence_duration = (now - row.last_seen).total_seconds() / 60
        findings.append(Finding(
            mmsi=row.mmsi,
            anomaly_type=AnomalyType.dark_vessel,
            details={
                "last_seen_at":            row.last_seen.isoformat(),
                "silence_minutes":         round(silence_duration, 1),
                "active_points_in_window": row.point_count,
                "last_lat":                row.latitude,
                "last_lon":                row.longitude,
                "vessel_name":             row.name,
            },
        ))

    logger.debug("Dark vessel detector: %d findings", len(findings))
    return findings


# ---------------------------------------------------------------------------
# Loitering detector
# ---------------------------------------------------------------------------

async def detect_loitering(
    session:               AsyncSession,
    window_hours:          float = 2.0,
    min_duration_minutes:  float = 30.0,
    max_displacement_nm:   float = 1.0,
    max_avg_sog:           float = 0.5,
    min_track_points:      int   = 10,
    recently_active_hours: float = 1.0,
) -> list[Finding]:
    """
    Identify vessels drifting or circling in a small area for an extended period.

    A vessel is flagged when, over the last `window_hours`:
      - It has at least `min_track_points` track records
      - The track spans at least `min_duration_minutes`
      - Its most recent track point is within the last `recently_active_hours`
        (excludes vessels that stopped transmitting earlier in the window)
      - Its straight-line displacement (first → last point) is under
        `max_displacement_nm` nautical miles
      - Its average SOG is below `max_avg_sog` knots
      - Its current nav_status is NOT 'moored' or 'at_anchor'
        (legitimately stationary vessels should not be flagged)
    """
    now            = datetime.now(timezone.utc)
    since          = now - timedelta(hours=window_hours)
    recently_since = now - timedelta(hours=recently_active_hours)

    # Step 1: aggregate stats per vessel in the window
    agg = (
        await session.execute(
            select(
                VesselTrack.mmsi,
                func.count(VesselTrack.id).label("point_count"),
                func.min(VesselTrack.recorded_at).label("first_seen"),
                func.max(VesselTrack.recorded_at).label("last_seen"),
                func.avg(VesselTrack.speed_over_ground).label("avg_sog"),
            )
            .where(VesselTrack.recorded_at >= since)
            .group_by(VesselTrack.mmsi)
            .having(func.count(VesselTrack.id) >= min_track_points)
        )
    ).all()

    # Filter by duration, SOG, and recency before the more expensive position queries
    candidates = [
        row for row in agg
        if row.avg_sog is not None
        and row.avg_sog <= max_avg_sog
        and row.last_seen >= recently_since
        and (row.last_seen - row.first_seen).total_seconds() / 60 >= min_duration_minutes
    ]

    if not candidates:
        return []

    # Step 2: drop vessels that are legitimately stationary (moored / at anchor)
    stationary = {NavigationStatus.moored, NavigationStatus.at_anchor}
    nav_rows = (
        await session.execute(
            select(Vessel.mmsi, Vessel.nav_status)
            .where(Vessel.mmsi.in_([r.mmsi for r in candidates]))
        )
    ).all()
    stationary_mmsis = {v.mmsi for v in nav_rows if v.nav_status in stationary}
    candidates = [r for r in candidates if r.mmsi not in stationary_mmsis]

    if not candidates:
        return []

    # Step 3: get first and last track point per candidate to compute displacement
    mmsi_list = [r.mmsi for r in candidates]

    # Subquery: row number per mmsi ordered by recorded_at (first and last)
    first_points_stmt = (
        select(VesselTrack.mmsi, VesselTrack.latitude, VesselTrack.longitude)
        .where(VesselTrack.mmsi.in_(mmsi_list))
        .where(VesselTrack.recorded_at >= since)
        .distinct(VesselTrack.mmsi)
        .order_by(VesselTrack.mmsi, VesselTrack.recorded_at.asc())
    )
    last_points_stmt = (
        select(VesselTrack.mmsi, VesselTrack.latitude, VesselTrack.longitude)
        .where(VesselTrack.mmsi.in_(mmsi_list))
        .where(VesselTrack.recorded_at >= since)
        .distinct(VesselTrack.mmsi)
        .order_by(VesselTrack.mmsi, VesselTrack.recorded_at.desc())
    )

    first_points = {r.mmsi: (r.latitude, r.longitude) for r in (await session.execute(first_points_stmt)).all()}
    last_points  = {r.mmsi: (r.latitude, r.longitude) for r in (await session.execute(last_points_stmt)).all()}

    findings = []
    for row in candidates:
        fp = first_points.get(row.mmsi)
        lp = last_points.get(row.mmsi)
        if not fp or not lp:
            continue

        displacement = _haversine_nm(fp[0], fp[1], lp[0], lp[1])
        if displacement > max_displacement_nm:
            continue

        duration_minutes = (row.last_seen - row.first_seen).total_seconds() / 60
        findings.append(Finding(
            mmsi=row.mmsi,
            anomaly_type=AnomalyType.loitering,
            details={
                "window_hours":       window_hours,
                "duration_minutes":   round(duration_minutes, 1),
                "displacement_nm":    round(displacement, 3),
                "avg_sog_knots":      round(float(row.avg_sog), 2),
                "track_points":       row.point_count,
                "first_seen":         row.first_seen.isoformat(),
                "last_seen":          row.last_seen.isoformat(),
                "first_lat":          fp[0],
                "first_lon":          fp[1],
                "last_lat":           lp[0],
                "last_lon":           lp[1],
            },
        ))

    logger.debug("Loitering detector: %d findings", len(findings))
    return findings

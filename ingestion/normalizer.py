"""
Convert raw AISStream.io messages into dicts suitable for upserting into
the Vessel and VesselTrack tables.

AISStream delivers two relevant message types:
  - PositionReport   (AIS msg 1/2/3): real-time lat/lon, SOG, COG, heading
  - ShipStaticData   (AIS msg 5):     name, call sign, IMO, type, dimensions
"""

import logging
from datetime import datetime, timezone

from ontology.models import NavigationStatus, VesselType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AIS vessel type code → VesselType  (ITU-R M.1371-5, Table 53)
# ---------------------------------------------------------------------------

def _vessel_type(code: int | None) -> VesselType:
    if code is None:
        return VesselType.unknown
    if 70 <= code <= 79:
        return VesselType.cargo
    if 80 <= code <= 89:
        return VesselType.tanker
    if 60 <= code <= 69:
        return VesselType.passenger
    if code == 30:
        return VesselType.fishing
    if code in (52, 53):
        return VesselType.tug
    if code == 35:
        return VesselType.military
    if code in (36, 37):
        return VesselType.pleasure
    if code == 0:
        return VesselType.unknown
    return VesselType.other


# ---------------------------------------------------------------------------
# AIS navigation status code → NavigationStatus  (ITU-R M.1371-5, Table 45)
# ---------------------------------------------------------------------------

_NAV_STATUS_MAP: dict[int, NavigationStatus] = {
    0:  NavigationStatus.under_way_engine,
    1:  NavigationStatus.at_anchor,
    2:  NavigationStatus.not_under_command,
    3:  NavigationStatus.restricted_maneuverability,
    5:  NavigationStatus.moored,
    6:  NavigationStatus.aground,
    15: NavigationStatus.unknown,
}


def _nav_status(code: int | None) -> NavigationStatus:
    if code is None:
        return NavigationStatus.unknown
    return _NAV_STATUS_MAP.get(code, NavigationStatus.unknown)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sentinel_to_none(value: float | int | None, sentinel: float) -> float | None:
    """Replace AIS sentinel values with None."""
    if value is None or value == sentinel:
        return None
    return float(value)


def _parse_timestamp(raw: str | None) -> datetime | None:
    """
    Parse AISStream's time_utc field to an aware UTC datetime.

    AISStream sends time_utc in the format:
        "2023-10-15 12:34:56.123456 +0000 UTC"
    The trailing ' UTC' token is not valid ISO 8601, so fromisoformat()
    fails silently. We strip it before parsing.
    """
    if not raw:
        return None

    # Strip the redundant trailing ' UTC' AISStream appends
    cleaned = raw.removesuffix(" UTC").strip()

    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        logger.debug("Parsed timestamp %r → %s", raw, dt)
        return dt
    except ValueError:
        logger.warning("Failed to parse timestamp %r (cleaned: %r)", raw, cleaned)
        return None


def _mmsi(meta: dict, body: dict) -> str | None:
    raw = meta.get("MMSI") or body.get("UserID")
    mmsi = str(raw).strip() if raw is not None else ""
    return mmsi if len(mmsi) == 9 and mmsi.isdigit() else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_position(msg: dict) -> dict | None:
    """
    Normalise a PositionReport message.

    Returns a flat dict of Vessel column values suitable for upsert, or None
    if the message is missing an MMSI.  The dict also carries 'position_timestamp'
    which the poller uses to build a VesselTrack row.
    """
    meta = msg.get("MetaData", {})
    body = msg.get("Message", {}).get("PositionReport", {})

    mmsi = _mmsi(meta, body)
    if not mmsi:
        return None

    # Prefer MetaData coordinates (already decoded); fall back to body
    lat = meta.get("latitude") if meta.get("latitude") is not None else body.get("Latitude")
    lon = meta.get("longitude") if meta.get("longitude") is not None else body.get("Longitude")
    if lat is None or lon is None or (lat == 0.0 and lon == 0.0):
        lat = lon = None

    time_utc = meta.get("time_utc")
    position_timestamp = _parse_timestamp(time_utc)
    if position_timestamp is None:
        logger.debug("MMSI %s: time_utc=%r → position_timestamp=None (track will be skipped)", mmsi, time_utc)

    return {
        "mmsi":               mmsi,
        "name":               (meta.get("ShipName") or "").strip() or None,
        "vessel_type":        VesselType.unknown,  # not in position reports
        "latitude":           float(lat) if lat is not None else None,
        "longitude":          float(lon) if lon is not None else None,
        "speed_over_ground":  _sentinel_to_none(body.get("Sog"), 102.3),
        "course_over_ground": _sentinel_to_none(body.get("Cog"), 360.0),
        "heading":            _sentinel_to_none(body.get("TrueHeading"), 511),
        "nav_status":         _nav_status(body.get("NavigationalStatus")),
        "position_timestamp": position_timestamp,
    }


def normalize_static(msg: dict) -> dict | None:
    """
    Normalise a ShipStaticData message.

    Returns a flat dict of Vessel column values.  Position fields are omitted
    so the upsert never overwrites a more recent position with stale static data.
    """
    meta = msg.get("MetaData", {})
    body = msg.get("Message", {}).get("ShipStaticData", {})

    mmsi = _mmsi(meta, body)
    if not mmsi:
        return None

    imo_raw = body.get("ImoNumber")
    imo = str(int(imo_raw)) if imo_raw and int(imo_raw) > 0 else None

    dim    = body.get("Dimension") or {}
    length = (dim.get("A") or 0) + (dim.get("B") or 0) or None
    beam   = (dim.get("C") or 0) + (dim.get("D") or 0) or None
    draught = body.get("Draught") or None

    return {
        "mmsi":         mmsi,
        "imo":          imo,
        "name":         (body.get("Name") or meta.get("ShipName") or "").strip() or None,
        "call_sign":    (body.get("CallSign") or "").strip() or None,
        "vessel_type":  _vessel_type(body.get("Type")),
        "length":       float(length) if length else None,
        "beam":         float(beam) if beam else None,
        "draught":      float(draught) if draught else None,
    }

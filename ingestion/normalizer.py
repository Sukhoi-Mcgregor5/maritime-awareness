"""
Convert raw AISHub vessel records into dicts suitable for upserting into
the Vessel table.  All field names match Vessel column names exactly.
"""

from datetime import datetime, timezone

from ontology.models import NavigationStatus, VesselType

# ---------------------------------------------------------------------------
# AIS vessel type code → VesselType
# Reference: ITU-R M.1371-5, Table 53
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
# AIS navigation status code → NavigationStatus
# Reference: ITU-R M.1371-5, Table 45
# ---------------------------------------------------------------------------

_NAV_STATUS_MAP: dict[int, NavigationStatus] = {
    0: NavigationStatus.under_way_engine,
    1: NavigationStatus.at_anchor,
    2: NavigationStatus.not_under_command,
    3: NavigationStatus.restricted_maneuverability,
    5: NavigationStatus.moored,
    6: NavigationStatus.aground,
    15: NavigationStatus.unknown,
}


def _nav_status(code: int | None) -> NavigationStatus:
    if code is None:
        return NavigationStatus.unknown
    return _NAV_STATUS_MAP.get(code, NavigationStatus.unknown)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(raw: str | None) -> datetime | None:
    """Parse AISHub time string 'YYYY-MM-DD HH:MM:SS' to aware UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _sentinel_to_none(value: float | int | None, sentinel) -> float | None:
    """AISHub uses magic numbers (511, 102.3, etc.) to signal 'not available'."""
    if value is None or value == sentinel:
        return None
    return float(value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize(raw: dict) -> dict:
    """
    Convert one raw AISHub vessel record to a flat dict of Vessel column values.

    Only position/identity fields that AISHub provides are mapped; physical
    dimensions (length, beam, draught, gross_tonnage) are not in the AISHub
    feed and are left as None so existing DB values are preserved on upsert.
    """
    mmsi = str(raw.get("MMSI", "")).strip()
    if not mmsi:
        return {}

    imo_raw = raw.get("IMO")
    imo = str(imo_raw) if imo_raw and int(imo_raw) > 0 else None

    sog = _sentinel_to_none(raw.get("SOG"), 102.3)   # 102.3 = not available
    cog = _sentinel_to_none(raw.get("COG"), 360.0)   # 360   = not available
    heading = _sentinel_to_none(raw.get("HEADING"), 511)  # 511 = not available

    lat = raw.get("LATITUDE")
    lon = raw.get("LONGITUDE")
    # Discard records with clearly invalid positions
    if lat is None or lon is None or (lat == 0.0 and lon == 0.0):
        lat = lon = None

    return {
        "mmsi": mmsi,
        "imo": imo,
        "name": (raw.get("NAME") or "").strip() or None,
        "call_sign": (raw.get("CALLSIGN") or "").strip() or None,
        "flag": None,  # not provided by AISHub free feed
        "vessel_type": _vessel_type(raw.get("TYPE")),
        "latitude": float(lat) if lat is not None else None,
        "longitude": float(lon) if lon is not None else None,
        "speed_over_ground": sog,
        "course_over_ground": cog,
        "heading": heading,
        "nav_status": _nav_status(raw.get("NAVSTAT")),
        "position_timestamp": _parse_timestamp(raw.get("TIME")),
    }

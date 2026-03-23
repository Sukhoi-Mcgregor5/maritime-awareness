"""
AISHub HTTP client.

AISHub distributes aggregated AIS data in exchange for sharing your own
receiver feed. Register at https://www.aishub.net/join-us to get a username.

API docs: http://www.aishub.net/api
"""

import logging
from dataclasses import dataclass

import httpx

from config import settings

logger = logging.getLogger(__name__)

# AISHub returns a nested list: index 0 is metadata, index 1 is vessel records.
_META_INDEX = 0
_DATA_INDEX = 1


@dataclass
class BoundingBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


class AISHubError(Exception):
    pass


class AISHubClient:
    """Async client for the AISHub REST API."""

    def __init__(self, username: str | None = None, timeout: float = 30.0):
        self._username = username or settings.aishub_username
        self._base_url = settings.aishub_url
        self._timeout = timeout

    async def fetch_vessels(self, bbox: BoundingBox | None = None) -> list[dict]:
        """
        Fetch the latest vessel positions from AISHub.

        Returns a list of raw vessel dicts as provided by the API.
        Pass a BoundingBox to restrict the geographic area; omit it for
        a global snapshot (large payload, use sparingly).
        """
        if not self._username:
            raise AISHubError(
                "AISHUB_USERNAME is not set. "
                "Register at https://www.aishub.net/join-us and add it to .env"
            )

        params: dict[str, str | int | float] = {
            "username": self._username,
            "format": 1,       # JSON
            "output": "extended",
            "compress": 0,
        }
        if bbox:
            params.update({
                "latmin": bbox.lat_min,
                "latmax": bbox.lat_max,
                "lonmin": bbox.lon_min,
                "lonmax": bbox.lon_max,
            })

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(self._base_url, params=params)

        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, list) or len(payload) < 2:
            raise AISHubError(f"Unexpected AISHub response structure: {payload}")

        meta = payload[_META_INDEX]
        if isinstance(meta, list) and meta:
            meta = meta[0]
        if meta.get("ERROR"):
            raise AISHubError(f"AISHub API error: {meta}")

        vessels = payload[_DATA_INDEX]
        logger.info("AISHub returned %d vessel records", len(vessels))
        return vessels

"""
AISStream.io WebSocket client.

Connects to wss://stream.aisstream.io/v0/stream and yields real-time AIS
messages as dicts.  Handles reconnection with exponential backoff so callers
(the poller) can treat it as a continuous, never-ending async generator.

Register for a free API key at https://aisstream.io
"""

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

import websockets
import websockets.exceptions

from config import settings

logger = logging.getLogger(__name__)

# BoundingBox: [[lat_min, lon_min], [lat_max, lon_max]]
# Pass a list of these to restrict the feed to specific sea areas.
WORLD = [[[-90, -180], [90, 180]]]

_RECONNECT_BASE = 1    # seconds before first reconnect attempt
_RECONNECT_MAX  = 60   # cap on backoff


class AISStreamError(Exception):
    pass


class AISStreamClient:
    """
    Async WebSocket client for AISStream.io.

    Usage::

        client = AISStreamClient()
        async for msg in client.stream():
            print(msg["MessageType"], msg["MetaData"]["MMSI"])
    """

    def __init__(
        self,
        api_key: str | None = None,
        bounding_boxes: list[list[list[float]]] | None = None,
        message_types: list[str] | None = None,
    ):
        self._api_key       = api_key or settings.aisstream_api_key
        self._url           = settings.aisstream_url
        self._bounding_boxes = bounding_boxes or WORLD
        self._message_types  = message_types or ["PositionReport", "ShipStaticData"]

    def _subscribe(self) -> str:
        return json.dumps({
            "APIKey":           self._api_key,
            "BoundingBoxes":    self._bounding_boxes,
            "FilterMessageTypes": self._message_types,
        })

    async def stream(self) -> AsyncGenerator[dict, None]:
        """
        Yield AIS message dicts indefinitely, reconnecting on any disconnect.

        Each yielded dict has the shape::

            {
              "MessageType": "PositionReport" | "ShipStaticData" | ...,
              "Message":     { ... },       # payload keyed by MessageType
              "MetaData":    { "MMSI": int, "ShipName": str,
                               "latitude": float, "longitude": float,
                               "time_utc": str, ... }
            }
        """
        if not self._api_key:
            raise AISStreamError(
                "AISSTREAM_API_KEY is not set. "
                "Register at https://aisstream.io and add it to .env"
            )

        backoff = _RECONNECT_BASE

        while True:
            try:
                async with websockets.connect(self._url) as ws:
                    await ws.send(self._subscribe())
                    logger.info("AISStream connected — subscribed to %s", self._message_types)
                    backoff = _RECONNECT_BASE  # reset on successful connection

                    async for raw in ws:
                        try:
                            yield json.loads(raw)
                        except json.JSONDecodeError:
                            logger.warning("Unparseable AISStream frame: %.120s", raw)

            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning("AISStream connection closed: %s — reconnecting in %ds", exc, backoff)
            except OSError as exc:
                logger.warning("AISStream network error: %s — reconnecting in %ds", exc, backoff)
            except asyncio.CancelledError:
                logger.info("AISStream client cancelled")
                return

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_MAX)

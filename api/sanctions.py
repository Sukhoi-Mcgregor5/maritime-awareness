from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from ingestion.sanctions import SANCTIONED_COUNTRIES, match_vessel_against_sanctions
from ontology.models import Vessel

router = APIRouter(prefix="/sanctions", tags=["sanctions"])

Db = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class SanctionMatch(BaseModel):
    id:           int
    name:         str
    entity_type:  str
    source:       str
    source_id:    str
    country:      str | None
    programs:     str | None
    identifiers:  dict | None
    remarks:      str | None
    match_reason: str


class SanctionCheckResponse(BaseModel):
    mmsi:            str
    vessel_name:     str | None
    flag:            str | None
    imo:             str | None
    risk_level:      str            # "high", "medium", "low"
    matches:         list[SanctionMatch]
    sanctioned_flag: str | None     # country name if flag is on watchlist
    checked_at:      datetime


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/check/{mmsi}", response_model=SanctionCheckResponse)
async def check_vessel_sanctions(mmsi: str, db: Db):
    """
    Check a vessel (by MMSI) against OFAC sanctions lists.

    Performs exact MMSI and IMO matches, a fuzzy name match against
    vessel-type entries, and a flag-state risk check.
    """
    vessel = (
        await db.execute(select(Vessel).where(Vessel.mmsi == mmsi))
    ).scalar_one_or_none()
    if vessel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Vessel {mmsi} not found")

    result = await match_vessel_against_sanctions(
        session=db,
        mmsi=mmsi,
        imo=vessel.imo,
        name=vessel.name,
        flag=vessel.flag,
    )

    return SanctionCheckResponse(
        mmsi=mmsi,
        vessel_name=vessel.name,
        flag=vessel.flag,
        imo=vessel.imo,
        risk_level=result["risk_level"],
        matches=[SanctionMatch(**m) for m in result["matches"]],
        sanctioned_flag=result["sanctioned_flag"],
        checked_at=datetime.now(timezone.utc),
    )


@router.get("/countries", response_model=dict[str, str])
async def list_sanctioned_countries():
    """Return the watchlist of flag states with active sanctions."""
    return SANCTIONED_COUNTRIES

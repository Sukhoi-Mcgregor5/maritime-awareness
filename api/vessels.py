from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from ontology.models import NavigationStatus, Vessel, VesselTrack, VesselType

router = APIRouter(prefix="/vessels", tags=["vessels"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class VesselCreate(BaseModel):
    mmsi: str = Field(..., min_length=9, max_length=9, pattern=r"^\d{9}$")
    imo: str | None = Field(None, max_length=10)
    name: str | None = Field(None, max_length=255)
    call_sign: str | None = Field(None, max_length=10)
    flag: str | None = Field(None, min_length=3, max_length=3)
    vessel_type: VesselType = VesselType.unknown
    length: float | None = Field(None, gt=0)
    beam: float | None = Field(None, gt=0)
    draught: float | None = Field(None, gt=0)
    gross_tonnage: float | None = Field(None, gt=0)


class PositionUpdate(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    speed_over_ground: float | None = Field(None, ge=0)
    course_over_ground: float | None = Field(None, ge=0, lt=360)
    heading: float | None = Field(None, ge=0, lt=360)
    nav_status: NavigationStatus = NavigationStatus.unknown
    position_timestamp: datetime | None = None

    @field_validator("position_timestamp", mode="before")
    @classmethod
    def default_timestamp(cls, v):
        return v or datetime.utcnow()


class VesselResponse(BaseModel):
    id: int
    mmsi: str
    imo: str | None
    name: str | None
    call_sign: str | None
    flag: str | None
    vessel_type: VesselType
    length: float | None
    beam: float | None
    draught: float | None
    gross_tonnage: float | None
    latitude: float | None
    longitude: float | None
    speed_over_ground: float | None
    course_over_ground: float | None
    heading: float | None
    nav_status: NavigationStatus
    position_timestamp: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class VesselPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[VesselResponse]


class TrackPointResponse(BaseModel):
    id: int
    mmsi: str
    latitude: float
    longitude: float
    speed_over_ground: float | None
    course_over_ground: float | None
    heading: float | None
    nav_status: NavigationStatus
    recorded_at: datetime
    ingested_at: datetime

    model_config = {"from_attributes": True}


class TrackResponse(BaseModel):
    mmsi: str
    since: datetime
    until: datetime
    count: int
    points: list[TrackPointResponse]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

Db = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=VesselPage)
async def list_vessels(
    db: Db,
    limit: Annotated[int, Query(ge=1, le=2000)] = 2000,
    offset: Annotated[int, Query(ge=0)] = 0,
    vessel_type: VesselType | None = None,
    flag: str | None = None,
):
    query = select(Vessel)
    if vessel_type:
        query = query.where(Vessel.vessel_type == vessel_type)
    if flag:
        query = query.where(Vessel.flag == flag.upper())

    total = len((await db.execute(query)).all())
    rows = (await db.execute(query.order_by(Vessel.id).offset(offset).limit(limit))).scalars().all()

    return VesselPage(total=total, limit=limit, offset=offset, items=rows)


@router.get("/bounds", response_model=VesselPage)
async def vessels_in_bounds(
    db: Db,
    south: Annotated[float, Query(ge=-90,  le=90,  description="Southern latitude bound")],
    west:  Annotated[float, Query(ge=-180, le=180, description="Western longitude bound")],
    north: Annotated[float, Query(ge=-90,  le=90,  description="Northern latitude bound")],
    east:  Annotated[float, Query(ge=-180, le=180, description="Eastern longitude bound")],
    limit: Annotated[int,   Query(ge=1,    le=2000)] = 2000,
):
    """Return vessels whose last known position falls within the given bounding box."""
    query = (
        select(Vessel)
        .where(Vessel.latitude.isnot(None))
        .where(Vessel.longitude.isnot(None))
        .where(Vessel.latitude  >= south)
        .where(Vessel.latitude  <= north)
        .where(Vessel.longitude >= west)
        .where(Vessel.longitude <= east)
    )
    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar_one()
    rows  = (await db.execute(query.order_by(Vessel.id).limit(limit))).scalars().all()
    return VesselPage(total=total, limit=limit, offset=0, items=rows)


@router.get("/{mmsi}", response_model=VesselResponse)
async def get_vessel(mmsi: str, db: Db):
    row = (await db.execute(select(Vessel).where(Vessel.mmsi == mmsi))).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Vessel {mmsi} not found")
    return row


@router.post("", response_model=VesselResponse, status_code=status.HTTP_201_CREATED)
async def create_vessel(payload: VesselCreate, db: Db):
    existing = (await db.execute(select(Vessel).where(Vessel.mmsi == payload.mmsi))).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Vessel {payload.mmsi} already exists")

    vessel = Vessel(**payload.model_dump())
    db.add(vessel)
    await db.flush()
    await db.refresh(vessel)
    return vessel


@router.patch("/{mmsi}/position", response_model=VesselResponse)
async def update_vessel_position(mmsi: str, payload: PositionUpdate, db: Db):
    vessel = (await db.execute(select(Vessel).where(Vessel.mmsi == mmsi))).scalar_one_or_none()
    if vessel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Vessel {mmsi} not found")

    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(vessel, field, value)

    await db.flush()
    await db.refresh(vessel)
    return vessel


@router.get("/{mmsi}/track", response_model=TrackResponse)
async def get_vessel_track(
    mmsi: str,
    db: Db,
    since: Annotated[datetime | None, Query(description="Start of time window (inclusive)")] = None,
    until: Annotated[datetime | None, Query(description="End of time window (inclusive)")] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
):
    vessel_exists = (
        await db.execute(select(Vessel.mmsi).where(Vessel.mmsi == mmsi))
    ).scalar_one_or_none()
    if vessel_exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Vessel {mmsi} not found")

    now = datetime.utcnow()
    since = since or datetime.min.replace(year=1970)
    until = until or now

    if since >= until:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="'since' must be before 'until'")

    query = (
        select(VesselTrack)
        .where(VesselTrack.mmsi == mmsi)
        .where(VesselTrack.recorded_at >= since)
        .where(VesselTrack.recorded_at <= until)
        .order_by(VesselTrack.recorded_at)
        .limit(limit)
    )
    points = (await db.execute(query)).scalars().all()

    count_result = await db.execute(
        select(func.count())
        .select_from(VesselTrack)
        .where(VesselTrack.mmsi == mmsi)
        .where(VesselTrack.recorded_at >= since)
        .where(VesselTrack.recorded_at <= until)
    )
    total = count_result.scalar_one()

    return TrackResponse(mmsi=mmsi, since=since, until=until, count=total, points=points)


@router.get("/{mmsi}/track/latest", response_model=TrackPointResponse)
async def get_latest_track_point(mmsi: str, db: Db):
    vessel_exists = (
        await db.execute(select(Vessel.mmsi).where(Vessel.mmsi == mmsi))
    ).scalar_one_or_none()
    if vessel_exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Vessel {mmsi} not found")

    point = (
        await db.execute(
            select(VesselTrack)
            .where(VesselTrack.mmsi == mmsi)
            .order_by(VesselTrack.recorded_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    if point is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No track data for vessel {mmsi}")

    return point

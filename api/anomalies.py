from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from ontology.models import Anomaly, AnomalyStatus, AnomalyType

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class AnomalyResponse(BaseModel):
    id:           int
    mmsi:         str
    anomaly_type: AnomalyType
    status:       AnomalyStatus
    detected_at:  datetime
    resolved_at:  datetime | None
    details:      dict | None

    model_config = {"from_attributes": True}


class AnomalyPage(BaseModel):
    total:  int
    limit:  int
    offset: int
    items:  list[AnomalyResponse]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

Db = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=AnomalyPage)
async def list_anomalies(
    db:           Db,
    limit:        Annotated[int, Query(ge=1, le=200)] = 50,
    offset:       Annotated[int, Query(ge=0)] = 0,
    status:       AnomalyStatus | None = None,
    anomaly_type: AnomalyType | None = None,
    mmsi:         str | None = None,
):
    query = select(Anomaly)
    if status:
        query = query.where(Anomaly.status == status)
    if anomaly_type:
        query = query.where(Anomaly.anomaly_type == anomaly_type)
    if mmsi:
        query = query.where(Anomaly.mmsi == mmsi)

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar_one()
    items = (
        await db.execute(query.order_by(Anomaly.detected_at.desc()).offset(offset).limit(limit))
    ).scalars().all()

    return AnomalyPage(total=total, limit=limit, offset=offset, items=items)


@router.get("/active", response_model=list[AnomalyResponse])
async def list_active_anomalies(
    db:           Db,
    anomaly_type: AnomalyType | None = None,
):
    """Return all currently active anomalies, optionally filtered by type."""
    query = select(Anomaly).where(Anomaly.status == AnomalyStatus.active)
    if anomaly_type:
        query = query.where(Anomaly.anomaly_type == anomaly_type)
    items = (await db.execute(query.order_by(Anomaly.detected_at.desc()))).scalars().all()
    return items


@router.get("/{anomaly_id}", response_model=AnomalyResponse)
async def get_anomaly(anomaly_id: int, db: Db):
    row = (await db.execute(select(Anomaly).where(Anomaly.id == anomaly_id))).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Anomaly {anomaly_id} not found")
    return row

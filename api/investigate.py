import json
from datetime import datetime, timedelta
from typing import Annotated

from mistralai.client import Mistral
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from ingestion.sanctions import match_vessel_against_sanctions
from ontology.models import (
    Anomaly,
    AnomalyStatus,
    AnomalyType,
    NavigationStatus,
    Vessel,
    VesselTrack,
    VesselType,
)

router = APIRouter(prefix="/investigate", tags=["investigate"])

Db = Annotated[AsyncSession, Depends(get_db)]

_SYSTEM_PROMPT = """\
You are a maritime intelligence analyst with access to a vessel tracking database.
Answer the operator's natural language query by calling the available tools to gather data,
then produce a structured intelligence brief.

After gathering all necessary data, respond with a JSON object containing exactly these keys:
- "summary": 1-2 sentence executive summary
- "findings": list of strings, each a key finding with supporting evidence
- "vessels_of_interest": list of MMSI strings relevant to the query
- "recommendations": list of suggested follow-up actions
- "confidence": "high", "medium", or "low" based on data completeness

Respond ONLY with the JSON object — no markdown fences, no extra text."""

# Mistral uses OpenAI-compatible function-calling format
_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_vessels",
            "description": (
                "Search vessels in the database by type, flag, name, MMSI, navigation status, "
                "or speed range. Returns current position and metadata for matching vessels."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "vessel_type": {
                        "type": "string",
                        "enum": ["cargo", "tanker", "passenger", "fishing", "tug", "military", "pleasure", "other", "unknown"],
                        "description": "Filter by vessel type",
                    },
                    "flag": {
                        "type": "string",
                        "description": "ISO 3166-1 alpha-3 flag state (e.g. 'GBR', 'USA', 'PRK')",
                    },
                    "name": {
                        "type": "string",
                        "description": "Partial vessel name match (case-insensitive)",
                    },
                    "mmsi": {
                        "type": "string",
                        "description": "Exact 9-digit MMSI",
                    },
                    "nav_status": {
                        "type": "string",
                        "enum": ["under_way_engine", "at_anchor", "not_under_command", "restricted_maneuverability", "moored", "aground", "unknown"],
                        "description": "Filter by navigation status",
                    },
                    "min_sog": {"type": "number", "description": "Minimum speed over ground (knots)"},
                    "max_sog": {"type": "number", "description": "Maximum speed over ground (knots)"},
                    "limit": {"type": "integer", "description": "Max results (default 20, max 100)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_anomalies",
            "description": (
                "Retrieve detected behavioural anomalies. "
                "'dark_vessel' = AIS signal went silent unexpectedly. "
                "'loitering' = vessel circling or drifting slowly in an area. "
                "Default status is 'active'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "anomaly_type": {
                        "type": "string",
                        "enum": ["dark_vessel", "loitering"],
                        "description": "Filter by anomaly type",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["active", "resolved"],
                        "description": "Anomaly status (default: active)",
                    },
                    "mmsi": {"type": "string", "description": "Filter to a specific vessel MMSI"},
                    "since_hours": {
                        "type": "number",
                        "description": "Only anomalies detected in the last N hours (default: 24)",
                    },
                    "limit": {"type": "integer", "description": "Max results (default 50, max 200)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_vessel_track",
            "description": (
                "Get historical position track for a specific vessel. "
                "Useful for understanding movement patterns, routes, and dwell times."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mmsi": {"type": "string", "description": "9-digit MMSI of the vessel"},
                    "hours": {
                        "type": "number",
                        "description": "Hours of history to retrieve (default: 24)",
                    },
                    "limit": {"type": "integer", "description": "Max track points (default 200, max 1000)"},
                },
                "required": ["mmsi"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_sanctions",
            "description": (
                "Check a vessel or entity against OFAC sanctions lists. "
                "Returns exact MMSI/IMO matches, fuzzy name matches, and a flag-state risk assessment. "
                "Use this to identify potential sanctions evasion or vessels linked to sanctioned entities."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mmsi": {"type": "string", "description": "9-digit MMSI to check"},
                    "imo":  {"type": "string", "description": "7-digit IMO number to check"},
                    "name": {"type": "string", "description": "Vessel or entity name to fuzzy-match"},
                    "flag": {"type": "string", "description": "ISO 3166-1 alpha-3 flag state (e.g. 'IRN', 'PRK')"},
                },
                "required": [],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executors
# ---------------------------------------------------------------------------

async def _search_vessels(params: dict, db: AsyncSession) -> str:
    query = select(Vessel)

    if v := params.get("vessel_type"):
        try:
            query = query.where(Vessel.vessel_type == VesselType(v))
        except ValueError:
            pass

    if f := params.get("flag"):
        query = query.where(Vessel.flag == f.upper())

    if n := params.get("name"):
        query = query.where(Vessel.name.ilike(f"%{n}%"))

    if m := params.get("mmsi"):
        query = query.where(Vessel.mmsi == m)

    if ns := params.get("nav_status"):
        try:
            query = query.where(Vessel.nav_status == NavigationStatus(ns))
        except ValueError:
            pass

    if params.get("min_sog") is not None:
        query = query.where(Vessel.speed_over_ground >= params["min_sog"])

    if params.get("max_sog") is not None:
        query = query.where(Vessel.speed_over_ground <= params["max_sog"])

    limit = min(int(params.get("limit", 20)), 100)
    rows = (await db.execute(query.limit(limit))).scalars().all()

    vessels = [
        {
            "mmsi": v.mmsi,
            "name": v.name,
            "flag": v.flag,
            "vessel_type": v.vessel_type.value if v.vessel_type else None,
            "nav_status": v.nav_status.value if v.nav_status else None,
            "latitude": v.latitude,
            "longitude": v.longitude,
            "speed_over_ground": v.speed_over_ground,
            "course_over_ground": v.course_over_ground,
            "position_timestamp": v.position_timestamp.isoformat() if v.position_timestamp else None,
        }
        for v in rows
    ]
    return json.dumps({"count": len(vessels), "vessels": vessels})


async def _get_anomalies(params: dict, db: AsyncSession) -> str:
    query = select(Anomaly)

    if at := params.get("anomaly_type"):
        try:
            query = query.where(Anomaly.anomaly_type == AnomalyType(at))
        except ValueError:
            pass

    status_val = params.get("status", "active")
    try:
        query = query.where(Anomaly.status == AnomalyStatus(status_val))
    except ValueError:
        pass

    if m := params.get("mmsi"):
        query = query.where(Anomaly.mmsi == m)

    since_hours = float(params.get("since_hours", 24))
    since = datetime.utcnow() - timedelta(hours=since_hours)
    query = query.where(Anomaly.detected_at >= since)

    limit = min(int(params.get("limit", 50)), 200)
    rows = (await db.execute(query.order_by(Anomaly.detected_at.desc()).limit(limit))).scalars().all()

    anomalies = [
        {
            "id": a.id,
            "mmsi": a.mmsi,
            "anomaly_type": a.anomaly_type.value,
            "status": a.status.value,
            "detected_at": a.detected_at.isoformat() if a.detected_at else None,
            "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
            "details": a.details,
        }
        for a in rows
    ]
    return json.dumps({"count": len(anomalies), "anomalies": anomalies})


async def _get_vessel_track(params: dict, db: AsyncSession) -> str:
    mmsi = params["mmsi"]
    hours = float(params.get("hours", 24))
    limit = min(int(params.get("limit", 200)), 1000)

    since = datetime.utcnow() - timedelta(hours=hours)
    rows = (
        await db.execute(
            select(VesselTrack)
            .where(VesselTrack.mmsi == mmsi)
            .where(VesselTrack.recorded_at >= since)
            .order_by(VesselTrack.recorded_at.desc())
            .limit(limit)
        )
    ).scalars().all()

    points = [
        {
            "latitude": p.latitude,
            "longitude": p.longitude,
            "speed_over_ground": p.speed_over_ground,
            "course_over_ground": p.course_over_ground,
            "nav_status": p.nav_status.value if p.nav_status else None,
            "recorded_at": p.recorded_at.isoformat() if p.recorded_at else None,
        }
        for p in rows
    ]
    return json.dumps({"mmsi": mmsi, "hours": hours, "count": len(points), "track": points})


async def _check_sanctions(params: dict, db: AsyncSession) -> str:
    result = await match_vessel_against_sanctions(
        session=db,
        mmsi=params.get("mmsi"),
        imo=params.get("imo"),
        name=params.get("name"),
        flag=params.get("flag"),
    )
    return json.dumps(result)


async def _execute_tool(name: str, tool_input: dict, db: AsyncSession) -> str:
    if name == "search_vessels":
        return await _search_vessels(tool_input, db)
    if name == "get_anomalies":
        return await _get_anomalies(tool_input, db)
    if name == "get_vessel_track":
        return await _get_vessel_track(tool_input, db)
    if name == "check_sanctions":
        return await _check_sanctions(tool_input, db)
    return json.dumps({"error": f"unknown tool: {name}"})


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class InvestigateRequest(BaseModel):
    query: str


class InvestigateResponse(BaseModel):
    query: str
    summary: str
    findings: list[str]
    vessels_of_interest: list[str]
    recommendations: list[str]
    confidence: str
    raw_brief: str


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=InvestigateResponse)
async def investigate(payload: InvestigateRequest, db: Db):
    """
    Accept a natural language maritime intelligence query, run an agentic loop
    against the vessel/anomaly database using Mistral, and return a structured brief.
    """
    client = Mistral(api_key=settings.mistral_api_key)
    messages: list[dict] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": payload.query},
    ]

    response = None
    while True:
        response = await client.chat.complete_async(
            model="mistral-small-latest",
            messages=messages,
            tools=_TOOLS,
            tool_choice="auto",
        )

        choice = response.choices[0]
        if choice.finish_reason != "tool_calls":
            break

        # Append the assistant turn with its tool_calls
        messages.append(choice.message)

        # Execute every requested tool and append results
        for tool_call in choice.message.tool_calls:
            tool_input = json.loads(tool_call.function.arguments)
            result = await _execute_tool(tool_call.function.name, tool_input, db)
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result,
            })

    raw_brief = response.choices[0].message.content or ""

    # Parse the JSON brief the model was instructed to produce
    summary = ""
    findings: list[str] = []
    vessels_of_interest: list[str] = []
    recommendations: list[str] = []
    confidence = "medium"

    try:
        parsed = json.loads(raw_brief)
        summary = parsed.get("summary", "")
        findings = parsed.get("findings", [])
        vessels_of_interest = [str(m) for m in parsed.get("vessels_of_interest", [])]
        recommendations = parsed.get("recommendations", [])
        confidence = parsed.get("confidence", "medium")
    except (json.JSONDecodeError, TypeError):
        summary = raw_brief[:500] if raw_brief else "No brief generated."

    return InvestigateResponse(
        query=payload.query,
        summary=summary,
        findings=findings,
        vessels_of_interest=vessels_of_interest,
        recommendations=recommendations,
        confidence=confidence,
        raw_brief=raw_brief,
    )

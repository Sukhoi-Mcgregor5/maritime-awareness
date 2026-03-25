import enum

from sqlalchemy import DateTime, Enum, Float, Index, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class VesselType(str, enum.Enum):
    cargo = "cargo"
    tanker = "tanker"
    passenger = "passenger"
    fishing = "fishing"
    tug = "tug"
    military = "military"
    pleasure = "pleasure"
    other = "other"
    unknown = "unknown"


class NavigationStatus(str, enum.Enum):
    under_way_engine = "under_way_engine"
    at_anchor = "at_anchor"
    not_under_command = "not_under_command"
    restricted_maneuverability = "restricted_maneuverability"
    moored = "moored"
    aground = "aground"
    unknown = "unknown"


class Vessel(Base):
    __tablename__ = "vessels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mmsi: Mapped[str] = mapped_column(String(9), unique=True, nullable=False, index=True)
    imo: Mapped[str | None] = mapped_column(String(10), nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    call_sign: Mapped[str | None] = mapped_column(String(10), nullable=True)
    flag: Mapped[str | None] = mapped_column(String(3), nullable=True)  # ISO 3166-1 alpha-3

    vessel_type: Mapped[VesselType] = mapped_column(
        Enum(VesselType), nullable=False, default=VesselType.unknown
    )

    # Physical dimensions (metres)
    length: Mapped[float | None] = mapped_column(Float, nullable=True)
    beam: Mapped[float | None] = mapped_column(Float, nullable=True)
    draught: Mapped[float | None] = mapped_column(Float, nullable=True)
    gross_tonnage: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Last known position
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    speed_over_ground: Mapped[float | None] = mapped_column(Float, nullable=True)  # knots
    course_over_ground: Mapped[float | None] = mapped_column(Float, nullable=True)  # degrees
    heading: Mapped[float | None] = mapped_column(Float, nullable=True)  # degrees
    nav_status: Mapped[NavigationStatus] = mapped_column(
        Enum(NavigationStatus), nullable=False, default=NavigationStatus.unknown
    )
    position_timestamp: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class VesselTrack(Base):
    """
    Immutable time-series record of every position update received for a vessel.

    Each row captures the state at a single AIS report timestamp.
    Keyed on (mmsi, recorded_at) so duplicate poll cycles are idempotent.
    mmsi is stored directly (no FK) to keep bulk inserts fast and to avoid
    ordering constraints relative to the vessels upsert.
    """

    __tablename__ = "vessel_tracks"
    __table_args__ = (
        # Deduplicates re-polls of the same AIS message
        UniqueConstraint("mmsi", "recorded_at", name="uq_vessel_tracks_mmsi_recorded_at"),
        # Primary query pattern: track for vessel X in time range Y
        Index("ix_vessel_tracks_mmsi_recorded_at", "mmsi", "recorded_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mmsi: Mapped[str] = mapped_column(String(9), nullable=False)

    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    speed_over_ground: Mapped[float | None] = mapped_column(Float, nullable=True)
    course_over_ground: Mapped[float | None] = mapped_column(Float, nullable=True)
    heading: Mapped[float | None] = mapped_column(Float, nullable=True)
    nav_status: Mapped[NavigationStatus] = mapped_column(
        Enum(NavigationStatus), nullable=False, default=NavigationStatus.unknown
    )

    # Timestamp carried in the AIS message itself
    recorded_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    # When this row was written to the DB
    ingested_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class EntityType(str, enum.Enum):
    vessel   = "vessel"
    company  = "company"
    person   = "person"
    aircraft = "aircraft"
    other    = "other"


class SanctionedEntity(Base):
    """
    An entity (vessel, company, or person) appearing on a sanctions list.

    Populated by ingestion/sanctions.py from OFAC SDN and consolidated lists.
    Indexed so vessel-matching queries (MMSI, IMO, name) are fast.
    """

    __tablename__ = "sanctioned_entities"
    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_sanctioned_source_id"),
        Index("ix_sanctioned_entities_name", "name"),
        Index("ix_sanctioned_entities_type", "entity_type"),
    )

    id:          Mapped[int]           = mapped_column(Integer, primary_key=True)
    source_id:   Mapped[str]           = mapped_column(String(50),  nullable=False)
    name:        Mapped[str]           = mapped_column(String(500), nullable=False)
    entity_type: Mapped[EntityType]    = mapped_column(Enum(EntityType), nullable=False, default=EntityType.other)
    # Structured vessel identifiers: {mmsi, imo, call_sign}
    identifiers: Mapped[dict | None]   = mapped_column(JSON,        nullable=True)
    source:      Mapped[str]           = mapped_column(String(20),  nullable=False)   # OFAC_SDN, OFAC_CONS
    country:     Mapped[str | None]    = mapped_column(String(100), nullable=True)
    programs:    Mapped[str | None]    = mapped_column(String(500), nullable=True)
    remarks:     Mapped[str | None]    = mapped_column(Text,        nullable=True)


class AnomalyType(str, enum.Enum):
    dark_vessel = "dark_vessel"
    loitering   = "loitering"


class AnomalyStatus(str, enum.Enum):
    active   = "active"
    resolved = "resolved"


class Anomaly(Base):
    """
    A detected behavioural anomaly for a vessel.

    Anomalies are opened when a detector first fires and resolved when the
    condition clears on a subsequent detection run.  details stores
    detector-specific context (e.g. silence duration, displacement).
    """

    __tablename__ = "anomalies"
    __table_args__ = (
        Index("ix_anomalies_mmsi_type_status", "mmsi", "anomaly_type", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mmsi: Mapped[str] = mapped_column(String(9), nullable=False, index=True)
    anomaly_type: Mapped[AnomalyType] = mapped_column(Enum(AnomalyType), nullable=False)
    status: Mapped[AnomalyStatus] = mapped_column(
        Enum(AnomalyStatus), nullable=False, default=AnomalyStatus.active
    )
    detected_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    resolved_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Detector-specific context stored as JSON
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)

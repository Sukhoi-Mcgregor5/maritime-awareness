import enum

from sqlalchemy import DateTime, Enum, Float, Integer, String, func
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
    imo: Mapped[str | None] = mapped_column(String(10), unique=True, nullable=True)
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

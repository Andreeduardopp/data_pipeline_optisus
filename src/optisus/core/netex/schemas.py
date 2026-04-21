"""
Internal Pydantic models for NeTEx entities (PT-EPIP subset).

These are the *in-memory* representation the translator produces and the
XML builder consumes. They are deliberately thinner than the NeTEx XSD:
only the fields PT-EPIP phase 1 actually requires are modelled.

All identifiers are already full URNs (``PT:{codespace}:{type}:{id}``)
by the time they reach these models — URN construction is the
translator's job, not the model's.
"""

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class _NetexBase(BaseModel):
    """Common behaviour: allow extras off, strict field names."""
    model_config = ConfigDict(extra="forbid")


# ═══════════════════════════════════════════════════════════════════════════
# Organisation / responsibility
# ═══════════════════════════════════════════════════════════════════════════

class Codespace(_NetexBase):
    """ResourceFrame/Codespace — PT-EPIP mandates exactly one per dataset."""

    id: str = Field(..., description="Codespace id URN")
    xmlns: str = Field(..., description="Short xmlns prefix (e.g. STCP, '980123456')")
    xmlns_url: str = Field(..., description="URL that identifies the codespace")
    description: Optional[str] = None


class Organisation(_NetexBase):
    """Authority or Operator — same underlying XML shape, different tag."""

    id: str
    version: str
    kind: Literal["Authority", "Operator"]
    name: str
    short_name: Optional[str] = None
    contact_email: Optional[str] = None


class ResponsibilityRoleAssignment(_NetexBase):
    """Binds an Organisation to a stakeholder role (ownership / operation)."""

    id: str
    version: str
    role: Literal["ownership", "operation"]
    organisation_ref: str


# ═══════════════════════════════════════════════════════════════════════════
# Site frame — stops
# ═══════════════════════════════════════════════════════════════════════════

class Location(_NetexBase):
    """WGS84 location (gml:pos in XML)."""

    longitude: float
    latitude: float


class Quay(_NetexBase):
    """A boarding position within a StopPlace."""

    id: str
    version: str
    name: Optional[str] = None
    public_code: Optional[str] = None
    centroid: Optional[Location] = None
    wheelchair_access: Optional[str] = None  # 'true'|'false'|'unknown'


class StopPlace(_NetexBase):
    """A station / stop area containing one or more Quays."""

    id: str
    version: str
    name: str
    centroid: Optional[Location] = None
    transport_mode: str
    stop_place_type: Optional[str] = None
    quays: List[Quay] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Service calendar frame
# ═══════════════════════════════════════════════════════════════════════════

class DayType(_NetexBase):
    """Weekly pattern — which days of the week this service runs."""

    id: str
    version: str
    name: Optional[str] = None
    days_of_week: List[str] = Field(
        default_factory=list,
        description="Subset of {Monday..Sunday}; maps to PropertyOfDay/DaysOfWeek",
    )


class OperatingPeriod(_NetexBase):
    """Date range during which a DayType is valid (from GTFS calendar.start_date/end_date)."""

    id: str
    version: str
    from_date: str  # ISO 8601 (YYYY-MM-DD) — translator converts from YYYYMMDD
    to_date: str


class DayTypeAssignment(_NetexBase):
    """Link a DayType either to an OperatingPeriod (regular) or a single Date (exception)."""

    id: str
    version: str
    order: int
    day_type_ref: str
    operating_period_ref: Optional[str] = None
    date: Optional[str] = None  # ISO 8601 for single-date exceptions
    is_available: bool = True


# ═══════════════════════════════════════════════════════════════════════════
# Service frame — lines, routes, scheduled stop points, journey patterns
# ═══════════════════════════════════════════════════════════════════════════

class Line(_NetexBase):
    """A commercial service — maps from GTFS route."""

    id: str
    version: str
    name: str
    public_code: Optional[str] = None
    transport_mode: str
    operator_ref: Optional[str] = None
    presentation_colour: Optional[str] = None       # hex, no '#'
    presentation_text_colour: Optional[str] = None  # hex, no '#'


class ScheduledStopPoint(_NetexBase):
    """A logical stop used within JourneyPatterns — referenced by passing times."""

    id: str
    version: str
    name: Optional[str] = None


class PassengerStopAssignment(_NetexBase):
    """Link a ScheduledStopPoint to its physical Quay (or StopPlace)."""

    id: str
    version: str
    order: int
    scheduled_stop_point_ref: str
    quay_ref: Optional[str] = None
    stop_place_ref: Optional[str] = None


class StopPointInJourneyPattern(_NetexBase):
    """Ordered stop within a JourneyPattern."""

    id: str
    version: str
    order: int
    scheduled_stop_point_ref: str
    for_alighting: bool = True
    for_boarding: bool = True


class JourneyPattern(_NetexBase):
    """Ordered sequence of ScheduledStopPoints for one direction on a Line.

    Serialised as ``<ServiceJourneyPattern>`` (a Line-bound subtype of
    JourneyPattern) so we can reference the Line directly via ``LineRef``
    without needing to emit a separate Route object.
    """

    id: str
    version: str
    name: Optional[str] = None
    line_ref: Optional[str] = None
    direction_type: Optional[Literal["outbound", "inbound"]] = None
    stop_points: List[StopPointInJourneyPattern] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Timetable frame
# ═══════════════════════════════════════════════════════════════════════════

class TimetabledPassingTime(_NetexBase):
    """A single stop's arrival / departure within a ServiceJourney."""

    id: Optional[str] = None
    stop_point_in_journey_pattern_ref: str
    arrival_time: Optional[str] = None    # HH:MM:SS (0-23); overflow goes to day_offset
    departure_time: Optional[str] = None
    arrival_day_offset: int = 0
    departure_day_offset: int = 0


class ServiceJourney(_NetexBase):
    """A specific vehicle run — from GTFS trip."""

    id: str
    version: str
    name: Optional[str] = None
    line_ref: str
    journey_pattern_ref: str
    day_type_refs: List[str] = Field(default_factory=list)
    operator_ref: Optional[str] = None
    passing_times: List[TimetabledPassingTime] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Dataset container (what translator produces, exporter consumes)
# ═══════════════════════════════════════════════════════════════════════════

class NetexDataset(_NetexBase):
    """All NeTEx objects for a single export, bucketed by frame."""

    # ResourceFrame
    codespace: Codespace
    organisations: List[Organisation] = Field(default_factory=list)
    role_assignments: List[ResponsibilityRoleAssignment] = Field(default_factory=list)

    # SiteFrame
    stop_places: List[StopPlace] = Field(default_factory=list)

    # ServiceCalendarFrame
    day_types: List[DayType] = Field(default_factory=list)
    operating_periods: List[OperatingPeriod] = Field(default_factory=list)
    day_type_assignments: List[DayTypeAssignment] = Field(default_factory=list)

    # ServiceFrame
    lines: List[Line] = Field(default_factory=list)
    scheduled_stop_points: List[ScheduledStopPoint] = Field(default_factory=list)
    passenger_stop_assignments: List[PassengerStopAssignment] = Field(default_factory=list)
    journey_patterns: List[JourneyPattern] = Field(default_factory=list)

    # TimetableFrame (grouped by line_ref for per-line file splits)
    service_journeys: List[ServiceJourney] = Field(default_factory=list)

    def service_journeys_by_line(self) -> dict[str, List[ServiceJourney]]:
        """Group journeys by their ``line_ref`` — used to split timetable files per PT-EPIP."""
        buckets: dict[str, List[ServiceJourney]] = {}
        for sj in self.service_journeys:
            buckets.setdefault(sj.line_ref, []).append(sj)
        return buckets

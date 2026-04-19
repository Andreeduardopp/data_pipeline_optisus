"""
GTFS and GTFS-ride Pydantic v2 models.

Defines validation schemas for all standard GTFS tables and the GTFS-ride
extension.  Each model mirrors the exact field names used in the
corresponding ``.txt`` file so that validated records can be exported
directly to CSV without renaming.

Reference: https://gtfs.org/documentation/schedule/reference/
GTFS-ride: https://github.com/ODOT-PTS/GTFS-ride

Color Palette note — these models are purely data-layer; visual theming
lives in ``ui_theme.py``.
"""

from enum import IntEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ═══════════════════════════════════════════════════════════════════════════
# Enums (integer-coded, matching GTFS spec values)
# ═══════════════════════════════════════════════════════════════════════════

class LocationType(IntEnum):
    """stops.txt — location_type values."""
    STOP = 0
    STATION = 1
    ENTRANCE_EXIT = 2
    GENERIC_NODE = 3
    BOARDING_AREA = 4


class RouteType(IntEnum):
    """routes.txt — route_type basic values."""
    TRAM = 0
    METRO = 1
    RAIL = 2
    BUS = 3
    FERRY = 4
    CABLE_TRAM = 5
    AERIAL_LIFT = 6
    FUNICULAR = 7
    TROLLEYBUS = 11
    MONORAIL = 12


class DirectionId(IntEnum):
    """trips.txt — direction_id values."""
    OUTBOUND = 0
    INBOUND = 1


class WheelchairAccessible(IntEnum):
    """trips.txt / stops.txt — wheelchair accessibility."""
    NO_INFO = 0
    ACCESSIBLE = 1
    NOT_ACCESSIBLE = 2


class BikesAllowed(IntEnum):
    """trips.txt — bikes_allowed values."""
    NO_INFO = 0
    ALLOWED = 1
    NOT_ALLOWED = 2


class PickupDropOffType(IntEnum):
    """stop_times.txt — pickup_type / drop_off_type values."""
    REGULAR = 0
    NO_SERVICE = 1
    PHONE_AGENCY = 2
    ASK_DRIVER = 3


class ContinuousPickupDropOff(IntEnum):
    """routes.txt / stop_times.txt — continuous_pickup / continuous_drop_off."""
    CONTINUOUS = 0
    NO_CONTINUOUS = 1
    PHONE_AGENCY = 2
    ASK_DRIVER = 3


class ExceptionType(IntEnum):
    """calendar_dates.txt — exception_type values."""
    SERVICE_ADDED = 1
    SERVICE_REMOVED = 2


class TransferType(IntEnum):
    """transfers.txt — transfer_type values."""
    RECOMMENDED = 0
    TIMED = 1
    MINIMUM_TIME = 2
    NOT_POSSIBLE = 3


class ExactTimes(IntEnum):
    """frequencies.txt — exact_times values."""
    FREQUENCY_BASED = 0
    SCHEDULE_BASED = 1


class Timepoint(IntEnum):
    """stop_times.txt — timepoint values."""
    APPROXIMATE = 0
    EXACT = 1


class RecordUse(IntEnum):
    """board_alight.txt (GTFS-ride) — record_use values."""
    BOARDINGS_AND_ALIGHTINGS = 0
    ALIGHTINGS_ONLY = 1
    BOARDINGS_ONLY = 2


class ScheduleRelationship(IntEnum):
    """board_alight.txt (GTFS-ride) — schedule_relationship values."""
    SCHEDULED = 0
    SKIPPED = 1
    NO_DATA = 2


class LoadCountMethod(IntEnum):
    """board_alight.txt (GTFS-ride) — load_count_method values."""
    ESTIMATED = 0
    APC = 1
    MANUAL = 2


class LoadType(IntEnum):
    """board_alight.txt (GTFS-ride) — load_type values."""
    SEATED_AND_STANDING = 0
    SEATED_ONLY = 1


# ═══════════════════════════════════════════════════════════════════════════
# Reusable validators
# ═══════════════════════════════════════════════════════════════════════════

_GTFS_TIME_PATTERN = r"^\d{1,3}:\d{2}:\d{2}$"
_GTFS_DATE_PATTERN = r"^\d{8}$"
_HEX_COLOR_PATTERN = r"^[0-9A-Fa-f]{6}$"


def _validate_gtfs_time(value: Optional[str], field_name: str) -> Optional[str]:
    """Validate HH:MM:SS time string (allows >24:00:00)."""
    if value is None:
        return value
    import re
    if not re.match(_GTFS_TIME_PATTERN, value):
        raise ValueError(
            f"{field_name} must be in HH:MM:SS format (got '{value}')"
        )
    return value


def _validate_gtfs_date(value: Optional[str], field_name: str) -> Optional[str]:
    """Validate YYYYMMDD date string."""
    if value is None:
        return value
    import re
    if not re.match(_GTFS_DATE_PATTERN, value):
        raise ValueError(
            f"{field_name} must be in YYYYMMDD format (got '{value}')"
        )
    return value


def _validate_hex_color(value: Optional[str], field_name: str) -> Optional[str]:
    """Validate 6-digit hex color string without '#'."""
    if value is None:
        return value
    import re
    if not re.match(_HEX_COLOR_PATTERN, value):
        raise ValueError(
            f"{field_name} must be a 6-digit hex color (got '{value}')"
        )
    return value


# ═══════════════════════════════════════════════════════════════════════════
# Core GTFS Models
# ═══════════════════════════════════════════════════════════════════════════

class GtfsAgency(BaseModel):
    """agency.txt — One or more transit agencies that provide the data."""

    agency_id: Optional[str] = Field(
        None,
        description="Unique ID for the agency — required if feed has multiple agencies",
    )
    agency_name: str = Field(
        ..., description="Full name of the transit agency"
    )
    agency_url: str = Field(
        ..., description="Agency website URL"
    )
    agency_timezone: str = Field(
        ..., description="Timezone (e.g. Europe/Lisbon)"
    )
    agency_lang: Optional[str] = Field(
        None, description="Primary language (ISO 639-1, e.g. pt)"
    )
    agency_phone: Optional[str] = Field(
        None, description="Voice telephone number"
    )
    agency_fare_url: Optional[str] = Field(
        None, description="URL for online fare purchase"
    )
    agency_email: Optional[str] = Field(
        None, description="Customer service email"
    )


class GtfsStop(BaseModel):
    """stops.txt — Individual locations where vehicles pick up or drop off riders."""

    stop_id: str = Field(
        ..., description="Unique identifier for the stop"
    )
    stop_name: Optional[str] = Field(
        None,
        description="Human-readable name (conditionally required for location_type 0,1,2)",
    )
    stop_lat: Optional[float] = Field(
        None,
        ge=-90.0, le=90.0,
        description="Latitude WGS84 (conditionally required for location_type 0,1,2)",
    )
    stop_lon: Optional[float] = Field(
        None,
        ge=-180.0, le=180.0,
        description="Longitude WGS84 (conditionally required for location_type 0,1,2)",
    )
    stop_code: Optional[str] = Field(
        None, description="Short public-facing code (shown on signs)"
    )
    stop_desc: Optional[str] = Field(
        None, description="Description of the stop"
    )
    zone_id: Optional[str] = Field(
        None,
        description="Fare zone — conditionally required if using fare_rules.txt",
    )
    stop_url: Optional[str] = Field(
        None, description="URL of a webpage about this stop"
    )
    location_type: Optional[int] = Field(
        None,
        ge=0, le=4,
        description="0=stop, 1=station, 2=entrance, 3=node, 4=boarding area",
    )
    parent_station: Optional[str] = Field(
        None,
        description="ID of the parent station (conditionally required if inside a station)",
    )
    stop_timezone: Optional[str] = Field(
        None, description="Timezone override for this stop"
    )
    wheelchair_boarding: Optional[int] = Field(
        None,
        ge=0, le=2,
        description="0=no info, 1=accessible, 2=not accessible",
    )
    level_id: Optional[str] = Field(
        None, description="Level within a station (references levels.txt)"
    )
    platform_code: Optional[str] = Field(
        None, description="Platform identifier (e.g. 3B)"
    )


class GtfsRoute(BaseModel):
    """routes.txt — Transit routes. A route is a group of trips displayed to riders as a single service."""

    route_id: str = Field(
        ..., description="Unique route identifier"
    )
    agency_id: Optional[str] = Field(
        None,
        description="Agency ID — conditionally required when feed has multiple agencies",
    )
    route_short_name: Optional[str] = Field(
        None,
        description="Short public name (e.g. L1, 32) — conditionally required",
    )
    route_long_name: Optional[str] = Field(
        None,
        description="Full name (e.g. Centro — Aeroporto) — conditionally required",
    )
    route_desc: Optional[str] = Field(
        None, description="Description of the route"
    )
    route_type: int = Field(
        ...,
        description="Mode: 0=tram, 1=metro, 2=rail, 3=bus, 4=ferry, 11=trolleybus",
    )
    route_url: Optional[str] = Field(
        None, description="URL of a webpage about the route"
    )
    route_color: Optional[str] = Field(
        None, description="Hex color for the route (e.g. FF0000)"
    )
    route_text_color: Optional[str] = Field(
        None, description="Hex color for text drawn over route_color"
    )
    route_sort_order: Optional[int] = Field(
        None, ge=0, description="Integer for ordering routes in UI"
    )
    continuous_pickup: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="Whether passengers can board between stops",
    )
    continuous_drop_off: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="Whether passengers can alight between stops",
    )

    @field_validator("route_color", "route_text_color", mode="before")
    @classmethod
    def _validate_color(cls, v: Optional[str]) -> Optional[str]:
        return _validate_hex_color(v, "route color")


class GtfsTrip(BaseModel):
    """trips.txt — Trips for each route. A trip is a sequence of two or more stops."""

    route_id: str = Field(
        ..., description="References routes.route_id"
    )
    service_id: str = Field(
        ..., description="References calendar.service_id"
    )
    trip_id: str = Field(
        ..., description="Unique trip identifier"
    )
    trip_headsign: Optional[str] = Field(
        None, description="Text displayed on vehicle (e.g. Guimarães)"
    )
    trip_short_name: Optional[str] = Field(
        None, description="Public short name for the trip"
    )
    direction_id: Optional[int] = Field(
        None, ge=0, le=1, description="0=outbound, 1=inbound"
    )
    block_id: Optional[str] = Field(
        None, description="Groups trips that share the same vehicle"
    )
    shape_id: Optional[str] = Field(
        None,
        description="References shapes.txt — conditionally required if shapes exist",
    )
    wheelchair_accessible: Optional[int] = Field(
        None,
        ge=0, le=2,
        description="0=no info, 1=accessible, 2=not accessible",
    )
    bikes_allowed: Optional[int] = Field(
        None,
        ge=0, le=2,
        description="0=no info, 1=allowed, 2=not allowed",
    )


class GtfsStopTime(BaseModel):
    """stop_times.txt — Times that a vehicle arrives at and departs from stops for each trip."""

    trip_id: str = Field(
        ..., description="References trips.trip_id"
    )
    arrival_time: Optional[str] = Field(
        None,
        description="HH:MM:SS — can exceed 24:00:00 for after-midnight trips (conditionally required)",
    )
    departure_time: Optional[str] = Field(
        None,
        description="HH:MM:SS — can exceed 24:00:00 (conditionally required)",
    )
    stop_id: str = Field(
        ..., description="References stops.stop_id"
    )
    stop_sequence: int = Field(
        ..., ge=0, description="Non-negative integer — order within the trip"
    )
    stop_headsign: Optional[str] = Field(
        None, description="Overrides trip_headsign at this specific stop"
    )
    pickup_type: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="0=regular, 1=no pickup, 2=phone agency, 3=ask driver",
    )
    drop_off_type: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="0=regular, 1=no drop-off, 2=phone agency, 3=ask driver",
    )
    continuous_pickup: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="Continuous boarding between this stop and next",
    )
    continuous_drop_off: Optional[int] = Field(
        None,
        ge=0, le=3,
        description="Continuous alighting between this stop and next",
    )
    shape_dist_traveled: Optional[float] = Field(
        None,
        ge=0.0,
        description="Distance along shape from first stop (same units as shapes.txt)",
    )
    timepoint: Optional[int] = Field(
        None,
        ge=0, le=1,
        description="0=approximate time, 1=exact time",
    )

    @field_validator("arrival_time", "departure_time", mode="before")
    @classmethod
    def _validate_time(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_time(v, "time")


class GtfsCalendar(BaseModel):
    """calendar.txt — Service dates specified using a weekly schedule with start and end dates."""

    service_id: str = Field(
        ..., description="Unique service pattern identifier"
    )
    monday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    tuesday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    wednesday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    thursday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    friday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    saturday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    sunday: int = Field(
        ..., ge=0, le=1, description="1=service runs, 0=does not run"
    )
    start_date: str = Field(
        ..., description="Start of service period (YYYYMMDD)"
    )
    end_date: str = Field(
        ..., description="End of service period (YYYYMMDD)"
    )

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        result = _validate_gtfs_date(v, "date")
        # Required field — should never be None here
        assert result is not None
        return result


# ═══════════════════════════════════════════════════════════════════════════
# Additional GTFS Models
# ═══════════════════════════════════════════════════════════════════════════

class GtfsCalendarDate(BaseModel):
    """calendar_dates.txt — Exceptions for services defined in calendar.txt."""

    service_id: str = Field(
        ..., description="References calendar.service_id"
    )
    date: str = Field(
        ..., description="Specific date (YYYYMMDD)"
    )
    exception_type: int = Field(
        ..., ge=1, le=2, description="1=service added, 2=service removed"
    )

    @field_validator("date", mode="before")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        result = _validate_gtfs_date(v, "date")
        assert result is not None
        return result


class GtfsShape(BaseModel):
    """shapes.txt — Rules for mapping vehicle travel paths (route alignments)."""

    shape_id: str = Field(
        ..., description="Shape identifier"
    )
    shape_pt_lat: float = Field(
        ..., ge=-90.0, le=90.0, description="Latitude of this point"
    )
    shape_pt_lon: float = Field(
        ..., ge=-180.0, le=180.0, description="Longitude of this point"
    )
    shape_pt_sequence: int = Field(
        ..., ge=0, description="Order of the point within the shape"
    )
    shape_dist_traveled: Optional[float] = Field(
        None,
        ge=0.0,
        description="Distance from first point (for interpolation)",
    )


class GtfsFrequency(BaseModel):
    """frequencies.txt — Headway (time between trips) for headway-based service."""

    trip_id: str = Field(
        ..., description="References trips.trip_id"
    )
    start_time: str = Field(
        ..., description="Time the frequency window begins (HH:MM:SS)"
    )
    end_time: str = Field(
        ..., description="Time the frequency window ends (HH:MM:SS)"
    )
    headway_secs: int = Field(
        ..., gt=0, description="Seconds between departures"
    )
    exact_times: Optional[int] = Field(
        None,
        ge=0, le=1,
        description="0=frequency-based (inexact headways), 1=schedule-based (exact)",
    )

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def _validate_time(cls, v: str) -> str:
        result = _validate_gtfs_time(v, "time")
        assert result is not None
        return result


class GtfsTransfer(BaseModel):
    """transfers.txt — Rules for making connections at transfer points between routes."""

    from_stop_id: str = Field(
        ..., description="Origin stop ID"
    )
    to_stop_id: str = Field(
        ..., description="Destination stop ID"
    )
    transfer_type: int = Field(
        ...,
        ge=0, le=3,
        description="0=recommended, 1=timed, 2=minimum time, 3=not possible",
    )
    min_transfer_time: Optional[int] = Field(
        None,
        ge=0,
        description="Seconds needed — conditionally required when transfer_type=2",
    )


class GtfsFeedInfo(BaseModel):
    """feed_info.txt — Dataset metadata including publisher, URL, and language."""

    feed_publisher_name: str = Field(
        ..., description="Name of the organization publishing the feed"
    )
    feed_publisher_url: str = Field(
        ..., description="Publisher URL"
    )
    feed_lang: str = Field(
        ..., description="Default language of feed text (IETF BCP 47)"
    )
    default_lang: Optional[str] = Field(
        None, description="Language for multilingual feed fallback"
    )
    feed_start_date: Optional[str] = Field(
        None, description="Earliest date the feed covers (YYYYMMDD)"
    )
    feed_end_date: Optional[str] = Field(
        None, description="Latest date the feed covers (YYYYMMDD)"
    )
    feed_version: Optional[str] = Field(
        None, description="Version string for the feed"
    )
    feed_contact_email: Optional[str] = Field(
        None, description="Contact email for feed issues"
    )
    feed_contact_url: Optional[str] = Field(
        None, description="Contact URL"
    )

    @field_validator("feed_start_date", "feed_end_date", mode="before")
    @classmethod
    def _validate_date(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_date(v, "feed date")


# ═══════════════════════════════════════════════════════════════════════════
# GTFS-ride Extension Models
# ═══════════════════════════════════════════════════════════════════════════

class GtfsBoardAlight(BaseModel):
    """board_alight.txt (GTFS-ride) — Stop-level ridership counts per trip."""

    trip_id: str = Field(
        ..., description="References trips.trip_id"
    )
    stop_id: str = Field(
        ..., description="References stops.stop_id"
    )
    stop_sequence: int = Field(
        ..., ge=0, description="References stop_times.stop_sequence"
    )
    record_use: int = Field(
        ...,
        ge=0, le=2,
        description="0=boardings+alightings, 1=alightings only, 2=boardings only",
    )
    schedule_relationship: Optional[int] = Field(
        None,
        ge=0, le=2,
        description="0=scheduled, 1=skipped, 2=no data",
    )
    boardings: Optional[int] = Field(
        None,
        ge=0,
        description="Number of passengers boarding (conditionally required)",
    )
    alightings: Optional[int] = Field(
        None,
        ge=0,
        description="Number of passengers alighting (conditionally required)",
    )
    current_load: Optional[int] = Field(
        None,
        ge=0,
        description="Passenger load when vehicle departs the stop",
    )
    load_count_method: Optional[int] = Field(
        None,
        ge=0, le=2,
        description="0=estimated, 1=APC, 2=manual",
    )
    load_type: Optional[int] = Field(
        None,
        ge=0, le=1,
        description="0=seated+standing, 1=seated only",
    )
    rack_down: Optional[int] = Field(
        None,
        ge=0, le=1,
        description="1=bike rack was deployed",
    )
    bike_boardings: Optional[int] = Field(
        None, ge=0, description="Bikes that boarded"
    )
    bike_alightings: Optional[int] = Field(
        None, ge=0, description="Bikes that alighted"
    )
    ramp_used: Optional[int] = Field(
        None,
        ge=0, le=1,
        description="1=wheelchair ramp was deployed",
    )
    ramp_boardings: Optional[int] = Field(
        None, ge=0, description="Wheelchair boardings"
    )
    ramp_alightings: Optional[int] = Field(
        None, ge=0, description="Wheelchair alightings"
    )
    service_date: Optional[str] = Field(
        None, description="Date of the actual trip (YYYYMMDD)"
    )
    service_arrival_time: Optional[str] = Field(
        None, description="Actual arrival time (HH:MM:SS)"
    )
    service_departure_time: Optional[str] = Field(
        None, description="Actual departure time (HH:MM:SS)"
    )
    source: Optional[str] = Field(
        None, description="Data source identifier"
    )

    @field_validator("service_date", mode="before")
    @classmethod
    def _validate_date(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_date(v, "service_date")

    @field_validator("service_arrival_time", "service_departure_time", mode="before")
    @classmethod
    def _validate_time(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_time(v, "service time")


class GtfsRidership(BaseModel):
    """ridership.txt (GTFS-ride) — Aggregated ridership counts for a period."""

    total_boardings: int = Field(
        ..., ge=0, description="Total boardings for the aggregation period"
    )
    total_alightings: int = Field(
        ..., ge=0, description="Total alightings for the aggregation period"
    )
    ridership_start_date: str = Field(
        ..., description="Start of the aggregation period (YYYYMMDD)"
    )
    ridership_end_date: str = Field(
        ..., description="End of the aggregation period (YYYYMMDD)"
    )
    service_id: Optional[str] = Field(
        None,
        description="Service days covered (conditionally required)",
    )
    monday_through_sunday: Optional[str] = Field(
        None, description="Which days are included in the aggregate"
    )
    trip_id: Optional[str] = Field(
        None, description="Restrict to a specific trip"
    )
    route_id: Optional[str] = Field(
        None, description="Restrict to a specific route"
    )
    direction_id: Optional[int] = Field(
        None, ge=0, le=1, description="Restrict to a direction"
    )
    stop_id: Optional[str] = Field(
        None, description="Restrict to a specific stop"
    )

    @field_validator("ridership_start_date", "ridership_end_date", mode="before")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        result = _validate_gtfs_date(v, "ridership date")
        assert result is not None
        return result


class GtfsRideFeedInfo(BaseModel):
    """ride_feed_info.txt (GTFS-ride) — Metadata about the ridership data."""

    ride_files: str = Field(
        ..., description="Which GTFS-ride files are included"
    )
    ride_start_date: str = Field(
        ..., description="Earliest date of ridership data (YYYYMMDD)"
    )
    ride_end_date: str = Field(
        ..., description="Latest date of ridership data (YYYYMMDD)"
    )
    gtfs_feed_date: Optional[str] = Field(
        None, description="Date the base GTFS feed was published (YYYYMMDD)"
    )
    default_currency_type: Optional[str] = Field(
        None, description="ISO 4217 currency code (e.g. EUR)"
    )
    ride_feed_version: Optional[str] = Field(
        None, description="Version string"
    )

    @field_validator(
        "ride_start_date", "ride_end_date", "gtfs_feed_date", mode="before"
    )
    @classmethod
    def _validate_date(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_date(v, "ride feed date")


class GtfsTripCapacity(BaseModel):
    """trip_capacity.txt (GTFS-ride) — Vehicle capacity information per trip."""

    trip_id: str = Field(
        ..., description="References trips.trip_id"
    )
    service_date: Optional[str] = Field(
        None, description="Date the capacity applies (YYYYMMDD)"
    )
    vehicle_description: Optional[str] = Field(
        None, description="Human-readable vehicle type"
    )
    seated_capacity: Optional[int] = Field(
        None, ge=0, description="Number of seats"
    )
    standing_capacity: Optional[int] = Field(
        None, ge=0, description="Standing passenger limit"
    )
    wheelchair_capacity: Optional[int] = Field(
        None, ge=0, description="Wheelchair spaces"
    )
    bike_capacity: Optional[int] = Field(
        None, ge=0, description="Bike rack spaces"
    )

    @field_validator("service_date", mode="before")
    @classmethod
    def _validate_date(cls, v: Optional[str]) -> Optional[str]:
        return _validate_gtfs_date(v, "service_date")


# ═══════════════════════════════════════════════════════════════════════════
# Registry — maps GTFS filename → Pydantic model class
# ═══════════════════════════════════════════════════════════════════════════

GTFS_TABLE_MODELS = {
    "agency": GtfsAgency,
    "stops": GtfsStop,
    "routes": GtfsRoute,
    "trips": GtfsTrip,
    "stop_times": GtfsStopTime,
    "calendar": GtfsCalendar,
    "calendar_dates": GtfsCalendarDate,
    "shapes": GtfsShape,
    "frequencies": GtfsFrequency,
    "transfers": GtfsTransfer,
    "feed_info": GtfsFeedInfo,
    # GTFS-ride
    "board_alight": GtfsBoardAlight,
    "ridership": GtfsRidership,
    "ride_feed_info": GtfsRideFeedInfo,
    "trip_capacity": GtfsTripCapacity,
}
"""Map from GTFS table name (without .txt) to the Pydantic model class."""

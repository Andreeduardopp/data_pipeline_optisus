from datetime import date, time, timedelta, datetime
from enum import Enum
from typing import List, Tuple, Optional
from pydantic import BaseModel, Field, FilePath


# --- Enums for Categorical Fields ---

class TemporalResolution(str, Enum):
    FIFTEEN_MIN = "15min"
    THIRTY_MIN = "30min"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class DayType(str, Enum):
    WEEKDAY = "weekday"
    SATURDAY = "saturday"
    SUNDAY_HOLIDAY = "sunday_holiday"

class Season(str, Enum):
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    WINTER = "winter"

class ZoneType(str, Enum):
    URBAN = "urban"
    SUBURBAN = "suburban"
    RURAL = "rural"

class FareCategory(str, Enum):
    SINGLE_TICKET = "single_ticket"
    MONTHLY_PASS = "monthly_pass"
    STUDENT = "student"
    SENIOR = "senior"
    SOCIAL = "social"
    FREE = "free"
    OTHER = "other"

class EventType(str, Enum):
    HOLIDAY = "holiday"
    SCHOOL_BREAK = "school_break"
    STRIKE = "strike"
    WEATHER = "weather"
    SPECIAL_EVENT = "special_event"


# --- Fleet Identification ---
class FleetIdentification(BaseModel):
    owner_operator: str = Field(..., description="Responsible company or public entity")
    vehicle_type: str = Field(..., description="Classification by propulsion (diesel, electric, H2, etc.)")
    manufacturer_model: str = Field(..., description="Brand, model, and technical series")
    vehicle_id: str = Field(..., description="Unique internal identifier for the vehicle")
    emissions_standard: str = Field(..., description="Euro Standard (e.g., Euro III, IV, V, VI, EEV)")
    total_capacity: int = Field(..., ge=0, description="Maximum number of passengers (seated + standing)")
    seated_capacity: int = Field(..., ge=0, description="Number of seats available")
    average_age: int = Field(..., ge=0, description="Average years since manufacture/acquisition")

# --- Fleet Energy Performance ---
class FleetEnergyPerformance(BaseModel):
    avg_consumption_per_vehicle: float = Field(..., ge=0.0, description="Average consumption (fuel/energy) per km (kWh or L/km)")
    energy_cons_per_pax_km: float = Field(..., ge=0.0, description="Energy consumption per passenger-km")
    average_co2_emissions: float = Field(..., ge=0.0, description="Average CO2eq emissions per km or year (gCO2/km)")
    operational_status: bool = Field(..., description="Number of active/inoperative vehicles (Binary status)")

# --- Electric Fleet Characteristics ---
class ElectricFleetCharacteristics(BaseModel):
    battery_capacity: float = Field(..., ge=0.0, description="Battery Capacity (kWh)")
    range_autonomy: float = Field(..., ge=0.0, description="Maximum distance with one fuel/charge cycle (km)")
    battery_soh: float = Field(..., ge=0.0, le=100.0, description="Battery State of Health (%)")

# --- Operations and Circulation ---
class OperationsAndCirculation(BaseModel):
    line_id: str = Field(..., description="Primary key linking to TransportedPassengers")
    stop_id: str = Field(..., description="Primary key linking to the spatial node")
    direction_id: int = Field(..., ge=0, le=1, description="Route direction (0 or 1, aligns with GTFS)")
    stop_sequence: int = Field(..., ge=1, description="Order of the stop on the route (Builds the graph edges)")
    stops_coordinates: Tuple[float, float] = Field(..., description="Geographic location (Lat, Lon)")
    service_start_time: time = Field(..., description="Daily operating start time per line")
    service_end_time: time = Field(..., description="Daily operating end time per line")
    average_trip_duration: timedelta = Field(..., description="Average round trip time per line (min)")
    operating_lines: str = Field(..., description="Line identification and codes")
    route_length: float = Field(..., ge=0.0, description="Total distance of each line (km)")
    deadhead_kilometers: float = Field(..., ge=0.0, description="Distance traveled without passengers (Garage <-> Line) (km)")
    avg_operational_speed: float = Field(..., ge=0.0, description="Average operational speed (km/h)")
    speed_profile: float = Field(..., ge=0.0, description="Average speed variation (Peak vs Off-Peak) (km/h)")
    punctuality_delays: timedelta = Field(..., description="Average delays per line (min/trip)")
    average_dwell_time: timedelta = Field(..., description="Average time stationary at stops (sec/min)")
    avg_vehicle_occupancy: float = Field(..., ge=0.0, le=100.0, description="Percentage of capacity used (%)")
    frequency: int = Field(..., ge=0, description="Number of circulations per day")
    infrastructure_location: str = Field(..., description="Garages, depots, and workshops (Address)")
    historical_data: Optional[FilePath] = Field(None, description="Daily/monthly circulation files (CSV)")

# --- Transported Passengers ---
class TransportedPassengers(BaseModel):
    timestamp: datetime = Field(..., description="Exact date and time of the validation period")
    line_id: str = Field(..., description="Identifier for the route")
    stop_id: str = Field(..., description="Identifier for the stop (spatial node)")
    direction_id: Optional[int] = Field(None, ge=0, le=1, description="Route direction (0 or 1, aligns with GTFS)")
    temporal_resolution: TemporalResolution = Field(..., description="Aggregation granularity of this record")
    number_of_validations: int = Field(..., ge=0, description="Total validations")
    number_of_users: int = Field(..., ge=0, description="Distinct passengers per line")
    boarding_count: int = Field(..., ge=0, description="Passengers boarding at this stop")
    alighting_count: int = Field(..., ge=0, description="Passengers alighting at this stop")
    passenger_km_index: float = Field(..., ge=0.0, description="Passengers per km traveled (IPK)")
    fare_category: FareCategory = Field(..., description="Fare type used for this validation group")

# --- Charging Infrastructure ---
class ChargingInfrastructure(BaseModel):
    charger_type: str = Field(..., description="AC normal, DC rapid, DC ultrafast")
    nominal_power: float = Field(..., ge=0.0, description="kW (e.g.: 22, 50, 150+)")
    est_charging_time: timedelta = Field(..., description="Estimated charging time (min)")
    brand_and_model: str = Field(..., description="Equipment manufacturer")
    charging_voltage: float = Field(..., ge=0.0, description="Interval (e.g.: 200–1000 V)")
    utilization_rate: float = Field(..., ge=0.0, le=100.0, description="Usage time vs. available time (%)")
    charging_point_location: Tuple[float, float] = Field(..., description="Garages or public space (Coordinates)")

# --- Geographic Data (GIS) ---
class GeographicData(BaseModel):
    lines_shp: FilePath = Field(..., description="Georeferenced transport network (SHP File)")
    stops_shp: FilePath = Field(..., description="Geographic coordinates of stops (SHP File)")
    gtfs_feed: FilePath = Field(..., description="Routes, stops, and schedules (.zip)")
    urban_zoning: FilePath = Field(..., description="Urban and rural areas (Polygon SHP)")
    contour_lines: FilePath = Field(..., description="Digital Terrain Model (GeoTIFF)")
    demand_mapping: FilePath = Field(..., description="Demand mapping by zone (Origin-Destination Matrix)")
    hydrography_roads: FilePath = Field(..., description="Rivers, bridges, relevant road networks (SHP/GeoJSON)")


class CalendarEvent(BaseModel):
    event_date: date = Field(..., description="Date of the event")
    event_type: EventType = Field(..., description="Category of the event")
    event_description: str = Field(..., description="Free-text event description")
    affected_lines: Optional[List[str]] = Field(None, description="Line IDs affected (None = all lines)")
    expected_demand_factor: Optional[float] = Field(
        None, ge=0.0, le=2.0,
        description="Multiplier vs normal demand (0.0=no service, 1.0=normal, >1.0=surge)",
    )

# --- Weather Observations ---
class WeatherObservation(BaseModel):
    timestamp: datetime = Field(..., description="Observation date and time")
    temperature_celsius: Optional[float] = Field(None, description="Air temperature (C)")
    precipitation_mm: Optional[float] = Field(None, ge=0.0, description="Precipitation (mm)")
    wind_speed_kmh: Optional[float] = Field(None, ge=0.0, description="Wind speed (km/h)")
    weather_condition: Optional[str] = Field(None, description="Descriptive condition (e.g. clear, rain, fog)")

# --- Stop Spatial Features (GNN node attributes) ---
class StopSpatialFeatures(BaseModel):
    stop_id: str = Field(..., description="Unique stop identifier matching TransportedPassengers")
    stop_name: Optional[str] = Field(None, description="Human-readable stop name")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="WGS-84 latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="WGS-84 longitude")
    zone_id: Optional[str] = Field(None, description="Traffic analysis zone or census tract ID")
    zone_type: Optional[ZoneType] = Field(None, description="Land-use classification around the stop")
    elevation_m: Optional[float] = Field(None, description="Elevation above sea level (m)")
    population_density: Optional[float] = Field(
        None, ge=0.0, description="Population per km2 in surrounding area",
    )
    num_lines_served: Optional[int] = Field(None, ge=1, description="Number of distinct lines serving this stop")
    is_terminal: Optional[bool] = Field(None, description="Whether this stop is a route terminal")
    is_interchange: Optional[bool] = Field(None, description="Whether passengers can transfer between lines here")

# --- Stop Connections (graph edges for adjacency matrix) ---
class StopConnection(BaseModel):
    source_stop_id: str = Field(..., description="Origin stop ID")
    target_stop_id: str = Field(..., description="Destination stop ID")
    line_id: str = Field(..., description="Line that links these stops")
    direction_id: int = Field(..., ge=0, le=1, description="Route direction (0 or 1)")
    sequence_order: int = Field(..., ge=1, description="Position of this edge in the route sequence")
    distance_km: Optional[float] = Field(None, ge=0.0, description="Distance between the two stops (km)")
    travel_time_seconds: Optional[int] = Field(None, ge=0, description="Typical travel time between stops (s)")

# --- Financial & Economic Data ---
class FinancialEconomicData(BaseModel):
    average_cost_per_route: float = Field(..., ge=0.0, description="Total average operating cost per line ($/line)")
    energy_cost_propulsion: float = Field(..., ge=0.0, description="Cost by type (diesel, electric, etc.) ($/km)")
    unit_price_fuel_energy: float = Field(..., ge=0.0, description="Price per liter or kWh ($/L or $/kWh)")
    maintenance_costs_annual: float = Field(..., ge=0.0, description="Preventive and corrective maintenance ($/vehicle)")
    average_cost_per_km: float = Field(..., ge=0.0, description="Total operating cost per km ($/km)")
    insurance_costs: float = Field(..., ge=0.0, description="Annual insurance costs ($/year)")
    personnel_count: int = Field(..., ge=0, description="Number of drivers and crews")

# --- Lifespan and Depreciation ---
class LifespanAndDepreciation(BaseModel):
    historical_replacement_date: date = Field(..., description="Dates of disposal/replacement")
    asset_value: float = Field(..., ge=0.0, description="Fleet book value (Currency)")
    depreciation_rate: float = Field(..., ge=0.0, le=100.0, description="Average annual depreciation by vehicle type (%)")


# ===================================================================
# Gold-Layer Output Schemas (ML-ready dataset definitions)
# These are NOT used for raw data ingestion; they define the structure
# of the datasets produced by the pipeline for downstream NN training.
# ===================================================================

# --- Time-Series Demand Sample ---
class TimeSeriesDemandSample(BaseModel):
    """Single observation for the temporal demand prediction model.
    The downstream model receives sequences of these grouped by line_id."""
    timestamp: datetime = Field(..., description="Observation timestamp")
    line_id: str = Field(..., description="Route identifier")
    demand: int = Field(..., ge=0, description="Target: total passenger count")
    temporal_resolution: TemporalResolution = Field(..., description="Aggregation granularity")
    hour_of_day: Optional[int] = Field(None, ge=0, le=23, description="Hour extracted from timestamp")
    day_of_week: int = Field(..., ge=0, le=6, description="0=Monday, 6=Sunday")
    day_of_month: int = Field(..., ge=1, le=31)
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=2000)
    is_weekend: bool = Field(..., description="Saturday or Sunday")
    is_holiday: bool = Field(..., description="Date falls on a CalendarEvent holiday")
    day_type: DayType = Field(..., description="Weekday / Saturday / Sunday-Holiday classification")
    season: Season = Field(..., description="Meteorological season")
    avg_frequency: Optional[float] = Field(
        None, ge=0.0, description="Scheduled departures per hour for this line at this time",
    )
    total_capacity: Optional[int] = Field(None, ge=0, description="Total vehicle capacity on the line")
    temperature_celsius: Optional[float] = Field(None, description="Air temperature (C)")
    precipitation_mm: Optional[float] = Field(None, ge=0.0, description="Precipitation (mm)")

# --- Spatio-Temporal Demand Sample ---
class SpatioTemporalDemandSample(BaseModel):
    """Single observation for the spatio-temporal demand prediction model.
    The downstream model uses (node_id, timestamp) as the compound key."""
    timestamp: datetime = Field(..., description="Observation timestamp")
    node_id: str = Field(..., description="stop_id acting as the spatial graph node")
    line_id: str = Field(..., description="Route identifier")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="WGS-84 latitude of the stop")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="WGS-84 longitude of the stop")
    demand: int = Field(..., ge=0, description="Target: passenger count at this stop")
    boarding_count: Optional[int] = Field(None, ge=0, description="Boardings at this stop")
    alighting_count: Optional[int] = Field(None, ge=0, description="Alightings at this stop")
    temporal_resolution: TemporalResolution = Field(..., description="Aggregation granularity")
    hour_of_day: Optional[int] = Field(None, ge=0, le=23, description="Hour extracted from timestamp")
    day_of_week: int = Field(..., ge=0, le=6, description="0=Monday, 6=Sunday")
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=2000)
    is_weekend: bool = Field(..., description="Saturday or Sunday")
    is_holiday: bool = Field(..., description="Date falls on a CalendarEvent holiday")
    day_type: DayType = Field(..., description="Weekday / Saturday / Sunday-Holiday classification")
    season: Season = Field(..., description="Meteorological season")
    zone_type: Optional[ZoneType] = Field(None, description="Land-use classification around the stop")
    population_density: Optional[float] = Field(None, ge=0.0, description="Population per km2 around stop")
    num_lines_served: Optional[int] = Field(None, ge=1, description="Lines serving this stop")
    is_terminal: Optional[bool] = Field(None, description="Route terminal flag")
    is_interchange: Optional[bool] = Field(None, description="Transfer point flag")
    avg_frequency: Optional[float] = Field(None, ge=0.0, description="Scheduled departures/hour")
    temperature_celsius: Optional[float] = Field(None, description="Air temperature (C)")
    precipitation_mm: Optional[float] = Field(None, ge=0.0, description="Precipitation (mm)")

# --- Network Topology (graph structure for spatio-temporal model) ---
class NetworkTopology(BaseModel):
    """Defines the graph structure for the spatio-temporal model.
    Stored as a Gold artifact alongside the demand samples."""
    nodes: List[StopSpatialFeatures] = Field(..., description="All stop nodes in the network")
    edges: List[StopConnection] = Field(..., description="All edges connecting stops")
    num_nodes: int = Field(..., ge=1, description="Total number of nodes")
    num_edges: int = Field(..., ge=0, description="Total number of edges")
    crs: str = Field(default="EPSG:4326", description="Coordinate reference system")

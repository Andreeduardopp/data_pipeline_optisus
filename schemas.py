from datetime import date, time, timedelta, datetime
from typing import Tuple, Optional
from pydantic import BaseModel, Field, FilePath

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
    stops: Tuple[float, float] = Field(..., description="Geographic location of stops (Latitude, Longitude)")
    infrastructure_location: str = Field(..., description="Garages, depots, and workshops (Address)")
    historical_data: FilePath = Field(..., description="Daily/monthly circulation files (CSV)")
    atypical_episodes: str = Field(..., description="Holidays, strikes, weather events")

# --- Transported Passengers ---
class TransportedPassengers(BaseModel):
    timestamp: datetime = Field(..., description="Exact date and time of the validation period")
    line_id: str = Field(..., description="Identifier for the route (Mandatory for Mode B)")
    stop_id: Optional[str] = Field(None, description="Identifier for the stop (Mandatory for Mode B)")
    number_of_validations: int = Field(..., ge=0, description="Total validations")
    number_of_users: int = Field(..., ge=0, description="Distinct passengers per line")
    average_data_by_period: int = Field(..., ge=0, description="Indicators per hour/day/month (2022–2024)")
    passenger_km_index: float = Field(..., ge=0.0, description="Passengers per km traveled (IPK)")
    fare_structure: str = Field(..., description="Fare structure (Categorical: single ticket, monthly pass, student rates, etc.)")

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

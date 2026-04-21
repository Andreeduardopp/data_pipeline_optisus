# Schema Definitions (GTFS)

This directory contains the core Pydantic v2 data modeling logic, enforcing rules at runtime.

## Files and their Importance

### `gtfs.py`
**Importance:** Serves as the ultimate programmatic source of truth for the GTFS specifications limits. While SQLite provides strict referential enforcement, these schemas provide pre-insertion validation preventing structural pollution. The file captures standard Core GTFS elements as well as the specialized "GTFS-ride" module extensions.

**Key Components:**
- **Enumerations**: Standard GTFS constants represented as typed ENUMs preventing drift configuration (e.g. `LocationType`, `RouteType`, `PickupDropOffType`, `ExceptionType`).
- **Standard GTFS Models** (`GtfsAgency`, `GtfsStop`, `GtfsRoute`, `GtfsTrip`, `GtfsStopTime`, `GtfsCalendar`, `GtfsShape`, etc): Provides precise mapping to matching `.txt` structures.
- **GTFS-ride Extensions Models** (`GtfsBoardAlight`, `GtfsRidership`, `GtfsRideFeedInfo`, `GtfsTripCapacity`): Represents complex ridership and hardware capacity analytics structurally.
- **Custom Validations** (`_validate_gtfs_time`, `_validate_gtfs_date`, `_validate_hex_color`): Class methods ensuring strings resolve properly even spanning unusual cases, like time-strings surpassing `24:00:00` for transit lines running past midnight into the following morning.

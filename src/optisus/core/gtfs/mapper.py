"""
Silver → GTFS data mapper.

Transforms Module 1's validated Silver-layer Parquet files into
GTFS-canonical records and inserts them into the project's GTFS database.

Each individual mapper reads a Silver Parquet file and returns a list of
plain dicts matching the GTFS column names.  The orchestrator calls all
applicable mappers and writes the results via ``gtfs_database.upsert_records()``.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from optisus.core.gtfs.database import (
    create_gtfs_database,
    upsert_records,
)
from optisus.core.storage.layers import get_project_silver_datasets

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# Data classes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class MappingResult:
    """Result of mapping one Silver dataset to one GTFS table."""
    gtfs_table: str = ""
    records_mapped: int = 0
    records_failed: int = 0
    warnings: List[str] = field(default_factory=list)


@dataclass
class MappingReport:
    """Aggregate result of map_project_to_gtfs()."""
    results: List[MappingResult] = field(default_factory=list)
    total_mapped: int = 0
    total_failed: int = 0
    unmapped_tables: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# ID generation helpers
# ═══════════════════════════════════════════════════════════════════════════

def generate_trip_id(line_id: str, direction_id: int, index: int) -> str:
    """Produce a deterministic, human-readable trip ID.

    Format: ``{line_id}_{direction_id}_{index:04d}``
    Example: ``L001_0_0001``
    """
    return f"{line_id}_{direction_id}_{index:04d}"


def generate_service_id(day_pattern: str = "WEEKDAY") -> str:
    """Produce a human-readable service ID from a day-pattern label.

    Accepted patterns (case-insensitive):
      weekday, saturday, sunday, sunday_holiday, daily, custom
    """
    return day_pattern.strip().upper().replace(" ", "_")


# ═══════════════════════════════════════════════════════════════════════════
# Mapping: Silver schema name → GTFS table the mapper populates
# ═══════════════════════════════════════════════════════════════════════════

# Keys must match the ``context`` labels used by storage_layers
# (which are the schema display names from ui_validation.TABULAR_SCHEMAS)
_SCHEMA_TO_MAPPER = {
    "Stop Spatial Features": "_map_stops",
    "Stop Connections": "_map_transfers",
    "Calendar Events": "_map_calendar_dates",
    "Transported Passengers": "_map_board_alight",
    "Operations and Circulation": "_map_operations",     # → routes, trips, stop_times
    "Fleet Identification": "_map_fleet",                 # → agency, trip_capacity
}


# ═══════════════════════════════════════════════════════════════════════════
# Individual mappers
# Each returns a dict  {gtfs_table_name: [list of record dicts]}
# ═══════════════════════════════════════════════════════════════════════════

def _read_silver(path: str) -> pd.DataFrame:
    """Read a Silver Parquet (or CSV fallback) into a DataFrame."""
    p = Path(path)
    if p.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


# ── Stops ────────────────────────────────────────────────────────────────

def map_stops(silver_path: str) -> List[Dict[str, Any]]:
    """Map StopSpatialFeatures → GTFS ``stops``."""
    df = _read_silver(silver_path)
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        location_type = 0
        if pd.notna(row.get("is_terminal")) and row["is_terminal"]:
            location_type = 1

        records.append({
            "stop_id": str(row["stop_id"]),
            "stop_name": row.get("stop_name"),
            "stop_lat": row.get("latitude"),
            "stop_lon": row.get("longitude"),
            "zone_id": row.get("zone_id"),
            "location_type": location_type,
        })
    return records


# ── Transfers ────────────────────────────────────────────────────────────

def map_transfers(silver_path: str) -> List[Dict[str, Any]]:
    """Map StopConnection → GTFS ``transfers``."""
    df = _read_silver(silver_path)
    records: List[Dict[str, Any]] = []
    seen: set = set()
    for _, row in df.iterrows():
        key = (str(row["source_stop_id"]), str(row["target_stop_id"]))
        if key in seen:
            continue
        seen.add(key)

        tt = row.get("travel_time_seconds")
        transfer_type = 2 if pd.notna(tt) and int(tt) > 0 else 0

        records.append({
            "from_stop_id": str(row["source_stop_id"]),
            "to_stop_id": str(row["target_stop_id"]),
            "transfer_type": transfer_type,
            "min_transfer_time": int(tt) if pd.notna(tt) else None,
        })
    return records


# ── Calendar Dates ───────────────────────────────────────────────────────

_REMOVE_EVENTS = {"holiday", "strike"}
_ADD_EVENTS = {"special_event"}


def map_calendar_dates(
    silver_path: str,
    service_id: str = "WEEKDAY",
) -> List[Dict[str, Any]]:
    """Map CalendarEvent → GTFS ``calendar_dates``."""
    df = _read_silver(silver_path)
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        event_type = str(row.get("event_type", "")).lower().strip()
        if event_type in _REMOVE_EVENTS:
            exception_type = 2
        elif event_type in _ADD_EVENTS:
            exception_type = 1
        else:
            # Default: treat unknown events as service removed
            exception_type = 2

        # Convert date to YYYYMMDD string
        raw_date = row.get("event_date", "")
        gtfs_date = _to_yyyymmdd(str(raw_date))
        if not gtfs_date:
            continue

        records.append({
            "service_id": service_id,
            "date": gtfs_date,
            "exception_type": exception_type,
        })
    return records


# ── Operations (routes + trips + stop_times) ─────────────────────────────

def map_routes(silver_path: str) -> List[Dict[str, Any]]:
    """Map OperationsAndCirculation → GTFS ``routes``."""
    df = _read_silver(silver_path)
    # Deduplicate: one route per unique line_id
    routes_df = df.drop_duplicates(subset=["line_id"])
    records: List[Dict[str, Any]] = []
    for _, row in routes_df.iterrows():
        records.append({
            "route_id": str(row["line_id"]),
            "route_short_name": str(row.get("operating_lines", row["line_id"])),
            "route_type": 3,  # Default: bus
        })
    return records


def map_trips(
    silver_path: str,
    service_id: str = "WEEKDAY",
) -> List[Dict[str, Any]]:
    """Map OperationsAndCirculation → GTFS ``trips``.

    Creates one trip per unique (line_id, direction_id) pair.
    """
    df = _read_silver(silver_path)
    trip_groups = df.drop_duplicates(subset=["line_id", "direction_id"])
    records: List[Dict[str, Any]] = []
    idx = 0
    for _, row in trip_groups.iterrows():
        line_id = str(row["line_id"])
        direction = int(row["direction_id"])
        idx += 1
        records.append({
            "route_id": line_id,
            "service_id": service_id,
            "trip_id": generate_trip_id(line_id, direction, idx),
            "direction_id": direction,
        })
    return records


def map_stop_times(
    silver_path: str,
    trips: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Map OperationsAndCirculation → GTFS ``stop_times``.

    Uses the trip list generated by ``map_trips()`` to assign trip_ids.
    Times are interpolated from ``service_start_time`` through
    ``service_end_time`` using ``average_dwell_time`` per stop.
    """
    df = _read_silver(silver_path)
    # Build a lookup: (line_id, direction_id) → trip_id
    trip_lookup: Dict[tuple, str] = {}
    for t in trips:
        trip_lookup[(t["route_id"], t["direction_id"])] = t["trip_id"]

    records: List[Dict[str, Any]] = []
    for (line_id, dir_id), grp in df.groupby(["line_id", "direction_id"]):
        trip_id = trip_lookup.get((str(line_id), int(dir_id)))
        if not trip_id:
            continue

        grp = grp.sort_values("stop_sequence")

        # Parse the service start time (use from first row)
        first = grp.iloc[0]
        start_str = str(first.get("service_start_time", "06:00:00"))
        current_secs = _time_str_to_seconds(start_str)

        # Dwell time per stop (seconds)
        dwell_raw = first.get("average_dwell_time", 30)
        dwell_secs = _parse_duration_secs(dwell_raw, default=30)

        for _, row in grp.iterrows():
            arrival = _seconds_to_time_str(current_secs)
            departure = _seconds_to_time_str(current_secs + dwell_secs)

            records.append({
                "trip_id": trip_id,
                "arrival_time": arrival,
                "departure_time": departure,
                "stop_id": str(row["stop_id"]),
                "stop_sequence": int(row["stop_sequence"]),
            })

            # Advance time: dwell + estimated inter-stop travel
            travel_secs = dwell_secs + 120  # 2 minutes default travel
            current_secs += travel_secs

    return records


# ── Fleet → Agency + Trip Capacity ───────────────────────────────────────

def map_agency(silver_path: str) -> List[Dict[str, Any]]:
    """Map FleetIdentification → GTFS ``agency``.

    Extracts the first unique ``owner_operator`` as the agency.
    """
    df = _read_silver(silver_path)
    operators = df["owner_operator"].dropna().unique()
    records: List[Dict[str, Any]] = []
    for i, op in enumerate(operators):
        agency_id = _safe_id(op) if len(operators) > 1 else _safe_id(op)
        records.append({
            "agency_id": agency_id,
            "agency_name": str(op),
            "agency_url": "https://example.com",
            "agency_timezone": "America/Sao_Paulo",
        })
    return records


def map_trip_capacity(
    silver_path: str,
    trips: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Map FleetIdentification → GTFS-ride ``trip_capacity``.

    Assigns the fleet's capacity data to each known trip.
    """
    df = _read_silver(silver_path)
    if df.empty or not trips:
        return []

    # Use average capacity across the fleet
    seated = int(df["seated_capacity"].mean()) if "seated_capacity" in df.columns else None
    total = int(df["total_capacity"].mean()) if "total_capacity" in df.columns else None
    standing = (total - seated) if (total and seated) else None

    records: List[Dict[str, Any]] = []
    for t in trips:
        records.append({
            "trip_id": t["trip_id"],
            "seated_capacity": seated,
            "standing_capacity": standing,
            "vehicle_description": str(df.iloc[0].get("manufacturer_model", "")) if not df.empty else None,
        })
    return records


# ── Board/Alight ─────────────────────────────────────────────────────────

def map_board_alight(
    silver_path: str,
    trip_lookup: Dict[tuple, str],
) -> List[Dict[str, Any]]:
    """Map TransportedPassengers → GTFS-ride ``board_alight``.

    Args:
        trip_lookup: mapping of ``(line_id, direction_id) → trip_id``
    """
    df = _read_silver(silver_path)
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        line_id = str(row.get("line_id", ""))
        dir_id = int(row["direction_id"]) if pd.notna(row.get("direction_id")) else 0
        trip_id = trip_lookup.get((line_id, dir_id))
        if not trip_id:
            # Try to find any trip for this line
            for (lid, did), tid in trip_lookup.items():
                if lid == line_id:
                    trip_id = tid
                    break
        if not trip_id:
            continue

        # Extract service date from timestamp
        ts = row.get("timestamp", "")
        service_date = _to_yyyymmdd(str(ts)[:10]) if pd.notna(ts) else None

        records.append({
            "trip_id": trip_id,
            "stop_id": str(row["stop_id"]),
            "stop_sequence": 0,  # Will be approximate
            "record_use": 0,     # boardings + alightings
            "boardings": int(row.get("boarding_count", 0)),
            "alightings": int(row.get("alighting_count", 0)),
            "current_load": int(row.get("number_of_users", 0)) if pd.notna(row.get("number_of_users")) else None,
            "service_date": service_date,
        })
    return records


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

# The required GTFS tables for a minimum viable feed
_REQUIRED_TABLES = {"agency", "stops", "routes", "trips", "stop_times", "calendar"}
_ALL_GTFS_TABLES = {
    "agency", "stops", "routes", "trips", "stop_times",
    "calendar", "calendar_dates", "shapes", "frequencies",
    "transfers", "feed_info",
    "board_alight", "ridership", "ride_feed_info", "trip_capacity",
}


def map_project_to_gtfs(
    project_slug: str,
    available_datasets: Optional[Dict[str, str]] = None,
) -> MappingReport:
    """Orchestrate all individual mappers for a project.

    Reads Silver datasets (auto-discovered or provided), maps them to
    GTFS records, and inserts into the project's GTFS database.

    Returns a ``MappingReport`` summarising what was mapped.
    """
    report = MappingReport()

    # 1. Ensure GTFS database exists
    create_gtfs_database(project_slug)

    # 2. Discover Silver datasets
    if available_datasets is None:
        available_datasets = get_project_silver_datasets(project_slug)

    populated_tables: set = set()
    trips: List[Dict[str, Any]] = []
    trip_lookup: Dict[tuple, str] = {}
    service_id = "WEEKDAY"

    # 3. Map stops first (needed as FK target)
    if "Stop Spatial Features" in available_datasets:
        _run_mapper(
            project_slug, "stops",
            map_stops, available_datasets["Stop Spatial Features"],
            report,
        )
        populated_tables.add("stops")

    # 4. Map operations → routes, trips, stop_times (depends on stops)
    if "Operations and Circulation" in available_datasets:
        ops_path = available_datasets["Operations and Circulation"]

        # routes
        _run_mapper(project_slug, "routes", map_routes, ops_path, report)
        populated_tables.add("routes")

        # trips
        trips = map_trips(ops_path, service_id)
        ir = upsert_records(project_slug, "trips", trips)
        report.results.append(MappingResult(
            gtfs_table="trips",
            records_mapped=ir.inserted,
            records_failed=ir.failed,
            warnings=ir.errors[:20],
        ))
        populated_tables.add("trips")

        # Build trip lookup for downstream mappers
        for t in trips:
            trip_lookup[(t["route_id"], t["direction_id"])] = t["trip_id"]

        # stop_times
        st_records = map_stop_times(ops_path, trips)
        ir = upsert_records(project_slug, "stop_times", st_records)
        report.results.append(MappingResult(
            gtfs_table="stop_times",
            records_mapped=ir.inserted,
            records_failed=ir.failed,
            warnings=ir.errors[:20],
        ))
        populated_tables.add("stop_times")

    # 5. Map transfers
    if "Stop Connections" in available_datasets:
        _run_mapper(
            project_slug, "transfers",
            map_transfers, available_datasets["Stop Connections"],
            report,
        )
        populated_tables.add("transfers")

    # 6. Map calendar dates
    if "Calendar Events" in available_datasets:
        records = map_calendar_dates(
            available_datasets["Calendar Events"], service_id
        )
        ir = upsert_records(project_slug, "calendar_dates", records)
        report.results.append(MappingResult(
            gtfs_table="calendar_dates",
            records_mapped=ir.inserted,
            records_failed=ir.failed,
            warnings=ir.errors[:20],
        ))
        populated_tables.add("calendar_dates")

    # 7. Map fleet → agency + trip_capacity
    if "Fleet Identification" in available_datasets:
        fleet_path = available_datasets["Fleet Identification"]
        _run_mapper(project_slug, "agency", map_agency, fleet_path, report)
        populated_tables.add("agency")

        if trips:
            tc_records = map_trip_capacity(fleet_path, trips)
            ir = upsert_records(project_slug, "trip_capacity", tc_records)
            report.results.append(MappingResult(
                gtfs_table="trip_capacity",
                records_mapped=ir.inserted,
                records_failed=ir.failed,
                warnings=ir.errors[:20],
            ))
            populated_tables.add("trip_capacity")

    # 8. Map board/alight
    if "Transported Passengers" in available_datasets and trip_lookup:
        ba_records = map_board_alight(
            available_datasets["Transported Passengers"], trip_lookup,
        )
        ir = upsert_records(project_slug, "board_alight", ba_records)
        report.results.append(MappingResult(
            gtfs_table="board_alight",
            records_mapped=ir.inserted,
            records_failed=ir.failed,
            warnings=ir.errors[:20],
        ))
        populated_tables.add("board_alight")

    # 9. Summarise
    report.total_mapped = sum(r.records_mapped for r in report.results)
    report.total_failed = sum(r.records_failed for r in report.results)
    report.unmapped_tables = sorted(_ALL_GTFS_TABLES - populated_tables)

    return report


def _run_mapper(
    project_slug: str,
    table_name: str,
    mapper_fn,
    silver_path: str,
    report: MappingReport,
) -> None:
    """Run a simple mapper and upsert its records."""
    try:
        records = mapper_fn(silver_path)
    except Exception as exc:
        report.results.append(MappingResult(
            gtfs_table=table_name,
            records_failed=1,
            warnings=[f"Mapper error: {exc}"],
        ))
        return
    ir = upsert_records(project_slug, table_name, records)
    report.results.append(MappingResult(
        gtfs_table=table_name,
        records_mapped=ir.inserted,
        records_failed=ir.failed,
        warnings=ir.errors[:20],
    ))


# ═══════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═══════════════════════════════════════════════════════════════════════════

def _to_yyyymmdd(date_str: str) -> Optional[str]:
    """Convert various date formats to YYYYMMDD string.

    Accepts: ``YYYY-MM-DD``, ``DD/MM/YYYY``, ``YYYYMMDD``.
    Returns None if parsing fails.
    """
    date_str = date_str.strip()
    if not date_str:
        return None
    # Already YYYYMMDD
    if re.match(r"^\d{8}$", date_str):
        return date_str
    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    # DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", date_str)
    if m:
        return f"{m.group(3)}{m.group(2)}{m.group(1)}"
    return None


def _time_str_to_seconds(t: str) -> int:
    """Parse ``HH:MM:SS`` or ``HH:MM`` to seconds since midnight."""
    parts = str(t).strip().split(":")
    h = int(parts[0]) if len(parts) >= 1 else 0
    m = int(parts[1]) if len(parts) >= 2 else 0
    s = int(parts[2]) if len(parts) >= 3 else 0
    return h * 3600 + m * 60 + s


def _seconds_to_time_str(secs: int) -> str:
    """Convert seconds since midnight to ``HH:MM:SS``."""
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _parse_duration_secs(val: Any, default: int = 30) -> int:
    """Parse a duration value that might be seconds, timedelta string, etc."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
    # Try "HH:MM:SS" or "MM:SS"
    parts = s.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    try:
        return int(float(s))
    except ValueError:
        return default


def _safe_id(text: str) -> str:
    """Convert text to a safe identifier string."""
    return re.sub(r"[^a-zA-Z0-9]+", "_", text.strip()).strip("_").upper()

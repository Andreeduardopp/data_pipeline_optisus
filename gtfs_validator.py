"""
Python-native GTFS feed validator.

Reads an exported GTFS ``.zip`` and checks it against the most important
GTFS spec rules.  Lighter-weight alternative to the official Java-based
MobilityData validator — runs without a JVM.

Checks performed:
  * Required file presence (agency, stops, routes, trips, stop_times,
    plus calendar or calendar_dates)
  * Required field presence and non-empty values
  * No duplicate primary keys
  * Referential integrity (trip↔route, trip↔service, stop_time↔trip,
    stop_time↔stop)
  * Format checks (times, dates, lat/lon, colors, route_type)
  * Logical checks (calendar date ordering, strictly increasing
    stop_sequence per trip)
"""

import csv
import io
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Set


# ═══════════════════════════════════════════════════════════════════════════
# Data classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class GtfsValidationIssue:
    """A single validation issue found in a GTFS feed."""
    severity: str            # "error" | "warning" | "info"
    file: str                # e.g., "stops.txt"
    field: Optional[str] = None
    row: Optional[int] = None
    message: str = ""


@dataclass
class GtfsValidationReport:
    """Aggregate report from ``validate_gtfs_feed()``."""
    is_valid: bool = True    # True if no errors (warnings are OK)
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    issues: List[GtfsValidationIssue] = field(default_factory=list)

    def add(self, issue: GtfsValidationIssue) -> None:
        self.issues.append(issue)
        if issue.severity == "error":
            self.error_count += 1
            self.is_valid = False
        elif issue.severity == "warning":
            self.warning_count += 1
        else:
            self.info_count += 1


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

REQUIRED_FILES = [
    "agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt",
]
CALENDAR_FILES = ["calendar.txt", "calendar_dates.txt"]  # at least one

REQUIRED_FIELDS: Dict[str, List[str]] = {
    "agency.txt":         ["agency_name", "agency_url", "agency_timezone"],
    "stops.txt":          ["stop_id"],
    "routes.txt":         ["route_id", "route_type"],
    "trips.txt":          ["route_id", "service_id", "trip_id"],
    "stop_times.txt":     ["trip_id", "stop_sequence"],
    "calendar.txt":       ["service_id", "monday", "tuesday", "wednesday",
                           "thursday", "friday", "saturday", "sunday",
                           "start_date", "end_date"],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
}

PRIMARY_KEYS: Dict[str, List[str]] = {
    "agency.txt":         ["agency_id"],
    "stops.txt":          ["stop_id"],
    "routes.txt":         ["route_id"],
    "trips.txt":          ["trip_id"],
    "stop_times.txt":     ["trip_id", "stop_sequence"],
    "calendar.txt":       ["service_id"],
    "calendar_dates.txt": ["service_id", "date"],
}

_TIME_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")
_DATE_RE = re.compile(r"^\d{8}$")
_COLOR_RE = re.compile(r"^[0-9A-Fa-f]{6}$")

# GTFS basic route types + extended (common subset)
_VALID_ROUTE_TYPES = {0, 1, 2, 3, 4, 5, 6, 7, 11, 12}


# ═══════════════════════════════════════════════════════════════════════════
# Main entrypoint
# ═══════════════════════════════════════════════════════════════════════════

def validate_gtfs_feed(zip_path: str) -> GtfsValidationReport:
    """Run all validation checks on an exported GTFS ``.zip`` file."""
    report = GtfsValidationReport()

    try:
        zf = zipfile.ZipFile(zip_path, "r")
    except zipfile.BadZipFile:
        report.add(GtfsValidationIssue("error", zip_path, None, None,
                                       "File is not a valid ZIP archive"))
        return report
    except FileNotFoundError:
        report.add(GtfsValidationIssue("error", zip_path, None, None,
                                       "File not found"))
        return report

    with zf:
        filenames: Set[str] = set(zf.namelist())

        _check_required_files(filenames, report)

        # Parse every .txt file into a list of dicts
        tables: Dict[str, List[Dict[str, str]]] = {}
        for name in filenames:
            if not name.endswith(".txt"):
                continue
            try:
                content = zf.read(name).decode("utf-8")
            except UnicodeDecodeError as exc:
                report.add(GtfsValidationIssue(
                    "error", name, None, None,
                    f"File is not valid UTF-8: {exc}",
                ))
                continue
            try:
                reader = csv.DictReader(io.StringIO(content))
                tables[name] = list(reader)
            except Exception as exc:  # pragma: no cover
                report.add(GtfsValidationIssue(
                    "error", name, None, None,
                    f"Could not parse CSV: {exc}",
                ))

        _check_required_fields(tables, report)
        _check_primary_key_uniqueness(tables, report)
        _check_referential_integrity(tables, report)
        _check_time_format(tables, report)
        _check_date_format_and_logic(tables, report)
        _check_coordinates(tables, report)
        _check_route_metadata(tables, report)
        _check_stop_sequence_monotonic(tables, report)

    return report


# ═══════════════════════════════════════════════════════════════════════════
# Individual check groups
# ═══════════════════════════════════════════════════════════════════════════

def _check_required_files(filenames: Set[str], report: GtfsValidationReport) -> None:
    for req in REQUIRED_FILES:
        if req not in filenames:
            report.add(GtfsValidationIssue(
                "error", req, None, None,
                f"Required file '{req}' missing from feed",
            ))
    if not any(f in filenames for f in CALENDAR_FILES):
        report.add(GtfsValidationIssue(
            "error", "calendar.txt", None, None,
            "Either calendar.txt or calendar_dates.txt is required",
        ))


def _check_required_fields(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    for fname, required in REQUIRED_FIELDS.items():
        if fname not in tables:
            continue
        rows = tables[fname]
        if not rows:
            report.add(GtfsValidationIssue(
                "warning", fname, None, None,
                f"File '{fname}' has no data rows",
            ))
            continue

        present_cols = set(rows[0].keys())
        for fld in required:
            if fld not in present_cols:
                report.add(GtfsValidationIssue(
                    "error", fname, fld, None,
                    f"Required field '{fld}' missing from header",
                ))
                continue
            for i, r in enumerate(rows):
                v = r.get(fld, "")
                if v is None or str(v).strip() == "":
                    report.add(GtfsValidationIssue(
                        "error", fname, fld, i,
                        f"Required field '{fld}' is empty",
                    ))


def _check_primary_key_uniqueness(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    for fname, pk in PRIMARY_KEYS.items():
        if fname not in tables:
            continue
        rows = tables[fname]
        if not rows:
            continue
        if not all(c in rows[0] for c in pk):
            # Header missing — already flagged elsewhere
            continue
        seen: Dict[tuple, int] = {}
        for i, r in enumerate(rows):
            key = tuple(r.get(c, "") for c in pk)
            # Skip rows where any PK component is empty (flagged elsewhere)
            if any(v == "" for v in key):
                continue
            if key in seen:
                report.add(GtfsValidationIssue(
                    "error", fname, None, i,
                    f"Duplicate primary key {pk}={list(key)} "
                    f"(also at row {seen[key]})",
                ))
            else:
                seen[key] = i


def _collect_values(
    tables: Dict[str, List[Dict[str, str]]],
    fname: str,
    col: str,
) -> Set[str]:
    if fname not in tables:
        return set()
    return {r[col] for r in tables[fname] if r.get(col)}


def _check_referential_integrity(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    route_ids = _collect_values(tables, "routes.txt", "route_id")
    stop_ids = _collect_values(tables, "stops.txt", "stop_id")
    trip_ids = _collect_values(tables, "trips.txt", "trip_id")
    service_ids = (
        _collect_values(tables, "calendar.txt", "service_id")
        | _collect_values(tables, "calendar_dates.txt", "service_id")
    )

    # trips → routes, calendar
    if "trips.txt" in tables:
        for i, r in enumerate(tables["trips.txt"]):
            rid = r.get("route_id")
            if rid and rid not in route_ids:
                report.add(GtfsValidationIssue(
                    "error", "trips.txt", "route_id", i,
                    f"route_id '{rid}' not found in routes.txt",
                ))
            sid = r.get("service_id")
            if sid and sid not in service_ids:
                report.add(GtfsValidationIssue(
                    "error", "trips.txt", "service_id", i,
                    f"service_id '{sid}' not found in calendar(_dates).txt",
                ))

    # stop_times → trips, stops
    if "stop_times.txt" in tables:
        for i, r in enumerate(tables["stop_times.txt"]):
            tid = r.get("trip_id")
            if tid and tid not in trip_ids:
                report.add(GtfsValidationIssue(
                    "error", "stop_times.txt", "trip_id", i,
                    f"trip_id '{tid}' not found in trips.txt",
                ))
            sid = r.get("stop_id")
            if sid and sid not in stop_ids:
                report.add(GtfsValidationIssue(
                    "error", "stop_times.txt", "stop_id", i,
                    f"stop_id '{sid}' not found in stops.txt",
                ))


def _check_time_format(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    if "stop_times.txt" not in tables:
        return
    for i, r in enumerate(tables["stop_times.txt"]):
        for col in ("arrival_time", "departure_time"):
            v = r.get(col)
            if v and not _TIME_RE.match(v):
                report.add(GtfsValidationIssue(
                    "error", "stop_times.txt", col, i,
                    f"{col} '{v}' does not match HH:MM:SS",
                ))


def _check_date_format_and_logic(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    if "calendar.txt" in tables:
        for i, r in enumerate(tables["calendar.txt"]):
            for col in ("start_date", "end_date"):
                v = r.get(col)
                if v and not _is_valid_yyyymmdd(v):
                    report.add(GtfsValidationIssue(
                        "error", "calendar.txt", col, i,
                        f"{col} '{v}' is not a valid YYYYMMDD date",
                    ))
            s = r.get("start_date", "")
            e = r.get("end_date", "")
            if s and e and _DATE_RE.match(s) and _DATE_RE.match(e) and s > e:
                report.add(GtfsValidationIssue(
                    "error", "calendar.txt", None, i,
                    f"start_date '{s}' is after end_date '{e}'",
                ))

    if "calendar_dates.txt" in tables:
        for i, r in enumerate(tables["calendar_dates.txt"]):
            v = r.get("date")
            if v and not _is_valid_yyyymmdd(v):
                report.add(GtfsValidationIssue(
                    "error", "calendar_dates.txt", "date", i,
                    f"date '{v}' is not a valid YYYYMMDD date",
                ))


def _check_coordinates(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    if "stops.txt" not in tables:
        return
    for i, r in enumerate(tables["stops.txt"]):
        lat = r.get("stop_lat", "")
        lon = r.get("stop_lon", "")
        if lat != "":
            try:
                lat_f = float(lat)
                if not -90.0 <= lat_f <= 90.0:
                    report.add(GtfsValidationIssue(
                        "error", "stops.txt", "stop_lat", i,
                        f"stop_lat {lat_f} is outside [-90, 90]",
                    ))
            except ValueError:
                report.add(GtfsValidationIssue(
                    "error", "stops.txt", "stop_lat", i,
                    f"stop_lat '{lat}' is not numeric",
                ))
        if lon != "":
            try:
                lon_f = float(lon)
                if not -180.0 <= lon_f <= 180.0:
                    report.add(GtfsValidationIssue(
                        "error", "stops.txt", "stop_lon", i,
                        f"stop_lon {lon_f} is outside [-180, 180]",
                    ))
            except ValueError:
                report.add(GtfsValidationIssue(
                    "error", "stops.txt", "stop_lon", i,
                    f"stop_lon '{lon}' is not numeric",
                ))


def _check_route_metadata(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    if "routes.txt" not in tables:
        return
    for i, r in enumerate(tables["routes.txt"]):
        # Colors
        for col in ("route_color", "route_text_color"):
            v = r.get(col, "")
            if v and not _COLOR_RE.match(v):
                report.add(GtfsValidationIssue(
                    "warning", "routes.txt", col, i,
                    f"{col} '{v}' is not a 6-digit hex string",
                ))
        # route_type
        rt = r.get("route_type", "")
        if rt:
            try:
                rt_i = int(rt)
                if rt_i not in _VALID_ROUTE_TYPES:
                    report.add(GtfsValidationIssue(
                        "warning", "routes.txt", "route_type", i,
                        f"route_type {rt_i} is not a standard value",
                    ))
            except ValueError:
                report.add(GtfsValidationIssue(
                    "error", "routes.txt", "route_type", i,
                    f"route_type '{rt}' is not an integer",
                ))


def _check_stop_sequence_monotonic(
    tables: Dict[str, List[Dict[str, str]]],
    report: GtfsValidationReport,
) -> None:
    if "stop_times.txt" not in tables:
        return
    by_trip: Dict[str, List[tuple]] = {}
    for i, r in enumerate(tables["stop_times.txt"]):
        tid = r.get("trip_id", "")
        seq_raw = r.get("stop_sequence", "")
        try:
            seq_i = int(seq_raw)
        except (ValueError, TypeError):
            continue
        by_trip.setdefault(tid, []).append((seq_i, i))

    for tid, lst in by_trip.items():
        lst.sort(key=lambda pair: pair[1])  # preserve file order
        prev = None
        for seq, row_idx in lst:
            if prev is not None and seq <= prev:
                report.add(GtfsValidationIssue(
                    "error", "stop_times.txt", "stop_sequence", row_idx,
                    f"trip_id '{tid}' stop_sequence must strictly increase "
                    f"(saw {prev} then {seq})",
                ))
            prev = seq


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _is_valid_yyyymmdd(s: str) -> bool:
    if not _DATE_RE.match(s):
        return False
    try:
        date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return True
    except ValueError:
        return False

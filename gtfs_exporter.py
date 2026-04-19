"""
GTFS feed exporter.

Reads from the project's GTFS SQLite database and produces a
standards-compliant ``.zip`` archive containing properly formatted
``.txt`` CSV files ready for publication.

Includes pre-export validation and a feed-completeness score.
"""

import csv
import io
import logging
import shutil
import sqlite3
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from gtfs_database import (
    get_connection,
    get_gtfs_db_path,
    get_table_columns,
    get_table_count,
    GTFS_DB_FILENAME,
)
from gtfs_validator import GtfsValidationReport, validate_gtfs_feed
from storage_layers import PROJECTS_ROOT

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Data classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ExportResult:
    """Result of ``export_gtfs_feed()``."""
    success: bool = False
    zip_path: Optional[str] = None
    files_included: List[str] = field(default_factory=list)
    total_records: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    completeness_score: float = 0.0


@dataclass
class ValidationResult:
    """Result of ``validate_before_export()``."""
    can_export: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class FeedCompleteness:
    """Result of ``compute_feed_completeness()``."""
    score: float = 0.0
    breakdown: Dict[str, dict] = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

# Tables grouped by requirement level for validation and scoring
_REQUIRED_TABLES = ["agency", "stops", "routes", "trips", "stop_times"]
_CALENDAR_TABLES = ["calendar", "calendar_dates"]  # at least one required
_RECOMMENDED_TABLES = ["feed_info", "shapes"]
_OPTIONAL_TABLES = ["frequencies", "transfers"]
_RIDE_TABLES = ["board_alight", "ridership", "ride_feed_info", "trip_capacity"]

# All GTFS tables that can be exported (in canonical export order)
_EXPORT_ORDER = [
    "agency", "stops", "routes", "trips", "stop_times",
    "calendar", "calendar_dates",
    "feed_info", "shapes", "frequencies", "transfers",
    # GTFS-ride extension
    "board_alight", "ridership", "ride_feed_info", "trip_capacity",
]

# Scoring weights (must sum to 100)
_WEIGHT_REQUIRED = 60.0      # 5 required tables + calendar group
_WEIGHT_RECOMMENDED = 25.0   # feed_info, shapes
_WEIGHT_OPTIONAL = 15.0      # frequencies, transfers, GTFS-ride


# ═══════════════════════════════════════════════════════════════════════════
# Pre-export validation
# ═══════════════════════════════════════════════════════════════════════════

def validate_before_export(project_slug: str) -> ValidationResult:
    """Check that the GTFS database has the minimum required tables.

    Errors are blocking (prevent export).  Warnings are informational.
    """
    result = ValidationResult()
    db_path = get_gtfs_db_path(project_slug)

    if not db_path.exists():
        result.can_export = False
        result.errors.append("GTFS database does not exist. Run the mapper first.")
        return result

    conn = get_connection(project_slug)
    try:
        counts = _get_all_counts(conn)
    finally:
        conn.close()

    # Check required tables
    for tbl in _REQUIRED_TABLES:
        if counts.get(tbl, 0) == 0:
            result.can_export = False
            result.errors.append(f"Required table '{tbl}' is empty.")

    # Calendar requirement: at least one of calendar / calendar_dates
    cal = counts.get("calendar", 0)
    cal_dates = counts.get("calendar_dates", 0)
    if cal == 0 and cal_dates == 0:
        result.can_export = False
        result.errors.append(
            "Neither 'calendar' nor 'calendar_dates' has records. "
            "At least one is required."
        )

    # Warnings for recommended tables
    for tbl in _RECOMMENDED_TABLES:
        if counts.get(tbl, 0) == 0:
            result.warnings.append(
                f"Recommended table '{tbl}' is empty — feed quality would improve with it."
            )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Feed completeness score
# ═══════════════════════════════════════════════════════════════════════════

def compute_feed_completeness(project_slug: str) -> FeedCompleteness:
    """Compute a 0–100 completeness score for the GTFS feed.

    Weighting:
      - Required tables:     60%
      - Recommended tables:  25%
      - Optional/GTFS-ride:  15%
    """
    result = FeedCompleteness()
    db_path = get_gtfs_db_path(project_slug)

    if not db_path.exists():
        return result

    conn = get_connection(project_slug)
    try:
        counts = _get_all_counts(conn)
    finally:
        conn.close()

    # ── Required (60%) ────────────────────────────────────────────────────
    required_items = list(_REQUIRED_TABLES) + ["calendar_group"]
    required_populated = 0
    for tbl in _REQUIRED_TABLES:
        pop = counts.get(tbl, 0) > 0
        result.breakdown[tbl] = {
            "populated": pop,
            "weight": _WEIGHT_REQUIRED / len(required_items),
            "records": counts.get(tbl, 0),
            "group": "required",
        }
        if pop:
            required_populated += 1

    # Calendar group counts as one required item
    cal_pop = (counts.get("calendar", 0) > 0 or counts.get("calendar_dates", 0) > 0)
    result.breakdown["calendar_group"] = {
        "populated": cal_pop,
        "weight": _WEIGHT_REQUIRED / len(required_items),
        "records": counts.get("calendar", 0) + counts.get("calendar_dates", 0),
        "group": "required",
    }
    if cal_pop:
        required_populated += 1

    required_score = (required_populated / len(required_items)) * _WEIGHT_REQUIRED

    # ── Recommended (25%) ─────────────────────────────────────────────────
    rec_items = _RECOMMENDED_TABLES
    rec_populated = 0
    for tbl in rec_items:
        pop = counts.get(tbl, 0) > 0
        result.breakdown[tbl] = {
            "populated": pop,
            "weight": _WEIGHT_RECOMMENDED / len(rec_items),
            "records": counts.get(tbl, 0),
            "group": "recommended",
        }
        if pop:
            rec_populated += 1
    rec_score = (rec_populated / len(rec_items)) * _WEIGHT_RECOMMENDED

    # ── Optional + GTFS-ride (15%) ────────────────────────────────────────
    opt_items = _OPTIONAL_TABLES + _RIDE_TABLES
    opt_populated = 0
    for tbl in opt_items:
        pop = counts.get(tbl, 0) > 0
        result.breakdown[tbl] = {
            "populated": pop,
            "weight": _WEIGHT_OPTIONAL / len(opt_items),
            "records": counts.get(tbl, 0),
            "group": "optional",
        }
        if pop:
            opt_populated += 1
    opt_score = (opt_populated / len(opt_items)) * _WEIGHT_OPTIONAL

    result.score = round(required_score + rec_score + opt_score, 1)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Core export
# ═══════════════════════════════════════════════════════════════════════════

def export_gtfs_feed(
    project_slug: str,
    output_dir: Optional[str] = None,
    include_ride: bool = True,
) -> ExportResult:
    """Export the GTFS database to a ``.zip`` file.

    Args:
        project_slug: project containing the GTFS database.
        output_dir: destination directory (defaults to project ``exports/``).
        include_ride: include GTFS-ride extension files.

    Returns:
        ``ExportResult`` with path, included files, and validation summary.
    """
    result = ExportResult()

    # 1. Validate
    vr = validate_before_export(project_slug)
    if not vr.can_export:
        result.errors = vr.errors
        result.warnings = vr.warnings
        return result
    result.warnings = vr.warnings

    # 2. Determine output path
    if output_dir is None:
        export_root = PROJECTS_ROOT / project_slug / "exports"
    else:
        export_root = Path(output_dir)
    export_root.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_filename = f"gtfs_{ts}.zip"
    zip_path = export_root / zip_filename

    # 3. Build the ZIP
    conn = get_connection(project_slug)
    try:
        tables_to_export = [
            t for t in _EXPORT_ORDER
            if (include_ride or t not in _RIDE_TABLES)
        ]

        with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as zf:
            for table_name in tables_to_export:
                columns = get_table_columns(table_name)
                if not columns:
                    continue
                csv_content = _table_to_csv(conn, table_name, columns)
                if csv_content is None:
                    continue

                gtfs_filename = f"{table_name}.txt"
                zf.writestr(gtfs_filename, csv_content)
                result.files_included.append(gtfs_filename)

                # Count records (header line excluded)
                record_count = csv_content.count("\r\n") - 1
                result.total_records += max(record_count, 0)

    finally:
        conn.close()

    if not result.files_included:
        result.errors.append("No tables had records to export.")
        return result

    # 4. Copy to latest/
    latest_dir = export_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_path = latest_dir / "gtfs.zip"
    shutil.copy2(str(zip_path), str(latest_path))

    # 5. Completeness score
    completeness = compute_feed_completeness(project_slug)
    result.completeness_score = completeness.score

    result.success = True
    result.zip_path = str(zip_path)

    logger.info(
        "GTFS feed exported to %s (%d files, %d records, %.0f%% complete)",
        zip_path, len(result.files_included), result.total_records,
        result.completeness_score,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Subset export (date / route slices via gtfs-kit)
# ═══════════════════════════════════════════════════════════════════════════

def export_gtfs_subset(
    project_slug: str,
    *,
    dates: Optional[List[str]] = None,
    route_ids: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
) -> ExportResult:
    """Export a date- or route-restricted subset of the GTFS feed.

    Uses gtfs-kit's ``restrict_to_dates`` / ``restrict_to_routes`` on the
    Feed built from the project's SQLite DB, then writes a ``.zip``.

    Args:
        project_slug: project containing the GTFS database.
        dates: optional list of YYYYMMDD strings to keep.
        route_ids: optional list of route_ids to keep.
        output_dir: destination directory (defaults to project ``exports/``).

    Notes:
        - GTFS-ride extension tables (board_alight, ridership, etc.) are
          **not** included — gtfs-kit's Feed schema doesn't cover them.
        - Does **not** update ``exports/latest/`` — that mirror is
          reserved for full exports.
    """
    result = ExportResult()

    if not dates and not route_ids:
        result.errors.append("Provide at least one of `dates` or `route_ids`.")
        return result

    # Lazy import to avoid hard-coupling when gtfs-kit isn't installed
    try:
        from gtfs_kit_bridge import GTFS_KIT_AVAILABLE, feed_from_db
    except ImportError:
        result.errors.append("gtfs-kit bridge not available.")
        return result

    if not GTFS_KIT_AVAILABLE:
        result.errors.append(
            "gtfs-kit is not installed — run `uv sync` to enable subset exports."
        )
        return result

    feed = feed_from_db(project_slug)
    if feed is None:
        result.errors.append(
            "Feed could not be built. Ensure agency, stops, routes, and "
            "trips all have records."
        )
        return result

    # Apply restrictions in sequence
    try:
        if route_ids:
            feed = feed.restrict_to_routes(route_ids)
        if dates:
            feed = feed.restrict_to_dates(dates)
    except Exception as exc:
        result.errors.append(f"Failed to apply restrictions: {exc}")
        return result

    # Sanity-check the resulting feed has any trips left
    if feed.trips is None or feed.trips.empty:
        result.errors.append(
            "No trips remain after applying the filters. Check your date "
            "range and route selection."
        )
        return result

    # Determine output path
    if output_dir is None:
        export_root = PROJECTS_ROOT / project_slug / "exports"
    else:
        export_root = Path(output_dir)
    export_root.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_filename = f"gtfs_subset_{ts}.zip"
    zip_path = export_root / zip_filename

    try:
        feed.to_file(zip_path)
    except Exception as exc:
        result.errors.append(f"Failed to write subset .zip: {exc}")
        return result

    # Count records written (sum across core tables on the restricted feed)
    total = 0
    for name in ("agency", "stops", "routes", "trips", "stop_times",
                 "calendar", "calendar_dates", "shapes", "frequencies",
                 "transfers", "feed_info"):
        df = getattr(feed, name, None)
        if df is not None and not df.empty:
            total += len(df)
            result.files_included.append(f"{name}.txt")

    result.total_records = total
    result.success = True
    result.zip_path = str(zip_path)
    result.warnings.append(
        "GTFS-ride extension tables (board_alight, ridership, "
        "ride_feed_info, trip_capacity) are not included in subset exports."
    )

    logger.info(
        "GTFS subset exported to %s (%d files, %d records, "
        "dates=%s, routes=%s)",
        zip_path, len(result.files_included), total,
        f"{len(dates)} day(s)" if dates else "all",
        f"{len(route_ids)} route(s)" if route_ids else "all",
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Export history
# ═══════════════════════════════════════════════════════════════════════════

def list_exports(project_slug: str) -> List[Dict[str, Any]]:
    """List all previous GTFS exports for a project, newest first.

    Each entry contains: ``filename``, ``path``, ``size_bytes``, and
    ``created_at`` (ISO timestamp).  The ``latest/`` mirror directory is
    excluded.
    """
    export_root = PROJECTS_ROOT / project_slug / "exports"
    if not export_root.exists():
        return []

    exports: List[Dict[str, Any]] = []
    for p in export_root.glob("gtfs_*.zip"):
        if not p.is_file():
            continue
        stat = p.stat()
        exports.append({
            "filename": p.name,
            "path": str(p),
            "size_bytes": stat.st_size,
            "created_at": datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc,
            ).isoformat(),
            "is_subset": p.name.startswith("gtfs_subset_"),
        })

    exports.sort(key=lambda e: e["created_at"], reverse=True)
    return exports


# ═══════════════════════════════════════════════════════════════════════════
# Latest-export validation
# ═══════════════════════════════════════════════════════════════════════════

def latest_export_path(project_slug: str) -> Optional[Path]:
    """Return the path to the project's most recent GTFS export, or None.

    Prefers the stable ``exports/latest/gtfs.zip`` mirror that
    ``export_gtfs_feed`` maintains.  Falls back to the newest
    ``gtfs_*.zip`` if the mirror is missing.
    """
    export_root = PROJECTS_ROOT / project_slug / "exports"
    latest_mirror = export_root / "latest" / "gtfs.zip"
    if latest_mirror.exists():
        return latest_mirror
    if not export_root.exists():
        return None
    # Prefer full exports over subset exports for the "latest" pointer.
    candidates = sorted(
        (p for p in export_root.glob("gtfs_*.zip")
         if not p.name.startswith("gtfs_subset_")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def validate_latest_export(project_slug: str) -> Optional[GtfsValidationReport]:
    """Run ``gtfs_validator`` against the latest exported ZIP.

    Returns ``None`` if no export exists yet.  The caller is responsible
    for caching if invoked from a hot UI path.
    """
    zip_path = latest_export_path(project_slug)
    if zip_path is None:
        return None
    return validate_gtfs_feed(str(zip_path))


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _table_to_csv(
    conn: sqlite3.Connection,
    table_name: str,
    column_order: List[str],
) -> Optional[str]:
    """Query a GTFS table and format as a CSV string.

    Returns ``None`` if the table is empty.
    Only includes columns that have at least one non-null value.
    Formatting follows GTFS conventions:
      - UTF-8, CRLF line endings
      - Quote fields containing commas, quotes, or newlines
      - Empty optional fields written as empty string
    """
    try:
        cur = conn.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return None

    if not rows:
        return None

    # Determine which columns have at least one non-null value
    # (to avoid exporting columns that are entirely empty)
    row_dicts = [dict(r) for r in rows]
    active_columns = [
        col for col in column_order
        if any(rd.get(col) is not None for rd in row_dicts)
    ]

    if not active_columns:
        return None

    # Write CSV to string buffer
    buf = io.StringIO()
    writer = csv.writer(
        buf,
        lineterminator="\r\n",
        quoting=csv.QUOTE_MINIMAL,
    )
    # Header
    writer.writerow(active_columns)
    # Data rows
    for rd in row_dicts:
        writer.writerow([
            _format_value(rd.get(col)) for col in active_columns
        ])

    return buf.getvalue()


def _format_value(val: Any) -> str:
    """Format a single cell value for GTFS CSV output.

    - None → empty string
    - int/float that is whole → integer string (no decimals)
    - Everything else → str
    """
    if val is None:
        return ""
    if isinstance(val, float):
        if val == int(val):
            return str(int(val))
        return str(val)
    return str(val)


def _get_all_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    """Get row counts for all GTFS tables."""
    counts: Dict[str, int] = {}
    for tbl in _EXPORT_ORDER:
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {tbl}")
            counts[tbl] = cur.fetchone()[0]
        except sqlite3.OperationalError:
            counts[tbl] = 0
    return counts

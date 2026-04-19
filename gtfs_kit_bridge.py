"""
gtfs-kit bridge adapter.

Converts the project's SQLite GTFS database (or an exported ZIP) into a
``gtfs_kit.Feed`` object so the rest of the app can reuse gtfs-kit's
analytics, mapping, and health-indicator helpers without duplicating I/O.

Only core GTFS tables are mapped — GTFS-ride extension tables
(board_alight, ridership, ride_feed_info, trip_capacity) are not part of
the gtfs-kit Feed schema and are handled elsewhere in the app.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    import gtfs_kit as gk
    GTFS_KIT_AVAILABLE = True
except ImportError:
    gk = None
    GTFS_KIT_AVAILABLE = False

from gtfs_database import get_connection, get_gtfs_db_path

logger = logging.getLogger(__name__)

DEFAULT_DIST_UNITS = "km"

_CORE_GTFS_TABLES = (
    "agency", "stops", "routes", "trips", "stop_times",
    "calendar", "calendar_dates", "shapes", "frequencies",
    "transfers", "feed_info",
)


# ═══════════════════════════════════════════════════════════════════════════
# Core loaders
# ═══════════════════════════════════════════════════════════════════════════

def feed_from_db(
    project_slug: str,
    dist_units: str = DEFAULT_DIST_UNITS,
) -> Optional["gk.Feed"]:
    """Build a gtfs-kit Feed from the project's SQLite GTFS database.

    Returns ``None`` if gtfs-kit is not installed, the database does not
    exist, or the feed has no core records (agency/stops/routes/trips
    all empty).
    """
    if not GTFS_KIT_AVAILABLE:
        logger.warning("gtfs-kit not installed — feed_from_db returning None")
        return None

    db_path = get_gtfs_db_path(project_slug)
    if not db_path.exists():
        return None

    conn = get_connection(project_slug)
    try:
        frames: dict[str, Optional[pd.DataFrame]] = {}
        for tbl in _CORE_GTFS_TABLES:
            df = _read_table(conn, tbl)
            frames[tbl] = df if df is not None and not df.empty else None
    finally:
        conn.close()

    # Require at minimum agency, stops, routes, trips to be non-empty
    core_required = ("agency", "stops", "routes", "trips")
    if any(frames.get(t) is None for t in core_required):
        return None

    return gk.Feed(dist_units=dist_units, **frames)


def feed_from_zip(
    zip_path: str | Path,
    dist_units: str = DEFAULT_DIST_UNITS,
) -> Optional["gk.Feed"]:
    """Build a gtfs-kit Feed from an exported GTFS .zip file."""
    if not GTFS_KIT_AVAILABLE:
        return None
    try:
        return gk.read_feed(Path(zip_path), dist_units=dist_units)
    except Exception as exc:
        logger.error("Failed to read GTFS feed from %s: %s", zip_path, exc)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Cache key helpers
# ═══════════════════════════════════════════════════════════════════════════

def db_signature(project_slug: str) -> tuple[str, float] | None:
    """Cache key for a project's SQLite DB (path + mtime).

    Returns ``None`` if the database does not exist.
    """
    db_path = get_gtfs_db_path(project_slug)
    if not db_path.exists():
        return None
    return (str(db_path), db_path.stat().st_mtime)


# ═══════════════════════════════════════════════════════════════════════════
# Analytics helpers
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class FeedAnalytics:
    """Summary statistics computed from a gtfs-kit Feed."""
    available: bool = False
    reason: str = ""
    indicators: dict[str, Any] = field(default_factory=dict)
    bounds: Optional[tuple[float, float, float, float]] = None
    centroid: Optional[tuple[float, float]] = None
    busiest_date: Optional[str] = None
    busiest_date_trips: int = 0
    service_start: Optional[str] = None
    service_end: Optional[str] = None
    num_active_dates: int = 0
    route_stats: Optional[pd.DataFrame] = None


def compute_analytics(feed: Optional["gk.Feed"]) -> FeedAnalytics:
    """Compute a compact analytics bundle for the Streamlit UI.

    Fails softly — any individual stat that errors out is skipped; the
    caller receives whatever could be computed plus a populated ``reason``
    string explaining gaps.
    """
    result = FeedAnalytics()
    if feed is None:
        result.reason = "Feed unavailable (database empty or gtfs-kit not installed)"
        return result

    result.available = True

    # Indicators (gtfs-kit's "assess_quality")
    try:
        qdf = feed.assess_quality()
        result.indicators = dict(zip(qdf["indicator"], qdf["value"]))
    except Exception as exc:
        logger.warning("assess_quality failed: %s", exc)

    # Spatial bounds + centroid
    try:
        b = feed.compute_bounds()
        result.bounds = (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
    except Exception:
        pass
    try:
        c = feed.compute_centroid()
        result.centroid = (float(c.y), float(c.x))  # (lat, lon)
    except Exception:
        pass

    # Service span
    try:
        dates = feed.get_dates()
        if dates:
            result.service_start = dates[0]
            result.service_end = dates[-1]
            result.num_active_dates = len(dates)
    except Exception:
        pass

    # Busiest date
    try:
        bd = feed.compute_busiest_date(feed.get_dates() or [])
        if isinstance(bd, tuple):
            result.busiest_date = bd[0]
            result.busiest_date_trips = int(bd[1])
    except Exception:
        pass

    # Route stats for first service week (cap size for UI responsiveness)
    try:
        week = feed.get_first_week()
        if week:
            rs = feed.compute_route_stats(week[:1])  # single day keeps it cheap
            result.route_stats = rs
    except Exception as exc:
        logger.warning("compute_route_stats failed: %s", exc)

    return result


def build_routes_map(
    feed: Optional["gk.Feed"],
    route_ids: Optional[list[str]] = None,
    show_stops: bool = False,
) -> Any:
    """Return a Folium map of the given routes (or all routes), or None."""
    if feed is None:
        return None
    try:
        return feed.map_routes(route_ids=route_ids, show_stops=show_stops)
    except Exception as exc:
        logger.warning("map_routes failed: %s", exc)
        return None


def build_stops_map(
    feed: Optional["gk.Feed"],
    stop_ids: Optional[list[str]] = None,
) -> Any:
    """Return a Folium map of the given stops (or all stops), or None."""
    if feed is None:
        return None
    try:
        return feed.map_stops(stop_ids=stop_ids) if stop_ids else feed.map_stops(stop_ids=feed.stops["stop_id"].tolist())
    except Exception as exc:
        logger.warning("map_stops failed: %s", exc)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _read_table(conn: sqlite3.Connection, table: str) -> Optional[pd.DataFrame]:
    """Read a GTFS table into a DataFrame.  Returns None on error or empty."""
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    except Exception:
        return None

    if df.empty:
        return None

    # gtfs-kit expects numeric columns to be numeric (not object).
    # SQLite stores INTEGER / REAL natively, so pandas usually infers
    # correctly, but TEXT columns that are conceptually numeric (none in
    # GTFS core) would need coercion here.
    return df

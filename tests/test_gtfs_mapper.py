"""
Tests for gtfs_mapper.py — Silver to GTFS data mapper.

Uses sample CSV files from ``samples/`` as test inputs (they match the
Silver schemas but are in CSV format — the mapper can read both).
"""

import pytest
from pathlib import Path

from optisus.core.gtfs.mapper import (
    # ID generation
    generate_trip_id,
    generate_service_id,
    # Individual mappers
    map_stops,
    map_transfers,
    map_calendar_dates,
    map_routes,
    map_trips,
    map_stop_times,
    map_agency,
    map_trip_capacity,
    map_board_alight,
    # Orchestrator
    map_project_to_gtfs,
    # Helpers
    _to_yyyymmdd,
    _time_str_to_seconds,
    _seconds_to_time_str,
    MappingReport,
)
from optisus.core.gtfs.database import (
    create_gtfs_database,
    get_table_count,
    get_table_records,
)

SAMPLES = Path(__file__).parent.parent / "samples"


# ═══════════════════════════════════════════════════════════════════════════
# ID generation
# ═══════════════════════════════════════════════════════════════════════════

class TestIdGeneration:
    def test_trip_id_format(self):
        assert generate_trip_id("L001", 0, 1) == "L001_0_0001"
        assert generate_trip_id("L002", 1, 12) == "L002_1_0012"

    def test_trip_id_deterministic(self):
        a = generate_trip_id("L001", 0, 1)
        b = generate_trip_id("L001", 0, 1)
        assert a == b

    def test_service_id(self):
        assert generate_service_id("weekday") == "WEEKDAY"
        assert generate_service_id("Sunday Holiday") == "SUNDAY_HOLIDAY"


# ═══════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═══════════════════════════════════════════════════════════════════════════

class TestHelpers:
    def test_yyyymmdd_from_iso(self):
        assert _to_yyyymmdd("2024-01-15") == "20240115"

    def test_yyyymmdd_already_formatted(self):
        assert _to_yyyymmdd("20240115") == "20240115"

    def test_yyyymmdd_slash_format(self):
        assert _to_yyyymmdd("15/01/2024") == "20240115"

    def test_yyyymmdd_empty(self):
        assert _to_yyyymmdd("") is None
        assert _to_yyyymmdd("invalid") is None

    def test_time_to_seconds(self):
        assert _time_str_to_seconds("08:30:00") == 30600
        assert _time_str_to_seconds("00:00:00") == 0
        assert _time_str_to_seconds("25:00:00") == 90000

    def test_seconds_to_time(self):
        assert _seconds_to_time_str(30600) == "08:30:00"
        assert _seconds_to_time_str(0) == "00:00:00"


# ═══════════════════════════════════════════════════════════════════════════
# Individual mappers (using sample CSV data)
# ═══════════════════════════════════════════════════════════════════════════

class TestMapStops:
    def test_basic_mapping(self):
        records = map_stops(str(SAMPLES / "stop_spatial_features.csv"))
        assert len(records) > 0
        r0 = records[0]
        assert "stop_id" in r0
        assert "stop_lat" in r0
        assert "stop_lon" in r0
        assert r0["stop_id"] == "S001"

    def test_terminal_becomes_station(self):
        records = map_stops(str(SAMPLES / "stop_spatial_features.csv"))
        # S002 has is_terminal=True → location_type=1
        s002 = next(r for r in records if r["stop_id"] == "S002")
        assert s002["location_type"] == 1


class TestMapTransfers:
    def test_basic_mapping(self):
        records = map_transfers(str(SAMPLES / "stop_connections.csv"))
        assert len(records) > 0
        r0 = records[0]
        assert "from_stop_id" in r0
        assert "to_stop_id" in r0
        assert "transfer_type" in r0
        assert r0["from_stop_id"] == "S001"
        assert r0["to_stop_id"] == "S002"

    def test_deduplication(self):
        records = map_transfers(str(SAMPLES / "stop_connections.csv"))
        pairs = [(r["from_stop_id"], r["to_stop_id"]) for r in records]
        assert len(pairs) == len(set(pairs))


class TestMapCalendarDates:
    def test_basic_mapping(self):
        records = map_calendar_dates(str(SAMPLES / "calendar_events.csv"))
        assert len(records) > 0
        r0 = records[0]
        assert "service_id" in r0
        assert "date" in r0
        assert "exception_type" in r0

    def test_holiday_is_service_removed(self):
        records = map_calendar_dates(str(SAMPLES / "calendar_events.csv"))
        holidays = [r for r in records if r["date"] == "20240101"]
        assert len(holidays) == 1
        assert holidays[0]["exception_type"] == 2

    def test_date_format_yyyymmdd(self):
        records = map_calendar_dates(str(SAMPLES / "calendar_events.csv"))
        for r in records:
            assert len(r["date"]) == 8
            assert r["date"].isdigit()


class TestMapRoutes:
    def test_basic_mapping(self):
        records = map_routes(str(SAMPLES / "operations_and_circulation.csv"))
        assert len(records) > 0
        r0 = records[0]
        assert "route_id" in r0
        assert "route_type" in r0
        assert r0["route_type"] == 3  # bus

    def test_deduplicated_by_line(self):
        records = map_routes(str(SAMPLES / "operations_and_circulation.csv"))
        ids = [r["route_id"] for r in records]
        assert len(ids) == len(set(ids))


class TestMapTrips:
    def test_basic_mapping(self):
        trips = map_trips(str(SAMPLES / "operations_and_circulation.csv"))
        assert len(trips) > 0
        t0 = trips[0]
        assert "trip_id" in t0
        assert "route_id" in t0
        assert "service_id" in t0

    def test_generated_trip_ids(self):
        trips = map_trips(str(SAMPLES / "operations_and_circulation.csv"))
        ids = [t["trip_id"] for t in trips]
        assert len(ids) == len(set(ids))  # all unique


class TestMapStopTimes:
    def test_basic_mapping(self):
        trips = map_trips(str(SAMPLES / "operations_and_circulation.csv"))
        records = map_stop_times(str(SAMPLES / "operations_and_circulation.csv"), trips)
        assert len(records) > 0
        r0 = records[0]
        assert "trip_id" in r0
        assert "arrival_time" in r0
        assert "departure_time" in r0
        assert "stop_id" in r0
        assert "stop_sequence" in r0

    def test_time_format_valid(self):
        trips = map_trips(str(SAMPLES / "operations_and_circulation.csv"))
        records = map_stop_times(str(SAMPLES / "operations_and_circulation.csv"), trips)
        import re
        for r in records[:10]:
            assert re.match(r"^\d{2}:\d{2}:\d{2}$", r["arrival_time"])
            assert re.match(r"^\d{2}:\d{2}:\d{2}$", r["departure_time"])


class TestMapAgency:
    def test_basic_mapping(self):
        records = map_agency(str(SAMPLES / "fleet_identification.csv"))
        assert len(records) >= 1
        r0 = records[0]
        assert "agency_id" in r0
        assert "agency_name" in r0
        assert "agency_url" in r0
        assert "agency_timezone" in r0


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator (end-to-end with temp database)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def isolated_project(tmp_path, monkeypatch):
    """Set up an isolated project with sample data as Silver datasets."""
    from optisus.core.gtfs import database as gtfs_database
    from optisus.core.gtfs import mapper as gtfs_mapper  # noqa: F401
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", tmp_path / "projects")
    # Also need to patch the storage_layers import inside gtfs_mapper
    # so get_project_silver_datasets works
    project_dir = tmp_path / "projects" / "test_project"
    project_dir.mkdir(parents=True, exist_ok=True)
    return "test_project"


class TestOrchestrator:
    def test_with_explicit_datasets(self, isolated_project):
        slug = isolated_project
        datasets = {
            "Stop Spatial Features": str(SAMPLES / "stop_spatial_features.csv"),
            "Operations and Circulation": str(SAMPLES / "operations_and_circulation.csv"),
            "Stop Connections": str(SAMPLES / "stop_connections.csv"),
            "Calendar Events": str(SAMPLES / "calendar_events.csv"),
            "Fleet Identification": str(SAMPLES / "fleet_identification.csv"),
        }
        report = map_project_to_gtfs(slug, available_datasets=datasets)
        assert report.total_mapped > 0
        assert report.total_failed == 0

        # Check that the database was populated
        assert get_table_count(slug, "stops") > 0
        assert get_table_count(slug, "routes") > 0
        assert get_table_count(slug, "trips") > 0
        assert get_table_count(slug, "stop_times") > 0
        assert get_table_count(slug, "agency") > 0

    def test_partial_datasets(self, isolated_project):
        slug = isolated_project
        datasets = {
            "Stop Spatial Features": str(SAMPLES / "stop_spatial_features.csv"),
        }
        report = map_project_to_gtfs(slug, available_datasets=datasets)
        assert report.total_mapped > 0
        assert get_table_count(slug, "stops") > 0
        assert get_table_count(slug, "routes") == 0
        assert "routes" in report.unmapped_tables

    def test_empty_datasets(self, isolated_project):
        slug = isolated_project
        report = map_project_to_gtfs(slug, available_datasets={})
        assert report.total_mapped == 0
        assert len(report.unmapped_tables) == len(report.unmapped_tables)

    def test_idempotent_mapping(self, isolated_project):
        """Running the mapper twice should upsert, not duplicate."""
        slug = isolated_project
        datasets = {
            "Stop Spatial Features": str(SAMPLES / "stop_spatial_features.csv"),
            "Operations and Circulation": str(SAMPLES / "operations_and_circulation.csv"),
        }
        map_project_to_gtfs(slug, available_datasets=datasets)
        count1 = get_table_count(slug, "stops")

        map_project_to_gtfs(slug, available_datasets=datasets)
        count2 = get_table_count(slug, "stops")

        assert count1 == count2  # upsert, not duplicate

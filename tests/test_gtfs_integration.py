"""
End-to-end integration tests for the GTFS pipeline.

Chains together: sample CSVs → Silver Parquet → GTFS mapper →
GTFS SQLite database → GTFS .zip export → feed validation.
"""

import csv
import io
import re
import sqlite3
import zipfile
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from optisus.core.gtfs.database import (
    check_integrity,
    create_gtfs_database,
    get_connection,
    get_table_count,
    upsert_records,
)
from optisus.core.gtfs.exporter import (
    export_gtfs_feed,
    list_exports,
    validate_before_export,
)
from optisus.core.gtfs.mapper import map_project_to_gtfs
from optisus.core.gtfs.validator import validate_gtfs_feed
from optisus.core.storage.layers import create_project


SAMPLES = Path(__file__).parent.parent / "samples"


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def isolated_data_lake(tmp_path, monkeypatch):
    """Redirect all storage roots to a temp directory."""
    from optisus.core.gtfs import database as gtfs_database
    from optisus.core.gtfs import exporter as gtfs_exporter
    from optisus.core.storage import layers as storage_layers

    fake_root = tmp_path / "data_lake_outputs"
    fake_projects = fake_root / "projects"
    fake_projects.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage_layers, "DATA_LAKE_ROOT", fake_root)
    monkeypatch.setattr(storage_layers, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(gtfs_exporter, "PROJECTS_ROOT", fake_projects)
    return fake_projects


def _all_sample_datasets() -> Dict[str, str]:
    """Return a Silver-dataset dict wired to the sample CSVs."""
    return {
        "Stop Spatial Features":      str(SAMPLES / "stop_spatial_features.csv"),
        "Operations and Circulation": str(SAMPLES / "operations_and_circulation.csv"),
        "Stop Connections":           str(SAMPLES / "stop_connections.csv"),
        "Calendar Events":            str(SAMPLES / "calendar_events.csv"),
        "Fleet Identification":       str(SAMPLES / "fleet_identification.csv"),
        "Transported Passengers":     str(SAMPLES / "transported_passengers.csv"),
    }


def _seed_calendar_row(slug: str, service_id: str = "WEEKDAY") -> None:
    """Insert a calendar.txt row so exports and FK checks pass cleanly."""
    upsert_records(slug, "calendar", [{
        "service_id": service_id,
        "monday": 1, "tuesday": 1, "wednesday": 1,
        "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
        "start_date": "20260101", "end_date": "20261231",
    }])


# ═══════════════════════════════════════════════════════════════════════════
# Test 1 — Full pipeline happy path
# ═══════════════════════════════════════════════════════════════════════════

class TestFullPipelineHappyPath:
    def test_end_to_end(self, isolated_data_lake, tmp_path):
        # 1. Create project
        create_project("Integration")
        slug = "integration"

        # 2-4. Map Silver → GTFS database (mapper also creates the DB)
        report = map_project_to_gtfs(slug, available_datasets=_all_sample_datasets())
        assert report.total_mapped > 0
        assert report.total_failed == 0

        # Seed calendar so FK to trips.service_id is unambiguous
        _seed_calendar_row(slug)

        # 5. Integrity check — clean
        integrity = check_integrity(slug)
        assert integrity.is_clean, f"violations: {integrity.violations}"

        # Required tables populated
        for tbl in ("agency", "stops", "routes", "trips", "stop_times"):
            assert get_table_count(slug, tbl) > 0, f"{tbl} is empty"

        # 6. Export to .zip
        export = export_gtfs_feed(slug)
        assert export.success, f"errors: {export.errors}"
        assert export.zip_path and Path(export.zip_path).exists()

        # 7. Validate feed
        vr = validate_gtfs_feed(export.zip_path)
        # All required files present; no FK errors
        assert vr.is_valid, (
            f"errors: {[i.message for i in vr.issues if i.severity == 'error']}"
        )

        # 8. Required files all present in the zip
        with zipfile.ZipFile(export.zip_path, "r") as zf:
            names = set(zf.namelist())
        for req in ("agency.txt", "stops.txt", "routes.txt",
                    "trips.txt", "stop_times.txt"):
            assert req in names, f"{req} missing from zip"


# ═══════════════════════════════════════════════════════════════════════════
# Test 2 — Partial data: graceful degradation
# ═══════════════════════════════════════════════════════════════════════════

class TestPartialDataExport:
    def test_missing_required_blocks_export(self, isolated_data_lake):
        slug = "partial"
        create_gtfs_database(slug)

        # Only stops + routes — missing agency, trips, stop_times, calendar
        upsert_records(slug, "stops", [
            {"stop_id": "S1", "stop_name": "A", "stop_lat": 41.0, "stop_lon": -8.0},
            {"stop_id": "S2", "stop_name": "B", "stop_lat": 41.1, "stop_lon": -8.1},
        ])
        upsert_records(slug, "routes", [
            {"route_id": "R1", "route_short_name": "Line 1", "route_type": 3},
        ])

        vr = validate_before_export(slug)
        assert not vr.can_export
        assert len(vr.errors) > 0

        er = export_gtfs_feed(slug)
        assert not er.success
        assert er.zip_path is None

    def test_complete_after_adding_missing_tables(self, isolated_data_lake):
        slug = "partial_then_full"
        create_gtfs_database(slug)

        upsert_records(slug, "agency", [
            {"agency_id": "A1", "agency_name": "Test",
             "agency_url": "https://t.example", "agency_timezone": "UTC"},
        ])
        upsert_records(slug, "stops", [
            {"stop_id": "S1", "stop_name": "A", "stop_lat": 41.0, "stop_lon": -8.0},
            {"stop_id": "S2", "stop_name": "B", "stop_lat": 41.1, "stop_lon": -8.1},
        ])
        upsert_records(slug, "routes", [
            {"route_id": "R1", "agency_id": "A1",
             "route_short_name": "Line 1", "route_type": 3},
        ])
        _seed_calendar_row(slug, "WD")
        upsert_records(slug, "trips", [
            {"route_id": "R1", "service_id": "WD",
             "trip_id": "T1", "direction_id": 0},
        ])
        upsert_records(slug, "stop_times", [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
             "arrival_time": "08:00:00", "departure_time": "08:01:00"},
            {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 1,
             "arrival_time": "08:10:00", "departure_time": "08:11:00"},
        ])

        er = export_gtfs_feed(slug)
        assert er.success
        assert Path(er.zip_path).exists()

        # Exported zip only contains tables with data
        with zipfile.ZipFile(er.zip_path, "r") as zf:
            names = set(zf.namelist())
        assert "agency.txt" in names
        assert "stops.txt" in names
        assert "routes.txt" in names
        assert "trips.txt" in names
        assert "stop_times.txt" in names
        assert "calendar.txt" in names
        # Empty tables excluded
        assert "shapes.txt" not in names
        assert "frequencies.txt" not in names


# ═══════════════════════════════════════════════════════════════════════════
# Test 3 — Mapping idempotency
# ═══════════════════════════════════════════════════════════════════════════

class TestMappingIdempotency:
    def test_second_mapping_same_counts_and_ids(self, isolated_data_lake):
        slug = "idempotent"
        datasets = _all_sample_datasets()

        map_project_to_gtfs(slug, available_datasets=datasets)
        counts_1 = {
            t: get_table_count(slug, t)
            for t in ("stops", "routes", "trips", "stop_times", "agency")
        }

        # Capture trip IDs after first run
        conn = get_connection(slug)
        trip_ids_1 = {r["trip_id"] for r in conn.execute("SELECT trip_id FROM trips")}
        conn.close()

        map_project_to_gtfs(slug, available_datasets=datasets)
        counts_2 = {
            t: get_table_count(slug, t)
            for t in ("stops", "routes", "trips", "stop_times", "agency")
        }
        conn = get_connection(slug)
        trip_ids_2 = {r["trip_id"] for r in conn.execute("SELECT trip_id FROM trips")}
        conn.close()

        assert counts_1 == counts_2, "mapping is not idempotent (row counts differ)"
        assert trip_ids_1 == trip_ids_2, "trip IDs changed across mapper runs"


# ═══════════════════════════════════════════════════════════════════════════
# Test 4 — Integrity violations detected
# ═══════════════════════════════════════════════════════════════════════════

class TestIntegrityViolations:
    def test_orphan_route_id_reported(self, isolated_data_lake):
        slug = "integrity"
        create_gtfs_database(slug)

        # Seed a minimum real feed so most FKs are valid
        upsert_records(slug, "agency", [
            {"agency_id": "A1", "agency_name": "T",
             "agency_url": "https://t.example", "agency_timezone": "UTC"},
        ])
        upsert_records(slug, "stops", [
            {"stop_id": "S1", "stop_name": "A", "stop_lat": 41.0, "stop_lon": -8.0},
        ])
        upsert_records(slug, "routes", [
            {"route_id": "R1", "agency_id": "A1",
             "route_short_name": "Line", "route_type": 3},
        ])
        _seed_calendar_row(slug)
        upsert_records(slug, "trips", [
            {"route_id": "R1", "service_id": "WEEKDAY",
             "trip_id": "T1", "direction_id": 0},
        ])

        # Force an orphaned FK by bypassing foreign-key enforcement
        from optisus.core.gtfs.database import get_gtfs_db_path
        conn = sqlite3.connect(str(get_gtfs_db_path(slug)))
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO trips (route_id, service_id, trip_id, direction_id) "
            "VALUES (?, ?, ?, ?)",
            ("GHOST_ROUTE", "WEEKDAY", "T_BAD", 0),
        )
        conn.commit()
        conn.close()

        report = check_integrity(slug)
        assert not report.is_clean
        ghost = [v for v in report.violations if v.record_id == "GHOST_ROUTE"]
        assert ghost, "orphaned route_id was not reported"
        assert ghost[0].violation_type == "orphaned_fk"
        assert ghost[0].table == "trips"


# ═══════════════════════════════════════════════════════════════════════════
# Test 5 — Export format compliance
# ═══════════════════════════════════════════════════════════════════════════

class TestExportFormatCompliance:
    @pytest.fixture()
    def exported_zip(self, isolated_data_lake) -> str:
        slug = "format_check"
        map_project_to_gtfs(slug, available_datasets=_all_sample_datasets())
        _seed_calendar_row(slug)
        er = export_gtfs_feed(slug)
        assert er.success
        return er.zip_path

    def test_all_files_utf8(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            for name in zf.namelist():
                zf.read(name).decode("utf-8")  # must not raise

    def test_headers_present(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                reader = csv.reader(io.StringIO(content))
                header = next(reader, None)
                assert header, f"{name} is empty / has no header"
                assert all(h.strip() for h in header), (
                    f"{name} header has blank columns: {header}"
                )

    def test_no_none_or_nan_strings(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                # These literal tokens should never appear in a GTFS CSV
                assert "None" not in content, f"{name} contains 'None'"
                assert "NaN" not in content, f"{name} contains 'NaN'"

    def test_time_format_hhmmss(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            if "stop_times.txt" not in zf.namelist():
                pytest.skip("stop_times.txt not in feed")
            content = zf.read("stop_times.txt").decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            time_re = re.compile(r"^\d{2}:\d{2}:\d{2}$")
            for row in reader:
                for col in ("arrival_time", "departure_time"):
                    v = row.get(col)
                    if v:
                        assert time_re.match(v), f"{col} '{v}' not HH:MM:SS"

    def test_date_format_yyyymmdd(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            date_re = re.compile(r"^\d{8}$")
            for fname, cols in (
                ("calendar.txt", ("start_date", "end_date")),
                ("calendar_dates.txt", ("date",)),
            ):
                if fname not in zf.namelist():
                    continue
                content = zf.read(fname).decode("utf-8")
                reader = csv.DictReader(io.StringIO(content))
                for row in reader:
                    for col in cols:
                        v = row.get(col)
                        if v:
                            assert date_re.match(v), (
                                f"{fname} {col} '{v}' not YYYYMMDD"
                            )

    def test_no_trailing_whitespace_in_headers(self, exported_zip):
        with zipfile.ZipFile(exported_zip, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                header_line = content.split("\r\n", 1)[0]
                assert header_line == header_line.strip(), (
                    f"{name} header has leading/trailing whitespace"
                )

    def test_validator_passes_on_generated_feed(self, exported_zip):
        vr = validate_gtfs_feed(exported_zip)
        errors = [i for i in vr.issues if i.severity == "error"]
        assert vr.is_valid, f"validator errors: {[e.message for e in errors]}"


# ═══════════════════════════════════════════════════════════════════════════
# Bonus: validator correctly flags a malformed feed
# ═══════════════════════════════════════════════════════════════════════════

class TestValidatorDetectsProblems:
    def test_missing_required_file(self, tmp_path):
        zip_path = tmp_path / "bad.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            # Only stops.txt, nothing else
            zf.writestr("stops.txt",
                        "stop_id,stop_name,stop_lat,stop_lon\r\n"
                        "S1,A,41.0,-8.0\r\n")

        vr = validate_gtfs_feed(str(zip_path))
        assert not vr.is_valid
        missing_msgs = [i.message for i in vr.issues
                        if "missing from feed" in i.message]
        assert any("agency.txt" in m for m in missing_msgs)
        assert any("routes.txt" in m for m in missing_msgs)

    def test_broken_foreign_key(self, tmp_path):
        zip_path = tmp_path / "fk.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt",
                        "agency_id,agency_name,agency_url,agency_timezone\r\n"
                        "A1,T,https://t.example,UTC\r\n")
            zf.writestr("stops.txt",
                        "stop_id,stop_name,stop_lat,stop_lon\r\n"
                        "S1,A,41.0,-8.0\r\n")
            zf.writestr("routes.txt",
                        "route_id,agency_id,route_short_name,route_type\r\n"
                        "R1,A1,Line,3\r\n")
            zf.writestr("calendar.txt",
                        "service_id,monday,tuesday,wednesday,thursday,"
                        "friday,saturday,sunday,start_date,end_date\r\n"
                        "WD,1,1,1,1,1,0,0,20260101,20261231\r\n")
            zf.writestr("trips.txt",
                        "route_id,service_id,trip_id\r\n"
                        "GHOST,WD,T1\r\n")
            zf.writestr("stop_times.txt",
                        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\r\n"
                        "T1,08:00:00,08:01:00,S1,0\r\n")

        vr = validate_gtfs_feed(str(zip_path))
        assert not vr.is_valid
        assert any("GHOST" in i.message and i.file == "trips.txt"
                   for i in vr.issues)

    def test_bad_coordinates(self, tmp_path):
        zip_path = tmp_path / "geo.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt",
                        "agency_id,agency_name,agency_url,agency_timezone\r\n"
                        "A1,T,https://t.example,UTC\r\n")
            zf.writestr("stops.txt",
                        "stop_id,stop_name,stop_lat,stop_lon\r\n"
                        "S1,A,999,0\r\n")
            zf.writestr("routes.txt",
                        "route_id,agency_id,route_short_name,route_type\r\n"
                        "R1,A1,Line,3\r\n")
            zf.writestr("calendar.txt",
                        "service_id,monday,tuesday,wednesday,thursday,"
                        "friday,saturday,sunday,start_date,end_date\r\n"
                        "WD,1,1,1,1,1,0,0,20260101,20261231\r\n")
            zf.writestr("trips.txt",
                        "route_id,service_id,trip_id\r\n"
                        "R1,WD,T1\r\n")
            zf.writestr("stop_times.txt",
                        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\r\n"
                        "T1,08:00:00,08:01:00,S1,0\r\n")

        vr = validate_gtfs_feed(str(zip_path))
        bad_lat = [i for i in vr.issues if i.field == "stop_lat"]
        assert bad_lat, "out-of-range stop_lat was not flagged"
        assert bad_lat[0].severity == "error"

    def test_calendar_date_reversal(self, tmp_path):
        zip_path = tmp_path / "cal.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt",
                        "agency_id,agency_name,agency_url,agency_timezone\r\n"
                        "A1,T,https://t.example,UTC\r\n")
            zf.writestr("stops.txt",
                        "stop_id,stop_name,stop_lat,stop_lon\r\n"
                        "S1,A,41.0,-8.0\r\n")
            zf.writestr("routes.txt",
                        "route_id,agency_id,route_short_name,route_type\r\n"
                        "R1,A1,Line,3\r\n")
            zf.writestr("calendar.txt",
                        "service_id,monday,tuesday,wednesday,thursday,"
                        "friday,saturday,sunday,start_date,end_date\r\n"
                        "WD,1,1,1,1,1,0,0,20261231,20260101\r\n")
            zf.writestr("trips.txt",
                        "route_id,service_id,trip_id\r\n"
                        "R1,WD,T1\r\n")
            zf.writestr("stop_times.txt",
                        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\r\n"
                        "T1,08:00:00,08:01:00,S1,0\r\n")

        vr = validate_gtfs_feed(str(zip_path))
        assert not vr.is_valid
        assert any("after end_date" in i.message for i in vr.issues)

    def test_clean_minimal_feed(self, tmp_path):
        zip_path = tmp_path / "ok.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt",
                        "agency_id,agency_name,agency_url,agency_timezone\r\n"
                        "A1,T,https://t.example,UTC\r\n")
            zf.writestr("stops.txt",
                        "stop_id,stop_name,stop_lat,stop_lon\r\n"
                        "S1,A,41.0,-8.0\r\n"
                        "S2,B,41.1,-8.1\r\n")
            zf.writestr("routes.txt",
                        "route_id,agency_id,route_short_name,route_type\r\n"
                        "R1,A1,Line,3\r\n")
            zf.writestr("calendar.txt",
                        "service_id,monday,tuesday,wednesday,thursday,"
                        "friday,saturday,sunday,start_date,end_date\r\n"
                        "WD,1,1,1,1,1,0,0,20260101,20261231\r\n")
            zf.writestr("trips.txt",
                        "route_id,service_id,trip_id\r\n"
                        "R1,WD,T1\r\n")
            zf.writestr("stop_times.txt",
                        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\r\n"
                        "T1,08:00:00,08:01:00,S1,0\r\n"
                        "T1,08:10:00,08:11:00,S2,1\r\n")
        vr = validate_gtfs_feed(str(zip_path))
        assert vr.is_valid, f"errors: {[i.message for i in vr.issues]}"
        assert vr.error_count == 0

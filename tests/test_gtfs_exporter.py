"""
Tests for gtfs_exporter.py — GTFS feed export, validation, and scoring.

Covers:
  - Pre-export validation (missing required tables → error)
  - Export produces a valid .zip with properly formatted .txt files
  - Empty optional tables are excluded from the .zip
  - Completeness score calculation
  - CSV formatting rules (encoding, line endings, quoting)
"""

import csv
import io
import zipfile
from pathlib import Path

import pytest

from optisus.core.gtfs.database import create_gtfs_database, upsert_records, get_connection
from optisus.core.gtfs.exporter import (
    validate_before_export,
    compute_feed_completeness,
    export_gtfs_feed,
    ExportResult,
    ValidationResult,
    FeedCompleteness,
)


@pytest.fixture()
def isolated_gtfs(tmp_path, monkeypatch):
    """Redirect PROJECTS_ROOT to temp dir and create a test project."""
    from optisus.core.gtfs import database as gtfs_database
    from optisus.core.gtfs import exporter as gtfs_exporter
    projects = tmp_path / "projects"
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", projects)
    monkeypatch.setattr(gtfs_exporter, "PROJECTS_ROOT", projects)
    project_dir = projects / "test_project"
    project_dir.mkdir(parents=True, exist_ok=True)
    return "test_project"


def _seed_minimum_feed(slug: str) -> None:
    """Populate the GTFS database with the minimum viable feed."""
    create_gtfs_database(slug)
    upsert_records(slug, "agency", [
        {"agency_id": "A1", "agency_name": "Test Agency",
         "agency_url": "https://test.com", "agency_timezone": "UTC"},
    ])
    upsert_records(slug, "stops", [
        {"stop_id": "S1", "stop_name": "Stop 1", "stop_lat": 41.15, "stop_lon": -8.61},
        {"stop_id": "S2", "stop_name": "Stop 2", "stop_lat": 41.16, "stop_lon": -8.62},
    ])
    upsert_records(slug, "routes", [
        {"route_id": "R1", "agency_id": "A1", "route_type": 3, "route_short_name": "Line 1"},
    ])
    upsert_records(slug, "calendar", [
        {"service_id": "WD", "monday": 1, "tuesday": 1, "wednesday": 1,
         "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
         "start_date": "20260101", "end_date": "20261231"},
    ])
    upsert_records(slug, "trips", [
        {"route_id": "R1", "service_id": "WD", "trip_id": "T1", "direction_id": 0},
    ])
    upsert_records(slug, "stop_times", [
        {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
         "arrival_time": "08:00:00", "departure_time": "08:01:00"},
        {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 1,
         "arrival_time": "08:10:00", "departure_time": "08:11:00"},
    ])


# ═══════════════════════════════════════════════════════════════════════════
# Pre-export validation
# ═══════════════════════════════════════════════════════════════════════════

class TestValidation:
    def test_no_database(self, isolated_gtfs):
        vr = validate_before_export(isolated_gtfs)
        assert not vr.can_export
        assert any("does not exist" in e for e in vr.errors)

    def test_empty_database(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        vr = validate_before_export(isolated_gtfs)
        assert not vr.can_export
        assert len(vr.errors) > 0

    def test_minimum_feed_passes(self, isolated_gtfs):
        _seed_minimum_feed(isolated_gtfs)
        vr = validate_before_export(isolated_gtfs)
        assert vr.can_export
        assert len(vr.errors) == 0

    def test_missing_stops(self, isolated_gtfs):
        """Remove a required table after seeding."""
        _seed_minimum_feed(isolated_gtfs)
        conn = get_connection(isolated_gtfs)
        conn.execute("DELETE FROM stop_times")
        conn.execute("DELETE FROM stops")
        conn.commit()
        conn.close()
        vr = validate_before_export(isolated_gtfs)
        assert not vr.can_export
        assert any("stops" in e for e in vr.errors)

    def test_warns_on_missing_recommended(self, isolated_gtfs):
        _seed_minimum_feed(isolated_gtfs)
        vr = validate_before_export(isolated_gtfs)
        # feed_info and shapes should trigger warnings
        assert len(vr.warnings) >= 1
        assert any("feed_info" in w for w in vr.warnings)


# ═══════════════════════════════════════════════════════════════════════════
# Completeness score
# ═══════════════════════════════════════════════════════════════════════════

class TestCompleteness:
    def test_empty_database_score_zero(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        fc = compute_feed_completeness(isolated_gtfs)
        assert fc.score == 0.0

    def test_minimum_feed_score(self, isolated_gtfs):
        _seed_minimum_feed(isolated_gtfs)
        fc = compute_feed_completeness(isolated_gtfs)
        # All required tables populated → 60 points
        assert fc.score >= 60.0
        assert fc.score < 100.0  # no recommended/optional

    def test_breakdown_has_all_tables(self, isolated_gtfs):
        _seed_minimum_feed(isolated_gtfs)
        fc = compute_feed_completeness(isolated_gtfs)
        # Should have entries for required, recommended, and optional
        assert "agency" in fc.breakdown
        assert "feed_info" in fc.breakdown
        assert "transfers" in fc.breakdown

    def test_nonexistent_db(self, isolated_gtfs):
        fc = compute_feed_completeness(isolated_gtfs)
        assert fc.score == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Export
# ═══════════════════════════════════════════════════════════════════════════

class TestExport:
    def test_export_minimum_feed(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        assert er.success
        assert er.zip_path is not None
        assert Path(er.zip_path).exists()
        assert er.total_records > 0
        assert "agency.txt" in er.files_included
        assert "stops.txt" in er.files_included
        assert "routes.txt" in er.files_included
        assert "trips.txt" in er.files_included
        assert "stop_times.txt" in er.files_included
        assert "calendar.txt" in er.files_included

    def test_export_excludes_empty_tables(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        # shapes and frequencies were never populated
        assert "shapes.txt" not in er.files_included
        assert "frequencies.txt" not in er.files_included

    def test_zip_contents_are_valid_csv(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        with zipfile.ZipFile(er.zip_path, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                reader = csv.reader(io.StringIO(content))
                header = next(reader)
                assert len(header) > 0, f"{name} has empty header"
                rows = list(reader)
                assert len(rows) > 0, f"{name} has no data rows"

    def test_csv_uses_crlf_line_endings(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        with zipfile.ZipFile(er.zip_path, "r") as zf:
            content = zf.read("agency.txt").decode("utf-8")
            assert "\r\n" in content

    def test_no_none_strings_in_csv(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        with zipfile.ZipFile(er.zip_path, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                assert "None" not in content, f"{name} contains 'None' string"
                assert "null" not in content.lower().split(","), f"{name} contains 'null'"

    def test_latest_copy(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        latest = tmp_path / "out" / "latest" / "gtfs.zip"
        assert latest.exists()

    def test_export_fails_without_required(self, isolated_gtfs, tmp_path):
        create_gtfs_database(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        assert not er.success
        assert len(er.errors) > 0
        assert er.zip_path is None

    def test_exclude_ride_tables(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        # Add a GTFS-ride record
        upsert_records(isolated_gtfs, "board_alight", [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
             "record_use": 0, "boardings": 10, "alightings": 5},
        ])
        # Export without ride
        er = export_gtfs_feed(
            isolated_gtfs, output_dir=str(tmp_path / "out"),
            include_ride=False,
        )
        assert er.success
        assert "board_alight.txt" not in er.files_included

    def test_include_ride_tables(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        upsert_records(isolated_gtfs, "board_alight", [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
             "record_use": 0, "boardings": 10, "alightings": 5},
        ])
        er = export_gtfs_feed(
            isolated_gtfs, output_dir=str(tmp_path / "out"),
            include_ride=True,
        )
        assert er.success
        assert "board_alight.txt" in er.files_included

    def test_completeness_in_result(self, isolated_gtfs, tmp_path):
        _seed_minimum_feed(isolated_gtfs)
        er = export_gtfs_feed(isolated_gtfs, output_dir=str(tmp_path / "out"))
        assert er.completeness_score >= 60.0

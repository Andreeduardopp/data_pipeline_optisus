"""
Tests for gtfs_database.py — SQLite GTFS database layer.

Covers:
  - Database creation is idempotent
  - Inserting valid records succeeds
  - Inserting invalid data fails gracefully (Pydantic validation)
  - FK violations are detected by integrity check
  - CRUD: read, count, delete, clear
  - Database summary
"""

import os
import pytest
from pathlib import Path
from unittest.mock import patch

from optisus.core.gtfs.database import (
    create_gtfs_database,
    get_gtfs_db_path,
    get_connection,
    database_exists,
    upsert_records,
    get_table_records,
    get_table_count,
    delete_records,
    clear_table,
    check_integrity,
    get_database_summary,
    get_table_columns,
    InsertResult,
    IntegrityReport,
    PROJECTS_ROOT,
)


@pytest.fixture()
def isolated_gtfs_db(tmp_path, monkeypatch):
    """Redirect PROJECTS_ROOT to a temp directory and create a test project."""
    from optisus.core.gtfs import database as gtfs_database
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", tmp_path / "projects")
    # Also patch storage_layers import used inside gtfs_database
    project_dir = tmp_path / "projects" / "test_project"
    project_dir.mkdir(parents=True, exist_ok=True)
    return "test_project"


# ═══════════════════════════════════════════════════════════════════════════
# Database lifecycle
# ═══════════════════════════════════════════════════════════════════════════

class TestCreateDatabase:
    def test_creates_db_file(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        db_path = create_gtfs_database(slug)
        assert db_path.exists()
        assert db_path.name == "gtfs.db"

    def test_idempotent(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        p1 = create_gtfs_database(slug)
        p2 = create_gtfs_database(slug)
        assert p1 == p2
        assert p1.exists()

    def test_meta_table_populated(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        conn = get_connection(slug)
        try:
            cur = conn.execute("SELECT key, value FROM _gtfs_meta")
            meta = {r["key"]: r["value"] for r in cur.fetchall()}
            assert "schema_version" in meta
            assert "created_at" in meta
            assert "last_modified" in meta
        finally:
            conn.close()

    def test_database_exists(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        assert not database_exists(slug)
        create_gtfs_database(slug)
        assert database_exists(slug)


class TestForeignKeysEnabled:
    def test_pragma_fk_on(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        conn = get_connection(slug)
        try:
            cur = conn.execute("PRAGMA foreign_keys")
            assert cur.fetchone()[0] == 1
        finally:
            conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# CRUD — upsert
# ═══════════════════════════════════════════════════════════════════════════

class TestUpsertRecords:
    def test_insert_valid_agency(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        result = upsert_records(slug, "agency", [
            {
                "agency_id": "MDP",
                "agency_name": "Metro do Porto",
                "agency_url": "https://www.metrodoporto.pt",
                "agency_timezone": "Europe/Lisbon",
            }
        ])
        assert result.inserted == 1
        assert result.failed == 0
        assert get_table_count(slug, "agency") == 1

    def test_insert_valid_stops(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        result = upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_name": "Central", "stop_lat": 41.15, "stop_lon": -8.61},
            {"stop_id": "S002", "stop_name": "Market", "stop_lat": 41.14, "stop_lon": -8.62},
        ])
        assert result.inserted == 2
        assert result.failed == 0
        assert get_table_count(slug, "stops") == 2

    def test_upsert_replaces_existing(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_name": "Old Name", "stop_lat": 41.15, "stop_lon": -8.61},
        ])
        upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_name": "New Name", "stop_lat": 41.15, "stop_lon": -8.61},
        ])
        records = get_table_records(slug, "stops")
        assert len(records) == 1
        assert records[0]["stop_name"] == "New Name"

    def test_invalid_record_fails(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        result = upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_lat": 91.0, "stop_lon": 0.0},  # lat out of range
        ])
        assert result.failed == 1
        assert result.inserted == 0
        assert len(result.errors) == 1

    def test_mixed_valid_invalid(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        result = upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_name": "Good", "stop_lat": 41.15, "stop_lon": -8.61},
            {"stop_id": "S002", "stop_lat": 91.0, "stop_lon": 0.0},  # invalid
            {"stop_id": "S003", "stop_name": "Also Good"},
        ])
        assert result.inserted == 2
        assert result.failed == 1

    def test_unknown_table(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        result = upsert_records(slug, "nonexistent", [{"foo": "bar"}])
        assert result.failed == 1
        assert "Unknown" in result.errors[0]


# ═══════════════════════════════════════════════════════════════════════════
# CRUD — read, count, delete, clear
# ═══════════════════════════════════════════════════════════════════════════

class TestReadOperations:
    def test_get_table_records(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [
            {"stop_id": f"S{i:03d}", "stop_name": f"Stop {i}"} for i in range(5)
        ])
        records = get_table_records(slug, "stops", limit=3)
        assert len(records) == 3
        records_all = get_table_records(slug, "stops", limit=100)
        assert len(records_all) == 5

    def test_get_table_count(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        assert get_table_count(slug, "stops") == 0
        upsert_records(slug, "stops", [
            {"stop_id": "S001"},
            {"stop_id": "S002"},
        ])
        assert get_table_count(slug, "stops") == 2


class TestDeleteOperations:
    def test_delete_by_pk(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [
            {"stop_id": "S001", "stop_name": "A"},
            {"stop_id": "S002", "stop_name": "B"},
            {"stop_id": "S003", "stop_name": "C"},
        ])
        deleted = delete_records(slug, "stops", ["S001", "S003"])
        assert deleted == 2
        assert get_table_count(slug, "stops") == 1
        remaining = get_table_records(slug, "stops")
        assert remaining[0]["stop_id"] == "S002"

    def test_clear_table(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [
            {"stop_id": f"S{i:03d}"} for i in range(10)
        ])
        cleared = clear_table(slug, "stops")
        assert cleared == 10
        assert get_table_count(slug, "stops") == 0


# ═══════════════════════════════════════════════════════════════════════════
# Integrity checks
# ═══════════════════════════════════════════════════════════════════════════

class TestIntegrityChecks:
    def test_clean_database(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        # Insert consistent data: agency → routes → trips → stop_times
        upsert_records(slug, "agency", [
            {"agency_id": "A1", "agency_name": "Test", "agency_url": "http://test.com", "agency_timezone": "UTC"},
        ])
        upsert_records(slug, "stops", [
            {"stop_id": "S1", "stop_name": "Stop 1"},
        ])
        upsert_records(slug, "routes", [
            {"route_id": "R1", "agency_id": "A1", "route_type": 3},
        ])
        upsert_records(slug, "calendar", [
            {"service_id": "WD", "monday": 1, "tuesday": 1, "wednesday": 1,
             "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
             "start_date": "20260101", "end_date": "20261231"},
        ])
        upsert_records(slug, "trips", [
            {"route_id": "R1", "service_id": "WD", "trip_id": "T1"},
        ])
        upsert_records(slug, "stop_times", [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
             "arrival_time": "08:00:00", "departure_time": "08:01:00"},
        ])
        report = check_integrity(slug)
        assert report.is_clean
        assert len(report.violations) == 0

    def test_orphaned_route_agency(self, isolated_gtfs_db):
        """Route references agency_id that doesn't exist."""
        slug = isolated_gtfs_db
        create_gtfs_database(slug)

        # Insert route without the agency — bypass FK by inserting directly
        conn = get_connection(slug)
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                "INSERT INTO routes (route_id, agency_id, route_type) VALUES (?, ?, ?)",
                ("R1", "MISSING_AGENCY", 3),
            )
            conn.commit()
        finally:
            conn.close()

        report = check_integrity(slug)
        assert not report.is_clean
        orphans = [v for v in report.violations if v.table == "routes" and v.violation_type == "orphaned_fk"]
        assert len(orphans) >= 1
        assert "MISSING_AGENCY" in orphans[0].detail

    def test_orphaned_stop_times(self, isolated_gtfs_db):
        """stop_times references trip_id that doesn't exist."""
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [{"stop_id": "S1"}])

        conn = get_connection(slug)
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                "INSERT INTO stop_times (trip_id, stop_id, stop_sequence) VALUES (?, ?, ?)",
                ("GHOST_TRIP", "S1", 0),
            )
            conn.commit()
        finally:
            conn.close()

        report = check_integrity(slug)
        assert not report.is_clean
        trip_orphans = [v for v in report.violations if "GHOST_TRIP" in v.detail]
        assert len(trip_orphans) >= 1

    def test_empty_database_is_clean(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        report = check_integrity(slug)
        assert report.is_clean


# ═══════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════

class TestDatabaseSummary:
    def test_nonexistent_db(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        summary = get_database_summary(slug)
        assert summary["exists"] is False

    def test_populated_summary(self, isolated_gtfs_db):
        slug = isolated_gtfs_db
        create_gtfs_database(slug)
        upsert_records(slug, "stops", [
            {"stop_id": "S1"},
            {"stop_id": "S2"},
        ])
        summary = get_database_summary(slug)
        assert summary["exists"] is True
        assert summary["table_counts"]["stops"] == 2
        assert summary["total_records"] >= 2
        assert "stops" in summary["populated_tables"]
        assert summary["integrity_clean"] is True
        assert "schema_version" in summary


# ═══════════════════════════════════════════════════════════════════════════
# Column ordering
# ═══════════════════════════════════════════════════════════════════════════

class TestTableColumns:
    def test_agency_columns(self):
        cols = get_table_columns("agency")
        assert cols[0] == "agency_id"
        assert "agency_name" in cols
        assert "agency_timezone" in cols

    def test_stop_times_columns(self):
        cols = get_table_columns("stop_times")
        assert "trip_id" in cols
        assert "stop_sequence" in cols
        assert "arrival_time" in cols

    def test_all_gtfs_tables_have_columns(self):
        from optisus.core.schemas.gtfs import GTFS_TABLE_MODELS
        for tbl in GTFS_TABLE_MODELS:
            cols = get_table_columns(tbl)
            assert len(cols) > 0, f"Table {tbl} has no columns"

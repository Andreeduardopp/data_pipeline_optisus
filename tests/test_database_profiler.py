"""
Tests for ``database_profiler`` — the read-only overview layer that
powers the Database Overview dashboard.

Covers:
  - Profile shape on an empty DB (all zero, no populated table names)
  - Profile shape on a populated DB (row counts, largest/smallest)
  - Lazy per-column profiling (null %, distinct, sample values)
  - Cache invalidation on DB mtime
  - FK relationships lifted from ``_FK_CHECKS``
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from optisus.core.gtfs import database as gtfs_database
from optisus.core.gtfs import database_profiler as profiler
from optisus.core.gtfs.database import (
    create_gtfs_database,
    upsert_records,
)
from optisus.core.gtfs.database_profiler import (
    clear_profile_cache,
    profile_database,
    profile_table_columns,
)


@pytest.fixture()
def isolated_gtfs(tmp_path, monkeypatch):
    """Redirect PROJECTS_ROOT to a temp dir, clear profiler caches.

    Both ``database`` and ``database_profiler`` read PROJECTS_ROOT, so
    we patch both to keep them in sync.
    """
    projects_root = tmp_path / "projects"
    projects_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", projects_root)
    monkeypatch.setattr(profiler, "PROJECTS_ROOT", projects_root)

    project_dir = projects_root / "test_project"
    project_dir.mkdir(parents=True, exist_ok=True)
    # Write a minimal project.json so _project_name resolves
    (project_dir / "project.json").write_text(
        '{"name": "Test Project", "slug": "test_project"}'
    )

    clear_profile_cache()
    yield "test_project"
    clear_profile_cache()


def _seed_basic(slug: str) -> None:
    """Insert a small but realistic set of records across core tables."""
    create_gtfs_database(slug)
    upsert_records(slug, "agency", [{
        "agency_id": "A1",
        "agency_name": "Acme Transit",
        "agency_url": "https://acme.test",
        "agency_timezone": "Europe/Lisbon",
    }])
    upsert_records(slug, "stops", [
        {"stop_id": "S1", "stop_name": "Alpha",  "stop_lat": 41.1, "stop_lon": -8.6},
        {"stop_id": "S2", "stop_name": "Beta",   "stop_lat": 41.2, "stop_lon": -8.5},
        {"stop_id": "S3", "stop_name": "Gamma",  "stop_lat": 41.3, "stop_lon": -8.4},
    ])
    upsert_records(slug, "routes", [{
        "route_id": "R1", "agency_id": "A1",
        "route_short_name": "1", "route_long_name": "Main",
        "route_type": 3,
    }])
    upsert_records(slug, "trips", [
        {"route_id": "R1", "service_id": "WD", "trip_id": "T1"},
        {"route_id": "R1", "service_id": "WD", "trip_id": "T2"},
    ])
    upsert_records(slug, "stop_times", [
        {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1,
         "arrival_time": "08:00:00", "departure_time": "08:00:00"},
        {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 2,
         "arrival_time": "08:05:00", "departure_time": "08:05:00"},
        {"trip_id": "T1", "stop_id": "S3", "stop_sequence": 3,
         "arrival_time": "08:10:00", "departure_time": "08:10:00"},
        {"trip_id": "T2", "stop_id": "S1", "stop_sequence": 1,
         "arrival_time": "09:00:00", "departure_time": "09:00:00"},
    ])


# ═══════════════════════════════════════════════════════════════════════════
# profile_database — cheap overview
# ═══════════════════════════════════════════════════════════════════════════

class TestProfileDatabaseEmpty:
    def test_not_created(self, isolated_gtfs):
        profile = profile_database(isolated_gtfs)
        assert profile.exists is False
        assert profile.total_records == 0

    def test_created_but_empty(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        assert profile.exists is True
        assert profile.total_records == 0
        assert profile.populated_tables == 0
        assert profile.empty_tables == profile.total_tables
        assert profile.completeness_pct == 0.0
        assert profile.largest_table == ""
        assert profile.smallest_populated_table == ""
        assert profile.integrity_clean is True
        assert profile.project_name == "Test Project"


class TestProfileDatabasePopulated:
    def test_record_counts(self, isolated_gtfs):
        _seed_basic(isolated_gtfs)
        profile = profile_database(isolated_gtfs)

        assert profile.exists is True
        # 1 agency + 3 stops + 1 route + 2 trips + 4 stop_times = 11
        assert profile.total_records == 11
        assert profile.populated_tables == 5
        assert profile.empty_tables == profile.total_tables - 5

    def test_largest_and_smallest(self, isolated_gtfs):
        _seed_basic(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        assert profile.largest_table == "stop_times"   # 4 rows
        assert profile.smallest_populated_table in {"agency", "routes"}  # both 1

    def test_completeness_pct(self, isolated_gtfs):
        _seed_basic(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        expected = 5 / profile.total_tables * 100
        assert profile.completeness_pct == pytest.approx(expected)


# ═══════════════════════════════════════════════════════════════════════════
# FK relationships surfaced
# ═══════════════════════════════════════════════════════════════════════════

class TestFkRelationships:
    def test_stop_times_references_trips_and_stops(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        by_name = {t.table_name: t for t in profile.tables}
        assert set(by_name["stop_times"].fk_references) == {"trips", "stops"}

    def test_routes_references_agency(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        by_name = {t.table_name: t for t in profile.tables}
        assert by_name["routes"].fk_references == ["agency"]

    def test_agency_has_no_refs(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        by_name = {t.table_name: t for t in profile.tables}
        assert by_name["agency"].fk_references == []


# ═══════════════════════════════════════════════════════════════════════════
# profile_table_columns — lazy per-column stats
# ═══════════════════════════════════════════════════════════════════════════

class TestProfileTableColumns:
    def test_distinct_and_samples_on_populated(self, isolated_gtfs):
        _seed_basic(isolated_gtfs)
        cols = profile_table_columns(isolated_gtfs, "stops")
        by_name = {c.name: c for c in cols}

        assert by_name["stop_id"].distinct_count == 3
        assert by_name["stop_id"].null_count == 0
        assert by_name["stop_id"].is_primary_key is True
        # Samples pulled from actual data
        assert set(by_name["stop_id"].sample_values) == {"S1", "S2", "S3"}

    def test_null_counts(self, isolated_gtfs):
        """Insert records where some nullable columns are left unset."""
        create_gtfs_database(isolated_gtfs)
        upsert_records(isolated_gtfs, "stops", [
            {"stop_id": "X1", "stop_name": "First",  "stop_lat": 0.0, "stop_lon": 0.0},
            # stop_name omitted → null
            {"stop_id": "X2",                         "stop_lat": 0.0, "stop_lon": 0.0},
            {"stop_id": "X3",                         "stop_lat": 0.0, "stop_lon": 0.0},
        ])
        cols = profile_table_columns(isolated_gtfs, "stops")
        by_name = {c.name: c for c in cols}

        assert by_name["stop_name"].null_count == 2
        assert by_name["stop_name"].null_pct == pytest.approx(66.666, abs=0.1)
        assert by_name["stop_id"].null_count == 0

    def test_empty_table_returns_zero_stats(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        cols = profile_table_columns(isolated_gtfs, "stops")
        # Columns still enumerated, but all counts zero
        assert len(cols) > 0
        assert all(c.null_count == 0 and c.distinct_count == 0 for c in cols)

    def test_unknown_table_returns_empty(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        assert profile_table_columns(isolated_gtfs, "does_not_exist") == []

    def test_profile_database_does_not_compute_columns(self, isolated_gtfs):
        """profile_database must stay cheap — it shouldn't call the column profiler."""
        _seed_basic(isolated_gtfs)
        profile = profile_database(isolated_gtfs)
        # TableProfile does not carry ColumnProfile — that's the contract
        assert not hasattr(profile.tables[0], "columns")


# ═══════════════════════════════════════════════════════════════════════════
# Cache invalidation on DB mtime
# ═══════════════════════════════════════════════════════════════════════════

class TestCacheInvalidation:
    def test_reflects_new_rows(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        p1 = profile_database(isolated_gtfs)
        assert p1.total_records == 0

        # Mutate — must bump DB mtime so the cache key changes
        time.sleep(0.05)
        upsert_records(isolated_gtfs, "agency", [{
            "agency_id": "A1",
            "agency_name": "Late Arrival",
            "agency_url": "https://late.test",
            "agency_timezone": "Europe/Lisbon",
        }])

        p2 = profile_database(isolated_gtfs)
        assert p2.total_records == 1
        assert p2.populated_tables == 1

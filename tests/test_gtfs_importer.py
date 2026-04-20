"""
Tests for core.gtfs.importer — GTFS ZIP ingestion.
"""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pytest

from optisus.core.gtfs.database import (
    create_gtfs_database,
    get_database_summary,
    get_table_count,
    upsert_records,
)
from optisus.core.gtfs.exporter import export_gtfs_feed
from optisus.core.gtfs.importer import (
    GtfsImportError,
    GtfsZipPreview,
    ImportMode,
    import_gtfs_zip,
    preview_gtfs_zip,
)


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def isolated_gtfs(tmp_path, monkeypatch):
    from optisus.core.gtfs import database as gtfs_database
    from optisus.core.gtfs import exporter as gtfs_exporter
    projects = tmp_path / "projects"
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", projects)
    monkeypatch.setattr(gtfs_exporter, "PROJECTS_ROOT", projects)
    project_dir = projects / "test_project"
    project_dir.mkdir(parents=True, exist_ok=True)
    return "test_project"


# ═══════════════════════════════════════════════════════════════════════════
# Helpers — build in-memory GTFS archives
# ═══════════════════════════════════════════════════════════════════════════

def _csv_bytes(header: List[str], rows: Iterable[Dict[str, object]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=header, lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if r.get(k) is None else r[k]) for k in header})
    return buf.getvalue().encode("utf-8")


def _minimal_feed_members() -> Dict[str, bytes]:
    """Return a dict of ``{filename: bytes}`` for a minimal valid GTFS feed."""
    return {
        "agency.txt": _csv_bytes(
            ["agency_id", "agency_name", "agency_url", "agency_timezone"],
            [{"agency_id": "A1", "agency_name": "Test",
              "agency_url": "https://t.example", "agency_timezone": "UTC"}],
        ),
        "stops.txt": _csv_bytes(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            [
                {"stop_id": "S1", "stop_name": "Stop 1", "stop_lat": 41.15, "stop_lon": -8.61},
                {"stop_id": "S2", "stop_name": "Stop 2", "stop_lat": 41.16, "stop_lon": -8.62},
            ],
        ),
        "routes.txt": _csv_bytes(
            ["route_id", "agency_id", "route_short_name", "route_type"],
            [{"route_id": "R1", "agency_id": "A1",
              "route_short_name": "L1", "route_type": 3}],
        ),
        "calendar.txt": _csv_bytes(
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"],
            [{"service_id": "WD", "monday": 1, "tuesday": 1, "wednesday": 1,
              "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
              "start_date": "20260101", "end_date": "20261231"}],
        ),
        "trips.txt": _csv_bytes(
            ["route_id", "service_id", "trip_id", "direction_id"],
            [{"route_id": "R1", "service_id": "WD", "trip_id": "T1", "direction_id": 0}],
        ),
        "stop_times.txt": _csv_bytes(
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
            [
                {"trip_id": "T1", "arrival_time": "08:00:00",
                 "departure_time": "08:01:00", "stop_id": "S1", "stop_sequence": 0},
                {"trip_id": "T1", "arrival_time": "08:10:00",
                 "departure_time": "08:11:00", "stop_id": "S2", "stop_sequence": 1},
            ],
        ),
    }


def _zip_bytes(members: Dict[str, bytes], *, folder_prefix: str = "") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(f"{folder_prefix}{name}", data)
    buf.seek(0)
    return buf.read()


# ═══════════════════════════════════════════════════════════════════════════
# Preview
# ═══════════════════════════════════════════════════════════════════════════

class TestPreview:
    def test_happy_path(self):
        data = _zip_bytes(_minimal_feed_members())
        preview = preview_gtfs_zip(io.BytesIO(data))
        assert preview.is_valid
        assert preview.recognised_tables["agency"] == 1
        assert preview.recognised_tables["stop_times"] == 2
        assert preview.missing_required == []
        assert preview.errors == []

    def test_rejects_missing_required(self):
        members = _minimal_feed_members()
        del members["stops.txt"]
        data = _zip_bytes(members)
        preview = preview_gtfs_zip(io.BytesIO(data))
        assert not preview.is_valid
        assert "stops" in preview.missing_required

    def test_rejects_missing_service_files(self):
        members = _minimal_feed_members()
        del members["calendar.txt"]
        data = _zip_bytes(members)
        preview = preview_gtfs_zip(io.BytesIO(data))
        assert not preview.is_valid
        assert "calendar_or_calendar_dates" in preview.missing_required

    def test_tolerates_nested_folder(self):
        data = _zip_bytes(_minimal_feed_members(), folder_prefix="feed/")
        preview = preview_gtfs_zip(io.BytesIO(data))
        assert preview.is_valid
        assert preview.recognised_tables["agency"] == 1

    def test_reports_unknown_files(self):
        members = _minimal_feed_members()
        members["notes.txt"] = b"hello,world\n1,2\n"
        data = _zip_bytes(members)
        preview = preview_gtfs_zip(io.BytesIO(data))
        assert preview.is_valid
        assert any("notes.txt" in u for u in preview.unknown_files)

    def test_bad_zip(self):
        preview = preview_gtfs_zip(io.BytesIO(b"not a zip"))
        assert not preview.is_valid
        assert preview.errors


# ═══════════════════════════════════════════════════════════════════════════
# Import
# ═══════════════════════════════════════════════════════════════════════════

class TestImport:
    def test_replace_on_empty_db(self, isolated_gtfs):
        slug = isolated_gtfs
        data = _zip_bytes(_minimal_feed_members())
        result = import_gtfs_zip(slug, io.BytesIO(data), mode=ImportMode.REPLACE)

        assert result.total_failed == 0
        assert result.inserted_by_table["agency"] == 1
        assert result.inserted_by_table["stops"] == 2
        assert result.inserted_by_table["stop_times"] == 2
        assert get_table_count(slug, "stop_times") == 2

    def test_replace_wipes_previous(self, isolated_gtfs):
        slug = isolated_gtfs
        create_gtfs_database(slug)
        upsert_records(slug, "agency", [
            {"agency_id": "OLD", "agency_name": "Old",
             "agency_url": "https://old.example", "agency_timezone": "UTC"},
        ])
        assert get_table_count(slug, "agency") == 1

        data = _zip_bytes(_minimal_feed_members())
        result = import_gtfs_zip(slug, io.BytesIO(data), mode=ImportMode.REPLACE)

        assert "agency" in result.cleared_tables
        assert get_table_count(slug, "agency") == 1  # only the new one
        # Confirm the old id is gone
        from optisus.core.gtfs.database import get_table_records
        rows = get_table_records(slug, "agency")
        assert rows[0]["agency_id"] == "A1"

    def test_merge_overwrites_matching_pks(self, isolated_gtfs):
        slug = isolated_gtfs
        create_gtfs_database(slug)
        upsert_records(slug, "agency", [
            {"agency_id": "A1", "agency_name": "OLD NAME",
             "agency_url": "https://old.example", "agency_timezone": "UTC"},
        ])

        data = _zip_bytes(_minimal_feed_members())
        result = import_gtfs_zip(slug, io.BytesIO(data), mode=ImportMode.MERGE)
        assert result.cleared_tables == []

        from optisus.core.gtfs.database import get_table_records
        rows = get_table_records(slug, "agency")
        assert len(rows) == 1
        assert rows[0]["agency_name"] == "Test"  # overwritten

    def test_abort_if_not_empty_raises(self, isolated_gtfs):
        slug = isolated_gtfs
        create_gtfs_database(slug)
        upsert_records(slug, "agency", [
            {"agency_id": "A1", "agency_name": "keep me",
             "agency_url": "https://keep.example", "agency_timezone": "UTC"},
        ])

        data = _zip_bytes(_minimal_feed_members())
        with pytest.raises(GtfsImportError):
            import_gtfs_zip(slug, io.BytesIO(data),
                            mode=ImportMode.ABORT_IF_NOT_EMPTY)
        assert get_table_count(slug, "agency") == 1

    def test_malformed_row_reported_others_succeed(self, isolated_gtfs):
        slug = isolated_gtfs
        members = _minimal_feed_members()
        members["routes.txt"] = _csv_bytes(
            ["route_id", "agency_id", "route_short_name", "route_type"],
            [
                {"route_id": "R1", "agency_id": "A1",
                 "route_short_name": "Good", "route_type": 3},
                {"route_id": "R2", "agency_id": "A1",
                 "route_short_name": "Bad", "route_type": "not_an_int"},
            ],
        )
        data = _zip_bytes(members)
        result = import_gtfs_zip(slug, io.BytesIO(data))
        assert result.inserted_by_table["routes"] == 1
        assert result.failed_by_table.get("routes", 0) == 1
        assert "routes" in result.errors_by_table

    def test_fk_safe_order(self, isolated_gtfs):
        """stop_times must insert after trips/stops even if alphabetically earlier."""
        slug = isolated_gtfs
        data = _zip_bytes(_minimal_feed_members())
        result = import_gtfs_zip(slug, io.BytesIO(data))
        assert result.total_failed == 0

    def test_rejects_missing_required(self, isolated_gtfs):
        slug = isolated_gtfs
        members = _minimal_feed_members()
        del members["trips.txt"]
        data = _zip_bytes(members)
        with pytest.raises(GtfsImportError, match="missing"):
            import_gtfs_zip(slug, io.BytesIO(data))

    def test_db_signature_changes(self, isolated_gtfs):
        slug = isolated_gtfs
        from optisus.core.gtfs.analytics import db_signature

        create_gtfs_database(slug)
        sig_before = db_signature(slug)

        data = _zip_bytes(_minimal_feed_members())
        import_gtfs_zip(slug, io.BytesIO(data))
        sig_after = db_signature(slug)

        assert sig_before != sig_after

    def test_zip_bomb_guard(self, isolated_gtfs):
        slug = isolated_gtfs
        # Build a zip and mutate a member's file_size in the central directory
        # to exceed MAX_MEMBER_BYTES before parsing.
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, body in _minimal_feed_members().items():
                zf.writestr(name, body)
        buf.seek(0)

        # Reopen and overwrite one ZipInfo's file_size in the in-memory infolist
        # via monkey-patching: easier route is to assert the importer rejects
        # archives where a member reports an absurd uncompressed size.
        from optisus.core.gtfs import importer as imp
        import zipfile as zfmod

        orig_infolist = zfmod.ZipFile.infolist

        def inflated_infolist(self):
            items = orig_infolist(self)
            if items:
                items[0].file_size = imp.MAX_MEMBER_BYTES + 1
            return items

        import_error = None
        try:
            zfmod.ZipFile.infolist = inflated_infolist  # type: ignore[assignment]
            with pytest.raises(GtfsImportError):
                import_gtfs_zip(slug, io.BytesIO(buf.getvalue()))
        finally:
            zfmod.ZipFile.infolist = orig_infolist  # type: ignore[assignment]

    def test_round_trip_export_then_import(self, isolated_gtfs, tmp_path):
        slug = isolated_gtfs

        create_gtfs_database(slug)
        for table, rows in [
            ("agency", [{"agency_id": "A1", "agency_name": "RT",
                         "agency_url": "https://rt.example",
                         "agency_timezone": "UTC"}]),
            ("stops", [
                {"stop_id": "S1", "stop_name": "Stop 1",
                 "stop_lat": 41.15, "stop_lon": -8.61},
                {"stop_id": "S2", "stop_name": "Stop 2",
                 "stop_lat": 41.16, "stop_lon": -8.62},
            ]),
            ("routes", [{"route_id": "R1", "agency_id": "A1",
                         "route_short_name": "L1", "route_type": 3}]),
            ("calendar", [{"service_id": "WD",
                           "monday": 1, "tuesday": 1, "wednesday": 1,
                           "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
                           "start_date": "20260101", "end_date": "20261231"}]),
            ("trips", [{"route_id": "R1", "service_id": "WD",
                        "trip_id": "T1", "direction_id": 0}]),
            ("stop_times", [
                {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 0,
                 "arrival_time": "08:00:00", "departure_time": "08:01:00"},
                {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 1,
                 "arrival_time": "08:10:00", "departure_time": "08:11:00"},
            ]),
        ]:
            r = upsert_records(slug, table, rows)
            assert r.failed == 0

        summary_before = get_database_summary(slug)["table_counts"]

        export = export_gtfs_feed(slug, output_dir=str(tmp_path / "out"))
        assert export.success and export.zip_path

        import_gtfs_zip(
            slug,
            export.zip_path,
            mode=ImportMode.REPLACE,
        )
        summary_after = get_database_summary(slug)["table_counts"]

        for table in ("agency", "stops", "routes", "trips", "stop_times", "calendar"):
            assert summary_before[table] == summary_after[table], (
                f"count mismatch for {table}: "
                f"before={summary_before[table]} after={summary_after[table]}"
            )

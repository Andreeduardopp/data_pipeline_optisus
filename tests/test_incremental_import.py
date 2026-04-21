"""
Tests for Layer 1 (MERGE_PARTIAL) and Layer 2 (batch_import).

Shares a minimal-feed builder with ``test_gtfs_importer.py``; kept as a
separate file so the incremental-update surface has its own test home.
"""

from __future__ import annotations

import csv
import io
import zipfile
from typing import Dict, Iterable, List

import pytest

from optisus.core.gtfs.batch_import import (
    BatchImportError,
    import_batch,
    infer_table_from_filename,
    preview_batch,
)
from optisus.core.gtfs.database import (
    create_gtfs_database,
    get_table_count,
    get_table_records,
)
from optisus.core.gtfs.importer import (
    GtfsImportError,
    ImportMode,
    import_gtfs_zip,
)


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures & helpers
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


def _csv_bytes(header: List[str], rows: Iterable[Dict[str, object]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=header, lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if r.get(k) is None else r[k]) for k in header})
    return buf.getvalue().encode("utf-8")


def _minimal_feed_members() -> Dict[str, bytes]:
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


def _zip_bytes(members: Dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    buf.seek(0)
    return buf.read()


def _seed_populated_db(slug: str) -> None:
    """Import a full minimal feed so the DB has parents for FK children."""
    import_gtfs_zip(
        slug,
        io.BytesIO(_zip_bytes(_minimal_feed_members())),
        mode=ImportMode.REPLACE,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Layer 1 — MERGE_PARTIAL
# ═══════════════════════════════════════════════════════════════════════════

class TestMergePartialRejectsOnEmptyDb:
    def test_empty_db_rejects_partial(self, isolated_gtfs):
        """Partial ZIP must refuse when DB is empty — there's nothing to merge into."""
        create_gtfs_database(isolated_gtfs)
        partial = _zip_bytes({"stops.txt": _minimal_feed_members()["stops.txt"]})
        with pytest.raises(GtfsImportError, match="requires an already-populated"):
            import_gtfs_zip(
                isolated_gtfs,
                io.BytesIO(partial),
                mode=ImportMode.MERGE_PARTIAL,
            )


class TestMergePartialOnPopulatedDb:
    def test_merge_partial_updates_only_given_tables(self, isolated_gtfs):
        _seed_populated_db(isolated_gtfs)

        # Sanity: originals survive
        assert get_table_count(isolated_gtfs, "stops") == 2
        assert get_table_count(isolated_gtfs, "trips") == 1
        assert get_table_count(isolated_gtfs, "agency") == 1

        # Partial ZIP: add one new stop + rename existing one
        updated_stops = _csv_bytes(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            [
                {"stop_id": "S1", "stop_name": "Stop 1 RENAMED",
                 "stop_lat": 41.15, "stop_lon": -8.61},
                {"stop_id": "S3", "stop_name": "Stop 3 NEW",
                 "stop_lat": 41.17, "stop_lon": -8.63},
            ],
        )
        partial = _zip_bytes({"stops.txt": updated_stops})

        result = import_gtfs_zip(
            isolated_gtfs,
            io.BytesIO(partial),
            mode=ImportMode.MERGE_PARTIAL,
        )
        assert result.inserted_by_table["stops"] == 2
        assert "stops" not in result.cleared_tables  # nothing was cleared

        # stops was upserted (S1 renamed, S2 untouched, S3 new)
        stops_by_id = {r["stop_id"]: r for r in get_table_records(isolated_gtfs, "stops")}
        assert stops_by_id["S1"]["stop_name"] == "Stop 1 RENAMED"
        assert stops_by_id["S2"]["stop_name"] == "Stop 2"  # untouched
        assert stops_by_id["S3"]["stop_name"] == "Stop 3 NEW"

        # Other tables untouched
        assert get_table_count(isolated_gtfs, "trips") == 1
        assert get_table_count(isolated_gtfs, "agency") == 1

    def test_empty_archive_rejects(self, isolated_gtfs):
        _seed_populated_db(isolated_gtfs)
        empty_zip = _zip_bytes({"notes.txt": b"unrelated\n"})
        with pytest.raises(GtfsImportError, match="no recognised GTFS files"):
            import_gtfs_zip(
                isolated_gtfs,
                io.BytesIO(empty_zip),
                mode=ImportMode.MERGE_PARTIAL,
            )

    def test_full_merge_still_requires_complete_feed(self, isolated_gtfs):
        """Layer 1 only relaxes the rule for MERGE_PARTIAL — MERGE still enforces it."""
        _seed_populated_db(isolated_gtfs)
        partial = _zip_bytes({"stops.txt": _minimal_feed_members()["stops.txt"]})
        with pytest.raises(GtfsImportError, match="missing required GTFS files"):
            import_gtfs_zip(
                isolated_gtfs,
                io.BytesIO(partial),
                mode=ImportMode.MERGE,
            )


# ═══════════════════════════════════════════════════════════════════════════
# Layer 2 — batch_import
# ═══════════════════════════════════════════════════════════════════════════

class TestFilenameInference:
    def test_matches_known_tables(self):
        assert infer_table_from_filename("stops.csv") == "stops"
        assert infer_table_from_filename("stops.txt") == "stops"
        assert infer_table_from_filename("STOPS.CSV") == "stops"
        assert infer_table_from_filename("feed/stop_times.txt") == "stop_times"

    def test_returns_none_for_unknown(self):
        assert infer_table_from_filename("random.csv") is None
        assert infer_table_from_filename("stops_backup.csv") is None


class TestBatchPreview:
    def test_preview_counts_rows(self):
        preview = preview_batch([
            ("stops.csv", _minimal_feed_members()["stops.txt"]),
        ])
        assert preview.is_valid
        assert preview.files[0].table == "stops"
        assert preview.files[0].row_count == 2

    def test_preview_flags_unknown(self):
        preview = preview_batch([("notes.csv", b"a,b\n1,2\n")])
        assert preview.unknown_files == ["notes.csv"]
        assert not preview.is_valid  # no mapped files

    def test_duplicate_tables_rejected(self):
        preview = preview_batch(
            [
                ("stops.csv", _minimal_feed_members()["stops.txt"]),
                ("stops2.csv", _minimal_feed_members()["stops.txt"]),
            ],
            table_overrides={"stops2.csv": "stops"},
        )
        assert "stops" in preview.duplicate_tables
        assert not preview.is_valid


class TestBatchImportHappyPath:
    def test_single_file_imports(self, isolated_gtfs):
        _seed_populated_db(isolated_gtfs)
        # Rename an existing stop via a batch of one
        new_stops = _csv_bytes(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            [{"stop_id": "S1", "stop_name": "BATCH-RENAMED",
              "stop_lat": 41.15, "stop_lon": -8.61}],
        )
        result = import_batch(
            isolated_gtfs,
            [("stops.csv", new_stops)],
        )
        assert result.committed
        assert result.inserted_by_table["stops"] == 1
        assert result.total_failed == 0
        stops = {r["stop_id"]: r for r in get_table_records(isolated_gtfs, "stops")}
        assert stops["S1"]["stop_name"] == "BATCH-RENAMED"

    def test_multi_file_runs_in_fk_safe_order(self, isolated_gtfs):
        """Child tables arrive before parents in the input; the importer must reorder."""
        create_gtfs_database(isolated_gtfs)
        m = _minimal_feed_members()
        # Intentionally pass stop_times FIRST — depends on trips+stops that don't exist yet.
        # Without FK-safe reordering this would fail.
        files = [
            ("stop_times.csv", m["stop_times.txt"]),
            ("trips.csv", m["trips.txt"]),
            ("routes.csv", m["routes.txt"]),
            ("agency.csv", m["agency.txt"]),
            ("stops.csv", m["stops.txt"]),
            ("calendar.csv", m["calendar.txt"]),
        ]
        result = import_batch(isolated_gtfs, files)
        assert result.committed
        assert result.total_failed == 0
        assert get_table_count(isolated_gtfs, "stop_times") == 2
        assert get_table_count(isolated_gtfs, "trips") == 1
        assert get_table_count(isolated_gtfs, "agency") == 1

    def test_override_maps_unknown_filename(self, isolated_gtfs):
        _seed_populated_db(isolated_gtfs)
        data = _csv_bytes(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            [{"stop_id": "S9", "stop_name": "Via override",
              "stop_lat": 41.0, "stop_lon": -8.0}],
        )
        result = import_batch(
            isolated_gtfs,
            [("weirdname.csv", data)],
            table_overrides={"weirdname.csv": "stops"},
        )
        assert result.committed
        assert get_table_count(isolated_gtfs, "stops") == 3


class TestBatchImportRejections:
    def test_no_mapped_files_raises(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        with pytest.raises(BatchImportError, match="No files mapped"):
            import_batch(isolated_gtfs, [("random.csv", b"a,b\n1,2\n")])

    def test_duplicate_table_targets_raises(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        m = _minimal_feed_members()
        with pytest.raises(BatchImportError, match="same table"):
            import_batch(
                isolated_gtfs,
                [
                    ("stops.csv", m["stops.txt"]),
                    ("stops_copy.csv", m["stops.txt"]),
                ],
                table_overrides={"stops_copy.csv": "stops"},
            )

    def test_empty_batch_raises(self, isolated_gtfs):
        create_gtfs_database(isolated_gtfs)
        with pytest.raises(BatchImportError):
            import_batch(isolated_gtfs, [])

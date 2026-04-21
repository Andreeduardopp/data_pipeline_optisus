"""
GTFS ZIP importer.

Accepts a complete GTFS ``.zip`` feed from disk or an in-memory file-like
object and populates a project's SQLite database via the existing
:func:`optisus.core.gtfs.database.upsert_records` pipeline.

Public API:
    - ``preview_gtfs_zip(source)`` — inspect the archive without writing.
    - ``import_gtfs_zip(slug, source, mode=...)`` — perform the import.
    - ``ImportMode`` — REPLACE / MERGE / ABORT_IF_NOT_EMPTY.
    - ``GtfsZipPreview`` / ``GtfsImportResult`` — result dataclasses.
    - ``GtfsImportError`` — raised for non-recoverable import failures.

The importer never extracts to disk; every member is streamed via
``ZipFile.open`` and parsed with ``csv.DictReader``.  Size and zip-bomb
guards run before any CSV parsing.
"""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union

from optisus.core.gtfs.database import (
    clear_all_tables,
    create_gtfs_database,
    get_database_summary,
    upsert_records,
)
from optisus.core.schemas.gtfs import GTFS_TABLE_MODELS

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_CHUNK_SIZE = 5_000
MAX_ARCHIVE_BYTES = 500 * 1024 * 1024          # 500 MB compressed
MAX_MEMBER_BYTES = 1024 * 1024 * 1024          # 1 GB uncompressed per file
MAX_MEMBER_COUNT = 50                          # reject archives with >50 members
MAX_ERRORS_PER_TABLE = 50                      # cap surfaced error strings

REQUIRED_FILES = ("agency", "stops", "routes", "trips", "stop_times")
REQUIRED_SERVICE_FILES = ("calendar", "calendar_dates")  # at least one of

# FK-safe insert order — parents before children.
INSERT_ORDER: tuple[str, ...] = (
    "agency",
    "feed_info",
    "calendar",
    "calendar_dates",
    "shapes",
    "stops",
    "routes",
    "trips",
    "frequencies",
    "transfers",
    "stop_times",
    # GTFS-ride (rare in .zip feeds but supported if present)
    "ride_feed_info",
    "trip_capacity",
    "board_alight",
    "ridership",
)


# ═══════════════════════════════════════════════════════════════════════════
# Types
# ═══════════════════════════════════════════════════════════════════════════

class ImportMode(str, Enum):
    REPLACE = "replace"
    MERGE = "merge"
    MERGE_PARTIAL = "merge_partial"
    ABORT_IF_NOT_EMPTY = "abort_if_not_empty"


class GtfsImportError(Exception):
    """Raised when the import cannot proceed (corrupt zip, guard rails, etc.)."""


@dataclass
class GtfsZipPreview:
    is_valid: bool = False
    recognised_tables: Dict[str, int] = field(default_factory=dict)
    unknown_files: List[str] = field(default_factory=list)
    missing_required: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class GtfsImportResult:
    inserted_by_table: Dict[str, int] = field(default_factory=dict)
    failed_by_table: Dict[str, int] = field(default_factory=dict)
    cleared_tables: List[str] = field(default_factory=list)
    skipped_tables: List[str] = field(default_factory=list)
    errors_by_table: Dict[str, List[str]] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def total_inserted(self) -> int:
        return sum(self.inserted_by_table.values())

    @property
    def total_failed(self) -> int:
        return sum(self.failed_by_table.values())


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _open_zipfile(source: Union[str, Path, BinaryIO]) -> zipfile.ZipFile:
    """Open a zip from a path or file-like, with size guards."""
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise GtfsImportError(f"GTFS archive not found: {path}")
        if path.stat().st_size > MAX_ARCHIVE_BYTES:
            raise GtfsImportError(
                f"Archive too large: {path.stat().st_size} bytes "
                f"(limit is {MAX_ARCHIVE_BYTES})"
            )
        try:
            return zipfile.ZipFile(path)
        except zipfile.BadZipFile as exc:
            raise GtfsImportError(f"Not a valid zip archive: {exc}") from exc

    # File-like
    try:
        source.seek(0, io.SEEK_END)
        size = source.tell()
        source.seek(0)
    except Exception:
        size = None
    if size is not None and size > MAX_ARCHIVE_BYTES:
        raise GtfsImportError(
            f"Archive too large: {size} bytes (limit is {MAX_ARCHIVE_BYTES})"
        )
    try:
        return zipfile.ZipFile(source)
    except zipfile.BadZipFile as exc:
        raise GtfsImportError(f"Not a valid zip archive: {exc}") from exc


def _map_members_to_tables(zf: zipfile.ZipFile) -> tuple[Dict[str, zipfile.ZipInfo], List[str]]:
    """Map known GTFS table names to their ZipInfo, returning also unknown .txt files."""
    known: Dict[str, zipfile.ZipInfo] = {}
    unknown: List[str] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = Path(info.filename).name  # tolerate nested folders
        stem = name.lower()
        if not stem.endswith(".txt"):
            continue
        table = stem[:-4]
        if table in GTFS_TABLE_MODELS:
            # First occurrence wins if the archive has duplicates
            known.setdefault(table, info)
        else:
            unknown.append(info.filename)
    return known, unknown


def _enforce_member_guards(zf: zipfile.ZipFile) -> None:
    members = zf.infolist()
    if len(members) > MAX_MEMBER_COUNT:
        raise GtfsImportError(
            f"Archive has {len(members)} members (limit is {MAX_MEMBER_COUNT})"
        )
    for info in members:
        if info.file_size > MAX_MEMBER_BYTES:
            raise GtfsImportError(
                f"Member '{info.filename}' is {info.file_size} bytes "
                f"uncompressed (limit is {MAX_MEMBER_BYTES})"
            )


def _iter_csv_rows(zf: zipfile.ZipFile, info: zipfile.ZipInfo):
    """Yield normalised row dicts from a CSV member, stripping BOM and nulling empties."""
    with zf.open(info, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        for row in reader:
            cleaned: Dict[str, Any] = {}
            for k, v in row.items():
                if k is None:
                    continue
                key = k.strip().lstrip("\ufeff")
                if not key:
                    continue
                if v is None:
                    cleaned[key] = None
                else:
                    s = v.strip()
                    cleaned[key] = s if s != "" else None
            yield cleaned


def _count_rows(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> int:
    count = 0
    for _ in _iter_csv_rows(zf, info):
        count += 1
    return count


def _required_file_issues(known: Dict[str, zipfile.ZipInfo]) -> List[str]:
    missing: List[str] = [t for t in REQUIRED_FILES if t not in known]
    if not any(t in known for t in REQUIRED_SERVICE_FILES):
        missing.append("calendar_or_calendar_dates")
    return missing


def _db_is_empty(project_slug: str) -> bool:
    summary = get_database_summary(project_slug)
    if not summary.get("exists"):
        return True
    counts = summary.get("table_counts") or {}
    return all(int(n) == 0 for n in counts.values())


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def preview_gtfs_zip(source: Union[str, Path, BinaryIO]) -> GtfsZipPreview:
    """Inspect a GTFS archive without writing anything to the database."""
    preview = GtfsZipPreview()
    try:
        zf = _open_zipfile(source)
    except GtfsImportError as exc:
        preview.errors.append(str(exc))
        return preview

    try:
        try:
            _enforce_member_guards(zf)
        except GtfsImportError as exc:
            preview.errors.append(str(exc))
            return preview

        known, unknown = _map_members_to_tables(zf)
        preview.unknown_files = unknown
        preview.missing_required = _required_file_issues(known)

        for table, info in known.items():
            try:
                preview.recognised_tables[table] = _count_rows(zf, info)
            except Exception as exc:  # noqa: BLE001
                preview.errors.append(f"Could not read {info.filename}: {exc}")

        preview.is_valid = (
            not preview.missing_required and not preview.errors
        )
    finally:
        zf.close()

    return preview


def import_gtfs_zip(
    project_slug: str,
    source: Union[str, Path, BinaryIO],
    *,
    mode: ImportMode = ImportMode.REPLACE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> GtfsImportResult:
    """Populate the project's GTFS database from a GTFS ``.zip`` feed."""
    started = time.monotonic()
    result = GtfsImportResult()

    zf = _open_zipfile(source)
    try:
        _enforce_member_guards(zf)

        known, _unknown = _map_members_to_tables(zf)

        # MERGE_PARTIAL accepts archives that don't carry every required file,
        # but only into an already-populated database. FK integrity is then
        # enforced row-by-row by SQLite's PRAGMA foreign_keys=ON.
        if mode == ImportMode.MERGE_PARTIAL:
            if not known:
                raise GtfsImportError(
                    "Archive contains no recognised GTFS files to merge."
                )
            if _db_is_empty(project_slug):
                raise GtfsImportError(
                    "MERGE_PARTIAL requires an already-populated database. "
                    "Use REPLACE or MERGE for the initial load."
                )
        else:
            missing = _required_file_issues(known)
            if missing:
                raise GtfsImportError(
                    "Archive is missing required GTFS files: " + ", ".join(missing)
                )

        create_gtfs_database(project_slug)

        if mode == ImportMode.ABORT_IF_NOT_EMPTY and not _db_is_empty(project_slug):
            raise GtfsImportError(
                "Project database is not empty and mode=ABORT_IF_NOT_EMPTY"
            )
        if mode == ImportMode.REPLACE:
            deleted = clear_all_tables(project_slug)
            result.cleared_tables = [t for t, n in deleted.items() if n > 0]

        # Insert tables in FK-safe order; skip any not present in the archive.
        for table in INSERT_ORDER:
            info = known.get(table)
            if info is None:
                result.skipped_tables.append(table)
                continue

            inserted = 0
            failed = 0
            errors: List[str] = []
            batch: List[Dict[str, Any]] = []

            def flush() -> None:
                nonlocal inserted, failed, errors, batch
                if not batch:
                    return
                res = upsert_records(project_slug, table, batch)
                inserted += res.inserted
                failed += res.failed
                if res.errors:
                    remaining = MAX_ERRORS_PER_TABLE - len(errors)
                    if remaining > 0:
                        errors.extend(res.errors[:remaining])
                batch = []

            try:
                for row in _iter_csv_rows(zf, info):
                    batch.append(row)
                    if len(batch) >= chunk_size:
                        flush()
                flush()
            except Exception as exc:  # noqa: BLE001
                failed += len(batch)
                errors.append(f"Fatal parse error: {exc}")
                batch = []

            result.inserted_by_table[table] = inserted
            if failed:
                result.failed_by_table[table] = failed
            if errors:
                result.errors_by_table[table] = errors

            logger.info(
                "imported table %s: inserted=%d failed=%d",
                table, inserted, failed,
            )
    finally:
        zf.close()

    result.duration_seconds = round(time.monotonic() - started, 3)
    return result

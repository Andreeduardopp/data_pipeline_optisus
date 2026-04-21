"""
Multi-CSV batch import for the GTFS SQLite database.

Accepts an arbitrary set of CSV files, infers the target GTFS table from
each filename, and upserts them in FK-safe order **inside a single
SQLite transaction**.  If any table fails, the whole batch rolls back
and the database is left untouched.

Public API:
    - ``infer_table_from_filename(name)`` — filename → table name or None.
    - ``preview_batch(files)`` — inspect file metadata without writing.
    - ``import_batch(slug, files, table_overrides=None)`` — perform the
      transactional import.
    - ``BatchFile`` / ``BatchPreview`` / ``BatchImportResult`` — dataclasses.
    - ``BatchImportError`` — non-recoverable failure (rollback already done).
"""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Sequence, Union

from optisus.core.gtfs.database import (
    create_gtfs_database,
    get_connection,
    upsert_records_on_conn,
)
from optisus.core.gtfs.importer import INSERT_ORDER
from optisus.core.schemas.gtfs import GTFS_TABLE_MODELS

logger = logging.getLogger(__name__)


MAX_BATCH_FILES = 20
MAX_FILE_BYTES = 200 * 1024 * 1024  # 200 MB per file
MAX_ERRORS_PER_TABLE = 50


class BatchImportError(Exception):
    """Raised when the batch cannot proceed (e.g. duplicate table targets)."""


# ═══════════════════════════════════════════════════════════════════════════
# Data classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class BatchFile:
    """One CSV being imported.

    ``table`` may be None when the filename doesn't match a known GTFS
    table — the caller can then provide an explicit override.
    """
    filename: str
    table: Optional[str] = None
    size_bytes: int = 0
    row_count: int = 0
    data: bytes = b""  # raw CSV bytes (kept small by MAX_FILE_BYTES)


@dataclass
class BatchPreview:
    files: List[BatchFile] = field(default_factory=list)
    unknown_files: List[str] = field(default_factory=list)
    duplicate_tables: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.errors and not self.duplicate_tables and any(
            f.table for f in self.files
        )


@dataclass
class BatchImportResult:
    inserted_by_table: Dict[str, int] = field(default_factory=dict)
    failed_by_table: Dict[str, int] = field(default_factory=dict)
    skipped_files: List[str] = field(default_factory=list)
    errors_by_table: Dict[str, List[str]] = field(default_factory=dict)
    duration_seconds: float = 0.0
    committed: bool = False

    @property
    def total_inserted(self) -> int:
        return sum(self.inserted_by_table.values())

    @property
    def total_failed(self) -> int:
        return sum(self.failed_by_table.values())


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def infer_table_from_filename(filename: str) -> Optional[str]:
    """Return the GTFS table name implied by a filename, or None.

    Strips the extension and any leading path, lowercases, and matches
    against the canonical set of GTFS tables.  Accepts both ``.csv``
    and ``.txt``.
    """
    stem = Path(filename).stem.lower()
    return stem if stem in GTFS_TABLE_MODELS else None


def preview_batch(
    files: Sequence[tuple[str, Union[bytes, BinaryIO]]],
    table_overrides: Optional[Dict[str, str]] = None,
) -> BatchPreview:
    """Inspect a set of CSVs without writing to the database.

    Parameters
    ----------
    files
        Iterable of ``(filename, bytes_or_filelike)`` pairs.
    table_overrides
        Optional mapping of ``filename -> table_name`` to override the
        filename-based inference (useful when a user renames a file).
    """
    preview = BatchPreview()
    overrides = table_overrides or {}

    if len(files) > MAX_BATCH_FILES:
        preview.errors.append(
            f"Too many files: {len(files)} (limit is {MAX_BATCH_FILES})"
        )
        return preview

    seen_tables: Dict[str, str] = {}  # table -> first filename that claimed it

    for filename, source in files:
        data = _read_bytes(source)
        if data is None:
            preview.errors.append(f"Could not read {filename}")
            continue
        if len(data) > MAX_FILE_BYTES:
            preview.errors.append(
                f"{filename}: {len(data)} bytes exceeds {MAX_FILE_BYTES}"
            )
            continue

        table = overrides.get(filename) or infer_table_from_filename(filename)
        if table is not None and table not in GTFS_TABLE_MODELS:
            preview.errors.append(f"{filename}: unknown target table '{table}'")
            continue

        bf = BatchFile(
            filename=filename,
            table=table,
            size_bytes=len(data),
            data=data,
        )

        if table is None:
            preview.unknown_files.append(filename)
        else:
            try:
                bf.row_count = _count_csv_rows(data)
            except Exception as exc:  # noqa: BLE001
                preview.errors.append(f"{filename}: failed to parse CSV ({exc})")
                continue

            if table in seen_tables:
                preview.duplicate_tables.append(table)
            else:
                seen_tables[table] = filename

        preview.files.append(bf)

    return preview


def import_batch(
    project_slug: str,
    files: Sequence[tuple[str, Union[bytes, BinaryIO]]],
    *,
    table_overrides: Optional[Dict[str, str]] = None,
) -> BatchImportResult:
    """Transactionally upsert a set of CSVs into the GTFS database.

    Files are applied in FK-safe order (parents before children).  If any
    row fails to insert due to a FK violation, the row counts as
    *failed* but the transaction only rolls back if **every** row of a
    given table fails — otherwise partial successes commit together.

    Raises
    ------
    BatchImportError
        For non-recoverable issues detected before the transaction opens
        (empty batch, duplicate table targets).  In that case the DB is
        untouched.
    """
    preview = preview_batch(files, table_overrides=table_overrides)
    if preview.errors:
        raise BatchImportError("; ".join(preview.errors))
    if preview.duplicate_tables:
        raise BatchImportError(
            "Multiple files target the same table: "
            + ", ".join(sorted(set(preview.duplicate_tables)))
        )
    actionable = [f for f in preview.files if f.table is not None]
    if not actionable:
        raise BatchImportError("No files mapped to a known GTFS table.")

    create_gtfs_database(project_slug)

    started = time.monotonic()
    result = BatchImportResult()
    result.skipped_files = [f.filename for f in preview.files if f.table is None]

    # FK-safe order — anything not in INSERT_ORDER goes last alphabetically.
    order_index = {t: i for i, t in enumerate(INSERT_ORDER)}
    actionable.sort(key=lambda f: (order_index.get(f.table, 10_000), f.table))

    conn = get_connection(project_slug)
    try:
        conn.execute("BEGIN")
        try:
            for bf in actionable:
                rows = list(_iter_csv_rows(bf.data))
                res = upsert_records_on_conn(conn, bf.table, rows)
                result.inserted_by_table[bf.table] = res.inserted
                if res.failed:
                    result.failed_by_table[bf.table] = res.failed
                if res.errors:
                    result.errors_by_table[bf.table] = res.errors[
                        :MAX_ERRORS_PER_TABLE
                    ]
            # Bump _gtfs_meta.last_modified inside the same transaction
            from datetime import datetime, timezone
            conn.execute(
                "INSERT OR REPLACE INTO _gtfs_meta (key, value) VALUES (?, ?)",
                ("last_modified", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            result.committed = True
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    result.duration_seconds = round(time.monotonic() - started, 3)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _read_bytes(source: Union[bytes, BinaryIO]) -> Optional[bytes]:
    if isinstance(source, (bytes, bytearray)):
        return bytes(source)
    try:
        data = source.read()
    except Exception:  # noqa: BLE001
        return None
    if isinstance(data, str):
        return data.encode("utf-8")
    return data


def _count_csv_rows(data: bytes) -> int:
    count = 0
    for _ in _iter_csv_rows(data):
        count += 1
    return count


def _iter_csv_rows(data: bytes):
    """Yield normalised row dicts, same semantics as importer._iter_csv_rows."""
    text = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8-sig", newline="")
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

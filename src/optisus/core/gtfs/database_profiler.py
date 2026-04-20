"""
Read-only profiling for the GTFS SQLite database.

Powers the Database Overview dashboard (Module 3).  Lives alongside
``database.py`` (CRUD) and ``analytics.py`` (gtfs-kit bridge) so each
module stays focused on one concern.

Two entry points:

* :func:`profile_database` — cheap (row counts + integrity + meta).
  Cached on ``(db_path, mtime)`` so repeated UI reruns are instant
  until the database actually changes.
* :func:`profile_table_columns` — expensive (per-column null counts,
  distinct counts, sample values).  Called lazily only when the user
  drills into a specific table.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from functools import lru_cache
from typing import List, Optional

from optisus.core.gtfs.database import (
    _FK_CHECKS,
    _TABLE_COLUMNS,
    _TABLE_PK,
    check_integrity,
    get_connection,
    get_database_summary,
    get_gtfs_db_path,
)
from optisus.core.storage.layers import PROJECTS_ROOT


# ═══════════════════════════════════════════════════════════════════════════
# Data classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ColumnProfile:
    """Per-column statistics for a single GTFS table."""
    name: str
    dtype: str
    not_null: bool
    is_primary_key: bool
    null_count: int
    null_pct: float
    distinct_count: int
    sample_values: List[str] = field(default_factory=list)


@dataclass
class TableProfile:
    """Row counts + schema metadata for one table (no column stats)."""
    table_name: str
    row_count: int
    column_count: int
    has_primary_key: bool
    fk_references: List[str] = field(default_factory=list)


@dataclass
class DatabaseProfile:
    """Complete database overview — cheap to compute."""
    project_slug: str
    project_name: str
    db_path: str
    exists: bool
    db_size_bytes: int = 0
    schema_version: str = ""
    created_at: str = ""
    last_modified: str = ""
    total_records: int = 0
    total_tables: int = 0
    populated_tables: int = 0
    empty_tables: int = 0
    tables: List[TableProfile] = field(default_factory=list)
    integrity_clean: bool = True
    violation_count: int = 0
    largest_table: str = ""
    smallest_populated_table: str = ""
    completeness_pct: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def profile_database(project_slug: str) -> DatabaseProfile:
    """Return a full (but cheap) profile of the project's GTFS database.

    Result is cached on the database file's ``(path, mtime)`` signature,
    so calls from the Streamlit UI are effectively free after the first
    one until the DB is written to.
    """
    db_path = get_gtfs_db_path(project_slug)
    if not db_path.exists():
        return DatabaseProfile(
            project_slug=project_slug,
            project_name=_project_name(project_slug),
            db_path=str(db_path),
            exists=False,
        )

    sig = (str(db_path), db_path.stat().st_mtime)
    return _cached_profile(project_slug, sig)


def profile_table_columns(
    project_slug: str,
    table_name: str,
    sample_limit: int = 5,
) -> List[ColumnProfile]:
    """Return per-column statistics for a single table.

    Expensive: runs ``COUNT(DISTINCT)`` and a null count per column.
    Cached on ``(db_path, mtime, table_name, sample_limit)``.
    """
    db_path = get_gtfs_db_path(project_slug)
    if not db_path.exists():
        return []
    sig = (str(db_path), db_path.stat().st_mtime)
    return _cached_columns(project_slug, table_name, sample_limit, sig)


def clear_profile_cache() -> None:
    """Clear memoised profiles (useful for tests)."""
    _cached_profile.cache_clear()
    _cached_columns.cache_clear()


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _project_name(project_slug: str) -> str:
    """Return the display name from project.json, falling back to the slug."""
    meta_path = PROJECTS_ROOT / project_slug / "project.json"
    if not meta_path.exists():
        return project_slug
    try:
        import json
        return json.loads(meta_path.read_text()).get("name", project_slug)
    except Exception:
        return project_slug


@lru_cache(maxsize=32)
def _cached_profile(project_slug: str, sig: tuple) -> DatabaseProfile:
    del sig  # cache-key only
    summary = get_database_summary(project_slug)
    db_path = get_gtfs_db_path(project_slug)
    counts = summary.get("table_counts", {})

    # Map: child table -> list of parent tables it references
    fk_by_table: dict[str, List[str]] = {}
    for fk in _FK_CHECKS:
        fk_by_table.setdefault(fk["table"], []).append(fk["ref_table"])

    tables: List[TableProfile] = []
    for tbl, cols in _TABLE_COLUMNS.items():
        refs = sorted(set(fk_by_table.get(tbl, [])))
        tables.append(TableProfile(
            table_name=tbl,
            row_count=counts.get(tbl, 0),
            column_count=len(cols),
            has_primary_key=bool(_TABLE_PK.get(tbl)),
            fk_references=refs,
        ))

    populated = [t for t in tables if t.row_count > 0]
    empty = [t for t in tables if t.row_count == 0]

    largest = max(populated, key=lambda t: t.row_count).table_name if populated else ""
    smallest = min(populated, key=lambda t: t.row_count).table_name if populated else ""

    total_tables = len(tables)
    completeness = (len(populated) / total_tables * 100.0) if total_tables else 0.0

    return DatabaseProfile(
        project_slug=project_slug,
        project_name=_project_name(project_slug),
        db_path=str(db_path),
        exists=True,
        db_size_bytes=summary.get("size_bytes", 0),
        schema_version=summary.get("schema_version", "unknown"),
        created_at=summary.get("created_at", ""),
        last_modified=summary.get("last_modified", ""),
        total_records=summary.get("total_records", 0),
        total_tables=total_tables,
        populated_tables=len(populated),
        empty_tables=len(empty),
        tables=tables,
        integrity_clean=summary.get("integrity_clean", True),
        violation_count=summary.get("violation_count", 0),
        largest_table=largest,
        smallest_populated_table=smallest,
        completeness_pct=completeness,
    )


@lru_cache(maxsize=64)
def _cached_columns(
    project_slug: str,
    table_name: str,
    sample_limit: int,
    sig: tuple,
) -> List[ColumnProfile]:
    del sig  # cache-key only
    if table_name not in _TABLE_COLUMNS:
        return []

    conn = get_connection(project_slug)
    try:
        # Total rows — used for null-pct denominator
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cur.fetchone()[0]
        except sqlite3.OperationalError:
            return []

        # Column metadata (name, type, notnull, pk)
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        col_meta = [
            {
                "name": r["name"],
                "dtype": r["type"] or "",
                "not_null": bool(r["notnull"]),
                "is_pk": bool(r["pk"]),
            }
            for r in cur.fetchall()
        ]

        results: List[ColumnProfile] = []
        for cm in col_meta:
            col = cm["name"]
            if total == 0:
                results.append(ColumnProfile(
                    name=col,
                    dtype=cm["dtype"],
                    not_null=cm["not_null"],
                    is_primary_key=cm["is_pk"],
                    null_count=0,
                    null_pct=0.0,
                    distinct_count=0,
                    sample_values=[],
                ))
                continue

            cur = conn.execute(
                f'SELECT COUNT(*) FROM {table_name} WHERE "{col}" IS NULL'
            )
            nulls = cur.fetchone()[0]
            cur = conn.execute(
                f'SELECT COUNT(DISTINCT "{col}") FROM {table_name}'
            )
            distinct = cur.fetchone()[0]
            cur = conn.execute(
                f'SELECT DISTINCT "{col}" FROM {table_name} '
                f'WHERE "{col}" IS NOT NULL LIMIT ?',
                (sample_limit,),
            )
            samples = [_stringify(row[0]) for row in cur.fetchall()]

            results.append(ColumnProfile(
                name=col,
                dtype=cm["dtype"],
                not_null=cm["not_null"],
                is_primary_key=cm["is_pk"],
                null_count=nulls,
                null_pct=(nulls / total * 100.0) if total else 0.0,
                distinct_count=distinct,
                sample_values=samples,
            ))
        return results
    finally:
        conn.close()


def _stringify(val) -> str:
    if val is None:
        return ""
    s = str(val)
    return s if len(s) <= 40 else s[:37] + "…"

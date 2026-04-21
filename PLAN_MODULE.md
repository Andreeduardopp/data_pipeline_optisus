# PID-GTFS — Standalone Package Plan (v2)

## Goal

Extract the GTFS core from `optisus` into a single importable Python package — **PID-GTFS** (import name `pid_gtfs`) — that a user can drop into their own project and use without installing the Streamlit app or the data-lake conventions.

The `optisus` app keeps its current GTFS code untouched for now. PID-GTFS is a clean copy that evolves independently; no back-integration and no PyPI release in this phase.

First release covers three concerns only: **schemas**, **ingestion**, and **extraction**. Validator is included (cheap, no extra deps, and an "extract without validate" API is a footgun). Analytics, mapper, and batch import are out of scope for v1.

---

## 1. Scope

| Module | Source file | Lines | Role |
|---|---|---|---|
| Schemas | `core/schemas/gtfs.py` | 829 | 15 Pydantic models, enums, time/date/color validators |
| DB engine | `core/gtfs/database.py` | 725 | SQLite create, upsert, query, integrity |
| Ingestion | `core/gtfs/importer.py` | 369 | ZIP preview + import (REPLACE / MERGE / ABORT) |
| Extraction | `core/gtfs/exporter.py` | 591 | ZIP export, subset export, completeness |
| Validator | `core/gtfs/validator.py` | 450 | Spec-rule checks (kept to back pre-export validation) |

**Out of v1:** `analytics.py`, `mapper.py`, `batch_import.py`, `database_profiler.py`. These either depend on heavy third-party libraries (gtfs-kit) or are tied to the Silver→GTFS pipeline.

---

## 2. Couplings to remove

Three concrete edits unblock extraction. All other code in the five kept files is already self-contained.

### 2.1 Drop `PROJECTS_ROOT` imports

- `database.py:23` → `from optisus.core.storage.layers import PROJECTS_ROOT`
- `exporter.py:30` → same import

Both files use `PROJECTS_ROOT / project_slug / "gtfs.db"` to resolve paths. Replace with a user-supplied path.

### 2.2 Replace `project_slug: str` plumbing with an explicit DB handle

Every public function in `database.py`, `importer.py`, `exporter.py` takes `project_slug: str` and internally calls `get_gtfs_db_path(project_slug)`. For a library this is inverted: the caller owns the path.

Introduce a small class that holds the path and exposes the existing functions as methods. The slug-as-db-name ergonomics the user asked for are preserved via a classmethod.

```python
class GtfsDatabase:
    def __init__(self, db_path: Path): ...

    @classmethod
    def from_slug(cls, slug: str, root: Path = Path.cwd()) -> "GtfsDatabase":
        return cls(root / f"{slug}.db")
```

Existing module-level functions become thin wrappers that call `GtfsDatabase(path).method(...)`, or are deleted if the class covers them.

### 2.3 Make exporter's pre-export validation optional but default-on

`exporter.py:29` imports `validate_gtfs_feed` from the validator. Since validator stays in v1, no import change is needed — but expose a `validate: bool = True` flag on `export_zip` so users can skip validation if they know what they are doing.

---

## 3. Public API (v1 surface)

One top-level import path. No submodule drilling required for common tasks.

```python
from pid_gtfs import (
    GtfsDatabase,
    preview_zip,
    import_zip,
    export_zip,
    ImportMode,
)
from pid_gtfs.schemas import (
    GtfsAgency, GtfsStop, GtfsRoute, GtfsTrip, GtfsStopTime,
    GtfsCalendar, GtfsCalendarDate, GtfsShape, GtfsFrequency,
    GtfsTransfer, GtfsFeedInfo,
    GTFS_TABLE_MODELS,
)
```

### 3.1 Quickstart

```python
from pid_gtfs import GtfsDatabase, import_zip, export_zip

db = GtfsDatabase("my_feed.db")                  # creates schema on first use
result = import_zip(db, "source.zip", mode="replace")
print(result.inserted, result.failed)

stops = db.get_records("stops", limit=100)
report = db.check_integrity()
assert report.is_clean

export_zip(db, "out.zip", validate=True)
```

### 3.2 `GtfsDatabase` methods (wrap existing `database.py` functions)

| Method | Wraps | Returns |
|---|---|---|
| `__init__(db_path)` | `create_gtfs_database` | — |
| `from_slug(slug, root)` | helper | `GtfsDatabase` |
| `upsert(table, records)` | `upsert_records` | `InsertResult` |
| `get_records(table, limit, offset)` | `get_table_records` | `list[dict]` |
| `count(table)` | `get_table_count` | `int` |
| `delete(table, where)` | `delete_records` | `int` |
| `clear(table)` | `clear_table` | `int` |
| `clear_all()` | `clear_all_tables` | `dict[str, int]` |
| `check_integrity()` | `check_integrity` | `IntegrityReport` |
| `summary()` | `get_database_summary` | `dict` |

### 3.3 Top-level functions

| Function | Wraps | Notes |
|---|---|---|
| `preview_zip(source)` | `preview_gtfs_zip` | Accepts path or `BinaryIO` |
| `import_zip(db, source, mode, …)` | `import_gtfs_zip` | Takes `GtfsDatabase`, not slug |
| `export_zip(db, out_path, validate=True)` | `export_gtfs_feed` | Writes single zip, not timestamped dir |

The existing exporter writes to `projects/<slug>/exports/latest/…` — v1 lets the caller pass an explicit output zip path instead.

Date- and route-restricted `export_subset` is **deferred to v2** because it depends on `gtfs-kit`. Keeping v1 stdlib+pydantic only avoids a heavy transitive dependency chain (pandas, shapely, folium). Full-feed export covers the primary user need.

---

## 4. Package layout

The package lives in a new top-level directory at the repo root (sibling to `data_pipeline_optisus/` and `GTFS_UserGuide/`):

```
PID-GTFS/
├── pyproject.toml              # name = "pid-gtfs", pydantic>=2 only
├── README.md                   # install + quickstart
├── src/
│   └── pid_gtfs/
│       ├── __init__.py         # re-exports the v1 surface
│       ├── schemas.py          # copied verbatim from core/schemas/gtfs.py
│       ├── database.py         # core/gtfs/database.py with PROJECTS_ROOT removed
│       ├── importer.py         # core/gtfs/importer.py, slug→db-handle
│       ├── exporter.py         # core/gtfs/exporter.py, slug→db-handle, path-based output
│       ├── validator.py        # core/gtfs/validator.py verbatim
│       └── py.typed            # PEP 561 marker
└── tests/
    ├── test_schemas.py
    ├── test_database.py
    ├── test_importer.py
    ├── test_exporter.py
    └── test_integration.py     # import → query → export round-trip
```

Distribution name: `pid-gtfs`. Import name: `pid_gtfs`. Dependencies: `pydantic>=2` only — everything else is stdlib (`sqlite3`, `csv`, `zipfile`, `pathlib`, `logging`).

---

## 5. Migration steps

Order matters — each step is independently testable. Since `optisus` is not being migrated in this phase, all edits are in the new `PID-GTFS/` directory; the original `data_pipeline_optisus/src/optisus/core/gtfs/` and `core/schemas/gtfs.py` are left untouched.

1. **Scaffold the package:** create `PID-GTFS/` with `pyproject.toml` (`name = "pid-gtfs"`, `pydantic>=2`), `src/pid_gtfs/`, and `tests/`.
2. **Copy `schemas/gtfs.py` → `src/pid_gtfs/schemas.py`** unchanged. Run the copied `test_schemas.py` to confirm no hidden imports.
3. **Copy `gtfs/validator.py` → `src/pid_gtfs/validator.py`** unchanged. Only imports schemas.
4. **Port `database.py`:**
   - Remove `from optisus.core.storage.layers import PROJECTS_ROOT`.
   - Introduce `GtfsDatabase` class holding `db_path`. Each method mirrors the current module-level function but takes `self.db_path` instead of resolving from slug.
   - Delete the module-level `project_slug`-based functions outright. No compatibility shims — `optisus` has its own copy.
5. **Port `importer.py`:** replace `project_slug: str` with `db: GtfsDatabase`. Internals that call `get_connection(project_slug)` become `db.connect()`.
6. **Port `exporter.py`:** same slug→handle swap. Add explicit `out_path: Path` parameter and `validate: bool = True` flag. Drop the `exports/latest` vs. timestamped directory logic — caller decides the output path.
7. **Write `__init__.py` re-exports** for the v1 surface listed in §3.
8. **Copy tests** from `tests/test_gtfs_*.py`. Adapt the `isolated_gtfs` fixture to hand out a `GtfsDatabase(tmp_path / "test.db")` directly instead of patching `PROJECTS_ROOT`.
9. **Write quickstart README** at `PID-GTFS/README.md`.
10. **Reference from `GTFS_UserGuide/`:** the user guide links to `PID-GTFS/` and includes a copy-or-clone quickstart. No PyPI release in this phase.

---

## 6. What this plan deliberately does not do

- Does not introduce a CLI. Library only.
- Does not add new validation rules beyond what `validator.py` already does.
- Does not touch the Silver→GTFS mapper — that logic stays in `optisus` because it encodes data-lake conventions that do not generalize.
- Does not wrap `gtfs-kit` — analytics is a later module.
- Does not modify `optisus`. The app keeps its current GTFS code; PID-GTFS is a clean copy.
- Does not publish to PyPI. Distribution is source-only (copy or clone) during this phase.

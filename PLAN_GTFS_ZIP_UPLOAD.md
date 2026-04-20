# Feature Plan — Upload GTFS ZIP → Populate Project Database

## Goal

Allow a user to upload a complete GTFS `.zip` feed on the **GTFS Data Maturity** page and have the project's SQLite database (`data_lake_outputs/projects/<slug>/gtfs.db`) populated from its `.txt` files, in one action. This is an alternative to the existing *Silver → GTFS mapping* flow and to the per-table CSV upload in Section 3 — useful for projects that already have a third-party GTFS feed and want to jump straight to validation, analytics, and exports.

## User story

> As a transit analyst I have a GTFS feed from another tool (e.g. an agency's public feed, or a feed produced by a scheduling product). I want to drop the `.zip` into the app, see what was loaded, and from there use the maturity dashboard, validator, analytics, maps, and exports exactly as if I had mapped from Silver.

## Non-goals

- Not a replacement for the Silver-data mapping flow — both continue to coexist.
- Not a GTFS-ride ingestion flow (the four extension tables are still populated only by the Silver mapper).
- Not a multi-file merge / diff tool. Import is single-feed.
- Not a streaming importer — we target feeds that fit comfortably in memory (hundreds of MB of `stop_times.txt` is the realistic upper bound).

## UX

A new section on the GTFS page, positioned **before** the existing *Direct GTFS Upload* (per-table CSV) section, named:

> **Section 3a — Import complete GTFS feed (.zip)**

Flow:

1. `st.file_uploader(type=["zip"])`.
2. On upload, a **Preview** panel appears:
   - Detected `.txt` files in the archive (one badge per file, green if recognised, grey if unknown).
   - Row counts per recognised table (read the headers + a `sum(1 for _ in reader)` pass — no full parse yet).
   - Required-file check (`agency`, `stops`, `routes`, `trips`, `stop_times`, plus `calendar` **or** `calendar_dates`). Missing required files → red banner, *Import* button disabled.
3. **Import mode** radio (only shown when the DB already has records):
   - `Replace` — clear every target table before insert (default when DB is empty).
   - `Merge` — upsert; existing rows with the same primary key are overwritten.
   - `Abort if not empty` — refuse to import unless the DB is empty (safest default for re-runs).
4. **Import** button → progress bar iterating tables in FK-safe order; final summary metrics (inserted / updated / failed / skipped tables) + an expandable error list capped at 50 rows per table.
5. After success, the page reruns so the existing Maturity Dashboard, Feed Completeness, Integrity Report, Analytics, and Export sections pick up the new DB state automatically.

## Architecture

### Where the new code lives

| File | Change |
|---|---|
| `src/optisus/core/gtfs/importer.py` | **New.** Pure-Python importer — takes a path or file-like object, validates archive structure, parses, and calls the existing `create_gtfs_database` + `upsert_records` + `clear_table`. |
| `src/optisus/core/gtfs/database.py` | Add a small `clear_all_tables(project_slug)` helper for `Replace` mode (currently only `clear_table` per table exists). |
| `src/optisus/ui/pages/gtfs_pipeline.py` | New section 3a rendering the uploader, preview, and import button. |
| `tests/test_gtfs_importer.py` | **New.** Unit + integration tests (see Testing). |

Keeping the importer in `core` means it is usable from a script or notebook without Streamlit — consistent with the existing core/ui split.

### Public API of `core/gtfs/importer.py`

```python
@dataclass
class GtfsZipPreview:
    is_valid: bool                       # required files present + all CSVs parseable
    recognised_tables: dict[str, int]    # table_name → row_count
    unknown_files: list[str]             # .txt files we will ignore
    missing_required: list[str]          # required files not found in the archive
    errors: list[str]                    # structural errors (corrupt zip, bad encoding)

@dataclass
class GtfsImportResult:
    inserted_by_table: dict[str, int]
    failed_by_table: dict[str, int]
    cleared_tables: list[str]
    skipped_tables: list[str]            # optional tables we did not touch
    errors_by_table: dict[str, list[str]]
    duration_seconds: float

class ImportMode(str, Enum):
    REPLACE = "replace"
    MERGE = "merge"
    ABORT_IF_NOT_EMPTY = "abort_if_not_empty"

def preview_gtfs_zip(source: str | Path | BinaryIO) -> GtfsZipPreview: ...

def import_gtfs_zip(
    project_slug: str,
    source: str | Path | BinaryIO,
    *,
    mode: ImportMode = ImportMode.REPLACE,
    chunk_size: int = 5_000,
) -> GtfsImportResult: ...
```

### Import algorithm

1. Open the archive (`zipfile.ZipFile`). Reject if it is not a valid zip.
2. Build a map `{table_name → ZipInfo}` by matching each `*.txt` member's stem against `GTFS_TABLE_MODELS`. Unknown `.txt` files go into `unknown_files`; non-`.txt` members (e.g. stray `modifications.json`) are ignored.
3. Validate required-file set: `{agency, stops, routes, trips, stop_times}` plus at least one of `{calendar, calendar_dates}`. If missing, raise `GtfsImportError` (importer) / show a red banner (UI).
4. `create_gtfs_database(slug)` — idempotent, guarantees schema exists.
5. Apply the selected import mode:
   - `ABORT_IF_NOT_EMPTY` — call `get_database_summary` and bail out if any core table is non-empty.
   - `REPLACE` — `clear_all_tables(slug)` **in reverse FK order** (stop_times → trips → routes, …) to avoid integrity failures during clear.
   - `MERGE` — no pre-clear.
6. Iterate tables **in FK-safe insert order** (parents before children). Canonical order:

   ```
   agency, feed_info, calendar, calendar_dates, shapes, stops,
   routes, trips, frequencies, transfers, stop_times
   ```

7. For each table:
   - Stream the CSV with `csv.DictReader` (UTF-8 with BOM tolerance).
   - Normalise each row: empty strings → `None` (SQLite null), strip BOM / surrounding whitespace on keys.
   - Chunk into `chunk_size`-row batches and call the existing `upsert_records(slug, table, batch)` — that already handles Pydantic validation, column filtering, and the `INSERT OR REPLACE`.
   - Accumulate counts and the first 50 error strings per table.
8. Return `GtfsImportResult`.

### Why delegate to `upsert_records`

`upsert_records` already:
- validates every row with the correct Pydantic model,
- filters to known columns (so extra GTFS columns we don't model are silently dropped),
- produces a consistent `InsertResult`,
- updates `_gtfs_meta.last_modified` so the `analytics.db_signature` cache invalidates automatically.

Re-using it keeps the importer thin and guarantees the same validation semantics as the per-table CSV uploader and the Silver mapper.

### FK-safety notes

- `PRAGMA foreign_keys = ON` is enabled in `get_connection`. Inserting in the order above satisfies every FK in the core schema.
- GTFS-ride tables (`board_alight`, `ridership`, `ride_feed_info`, `trip_capacity`) are **not** in the standard `.zip` spec. If a feed happens to ship them, we import them last (after all core tables), but they remain optional.
- The `upsert_records` call wraps each table in its own transaction via `conn.commit()` at the end, so partial failures in one table do not corrupt the ones already loaded.

## Edge cases and decisions

| Case | Decision |
|---|---|
| Zip contains nested folder (e.g. `feed/agency.txt`) | Accept — match on `Path(member).name`, not full path. Common in feeds downloaded from transit APIs. |
| Mixed line endings (`\r\n` vs `\n`) | `csv.DictReader` handles both; do not pre-normalise. |
| UTF-8 BOM on first column name | Strip `\ufeff` from the first header. |
| Extremely large `stop_times.txt` | Stream + chunk; do not `pd.read_csv` the whole thing. A single 500 MB file is the realistic worst case. |
| File names with different case (`Agency.txt`) | Lower-case the stem before matching. |
| `feed_info.txt` missing | Allowed — it is optional in the spec. |
| `shapes.txt` missing | Allowed — shapes are optional (just hides the route geometry map). |
| Both `calendar` and `calendar_dates` missing | Reject in preview — at least one service-definition file is required. |
| Unknown columns in a known table | Silently dropped by `upsert_records` (pre-existing behaviour). |
| User imports into a project where Silver→GTFS mapping has already populated the DB with `MERGE` | Allowed — PK collisions overwrite the existing row. We surface the overwrite count in a small warning. |
| Import fails mid-run (e.g. malformed row in `stop_times`) | Rows already committed stay. The UI shows per-table success / failure so the user can decide to re-import. No automatic rollback across tables — matches the existing mapper's semantics. |
| Zip is really a single `.txt` renamed to `.zip` | `ZipFile` raises `BadZipFile` → surfaced as a clean error. |
| Very small feeds from `gtfs-kit` sample data | Must work — used in tests. |

## Security / resource guards

- Reject archives above a configurable size (default **500 MB**) — prevents accidental uploads of multi-GB feeds that would OOM Streamlit.
- Reject archives with more than **50 members** or any individual member above 1 GB uncompressed (zip-bomb guard — check `ZipInfo.file_size` before reading).
- Read each member through `ZipFile.open` (streaming) — never `ZipFile.extractall`.
- Never write the uploaded bytes to disk outside of the import transaction; the feed goes straight from the uploaded file-like into the DB.

## Testing

New file `tests/test_gtfs_importer.py`, using the existing `isolated_gtfs` fixture pattern:

1. **Preview happy path** — build a zip in-memory from the repo's `samples/` CSVs, assert `recognised_tables` counts match, `missing_required` is empty.
2. **Preview rejects missing required file** — remove `stops.txt`, assert `is_valid=False` and `stops` is in `missing_required`.
3. **Preview tolerates nested folder** — zip members under `feed/agency.txt`, assert still recognised.
4. **Import REPLACE on empty DB** — assert per-table row counts match source CSVs and `check_integrity` returns clean.
5. **Import REPLACE on populated DB** — pre-populate via the mapper, then re-import; assert `cleared_tables` covers every core table and final counts match the zip (not the mapper output).
6. **Import MERGE** — pre-populate with two stops, import a zip with one of those stops modified; assert count increases and the modified stop's field reflects the zip value.
7. **Import ABORT_IF_NOT_EMPTY** — pre-populate and assert `GtfsImportError` is raised with a clear message, DB untouched.
8. **FK-safe insert order** — import a feed where `stop_times` rows reference `trips` defined later alphabetically; assert no integrity error (covers the canonical order).
9. **Malformed row reported but others succeed** — inject a bad `route_type` into one routes row; assert that row is in `errors_by_table['routes']` and the remaining rows are inserted.
10. **`db_signature` changes after import** — guards the analytics cache invalidation.
11. **Zip-bomb guard** — construct a member with `file_size = 2 * 1024**3` (fake via `ZipInfo`), assert a clear rejection before parsing.
12. **Round-trip** — export with `export_gtfs_feed`, import the result into a fresh project, assert the two `get_database_summary` outputs are structurally identical (same table counts).

UI smoke test (manual, documented under the section's Test plan in the eventual PR): upload a small published feed (e.g. a San Francisco Muni sample) and confirm the maturity dashboard lights up without errors.

## Documentation

- Add a short "Import an existing GTFS feed" subsection to the *Programmatic usage* block in `README.md` showing the `import_gtfs_zip` call from a script.
- Extend the GTFS-pipeline paragraph in `CLAUDE.md` to list `core.gtfs.importer` alongside `mapper`, `exporter`, `validator`, `analytics`.

## Rollout

Single PR is fine — the change is additive, core-only on the backend, and one new section on one page. No data migration, no config flag.

## Implementation checklist

- [ ] `core/gtfs/importer.py` — `preview_gtfs_zip`, `import_gtfs_zip`, dataclasses, `ImportMode` enum.
- [ ] `core/gtfs/database.py` — `clear_all_tables` helper (reverse FK order).
- [ ] `tests/test_gtfs_importer.py` — the 12 cases above.
- [ ] `ui/pages/gtfs_pipeline.py` — Section 3a renderer, state reset after successful import.
- [ ] `README.md` / `CLAUDE.md` — mention the importer module.

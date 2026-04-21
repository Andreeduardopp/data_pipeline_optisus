# Core GTFS Engine

This directory contains the central business logic and data operations for managing General Transit Feed Specification (GTFS) data within Optisus. It functions as the bridge between raw transit data, the relational SQLite database, and compliant feed exports.

## Files and their Importance

### `analytics.py`
**Importance:** Acts as an adapter connecting the internal GTFS SQLite database to the external `gtfs-kit` library. Enables advanced spatial and statistical analyses without duplicating feed parsing logic.
**Key Methods:**
- `feed_from_db()`: Instantiates a `gtfs-kit` Feed directly from the project's SQLite DB.
- `feed_from_zip()`: Reads a raw GTFS Zip into a `gtfs-kit` Feed.
- `compute_analytics()`: Generates summary statistics and health indicators (quality, density, active dates) to power the frontend dashboards.
- `build_routes_map()`, `build_stops_map()`: Generate map visualizations dynamically representing network coverage.

### `batch_import.py`
**Importance:** Enables multi-file CSV uploads to be processed seamlessly and transactionally, handling relationships safely and rolling back completely on any table-level failure.
**Key Methods:**
- `infer_table_from_filename()`: Safely identifies target tables based on loose file naming conventions.
- `preview_batch()`: Parses file headers and sizes prior to import, identifying missing relationships or bad targets without making database state changes.
- `import_batch()`: Transactionally upserts all files into their assigned SQLite tables using FK-safe insertion order constraints.

### `database.py`
**Importance:** The foundational persistence layer. Defines the rigid GTFS SQL schema representing the Level 3 (Relational) data maturity state of the platform.
**Key Methods:**
- `get_connection()`: Provides SQLite connections rigidly enforcing `PRAGMA foreign_keys = ON`.
- `create_gtfs_database()`: Initializes all tables including GTFS-ride extensions.
- `upsert_records()` & `upsert_records_on_conn()`: Validates records using the Pydantic schemas and executes bulk database-writes.
- `check_integrity()`: Validates internal relationship orphans (e.g. routes pointing to a nonexistent agency).
- `get_database_summary()`: Produces rapid database metrics (row counts, file sizes, last modified times).

### `database_profiler.py`
**Importance:** Provides read-only analytical caching to keep the frontend "Module 3" overview snappy without executing expensive queries against large datasets on every page load.
**Key Methods:**
- `profile_database()`: A highly efficient and LRU-cached aggregation of table sizes and metrics.
- `profile_table_columns()`: A lazy-loaded, deep column analysis reporting NULL percentages and distinct sample values for sophisticated debugging.

### `exporter.py`
**Importance:** Serves as the "output" terminal of the application, taking the internal database schemas and transforming them back into spec-compliant `.zip` deliverables.
**Key Methods:**
- `export_gtfs_feed()`: Produces a fully compliant export. Coordinates schema checks, CSV encoding rules (managing NULLs), and Zip packaging logic.
- `export_gtfs_subset()`: Integrates with `gtfs-kit` to output time-bounded or route-bounded subsets of the data securely.
- `compute_feed_completeness()`: Weights populated tables to assign a global "Completeness Score", highlighting how robust a feed is compared to the minimum viable product.

### `importer.py`
**Importance:** Ingests third-party GTFS archives into the platform safely without consuming massive amounts of RAM, preventing zip-bomb attacks, processing sizes incrementally, and running structural validations beforehand.
**Key Methods:**
- `preview_gtfs_zip()`: Performs dry-run inspections, extracting member sizes and mandatory dependencies without disk expansion.
- `import_gtfs_zip()`: Orders inserts hierarchically based on foreign-key dependency trees, utilizing file streams off disk.

### `mapper.py`
**Importance:** The core "ETL Orchestrator". It reads from the Level 2 "Silver Parquet" artifacts mapping standard fields back into specialized GTFS constraints (Level 3).
**Key Methods:**
- `map_project_to_gtfs()`: Main job orchestrator connecting localized Silver dataset formats to the unified GTFS layout.
- Individual conversion mappers (`map_stops`, `map_transfers`, `map_calendar_dates`, `map_routes`, `map_trips`, `map_stop_times`, `map_agency`, `map_board_alight`): Run unique logic like interpolation (for `stop_times` dwell durations calculations), and strict identifier deduplication tracking.

### `validator.py`
**Importance:** A python-based lightweight GTFS validation standard checker, allowing the interface to guarantee basic consistency and data quality.
**Key Methods:**
- `validate_gtfs_feed()`: Top level engine evaluating uniqueness constraints, header availability, file inclusions, and types.
- Underlying logic checkers (`_check_referential_integrity`, `_check_time_format`, `_check_stop_sequence_monotonic`, etc): Guarantees the export conforms strictly to downstream routing consumer bounds.

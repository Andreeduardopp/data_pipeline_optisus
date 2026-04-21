# Modularization Plan: Independent GTFS Python Packages

This document outlines the theoretical decoupling of the GTFS core logic into independent, standalone Python packages. This allows other developers or projects to import only the specific functionality they need without adopting the entire pipeline structure.

---

## 1. `gtfs-models` (The Schema Module)
**Purpose:** Provides strictly typed, native Python representation of General Transit Feed Specification structures without requiring a database.
**Primary Use Case:** API endpoints, data ingestion gateways, or custom scripts that need to validate JSON payloads or transit entity boundaries.

### Main Methods & Classes:
* **Models (`GtfsAgency`, `GtfsStop`, `GtfsBoardAlight`, etc.)**: Object constructors that instantly flag missing fields, type errors, or non-compliant relationships based on Pydantic v2 schemas.
* **`_validate_gtfs_time()`**: Parses and accepts complex transit time strings extending past midnight (e.g., `"25:35:00"`).
* **`_validate_gtfs_date()`**: Ensures standardized `YYYYMMDD` formats.
* **`_validate_hex_color()`**: Prevents visual anomalies by cleaning and standardizing route color strings.

---

## 2. `gtfs-sqlite` (The Database Engine)
**Purpose:** Houses the physical data persistence logic, handling the creation, validation, and querying of relational GTFS databases configured tightly with Foreign Key protections.
**Primary Use Case:** Developers building local datalakes or data scientists needing a safe relational engine for complex queries.

### Main Methods & Actions:

1. **Database Initialization (`create_gtfs_database()`)**:
   - **What it does**: This method establishes the foundational, empty database structure for your GTFS project. It acts as the initial setup, creating the necessary tables with their defined schemas, primary keys, and foreign key constraints, ready to receive GTFS data.
   - **Input**: `project_slug` (A unique text identifier for your project, e.g., "nyc_transit_2026").
   - **Output**: A Path object indicating the exact location on the file system where the database file (gtfs.db) was created.

2. **Secure Data Import (`upsert_records()`)**:
   - **What it does**: This is the primary mechanism for ingesting data into the GTFS tables. It intelligently handles both new record creation and updates to existing records based on their primary keys. Crucially, all incoming data is subjected to strict validation against the GTFS specification and database schema rules before being committed, ensuring data integrity and preventing the introduction of invalid entries.
   - **Inputs**:
     - `project_slug` (Identifies the target project database).
     - `table_name` (The specific GTFS table to update, e.g., "stops" or "routes").
     - `records` (A list of dictionaries, where each dictionary represents a row of data, e.g., `[{"stop_id": "1", "stop_lat": 40.7, ...}]`).
   - **Output**: An InsertResult object, which provides a detailed receipt of the operation, including the number of records inserted, updated, failed, and a list of specific errors for any rejected rows.

3. **System Health Check (`check_integrity()`)**:
   - **What it does**: This method performs a comprehensive scan of the database to identify and report any referential integrity violations or orphaned data. It detects inconsistencies such as a trip referencing a non-existent route or a stop time referencing a non-existent stop, which are critical for maintaining a valid GTFS feed.
   - **Input**: `project_slug` (Identifies the project database to scan).
   - **Output**: An IntegrityReport object, providing a clear is_clean: True/False status. If False, it includes a detailed list of violations (e.g., "Trip #900 references Route #B which is missing").

4. **Dashboard Metrics (`get_database_summary()`)**:
   - **What it does**: Designed for rapid retrieval of high-level database statistics, this method provides a quick overview of the project's data status. It's optimized for user interface dashboards, allowing for immediate insights without requiring the download of large datasets.
   - **Input**: `project_slug`.
   - **Output**: A data dictionary summarizing the current state (e.g., `{"total_records": 45000, "table_counts": {"stops": 500}, "last_modified": "2026-04-20"}`).

5. **Reading Data (`get_table_records()`)**:
   - **What it does**: This method safely retrieves rows of data from a specified table. It supports pagination, allowing users to fetch data in manageable chunks, which is essential for working with large datasets without overwhelming system resources.
   - **Inputs**:
     - `project_slug`.
     - `table_name`.
     - `limit` & `offset` (Pagination parameters, enabling requests for specific ranges of rows, e.g., "Rows 100 to 200").
   - **Output**: A list of dictionaries, each representing a row of data exactly as stored in the target table.

6. **Safe Factory Reset (`clear_all_tables()`)**:
   - **What it does**: This method performs a controlled and safe deletion of all data within a project's database. It is designed to respect relational dependencies, ensuring that "child" records (e.g., stop_times) are deleted before their corresponding "parent" records (e.g., trips or stops), thereby preventing database integrity errors during the cleanup process.
   - **Input**: `project_slug`.
   - **Output**: A dictionary serving as a deletion receipt, detailing the number of rows wiped from each specific table (e.g., `{"stop_times": 3400, "stops": 120}`).

---

## 3. `gtfs-exchange` (The I/O & Converter Module)
**Purpose:** Bridges external file formats (CSVs, ZIPs) with the internal database engine.
**Primary Use Case:** Converting legacy static feeds to relational structures, or pulling subsets of a heavy database to distribute to the public.

### Main Methods & Actions:
* **`import_gtfs_zip(zip_path)`**: Safely unzips and ingests an entire external GTFS feed into the database, respecting the foreign-key insertion order (e.g., Agencies before Routes before Trips).
* **`import_batch(csv_files)`**: Transactional mechanism that allows a user to upload 5 independent CSV files at once, rolling back the operation completely if one table breaks.
* **`export_gtfs_feed()`**: Transpiles the SQLite database back out to a strict, spec-compliant `.zip` package.
* **`export_gtfs_subset(dates, routes)`**: Filters the SQL database geometrically/temporally, allowing a user to generate a `.zip` covering only a specific bus route on a specific weekend.
* **`compute_feed_completeness()`**: Scans available tables to weight and assign an overall dataset maturity score.

---

## 4. `gtfs-validator` (The Quality Assurance Module)
**Purpose:** Pure Python diagnostic tool for auditing raw GTFS Zip binaries against global specifications.
**Primary Use Case:** Auditing a third-party feed or running standard deployment checks before dispatching a feed to a routing vendor (e.g., Google Maps).

### Main Methods & Actions:
* **`validate_gtfs_feed(zip_path)`**: The primary orchestrator. Extracts headers, identifies types, and creates a consolidated compliance report (Warnings vs. Critical Errors).
* **`_check_referential_integrity()`**: Validates IDs linking CSVs logically.
* **`_check_stop_sequence_monotonic()`**: Identifies timeline errors where a bus reaches stop sequence "4" before stop sequence "3".

---

## 5. `gtfs-analytics` (The Analytics & Spatial Module)
**Purpose:** Provides high-level mathematical and spatial summarization using third-party adapters.
**Primary Use Case:** Deep-dive explorations, rendering map plots, or auditing NULL boundaries to find mapping blindspots.

### Main Methods & Actions:
* **`feed_from_db()`**: Acts as an adapter, piping SQLite queries instantly into `gtfs-kit` dataframes.
* **`compute_analytics()`**: Yields high-level transport indicators such as standard service dates and spatial densities.
* **`build_routes_map()` / `build_stops_map()`**: Renders geographical GeoJSON/Folium representations.
* **`profile_table_columns()`**: A lazy-loaded inspection that generates distinct value counts and strictly measures the percentage of missing strings (NULLs) across massive transit tables.

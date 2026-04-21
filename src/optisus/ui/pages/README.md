# UI Application Pages (GTFS Features)

This directory holds the interface layout defining user interactions and data presentation.

## Files and their Importance

### `gtfs_pipeline.py`
**Importance:** Serves as the primary operational dashboard ("Module 2" GTFS Data Maturity Pipeline). This is the command center where users interactively convert, import, view, and export complex operational transit datasets.

**Key Sections/Methods:**
- **Data Maturity Dashboard** (`_render_maturity_dashboard`): Visually tracks the user's progress through four distinctive processing levels (Raw -> Validated -> Relational -> Published GTFS).
- **Feed Completeness Gauge** (`_render_completeness_gauge`): Evaluates mandatory versus optimal records generating an aggregate metric of how sophisticated a generated feed holds according to the core framework (`optisus.core.gtfs.exporter`).
- **Export UI Sections** (`_render_export_section`): Directly connects interface triggers to backend pipeline scripts providing controls for generating full or subset exports bounded by routes or specific dates. 
- **Upload / Status Tools**: Reflects real-time statistics concerning the active SQLite table footprint.

### `db_overview.py`
**Importance:** Acts as "Module 3", the database architectural explorer. It breaks the detailed diagnostic analysis apart from the main operational pipeline interface. This protects the pipeline performance by moving heavy data-profiling metrics distinctively to a focused "data-profiling" environment view.

**Key Sections/Methods:**
- **Table Heatmap Grid** (`_render_heatmap`): Organizes individual elements mapping visual weight to their data density across multiple groups (e.g. Core, Service, Spatial).
- **Deep-Dive Profiler** (`_render_table_deep_dive`): Links to `database_profiler` for targeted, lazy-loaded extraction of NULL-percentages, distinct row analysis, and row-level insights.
- **Visual ER Diagrams** (`_render_er_diagram`): Uses native mermaid JS markup output rendering visually interactive relationship maps leveraging foreign key definitions automatically.

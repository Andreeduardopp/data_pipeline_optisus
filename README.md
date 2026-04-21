# Data Pipeline Optisus

This project provides a configurable ingestion pipeline for tabular and spatial transit data, with schema-based validation, a Bronze → Silver → Gold storage model, and a complete GTFS maturity pipeline (mapping, `.zip` ingestion, SQLite persistence, spec validation, analytics, and feed export).

The codebase is packaged under `src/optisus/` with a clean **core vs. ui** split:

- `optisus.core.*` — pure Python (no Streamlit): schemas, ingestion, storage, ML mode builders, MLOps feature store, GTFS (database, mapper, importer, batch importer, exporter, validator, database profiler, gtfs-kit analytics bridge). Reusable from scripts, notebooks, or tests.
- `optisus.ui.*` — Streamlit layer: app entry, theme, and three pages (`ml_pipeline`, `gtfs_pipeline`, `db_overview`).

Root-level `app.py` and `admin_ui.py` are thin bootstraps that call `optisus.ui.app.main()`.

## Package layout

```
src/optisus/
├── core/                      # pure Python, no streamlit imports
│   ├── schemas/               # ingestion + GTFS Pydantic models, lineage metadata
│   ├── ingestion/             # tabular (CSV/Excel) + geospatial (Shapefile/GeoJSON)
│   ├── storage/               # project/run CRUD, Bronze/Silver/Gold layout
│   ├── ml/                    # Mode A / Mode B Gold artifact builders
│   ├── mlops/                 # versioned feature store, audit logging
│   └── gtfs/                  # database, mapper, importer, batch_import,
│                              # exporter, validator, database_profiler, analytics
└── ui/                        # streamlit layer
    ├── app.py                 # main() — sidebar, theme, st.navigation
    ├── theme.py               # shared CSS, color tokens, logo
    └── pages/
        ├── ml_pipeline.py     # Module 1 — ML Data Preparation
        ├── gtfs_pipeline.py   # Module 2 — GTFS Data Maturity
        └── db_overview.py     # Module 3 — Database Overview
```

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — install with:

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

  Or via pip: `pip install uv`

## Install dependencies with uv

From the project root:

```bash
uv sync
```

This creates a virtual environment (if needed), installs the project and its dependencies from `pyproject.toml`, and uses the lockfile `uv.lock` for reproducible installs. The project is installed in editable mode via the hatchling build backend (see `[tool.hatch.build.targets.wheel]` in `pyproject.toml`), so edits under `src/optisus/` are picked up automatically.

## Run the Streamlit app

Start the multipage app:

```bash
uv run streamlit run app.py
```

`uv run` uses the project's virtual environment and dependencies, so you don't need to activate the venv manually.

Or, after activating the environment (e.g. `source .venv/bin/activate` on Linux/macOS):

```bash
streamlit run app.py
```

The legacy command `streamlit run admin_ui.py` also works (it delegates to `app.py`).

The app will open in your browser (default: http://localhost:8501).

## How it works

The app is a multipage Streamlit application with three modules selectable from the sidebar:

| Module | Purpose |
|---|---|
| **Module 1 — ML Data Preparation** (`ml_pipeline`) | Project management, tabular/spatial uploads, Bronze→Silver→Gold runs, dual-mode forecasting builds |
| **Module 2 — GTFS Data Maturity** (`gtfs_pipeline`) | Map Silver datasets to GTFS, ingest full or partial GTFS feeds, batch-update tables, browse/edit, validate, export, visualize |
| **Module 3 — Database Overview** (`db_overview`) | Read-only dashboard: database health, table heatmap, per-column profiling, ER diagram, storage footprint |

### Projects

The first screen lists your **projects**. A project is a named container for
all the data-lake runs that belong to a single study or scenario.

1. **Create a project** — type a name and click *Create project*.
2. **Select a project** — pick one from the dropdown.
3. **Upload data** — switch to the *Tabular Data* or *Spatial Data* tab and
   upload files as before. Each "Validate & Save" creates a new versioned run
   inside the selected project.

### Dual-mode forecasting (Mode A / Mode B)

When you open a project, a **Forecasting Modes** panel shows two modes
side-by-side with a live requirements checklist:

| | Mode A | Mode B |
|---|---|---|
| **Goal** | Multivariate time-series demand forecasting | Spatio-temporal graph forecasting |
| **Required datasets** | Transported Passengers, Financial & Economic Data | Transported Passengers, Stop Spatial Features, Stop Connections |
| **Optional datasets** | Weather Observations, Calendar Events | Weather Observations, Calendar Events |
| **Gold output** | `mode_a_timeseries.parquet` (+ economic context) | `mode_b_spatiotemporal.parquet` + `network_topology.json` |
| **Target models** | LSTM / Transformer | GNN / Graph-Transformer |

**Quality Gate** — the *Build* button is only enabled when every required
dataset for the selected mode has been uploaded and validated into Silver.
If prerequisites are missing, the UI shows exactly which datasets still need
to be provided.

**Workflow:**

1. Upload the required (and optionally enrichment) datasets via the
   *Tabular Data* tab, selecting the matching schema for each file.
2. Select **Mode A** or **Mode B** and click **Build**.
3. The pipeline reads the latest Silver artifacts, applies temporal feature
   engineering, validates each output row against the Gold-layer Pydantic
   schema, and writes the result into a new versioned Gold run.

### GTFS Data Maturity module

Module 2 gives each project a dedicated SQLite GTFS database
(`projects/<slug>/gtfs.db`) with FK constraints enforcing referential
integrity across the 15 canonical GTFS / GTFS-ride tables. The page
surfaces:

1. **Database status bar** — existence, size, table counts.
2. **Maturity dashboard** — progression across levels (schema coverage,
   referential integrity, spec validation, gtfs-kit quality tags).
3. **Feed completeness gauge** — scored coverage of required vs. optional
   tables.
4. **GTFS table browser** — paginated view/edit of each table.
5. **Import GTFS `.zip`** — drop an existing agency/vendor feed, preview
   its contents, and ingest in one of four modes:
   - `REPLACE` — wipe the DB and load from the zip.
   - `MERGE` — upsert into the existing DB (requires a complete feed).
   - `MERGE_PARTIAL` — upsert only the tables present in the archive,
     leaving everything else untouched. Only available when the database
     is already populated; auto-selected when the uploaded ZIP is
     incomplete. FK integrity is enforced row-by-row by SQLite.
   - `ABORT_IF_NOT_EMPTY` — safe mode; refuses to write if any table has rows.

   The importer streams every member through `ZipFile.open`, enforces
   size / zip-bomb guards, and reuses the same row-validation + SQL upsert
   path as the Silver → GTFS mapper.
6. **Batch CSV update** — drop several GTFS CSVs at once
   (e.g. `stops.csv`, `routes.csv`, `trips.csv`). Filenames are matched
   to GTFS tables (overridable per file); the import runs in FK-safe
   order inside **one SQLite transaction** — if anything fails, the
   database is rolled back to its prior state.
7. **Silver → GTFS mapping wizard** — map validated Silver datasets onto
   GTFS tables without needing an existing feed.
8. **Integrity report** — FK and structural checks.
9. **Export & validate** — produce a full-feed `.zip` (`exports/latest/gtfs.zip`)
   or date-/route-filtered subset exports (`exports/<timestamp>/`), plus
   analytics and Folium route/stop maps via the gtfs-kit bridge.

### Database Overview module

Module 3 is a read-only dashboard over the project's GTFS SQLite database.
Every call is served from a small mtime-keyed cache, so switching
projects or flipping between tables is instant until the database is
next written to.

1. **Header card** — total records, populated vs. total tables, DB size,
   completeness %, schema version, timestamps, and an integrity badge.
2. **Table heatmap** — all 15 GTFS / GTFS-ride tables grouped by role
   (Core / Service / Spatial / Metadata / GTFS-ride) and colour-coded by
   row count. Each card shows the column count and FK parents.
3. **Table deep-dive** — pick a table and run **lazy** per-column
   profiling: data type, null %, distinct count, sample values. Computed
   on demand so an empty DB or an unopened table costs nothing.
4. **ER diagram** — a Mermaid `erDiagram` derived from the canonical
   FK list; edge labels carry the FK column name and parent→child row
   counts.
5. **Storage footprint** — DB size, `exports/` directory size, `runs/`
   directory size, and total project footprint.

### Storage layout

```
data_lake_outputs/
└── projects/
    └── <project_slug>/
        ├── project.json
        ├── gtfs.db                         # per-project GTFS SQLite DB
        ├── exports/                        # GTFS zip exports
        │   ├── latest/gtfs.zip
        │   └── <timestamp>/gtfs.zip        # subset exports
        └── runs/
            ├── run_id_<ts>_<schema>/       # per-upload runs
            │   ├── bronze/                 # raw uploaded files
            │   ├── silver/                 # validated Parquet / reports
            │   ├── gold/                   # aggregate metrics
            │   └── lineage.json
            └── run_id_<ts>_mode_a_build/   # mode build runs
                ├── gold/
                │   ├── mode_a_timeseries.parquet
                │   ├── mode_a_economic_context.parquet
                │   └── mode_a_timeseries_metrics.json
                ├── lineage.json
                └── _SUCCESS
```

Legacy runs created before the project feature (`data_lake_outputs/run_id_*`)
remain on disk but are not shown in the UI.

## Programmatic (non-UI) usage

Because `optisus.core` has no Streamlit dependency, you can drive the pipeline from a script or notebook:

```python
from optisus.core.storage.layers import create_project, list_projects
from optisus.core.gtfs.mapper import map_project_to_gtfs
from optisus.core.gtfs.importer import import_gtfs_zip, preview_gtfs_zip, ImportMode
from optisus.core.gtfs.batch_import import import_batch, preview_batch
from optisus.core.gtfs.exporter import export_gtfs_feed, export_gtfs_subset
from optisus.core.gtfs.validator import validate_gtfs_feed
from optisus.core.gtfs.analytics import feed_from_db, compute_analytics, build_routes_map
from optisus.core.gtfs.database_profiler import profile_database, profile_table_columns

slug = create_project("Demo City")

# Option A — map from your Silver datasets
map_project_to_gtfs(slug)

# Option B — ingest an existing GTFS .zip (agency feed, vendor export, …)
preview = preview_gtfs_zip("path/to/feed.zip")          # inspect before committing
import_gtfs_zip(slug, "path/to/feed.zip", mode=ImportMode.REPLACE)

# Option C — incremental update from a partial .zip
#           (only the tables present are touched; DB must already be populated)
import_gtfs_zip(slug, "path/to/partial.zip", mode=ImportMode.MERGE_PARTIAL)

# Option D — transactional batch CSV update
#           (FK-safe order, single SQLite transaction, rollback on failure)
with open("stops.csv", "rb") as s, open("routes.csv", "rb") as r:
    import_batch(slug, [("stops.csv", s.read()), ("routes.csv", r.read())])

result = export_gtfs_feed(slug)                         # exports/latest/gtfs.zip
subset = export_gtfs_subset(slug, dates=["20260501"])   # exports/<timestamp>/gtfs.zip
report = validate_gtfs_feed(result.zip_path)

feed = feed_from_db(slug)                               # gtfs-kit Feed, no ZIP round-trip
stats = compute_analytics(feed)
routes_map = build_routes_map(feed)                     # Folium map

# Read-only dashboard data (mtime-cached; lazy per-column profiling)
overview = profile_database(slug)                       # cheap summary
cols = profile_table_columns(slug, "stops")             # null %, distinct, samples
```

## Running tests

```bash
uv run pytest
```

Tests use an `isolated_data_lake` / `isolated_gtfs` pytest fixture that
redirects `DATA_LAKE_ROOT` / `PROJECTS_ROOT` to `tmp_path`, so the real
filesystem is never touched. Coverage includes storage layers, all GTFS
Pydantic schemas, the SQLite database layer, the Silver → GTFS mapper,
the GTFS `.zip` importer (preview, modes, FK order, zip-bomb guards,
round-trip), the `MERGE_PARTIAL` incremental path, the transactional
batch CSV importer (filename inference, FK-safe ordering, duplicate-target
rejection), the read-only database profiler (row counts, lazy column
stats, mtime cache invalidation), the exporter (full + subset), and an
end-to-end integration test from sample files through Silver, DB, zip
export, and the validator.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the app (opens at http://localhost:8501)
uv run streamlit run app.py

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_storage_layers.py

# Run a specific test class or function
uv run pytest tests/test_storage_layers.py::TestCreateProject
```

## Architecture

This is a configurable data ingestion and processing pipeline for public transit system data, implementing a **Bronze → Silver → Gold data lake architecture** with a multipage Streamlit UI.

### Package layout (src-layout)

The project is packaged as `optisus` under `src/` with a clean **core vs. ui** split — core modules are pure Python (no Streamlit), so they can be reused from scripts, notebooks, or tests without importing the UI layer.

```
src/optisus/
├── core/                      # pure-Python, no streamlit imports
│   ├── schemas/
│   │   ├── ingestion.py       # 12 ingestion + 2 Gold Pydantic models
│   │   ├── gtfs.py            # 15 GTFS/GTFS-ride Pydantic models + enums
│   │   └── metadata.py        # lineage / run metadata models
│   ├── ingestion/
│   │   ├── tabular.py         # CSV/Excel parsing, row-by-row validation
│   │   └── geospatial.py      # Shapefile/GeoJSON, GeoParquet output
│   ├── storage/
│   │   └── layers.py          # project/run CRUD, Bronze/Silver/Gold layout
│   ├── ml/
│   │   └── mode_builders.py   # Mode A / Mode B Gold artifact builders
│   ├── mlops/
│   │   └── store.py           # versioned feature store, audit logging
│   └── gtfs/
│       ├── database.py        # SQLite GTFS database layer
│       ├── mapper.py          # Silver → GTFS mapping orchestrator
│       ├── exporter.py        # GTFS .zip export + subset exports
│       ├── validator.py       # spec-rule GTFS validator (authoritative)
│       └── analytics.py       # gtfs-kit bridge — feed analytics + maps
└── ui/                        # streamlit layer
    ├── app.py                 # main() — sidebar, theme, st.navigation
    ├── theme.py               # shared CSS, color tokens, logo
    ├── components/            # (reserved for shared widgets)
    └── pages/
        ├── ml_pipeline.py     # Module 1 — ML Data Preparation
        └── gtfs_pipeline.py   # Module 2 — GTFS Data Maturity
```

Root-level `app.py` and `admin_ui.py` are thin bootstraps that call `optisus.ui.app.main()` — Streamlit resolves `st.Page(...)` paths relative to the main script, so the page paths remain `src/optisus/ui/pages/*.py`.

### Multipage structure

The app uses `st.navigation()` with two modules:

| Entry point | Role |
|---|---|
| `app.py` (root) | Thin bootstrap — delegates to `optisus.ui.app.main` |
| `admin_ui.py` (root) | Legacy wrapper — delegates to `optisus.ui.app.main` |
| `src/optisus/ui/app.py` | `main()` — page config, theme, sidebar, navigation |
| `src/optisus/ui/pages/ml_pipeline.py` | Module 1 UI — project management, uploads, mode building |
| `src/optisus/ui/pages/gtfs_pipeline.py` | Module 2 UI — GTFS data maturity pipeline |

### Data flow

```
Upload (CSV/GeoJSON/Shapefile)
  → Bronze (raw files)
  → Silver (validated Parquet via Pydantic row-by-row validation)
  → Gold (ML-ready artifacts: Mode A time-series or Mode B spatio-temporal)
```

All metadata and lineage is stored as JSON files; no external database is used for the ML pipeline. The `data_lake_outputs/` directory is the root, organized as `projects/<project_slug>/runs/<run_id>/`.

The GTFS pipeline uses a per-project SQLite database (`projects/<slug>/gtfs.db`) with FK constraints to enforce referential integrity between stops, routes, trips, stop_times, etc.

### Dual ML output modes

- **Mode A** — Multivariate time-series forecasting (LSTM/Transformer). Requires: Transported Passengers + Financial & Economic Data. Output: `mode_a_timeseries.parquet`.
- **Mode B** — Spatio-temporal graph forecasting (GNN/Graph-Transformer). Requires: Transported Passengers + Stop Spatial Features + Stop Connections. Output: `mode_b_spatiotemporal.parquet` + `network_topology.json`.

Both modes support optional Weather Observations and Calendar Events. A **quality gate** enforces that all required Silver datasets exist before building.

### GTFS pipeline

- Silver datasets are mapped to the 15 canonical GTFS/GTFS-ride tables via `core.gtfs.mapper`.
- Records are persisted to SQLite via `core.gtfs.database` with Pydantic validation.
- `core.gtfs.validator` runs spec-rule checks (the authoritative validator).
- `core.gtfs.exporter` produces full-feed `.zip` exports under `exports/latest/` plus date- and route-filtered subset exports (via gtfs-kit's `restrict_to_dates` / `restrict_to_routes`) under `exports/<timestamp>/`.
- `core.gtfs.analytics` is a thin bridge that builds a gtfs-kit `Feed` directly from the SQLite DB (no ZIP round-trip), cached on the DB mtime. It powers the analytics panel and Folium route/stop maps and supplies the quality-tag badge for Level 4 on the maturity dashboard.

### The 12 ingestion schemas (defined in `core/schemas/ingestion.py`)

Fleet Identification, Fleet Energy Performance, Electric Fleet Characteristics, Operations and Circulation, Transported Passengers, Charging Infrastructure, Calendar Events, Weather Observations, Stop Spatial Features, Stop Connections, Financial & Economic Data, Lifespan and Depreciation.

### Testing

Tests live in `tests/` and use an `isolated_data_lake` (or `isolated_gtfs`) pytest fixture that redirects `DATA_LAKE_ROOT` / `PROJECTS_ROOT` to `tmp_path` so the real filesystem is never touched.

| Test file | Coverage |
|---|---|
| `tests/test_storage_layers.py` | Project CRUD, run creation, lineage |
| `tests/test_gtfs_schemas.py` | All 15 GTFS Pydantic models, enums, format validation |
| `tests/test_gtfs_database.py` | SQLite layer — upsert, FK integrity, CRUD, summary |
| `tests/test_gtfs_mapper.py` | Silver → GTFS mapping (per-table + orchestrator) |
| `tests/test_gtfs_exporter.py` | Pre-export validation, zip format, completeness score |
| `tests/test_gtfs_integration.py` | End-to-end: samples → Silver → DB → zip → validator |

Fixtures that need to patch module-level globals use `from optisus.core.X import Y as alias` so `monkeypatch.setattr(alias, "FLAG", ...)` continues to work.

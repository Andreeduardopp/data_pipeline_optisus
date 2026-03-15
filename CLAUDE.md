# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the admin UI (opens at http://localhost:8501)
uv run streamlit run admin_ui.py

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_storage_layers.py

# Run a specific test class or function
uv run pytest tests/test_storage_layers.py::TestCreateProject
```

## Architecture

This is a configurable data ingestion and processing pipeline for public transit system data, implementing a **Bronze → Silver → Gold data lake architecture** with a Streamlit admin UI.

### Data Flow

```
Upload (CSV/GeoJSON/Shapefile)
  → Bronze (raw files)
  → Silver (validated Parquet via Pydantic row-by-row validation)
  → Gold (ML-ready artifacts: Mode A time-series or Mode B spatio-temporal)
```

All metadata and lineage is stored as JSON files; no external database is used. The `data_lake_outputs/` directory is the root, organized as `projects/<project_slug>/runs/<run_id>/`.

### Dual ML Output Modes

- **Mode A** — Multivariate time-series forecasting (LSTM/Transformer). Requires: Transported Passengers + Financial & Economic Data. Output: `mode_a_timeseries.parquet`.
- **Mode B** — Spatio-temporal graph forecasting (GNN/Graph-Transformer). Requires: Transported Passengers + Stop Spatial Features + Stop Connections. Output: `mode_b_spatiotemporal.parquet` + `network_topology.json`.

Both modes support optional Weather Observations and Calendar Events. A **quality gate** enforces that all required Silver datasets exist before building.

### Module Responsibilities

| File | Role |
|---|---|
| `admin_ui.py` | Streamlit web app — project management, file uploads, mode building |
| `schemas.py` | Pydantic models for all 12 ingestion schemas and 2 Gold output schemas |
| `ingestion_tabular.py` | CSV/Excel parsing, column normalization, row-by-row validation |
| `ingestion_geo.py` | Shapefile/GeoJSON reading, geometry validation, GeoParquet output |
| `storage_layers.py` | Project/run CRUD, Bronze/Silver/Gold directory creation, lineage tracking |
| `mode_builders.py` | Mode A and Mode B artifact generation, quality gate evaluation |
| `ui_validation.py` | Schema introspection helpers, field listing, mode requirements checklist |
| `mlops_storage.py` | Versioned feature store creation, audit logging |

### The 12 Ingestion Schemas (defined in `schemas.py`)

Fleet Identification, Fleet Energy Performance, Electric Fleet Characteristics, Operations and Circulation, Transported Passengers, Charging Infrastructure, Calendar Events, Weather Observations, Stop Spatial Features, Stop Connections, Financial & Economic Data, Lifespan and Depreciation.

### Testing

Tests are in `tests/test_storage_layers.py` and use an `isolated_data_lake` pytest fixture to avoid touching the real filesystem. Test classes cover safe slug generation, project idempotency, run creation, and lineage retrieval.

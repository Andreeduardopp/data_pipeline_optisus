# Data Pipeline Optisus

This project provides a configurable ingestion pipeline for tabular and spatial transit data, with schema-based validation, a Bronze → Silver → Gold storage model, and a complete GTFS maturity pipeline (mapping, SQLite persistence, spec validation, analytics, and feed export).

The codebase is packaged under `src/optisus/` with a clean **core vs. ui** split:

- `optisus.core.*` — pure Python (no Streamlit): schemas, ingestion, storage, ML mode builders, MLOps feature store, GTFS (database, mapper, exporter, validator, gtfs-kit analytics bridge). Reusable from scripts, notebooks, or tests.
- `optisus.ui.*` — Streamlit layer: app entry, theme, and two pages (`ml_pipeline`, `gtfs_pipeline`).

Root-level `app.py` and `admin_ui.py` are thin bootstraps that call `optisus.ui.app.main()`.

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

`uv run` uses the project’s virtual environment and dependencies, so you don’t need to activate the venv manually.

Or, after activating the environment (e.g. `source .venv/bin/activate` on Linux/macOS):

```bash
streamlit run app.py
```

The legacy command `streamlit run admin_ui.py` also works (it delegates to `app.py`).

The app will open in your browser (default: http://localhost:8501).

## How it works

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

### Storage layout

```
data_lake_outputs/
└── projects/
    └── <project_slug>/
        ├── project.json
        └── runs/
            ├── run_id_<ts>_<schema>/        # per-upload runs
            │   ├── bronze/                  # raw uploaded files
            │   ├── silver/                  # validated Parquet / reports
            │   ├── gold/                    # aggregate metrics
            │   └── lineage.json
            └── run_id_<ts>_mode_a_build/    # mode build runs
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
from optisus.core.gtfs.importer import import_gtfs_zip, ImportMode
from optisus.core.gtfs.exporter import export_gtfs_feed, export_gtfs_subset
from optisus.core.gtfs.validator import validate_gtfs_feed
from optisus.core.gtfs.analytics import feed_from_db, compute_analytics

slug = create_project("Demo City")

# Option A — map from your Silver datasets
map_project_to_gtfs(slug)

# Option B — ingest an existing GTFS .zip (agency feed, vendor export, …)
import_gtfs_zip(slug, "path/to/feed.zip", mode=ImportMode.REPLACE)

result = export_gtfs_feed(slug)             # writes exports/latest/gtfs.zip
report = validate_gtfs_feed(result.zip_path)

feed = feed_from_db(slug)                   # gtfs-kit Feed, no ZIP round-trip
stats = compute_analytics(feed)
```

## Running tests

```bash
uv run pytest
```

# Data Pipeline Optisus

This project aims to provide a configurable ingestion pipeline for tabular and spatial data, with schema-based validation and a Bronze → Silver → Gold storage model. The codebase is organized around a Streamlit admin UI (`admin_ui.py`) for uploads and validation; ingestion logic for tabular and geo data (`ingestion_tabular.py`, `ingestion_geo.py`); schema and validation rules (`schemas.py`, `ui_validation.py`); and layered persistence and lineage (`storage_layers.py`, `mlops_storage.py`).

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

This creates a virtual environment (if needed), installs the project and its dependencies from `pyproject.toml`, and uses the lockfile `uv.lock` for reproducible installs.

To add the project in editable mode and ensure the CLI is available:

```bash
uv sync
```

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

## Running tests

```bash
uv run pytest
```

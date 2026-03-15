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

Start the admin UI:

```bash
uv run streamlit run admin_ui.py
```

`uv run` uses the project’s virtual environment and dependencies, so you don’t need to activate the venv manually.

Or, after activating the environment (e.g. `source .venv/bin/activate` on Linux/macOS):

```bash
streamlit run admin_ui.py
```

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

### Storage layout

```
data_lake_outputs/
└── projects/
    └── <project_slug>/
        ├── project.json          # project metadata
        └── runs/
            └── run_id_<ts>_<ctx>/
                ├── bronze/       # raw uploaded files
                ├── silver/       # validated Parquet / reports
                ├── gold/         # aggregate metrics
                └── lineage.json  # links all three layers
```

Legacy runs created before the project feature (`data_lake_outputs/run_id_*`)
remain on disk but are not shown in the UI.

## Running tests

```bash
uv run pytest
```

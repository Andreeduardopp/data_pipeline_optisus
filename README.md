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

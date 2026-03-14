"""
Bronze / Silver / Gold data lake storage utilities for the admin UI.
Each upload run creates a timestamped directory with three sub-layers.
"""
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = Path("data_lake_outputs")


def _safe_name(text: str) -> str:
    """Convert arbitrary text to a filesystem-safe snake_case string."""
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def create_layered_run(context: str) -> Dict[str, Path]:
    """
    Create a run-scoped directory with bronze / silver / gold sub-folders.

    Args:
        context: Human-readable label (e.g. schema name). Sanitised for the filesystem.

    Returns:
        Dict with keys 'root', 'bronze', 'silver', 'gold' mapping to Paths.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_id_{ts}_{_safe_name(context)}"
    root = DATA_LAKE_ROOT / run_id

    paths = {
        "root": root,
        "bronze": root / "bronze",
        "silver": root / "silver",
        "gold": root / "gold",
        "run_id": run_id,
    }
    for key in ("bronze", "silver", "gold"):
        paths[key].mkdir(parents=True, exist_ok=True)

    logger.info(f"Created layered run at {root}")
    return paths


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

def save_bronze_file(
    source_path: str,
    bronze_dir: Path,
    prefix: str,
) -> Path:
    """
    Copy the raw uploaded file into the Bronze layer unchanged.

    Returns:
        Path to the saved bronze file.
    """
    src = Path(source_path)
    dest = bronze_dir / f"{_safe_name(prefix)}{src.suffix}"
    shutil.copy2(str(src), str(dest))
    logger.info(f"Bronze: saved raw file to {dest}")
    return dest


def save_bronze_bytes(
    data: bytes,
    bronze_dir: Path,
    filename: str,
) -> Path:
    """
    Write raw uploaded bytes directly into the Bronze layer.

    Returns:
        Path to the saved bronze file.
    """
    dest = bronze_dir / filename
    dest.write_bytes(data)
    logger.info(f"Bronze: saved raw bytes to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

def save_silver_tabular(
    df: pd.DataFrame,
    silver_dir: Path,
    dataset_name: str,
) -> Path:
    """Save validated tabular DataFrame as compressed Parquet in the Silver layer."""
    dest = silver_dir / f"{_safe_name(dataset_name)}.parquet"
    df.to_parquet(dest, engine="pyarrow", compression="gzip", index=False)
    logger.info(f"Silver: saved tabular parquet to {dest}")
    return dest


def save_silver_spatial(
    gdf: "gpd.GeoDataFrame",
    silver_dir: Path,
    dataset_name: str,
) -> Path:
    """Save validated GeoDataFrame as GeoParquet in the Silver layer."""
    dest = silver_dir / f"{_safe_name(dataset_name)}.geoparquet"
    gdf.to_parquet(dest, index=False)
    logger.info(f"Silver: saved spatial geoparquet to {dest}")
    return dest


def save_silver_validation_report(
    silver_dir: Path,
    dataset_name: str,
    *,
    total_rows: int,
    valid_rows: int,
    invalid_rows: int,
    missing_mandatory_columns: Optional[List[str]] = None,
) -> Path:
    """Write a JSON validation report into the Silver layer."""
    report = {
        "dataset": dataset_name,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "missing_mandatory_columns": missing_mandatory_columns or [],
        "timestamp": datetime.now().isoformat(),
    }
    dest = silver_dir / f"{_safe_name(dataset_name)}_validation_report.json"
    dest.write_text(json.dumps(report, indent=2))
    logger.info(f"Silver: validation report saved to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

def build_gold_metrics(df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """
    Compute basic aggregate/quality metrics from a Silver DataFrame.

    Metrics:
        - row_count, column_count
        - null_ratio per column
        - numeric describe stats (mean, std, min, max, 25%, 50%, 75%)
    """
    null_ratios = (df.isnull().sum() / max(len(df), 1)).to_dict()

    numeric_stats = {}
    desc = df.describe()
    for col in desc.columns:
        numeric_stats[col] = desc[col].to_dict()

    return {
        "dataset": dataset_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "null_ratio_per_column": null_ratios,
        "numeric_stats": numeric_stats,
        "generated_at": datetime.now().isoformat(),
    }


def build_gold_spatial_metrics(
    gdf: "gpd.GeoDataFrame",
    dataset_name: str,
) -> Dict[str, Any]:
    """
    Compute basic metrics for a spatial Silver GeoDataFrame.
    Focuses on row count, non-geometry column completeness, and geometry type summary.
    """
    non_geom_cols = [c for c in gdf.columns if c != gdf.geometry.name]
    null_ratios = {}
    for col in non_geom_cols:
        null_ratios[col] = float(gdf[col].isnull().sum() / max(len(gdf), 1))

    geom_types = gdf.geometry.geom_type.value_counts().to_dict()

    return {
        "dataset": dataset_name,
        "row_count": len(gdf),
        "column_count": len(gdf.columns),
        "columns": list(gdf.columns),
        "null_ratio_per_column": null_ratios,
        "geometry_type_counts": geom_types,
        "generated_at": datetime.now().isoformat(),
    }


def save_gold_metrics(
    metrics: Dict[str, Any],
    gold_dir: Path,
    dataset_name: str,
) -> Path:
    """Write gold metrics JSON to the Gold layer."""
    dest = gold_dir / f"{_safe_name(dataset_name)}_metrics.json"
    dest.write_text(json.dumps(metrics, indent=2, default=str))
    logger.info(f"Gold: metrics saved to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

def write_layer_lineage(
    run_root: Path,
    *,
    run_id: str,
    context: str,
    bronze_artifacts: List[str],
    silver_artifacts: List[str],
    gold_artifacts: List[str],
) -> Path:
    """Write a lineage JSON at the run root linking all three layers."""
    payload = {
        "run_id": run_id,
        "context": context,
        "timestamp": datetime.now().isoformat(),
        "bronze": bronze_artifacts,
        "silver": silver_artifacts,
        "gold": gold_artifacts,
    }
    dest = run_root / "lineage.json"
    dest.write_text(json.dumps(payload, indent=2))
    logger.info(f"Lineage written to {dest}")
    return dest

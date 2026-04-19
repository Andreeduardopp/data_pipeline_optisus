"""
Dual-mode artifact builders and quality-gate evaluator.

Mode A  – Multivariate time-series demand samples  (TimeSeriesDemandSample).
Mode B  – Spatio-temporal demand samples + network topology graph.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from pydantic import ValidationError

from optisus.core.schemas.ingestion import (
    DayType,
    Season,
    TimeSeriesDemandSample,
    SpatioTemporalDemandSample,
    NetworkTopology,
    StopSpatialFeatures,
    StopConnection,
)
from optisus.core.schemas.metadata import MODE_REQUIREMENTS
from optisus.core.storage.layers import (
    create_project_layered_run,
    save_gold_metrics,
    write_layer_lineage,
    build_gold_metrics,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Quality Gate
# ---------------------------------------------------------------------------

def evaluate_quality_gate(
    mode: str,
    available_datasets: Dict[str, str],
) -> Tuple[bool, List[str]]:
    """Check whether all required Silver datasets exist for *mode*.

    Returns ``(passed, missing_dataset_labels)``.
    """
    required = MODE_REQUIREMENTS[mode]["required"]
    missing = [ds for ds in required if ds not in available_datasets]
    return (len(missing) == 0, missing)


# ---------------------------------------------------------------------------
# Temporal-feature helpers
# ---------------------------------------------------------------------------

def _season_from_month(month: int) -> str:
    if month in (3, 4, 5):
        return Season.SPRING.value
    if month in (6, 7, 8):
        return Season.SUMMER.value
    if month in (9, 10, 11):
        return Season.AUTUMN.value
    return Season.WINTER.value


def _day_type(day_of_week: int, is_holiday: bool) -> str:
    if is_holiday or day_of_week == 6:
        return DayType.SUNDAY_HOLIDAY.value
    if day_of_week == 5:
        return DayType.SATURDAY.value
    return DayType.WEEKDAY.value


def _load_silver_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def _load_calendar_holidays(calendar_path: Optional[str]) -> Set[str]:
    """Return a set of ISO-date strings for holiday events."""
    if not calendar_path:
        return set()
    try:
        df = pd.read_parquet(calendar_path)
        holidays = df[df["event_type"] == "holiday"]
        return set(pd.to_datetime(holidays["event_date"]).dt.strftime("%Y-%m-%d"))
    except Exception:
        return set()


def _load_weather_lookup(weather_path: Optional[str]) -> Optional[pd.DataFrame]:
    """Load weather parquet sorted by timestamp for nearest-match joining."""
    if not weather_path:
        return None
    try:
        df = pd.read_parquet(weather_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception:
        return None


def _nearest_weather(
    weather_df: Optional[pd.DataFrame],
    ts: "pd.Timestamp",
) -> Tuple[Optional[float], Optional[float]]:
    """Return (temperature_celsius, precipitation_mm) from nearest observation."""
    if weather_df is None or weather_df.empty:
        return None, None
    idx = int(weather_df["timestamp"].searchsorted(ts))
    if idx >= len(weather_df):
        idx = len(weather_df) - 1
    row = weather_df.iloc[idx]
    temp = row.get("temperature_celsius")
    precip = row.get("precipitation_mm")
    if pd.isna(temp):
        temp = None
    if pd.isna(precip):
        precip = None
    return temp, precip


def _safe_int(val: Any) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return int(val)


# ---------------------------------------------------------------------------
# Mode A Builder
# ---------------------------------------------------------------------------

def build_mode_a_artifacts(
    project_slug: str,
    available_datasets: Dict[str, str],
) -> Tuple[Optional[str], List[str]]:
    """Build Mode A Gold artifacts (TimeSeriesDemandSample parquet).

    Returns ``(run_dir_path | None, list_of_warnings)``.
    """
    passengers_df = _load_silver_parquet(available_datasets["Transported Passengers"])
    passengers_df["timestamp"] = pd.to_datetime(passengers_df["timestamp"])

    weather_path = available_datasets.get("Weather Observations")
    calendar_path = available_datasets.get("Calendar Events")
    holidays = _load_calendar_holidays(calendar_path)
    weather_df = _load_weather_lookup(weather_path)

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for _, row in passengers_df.iterrows():
        ts: pd.Timestamp = row["timestamp"]
        date_str = ts.strftime("%Y-%m-%d")
        dow = ts.weekday()
        month = ts.month
        is_holiday = date_str in holidays
        is_weekend = dow >= 5
        temp, precip = _nearest_weather(weather_df, ts)

        sample: Dict[str, Any] = {
            "timestamp": ts.to_pydatetime(),
            "line_id": row.get("line_id", ""),
            "demand": _safe_int(row.get("number_of_validations", 0)) or 0,
            "temporal_resolution": row.get("temporal_resolution", "daily"),
            "hour_of_day": ts.hour if ts.hour != 0 or ts.minute != 0 else None,
            "day_of_week": dow,
            "day_of_month": ts.day,
            "month": month,
            "year": ts.year,
            "is_weekend": is_weekend,
            "is_holiday": is_holiday,
            "day_type": _day_type(dow, is_holiday),
            "season": _season_from_month(month),
            "avg_frequency": None,
            "total_capacity": None,
            "temperature_celsius": temp,
            "precipitation_mm": precip,
        }

        try:
            validated = TimeSeriesDemandSample(**sample)
            rows.append(validated.model_dump())
        except ValidationError as e:
            warnings.append(f"Row ts={ts}: {e.errors()}")

    if not rows:
        return None, warnings or ["No valid rows produced."]

    layers = create_project_layered_run(project_slug, "mode_a_build")

    ts_df = pd.DataFrame(rows)
    ts_path = layers["gold"] / "mode_a_timeseries.parquet"
    ts_df.to_parquet(ts_path, engine="pyarrow", compression="gzip", index=False)
    gold_artifacts: List[str] = [str(ts_path)]

    financial_path = available_datasets.get("Financial & Economic Data")
    if financial_path:
        fin_df = _load_silver_parquet(financial_path)
        fin_dest = layers["gold"] / "mode_a_economic_context.parquet"
        fin_df.to_parquet(fin_dest, engine="pyarrow", compression="gzip", index=False)
        gold_artifacts.append(str(fin_dest))

    metrics = build_gold_metrics(ts_df, "mode_a_timeseries")
    metrics_path = save_gold_metrics(metrics, layers["gold"], "mode_a_timeseries")
    gold_artifacts.append(str(metrics_path))

    silver_sources: List[str] = [available_datasets["Transported Passengers"]]
    if financial_path:
        silver_sources.append(financial_path)
    if weather_path:
        silver_sources.append(weather_path)
    if calendar_path:
        silver_sources.append(calendar_path)

    write_layer_lineage(
        layers["root"],
        run_id=layers["run_id"],
        context="mode_a_build",
        bronze_artifacts=[],
        silver_artifacts=silver_sources,
        gold_artifacts=gold_artifacts,
    )

    (layers["root"] / "_SUCCESS").touch()
    logger.info(f"Mode A build complete: {layers['root']}")
    return str(layers["root"]), warnings


# ---------------------------------------------------------------------------
# Mode B Builder
# ---------------------------------------------------------------------------

def build_mode_b_artifacts(
    project_slug: str,
    available_datasets: Dict[str, str],
) -> Tuple[Optional[str], List[str]]:
    """Build Mode B Gold artifacts (SpatioTemporalDemandSample + NetworkTopology).

    Returns ``(run_dir_path | None, list_of_warnings)``.
    """
    passengers_df = _load_silver_parquet(available_datasets["Transported Passengers"])
    stops_df = _load_silver_parquet(available_datasets["Stop Spatial Features"])
    connections_df = _load_silver_parquet(available_datasets["Stop Connections"])

    weather_path = available_datasets.get("Weather Observations")
    calendar_path = available_datasets.get("Calendar Events")
    holidays = _load_calendar_holidays(calendar_path)
    weather_df = _load_weather_lookup(weather_path)

    # -- Build NetworkTopology -----------------------------------------------
    topo_warnings: List[str] = []

    node_records: List[StopSpatialFeatures] = []
    for _, row in stops_df.iterrows():
        try:
            node_records.append(StopSpatialFeatures(**row.to_dict()))
        except ValidationError as e:
            topo_warnings.append(
                f"Node {row.get('stop_id', '?')}: {e.errors()}"
            )

    edge_records: List[StopConnection] = []
    for _, row in connections_df.iterrows():
        try:
            edge_records.append(StopConnection(**row.to_dict()))
        except ValidationError as e:
            topo_warnings.append(
                f"Edge {row.get('source_stop_id', '?')}->"
                f"{row.get('target_stop_id', '?')}: {e.errors()}"
            )

    if not node_records:
        return None, topo_warnings or ["No valid stop nodes produced."]

    try:
        topology = NetworkTopology(
            nodes=node_records,
            edges=edge_records,
            num_nodes=len(node_records),
            num_edges=len(edge_records),
        )
    except ValidationError as e:
        return None, [f"NetworkTopology validation failed: {e.errors()}"]

    stop_lookup: Dict[str, Dict[str, Any]] = {
        n.model_dump()["stop_id"]: n.model_dump() for n in node_records
    }

    # -- Build spatio-temporal samples ---------------------------------------
    passengers_df["timestamp"] = pd.to_datetime(passengers_df["timestamp"])
    sample_warnings: List[str] = []
    rows: List[Dict[str, Any]] = []

    for _, row in passengers_df.iterrows():
        stop_id = str(row.get("stop_id", ""))
        stop_info = stop_lookup.get(stop_id)
        if not stop_info:
            continue

        ts: pd.Timestamp = row["timestamp"]
        date_str = ts.strftime("%Y-%m-%d")
        dow = ts.weekday()
        month = ts.month
        is_holiday = date_str in holidays
        is_weekend = dow >= 5
        temp, precip = _nearest_weather(weather_df, ts)

        sample: Dict[str, Any] = {
            "timestamp": ts.to_pydatetime(),
            "node_id": stop_id,
            "line_id": row.get("line_id", ""),
            "latitude": stop_info["latitude"],
            "longitude": stop_info["longitude"],
            "demand": _safe_int(row.get("number_of_validations", 0)) or 0,
            "boarding_count": _safe_int(row.get("boarding_count")),
            "alighting_count": _safe_int(row.get("alighting_count")),
            "temporal_resolution": row.get("temporal_resolution", "daily"),
            "hour_of_day": ts.hour if ts.hour != 0 or ts.minute != 0 else None,
            "day_of_week": dow,
            "month": month,
            "year": ts.year,
            "is_weekend": is_weekend,
            "is_holiday": is_holiday,
            "day_type": _day_type(dow, is_holiday),
            "season": _season_from_month(month),
            "zone_type": stop_info.get("zone_type"),
            "population_density": stop_info.get("population_density"),
            "num_lines_served": stop_info.get("num_lines_served"),
            "is_terminal": stop_info.get("is_terminal"),
            "is_interchange": stop_info.get("is_interchange"),
            "avg_frequency": None,
            "temperature_celsius": temp,
            "precipitation_mm": precip,
        }

        try:
            validated = SpatioTemporalDemandSample(**sample)
            rows.append(validated.model_dump())
        except ValidationError as e:
            sample_warnings.append(f"Row stop={stop_id} ts={ts}: {e.errors()}")

    all_warnings = topo_warnings + sample_warnings

    if not rows:
        return None, all_warnings or ["No valid spatio-temporal samples produced."]

    layers = create_project_layered_run(project_slug, "mode_b_build")

    topo_path = layers["gold"] / "network_topology.json"
    topo_path.write_text(
        json.dumps(topology.model_dump(), indent=2, default=str)
    )
    gold_artifacts: List[str] = [str(topo_path)]

    st_df = pd.DataFrame(rows)
    st_path = layers["gold"] / "mode_b_spatiotemporal.parquet"
    st_df.to_parquet(st_path, engine="pyarrow", compression="gzip", index=False)
    gold_artifacts.append(str(st_path))

    metrics = build_gold_metrics(st_df, "mode_b_spatiotemporal")
    metrics_path = save_gold_metrics(metrics, layers["gold"], "mode_b_spatiotemporal")
    gold_artifacts.append(str(metrics_path))

    silver_sources: List[str] = [
        available_datasets["Transported Passengers"],
        available_datasets["Stop Spatial Features"],
        available_datasets["Stop Connections"],
    ]
    if weather_path:
        silver_sources.append(weather_path)
    if calendar_path:
        silver_sources.append(calendar_path)

    write_layer_lineage(
        layers["root"],
        run_id=layers["run_id"],
        context="mode_b_build",
        bronze_artifacts=[],
        silver_artifacts=silver_sources,
        gold_artifacts=gold_artifacts,
    )

    (layers["root"] / "_SUCCESS").touch()
    logger.info(f"Mode B build complete: {layers['root']}")
    return str(layers["root"]), all_warnings

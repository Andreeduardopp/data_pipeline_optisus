"""
Schema introspection and validation helpers for the admin UI.
Provides model/field listing and default required fields from schemas.py.
"""
from typing import Any, Dict, List, Type, Tuple

from pydantic import BaseModel

# Tabular schema models only (exclude GeographicData, which is for JSON config)
# Gold-layer output schemas are also excluded — they validate pipeline output, not uploads.
from schemas import (
    FleetIdentification,
    FleetEnergyPerformance,
    ElectricFleetCharacteristics,
    OperationsAndCirculation,
    TransportedPassengers,
    ChargingInfrastructure,
    CalendarEvent,
    WeatherObservation,
    StopSpatialFeatures,
    StopConnection,
    FinancialEconomicData,
    LifespanAndDepreciation,
)

TABULAR_SCHEMAS: List[Tuple[str, Type[BaseModel]]] = [
    ("Fleet Identification", FleetIdentification),
    ("Fleet Energy Performance", FleetEnergyPerformance),
    ("Electric Fleet Characteristics", ElectricFleetCharacteristics),
    ("Operations and Circulation", OperationsAndCirculation),
    ("Transported Passengers", TransportedPassengers),
    ("Charging Infrastructure", ChargingInfrastructure),
    ("Calendar Events", CalendarEvent),
    ("Weather Observations", WeatherObservation),
    ("Stop Spatial Features", StopSpatialFeatures),
    ("Stop Connections", StopConnection),
    ("Financial & Economic Data", FinancialEconomicData),
    ("Lifespan and Depreciation", LifespanAndDepreciation),
]


def get_schema_fields(model_class: Type[BaseModel]) -> List[Dict[str, Any]]:
    """
    Return list of field info for a Pydantic model: name, required by default, description, type.
    """
    result = []
    for name, info in model_class.model_fields.items():
        # Pydantic 2: required = no default and not Optional
        required = info.is_required()
        desc = (info.description or "").strip()
        result.append({
            "name": name,
            "required_by_default": required,
            "description": desc or "(no description)",
            "type": _format_annotation(info.annotation),
        })
    return result


def _format_annotation(annotation: Any) -> str:
    """Human-readable type string for UI."""
    if annotation is None:
        return "None"
    if hasattr(annotation, "__name__"):
        return getattr(annotation, "__name__", str(annotation))
    origin = getattr(annotation, "__origin__", None)
    args = getattr(annotation, "__args__", ())
    if origin is not None and args:
        name = getattr(origin, "__name__", str(origin))
        args_str = ", ".join(_format_annotation(a) for a in args)
        return f"{name}[{args_str}]"
    return str(annotation)


def get_default_required_fields(model_class: Type[BaseModel]) -> List[str]:
    """Return field names that are required by the schema (no default)."""
    return [
        name for name, info in model_class.model_fields.items()
        if info.is_required()
    ]


def get_all_field_names(model_class: Type[BaseModel]) -> List[str]:
    """Return all field names of the model in definition order."""
    return list(model_class.model_fields.keys())


def generate_template_csv(model_class: Type[BaseModel]) -> str:
    """Return a CSV string with two rows: column headers and a description hint row."""
    fields = get_schema_fields(model_class)
    header = ",".join(f["name"] for f in fields)
    hints = ",".join(
        f'"{f["description"]}"' if "," in f["description"] else f["description"]
        for f in fields
    )
    return f"{header}\n{hints}\n"


# ---------------------------------------------------------------------------
# Dual-mode requirements
# ---------------------------------------------------------------------------

MODE_A = "Mode A"
MODE_B = "Mode B"

MODE_REQUIREMENTS: Dict[str, Dict[str, List[str]]] = {
    MODE_A: {
        "required": [
            "Transported Passengers",
            "Financial & Economic Data",
        ],
        "optional": [
            "Weather Observations",
            "Calendar Events",
        ],
    },
    MODE_B: {
        "required": [
            "Transported Passengers",
            "Stop Spatial Features",
            "Stop Connections",
        ],
        "optional": [
            "Weather Observations",
            "Calendar Events",
        ],
    },
}

MODE_DESCRIPTIONS: Dict[str, Dict[str, str]] = {
    MODE_A: {
        "title": "Multivariate Time-Series Forecasting",
        "description": (
            "Produces TimeSeriesDemandSample artifacts: demand sequences "
            "grouped by line, enriched with temporal features, weather, and "
            "macroeconomic indicators. Suitable for LSTM / Transformer models."
        ),
        "output_artifact": "mode_a_timeseries.parquet",
    },
    MODE_B: {
        "title": "Spatio-Temporal Graph Forecasting",
        "description": (
            "Produces SpatioTemporalDemandSample artifacts and a NetworkTopology "
            "graph (nodes + edges). Suitable for GNN / Graph-Transformer models."
        ),
        "output_artifact": "mode_b_spatiotemporal.parquet + network_topology.json",
    },
}


def get_mode_requirements(mode: str) -> Dict[str, List[str]]:
    """Return {'required': [...], 'optional': [...]} dataset labels for a mode."""
    return MODE_REQUIREMENTS[mode]


def get_mode_dataset_checklist(
    mode: str,
    available_datasets: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Build a checklist marking each dataset as present/missing for the mode."""
    reqs = MODE_REQUIREMENTS[mode]
    checklist: List[Dict[str, Any]] = []
    for label in reqs["required"]:
        checklist.append({
            "dataset": label,
            "kind": "required",
            "available": label in available_datasets,
            "path": available_datasets.get(label),
        })
    for label in reqs["optional"]:
        checklist.append({
            "dataset": label,
            "kind": "optional",
            "available": label in available_datasets,
            "path": available_datasets.get(label),
        })
    return checklist

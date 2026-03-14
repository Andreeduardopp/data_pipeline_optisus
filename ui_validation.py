"""
Schema introspection and validation helpers for the admin UI.
Provides model/field listing and default required fields from schemas.py.
"""
from typing import Any, Dict, List, Type, Tuple

from pydantic import BaseModel

# Tabular schema models only (exclude GeographicData, which is for JSON config)
from schemas import (
    FleetIdentification,
    FleetEnergyPerformance,
    ElectricFleetCharacteristics,
    OperationsAndCirculation,
    TransportedPassengers,
    ChargingInfrastructure,
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

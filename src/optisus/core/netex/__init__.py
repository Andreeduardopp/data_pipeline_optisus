"""
NeTEx export (PT-EPIP profile).

Translates a project's GTFS SQLite database into a zipped multi-file
NeTEx dataset conforming to the Portuguese PT-EPIP profile, ready for
submission to the PNDT (Ponto Nacional de Acesso).

Phase 1 ships ``config``, ``urn``, and ``mappings``. Translator, XML
builder, and exporter arrive in subsequent phases.
"""

from optisus.core.netex.config import (
    NETEX_CONFIG_FILENAME,
    PLACEHOLDER_CODESPACE,
    NetexAuthority,
    NetexExportConfig,
    NetexOperator,
    load_netex_config,
    save_netex_config,
)
from optisus.core.netex.mappings import (
    LOCATION_TYPE_TO_STOP_PLACE_TYPE,
    ROUTE_TYPE_TO_VEHICLE_MODE,
    WHEELCHAIR_TO_LIMITATION,
    vehicle_mode_for_route_type,
)
from optisus.core.netex.urn import build_urn, sanitise_local_id

__all__ = [
    "NETEX_CONFIG_FILENAME",
    "PLACEHOLDER_CODESPACE",
    "NetexAuthority",
    "NetexExportConfig",
    "NetexOperator",
    "load_netex_config",
    "save_netex_config",
    "LOCATION_TYPE_TO_STOP_PLACE_TYPE",
    "ROUTE_TYPE_TO_VEHICLE_MODE",
    "WHEELCHAIR_TO_LIMITATION",
    "vehicle_mode_for_route_type",
    "build_urn",
    "sanitise_local_id",
]

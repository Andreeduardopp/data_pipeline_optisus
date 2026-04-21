"""
NeTEx export orchestrator.

Public entry point ``export_netex`` wires everything together:

  1. Pre-flight validation (reuses GTFS ``validate_before_export`` plus
     NeTEx-specific checks — codespace ≠ placeholder, every route_type
     maps to a known VehicleMode).
  2. Translate ``gtfs.db`` into a :class:`NetexDataset` (``translator``).
  3. Serialise the dataset into the three PT-EPIP output XMLs
     (``frames`` + ``xml_builder``).
  4. Write the multi-file zip under
     ``projects/<slug>/exports/netex/<timestamp>/`` using the PNDT
     naming convention
     ``NETEX_PT_{OPERATOR_SHORT_NAME}_{VERSION}_{YYYYMMDDHHMM}.zip``.
"""

import logging
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from optisus.core.gtfs.exporter import validate_before_export
from optisus.core.netex.config import NetexExportConfig
from optisus.core.netex.frames import (
    build_lines_publication,
    build_stops_publication,
    build_timetable_publication_for_line,
)
from optisus.core.netex.mappings import ROUTE_TYPE_TO_VEHICLE_MODE
from optisus.core.netex.translator import translate_project
from optisus.core.netex.urn import sanitise_local_id
from optisus.core.netex.xml_builder import serialize
from optisus.core.storage.layers import PROJECTS_ROOT

logger = logging.getLogger(__name__)

STOPS_FILENAME = "SiteFrame_Stops.xml"
LINES_FILENAME = "ServiceFrame_Lines.xml"
TIMETABLE_FILENAME_PREFIX = "TimetableFrame_Schedules_Line_"


@dataclass
class NetexExportResult:
    success: bool = False
    zip_path: Optional[str] = None
    files_included: List[str] = field(default_factory=list)
    stop_place_count: int = 0
    line_count: int = 0
    service_journey_count: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Pre-flight
# ═══════════════════════════════════════════════════════════════════════════

def _validate_netex_preflight(
    project_slug: str, config: NetexExportConfig,
) -> NetexExportResult:
    result = NetexExportResult()

    gtfs_check = validate_before_export(project_slug)
    result.warnings = list(gtfs_check.warnings)
    if not gtfs_check.can_export:
        result.errors = list(gtfs_check.errors)
        return result

    if config.is_placeholder():
        result.errors.append(
            "Codespace is still the placeholder 'FIXME'. Set the operator's NIF "
            "or IMT-assigned short name in the NeTEx config before exporting — "
            "PNDT will reject any submission without a registered codespace."
        )
        return result

    # Defer unmapped-route-type check to the translator for now — it will
    # raise KeyError with a clear message. We surface that at the call site.
    _ = ROUTE_TYPE_TO_VEHICLE_MODE  # kept for clarity; used during translate
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Zip writer
# ═══════════════════════════════════════════════════════════════════════════

def _compose_zip_name(operator_short_name: str, version: str) -> str:
    safe_op = sanitise_local_id(operator_short_name).upper()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    return f"NETEX_PT_{safe_op}_{version}_{stamp}.zip"


def _timetable_filename(line_ref: str) -> str:
    local = sanitise_local_id(line_ref.rsplit(":", 1)[-1])
    return f"{TIMETABLE_FILENAME_PREFIX}{local}.xml"


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def export_netex(
    project_slug: str,
    config: NetexExportConfig,
    output_dir: Optional[Path] = None,
) -> NetexExportResult:
    """Produce a PT-EPIP NeTEx zip from the project's GTFS database."""
    preflight = _validate_netex_preflight(project_slug, config)
    if preflight.errors:
        return preflight

    result = NetexExportResult(warnings=preflight.warnings)

    version = datetime.now(timezone.utc).strftime("%Y%m%d")

    try:
        dataset = translate_project(project_slug, config, version)
    except KeyError as e:
        result.errors.append(f"Unmapped GTFS value during translation: {e}")
        return result

    result.stop_place_count = len(dataset.stop_places)
    result.line_count = len(dataset.lines)
    result.service_journey_count = len(dataset.service_journeys)

    # Build XML trees
    stops_xml = serialize(build_stops_publication(dataset, config, version))
    lines_xml = serialize(build_lines_publication(dataset, config, version))

    per_line_xmls: List[tuple[str, bytes]] = []
    for line_ref, journeys in dataset.service_journeys_by_line().items():
        tree = build_timetable_publication_for_line(
            dataset, config, line_ref, journeys, version,
        )
        per_line_xmls.append((_timetable_filename(line_ref), serialize(tree)))

    # Write zip
    if output_dir is None:
        export_root = PROJECTS_ROOT / project_slug / "exports" / "netex"
    else:
        export_root = Path(output_dir)

    ts_dir = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    target_dir = export_root / ts_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    zip_name = _compose_zip_name(config.operator.short_name, version)
    zip_path = target_dir / zip_name

    with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(STOPS_FILENAME, stops_xml)
        zf.writestr(LINES_FILENAME, lines_xml)
        for filename, payload in per_line_xmls:
            zf.writestr(filename, payload)
        result.files_included = [STOPS_FILENAME, LINES_FILENAME, *[f for f, _ in per_line_xmls]]

    result.success = True
    result.zip_path = str(zip_path)
    logger.info(
        "NeTEx export: %s (%d stop places, %d lines, %d journeys)",
        zip_path, result.stop_place_count, result.line_count, result.service_journey_count,
    )
    return result

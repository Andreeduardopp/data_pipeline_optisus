"""Scratch script — produce a real NeTEx export zip and unpack it to ./_netex_inspect/.

Deleted after inspection. Do NOT commit.
"""

import shutil
import sys
import zipfile
from pathlib import Path

# Redirect PROJECTS_ROOT to a local temp dir *before* importing modules that cache it.
ROOT = Path(__file__).parent
INSPECT_DIR = ROOT / "_netex_inspect"
if INSPECT_DIR.exists():
    shutil.rmtree(INSPECT_DIR)
INSPECT_DIR.mkdir()

FAKE_PROJECTS = INSPECT_DIR / "projects"
FAKE_PROJECTS.mkdir()

from optisus.core.storage import layers as storage_layers
from optisus.core.gtfs import database as gtfs_database
from optisus.core.gtfs import exporter as gtfs_exporter
from optisus.core.netex import exporter as netex_exporter
from optisus.core.netex import config as netex_config

storage_layers.DATA_LAKE_ROOT = INSPECT_DIR
storage_layers.PROJECTS_ROOT = FAKE_PROJECTS
gtfs_database.PROJECTS_ROOT = FAKE_PROJECTS
gtfs_exporter.PROJECTS_ROOT = FAKE_PROJECTS
netex_exporter.PROJECTS_ROOT = FAKE_PROJECTS
netex_config.PROJECTS_ROOT = FAKE_PROJECTS

from optisus.core.gtfs.database import create_gtfs_database, upsert_records
from optisus.core.netex.config import NetexAuthority, NetexExportConfig, NetexOperator
from optisus.core.netex.exporter import export_netex

slug = "demo"
(FAKE_PROJECTS / slug).mkdir()
create_gtfs_database(slug)

upsert_records(slug, "agency", [{
    "agency_id": "GUIMABUS", "agency_name": "Guimabus",
    "agency_url": "https://guimabus.pt", "agency_timezone": "Europe/Lisbon",
    "agency_email": "info@guimabus.pt",
}])
upsert_records(slug, "stops", [
    {"stop_id": "STATION_1", "stop_name": "Central Station",
     "stop_lat": 41.44, "stop_lon": -8.29, "location_type": 1},
    {"stop_id": "QUAY_A", "stop_name": "Central - Platform A",
     "stop_lat": 41.440, "stop_lon": -8.291, "location_type": 0,
     "parent_station": "STATION_1", "platform_code": "A"},
    {"stop_id": "ORPHAN", "stop_name": "Mercado",
     "stop_lat": 41.45, "stop_lon": -8.30, "location_type": 0},
])
upsert_records(slug, "routes", [
    {"route_id": "R1", "agency_id": "GUIMABUS",
     "route_short_name": "1", "route_long_name": "Centro - Aeroporto",
     "route_type": 3, "route_color": "0077BE", "route_text_color": "FFFFFF"},
    {"route_id": "R2", "agency_id": "GUIMABUS",
     "route_short_name": "N1", "route_long_name": "Noite",
     "route_type": 3},
])
upsert_records(slug, "calendar", [{
    "service_id": "WK",
    "monday": 1, "tuesday": 1, "wednesday": 1, "thursday": 1,
    "friday": 1, "saturday": 0, "sunday": 0,
    "start_date": "20260101", "end_date": "20261231",
}])
upsert_records(slug, "calendar_dates", [{
    "service_id": "WK", "date": "20260501", "exception_type": 2,
}])
upsert_records(slug, "trips", [
    {"route_id": "R1", "service_id": "WK", "trip_id": "T1",
     "direction_id": 0, "trip_headsign": "Aeroporto"},
    {"route_id": "R2", "service_id": "WK", "trip_id": "T_NIGHT",
     "direction_id": 0, "trip_headsign": "Overnight"},
])
upsert_records(slug, "stop_times", [
    {"trip_id": "T1", "stop_id": "QUAY_A", "stop_sequence": 1,
     "arrival_time": "08:00:00", "departure_time": "08:00:00"},
    {"trip_id": "T1", "stop_id": "ORPHAN", "stop_sequence": 2,
     "arrival_time": "08:15:00", "departure_time": "08:15:00"},
    {"trip_id": "T_NIGHT", "stop_id": "QUAY_A", "stop_sequence": 1,
     "arrival_time": "23:50:00", "departure_time": "23:50:00"},
    {"trip_id": "T_NIGHT", "stop_id": "ORPHAN", "stop_sequence": 2,
     "arrival_time": "25:10:00", "departure_time": "25:10:00"},
])

cfg = NetexExportConfig(
    codespace="GUIMABUS",
    authority=NetexAuthority(
        id="CIM_AVE", name="CIM do Ave", contact_email="legal@cimave.pt",
    ),
    operator=NetexOperator(
        id="GUIMABUS", name="Guimabus", short_name="GUIMABUS",
        contact_email="info@guimabus.pt",
    ),
    participant_ref="PNDT_TEST",
)

result = export_netex(slug, cfg)
if not result.success:
    print("EXPORT FAILED:", result.errors, file=sys.stderr)
    sys.exit(1)

zip_path = Path(result.zip_path)
print(f"zip: {zip_path}")
print(f"files: {result.files_included}")
print(f"counts: stop_places={result.stop_place_count} lines={result.line_count} "
      f"service_journeys={result.service_journey_count}")

extract_dir = INSPECT_DIR / "unpacked"
extract_dir.mkdir(exist_ok=True)
with zipfile.ZipFile(zip_path) as zf:
    zf.extractall(extract_dir)

for f in sorted(extract_dir.iterdir()):
    print(f"unpacked: {f.name} ({f.stat().st_size} bytes)")

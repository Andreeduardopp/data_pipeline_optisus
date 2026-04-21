"""
End-to-end tests for the NeTEx export pipeline.

Covers:
  - Calendar translation (bitmask, exception overlay, calendar_dates-only)
  - Quay hierarchy (parent_station, orphan-synthesis)
  - Overnight trips produce a DayOffset
  - Multi-agency feed yields one Operator per agency plus Authority
  - Organisation + ResponsibilityRoleAssignment always emitted (dual entity)
  - XML namespaces and root element
  - Zip naming (PNDT convention) and multi-file layout
  - Placeholder codespace blocks export
"""

import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import pytest
from lxml import etree

from optisus.core.gtfs.database import create_gtfs_database, upsert_records
from optisus.core.netex.calendar import translate_calendar
from optisus.core.netex.config import (
    PLACEHOLDER_CODESPACE,
    NetexAuthority,
    NetexExportConfig,
    NetexOperator,
)
from optisus.core.netex.exporter import (
    LINES_FILENAME,
    STOPS_FILENAME,
    TIMETABLE_FILENAME_PREFIX,
    export_netex,
)
from optisus.core.netex.translator import translate_project, translate_site_frame


NETEX_NS = "http://www.netex.org.uk/netex"
NS = {"n": NETEX_NS, "xsi": "http://www.w3.org/2001/XMLSchema-instance"}


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def isolated_netex(tmp_path, monkeypatch):
    """Redirect every PROJECTS_ROOT used across the netex pipeline to tmp_path."""
    from optisus.core.gtfs import database as gtfs_database
    from optisus.core.gtfs import exporter as gtfs_exporter
    from optisus.core.netex import config as netex_config
    from optisus.core.netex import exporter as netex_exporter
    from optisus.core.storage import layers as storage_layers

    fake_root = tmp_path / "data_lake_outputs"
    fake_projects = fake_root / "projects"
    fake_projects.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage_layers, "DATA_LAKE_ROOT", fake_root)
    monkeypatch.setattr(storage_layers, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(gtfs_database, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(gtfs_exporter, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(netex_exporter, "PROJECTS_ROOT", fake_projects)
    monkeypatch.setattr(netex_config, "PROJECTS_ROOT", fake_projects)
    return fake_projects


def _make_config(codespace: str = "STCP") -> NetexExportConfig:
    return NetexExportConfig(
        codespace=codespace,
        authority=NetexAuthority(
            id="CIM_AVE", name="CIM do Ave", contact_email="legal@cimave.pt",
        ),
        operator=NetexOperator(
            id="GUIMABUS", name="Guimabus", short_name="GUIMABUS",
        ),
    )


def _seed_minimal_feed(slug: str) -> None:
    """Populate a tiny but complete GTFS database.

    Two routes, two stops (one is a quay under a station), two trips on
    different services, with one overnight trip to exercise DayOffset.
    """
    (Path(__file__).parent.parent / "data_lake_outputs").mkdir(exist_ok=True)
    create_gtfs_database(slug)

    upsert_records(slug, "agency", [{
        "agency_id": "GUIMABUS", "agency_name": "Guimabus",
        "agency_url": "https://guimabus.pt", "agency_timezone": "Europe/Lisbon",
    }])
    upsert_records(slug, "stops", [
        {"stop_id": "STATION_1", "stop_name": "Central Station",
         "stop_lat": 41.44, "stop_lon": -8.29, "location_type": 1},
        {"stop_id": "QUAY_A", "stop_name": "Central — Platform A",
         "stop_lat": 41.440, "stop_lon": -8.291, "location_type": 0,
         "parent_station": "STATION_1"},
        {"stop_id": "ORPHAN", "stop_name": "Mercado",
         "stop_lat": 41.45, "stop_lon": -8.30, "location_type": 0},
    ])
    upsert_records(slug, "routes", [
        {"route_id": "R1", "agency_id": "GUIMABUS",
         "route_short_name": "1", "route_long_name": "Centro — Aeroporto",
         "route_type": 3},
        {"route_id": "R2", "agency_id": "GUIMABUS",
         "route_short_name": "2", "route_long_name": "Noite",
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


# ═══════════════════════════════════════════════════════════════════════════
# Calendar translation
# ═══════════════════════════════════════════════════════════════════════════

class TestCalendarTranslation:

    def test_bitmask_becomes_days_of_week(self):
        dts, ops, ass = translate_calendar(
            calendar_rows=[{
                "service_id": "WK",
                "monday": 1, "tuesday": 1, "wednesday": 1,
                "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
                "start_date": "20260101", "end_date": "20261231",
            }],
            calendar_dates_rows=[],
            codespace="STCP", version="20260420",
        )
        assert len(dts) == 1
        assert dts[0].days_of_week == [
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        ]
        assert len(ops) == 1
        assert ops[0].from_date == "2026-01-01"
        assert ops[0].to_date == "2026-12-31"
        assert len(ass) == 1
        assert ass[0].is_available is True

    def test_exception_overlay(self):
        _, _, ass = translate_calendar(
            calendar_rows=[{
                "service_id": "WK",
                "monday": 1, "tuesday": 1, "wednesday": 1,
                "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
                "start_date": "20260101", "end_date": "20261231",
            }],
            calendar_dates_rows=[
                {"service_id": "WK", "date": "20260501", "exception_type": 2},
                {"service_id": "WK", "date": "20260606", "exception_type": 1},
            ],
            codespace="STCP", version="20260420",
        )
        dates = {a.date: a.is_available for a in ass if a.date}
        assert dates == {"2026-05-01": False, "2026-06-06": True}

    def test_calendar_dates_only(self):
        dts, ops, ass = translate_calendar(
            calendar_rows=[],
            calendar_dates_rows=[
                {"service_id": "HOL", "date": "20260601", "exception_type": 1},
            ],
            codespace="STCP", version="20260420",
        )
        assert len(dts) == 1
        assert dts[0].days_of_week == []
        assert ops == []
        assert len(ass) == 1
        assert ass[0].date == "2026-06-01"


# ═══════════════════════════════════════════════════════════════════════════
# Site frame — Quay hierarchy
# ═══════════════════════════════════════════════════════════════════════════

class TestQuayHierarchy:

    def test_station_contains_child_quays(self):
        stops = [
            {"stop_id": "ST1", "stop_name": "Station", "stop_lat": 1.0, "stop_lon": 1.0,
             "location_type": 1, "parent_station": None},
            {"stop_id": "Q1", "stop_name": "Platform A", "stop_lat": 1.0, "stop_lon": 1.0,
             "location_type": 0, "parent_station": "ST1"},
        ]
        places = translate_site_frame(stops, "STCP", "20260420")
        assert len(places) == 1
        assert len(places[0].quays) == 1
        assert places[0].quays[0].name == "Platform A"

    def test_orphan_stop_gets_synthetic_stop_place(self):
        stops = [
            {"stop_id": "LONE", "stop_name": "Lone Stop", "stop_lat": 2.0, "stop_lon": 2.0,
             "location_type": 0, "parent_station": None},
        ]
        places = translate_site_frame(stops, "STCP", "20260420")
        assert len(places) == 1
        assert len(places[0].quays) == 1
        assert places[0].id.endswith("LONE_place")


# ═══════════════════════════════════════════════════════════════════════════
# End-to-end translator (against a real SQLite)
# ═══════════════════════════════════════════════════════════════════════════

class TestTranslateProject:

    def test_produces_complete_dataset(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)
        cfg = _make_config()

        ds = translate_project(slug, cfg, version="20260420")

        assert ds.codespace.xmlns == "STCP"
        # Authority + Operator
        kinds = [o.kind for o in ds.organisations]
        assert kinds.count("Authority") == 1
        assert kinds.count("Operator") >= 1
        assert len(ds.role_assignments) == 2

        # Stops — one real StopPlace + one synthetic for ORPHAN
        names = {sp.name for sp in ds.stop_places}
        assert "Central Station" in names
        assert "Mercado" in names

        assert {ln.public_code for ln in ds.lines} == {"1", "2"}
        assert {sj.id.rsplit(":", 1)[-1] for sj in ds.service_journeys} == {"T1", "T_NIGHT"}

    def test_overnight_trip_emits_day_offset(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)
        cfg = _make_config()

        ds = translate_project(slug, cfg, version="20260420")
        night = [sj for sj in ds.service_journeys
                 if sj.id.endswith(":T_NIGHT")][0]
        # The 25:10:00 passing time should be represented as 01:10:00 + day_offset=1
        offsets = [pt.departure_day_offset for pt in night.passing_times]
        assert 1 in offsets
        times = [pt.departure_time for pt in night.passing_times]
        assert "01:10:00" in times


# ═══════════════════════════════════════════════════════════════════════════
# Pre-flight gating
# ═══════════════════════════════════════════════════════════════════════════

class TestPreflight:

    def test_placeholder_codespace_blocks_export(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)
        cfg = _make_config(codespace=PLACEHOLDER_CODESPACE)

        result = export_netex(slug, cfg)
        assert result.success is False
        assert any("FIXME" in e or "placeholder" in e for e in result.errors)
        assert result.zip_path is None


# ═══════════════════════════════════════════════════════════════════════════
# End-to-end export
# ═══════════════════════════════════════════════════════════════════════════

class TestExportRoundtrip:

    def test_zip_name_follows_pndt_convention(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        assert result.success, result.errors
        name = Path(result.zip_path).name
        assert name.startswith("NETEX_PT_GUIMABUS_")
        assert name.endswith(".zip")
        # NETEX_PT_{OP}_{YYYYMMDD}_{YYYYMMDDHHMM}.zip → 5 underscore-separated segments before .zip
        parts = name.removesuffix(".zip").split("_")
        assert parts[0] == "NETEX"
        assert parts[1] == "PT"
        assert parts[2] == "GUIMABUS"
        assert len(parts[3]) == 8   # YYYYMMDD version
        assert len(parts[4]) == 12  # YYYYMMDDHHMM timestamp

    def test_zip_contains_expected_files(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        assert result.success, result.errors

        with zipfile.ZipFile(result.zip_path) as zf:
            names = set(zf.namelist())

        assert STOPS_FILENAME in names
        assert LINES_FILENAME in names
        # One timetable per route (R1, R2)
        timetable_files = [n for n in names if n.startswith(TIMETABLE_FILENAME_PREFIX)]
        assert len(timetable_files) == 2

    def test_stops_xml_has_correct_namespaces_and_root(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            stops_xml = zf.read(STOPS_FILENAME)

        root = etree.fromstring(stops_xml)
        assert root.tag == f"{{{NETEX_NS}}}PublicationDelivery"
        assert root.nsmap[None] == NETEX_NS
        assert "xsi" in root.nsmap

        # CompositeFrame wraps the frames now — they live under frames/
        frames = root.xpath(".//n:CompositeFrame/n:frames/*", namespaces=NS)
        frame_tags = {etree.QName(f).localname for f in frames}
        assert frame_tags == {"ResourceFrame", "SiteFrame", "ServiceCalendarFrame"}

    def test_resource_frame_emits_two_organisations_and_role_assignment(
        self, isolated_netex,
    ):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            stops_xml = zf.read(STOPS_FILENAME)
        root = etree.fromstring(stops_xml)

        authorities = root.xpath(".//n:ResourceFrame//n:Authority", namespaces=NS)
        operators = root.xpath(".//n:ResourceFrame//n:Operator", namespaces=NS)
        roles = root.xpath(
            ".//n:ResourceFrame//n:ResponsibilityRoleAssignment", namespaces=NS,
        )
        assert len(authorities) == 1
        assert len(operators) >= 1
        assert len(roles) == 2

    def test_timetable_xml_has_service_journeys(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            timetable_names = [n for n in zf.namelist()
                               if n.startswith(TIMETABLE_FILENAME_PREFIX)]
            assert timetable_names
            xml = zf.read(timetable_names[0])

        root = etree.fromstring(xml)
        sjs = root.xpath(".//n:ServiceJourney", namespaces=NS)
        assert len(sjs) >= 1

    def test_urns_use_pt_codespace_prefix(self, isolated_netex):
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config("980123456"))
        with zipfile.ZipFile(result.zip_path) as zf:
            stops_xml = zf.read(STOPS_FILENAME)
        root = etree.fromstring(stops_xml)

        stop_places = root.xpath(".//n:StopPlace", namespaces=NS)
        assert stop_places
        for sp in stop_places:
            assert sp.get("id").startswith("PT:980123456:StopPlace:")


# ═══════════════════════════════════════════════════════════════════════════
# P0 + P1 PT-EPIP compliance fixes
# ═══════════════════════════════════════════════════════════════════════════

class TestEpipCompliance:
    """Regression tests for the fixes uncovered during manual XML inspection."""

    def test_journey_pattern_is_service_journey_pattern_with_line_ref(
        self, isolated_netex,
    ):
        """P0-1: was JourneyPattern+RouteRef (ref'ing a Line URN) — now ServiceJourneyPattern+LineRef."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            lines_xml = zf.read(LINES_FILENAME)
        root = etree.fromstring(lines_xml)

        # Old element name must be gone
        assert root.xpath(".//n:JourneyPattern", namespaces=NS) == []
        assert root.xpath(".//n:RouteRef", namespaces=NS) == []

        sjps = root.xpath(".//n:ServiceJourneyPattern", namespaces=NS)
        assert sjps
        for sjp in sjps:
            line_refs = sjp.xpath("./n:LineRef", namespaces=NS)
            assert len(line_refs) == 1
            assert ":Line:" in line_refs[0].get("ref")

    def test_stop_place_transport_mode_derived_from_routes(self, isolated_netex):
        """P0-2: transport_mode must be a real enum value, not 'unknown'."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            stops_xml = zf.read(STOPS_FILENAME)
        root = etree.fromstring(stops_xml)

        modes = root.xpath(".//n:StopPlace/n:TransportMode/text()", namespaces=NS)
        assert modes, "Expected TransportMode elements on every StopPlace"
        assert "unknown" not in modes
        # Feed has only route_type=3 (bus), so every served stop should be 'bus'.
        assert set(modes) == {"bus"}

    def test_responsibility_role_type_element_is_canonical(self, isolated_netex):
        """P0-3: must be <ResponsibilityRoleType> with enum values, not invented tags."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            stops_xml = zf.read(STOPS_FILENAME)
        root = etree.fromstring(stops_xml)

        # Invented tags must not appear anywhere
        assert root.xpath(".//n:DataRoleType", namespaces=NS) == []
        assert root.xpath(".//n:StakeholderRoleType", namespaces=NS) == []

        roles = root.xpath(
            ".//n:ResponsibilityRoleAssignment/n:ResponsibilityRoleType/text()",
            namespaces=NS,
        )
        assert set(roles) == {"ownership", "operation"}

        # And the ref tag must be the ResponsibleOrganisationRef substitution
        refs = root.xpath(
            ".//n:ResponsibilityRoleAssignment/n:ResponsibleOrganisationRef",
            namespaces=NS,
        )
        assert len(refs) == 2

    def test_timetabled_passing_time_has_no_version_attribute(self, isolated_netex):
        """P0-4: concrete TPT instances should not carry version='any'."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            tt_names = [n for n in zf.namelist()
                        if n.startswith(TIMETABLE_FILENAME_PREFIX)]
            xml = zf.read(tt_names[0])
        root = etree.fromstring(xml)

        tpts = root.xpath(".//n:TimetabledPassingTime", namespaces=NS)
        assert tpts
        for tpt in tpts:
            assert tpt.get("version") is None
            assert tpt.get("id") is not None

    def test_composite_frame_carries_epip_profile_ref(self, isolated_netex):
        """P1-5 + P1-7: each file wrapped in CompositeFrame with EPIP TypeOfFrameRef."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            files_and_expected = [
                (STOPS_FILENAME, "epip:EU_PI_STOP"),
                (LINES_FILENAME, "epip:EU_PI_LINE_OFFER"),
            ]
            for name, expected_ref in files_and_expected:
                xml = zf.read(name)
                root = etree.fromstring(xml)
                composites = root.xpath(".//n:CompositeFrame", namespaces=NS)
                assert len(composites) == 1, name
                refs = composites[0].xpath(
                    "./n:TypesOfFrameRef/n:TypeOfFrameRef/@ref", namespaces=NS,
                )
                assert expected_ref in refs, f"{name}: missing {expected_ref}"

            # Timetable file
            tt_names = [n for n in zf.namelist()
                        if n.startswith(TIMETABLE_FILENAME_PREFIX)]
            tt_root = etree.fromstring(zf.read(tt_names[0]))
            tt_refs = tt_root.xpath(
                ".//n:CompositeFrame/n:TypesOfFrameRef/n:TypeOfFrameRef/@ref",
                namespaces=NS,
            )
            assert "epip:EU_PI_TIMETABLE" in tt_refs

    def test_composite_frame_has_valid_between(self, isolated_netex):
        """P1-6: CompositeFrame must carry a ValidBetween derived from calendar."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            root = etree.fromstring(zf.read(STOPS_FILENAME))
        valids = root.xpath(".//n:CompositeFrame/n:ValidBetween", namespaces=NS)
        assert len(valids) == 1
        from_dates = valids[0].xpath("./n:FromDate/text()", namespaces=NS)
        to_dates = valids[0].xpath("./n:ToDate/text()", namespaces=NS)
        assert from_dates and from_dates[0].startswith("2026-01-01")
        assert to_dates and to_dates[0].startswith("2026-12-31")

    def test_resource_frame_emits_data_source(self, isolated_netex):
        """P1-8: ResourceFrame/dataSources/DataSource for lineage."""
        slug = "demo"
        (isolated_netex / slug).mkdir(exist_ok=True)
        _seed_minimal_feed(slug)

        result = export_netex(slug, _make_config())
        with zipfile.ZipFile(result.zip_path) as zf:
            root = etree.fromstring(zf.read(STOPS_FILENAME))
        sources = root.xpath(".//n:ResourceFrame/n:dataSources/n:DataSource",
                             namespaces=NS)
        assert len(sources) >= 1
        assert sources[0].get("id").startswith("PT:STCP:DataSource:")

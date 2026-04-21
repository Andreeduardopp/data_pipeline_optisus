"""
Unit tests for the NeTEx export scaffold — config, urn, mappings.

Covers:
  - NetexExportConfig codespace regex + placeholder gating
  - Persistence round-trip via load/save
  - URN builder format and local_id sanitisation
  - Mapping coverage for every RouteType enum
"""

import json

import pytest
from pydantic import ValidationError

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
    ROUTE_TYPE_TO_VEHICLE_MODE,
    vehicle_mode_for_route_type,
)
from optisus.core.netex.urn import build_urn, sanitise_local_id
from optisus.core.schemas.gtfs import RouteType


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def isolated_netex_projects(tmp_path, monkeypatch):
    """Redirect PROJECTS_ROOT inside netex.config to a temp directory."""
    from optisus.core.netex import config as netex_config
    monkeypatch.setattr(netex_config, "PROJECTS_ROOT", tmp_path / "projects")
    (tmp_path / "projects" / "demo").mkdir(parents=True, exist_ok=True)
    return "demo"


def _valid_config(codespace: str = "980123456") -> NetexExportConfig:
    return NetexExportConfig(
        codespace=codespace,
        authority=NetexAuthority(id="CIM_AVE", name="CIM do Ave"),
        operator=NetexOperator(
            id="GUIMABUS", name="Guimabus", short_name="GUIMABUS"
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════
# NetexExportConfig — codespace rules
# ═══════════════════════════════════════════════════════════════════════════

class TestNetexExportConfigCodespace:

    def test_accepts_numeric_nif(self):
        cfg = _valid_config("980123456")
        assert cfg.codespace == "980123456"
        assert cfg.codespace_full == "PT:980123456"

    def test_accepts_upper_alphanumeric(self):
        cfg = _valid_config("STCP")
        assert cfg.codespace == "STCP"

    def test_accepts_placeholder_but_flags_it(self):
        cfg = _valid_config(PLACEHOLDER_CODESPACE)
        assert cfg.is_placeholder() is True

    def test_non_placeholder_is_not_flagged(self):
        cfg = _valid_config("STCP")
        assert cfg.is_placeholder() is False

    def test_rejects_lowercase(self):
        with pytest.raises(ValidationError):
            _valid_config("stcp")

    def test_rejects_whitespace(self):
        with pytest.raises(ValidationError):
            _valid_config("ST CP")

    def test_rejects_hyphen(self):
        with pytest.raises(ValidationError):
            _valid_config("PT-TEST")

    def test_rejects_empty(self):
        with pytest.raises(ValidationError):
            _valid_config("")

    def test_default_codespace_is_placeholder(self):
        cfg = NetexExportConfig(
            authority=NetexAuthority(id="A", name="A"),
            operator=NetexOperator(id="O", name="O", short_name="O"),
        )
        assert cfg.codespace == PLACEHOLDER_CODESPACE
        assert cfg.is_placeholder() is True


# ═══════════════════════════════════════════════════════════════════════════
# NetexExportConfig — defaults and persistence
# ═══════════════════════════════════════════════════════════════════════════

class TestNetexExportConfigDefaults:

    def test_default_lang_is_pt(self):
        cfg = _valid_config()
        assert cfg.default_lang == "pt"

    def test_default_version_strategy_is_timestamp(self):
        cfg = _valid_config()
        assert cfg.version_strategy == "timestamp"

    def test_rejects_unknown_version_strategy(self):
        with pytest.raises(ValidationError):
            NetexExportConfig(
                codespace="STCP",
                authority=NetexAuthority(id="A", name="A"),
                operator=NetexOperator(id="O", name="O", short_name="O"),
                version_strategy="rolling",  # type: ignore[arg-type]
            )


class TestNetexExportConfigPersistence:

    def test_load_returns_none_when_missing(self, isolated_netex_projects):
        assert load_netex_config(isolated_netex_projects) is None

    def test_roundtrip(self, isolated_netex_projects):
        slug = isolated_netex_projects
        cfg = _valid_config("STCP")
        path = save_netex_config(slug, cfg)
        assert path.name == NETEX_CONFIG_FILENAME
        loaded = load_netex_config(slug)
        assert loaded is not None
        assert loaded.codespace == "STCP"
        assert loaded.authority.name == "CIM do Ave"
        assert loaded.operator.short_name == "GUIMABUS"

    def test_save_creates_project_dir_if_needed(self, tmp_path, monkeypatch):
        from optisus.core.netex import config as netex_config
        monkeypatch.setattr(netex_config, "PROJECTS_ROOT", tmp_path / "projects")
        cfg = _valid_config("STCP")
        path = save_netex_config("fresh_project", cfg)
        assert path.exists()
        assert json.loads(path.read_text(encoding="utf-8"))["codespace"] == "STCP"


# ═══════════════════════════════════════════════════════════════════════════
# URN builder
# ═══════════════════════════════════════════════════════════════════════════

class TestBuildUrn:

    def test_happy_path(self):
        assert (
            build_urn("980123456", "StopPlace", "001")
            == "PT:980123456:StopPlace:001"
        )

    def test_sanitises_local_id_spaces(self):
        assert (
            build_urn("STCP", "Line", "Linha 1")
            == "PT:STCP:Line:Linha_1"
        )

    def test_sanitises_local_id_special_chars(self):
        assert (
            build_urn("STCP", "Line", "L1/A&B")
            == "PT:STCP:Line:L1_A_B"
        )

    def test_preserves_hyphen_and_underscore(self):
        assert (
            build_urn("STCP", "ServiceJourney", "trip-42_morning")
            == "PT:STCP:ServiceJourney:trip-42_morning"
        )

    def test_accepts_non_string_local_id(self):
        assert (
            build_urn("STCP", "Quay", 123)  # type: ignore[arg-type]
            == "PT:STCP:Quay:123"
        )

    def test_empty_codespace_raises(self):
        with pytest.raises(ValueError):
            build_urn("", "StopPlace", "001")

    def test_empty_object_type_raises(self):
        with pytest.raises(ValueError):
            build_urn("STCP", "", "001")

    def test_empty_local_id_raises(self):
        with pytest.raises(ValueError):
            build_urn("STCP", "StopPlace", "")


class TestSanitiseLocalId:

    def test_preserves_allowed(self):
        assert sanitise_local_id("abc_XYZ-123") == "abc_XYZ-123"

    def test_replaces_disallowed(self):
        assert sanitise_local_id("a b.c:d/e") == "a_b_c_d_e"


# ═══════════════════════════════════════════════════════════════════════════
# Mappings — coverage
# ═══════════════════════════════════════════════════════════════════════════

class TestRouteTypeMapping:

    def test_every_route_type_enum_has_a_vehicle_mode(self):
        missing = [rt for rt in RouteType if rt.value not in ROUTE_TYPE_TO_VEHICLE_MODE]
        assert missing == [], f"Unmapped RouteType values: {missing}"

    def test_helper_returns_expected_strings(self):
        assert vehicle_mode_for_route_type(RouteType.BUS.value) == "bus"
        assert vehicle_mode_for_route_type(RouteType.METRO.value) == "metro"
        assert vehicle_mode_for_route_type(RouteType.TROLLEYBUS.value) == "trolleyBus"
        assert vehicle_mode_for_route_type(RouteType.FERRY.value) == "water"
        assert vehicle_mode_for_route_type(RouteType.FUNICULAR.value) == "funicular"

    def test_helper_raises_on_unknown(self):
        with pytest.raises(KeyError):
            vehicle_mode_for_route_type(99)

    def test_all_values_are_netex_conformant(self):
        allowed_modes = {
            "air", "bus", "trolleyBus", "tram", "coach", "rail", "intercityRail",
            "urbanRail", "metro", "water", "cableway", "funicular", "taxi",
            "selfDrive", "other",
        }
        unknown = set(ROUTE_TYPE_TO_VEHICLE_MODE.values()) - allowed_modes
        assert unknown == set(), f"Non-NeTEx VehicleMode strings: {unknown}"

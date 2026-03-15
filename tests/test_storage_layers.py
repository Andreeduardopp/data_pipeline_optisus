"""Tests for project-aware storage layer helpers."""
import json
from pathlib import Path

import pytest

from storage_layers import (
    _safe_name,
    create_project,
    list_projects,
    list_project_runs,
    create_project_layered_run,
    create_layered_run,
    PROJECTS_ROOT,
    DATA_LAKE_ROOT,
)


@pytest.fixture(autouse=True)
def isolated_data_lake(tmp_path, monkeypatch):
    """Redirect DATA_LAKE_ROOT and PROJECTS_ROOT to a temp directory for every test."""
    fake_root = tmp_path / "data_lake_outputs"
    fake_projects = fake_root / "projects"
    monkeypatch.setattr("storage_layers.DATA_LAKE_ROOT", fake_root)
    monkeypatch.setattr("storage_layers.PROJECTS_ROOT", fake_projects)
    return fake_root


class TestSafeName:
    def test_basic(self):
        assert _safe_name("Fleet Identification") == "fleet_identification"

    def test_special_characters(self):
        assert _safe_name("  My--Project!!  ") == "my_project"

    def test_empty_after_strip(self):
        assert _safe_name("---") == ""


class TestCreateProject:
    def test_creates_directory_and_metadata(self, isolated_data_lake):
        path = create_project("My First Project")
        assert path.exists()
        assert (path / "runs").is_dir()

        meta = json.loads((path / "project.json").read_text())
        assert meta["name"] == "My First Project"
        assert meta["slug"] == "my_first_project"
        assert "created_at" in meta

    def test_idempotent(self, isolated_data_lake):
        p1 = create_project("Demo")
        meta_1 = (p1 / "project.json").read_text()
        p2 = create_project("Demo")
        meta_2 = (p2 / "project.json").read_text()
        assert p1 == p2
        assert meta_1 == meta_2

    def test_rejects_empty_name(self):
        with pytest.raises(ValueError):
            create_project("---")


class TestListProjects:
    def test_empty_when_no_projects(self, isolated_data_lake):
        assert list_projects() == []

    def test_returns_created_projects(self, isolated_data_lake):
        create_project("Alpha")
        create_project("Beta")
        projects = list_projects()
        slugs = {p["slug"] for p in projects}
        assert slugs == {"alpha", "beta"}


class TestCreateProjectLayeredRun:
    def test_creates_bronze_silver_gold(self, isolated_data_lake):
        create_project("test_proj")
        layers = create_project_layered_run("test_proj", "Fleet Identification")
        assert layers["bronze"].is_dir()
        assert layers["silver"].is_dir()
        assert layers["gold"].is_dir()
        assert "run_id" in layers
        assert "test_proj" in str(layers["root"])

    def test_run_id_contains_context(self, isolated_data_lake):
        create_project("p")
        layers = create_project_layered_run("p", "spatial_data")
        assert "spatial_data" in layers["run_id"]


class TestListProjectRuns:
    def test_empty_for_new_project(self, isolated_data_lake):
        create_project("empty_proj")
        assert list_project_runs("empty_proj") == []

    def test_returns_runs_with_lineage(self, isolated_data_lake):
        create_project("proj")
        layers = create_project_layered_run("proj", "test_context")
        lineage = {
            "run_id": layers["run_id"],
            "context": "test_context",
            "timestamp": "2025-01-01T00:00:00",
            "bronze": ["a.csv"],
            "silver": ["b.parquet"],
            "gold": ["c.json"],
        }
        (layers["root"] / "lineage.json").write_text(json.dumps(lineage))
        runs = list_project_runs("proj")
        assert len(runs) == 1
        assert runs[0]["run_id"] == layers["run_id"]


class TestLegacyCreateLayeredRun:
    def test_still_works_outside_projects(self, isolated_data_lake):
        layers = create_layered_run("legacy_context")
        assert layers["bronze"].is_dir()
        assert "projects" not in str(layers["root"])

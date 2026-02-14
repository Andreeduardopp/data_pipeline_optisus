import pandas as pd
import shutil
from pathlib import Path
from mlops_storage import save_feature_store
from schemas import GeographicData

def test_mlops_storage():
    # 1. Create dummy tabular data
    df_fleet = pd.DataFrame({"id": [1, 2], "value": [10.5, 20.0]})
    tabular_dfs = {"fleet_data": df_fleet}
    
    # 2. Create dummy geospatial metadata
    # We'll valid paths for the Pydantic model by creating temp files
    Path("temp_lines.shp").touch()
    Path("temp_stops.shp").touch()
    Path("temp_gtfs.zip").touch()
    Path("temp_zoning.shp").touch()
    Path("temp_terrain.tif").touch()
    Path("temp_demand.csv").touch()
    Path("temp_hydro.shp").touch()
    
    geo_data = {
        "lines_shp": str(Path("temp_lines.shp").absolute()),
        "stops_shp": str(Path("temp_stops.shp").absolute()),
        "gtfs_feed": str(Path("temp_gtfs.zip").absolute()),
        "urban_zoning": str(Path("temp_zoning.shp").absolute()),
        "contour_lines": str(Path("temp_terrain.tif").absolute()),
        "demand_mapping": str(Path("temp_demand.csv").absolute()),
        "hydrography_roads": str(Path("temp_hydro.shp").absolute())
    }
    
    # Instantiate Pydantic model to ensure compatibility
    geo_model = GeographicData(**geo_data)

    scenario_name = "test_scenario_001"
    
    print("Running save_feature_store...")
    run_dir = save_feature_store(tabular_dfs, geo_model, scenario_name)
    print(f"Resulting run directory: {run_dir}")
    
    # 3. Validation
    run_path = Path(run_dir)
    
    assert run_path.exists(), "Run directory not created"
    assert (run_path / "_SUCCESS").exists(), "_SUCCESS file missing"
    assert (run_path / "lineage_audit.json").exists(), "lineage_audit.json missing"
    assert (run_path / "mode_a_artifacts" / "fleet_data.parquet").exists(), "fleet_data.parquet missing"
    assert (run_path / "mode_b_artifacts" / "geo_references.json").exists(), "geo_references.json missing"
    
    print("\nVerification successful!")
    
    # Cleanup
    # shutil.rmtree(run_path) # Optional: keep it to inspect
    for f in geo_data.values():
        Path(f).unlink(missing_ok=True)

if __name__ == "__main__":
    test_mlops_storage()

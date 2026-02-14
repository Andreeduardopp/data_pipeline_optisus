import json
import os
from ingestion_geo import ingest_geo_metadata

def test_geo_ingestion():
    # Create dummy files to pass FilePath validation
    files = [
        "lines.shp", "stops.shp", "gtfs.zip", "zoning.shp", 
        "terrain.tif", "demand.csv", "hydro.shp"
    ]
    
    abs_paths = {}
    for f in files:
        with open(f, "w") as fh:
            fh.write("dummy content")
        abs_paths[f] = os.path.abspath(f)

    # Create valid JSON config
    config = {
        "lines_shp": abs_paths["lines.shp"],
        "stops_shp": abs_paths["stops.shp"],
        "gtfs_feed": abs_paths["gtfs.zip"],
        "urban_zoning": abs_paths["zoning.shp"],
        "contour_lines": abs_paths["terrain.tif"],
        "demand_mapping": abs_paths["demand.csv"],
        "hydrography_roads": abs_paths["hydro.shp"]
    }
    
    with open("geo_config_valid.json", "w") as f:
        json.dump(config, f)
        
    print("Testing valid configuration...")
    result = ingest_geo_metadata("geo_config_valid.json")
    
    if result:
        print("Success! Metadata validated.")
        print(result.model_dump())
    else:
        print("Failed!")

    # Cleanup
    for f in files:
        if os.path.exists(f):
            os.remove(f)
    if os.path.exists("geo_config_valid.json"):
        os.remove("geo_config_valid.json")

if __name__ == "__main__":
    test_geo_ingestion()

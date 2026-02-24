import json
import logging
from pathlib import Path
from typing import List, Optional

import geopandas as gpd
from pydantic import ValidationError

from schemas import GeographicData

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def ingest_spatial_data(
    file_path: str,
    required_columns: List[str],
    output_dir: str,
    file_name: str,
) -> Optional[gpd.GeoDataFrame]:
    """
    Ingest spatial data from Shapefile or GeoJSON, validate required columns and geometry,
    and save as GeoParquet.

    Args:
        file_path: Path to the spatial file (.shp or .geojson).
        required_columns: List of column names that must exist (e.g. ['stop_id', 'geometry']).
        output_dir: Directory to write the output GeoParquet file.
        file_name: Base name for the output file (without extension).

    Returns:
        Cleaned GeoDataFrame if successful, None otherwise.
    """
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix not in (".shp", ".geojson"):
        logger.error(f"Unsupported spatial file extension: {suffix}. Use .shp or .geojson")
        return None

    logger.info(f"Reading spatial file: {file_path} (type: {suffix})")

    try:
        gdf = gpd.read_file(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading spatial file: {e}")
        return None

    initial_count = len(gdf)

    # Validate required columns
    missing = [c for c in required_columns if c not in gdf.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}. Available: {list(gdf.columns)}")
        return None
    logger.info(f"Required columns present: {required_columns}")

    # Validate geometry: drop null, empty, or invalid geometries
    geom_col = gdf.geometry.name
    invalid = (
        gdf[geom_col].isna()
        | gdf[geom_col].is_empty
        | ~gdf[geom_col].is_valid
    )
    if invalid.any():
        n_invalid = int(invalid.sum())
        gdf = gdf[~invalid].copy()
        logger.warning(f"Dropped {n_invalid} row(s) with null, empty, or invalid geometry.")
    if gdf.empty:
        logger.warning("No valid geometries remaining.")
        return None

    logger.info(f"Spatial ingestion complete. {len(gdf)} valid rows (from {initial_count} total).")

    out_path = Path(output_dir) / f"{file_name}.geoparquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_path, index=False)
    logger.info(f"Saved GeoParquet to {out_path}")

    return gdf


def ingest_geo_metadata(json_path: str) -> Optional[GeographicData]:
    """
    Reads a JSON configuration file containing paths to geospatial datasets.
    Validates the data against the GeographicData Pydantic model.
    Checks if files specified in FilePath fields actually exist.
    """
    logger.info(f"Reading geospatial metadata from: {json_path}")
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {json_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return None

    try:
        # Pydantic's FilePath type automatically checks if the file exists on instantiation
        geo_data = GeographicData(**data)
        logger.info("Geospatial metadata successfully validated.")
        return geo_data

    except ValidationError as e:
        logger.error("Validation failed for one or more fields:")
        for error in e.errors():
            loc = " -> ".join(str(l) for l in error['loc'])
            msg = error['msg']
            logger.error(f"  - Field '{loc}': {msg}")
            
            # Specific hint for file path errors
            if error['type'] == 'path_not_file' or 'path' in error['type']:
                 logger.error(f"    Possible cause: The file specified in '{loc}' does not exist on disk.")
        
        return None

if __name__ == "__main__":
    # Example usage
    # result = ingest_geo_metadata("geo_config.json")
    pass

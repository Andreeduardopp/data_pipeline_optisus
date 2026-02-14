import json
import logging
from typing import Optional
from pydantic import ValidationError
from schemas import GeographicData

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Union
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_versioned_storage(base_dir: str = "feature_store") -> Path:
    """
    Create a timestamped directory for a given run.
    All successfully validated data for a given run must be saved inside this folder.

    Returns:
        Path to the created directory: {base_dir}/v_{YYYYMMDD_HHMMSS}/
    """
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_dir = Path(base_dir) / f"v_{timestamp_str}"
    versioned_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created versioned storage at {versioned_dir}")
    return versioned_dir


def save_feature_store(tabular_dfs: Dict[str, pd.DataFrame], geo_meta: Union[Dict, Any], scenario_name: str) -> str:
    """
    Saves tabular and geospatial artifacts to a structured feature store.
    
    Args:
        tabular_dfs: Dictionary of {"dataset_name": pd.DataFrame} containing operational/economic data.
        geo_meta: Dictionary or Pydantic model of geospatial metadata/paths.
        scenario_name: Name of the scenario for the run ID.
        
    Returns:
        The absolute path to the created run directory.
    """
    # 1. Generate Run ID
    # Format: run_id_{YYYYMMDD_HHMMSS}_{scenario_name}
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_id_{timestamp_str}_{scenario_name}"
    
    # Define Base Path
    # Using current working directory as root for /feature_store_outputs/
    base_dir = Path(os.getcwd()) / "feature_store_outputs" / run_id
    
    mode_a_dir = base_dir / "mode_a_artifacts"
    mode_b_dir = base_dir / "mode_b_artifacts"
    
    # 2. Create Directory Structure
    try:
        mode_a_dir.mkdir(parents=True, exist_ok=True)
        mode_b_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory structure at {base_dir}")
    except Exception as e:
        logger.error(f"Failed to create directories: {e}")
        raise

    # 3. Save Tabular Data (Mode A - Operational/Economic)
    saved_datasets = []
    
    for name, df in tabular_dfs.items():
        if isinstance(df, pd.DataFrame):
            # Save as highly compressed parquet
            file_path = mode_a_dir / f"{name}.parquet"
            # Using gzip for high compression
            df.to_parquet(file_path, compression='gzip', index=False)
            logger.info(f"Saved tabular dataset '{name}' to {file_path}")
            saved_datasets.append(name)
        else:
            logger.warning(f"Item '{name}' is not a DataFrame, skipping tabular save.")

    # 4. Save Geo Metadata (Mode B - Geospatial)
    if geo_meta:
        geo_path = mode_b_dir / "geo_references.json"
        
        # Extract dict from Pydantic model if necessary
        meta_data_to_save = geo_meta
        if hasattr(geo_meta, 'model_dump'): # Pydantic v2
            meta_data_to_save = geo_meta.model_dump()
        elif hasattr(geo_meta, 'dict'): # Pydantic v1
            meta_data_to_save = geo_meta.dict()
            
        with open(geo_path, 'w') as f:
            json.dump(meta_data_to_save, f, indent=4)
        logger.info(f"Saved geospatial metadata to {geo_path}")

    # 5. Lineage Audit
    lineage = {
        "execution_timestamp": datetime.now().isoformat(),
        "run_id_timestamp": timestamp_str,
        "scenario_name": scenario_name,
        "datasets_processed": saved_datasets,
        "geo_metadata_included": bool(geo_meta)
    }
    
    lineage_path = base_dir / "lineage_audit.json"
    with open(lineage_path, 'w') as f:
        json.dump(lineage, f, indent=4)
    logger.info(f"Created lineage audit at {lineage_path}")

    # 6. Success Flag
    success_file = base_dir / "_SUCCESS"
    success_file.touch()
    logger.info("Run flagged as _SUCCESS")

    return str(base_dir)

if __name__ == "__main__":
    pass

import argparse
import logging
import sys
from typing import Dict, Any

from schemas import FleetIdentification, FleetEnergyPerformance
from ingestion_tabular import ingest_tabular_data
from ingestion_geo import ingest_geo_metadata
from mlops_storage import save_feature_store

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Optisus Data Ingestion Pipeline CLI")
    
    parser.add_argument("--scenario-name", required=True, type=str, help="Name of the scenario (e.g., high_inflation_2026)")
    parser.add_argument("--fleet-csv", type=str, help="Path to the Fleet Identification CSV")
    parser.add_argument("--energy-csv", type=str, help="Path to the Fleet Energy Performance CSV")
    parser.add_argument("--geo-config", type=str, help="Path to the JSON file containing GIS file paths")
    
    args = parser.parse_args()
    
    clean_tabular_dfs = {}
    clean_geo_meta = None # Using None as default if empty, but dictionary is also fine if strict. Requirements said "Initialize empty dictionaries for ... clean_geo_meta" but geo_meta is later passed as a Dict/Model. 
    # Let's use empty dict for clean_geo_meta as requested.
    clean_geo_meta = {}

    # 1. Ingest Fleet CSV
    if args.fleet_csv:
        logger.info("Processing Fleet CSV...")
        df_fleet = ingest_tabular_data(args.fleet_csv, FleetIdentification)
        if not df_fleet.empty:
            clean_tabular_dfs["fleet_identification"] = df_fleet
        else:
             logger.warning("Fleet CSV yielded no valid rows.")

    # 2. Ingest Energy CSV
    if args.energy_csv:
        logger.info("Processing Energy CSV...")
        df_energy = ingest_tabular_data(args.energy_csv, FleetEnergyPerformance)
        if not df_energy.empty:
            clean_tabular_dfs["fleet_energy"] = df_energy
        else:
            logger.warning("Energy CSV yielded no valid rows.")

    # 3. Ingest Geo Config
    if args.geo_config:
        logger.info("Processing Geo Config...")
        geo_result = ingest_geo_metadata(args.geo_config)
        if geo_result:
            clean_geo_meta = geo_result
        else:
            logger.warning("Geo config ingestion failed.")

    # 4. Check for critical failure (no data)
    # The requirement says: "If both dictionaries are empty... exit(1)"
    # Note: clean_geo_meta might be a Pydantic model if successful, or {} if strict init. 
    # ingest_geo_metadata returns a Model or None.
    # So if clean_geo_meta is still {}, it's empty. If it's a Model, it's not empty.
    
    geo_is_empty = not clean_geo_meta
    tabular_is_empty = not clean_tabular_dfs
    
    if geo_is_empty and tabular_is_empty:
        logger.error("No valid data provided to the pipeline.")
        sys.exit(1)

    # 5. Save Feature Store
    logger.info("Saving to Feature Store...")
    try:
        run_path = save_feature_store(clean_tabular_dfs, clean_geo_meta if not geo_is_empty else {}, args.scenario_name)
        logger.info("Pipeline completed successfully.")
        print(f"OUTPUT_DIR: {run_path}")
    except Exception as e:
        logger.error(f"Pipeline failed during storage: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

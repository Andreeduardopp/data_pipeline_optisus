import argparse
import logging
import sys
from pathlib import Path

from schemas import (
    FleetIdentification,
    FleetEnergyPerformance,
    FinancialEconomicData,
    TransportedPassengers,
)
from ingestion_tabular import ingest_tabular_data
from ingestion_geo import ingest_geo_metadata, ingest_spatial_data
from mlops_storage import create_versioned_storage, save_feature_store

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_demo_mode():
    """
    Demo entry point: dummy paths, versioned storage, tabular + spatial ingestion, _SUCCESS.
    """
    # Dummy paths as per spec (e.g. raw_demand.xlsx, raw_economics.csv, raw_stops.geojson)
    raw_dir = Path(".").resolve()
    dummy_paths = {
        "demand": raw_dir / "raw_demand.xlsx",
        "economics": raw_dir / "raw_economics.csv",
        "stops": raw_dir / "raw_stops.geojson",
    }

    versioned_dir = create_versioned_storage(base_dir="feature_store")
    output_dir = str(versioned_dir)

    # Tabular: raw_demand.xlsx (TransportedPassengers), raw_economics.csv (FinancialEconomicData)
    if dummy_paths["demand"].exists():
        logger.info("Processing raw_demand.xlsx...")
        ingest_tabular_data(
            str(dummy_paths["demand"]),
            TransportedPassengers,
            output_dir=output_dir,
            file_name="raw_demand",
        )
    else:
        logger.warning(f"Dummy file not found: {dummy_paths['demand']}, skipping.")

    if dummy_paths["economics"].exists():
        logger.info("Processing raw_economics.csv...")
        ingest_tabular_data(
            str(dummy_paths["economics"]),
            FinancialEconomicData,
            output_dir=output_dir,
            file_name="raw_economics",
        )
    else:
        logger.warning(f"Dummy file not found: {dummy_paths['economics']}, skipping.")

    # Spatial: raw_stops.geojson
    if dummy_paths["stops"].exists():
        logger.info("Processing raw_stops.geojson...")
        ingest_spatial_data(
            str(dummy_paths["stops"]),
            required_columns=["stop_id", "geometry"],
            output_dir=output_dir,
            file_name="raw_stops",
        )
    else:
        logger.warning(f"Dummy file not found: {dummy_paths['stops']}, skipping.")

    (versioned_dir / "_SUCCESS").touch()
    logger.info("Demo run completed. _SUCCESS written.")
    print(f"OUTPUT_DIR: {versioned_dir}")


def main():
    parser = argparse.ArgumentParser(description="Optisus Data Ingestion Pipeline CLI")
    parser.add_argument("--demo", action="store_true", help="Run with dummy paths into feature_store/v_YYYYMMDD_HHMMSS/")
    parser.add_argument("--scenario-name", type=str, help="Name of the scenario (e.g., high_inflation_2026)")
    parser.add_argument("--fleet-csv", type=str, help="Path to the Fleet Identification CSV")
    parser.add_argument("--energy-csv", type=str, help="Path to the Fleet Energy Performance CSV")
    parser.add_argument("--geo-config", type=str, help="Path to the JSON file containing GIS file paths")

    args = parser.parse_args()

    if args.demo:
        run_demo_mode()
        return

    if not args.scenario_name:
        parser.error("--scenario-name is required when not using --demo")

    clean_tabular_dfs = {}
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

    geo_is_empty = not clean_geo_meta
    tabular_is_empty = not clean_tabular_dfs

    if geo_is_empty and tabular_is_empty:
        logger.error("No valid data provided to the pipeline.")
        sys.exit(1)

    logger.info("Saving to Feature Store...")
    try:
        run_path = save_feature_store(
            clean_tabular_dfs, clean_geo_meta if not geo_is_empty else {}, args.scenario_name
        )
        logger.info("Pipeline completed successfully.")
        print(f"OUTPUT_DIR: {run_path}")
    except Exception as e:
        logger.error(f"Pipeline failed during storage: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

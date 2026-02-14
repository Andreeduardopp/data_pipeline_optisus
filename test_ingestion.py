from ingestion_tabular import ingest_fleet_data
import pandas as pd

def test_ingestion():
    csv_path = "test_fleet.csv"
    print(f"Testing ingestion from {csv_path}...")
    df = ingest_fleet_data(csv_path)
    
    print("\nResulting DataFrame:")
    print(df)
    
    # Validation checks
    assert len(df) == 2, f"Expected 2 valid rows, got {len(df)}"
    # Check normalization and mapping
    expected_cols = [
        'owner_operator', 'vehicle_type', 'manufacturer_model', 'vehicle_id', 
        'emissions_standard', 'total_capacity', 'seated_capacity', 'average_age',
        'avg_consumption_per_vehicle', 'energy_cons_per_pax_km', 'average_co2_emissions', 'operational_status'
    ]
    # The dataframe will have these keys because we formed it from model_dump()
    # model_dump() uses the field names from the Pydantic model.
    
    # We merged two models. Let's check columns.
    print("\nColumns:", df.columns.tolist())
    
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"
    
    print("\nTest Passed!")

if __name__ == "__main__":
    test_ingestion()

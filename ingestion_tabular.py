import pandas as pd
import logging
from typing import Optional, Dict, Any, Type
from pydantic import ValidationError, BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_column_name(name: str) -> str:
    """
    Converts a column name to snake_case.
    """
    return (
        name.strip()
        .lower()
        .replace(" / ", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )

def validate_row(row: Dict[str, Any], row_index: int, model_class: Type[BaseModel]) -> Optional[Dict[str, Any]]:
    """
    Validates a single row against the provided Pydantic model class.
    Returns the dictionary dump if valid, None otherwise.
    """
    try:
        instance = model_class(**row)
        return instance.model_dump()

    except ValidationError as e:
        logger.warning(f"Row {row_index} failed validation for {model_class.__name__}: {e.errors()}")
        return None
    except ValueError as e:
        logger.warning(f"Row {row_index} failed value conversion: {e}")
        return None

def ingest_tabular_data(csv_path: str, model_class: Type[BaseModel]) -> pd.DataFrame:
    """
    Reads a CSV file, preprocesses it, and validates rows against a specific Pydantic schema.
    Returns a DataFrame containing only the valid rows.
    """
    logger.info(f"Starting ingestion for file: {csv_path} using model: {model_class.__name__}")
    
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        logger.error(f"File not found: {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return pd.DataFrame()

    # Preprocess column names
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    # Common mappings
    column_mapping = {
        "avg_consumption_vehicle": "avg_consumption_per_vehicle",
        "average_consumption_vehicle": "avg_consumption_per_vehicle",
        "energy_cons_per_pax_km": "energy_cons_per_pax_km",
    }
    df.rename(columns=column_mapping, inplace=True)

    valid_rows = []
    records = df.to_dict(orient='records')
    
    for i, record in enumerate(records):
        clean_record = {}
        for k, v in record.items():
            if isinstance(v, str):
                if v.lower() in ['n/a', 'nan', '', 'null']:
                    clean_record[k] = None
                else:
                    clean_record[k] = v
            else:
                if pd.isna(v):
                     clean_record[k] = None
                else:
                    clean_record[k] = v
        
        validated_data = validate_row(clean_record, i + 2, model_class)
        if validated_data:
            valid_rows.append(validated_data)

    if not valid_rows:
        logger.warning("No valid rows found.")
        return pd.DataFrame()

    clean_df = pd.DataFrame(valid_rows)
    logger.info(f"Ingestion complete. {len(clean_df)} valid rows out of {len(df)} total rows.")
    return clean_df

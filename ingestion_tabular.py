import pandas as pd
import logging
from pathlib import Path
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

def _read_tabular_file(file_path: str) -> pd.DataFrame:
    """Read tabular data from CSV or Excel based on file extension."""
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        logger.info(f"Detected file type: CSV")
        return pd.read_csv(file_path)
    if suffix in (".xlsx", ".xls"):
        logger.info(f"Detected file type: Excel ({suffix})")
        return pd.read_excel(file_path, engine="openpyxl" if suffix == ".xlsx" else None)
    raise ValueError(f"Unsupported file extension: {suffix}. Use .csv, .xlsx, or .xls")


def ingest_tabular_data(
    file_path: str,
    model_class: Type[BaseModel],
    output_dir: Optional[str] = None,
    file_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Ingest tabular data from CSV or Excel, validate rows against a Pydantic schema,
    optionally save cleaned data as Parquet.

    Args:
        file_path: Path to the CSV or Excel file.
        model_class: Pydantic model class for row validation.
        output_dir: If set, save the cleaned DataFrame here as Parquet.
        file_name: Base name for the output file (without extension). Used only if output_dir is set.

    Returns:
        DataFrame containing only valid rows. Invalid rows are logged and dropped.
    """
    logger.info(f"Starting ingestion for file: {file_path} using model: {model_class.__name__}")

    try:
        df = _read_tabular_file(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return pd.DataFrame()

    initial_count = len(df)

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
    records = df.to_dict(orient="records")

    for i, record in enumerate(records):
        clean_record = {}
        for k, v in record.items():
            if isinstance(v, str):
                if v.lower() in ["n/a", "nan", "", "null"]:
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

    dropped_count = initial_count - len(valid_rows)
    if dropped_count > 0:
        logger.warning(f"Dropped {dropped_count} row(s) due to validation errors.")

    if not valid_rows:
        logger.warning("No valid rows found.")
        return pd.DataFrame()

    clean_df = pd.DataFrame(valid_rows)
    logger.info(
        f"Ingestion complete. Ingested {initial_count} rows, dropped {dropped_count}, saved {len(clean_df)} valid rows."
    )

    if output_dir and file_name:
        out_path = Path(output_dir) / f"{file_name}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        clean_df.to_parquet(out_path, engine="pyarrow", index=False)
        logger.info(f"Saved Parquet to {out_path}")

    return clean_df

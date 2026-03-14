import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Type, List, Tuple
from pydantic import ValidationError, BaseModel, create_model

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

def _build_relaxed_model(
    model_class: Type[BaseModel],
    required_field_names: List[str],
) -> Type[BaseModel]:
    """
    Build a Pydantic model with the same fields as model_class, but only
    required_field_names are required; all others are Optional with default None.
    """
    fields_spec = {}
    for name, info in model_class.model_fields.items():
        ann = info.annotation
        if name in required_field_names:
            fields_spec[name] = (ann, info)
        else:
            fields_spec[name] = (Optional[ann], None)
    return create_model(
        f"{model_class.__name__}Relaxed",
        **fields_spec,
    )


def _check_mandatory_columns(
    df: pd.DataFrame, required_fields: List[str]
) -> Tuple[bool, List[str]]:
    """Return (all_present, list_of_missing)."""
    missing = [f for f in required_fields if f not in df.columns]
    return (len(missing) == 0, missing)


def read_tabular_for_preview(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Read tabular file and return DataFrame with normalized column names.
    For UI preview and mandatory-column check before full validation.
    Returns (df, None) on success, (None, error_message) on failure.
    """
    try:
        df = _read_tabular_file(file_path)
    except FileNotFoundError:
        return None, f"File not found: {file_path}"
    except Exception as e:
        return None, str(e)
    df.columns = [normalize_column_name(c) for c in df.columns]
    column_mapping = {
        "avg_consumption_vehicle": "avg_consumption_per_vehicle",
        "average_consumption_vehicle": "avg_consumption_per_vehicle",
        "energy_cons_per_pax_km": "energy_cons_per_pax_km",
    }
    df.rename(columns=column_mapping, inplace=True)
    return df, None


def validate_row(
    row: Dict[str, Any], row_index: int, model_class: Type[BaseModel]
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Validates a single row against the provided Pydantic model class.
    Returns (dictionary dump, None) if valid, (None, error_message) otherwise.
    """
    try:
        instance = model_class(**row)
        return instance.model_dump(), None
    except ValidationError as e:
        err_msg = "; ".join(f"{x.get('loc', ())}: {x.get('msg', '')}" for x in e.errors())
        logger.warning(f"Row {row_index} failed validation for {model_class.__name__}: {e.errors()}")
        return None, err_msg
    except ValueError as e:
        logger.warning(f"Row {row_index} failed value conversion: {e}")
        return None, str(e)

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
    required_fields_override: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Ingest tabular data from CSV or Excel, validate rows against a Pydantic schema,
    optionally save cleaned data as Parquet.

    Args:
        file_path: Path to the CSV or Excel file.
        model_class: Pydantic model class for row validation.
        output_dir: If set, save the cleaned DataFrame here as Parquet.
        file_name: Base name for the output file (without extension). Used only if output_dir is set.
        required_fields_override: If set, only these columns are enforced as mandatory; they must
            exist in the file. Row validation uses a relaxed model where other fields are optional.

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

    validation_model = model_class
    if required_fields_override is not None:
        ok, missing = _check_mandatory_columns(df, required_fields_override)
        if not ok:
            logger.error(f"Missing mandatory columns: {missing}. Available: {list(df.columns)}")
            return pd.DataFrame()
        validation_model = _build_relaxed_model(model_class, required_fields_override)

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

        validated_data, _ = validate_row(clean_record, i + 2, validation_model)
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


def validate_tabular_for_ui(
    file_path: str,
    model_class: Type[BaseModel],
    required_fields_override: Optional[List[str]] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str], List[Tuple[int, str]]]:
    """
    Validate tabular file for UI: returns read_error, missing_columns, row_errors, and clean_df.
    Returns (clean_df, read_error, missing_columns, row_errors).
    - read_error: if file could not be read.
    - missing_columns: list of mandatory columns missing from the file.
    - row_errors: list of (row_index_1based, error_message) for invalid rows.
    - clean_df: validated DataFrame (only valid rows); None if read error or mandatory columns missing.
    """
    from ui_validation import get_default_required_fields

    df, read_err = read_tabular_for_preview(file_path)
    if read_err:
        return None, read_err, [], []

    mandatory = (
        required_fields_override
        if required_fields_override is not None
        else get_default_required_fields(model_class)
    )
    ok, missing = _check_mandatory_columns(df, mandatory)
    if not ok:
        return None, None, missing, []

    validation_model = (
        _build_relaxed_model(model_class, mandatory)
        if required_fields_override is not None
        else model_class
    )

    valid_rows = []
    row_errors: List[Tuple[int, str]] = []
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

        validated_data, err_msg = validate_row(clean_record, i + 2, validation_model)
        if validated_data:
            valid_rows.append(validated_data)
        else:
            row_errors.append((i + 2, err_msg or "Validation failed"))

    if not valid_rows:
        return None, None, [], row_errors

    clean_df = pd.DataFrame(valid_rows)
    return clean_df, None, [], row_errors

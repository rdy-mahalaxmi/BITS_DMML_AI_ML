# validate.py
import pandas as pd
import os
from datetime import datetime
import logging

# -----------------------------
# Directories & Logging
# -----------------------------
STORAGE_DIR = "data/lake"
REPORT_DIR = "data/reports"
LOG_DIR = "logs"

os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/validate.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("validate")

# -----------------------------
# Helper: get latest file for a source
# -----------------------------
def get_latest_file(folder):
    all_dates = sorted(os.listdir(folder), reverse=True)
    if not all_dates:
        raise FileNotFoundError(f"No folders found in {folder}")
    latest_folder = os.path.join(folder, all_dates[0])
    files = os.listdir(latest_folder)
    if not files:
        raise FileNotFoundError(f"No files found in {latest_folder}")
    return os.path.join(latest_folder, files[0])

# -----------------------------
# Validation function
# -----------------------------
def validate_file(file_path):
    df = pd.read_csv(file_path)
    logger.info(f"Validating file: {file_path}")
    print(f"[Validate] File: {file_path}")
    print(f"[Validate] Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    # 1. Missing values
    missing = df.isnull().sum()
    missing_count = missing.sum()
    print(f"[Validate] Missing values per column:\n{missing}")
    if missing_count > 0:
        logger.warning(f"Missing values found:\n{missing}")

    # 2. Data types
    print(f"[Validate] Data types:\n{df.dtypes}")
    logger.info(f"Data types:\n{df.dtypes}")

    # 3. Duplicates
    duplicates = df.duplicated().sum()
    print(f"[Validate] Duplicate rows: {duplicates}")
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows")

    # 4. Numeric ranges and conversion (example for TotalCharges)
    if 'TotalCharges' in df.columns:
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        invalid_total = df[df['TotalCharges'] < 0].shape[0]
        print(f"[Validate] Rows with TotalCharges < 0: {invalid_total}")
        if invalid_total > 0:
            logger.warning(f"Rows with invalid TotalCharges: {invalid_total}")
        invalid_nan = df['TotalCharges'].isna().sum()
        print(f"[Validate] Rows with invalid/missing TotalCharges: {invalid_nan}")
        if invalid_nan > 0:
            logger.warning(f"Rows with invalid/missing TotalCharges: {invalid_nan}")

    # -----------------------------
    # Generate Data Quality Report
    # -----------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_name = os.path.splitext(os.path.basename(file_path))[0]
    report_path = os.path.join(REPORT_DIR, f"{dataset_name}_data_quality_{timestamp}.csv")

    report = pd.DataFrame({
        'Column': df.columns,
        'DataType': df.dtypes,
        'MissingValues': df.isnull().sum(),
        'UniqueValues': df.nunique()
    })

    report.to_csv(report_path, index=False)
    print(f"[Validate] Data quality report saved at {report_path}")
    logger.info(f"Data quality report saved at {report_path}")

# -----------------------------
# Main Runner
# -----------------------------
def run_validation():
    sources = ["csv_source", "api_source"]
    for source in sources:
        source_path = os.path.join(STORAGE_DIR, source)
        try:
            latest_file = get_latest_file(source_path)
            validate_file(latest_file)
        except FileNotFoundError as e:
            print(f"[Validate] Warning: {e}")
            logger.warning(e)

if __name__ == "__main__":
    run_validation()

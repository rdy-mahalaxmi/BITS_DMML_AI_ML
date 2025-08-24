import pandas as pd
import os
from sklearn.preprocessing import StandardScaler, LabelEncoder
import sqlite3
from datetime import datetime
import logging

# -----------------------------
# Directories & Logging
# -----------------------------
PROC_DIR = "data/processed"
DB_DIR = "data/db"
LOG_DIR = "logs"
SCHEMA_FILE = "schema.sql"
SUMMARY_FILE = "transform_summary.md"

os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/transform.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("transform")

# -----------------------------
# Helper: get latest processed file for a source
# -----------------------------
def get_latest_file(folder, source):
    source_files = [f for f in os.listdir(folder) if f.startswith(source)]
    if not source_files:
        raise FileNotFoundError(f"No processed files found for source '{source}' in {folder}")
    latest_file = sorted(source_files, reverse=True)[0]
    return os.path.join(folder, latest_file)

# -----------------------------
# Generate SQL Schema
# -----------------------------
def generate_schema(table_name, df, schema_path):
    with open(schema_path, "a") as f:
        f.write(f"-- Schema for {table_name}\n")
        f.write(f"CREATE TABLE {table_name} (\n")
        cols = []
        for col, dtype in zip(df.columns, df.dtypes):
            if "int" in str(dtype):
                sql_type = "INTEGER"
            elif "float" in str(dtype):
                sql_type = "REAL"
            else:
                sql_type = "TEXT"
            cols.append(f"    {col} {sql_type}")
        f.write(",\n".join(cols))
        f.write("\n);\n\n")

# -----------------------------
# Update Transformation Summary
# -----------------------------
def update_summary(dataset_name, df, summary_path):
    with open(summary_path, "a") as f:
        f.write(f"## Dataset: {dataset_name}\n")
        f.write(f"- Total rows: {df.shape[0]}\n")
        f.write(f"- Total columns: {df.shape[1]}\n")
        f.write("- Derived Features:\n")
        derived_features = []
        if "TotalSpend" in df.columns:
            derived_features.append("TotalSpend = tenure * MonthlyCharges")
        if "TenureYears" in df.columns:
            derived_features.append("TenureYears = tenure / 12")
        if "AvgMonthlySpend" in df.columns:
            derived_features.append("AvgMonthlySpend = TotalSpend / tenure")
        for feat in derived_features:
            f.write(f"  - {feat}\n")
        f.write("- Scaled numeric features using StandardScaler\n")
        f.write("- Encoded categorical columns using LabelEncoder\n\n")

# -----------------------------
# Feature Engineering / Transformation
# -----------------------------
def transform_dataset(file_path, dataset_name):
    df = pd.read_csv(file_path)
    logger.info(f"Transforming dataset: {file_path}")
    print(f"[Transform] Dataset: {file_path} | Rows: {df.shape[0]} | Columns: {df.shape[1]}")

    # -----------------------------
    # Feature Engineering
    # -----------------------------
    if 'tenure' in df.columns and 'MonthlyCharges' in df.columns:
        df['TotalSpend'] = df['tenure'] * df['MonthlyCharges']
        df['TenureYears'] = df['tenure'] / 12
        df['AvgMonthlySpend'] = df['TotalSpend'] / df['tenure']

    # Label encode categorical features
    for col in df.select_dtypes(include='object').columns:
        df[col] = LabelEncoder().fit_transform(df[col])

    # Standardize numeric features
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    df[numeric_cols] = StandardScaler().fit_transform(df[numeric_cols])

    # -----------------------------
    # FIX: Avoid double "_transformed" suffix
    # -----------------------------
    if dataset_name.endswith("_transformed"):
        transformed_name = dataset_name
    else:
        transformed_name = f"{dataset_name}_transformed"

    # Save transformed CSV
    transformed_csv_path = os.path.join(PROC_DIR, f"{transformed_name}.csv")
    df.to_csv(transformed_csv_path, index=False)
    logger.info(f"Transformed CSV saved at {transformed_csv_path}")
    print(f"[Transform] Transformed dataset saved at {transformed_csv_path}")

    # Save to SQLite database
    db_path = os.path.join(DB_DIR, "churn_data.db")
    conn = sqlite3.connect(db_path)
    table_name = transformed_name.lower() + "_table"
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    logger.info(f"Dataset stored in SQLite DB: {db_path} | Table: {table_name}")
    print(f"[Transform] Dataset stored in SQLite DB: {db_path} | Table: {table_name}")

    # Generate schema & summary
    generate_schema(table_name, df, SCHEMA_FILE)
    update_summary(dataset_name, df, SUMMARY_FILE)

    # Example queries
    print(f"[Transform] Sample queries for table '{table_name}':")
    print(f"1. SELECT COUNT(*) FROM {table_name};")
    print(f"2. SELECT * FROM {table_name} LIMIT 5;")
    if 'TotalSpend' in df.columns:
        print(f"3. SELECT AVG(TotalSpend) FROM {table_name};")

# -----------------------------
# Main Runner
# -----------------------------
def run_transformation():
    # Clear previous schema & summary before new run
    open(SCHEMA_FILE, "w").close()
    open(SUMMARY_FILE, "w").close()

    sources = ["churn_prepared", "users_prepared"]
    created_tables = []

    for source in sources:
        try:
            latest_file = get_latest_file(PROC_DIR, source)
            dataset_name = os.path.splitext(os.path.basename(latest_file))[0]
            transform_dataset(latest_file, dataset_name)
            transformed_name = dataset_name if dataset_name.endswith("_transformed") else f"{dataset_name}_transformed"
            created_tables.append(transformed_name.lower() + "_table")
        except FileNotFoundError as e:
            logger.warning(e)
            print(f"[Transform] Warning: {e}")

    # List all created tables at the end
    if created_tables:
        print("\n[Transform]  Created the following tables in SQLite DB:")
        for table in created_tables:
            print(f"   - {table}")
    else:
        print("\n[Transform]  No tables were created. Check your input files.")

if __name__ == "__main__":
    run_transformation()
